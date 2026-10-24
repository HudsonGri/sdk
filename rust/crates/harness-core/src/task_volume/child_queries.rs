//! Lifecycle cuts exclude retained prompts, inbox bodies and earlier answers.
use super::*;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// A bounded direct-child page, resuming strictly after its last reference.
    /// `None` means a bounded suffix replay made progress; continue with the
    /// same key. New references preceding an existing cursor arrive through
    /// the ordinary Children follow opened at the caller's initial cut.
    pub async fn child_references_page(
        &self,
        after: Option<&Component>,
        maximum: usize,
    ) -> Result<Option<Vec<Component>>> {
        if maximum == 0 {
            return Err(Error::LimitExceeded);
        }
        let (_, complete) = self.refresh_bounded::<true>(maximum as u64).await?;
        if !complete {
            return Ok(None);
        }
        let index = self.index.lock().await;
        Ok(Some(reference_page(&index, after, maximum)))
    }

    /// Immutable outcomes after an exact turn cursor. Only the requested
    /// child's result range is visited; retained prompts and siblings are not.
    pub async fn child_results_page(
        &self,
        reference: &Component,
        after: u64,
        maximum: usize,
    ) -> Result<Option<Vec<FileValue>>> {
        if maximum == 0 {
            return Err(Error::LimitExceeded);
        }
        let (_, complete) = self.refresh_bounded::<true>(maximum as u64).await?;
        if !complete {
            return Ok(None);
        }
        let Some(first) = after.checked_add(1) else {
            return Ok(Some(Vec::new()));
        };
        let key = |turn| FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Result(turn),
        };
        let index = self.index.lock().await;
        Ok(Some(
            index
                .files
                .range(key(first)..=key(u64::MAX))
                .take(maximum)
                .map(|(_, stored)| {
                    item_examined();
                    stored.file.clone()
                })
                .collect(),
        ))
    }

    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[doc(hidden)]
    pub async fn child_request(&self, reference: &Component) -> Result<FileValue> {
        let key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        };
        let snapshot = self
            .snapshot_files(&[key.clone()], &Query::File(key))
            .await?;
        snapshot
            .files
            .into_iter()
            .next()
            .ok_or(Error::InvalidSegment)
    }

    #[doc(hidden)]
    pub async fn child_input_count(&self, reference: &Component) -> Result<u64> {
        self.refresh().await?;
        let index = self.index.lock().await;
        Ok(index
            .child_inputs
            .get(reference)
            .map_or(0, |inputs| inputs.len() as u64))
    }

    // The derived index preserves accepted order, independently of inbox names.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[doc(hidden)]
    pub async fn child_input(
        &self,
        reference: &Component,
        delivered: u64,
    ) -> Result<Option<FileValue>> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let key = index
            .child_inputs
            .get(reference)
            .and_then(|inputs| usize::try_from(delivered).ok().and_then(|i| inputs.get(i)));
        Ok(key
            .and_then(|key| index.files.get(key))
            .map(|stored| stored.file.clone()))
    }

    #[doc(hidden)]
    pub async fn child_lifecycle(&self, reference: Option<&Component>) -> Result<Snapshot> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let mut files = Vec::new();
        let candidates = match reference {
            Some(reference) => index
                .child_inputs
                .range(reference.clone()..=reference.clone()),
            None => index.child_inputs.range(..),
        };
        for (candidate, _) in candidates {
            let key = |file| FileKey::Child {
                reference: candidate.clone(),
                file,
            };
            for kind in [ChildFile::Control, ChildFile::Status, ChildFile::Delivered] {
                if let Some(stored) = index.files.get(&key(kind)) {
                    files.push(stored.file.clone());
                }
            }
            if let Some((_, stored)) = index
                .files
                .range(key(ChildFile::Result(0))..=key(ChildFile::Result(u64::MAX)))
                .next_back()
            {
                files.push(stored.file.clone());
            }
        }
        let generation = self.generation(&index);
        drop(index);
        let query = reference.map_or(Query::Children, |reference| Query::Child(reference.clone()));
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(Snapshot {
            files,
            cursor: self.cursor(&query, generation, watermarks)?,
            generation,
        })
    }
}

fn reference_page(index: &Index, after: Option<&Component>, maximum: usize) -> Vec<Component> {
    index
        .child_requests
        .range((
            after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            std::ops::Bound::Unbounded,
        ))
        .take(maximum)
        .map(|reference| {
            item_examined();
            reference.clone()
        })
        .collect()
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    fn reference(n: u32) -> Component {
        Component::new(format!("worker-{n:06}")).unwrap()
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn child_pages_match_the_independent_sorted_request_set(
            admitted in proptest::collection::vec(0u32..512, 0..256), window in 1usize..17,
        ) {
            let mut index = Index::default();
            let expected: std::collections::BTreeSet<_> = admitted.iter().map(|n| reference(*n)).collect();
            for n in 0..512 {
                let key = FileKey::Child { reference: reference(n), file: ChildFile::Inbox(Component::new("mail").unwrap()) };
                apply(&mut index, FileValue::new(key, Vec::new()), 1);
            }
            for n in admitted {
                let key = FileKey::Child { reference: reference(n), file: ChildFile::Request };
                apply(&mut index, FileValue::new(key, Vec::new()), 1);
            }
            let mut after = None;
            let mut actual = Vec::new();
            loop {
                let (page, examined) = crate::test_support::measure_task_volume_work(|| reference_page(&index, after.as_ref(), window));
                prop_assert!(examined <= window as u64);
                if page.is_empty() { break; }
                prop_assert!(after.as_ref().is_none_or(|old| old < &page[0]));
                after = page.last().cloned();
                actual.extend(page);
            }
            prop_assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
        }
    }
    fn populated() -> Index {
        let mut index = Index::default();
        for n in 0..100_000 {
            index.child_requests.insert(reference(n));
        }
        index
    }
    #[test]
    fn one_child_page_is_bounded_with_one_hundred_thousand_siblings() {
        let index = populated();
        let (page, examined) = crate::test_support::measure_task_volume_work(|| {
            reference_page(&index, Some(&reference(99_990)), 4)
        });
        assert_eq!(page, (99_991..99_995).map(reference).collect::<Vec<_>>());
        assert_eq!(examined, 4);
    }
    #[test]
    #[should_panic(expected = "child page exceeded its work bound")]
    fn negative_control_filtering_all_siblings_exceeds_the_page_bound() {
        let index = populated();
        let (_, examined) = crate::test_support::measure_task_volume_work(|| {
            index
                .child_requests
                .iter()
                .filter(|candidate| {
                    item_examined();
                    *candidate > &reference(99_990)
                })
                .take(4)
                .cloned()
                .collect::<Vec<_>>()
        });
        assert!(examined <= 4, "child page exceeded its work bound");
    }
}

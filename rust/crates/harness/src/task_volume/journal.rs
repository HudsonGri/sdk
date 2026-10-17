//! A task file joined to its owner's canonical journal transaction.
use super::*;

pub(crate) struct JournalFile {
    pub(crate) condition: CommitCondition,
    pub(crate) path: StreamPath,
    pub(crate) record: acyclic_stream::OwnedBytes,
}

impl<P: StreamProvider> TaskVolume<P> {
    pub(crate) async fn prepare_journal_file(
        &self,
        file: &FileValue,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> crate::Result<Option<JournalFile>> {
        // The chunk's ordinal is its canonical journal position. That journal's
        // CAS fences identity, order and immutability; retries and rejected
        // events never reach preparation. No task-index replay is needed.
        if !matches!(
            file.key,
            FileKey::Task {
                file: TaskFile::Chunk { .. },
                ..
            }
        ) {
            return Err(Error::Unauthorized.into());
        }
        file.validate()?;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if crate::test_support::differential::chunk_scan_enabled() {
            // Deliberately quadratic regression: rebuild and copy the prefix.
            let _ = self.snapshot(&Query::Root, 1024).await?;
        }
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let bounds =
            crate::store::operations::bounds_with_work(&self.provider, &self.path, &mut work)
                .await?
                .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?;
        let anchor = bounds.tail.checked_sub(1).ok_or(Error::ResetRequired)?;
        let mut records = crate::store::operations::read_records(
            &self.provider,
            &self.path,
            anchor,
            1,
            &mut work,
        )
        .await?
        .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?;
        let record = records.next(&mut work).await?.ok_or(Error::ResetRequired)?;
        item_examined();
        if record.sequence != anchor {
            return Err(Error::ResetRequired.into());
        }
        let condition = CommitCondition::Head {
            path: self.path.clone(),
            expected: bounds.tail,
            commit_id: record.commit_id,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let original = io.and_then(crate::filesystem::PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let original = None;
        let (record, _) = segment::encode_record(file, original)?;
        Ok(Some(JournalFile {
            condition,
            path: self.path.clone(),
            record,
        }))
    }
}

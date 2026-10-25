use super::*;

// All lazy-path consumers use the same owner resolution and output verification.
// Caller-specific authority narrowing stays at the admitted task boundary.
pub(crate) async fn verified_private_path<'a, 'io>(
    reader: &'a dyn ContentResidencyVerifier,
    selection: (&'a VolumeRef, &'a str, &'a str, Option<&'a GenerationRef>),
    maximum_bytes: u64,
    access: ContentAccess<'a, 'io>,
) -> Result<(
    FileRef,
    acyclic_stream::OwnedBytes,
    Option<ContentHold<'io>>,
)>
where
    'io: 'a,
{
    if maximum_bytes > MAX_EXACT_JS_INTEGER {
        return Err(crate::executor::stock_boundary(
            access.io,
            crate::wire::ErrorCode::Invalid,
            "private source byte bound exceeds protocol precision",
        ));
    }
    let (volume, prefix, path, generation) = selection;
    let (file, bytes, hold) = reader
        .read_private_path(volume, prefix, path, generation, maximum_bytes, access)?
        .await?;
    if file.volume() != volume || file.path() != path || bytes.len() as u64 > maximum_bytes {
        return Err(crate::executor::stock_boundary(
            access.io,
            crate::wire::ErrorCode::Storage,
            "private path reader returned another file",
        ));
    }
    crate::executor::with_history_io(access.io, |work| file.descriptor().verify(&bytes, work))?;
    Ok((file, bytes, hold))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    struct Reader {
        file: FileRef,
        bytes: Vec<u8>,
    }

    impl ContentResidencyVerifier for Reader {
        fn read_private_path<'a, 'io>(
            &'a self,
            _volume: &'a VolumeRef,
            _prefix: &'a str,
            _path: &'a str,
            _generation: Option<&'a GenerationRef>,
            _maximum: u64,
            access: ContentAccess<'a, 'io>,
        ) -> Result<
            BoundedContentFuture<
                'a,
                (
                    FileRef,
                    acyclic_stream::OwnedBytes,
                    Option<ContentHold<'io>>,
                ),
            >,
        >
        where
            'io: 'a,
        {
            BoundedContentFuture::new(access.io, || async move {
                Ok((self.file.clone(), self.bytes.clone().into(), None))
            })
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn every_lazy_consumer_rejects_provider_substitution(
            bytes in proptest::collection::vec(any::<u8>(), 1..128),
            mutation in 0_u8..5,
        ) {
            let volume = VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None).unwrap(),
                "private".into(), VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([11; 16])), &mut None,
            ).unwrap();
            let file = FileRef::new(volume, "file.txt".into(), "generation-1".into(),
                FileDescriptor::from_bytes(&bytes, "text/plain", &mut None).unwrap(),
                "file.txt".into(), &mut None,
            ).unwrap();
            let (generation, _) = GenerationRef::new(
                file.volume().provider(), b"generation", None, None,
            ).unwrap();
            let source = crate::context::PinnedContextPath {
                root: crate::context::ContextRoot {
                    volume: file.volume().clone(), directory: String::new(),
                },
                generation, path: file.path().into(),
            };
            let mut returned = serde_json::to_value(&file).unwrap();
            let mut actual = bytes.clone();
            let mut maximum = bytes.len() as u64;
            match mutation {
                1 => returned["path"] = serde_json::json!("other"),
                2 => returned["volume"]["id"] = serde_json::json!("other"),
                3 => actual[0] ^= 1,
                4 => maximum -= 1,
                _ => {},
            }
            let reader = Reader { file: serde_json::from_value(returned).unwrap(), bytes: actual };
            let resolved = futures::executor::block_on(verified_private_path(
                &reader, (&source.root.volume, "", &source.path, Some(&source.generation)),
                maximum, ContentAccess::default(),
            ));
            prop_assert_eq!(resolved.is_ok(), mutation == 0);
            let pinned = futures::executor::block_on(source.read_remaining(&reader, maximum));
            prop_assert_eq!(pinned.is_ok(), mutation == 0);
            let skill = crate::context::SkillMetadata {
                name: "private-skill".into(), description: "Read the selected body".into(),
                fields: BTreeMap::new(), source: source.clone(),
            };
            let invoked = futures::executor::block_on(skill.invoke(&reader, maximum));
            prop_assert_eq!(invoked.is_ok(), mutation == 0
                && std::str::from_utf8(&bytes).is_ok() && !bytes.contains(&0));
            // The unchecked owner response is accepted even for a substituted body;
            // removing shared verification therefore makes this oracle fail.
            let unchecked = reader.read_private_path(
                &source.root.volume, "", &source.path, Some(&source.generation),
                maximum, ContentAccess::default(),
            ).unwrap();
            prop_assert!(futures::executor::block_on(unchecked).is_ok());
        }
    }
}

use super::*;
use acyclic_stream::MemoryStream;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn delta_segments_are_accepted_together_and_retry_without_new_records(
        parts in prop::collection::vec("[a-zλ]{1,16}", 1..8),
        mutation in 0u8..5,
    ) {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()),
                StreamPath::new("atomic-delta-property").unwrap()).await.unwrap();
            let task = Component::new("task").unwrap();
            let mut writes: Vec<_> = parts.iter().enumerate().map(|(part, text)| {
                let mut write = Write::new(FileKey::Task { task: task.clone(),
                    file: TaskFile::Chunk { turn: 0, sequence: part as u64 } }, text.as_bytes().to_vec());
                write.channel = Some(Channel::Text);
                write
            }).collect();
            let expected = volume.head().await.unwrap();
            match mutation {
                1 => writes[0].channel = None,
                2 => writes[0].publication = Some(crate::OperationId::from_bytes([1; 16])),
                3 => writes[0].key = FileKey::Goal,
                4 => writes.clear(),
                _ => {},
            }
            let result = volume.append_delta_if(expected, &writes, vec![], None, None).await;
            prop_assert_eq!(result.is_ok(), mutation == 0);
            let snapshot = volume.snapshot(&Query::Root, 16).await.unwrap();
            if mutation == 0 {
                let generation = result.unwrap();
                prop_assert_eq!(generation.position, expected.position + parts.len() as u64);
                prop_assert_eq!(&snapshot.files, &writes);
                prop_assert_eq!(volume.append_delta_if(generation, &writes, vec![], None, None).await.unwrap(), generation);
                let reopened = TaskVolume::open(volume.provider.clone(), volume.path.clone()).await.unwrap();
                prop_assert_eq!(reopened.snapshot(&Query::Root, 16).await.unwrap().files, writes);
            } else {
                prop_assert!(snapshot.files.is_empty());
                prop_assert_eq!(snapshot.generation, expected);
            }
            Ok(())
        })?;
    }
}

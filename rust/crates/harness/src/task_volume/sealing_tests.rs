use super::*;
use proptest::prelude::*;

#[tokio::test]
async fn legacy_headers_remain_readable_and_old_schema_cursors_reset()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for schema in 2..=4 {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let path = StreamPath::new(format!("schema-{schema}"))?;
        let header = Header {
            volume: Uuid::from_bytes([1; 16]),
            epoch: Uuid::from_bytes([2; 16]),
            schema,
        };
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![serde_json::to_vec(&header)?.into()],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        let volume = TaskVolume::open_existing(provider.clone(), path.clone()).await?;
        let snapshot = volume.snapshot(&Query::Root, 1).await?;
        assert_eq!(snapshot.cursor.schema, SCHEMA);
        let mut legacy = snapshot.cursor;
        legacy.schema = schema;
        let reset = volume.observe(Query::Root, Some(legacy), 1).await?;
        assert!(reset.reset);
        assert_eq!(
            reset.snapshot.ok_or("missing schema reset")?.cursor.schema,
            SCHEMA
        );
        let mut records = provider
            .read(ReadRequest {
                path,
                from: 0,
                limit: 1,
            })
            .await?;
        let record = records.next().await.ok_or("missing legacy header")??;
        assert!(FileValue::from_record(&record)?.is_none());
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn retained_completion_is_required_to_reject_late_stream_files(turn in any::<u64>(), sequence in any::<u64>()) {
        let task = Component::new("t").unwrap();
        let completion = Write::new(FileKey::Task { task: task.clone(), file: TaskFile::Completion(turn) }, b"{}".to_vec());
        let mut chunk = Write::new(FileKey::Task { task, file: TaskFile::Chunk { turn, sequence } }, b"late".to_vec());
        chunk.channel = Some(Channel::Text);
        let mut index = Index::default();
        apply(&mut index, completion.clone(), 1);
        prop_assert!(matches!(validate_file(&index, &chunk), Err(Error::ImmutableConflict(key)) if key == completion.key));
        // Negative control: losing the completion marker while rebuilding the
        // index must be detected by the same rejection oracle.
        index.files.remove(&completion.key);
        prop_assert!(!matches!(validate_file(&index, &chunk), Err(Error::ImmutableConflict(_))));
    }

    #[test]
    fn one_model_publication_cannot_be_reassigned_to_another_turn(turn in any::<u64>(), publication in any::<[u8; 16]>()) {
        let task = Component::new("t").unwrap();
        let publication = crate::OperationId::from_bytes(publication);
        let mut original = Write::new(FileKey::Task { task: task.clone(), file: TaskFile::TurnRequest(turn) }, b"request".to_vec());
        original.publication = Some(publication);
        let mut next = original.clone();
        next.key = FileKey::Task { task: task.clone(), file: TaskFile::TurnRequest(turn.wrapping_add(1)) };
        let mut index = Index::default();
        apply(&mut index, original, 1);
        prop_assert_eq!(index.turn_publications.get(&(task.clone(), publication)), Some(&turn));
        prop_assert!(matches!(validate_file(&index, &next), Err(Error::ImmutableConflict(_))));
        // The old behavior without the inverse publication index admits the
        // same dispatch twice. The identical rejection oracle detects it.
        index.turn_publications.clear();
        prop_assert!(validate_file(&index, &next).is_ok());
    }

    #[test]
    fn failure_identities_advance_without_wrapping_or_reading_neighbor_turns(
        stored in prop_oneof![Just(u64::MAX), any::<u64>()], observed in any::<u64>(),
    ) {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(Arc::new(acyclic_stream::MemoryStream::default()),
                StreamPath::new("failure-identities").unwrap()).await.unwrap();
            let task = Component::new("t").unwrap();
            for (turn, sequence) in [(1, u64::MAX), (0, stored)] {
                let write = Write::new(FileKey::Task { task: task.clone(),
                    file: TaskFile::Error { turn, sequence } }, b"{}".to_vec());
                volume.append(volume.head().await.unwrap(), &write).await.unwrap();
            }
            let next = volume.next_model_error(&task, 0, observed).await;
            if stored == u64::MAX {
                prop_assert!(matches!(next, Err(Error::LimitExceeded)));
            } else {
                let next = next.unwrap();
                prop_assert!(next > stored && next >= observed);
                let next = volume.next_model_error(&task, 0, 0).await.unwrap();
                prop_assert!(next > stored);
                // Losing the retained failure during index reconstruction must
                // fail the same strict-advance oracle.
                volume.index.lock().await.files.remove(&FileKey::Task {
                    task: task.clone(), file: TaskFile::Error { turn: 0, sequence: stored },
                });
                let broken = volume.next_model_error(&task, 0, 0).await.unwrap();
                prop_assert!(broken <= stored);
            }
            Ok(())
        })?;
    }

}

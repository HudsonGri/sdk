use super::*;
use proptest::prelude::*;

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
}

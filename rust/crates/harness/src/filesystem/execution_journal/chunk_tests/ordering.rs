//! Live delta chunks take the task-wide publication order, not UUID order.
use super::*;
use crate::task_volume::{Channel, FileKey, TaskFile};

fn order(files: &[crate::task_volume::FileValue]) -> Vec<(u64, [u8; 16], Vec<u8>)> {
    files
        .iter()
        .map(|file| match &file.key {
            FileKey::Task {
                file:
                    TaskFile::Chunk {
                        sequence,
                        execution,
                        ..
                    },
                ..
            } => (*sequence, execution.into_bytes(), file.bytes.to_vec()),
            key => panic!("unexpected {key:?}"),
        })
        .collect()
}

#[tokio::test]
async fn delta_chunks_follow_publication_order_across_reverse_sorted_executions() {
    let (f, journal, files, model) = fixture().await.unwrap();
    publish_peers(f.stream.as_ref(), &files, &task_of(&journal), 1).await;
    journal
        .append_delta(OP, "event:100", 0, &model, None)
        .await
        .unwrap();
    // `OP` sorts before the peer's UUID, yet its delta follows the peer's.
    assert!(OP.into_bytes() < [255; 16]);
    let reopened = files.inner.reopen().await.unwrap();
    let expected = vec![
        (0, [255; 16], b"peer".to_vec()),
        (1, OP.into_bytes(), b"working".to_vec()),
    ];
    assert_eq!(
        order(&reopened.snapshot(&Query::Root, 8).await.unwrap().files),
        expected
    );
    // The observation names that same chunk, which reads back after reopen.
    let records = journal.replay(OP, 2, 8, None).unwrap().await.unwrap();
    let ExecutionEvent::Model { event, .. } = &records[0].event else {
        unreachable!()
    };
    assert!(
        event.path().ends_with(&format!("/chunks/{OP}/1")),
        "{}",
        event.path()
    );
    assert_eq!(
        journal.load(event, None).unwrap().await.unwrap().as_ref(),
        br#"{"delta":"working","kind":"content"}"#
    );
}

#[tokio::test]
async fn a_peer_publication_after_naming_the_next_chunk_is_a_sequence_conflict() {
    let (f, journal, files, _) = fixture().await.unwrap();
    let task = task_of(&journal);
    let named = files
        .inner
        .next_chunk_key(task.clone(), OP, 0, None)
        .await
        .unwrap();
    publish_peers(f.stream.as_ref(), &files, &task, 1).await;
    let error = files
        .inner
        .prepare_named_chunk(named.clone(), Channel::Text, b"late", None)
        .await;
    assert!(
        matches!(&error, Err(crate::Error::TaskFiles(error)) if matches!(**error, crate::task_volume::Error::SequenceConflict(_))),
        "{:?}",
        error.map(drop)
    );
    // Negative control: an unchecked key would repeat sequence 0 beside the peer.
    assert!(
        files
            .inner
            .prepare_journal_file(named, Channel::Text, b"late", None)
            .await
            .unwrap()
            .is_some()
    );
}

use super::*;

fn future_bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
    std::mem::size_of::<F>()
}

type Journal = FilesystemWorkflowJournal<
    acyclic_stream::LocalStream,
    acyclic_fs::facade::LocalAuthorityBackend,
    acyclic_fs::facade::LocalObjectBackend,
>;

#[test]
fn workflow_commit_frames_stay_bounded() {
    // Type inspection never constructs a journal or performs an operation.
    let borrowed = future_bytes(
        |(journal, key, record): (&Journal, &IdempotencyKey, &WorkflowRecord)| {
            journal.commit_borrowed_with_io(0, key.clone(), record, None)
        },
    );
    let owned = future_bytes(
        |(journal, key, record): (&Journal, IdempotencyKey, WorkflowRecord)| {
            journal.commit_with_io(0, key, record, None)
        },
    );
    eprintln!("workflow commit futures: borrowed={borrowed} owned={owned} bytes");
    assert!(borrowed <= 4096, "borrowed commit future: {borrowed}");
    assert!(owned <= 4096, "owned commit future: {owned}");
}

//! Both contenders execute the production capture, claim and outcome path.
use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use loom::sync::atomic::{AtomicUsize, Ordering};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn schedule<const CLAIM: bool>() {
    let volume = Arc::new(
        loom::future::block_on(Box::pin(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("hook-effects-model").unwrap(),
        )))
        .unwrap(),
    );
    let executions = Arc::new(AtomicUsize::new(0));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let volume = Arc::clone(&volume);
            let executions = Arc::clone(&executions);
            loom::thread::spawn(move || {
                let result = loom::future::block_on(Box::pin(run_once::<_, _, _, _, _, CLAIM>(
                    &volume,
                    (
                        TaskId::from_bytes([1; 16]),
                        OperationId::from_bytes([2; 16]),
                        "session-start",
                    ),
                    4096,
                    true,
                    || std::future::ready(Ok((Vec::new(), None))),
                    || async {
                        executions.fetch_add(1, Ordering::SeqCst);
                        loom::thread::yield_now();
                        Ok(HookOutput::default())
                    },
                )));
                if CLAIM {
                    assert!(result.is_ok() || matches!(result, Err(Error::Indeterminate(_))));
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "hook executed more than once"
    );
}

#[test]
fn competing_dispatches_claim_once_with_negative_control() {
    loom::model(schedule::<true>);
    let failure = std::panic::catch_unwind(|| loom::model(schedule::<false>))
        .expect_err("missing claim escaped the duplicate-dispatch detector");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("hook executed more than once"),
        "unexpected failure: {text}"
    );
}

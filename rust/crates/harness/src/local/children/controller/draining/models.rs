use super::*;
use loom::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

fn effects(broken: bool, assert_property: bool) {
    let signal = acyclic_fs::CancellationToken::new();
    let cancellation = acyclic_fs::CancellationToken::new();
    let drained = acyclic_fs::CancellationToken::new();
    let first = Arc::new(AtomicBool::new(false));
    let second = Arc::new(AtomicBool::new(false));
    let parent_done = Arc::new(AtomicBool::new(false));
    let mut jobs: FuturesUnordered<BoxProviderFuture<'static, Result<()>>> =
        FuturesUnordered::new();
    let wake = signal.clone();
    let saved = first.clone();
    jobs.push(Box::pin(async move {
        wake.cancelled().await;
        saved.store(true, Ordering::Release);
        Ok(())
    }));
    let saved = second.clone();
    jobs.push(Box::pin(async move {
        saved.store(true, Ordering::Release);
        Ok(())
    }));
    let stop = cancellation.clone();
    let fence = drained.clone();
    let finished = parent_done.clone();
    let parent = async move {
        let result =
            crate::tools::cancellable_child_wait(std::future::pending::<Result<()>>(), Some(&stop))
                .await;
        let cancelled = matches!(
            result,
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled
            ))
        );
        fence.cancelled().await;
        finished.store(cancelled, Ordering::Release);
        Ok(())
    };
    let publisher = thread::spawn(move || signal.cancel());
    if broken {
        crate::task_volume::model_checking::set_fault(
            crate::task_volume::model_checking::Fault::DropSiblingEffects,
        );
    }
    loom::future::block_on(Box::pin(finish(parent, &mut jobs, &cancellation, &drained)));
    publisher.join().unwrap();
    if assert_property {
        assert!(
            first.load(Ordering::Acquire)
                && second.load(Ordering::Acquire)
                && parent_done.load(Ordering::Acquire)
                && jobs.is_empty(),
            "controller returned before sibling effect futures drained"
        );
    }
}

#[test]
fn controller_errors_drain_all_siblings_and_release_the_parent_completion_fence() {
    loom::model(|| effects(false, true));
    let failure = std::panic::catch_unwind(|| loom::model(|| effects(true, true)))
        .expect_err("dropped siblings were not detected");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("controller returned before sibling effect futures drained"),
        "unexpected negative control: {message}"
    );
    loom::model(|| effects(true, false));
}

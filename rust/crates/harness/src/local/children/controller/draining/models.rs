use super::*;
use loom::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

fn effects(broken: bool, assert_property: bool, concurrent: bool) {
    let signal = acyclic_fs::CancellationToken::new();
    let cancellation = acyclic_fs::CancellationToken::new();
    let drained = acyclic_fs::CancellationToken::new();
    let first = Arc::new(AtomicBool::new(false));
    let second = (!concurrent).then(|| Arc::new(AtomicBool::new(false)));
    let parent_done = Arc::new(AtomicBool::new(false));
    let mut jobs: FuturesUnordered<BoxProviderFuture<'static, Result<()>>> =
        FuturesUnordered::new();
    let wake = signal.clone();
    let saved = first.clone();
    jobs.push(Box::pin(async move {
        if concurrent {
            wake.cancelled().await;
        }
        saved.store(true, Ordering::Release);
        Ok(())
    }));
    if let Some(saved) = second.clone() {
        jobs.push(Box::pin(async move {
            saved.store(true, Ordering::Release);
            Ok(())
        }));
    }
    let stop = cancellation.clone();
    let fence = drained.clone();
    let finished = parent_done.clone();
    let parent = async move {
        // finish cancels this private token before polling the parent. The
        // passive wait's already-cancelled branch is independent of the effect
        // signal, and is exercised by the serial multiple-sibling model.
        let cancelled = if concurrent {
            stop.is_cancelled()
        } else {
            let result = crate::tools::cancellable_child_wait(
                std::future::pending::<Result<()>>(),
                Some(&stop),
            )
            .await;
            matches!(
                result,
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                ))
            )
        };
        fence.cancelled().await;
        finished.store(cancelled, Ordering::Release);
        Ok(())
    };
    let publisher = concurrent.then(|| thread::spawn(move || signal.cancel()));
    if broken {
        crate::task_volume::model_checking::set_fault(
            crate::task_volume::model_checking::Fault::DropSiblingEffects,
        );
    }
    loom::future::block_on(Box::pin(finish(parent, &mut jobs, &cancellation, &drained)));
    if let Some(publisher) = publisher {
        publisher.join().unwrap();
    }
    if assert_property {
        assert!(
            first.load(Ordering::Acquire)
                && second
                    .as_ref()
                    .is_none_or(|saved| saved.load(Ordering::Acquire))
                && parent_done.load(Ordering::Acquire)
                && jobs.is_empty(),
            "controller returned before sibling effect futures drained"
        );
    }
}

// A ready sibling touches no external state: loop completeness is independent
// of the pending effect's wakeup. Exhaust them separately instead of multiplying
// their schedules; both still run the production finish and cancellation paths.
fn check(concurrent: bool) {
    loom::model(move || effects(false, true, concurrent));
    let failure = std::panic::catch_unwind(|| loom::model(move || effects(true, true, concurrent)))
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
    loom::model(move || effects(true, false, concurrent));
}

#[test]
fn controller_error_waits_for_the_concurrent_effect_and_parent_completion_fence() {
    check(true);
}

#[test]
fn controller_error_drains_every_ready_sibling_before_releasing_the_parent() {
    check(false);
}

use super::*;
use loom::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn complete<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("model operation did not finish"),
    }
}

fn schedule(broken: bool) {
    let state = StateArc::new(AsyncMutex::new(()));
    let cleaned = StateArc::new(AtomicBool::new(false));
    let completed = StateArc::clone(&cleaned);
    let mut worker = None;
    let mut command = Box::pin(serialized(&state, async |guard| {
        let held = StateArc::new(guard);
        let check = if broken {
            // Former lifecycle hazard: the worker retains its process but not
            // its view guard; caller cancellation can switch the checkout.
            Arc::new(|| Ok(())) as Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>
        } else {
            retain_guard(StateArc::clone(&held), || Ok(()))
        };
        worker = Some(thread::spawn(move || {
            thread::yield_now();
            check().unwrap();
            completed.store(true, Ordering::SeqCst);
        }));
        std::future::pending::<()>().await;
        drop(held);
    }));
    assert!(matches!(
        command
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    drop(command);
    let observed = complete(serialized(&state, async |_| cleaned.load(Ordering::SeqCst)));
    worker.unwrap().join().unwrap();
    assert!(observed, "hook cleanup escaped its workspace guard");
}

#[test]
fn cancellation_keeps_hook_workers_inside_the_selected_workspace() {
    loom::model(|| schedule(false));
    let failure = std::panic::catch_unwind(|| loom::model(|| schedule(true)))
        .expect_err("missing guard retention escaped the cleanup detector");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("hook cleanup escaped its workspace guard"),
        "{text}"
    );
}

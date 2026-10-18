use super::*;
use loom::sync::atomic::{AtomicBool, Ordering};

fn handoff(forget_ready: bool) {
    loom::model(move || {
        let host = Host::new();
        lock(&host.state).forget_ready = forget_ready;
        let source = Arc::new(AtomicBool::new(false));
        let published = source.clone();
        let (sender, receiver) = loom::sync::mpsc::channel::<Waker>();
        let worker = host.clone();
        let thread = loom::thread::spawn(move || {
            worker.run(|| {
                let mut sender = Some(sender);
                worker.block(std::future::poll_fn(|cx| {
                    if let Some(sender) = sender.take() {
                        sender.send(cx.waker().clone()).unwrap();
                    }
                    if source.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }));
            })
        });
        let waking = receiver.recv().unwrap();
        published.store(true, Ordering::SeqCst);
        waking.wake();
        executor::complete_io(host.drive(std::future::poll_fn(|_| {
            if lock(&host.state).phase == Phase::Finished {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })));
        thread.join().unwrap();
    });
}

#[test]
fn readiness_racing_worker_parking_cannot_be_lost() {
    handoff(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_forgetting_worker_readiness_deadlocks() {
    handoff(true);
}

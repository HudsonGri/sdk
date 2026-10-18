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

fn queue_wakes(forget_io: bool) {
    loom::model(move || {
        let host = Host::new();
        lock(&host.state).forget_io = forget_io;
        let capacity = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(AtomicBool::new(false));
        let waiting = Arc::new(Mutex::new(None::<Waker>));
        let (started, start) = loom::sync::mpsc::channel();
        let (admitted, admission) = loom::sync::mpsc::channel();
        let worker = host.clone();
        let space = capacity.clone();
        let complete = terminal.clone();
        let wake = waiting.clone();
        let thread = loom::thread::spawn(move || {
            start.recv().unwrap();
            space.store(true, Ordering::SeqCst);
            let signal = lock(&wake).as_ref().unwrap().clone();
            signal.wake();
            admission.recv().unwrap();
            worker.run(|| ());
            complete.store(true, Ordering::SeqCst);
            let signal = lock(&wake).as_ref().unwrap().clone();
            signal.wake();
        });
        let mut started = Some(started);
        let mut admitted = Some(admitted);
        executor::complete_io(host.drive(std::future::poll_fn(|cx| {
            *lock(&waiting) = Some(cx.waker().clone());
            if let Some(started) = started.take() {
                started.send(()).unwrap();
            }
            if capacity.load(Ordering::SeqCst)
                && let Some(admitted) = admitted.take()
            {
                admitted.send(()).unwrap();
            }
            if terminal.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })));
        thread.join().unwrap();
    });
}

#[test]
fn queue_capacity_and_physical_terminal_wakes_are_latched() {
    queue_wakes(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_forgetting_io_readiness_deadlocks() {
    queue_wakes(true);
}

use super::queue::Queue;
use loom::sync::Arc;

fn run(omit_wakeup: bool) {
    loom::model(move || {
        let queue = Arc::new(Queue::new(1));
        let writer = {
            let queue = Arc::clone(&queue);
            loom::thread::spawn(move || {
                if omit_wakeup {
                    queue.push_without_wakeup(1).unwrap();
                } else {
                    queue.push(1).unwrap();
                }
                queue.push(2).unwrap();
                queue.flush().unwrap();
                queue.close(false);
            })
        };
        let mut actual = Vec::new();
        while queue.take(&mut actual, 1).unwrap() {
            queue.committed(1).unwrap();
        }
        writer.join().unwrap();
        assert_eq!(actual, [1, 2]);
    });
}
#[test]
fn bounded_queue_has_no_lost_wakeup_or_record() {
    run(false);
}
#[test]
fn wakeup_control_process() {
    match std::env::var("ACYCLIC_AUDIT_BROKEN_QUEUE").as_deref() {
        Ok("commit") => shutdown(true),
        Ok(_) => run(true),
        Err(_) => run(false),
    }
}

#[test]
fn missing_wakeup_negative_control() {
    negative_control("wakeup");
}

fn negative_control(fault: &str) {
    // Loom may abort while unwinding a detected deadlock with live thread Arcs.
    // Isolate that deliberately broken model; assert its actual failure cause.
    let module = module_path!().split_once("::").unwrap().1;
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("{module}::wakeup_control_process"),
            "--nocapture",
        ])
        .env("ACYCLIC_AUDIT_BROKEN_QUEUE", fault)
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    assert!(!output.status.success(), "broken wakeup model succeeded");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("deadlock"),
        "broken model failed without detecting a deadlock: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn persistence_failure_unblocks_producers_and_barriers() {
    loom::model(|| {
        let queue = Arc::new(Queue::new(1));
        queue.push(1).unwrap();
        let producer = {
            let queue = Arc::clone(&queue);
            loom::thread::spawn(move || {
                let _ = queue.push(2);
                queue.flush()
            })
        };
        queue.close(true);
        assert!(producer.join().unwrap().is_err());
        assert!(queue.push(3).is_err());
    });
}

#[test]
fn a_partial_batch_does_not_wait_for_a_future_request_or_barrier() {
    loom::model(|| {
        let queue = Queue::new(2);
        queue.push(1).unwrap();
        let mut batch = Vec::new();
        assert!(queue.take(&mut batch, 2).unwrap());
        assert_eq!(batch, [1]);
        batch.clear();
        queue.committed(1).unwrap();
        queue.flush().unwrap();
        queue.close(false);
        assert!(!queue.take(&mut batch, 2).unwrap());
    });
}

fn startup(omit_lock: bool) {
    let violated = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = std::sync::Arc::clone(&violated);
    loom::model(move || {
        let cell = Arc::new(super::startup::Startup::<usize>::default());
        let starts = Arc::new(loom::sync::atomic::AtomicUsize::new(0));
        let open = move |cell: Arc<super::startup::Startup<usize>>,
                         starts: Arc<loom::sync::atomic::AtomicUsize>| {
            let create = || {
                starts.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
                Ok(7)
            };
            if omit_lock {
                cell.get_split(create)
            } else {
                cell.get(create)
            }
        };
        let other = loom::thread::spawn({
            let cell = Arc::clone(&cell);
            let starts = Arc::clone(&starts);
            move || open(cell, starts)
        });
        assert_eq!(open(cell, Arc::clone(&starts)).unwrap(), 7);
        assert_eq!(other.join().unwrap().unwrap(), 7);
        if starts.load(loom::sync::atomic::Ordering::SeqCst) != 1 {
            observed.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
    assert!(
        !violated.load(std::sync::atomic::Ordering::Relaxed),
        "duplicate audit worker"
    );
}
#[test]
fn concurrent_views_start_exactly_one_worker() {
    startup(false);
}
#[test]
#[should_panic(expected = "duplicate audit worker")]
fn split_startup_negative_control() {
    startup(true);
}

#[test]
fn poisoning_cannot_strand_a_backpressured_producer() {
    loom::model(|| {
        let queue = Arc::new(Queue::new(1));
        queue.push(1).unwrap();
        let producer = {
            let queue = Arc::clone(&queue);
            loom::thread::spawn(move || queue.push(2))
        };
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| queue.committed(2))).is_err()
        );
        queue.close(true);
        assert!(producer.join().unwrap().is_err());
    });
}

fn shutdown(omit_commit: bool) {
    loom::model(move || {
        let queue = Arc::new(Queue::new(1));
        queue.push(1).unwrap();
        let consumer = loom::thread::spawn({
            let queue = queue.clone();
            move || {
                let mut batch = Vec::new();
                assert!(queue.take(&mut batch, 1).unwrap());
                assert_eq!(batch, [1]);
                if !omit_commit {
                    queue.committed(batch.len()).unwrap();
                }
            }
        });
        queue.close_and_flush().unwrap();
        consumer.join().unwrap();
    });
}

#[test]
fn final_owner_waits_for_admitted_records() {
    shutdown(false);
}

#[test]
fn missing_commit_negative_control() {
    negative_control("commit");
}

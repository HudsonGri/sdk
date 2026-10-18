#[cfg(not(acyclic_harness_loom))]
fn reinitialized_after_one_byte(broken: bool) -> usize {
    use super::{BATCH, Channel, publish};
    use std::task::{Context, Poll, Waker};
    let channel = Channel::new(BATCH);
    let mut storage = channel.storage().unwrap();
    storage.storage_mut().fill(0xa5);
    // Warm the native condition variable before measuring repeated handoffs.
    channel.space.notify_one();
    let mut changed = 0;
    let allocations = acyclic_test_support::measure(|| {
        assert!(publish(&channel, storage.finish(1)));
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Some(Ok(storage))) = channel.poll_recv(&mut cx) else {
            panic!("published input disappeared");
        };
        assert_eq!(storage.bytes(), [0xa5]);
        channel.recycle(storage);
        let mut storage = channel.storage().unwrap();
        if broken {
            // Restore the old resize's writes to the already initialized tail.
            storage.storage_mut()[1..].fill(0);
        }
        assert_eq!(storage.storage_mut().len(), BATCH);
        changed = storage
            .storage_mut()
            .iter()
            .filter(|byte| **byte != 0xa5)
            .count();
    });
    assert_eq!(allocations.count_total, 0);
    changed
}

#[cfg(not(acyclic_harness_loom))]
#[test]
fn recycled_one_byte_reads_initialize_exactly_zero_bytes() {
    assert_eq!(reinitialized_after_one_byte(false), 0);
}

#[cfg(not(acyclic_harness_loom))]
#[test]
fn negative_control_detects_reinitializing_the_recycled_tail() {
    assert_eq!(reinitialized_after_one_byte(true), super::BATCH - 1);
    assert!(
        std::panic::catch_unwind(|| assert_eq!(reinitialized_after_one_byte(true), 0)).is_err()
    );
}

/// Shutdown models over the production channel and pump. The cancel object
/// is modelled as the OS one behaves: level-triggered, so a cancel before
/// the read is admitted still ends it.
#[cfg(acyclic_harness_loom)]
mod model {
    use std::io;

    use loom::sync::{Arc, Condvar, Mutex};

    use super::super::{Channel, Source, pump};

    struct Cancel {
        set: Mutex<bool>,
        changed: Condvar,
        /// False models `CancelSynchronousIo` alone: a cancel issued before
        /// the read starts is lost.
        level: bool,
    }

    impl Cancel {
        fn signal(&self) {
            if self.level {
                *self.set.lock().unwrap() = true;
            }
            self.changed.notify_all();
        }

        fn wait(&self) {
            let mut set = self.set.lock().unwrap();
            while !*set {
                set = self.changed.wait(set).unwrap();
            }
        }
    }

    /// Reads `reads` bytes at once, then waits until cancelled.
    struct Input {
        reads: usize,
        cancel: Arc<Cancel>,
    }

    impl Source for Input {
        fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
            if self.reads == 0 {
                self.cancel.wait();
                return Ok(None);
            }
            self.reads -= 1;
            bytes[0] = 1;
            Ok(Some(1))
        }
    }

    /// Drop (close, then cancel) races the reader through `reads` reads.
    fn shutdown(reads: usize, notify: bool, level: bool) {
        loom::model(move || {
            let channel = Arc::new(Channel::new(1));
            let cancel = Arc::new(Cancel {
                set: Mutex::new(false),
                changed: Condvar::new(),
                level,
            });
            let mut input = Input {
                reads,
                cancel: Arc::clone(&cancel),
            };
            let reader = Arc::clone(&channel);
            let thread = loom::thread::spawn(move || pump(&mut input, &reader));
            if notify {
                channel.close();
            } else {
                channel.close_unnotified();
            }
            cancel.signal();
            thread.join().unwrap();
        });
    }

    #[test]
    fn drop_racing_the_first_read_admission_always_ends_the_reader() {
        shutdown(0, true, true);
    }

    #[test]
    fn drop_racing_a_full_queue_always_ends_the_reader() {
        // Two batches fill the queue; the third waits for space.
        shutdown(3, true, true);
    }

    fn wake_model(broken: bool) {
        loom::model(move || {
            let channel = Arc::new(Channel::new(1));
            let mut input = Input {
                reads: 1,
                cancel: Arc::new(Cancel {
                    set: Mutex::new(true),
                    changed: Condvar::new(),
                    level: true,
                }),
            };
            let reader = Arc::clone(&channel);
            let producer = loom::thread::spawn(move || pump(&mut input, &reader));
            let bytes = loom::future::block_on(Box::pin(async {
                let mut bytes = Vec::new();
                while let Some(batch) = std::future::poll_fn(|cx| {
                    if broken {
                        channel.poll_recv_split(cx)
                    } else {
                        channel.poll_recv(cx)
                    }
                })
                .await
                {
                    bytes.extend_from_slice(batch.unwrap().bytes());
                }
                bytes
            }));
            assert_eq!(bytes, [1]);
            producer.join().unwrap();
        });
    }

    #[test]
    fn publication_and_finish_never_lose_the_consumers_wakeup() {
        wake_model(false);
    }

    #[test]
    fn shutdown_child() {
        match std::env::var("ACYCLIC_READER_SHUTDOWN_CHILD").as_deref() {
            Ok("unnotified") => shutdown(3, false, true),
            Ok("edge") => shutdown(0, true, false),
            Ok("waker") => wake_model(true),
            _ => {}
        }
    }

    /// A Loom deadlock can abort the process, so each control runs in a
    /// child of this test binary.
    fn child_deadlocks(mode: &str) -> io::Result<bool> {
        // libtest names omit the crate prefix, unlike module_path!().
        let module = module_path!()
            .split_once("::")
            .map_or(module_path!(), |(_, module)| module);
        let child = format!("{module}::shutdown_child");
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", &child, "--test-threads=1"])
            .env("ACYCLIC_READER_SHUTDOWN_CHILD", mode)
            .output()?;
        // The test harness reports a panic on stdout; an abort, on stderr.
        let report = [output.stdout, output.stderr].concat();
        Ok(!output.status.success() && String::from_utf8_lossy(&report).contains("deadlock"))
    }

    #[test]
    fn negative_control_detects_a_close_that_never_releases_a_full_queue() -> io::Result<()> {
        assert!(child_deadlocks("unnotified")?);
        Ok(())
    }

    #[test]
    fn negative_control_detects_split_waker_registration() -> io::Result<()> {
        assert!(child_deadlocks("waker")?);
        Ok(())
    }

    #[test]
    fn negative_control_detects_a_cancel_lost_before_read_admission() -> io::Result<()> {
        assert!(child_deadlocks("edge")?);
        Ok(())
    }
}

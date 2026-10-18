//! The controller outlives the killed turn and all of its accepted operations.
use super::*;
use acyclic_native_runtime::testing::{Boundary, DurableFault, InjectedStorageFault, Schedule};
use std::time::Duration;
use tokio::sync::mpsc;

pub(super) enum Outcome<T> {
    Completed(T),
    Crashed(InjectedStorageFault),
}

pub(super) struct Driver {
    pub schedule: Schedule,
    events: mpsc::UnboundedReceiver<Boundary>,
    pub boundaries: Vec<(String, String)>,
}

impl Driver {
    pub fn new(seed: u64, cut: Option<(usize, DurableFault)>) -> Self {
        Self::from_scope(Schedule::seeded(seed), cut)
    }

    pub fn without_entropy(seed: u64) -> Self {
        Self::from_scope(Schedule::without_entropy(seed), None)
    }

    fn from_scope(
        (schedule, events): (Schedule, mpsc::UnboundedReceiver<Boundary>),
        cut: Option<(usize, DurableFault)>,
    ) -> Self {
        if let Some((ordinal, fault)) = cut {
            schedule.cut_at(ordinal, fault);
        }
        Self {
            schedule,
            events,
            boundaries: Vec::new(),
        }
    }

    pub fn run<'a, T: 'a>(&'a mut self, future: EntryFuture<'a, T>) -> EntryFuture<'a, Outcome<T>> {
        entry(async move {
            let schedule = self.schedule.clone();
            let mut turn = entry(schedule.scope(future));
            // This deadline is an oracle: exceeding it fails the simulation.
            // It does not provide the production operation's missing timeout.
            let deadline = tokio::time::sleep(Duration::from_secs(30));
            tokio::pin!(deadline);
            let outcome = loop {
                tokio::select! {
                    biased;
                    event = self.events.recv() => {
                        let event = event.expect("lost boundary driver");
                        self.boundaries.push((event.name.into(), event.detail.to_string()));
                        if let Some(cause) = schedule.storage_fault() {
                            schedule.cancel();
                            // Drop all caller state before any accepted completion
                            // receives its acknowledgement permit.
                            drop(turn);
                            event.release();
                            break Outcome::Crashed(cause);
                        }
                        event.release();
                    },
                    result = &mut turn => {
                        let cause = schedule.storage_fault();
                        if cause.is_some() {
                            schedule.cancel();
                        }
                        drop(turn);
                        break match cause {
                            Some(cause) => {
                                drop(result);
                                Outcome::Crashed(cause)
                            },
                            None => Outcome::Completed(result),
                        };
                    },
                    () = &mut deadline => panic!(
                        "turn exceeded the simulated deadline; last boundary: {:?}",
                        self.boundaries.last()
                    ),
                }
            };
            loop {
                tokio::select! {
                    biased;
                    event = self.events.recv() => {
                        let event = event.expect("lost drain driver");
                        self.boundaries.push((event.name.into(), event.detail.to_string()));
                        event.release();
                    },
                    () = schedule.drained() => break,
                    () = &mut deadline => panic!("accepted work exceeded the simulated deadline"),
                }
            }
            outcome
        })
    }
}

#[test]
fn every_file_cut_drops_the_turn_before_independent_reopen() -> Result<()> {
    use std::io::Read;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let directory = tempfile::tempdir().map_err(storage)?;
        let path = directory.path().join("journal");
        let write = |path: std::path::PathBuf| -> EntryFuture<'static, std::io::Result<()>> {
            entry(async move {
                let file = std::fs::File::create(path)?;
                // Real original worker, held file, and physical write calls.
                acyclic_native_runtime::testing::spawn_blocking(move || {
                    let mut file = file;
                    acyclic_native_runtime::write_file_all(&mut file, b"first")?;
                    acyclic_native_runtime::write_file_all(&mut file, b"second")
                })
                .await?
            })
        };
        let mut baseline = Driver::new(71, None);
        match baseline.run(write(path.clone())).await {
            Outcome::Completed(result) => result.map_err(storage)?,
            Outcome::Crashed(cause) => panic!("uncut run crashed: {cause}"),
        }
        let writes = baseline.schedule.durable_writes();
        assert_eq!(writes.len(), 2, "original writes escaped enumeration");
        for cut in 0..writes.len() {
            for fault in [
                DurableFault::Before,
                DurableFault::After,
                DurableFault::Torn(2),
                DurableFault::Failed,
                DurableFault::DiskFull,
            ] {
                let mut driver = Driver::new(71, Some((cut, fault)));
                let Outcome::Crashed(cause) = driver.run(write(path.clone())).await else {
                    panic!("cut {cut} {fault:?} did not kill the original turn");
                };
                assert_eq!((cause.ordinal, cause.fault), (cut, fault));
                assert_eq!(driver.schedule.durable_writes().len(), cut + 1);
                drop(driver);
                let mut restored = Vec::new();
                std::fs::File::open(&path)
                    .map_err(storage)?
                    .read_to_end(&mut restored)
                    .map_err(storage)?;
                let prefix = if cut == 0 { b"".as_slice() } else { b"first" };
                let offered = if cut == 0 {
                    b"first".as_slice()
                } else {
                    b"second"
                };
                let retained = match fault {
                    DurableFault::After => offered.len(),
                    DurableFault::Torn(count) => count,
                    _ => 0,
                };
                assert_eq!(restored, [prefix, &offered[..retained]].concat());
            }
        }
        Ok(())
    })
}

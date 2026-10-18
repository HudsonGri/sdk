//! Model the production select/settlement boundary. Process completion and
//! provider follows are atomic contracts; their internal locks are not modeled.
use super::*;
use shuttle::{
    future,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    cancelled: bool,
    reaped: bool,
    settled: bool,
}

#[test]
fn cancellation_and_follow_failure_both_await_physical_cleanup() {
    for failed_follow in [false, true] {
        model(false, failed_follow);
    }
}

#[test]
#[should_panic(expected = "settled while the native process was still active")]
fn negative_control_detects_dropping_execution_before_cleanup() {
    model(true, false);
}

fn model(broken: bool, failed_follow: bool) {
    shuttle::check_dfs(
        move || {
            future::block_on(async {
                // Model lock order: this is the only effect-state lock. Every
                // guard retires before yielding, awaiting, or polling a future.
                let state = Arc::new(Mutex::new(State::default()));
                let process_state = state.clone();
                let mut process = future::spawn(async move {
                    future::yield_now().await;
                    {
                        let state = process_state.lock().unwrap();
                        if !state.cancelled {
                            assert!(!state.settled, "native effect after settlement");
                        }
                    }
                    future::yield_now().await;
                    process_state.lock().unwrap().reaped = true;
                });
                // The physical job remains independently owned when this async
                // waiter is dropped, as with run_blocking_io in the checkout path.
                let execution = async {
                    (&mut process).await.unwrap();
                    Ok(())
                };
                let cancelled = async {
                    future::yield_now().await;
                    if failed_follow {
                        Err(crate::Error::Storage("follow ended".into()))
                    } else {
                        Ok(())
                    }
                };
                let stop = || state.lock().unwrap().cancelled = true;
                let result = if broken {
                    tokio::pin!(execution, cancelled);
                    // Deliberately broken variant: cancellation abandons the
                    // waiter and returns without joining the physical job.
                    tokio::select! {
                        biased;
                        _ = &mut cancelled => {stop(); Ok(((), Some(())))}
                        result = &mut execution => result.map(|value| (value, None)),
                    }
                } else {
                    settle(execution, cancelled, stop, || Err(crate::Error::Cancelled)).await
                };
                {
                    let mut state = state.lock().unwrap();
                    state.settled = true;
                    assert!(
                        state.reaped,
                        "settled while the native process was still active"
                    );
                    if state.cancelled {
                        assert_eq!(result.is_err(), failed_follow);
                    }
                }
            });
        },
        None,
    );
}

#[test]
fn preparation_cancellation_reconciles_admission_before_result_and_acknowledgement() {
    preparation_model(0, false);
    preparation_model(0, true);
}

#[test]
#[should_panic(expected = "cancelled preparation left the invocation unresolved")]
fn negative_control_detects_escaping_preparation_cancellation() {
    preparation_model(1, false);
}

#[test]
#[should_panic(expected = "settled while admission remained live")]
fn negative_control_detects_skipping_partial_admission_reconciliation() {
    preparation_model(2, false);
}

#[test]
#[should_panic(expected = "cancelled preparation left the invocation unresolved")]
fn negative_control_detects_stranding_the_preparation_workers_lease() {
    preparation_model(3, false);
}

#[derive(Default)]
struct PreparationState {
    admitted: bool,
    lease_live: bool,
    reconciled: bool,
    returned: bool,
}

fn preparation_model(broken: u8, failed_reconciliation: bool) {
    // Atomic witness counts explored executions, not synchronization of the
    // modeled path. The real budget constructor must reject in some schedule.
    let reached = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    let witness = reached.clone();
    shuttle::check_dfs(
        move || {
            let witness = witness.clone();
            future::block_on(async {
                let token = acyclic_fs::CancellationToken::new();
                // Only model lock; guards retire before every await.
                let state = Arc::new(Mutex::new(PreparationState::default()));
                let admission = async {
                    if token.is_cancelled() {
                        return Ok(false);
                    }
                    // The dispatch check has passed. Cancellation can now
                    // arrive before budget setup or after partial admission.
                    future::yield_now().await;
                    let io = crate::filesystem::PreparationIo::from_budget(
                        acyclic_fs::performance::work_budget(8, 128),
                        &token,
                    );
                    if matches!(io, Err(crate::Error::Cancelled)) {
                        witness.fetch_or(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    let io = io?;
                    state.lock().unwrap().admitted = true;
                    future::yield_now().await;
                    let checked = io.check_cancellation();
                    if matches!(checked, Err(crate::Error::Cancelled)) {
                        witness.fetch_or(2, std::sync::atomic::Ordering::Relaxed);
                    }
                    checked?;
                    state.lock().unwrap().lease_live = true;
                    future::yield_now().await;
                    let checked = io.check_cancellation();
                    if matches!(checked, Err(crate::Error::Cancelled)) {
                        witness.fetch_or(4, std::sync::atomic::Ordering::Relaxed);
                    }
                    let checked = if broken == 3 {
                        checked.map(|()| true)
                    } else {
                        // The actual preparation-command boundary returns a
                        // result to its worker, which retires its original lease.
                        cancellation_outcome(checked.map(|()| true), || Ok(false))
                    }?;
                    state.lock().unwrap().lease_live = false;
                    if !checked {
                        return Err(crate::Error::Cancelled);
                    }
                    Ok(true)
                };
                let reconcile = async {
                    future::yield_now().await;
                    let mut state = state.lock().unwrap();
                    if failed_reconciliation {
                        return Err(crate::Error::Storage("uncertain admission".into()));
                    }
                    if state.lease_live {
                        return Err(crate::Error::Indeterminate(crate::OperationId::from_bytes(
                            [1; 16],
                        )));
                    }
                    assert!(!state.returned, "admission reconciled after settlement");
                    state.admitted = false;
                    state.reconciled = true;
                    Ok(())
                };
                let execution = async {
                    if broken == 2 {
                        admission.await
                    } else {
                        reconcile_cancelled(admission, reconcile).await
                    }
                };
                let publication = future::spawn(async {
                    future::yield_now().await;
                    Ok(())
                });
                let cancelled =
                    async { publication.await.expect("cancellation publication joined") };
                let result = settle(
                    execution,
                    cancelled,
                    || token.cancel(),
                    || {
                        if broken == 1 {
                            // Previous production behavior propagated Cancelled
                            // before either publication could be reached.
                            Err(crate::Error::Cancelled)
                        } else {
                            Ok(false)
                        }
                    },
                )
                .await;
                let mut state = state.lock().unwrap();
                state.returned = true;
                match result {
                    Ok((ran, acknowledgement)) => {
                        if !ran && acknowledgement.is_some() {
                            assert!(!state.admitted, "settled while admission remained live");
                        }
                        if state.reconciled {
                            assert!(!ran && acknowledgement.is_some() && !state.admitted);
                        }
                    }
                    Err(crate::Error::Storage(message)) if failed_reconciliation => {
                        assert_eq!(message, "uncertain admission");
                    }
                    Err(error) => {
                        panic!("cancelled preparation left the invocation unresolved: {error}")
                    }
                }
            });
        },
        None,
    );
    assert_eq!(
        reached.load(std::sync::atomic::Ordering::Relaxed),
        7,
        "model must reach cancellation before budget setup, after admission, and with a worker lease"
    );
}

use super::*;

const MAX_FUTURE_BYTES: usize = 4096;

fn assert_bounded<F: std::future::Future>(name: &str, future: F) {
    let bytes = std::mem::size_of_val(&future);
    eprintln!("{name} future: {bytes} bytes");
    assert!(bytes <= MAX_FUTURE_BYTES, "{name} future is {bytes} bytes");
}

#[tokio::test]
async fn execution_and_worker_futures_stay_bounded() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease, &mut None).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    let turn = OperationId::from_bytes([106; 16]);
    let model = Model::new("synthetic".into(), "size".into(), "1".into(), Value::Null)?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    let execution = runtime
        .stock_execution(
            task,
            fence.clone(),
            turn,
            model.clone(),
            provider.clone(),
            ContextPipeline::default(),
            fixture.tools.clone(),
            None,
        )
        .await?;
    let input = TurnInput {
        operation_id: execution.operation_id(),
        input: crate::model::ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: 1,
    };
    assert_bounded(
        "deadline",
        deadline::run(
            runtime.task_host(),
            task,
            Some(&fence),
            Some(20),
            input.operation_id,
            || async { Ok(()) },
            None,
            |_| false,
        ),
    );
    assert_bounded("execution", execution.execute(input.clone()));
    assert_bounded(
        "execution progress",
        execution.execute_progress(input.clone()),
    );
    assert_bounded("model step", execution.model_step(&input, 0, &[]));
    let commands = runtime.commands();
    assert_bounded("run task", runtime.run_task(lease.clone(), &commands, 2));
    let worker_context =
        WorkerContext::Ordinary(Arc::new(runtime.task_context(task, fence.clone()).await?));
    assert_bounded(
        "worker command dispatch",
        runtime.dispatch_commands(task, &worker_context, &fence, &[], &commands, None),
    );
    assert_bounded(
        "run task observed",
        runtime.run_task_observed(&lease, &commands, 2, None),
    );
    assert_bounded(
        "resume task",
        runtime.resume_task(lease.clone(), &commands, 2, None),
    );
    assert_bounded(
        "execution reconciliation",
        execution.reconcile(input.clone(), 1),
    );
    let execution = runtime
        .stock_execution(
            task,
            fence,
            turn,
            model,
            provider,
            ContextPipeline::default(),
            fixture.tools.clone(),
            None,
        )
        .await?;
    assert_bounded(
        "execution reconciliation progress",
        execution.reconcile_progress(input),
    );
    Ok(())
}

#[tokio::test]
async fn reconciliation_has_only_its_progress_allocation() -> Result<()> {
    use futures::FutureExt as _;

    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease, &mut None).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let model = Model::new("synthetic".into(), "size".into(), "1".into(), Value::Null)?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    for (valid, allowance) in [(true, 1_000), (false, 1_000), (true, 0)] {
        let execution = runtime
            .stock_execution(
                task,
                LeaseFence::from(&lease.reservation),
                OperationId::from_bytes([106; 16]),
                model.clone(),
                provider.clone(),
                ContextPipeline::default(),
                fixture.tools.clone(),
                None,
            )
            .await?;
        let input = TurnInput {
            operation_id: if valid {
                execution.operation_id()
            } else {
                OperationId::from_bytes([107; 16])
            },
            input: crate::model::ModelContent::Text("question".into()),
            selected_context: None,
            max_steps: 1,
        };
        let allocations = acyclic_test_support::measure(|| {
            let future = execution.reconcile(input, allowance);
            if valid && allowance != 0 {
                // Valid construction owns exactly one funded progress future.
                drop(future);
            } else {
                // Poll through observation into input validation, before store I/O.
                // Nested observation boxes would be allocated during this poll.
                assert!(matches!(future.now_or_never(), Some(Err(_))));
            }
        });
        // Rejected inputs also allocate their owned diagnostic. A zero
        // allowance allocates that diagnostic without a progress frame.
        let expected = u64::from(allowance != 0) + u64::from(!valid || allowance == 0);
        assert_eq!(allocations.count_total, expected);
    }
    Ok(())
}

// Exact former body: settlement's real future is inline outside the funded
// boundary. Keep this independent from the candidate's state-machine layout.
#[cfg(not(target_arch = "wasm32"))]
async fn inline_cancellation_resume(
    runtime: &Runtime,
    lease: crate::distributed::WorkLease,
    commands: &dyn TaskCommandHost,
    maximum_transitions: u32,
    io: Option<&PreparationIo>,
) -> TaskWorkerAttempt {
    let result = match crate::conversation::BoundedContentFuture::new(io, || {
        runtime.run_task_observed(&lease, commands, maximum_transitions, io)
    }) {
        Ok(future) => future.await,
        Err(error) => Err(error),
    };
    // This rejection is returned only after a model stream has been dropped
    // or before a tool dispatch. Indeterminate native work retains its lease.
    if matches!(
        &result,
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled
        ))
    ) {
        let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        let fence = LeaseFence::from(&lease.reservation);
        let settled = async {
            runtime.host.cancel(task).await?;
            runtime
                .host
                .settle_task(
                    task,
                    &fence,
                    Outcome::Cancelled,
                    &mut observed::history(io)?
                        .as_mut()
                        .map(|work| work as &mut dyn crate::store::HistoryIo),
                )
                .await
        }
        .await;
        return match settled {
            Ok(()) => TaskWorkerAttempt::Progress(WorkerTurn::Completed { task }.with_lease(lease)),
            Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
        };
    }
    match result {
        Ok(progress) => TaskWorkerAttempt::Progress(progress.with_lease(lease)),
        Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn resume_layout_and_construction_allocations_reject_the_original_inline_path() -> Result<()>
{
    use proptest::prelude::*;
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    let commands = runtime.commands();
    let current = std::mem::size_of_val(&runtime.resume_task(lease.clone(), &commands, 2, None));
    let original = std::mem::size_of_val(&inline_cancellation_resume(
        &runtime,
        lease.clone(),
        &commands,
        2,
        None,
    ));
    eprintln!("resume frame current={current} original={original}");
    let mut runner =
        proptest::test_runner::TestRunner::new(proptest::test_runner::Config::with_cases(16));
    runner
        .run(&(1usize..9), |n| {
            for count in [n, 2 * n] {
                for broken in [false, true] {
                    // Prepare owned leases outside the measured construction. Drop
                    // is included, so a new hidden allocation anywhere still fails.
                    let leases = (0..count).map(|_| lease.clone()).collect::<Vec<_>>();
                    let mut frame_bytes = 0;
                    let allocations = acyclic_test_support::measure(|| {
                        for lease in leases {
                            if broken {
                                let future =
                                    inline_cancellation_resume(&runtime, lease, &commands, 2, None);
                                frame_bytes += std::mem::size_of_val(&future);
                                drop(future);
                            } else {
                                let future = runtime.resume_task(lease, &commands, 2, None);
                                frame_bytes += std::mem::size_of_val(&future);
                                drop(future);
                            }
                        }
                    });
                    prop_assert_eq!(allocations.count_total, 0);
                    prop_assert_eq!(frame_bytes, count * if broken { original } else { current });
                    prop_assert_eq!(frame_bytes <= count * MAX_FUTURE_BYTES, !broken);
                }
            }
            Ok(())
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn cancellation_and_invalid_input_keep_the_original_worker_outcomes() -> Result<()> {
    struct CancelCommand;
    impl TaskCommandHost for CancelCommand {
        fn execute<'a>(
            &'a self,
            _: crate::runtime::TaskContext,
            _: LeaseFence,
            _: WorkflowCommand,
            _: Value,
        ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
            Box::pin(async {
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled,
                ))
            })
        }
    }
    for maximum in [0, 2] {
        for original in [false, true] {
            // Each path initializes its own real filesystem, journal, scheduler
            // and command state; neither path consumes the other's output.
            let fixture = Fixture::new().await?;
            let runtime = fixture.runtime(None).await?;
            let lease = fixture.admit(&runtime).await?;
            let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
            let attempt = if original {
                inline_cancellation_resume(&runtime, lease.clone(), &CancelCommand, maximum, None)
                    .await
            } else {
                runtime
                    .resume_task(lease.clone(), &CancelCommand, maximum, None)
                    .await
            };
            if maximum == 0 {
                match attempt {
                    TaskWorkerAttempt::Unresolved {
                        lease: retained,
                        error: Error::Invalid(message),
                    } => {
                        assert_eq!(retained, lease);
                        assert_eq!(message, "invalid worker transition allowance");
                    }
                    _ => return Err(Error::Invalid("wrong invalid-input outcome".into())),
                }
            } else {
                assert!(matches!(attempt, TaskWorkerAttempt::Progress(
                    TaskWorkerOutcome::Completed { task: completed }
                ) if completed == task));
            }
            let state = fixture.scheduler_state(task).await?;
            assert_eq!(state.cancellation_requested, maximum != 0);
            assert_eq!(state.outcome, (maximum != 0).then_some(Outcome::Cancelled));
            assert_eq!(
                state.reservation,
                (maximum == 0).then_some(lease.reservation)
            );
            assert_eq!(
                fixture.machine.transitioned.load(Ordering::SeqCst),
                usize::from(maximum != 0)
            );
            assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}

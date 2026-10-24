use super::*;
use crate::durable_host::tests::created_fixture::CreationFixture;

const BUDGET: acyclic_fs::WorkBudget = acyclic_fs::WorkBudget {
    // One ledger covers owner/proof replay, staging, and both authenticated reads.
    items_examined: 1_048_576,
    // Admission decoders and content-operation futures allocate on that same ledger.
    allocation_operations: 131_072,
    ..acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024)
};

fn work(cancellation: &acyclic_fs::CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(BUDGET, cancellation).map_err(|failure| Error::NativeWork {
        cause: failure.0,
        custody: None,
    })
}

#[tokio::test]
async fn admission_observation_retains_only_the_original_provider_custody() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let baseline = original.live_allocation_bytes();
    let io = crate::filesystem::PreparationIo::from_native_work(&original)?;
    let admission = crate::runtime::observe_task_admission(
        fixture.runtime.task_host().as_ref(),
        fixture.root,
        Some(&io),
    )
    .await?;
    assert_eq!(admission.task_id(), fixture.root);
    assert!(original.live_allocation_bytes() > baseline);

    let foreign = work(&acyclic_fs::CancellationToken::new())?;
    let foreign_baseline = foreign.live_allocation_bytes();
    let foreign_custody = foreign.claim(1, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let rejected = super::super::task_workflow::retain_task_value(
        (admission, Some(foreign_custody)),
        Some(&io),
    );
    assert!(matches!(
        rejected,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ..
        })
    ));
    assert_eq!(foreign.live_allocation_bytes(), foreign_baseline + 1);
    drop(rejected);
    assert_eq!(foreign.live_allocation_bytes(), foreign_baseline);
    drop(io);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

async fn root_context(
    fixture: &CreationFixture,
    work: &NativeMaterializationWork,
) -> Result<Arc<TaskContext>> {
    fixture
        .runtime
        .task_context_with_native_work(
            fixture.root,
            &crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation),
            work,
        )
        .await
}

fn exhaust_memory(
    original: &NativeMaterializationWork,
) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let available = BUDGET.peak_allocation_bytes - original.live_allocation_bytes();
    let quota = original.claim(available, u64::from(available != 0), Default::default())?;
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(
            usize::try_from(available).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok((buffer, quota))
}

#[tokio::test]
async fn host_preparation_borrows_the_original_allowance_and_cancellation() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let context = root_context(&fixture, &original).await?;
    let io = context.preparation_io()?.expect("issued native context");
    let mut accounting = io.accounting(io.cancellation());
    assert!(
        accounting
            .native_work()
            .unwrap()
            .shares_allocation_context(&original)
    );
    let used = original.receipt().items_examined;
    original.charge(acyclic_fs::WorkCounters {
        items_examined: BUDGET.items_examined - used,
        ..Default::default()
    })?;
    assert!(
        accounting
            .charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })
            .is_err()
    );
    // The old host-created ledger admits that same item independently.
    let independent_cancellation = acyclic_fs::CancellationToken::new();
    let independent = work(&independent_cancellation)?;
    independent.charge(acyclic_fs::WorkCounters {
        items_examined: 1,
        ..Default::default()
    })?;
    cancellation.cancel();
    assert!(accounting.charge(Default::default()).is_err());
    independent_cancellation.check()?;
    Ok(())
}

#[tokio::test]
async fn shared_native_content_uses_original_owner_and_keeps_real_output_custody() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let task = crate::TaskId::from_bytes([84; 16]);
    assert!(
        matches!(fixture.admit_child(created.request.child.clone()).await?,
        crate::Admission::Accepted(admitted) if admitted == task)
    );
    let observation = fixture.observation(task).await?;
    let reservation = observation
        .reservation
        .ok_or_else(|| Error::NotFound("created native context lease".into()))?;
    let fence = crate::scheduler::LeaseFence::from(&reservation);
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&original)?;
    let shared = fixture
        .runtime
        .task_context_with_native_work(task, &fence, &original)
        .await?;
    let content_io = crate::executor::PreparationIo::from_native_work(&original)?;
    let owner = shared.journal_owner::<acyclic_stream::MemoryStream>(&fence, &original)?;
    let reader = crate::runtime::content_residency_verifier(
        Arc::clone(&shared),
        Arc::clone(&owner),
        Some(&io),
    )?;
    let staged = {
        let publication_io = crate::executor::PreparationIo::from_native_work(&original)?;
        shared
            .as_ref()
            .stage_file_once(
                crate::OperationId::from_bytes([118; 16]),
                "native/shared.txt",
                b"same issued context",
                "text/plain",
                "shared.txt",
                Some(&publication_io),
            )
            .await
            .and_then(|(file, retained)| Ok((file, publication_io.take_native_custody(retained)?)))
    }?;
    let before = original.live_allocation_bytes();
    let bytes = reader
        .read(&staged.0, 65_536, None, Some(&content_io))?
        .await?;
    assert_eq!(bytes.as_ref(), b"same issued context");
    assert!(bytes.matches_meter(&original.runtime_meter()));
    let held = original.live_allocation_bytes() - before;
    assert!(held > 0);
    let shared_bytes = bytes.clone();
    drop(bytes);
    assert_eq!(shared_bytes.as_ref(), b"same issued context");
    assert!(shared_bytes.matches_meter(&original.runtime_meter()));
    assert_eq!(original.live_allocation_bytes(), before + held);
    drop(shared_bytes);
    assert_eq!(original.live_allocation_bytes(), before);
    reader.verify(&staged.0, 65_536, Some(&content_io))?.await?;

    let root_fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
    let root = root_context(&fixture, &original).await?;
    let root_owner = root.journal_owner::<acyclic_stream::MemoryStream>(&root_fence, &original)?;
    let wrong_owner = crate::runtime::content_residency_verifier(
        Arc::clone(&shared),
        Arc::clone(&root_owner),
        Some(&io),
    );
    assert!(matches!(
        wrong_owner,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    let unmeasured = Arc::new(
        fixture
            .host
            .journal_owner(task, &(fence.clone()), None)
            .await?,
    );
    let rejected =
        crate::runtime::content_residency_verifier(Arc::clone(&shared), unmeasured, Some(&io));
    assert!(matches!(
        rejected,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    let root_reader =
        crate::runtime::content_residency_verifier(Arc::clone(&root), root_owner, Some(&io))?;
    assert!(matches!(
        root_reader
            .read(&staged.0, 65_536, None, Some(&content_io))?
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    let blocker = exhaust_memory(&original)?;
    let denied = reader.read(&staged.0, 65_536, None, Some(&content_io));
    assert!(matches!(
        denied,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        })
    ));
    drop(blocker);
    cancellation.cancel();
    assert!(matches!(
        reader.read(&staged.0, 65_536, None, Some(&content_io)),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn native_tool_context_borrows_the_issued_context_and_invalidates_clone_funding() -> Result<()>
{
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&original)?;
    let shared = root_context(&fixture, &original).await?;
    assert!(shared.native_work()?.shares_allocation_context(&original));
    shared.as_ref().require_original_native_work(&original)?;
    let operation = crate::OperationId::from_bytes([119; 16]);
    let call_id = operation.to_string();
    let baseline = original.live_allocation_bytes();
    let tool = ToolContext::new(Arc::clone(&shared), operation, &call_id, Some(&io))?;

    assert!(original.live_allocation_bytes() > baseline);

    let clone = tool.clone();

    drop(tool);
    assert_eq!(original.live_allocation_bytes(), baseline);
    drop(clone);

    let foreign = work(&cancellation)?;
    assert!(matches!(
        ToolContext::new(
            Arc::clone(&shared),
            operation,
            &call_id,
            Some(&crate::filesystem::PreparationIo::from_native_work(
                &foreign
            )?)
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        ToolContext::new(Arc::clone(&shared), operation, "bad/call", Some(&io)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));

    let blocker = exhaust_memory(&original)?;

    assert!(matches!(
        ToolContext::new(Arc::clone(&shared), operation, &call_id, Some(&io)),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        })
    ));

    drop(blocker);
    let original_io = crate::filesystem::PreparationIo::from_native_work(&original)?;
    let retained = original.live_allocation_bytes();
    cancellation.cancel();
    assert!(matches!(
        shared.require_original_native_work(&original),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(original.live_allocation_bytes(), retained);
    assert!(matches!(
        ToolContext::new(shared, operation, &call_id, Some(&original_io)),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn native_tool_scope_restricts_without_replacing_the_issued_context_or_lineage() -> Result<()>
{
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let task = crate::TaskId::from_bytes([84; 16]);
    assert!(
        matches!(fixture.admit_child(created.request.child.clone()).await?,
        crate::Admission::Accepted(admitted) if admitted == task)
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&original)?;
    let shared = fixture
        .harness
        .durable_context(task, created.request.child.operation_id, Some(&io))
        .await?;
    let read = created
        .private
        .capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let write = created
        .private
        .capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    assert!(shared.as_ref().scope().grants().contains(&write));
    let limits = shared.as_ref().scope().limits();
    let requested = RuntimeScope::new(crate::Capabilities::new([read.as_str()]), limits)?
        .with_run_limits(shared.as_ref().scope().run_limits(), &mut None)?;
    let operation = crate::OperationId::from_bytes([121; 16]);
    let tool = ToolContext::new(Arc::clone(&shared), operation, "scoped-control", Some(&io))?
        .with_native_scope(&requested, requested.limits(), &original)?;

    assert!(tool.effective_scope().grants().contains(&read));
    assert!(!tool.effective_scope().grants().contains(&write));
    assert!(shared.as_ref().scope().grants().contains(&write));
    assert_eq!(
        tool.effective_scope().run_limits(),
        shared.as_ref().scope().run_limits()
    );

    tool.validate_stock_scope_with_native_work(&requested, limits, &original)?;
    assert!(matches!(
        tool.validate_stock_scope_with_native_work(shared.as_ref().scope(), limits, &original,),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    let mut execution_limits = limits;
    execution_limits.file_bytes = 1;
    assert!(limits.file_bytes > execution_limits.file_bytes);
    let stock = ToolContext::new(
        Arc::clone(&shared),
        operation,
        "stock-execution-limits",
        Some(&io),
    )?
    .with_native_scope(&requested, execution_limits, &original)?;

    assert_eq!(stock.effective_scope().limits(), execution_limits);
    assert_eq!(requested.limits(), limits);
    assert_eq!(stock.effective_scope().grants(), requested.grants());
    assert_eq!(stock.effective_scope().run_limits(), requested.run_limits());
    assert!(matches!(
        stock.validate_stock_scope_with_native_work(&requested, limits, &original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    drop(stock);

    #[allow(
        clippy::redundant_clone,
        reason = "Cloning clears native authority; moving would bypass this regression."
    )]
    let clone = tool.clone();
    assert!(matches!(
        clone.validate_stock_scope_with_native_work(&requested, limits, &original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    drop(clone);

    let widening = RuntimeScope::new(
        crate::Capabilities::new([read.as_str(), write.as_str()]),
        limits,
    )?
    .with_run_limits(shared.as_ref().scope().run_limits(), &mut None)?;
    let candidate = ToolContext::new(Arc::clone(&shared), operation, "scope-widen", Some(&io))?
        .with_native_scope(&requested, requested.limits(), &original)?;
    assert!(matches!(
        candidate.with_native_scope(&widening, widening.limits(), &original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    let mut invalid = limits;
    invalid.file_bytes += 1;
    assert!(matches!(
        shared
            .as_ref()
            .validate_stock_scope_with_native_work(&requested, invalid, &original,),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized | crate::wire::ErrorCode::Invalid,
            ..
        })
    ));

    let run_widening = RuntimeScope::new(crate::Capabilities::new([read.as_str()]), limits)?;
    assert!(shared.as_ref().scope().run_limits().concurrency.is_some());
    assert!(matches!(
        shared
            .as_ref()
            .validate_stock_scope_with_native_work(&run_widening, limits, &original,),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    Ok(())
}

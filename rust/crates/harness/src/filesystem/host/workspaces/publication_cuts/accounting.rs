use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, WorkCounters};
use std::cell::RefCell;

#[derive(Clone, Copy, Default)]
pub(super) struct Case {
    pub limit: Option<u64>,
    pub cancel: bool,
    pub reset: bool,
}

#[derive(Clone)]
enum Identity {
    Owned(usize),
    Native(acyclic_fs::NativeMaterializationWork),
}

impl Identity {
    fn of(io: &PreparationIo) -> Self {
        match io {
            PreparationIo::Owned { work, .. } => Self::Owned(work.as_ref() as *const _ as usize),
            PreparationIo::Native { work, .. } => Self::Native(work.clone()),
        }
    }
    fn matches(&self, io: &PreparationIo) -> bool {
        match (self, io) {
            (Self::Owned(original), PreparationIo::Owned { work, .. }) => {
                *original == work.as_ref() as *const _ as usize
            }
            (Self::Native(original), PreparationIo::Native { work, .. }) => {
                original.shares_allocation_context(work)
            }
            _ => false,
        }
    }
}

pub(super) struct Audit {
    case: Case,
    cancellation: CancellationToken,
    identity: Option<Identity>,
    same_ledger: bool,
    attempts: Vec<u64>,
    calls: u64,
    provider_work: [u64; 5],
    operations: Vec<OperationId>,
}

impl Audit {
    pub fn new(
        case: Case,
        cancellation: CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            case,
            cancellation,
            identity: io.map(Identity::of),
            same_ledger: true,
            attempts: Vec::new(),
            calls: 0,
            provider_work: [0; 5],
            operations: Vec::new(),
        }))
    }
}

thread_local! {
    static ACTIVE: RefCell<Option<Arc<Mutex<Audit>>>> = const { RefCell::new(None) };
}

pub(super) fn poll<T>(audit: &Arc<Mutex<Audit>>, poll: impl FnOnce() -> T) -> T {
    struct Reset(Option<Arc<Mutex<Audit>>>);
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.set(self.0.take());
        }
    }
    let _reset = Reset(ACTIVE.replace(Some(audit.clone())));
    poll()
}

pub(crate) fn attempt(io: Option<&PreparationIo>) {
    let cancel = ACTIVE.with(|active| {
        let active = active.borrow();
        let mut audit = active.as_ref()?.lock().unwrap();
        let calls = audit.calls;
        audit.attempts.push(calls);
        audit.same_ledger &= match (&audit.identity, io) {
            (None, None) => true,
            (Some(original), Some(io)) => original.matches(io),
            _ => false,
        };
        (audit.case.cancel && audit.attempts.len() == 2).then(|| audit.cancellation.clone())
    });
    if let Some(cancel) = cancel {
        cancel.cancel();
    }
}

// Mutation of the production retry path: replace the invocation ledger/token at
// the second cut. The same output, budget, cancellation and accounting oracles
// below must each reject it, independently for measured and native publication.
pub(crate) fn replacement(io: &PreparationIo) -> Result<Option<PreparationIo>> {
    let reset = ACTIVE.with(|active| {
        active.borrow().as_ref().is_some_and(|audit| {
            let audit = audit.lock().unwrap();
            audit.case.reset && !audit.attempts.is_empty()
        })
    });
    if !reset {
        return Ok(None);
    }
    let mode = if io.original_native_work().is_some() {
        Mode::Native
    } else {
        Mode::Measured
    };
    new_io(
        mode,
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &CancellationToken::new(),
    )
}

pub(super) fn new_io(
    mode: Mode,
    budget: WorkBudget,
    token: &CancellationToken,
) -> Result<Option<PreparationIo>> {
    match mode {
        Mode::Raw | Mode::Batched => Ok(None),
        Mode::Measured => PreparationIo::from_budget(budget, token).map(Some),
        Mode::Native => PreparationIo::from_native_work(
            &acyclic_fs::NativeMaterializationWork::new(budget, token)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?,
        )
        .map(Some),
    }
}

fn projection(work: WorkCounters) -> [u64; 5] {
    [
        work.authority_records_read,
        work.authority_records_appended,
        work.authority_bytes_read,
        work.authority_bytes_written,
        work.materializations,
    ]
}

pub(super) fn provider_before(
    _call: &str,
    work: &mut AuthorityWork<'_>,
) -> std::result::Result<Option<WorkCounters>, AuthorityStoreError> {
    ACTIVE.with(|active| {
        let active = active.borrow();
        let Some(audit) = active.as_ref() else {
            return Ok(None);
        };
        let before = work.spent();
        // An independent provider cost: exactly one materialization per reached
        // authority operation. These workspace operations otherwise perform none.
        let unit = WorkCounters {
            materializations: 1,
            ..Default::default()
        };
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        if let Some(original) = work.original {
            original.charge(unit)?;
            let mut audit = audit.lock().unwrap();
            if let Some(Identity::Native(expected)) = &audit.identity {
                if !expected.shares_allocation_context(original) {
                    audit.same_ledger = false;
                }
            }
        } else {
            let next = work.work.checked_add(unit)?;
            next.verify(work.budget)?;
            work.work = next;
        }
        audit.lock().unwrap().calls += 1;
        Ok(Some(before))
    })
}

pub(super) fn provider_after(before: Option<WorkCounters>, work: &AuthorityWork<'_>) {
    let Some(before) = before else { return };
    ACTIVE.with(|active| {
        let active = active.borrow();
        let mut audit = active.as_ref().unwrap().lock().unwrap();
        for ((sum, before), after) in audit
            .provider_work
            .iter_mut()
            .zip(projection(before))
            .zip(projection(work.spent()))
        {
            *sum += after - before;
        }
    });
}

pub(super) fn operation(operation: OperationId) {
    ACTIVE.with(|active| {
        if let Some(audit) = active.borrow().as_ref() {
            audit.lock().unwrap().operations.push(operation);
        }
    });
}

#[derive(Debug)]
pub(super) struct Report {
    pub retry_prefix: Option<u64>,
}

fn rejection(error: &Error) -> Option<std::result::Result<(), acyclic_fs::WorkError>> {
    use acyclic_fs::{FsError as F, NativeTreeMaterializationError as N, WorkspaceError as W};
    fn native(error: &N) -> Option<std::result::Result<(), acyclic_fs::WorkError>> {
        match error {
            N::Cancelled => Some(Ok(())),
            N::Work(work) => Some(Err(*work)),
            _ => None,
        }
    }
    fn fs(error: &F) -> Option<std::result::Result<(), acyclic_fs::WorkError>> {
        match error {
            F::Cancelled(_) => Some(Ok(())),
            F::Work(work) => Some(Err(*work)),
            F::NativeKernel(error) => native(error),
            F::Authority(AuthorityStoreError::Work(work)) => Some(Err(*work)),
            F::Authority(AuthorityStoreError::NativeOriginal(error)) => native(error),
            F::Authority(AuthorityStoreError::Cancelled) => Some(Ok(())),
            _ => None,
        }
    }
    match error {
        Error::Cancelled => Some(Ok(())),
        Error::NativeWork { cause, .. } => native(cause),
        Error::NativeFilesystem { source, .. } => fs(source),
        Error::NativeWorkspace { source, .. } => match source.as_ref() {
            W::Cancelled(_) => Some(Ok(())),
            W::Work(work) => Some(Err(*work)),
            W::Filesystem(error) => fs(error),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn verify(
    audit: &Arc<Mutex<Audit>>,
    io: Option<&PreparationIo>,
    result: &Result<GenerationRef>,
) -> Result<Report> {
    let audit = audit.lock().unwrap();
    let fail = |reason: &str| Error::Invalid(format!("retry accounting proof: {reason}"));
    if audit.case.limit.is_some() || audit.case.cancel {
        if result.is_ok() {
            return Err(fail("retry escaped the original budget/cancellation"));
        }
        if audit.attempts.len() != 2 {
            return Err(fail("rejection did not reach the retry cut"));
        }
        let cause = result.as_ref().err().and_then(rejection);
        if audit.case.cancel && cause != Some(Ok(())) {
            return Err(fail("retry lost the typed cancellation cause"));
        }
        if let Some(limit) = audit.case.limit {
            if cause
                != Some(Err(acyclic_fs::WorkError::BudgetExceeded {
                    counter: "materializations",
                    observed: limit + 1,
                    maximum: limit,
                }))
            {
                return Err(fail("retry lost the exact exhausted-budget cause"));
            }
        }
    }
    if let Some(io) = io {
        if projection(io.receipt()) != audit.provider_work {
            return Err(fail("exact provider work was lost or charged twice"));
        }
        if io.receipt().materializations != audit.calls {
            return Err(fail(
                "exact retry work differs from independent provider calls",
            ));
        }
    }
    if !audit.same_ledger {
        return Err(fail("retry changed ledger identity"));
    }
    if audit.operations.iter().any(|id| *id != audit.operations[0]) {
        return Err(fail("retry changed operation identity"));
    }
    if let Some(limit) = audit.case.limit {
        if audit.calls != limit {
            return Err(fail("exhaustion lost the exact admitted prefix"));
        }
    }
    if audit.case.cancel && audit.attempts[1] != audit.calls {
        return Err(fail(
            "cancellation admitted provider work after the retry cut",
        ));
    }
    Ok(Report {
        retry_prefix: audit.attempts.get(1).copied(),
    })
}

fn memory() -> MemoryAuthorityBackend {
    MemoryAuthorityBackend::new(Arc::new(acyclic_stream::MemoryStream::default()))
}

#[tokio::test]
async fn every_publication_path_rejects_the_conflict_control() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RESTORE_CONFLICT.set(false);
        }
    }
    let _reset = Reset;
    for mode in [Mode::Batched, Mode::Raw, Mode::Measured, Mode::Native] {
        RESTORE_CONFLICT.set(true);
        assert!(
            schedule(memory(), mode, false).await.is_err(),
            "conflict control escaped {mode:?}"
        );
    }
}

#[tokio::test]
async fn retries_keep_exact_work_identity_budget_and_cancellation_with_controls() -> Result<()> {
    for mode in [Mode::Measured, Mode::Native] {
        let baseline = schedule_accounted(memory(), mode, false, Case::default()).await?;
        let limit = baseline
            .retry_prefix
            .expect("schedule never reached the retry");
        for case in [
            Case::default(),
            Case {
                limit: Some(limit),
                ..Default::default()
            },
            Case {
                cancel: true,
                ..Default::default()
            },
        ] {
            schedule_accounted(memory(), mode, false, case).await?;
            let broken = schedule_accounted(
                memory(),
                mode,
                false,
                Case {
                    reset: true,
                    ..case
                },
            )
            .await;
            assert!(
                matches!(broken, Err(Error::Invalid(ref reason)) if reason.starts_with("retry accounting proof:")),
                "ledger-reset control escaped {mode:?}: {broken:?}"
            );
        }
    }
    Ok(())
}

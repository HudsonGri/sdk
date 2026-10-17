use super::*;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, IdempotencyKey, PublicationPermit,
    TransactionCommit, WorkBudget, Workspace, WorkspaceError,
};
use bytes::Bytes;

type Start = dyn Fn(Arc<queue::Queue<Pending>>) -> Result<(), AuditError> + Send + Sync;
pub(crate) struct Recorder {
    log: startup::Startup<Arc<AuditLog>>,
    start: Box<Start>,
}
const CAPACITY: usize = 64;
const BATCH_RECORDS: usize = 16;
pub(super) const DIRECTORY: &str = "/decisions";
const SINK_POLICY: &[u8] = br#"{"schema_version":1,"version":1,"layers":[]}"#;

// The audit volume is service-owned, never part of the projected project tree.
// Its deny-all policy prevents project capabilities from reading sensitive paths
// in records or rewriting the log. Only the private recorder uses raw storage.
pub(super) async fn sink<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    raw: &crate::Fs<A, O>,
    source: crate::VolumeId,
    lifecycle: crate::Lifecycle,
) -> Result<Workspace<A, O>, AuditError> {
    let cancellation = CancellationToken::new();
    let workspace = raw
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            format!(
                "acyclic-audit-{}",
                uuid::Uuid::from_bytes(source.into_bytes())
            ),
            crate::VolumeInitialization {
                config: crate::VolumeConfig::portable(lifecycle),
                access_policy: Some(SINK_POLICY),
            },
            crate::OperationId::from_bytes(source.into_bytes()),
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await
        .map_err(|_| AuditError)?
        .value
        .workspace();
    let mut checkout = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|_| AuditError)?
        .value;
    let (policy, _) = checkout
        .read_access_policy(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|_| AuditError)?
        .value;
    if !policy.is_some_and(|policy| policy.bytes.as_ref() == SINK_POLICY) {
        return Err(AuditError);
    }
    Ok(workspace)
}
impl Recorder {
    pub(crate) fn new<A, O>(raw: crate::Fs<A, O>) -> Self
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        Self {
            log: Default::default(),
            start: Box::new(move |queue| {
                let raw = raw.clone();
                std::thread::Builder::new()
                    .name("fs-audit".into())
                    .spawn(move || {
                        struct Finish<'a> {
                            queue: &'a queue::Queue<Pending>,
                            failed: bool,
                        }
                        impl Drop for Finish<'_> {
                            fn drop(&mut self) {
                                self.queue.close(self.failed);
                            }
                        }
                        let mut finish = Finish {
                            queue: &queue,
                            failed: true,
                        };
                        // Own the executor: storage remains usable while the caller's
                        // runtime retires and the final admitted batch is drained.
                        if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                        {
                            finish.failed = runtime.block_on(write_batches(&raw, &queue)).is_err();
                        }
                    })
                    .map_err(|_| AuditError)?;
                Ok(())
            }),
        }
    }
    pub(crate) fn flush(&self) -> Result<(), AuditError> {
        if let Some(log) = self.log.started()? {
            log.flush()?;
        }
        Ok(())
    }

    fn log(&self) -> Result<Arc<AuditLog>, AuditError> {
        self.log.get(|| {
            let queue = Arc::new(queue::Queue::new(CAPACITY));
            (self.start)(Arc::clone(&queue))?;
            // Only a successful start constructs the closing owner, so startup
            // failure cannot acquire the queue lock under the startup lock.
            Ok(Arc::new(AuditLog {
                id: uuid::Uuid::now_v7(),
                queue,
            }))
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Bind the volume-file audit writer to an authenticated compiled decision view.
    /// `credential_fingerprint` identifies the verified grant without retaining its bearer proof.
    pub fn audit_view(
        &self,
        mut view: crate::access::PathTrie,
        principal: Principal,
        capability_chain: Capability,
        credential_fingerprint: Option<[u8; 32]>,
        mut custody: Option<crate::NativeAllocation>,
    ) -> Result<crate::access::PathTrie, AuditError> {
        if view.volume() != uuid::Uuid::from_bytes(self.id().volume_id().into_bytes()).to_string()
            || view.config() != self.volume_config()
            || !view
                .audit_binding(&principal, &capability_chain)
                .map_err(|_| AuditError)?
        {
            return Err(AuditError);
        }
        if let Some(custody) = &mut custody {
            custody
                .grow(
                    (size_of::<Identity>()
                        + size_of::<Binding>()
                        + 4 * size_of::<usize>()
                        + view.rules_owned_bytes()) as u64,
                )
                .map_err(|_| AuditError)?;
        }
        let log = self.volume.fs.audit_recorder()?.log()?;
        view.audit = Some(Arc::new(Binding {
            log,
            identity: Arc::new(Identity {
                principal,
                capability_chain,
                credential_fingerprint,
                volume_id: self.id().volume_id(),
                config: self.volume_config(),
                rules: view.rules.clone(),
                _custody: custody,
            }),
        }));
        Ok(view)
    }
}

async fn write_batches<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    raw: &crate::Fs<A, O>,
    queue: &queue::Queue<Pending>,
) -> Result<(), AuditError> {
    let mut batch = Vec::with_capacity(BATCH_RECORDS);
    let mut sequence = 0u64;
    let mut destination: Option<(crate::VolumeId, Workspace<A, O>)> = None;
    while queue.take(&mut batch, BATCH_RECORDS)? {
        for record in &mut batch {
            record.sequence = sequence;
            sequence = sequence.checked_add(1).ok_or(AuditError)?;
        }
        batch.sort_unstable_by_key(|record| record.binding.volume_id);
        for records in batch.chunk_by(|a, b| a.binding.volume_id == b.binding.volume_id) {
            let context = &records[0].binding;
            if destination
                .as_ref()
                .is_none_or(|(source, _)| *source != context.volume_id)
            {
                destination = Some((
                    context.volume_id,
                    sink(raw, context.volume_id, context.config.lifecycle).await?,
                ));
            }
            let workspace = &destination.as_ref().ok_or(AuditError)?.1;
            let mut bytes = Vec::new();
            for record in records {
                serde_json::to_writer(&mut bytes, record).map_err(|_| AuditError)?;
                bytes.push(b'\n');
            }
            append_batch(workspace, Bytes::from(bytes))
                .await
                .map_err(|_| AuditError)?;
        }
        let count = batch.len();
        batch.clear();
        queue.committed(count)?;
    }
    Ok(())
}

async fn append_batch<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    bytes: Bytes,
) -> Result<(), WorkspaceError> {
    let cancellation = CancellationToken::new();
    // A batch has one immutable file and stable operation identity across CAS retries.
    let operation = IdempotencyKey::new();
    let path = format!(
        "{DIRECTORY}/{}.jsonl",
        uuid::Uuid::from_bytes(operation.operation_id().into_bytes())
    );
    loop {
        let mut transaction = workspace
            .begin_transaction(operation, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        transaction
            .create_dir_all(DIRECTORY, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        transaction
            .create_file(
                &path,
                bytes.clone(),
                Default::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?;
        let outcome = transaction
            .commit::<WorkspaceError, _, _>(
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        match outcome {
            TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => {
                return Ok(());
            }
            TransactionCommit::Conflict { .. } => continue,
            TransactionCommit::Fenced => return Err(WorkspaceError::StaleSource),
            TransactionCommit::IdempotencyConflict => {
                return Err(WorkspaceError::Filesystem(crate::FsError::Authority(
                    crate::AuthorityStoreError::RejectedStatic("audit batch idempotency conflict"),
                )));
            }
        }
    }
}

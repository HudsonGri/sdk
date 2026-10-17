use super::*;
use crate::{
    core::{Event, EventPayload},
    interaction::ApprovalDecisionFile,
    task_volume::{
        self, Component, Cursor, FileKey, FileQuerySource, FileValue, Generation, Observation,
        Query, SCHEMA, Snapshot, TaskFile,
    },
};
use acyclic_fs::WorkBudget;
use acyclic_stream::{BoxProviderFuture, ReadRequest, Record, StreamPath};
use futures::StreamExt as _;
use std::collections::BTreeMap;

/// File adapter over the interaction owner's canonical history. It retains no settlement state.
pub struct InteractionFiles<P, A, O> {
    owner: Arc<FilesystemInteractionHost<P, A, O>>,
    namespace: Component,
    budget: WorkBudget,
}

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Exposes requests and authenticated decisions as files over the same owner transaction.
    pub fn approval_files(
        self: &Arc<Self>,
        namespace: Component,
        budget: WorkBudget,
    ) -> Result<InteractionFiles<P, A, O>> {
        if !budget.has_finite_allowances() {
            return Err(Error::Invalid(
                "approval files require finite read work".into(),
            ));
        }
        Ok(InteractionFiles {
            owner: self.clone(),
            namespace,
            budget,
        })
    }
}

impl<P, A, O> InteractionFiles<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Writes a decision through the owner's authenticated, versioned resolve transaction.
    pub async fn write(&self, file: &FileValue, responder: &Scope) -> Result<ResolutionReceipt> {
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, principal },
        } = &file.key
        else {
            return Err(Error::Unauthorized(
                "approval request files are read-only".into(),
            ));
        };
        if task != &self.namespace || principal.as_str() != responder.id() || file.channel.is_some()
        {
            return Err(Error::Unauthorized(
                "approval decision principal or namespace differs".into(),
            ));
        }
        let id: Uuid = request
            .as_str()
            .parse()
            .map_err(|_| Error::Invalid("invalid approval request identity".into()))?;
        if id.hyphenated().to_string() != request.as_str() {
            return Err(Error::Invalid(
                "noncanonical approval request identity".into(),
            ));
        }
        let id = InteractionId::from_bytes(*id.as_bytes());
        self.owner.verify_responder_scope(id, responder, None)?;
        let decision = ApprovalDecisionFile::decode(&file.bytes, self.owner.maximum_bytes)?;
        let (ticket, _) = self
            .owner
            .read(id, None)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        if ticket.kind != InteractionKind::Approval
            || ticket.request.descriptor().sha256() != &decision.request_sha256
        {
            return Err(Error::Conflict(
                "approval decision request binding differs".into(),
            ));
        }
        let operation = file
            .publication
            .ok_or_else(|| Error::Invalid("approval decision requires a write identity".into()))?;
        self.owner
            .resolve_response(
                (operation, id, decision.expected_version),
                responder,
                InteractionResponse::Approval {
                    approved: decision.approved,
                    reason: decision.reason,
                },
                None,
            )
            .await
    }

    fn copy(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            namespace: self.namespace.clone(),
            budget: self.budget,
        }
    }

    fn select(&self, query: &Query) -> task_volume::Result<()> {
        match query {
            Query::Approvals(task) if task == &self.namespace => Ok(()),
            Query::File(FileKey::Task {
                task,
                file: TaskFile::ApprovalRequest(_) | TaskFile::ApprovalDecision { .. },
            }) if task == &self.namespace => Ok(()),
            _ => Err(task_volume::Error::Unauthorized),
        }
    }

    async fn project(&self, event: &Event, io: &PreparationIo) -> Result<Option<FileValue>> {
        let (key, bytes) = match &event.payload {
            EventPayload::InteractionOpened { ticket }
                if ticket.kind == InteractionKind::Approval =>
            {
                let bytes = self.owner.read_file(&ticket.request, Some(io)).await?;
                let (request, retained, custody) =
                    with_history_io(Some(io), |work| ticket.validate_request_bytes(&bytes, work))?;
                let hold = io.guard_owned(retained)?;
                drop((request, custody, hold));
                (
                    TaskFile::ApprovalRequest(
                        Component::new(ticket.id.hyphenated().to_string())
                            .map_err(|_| Error::Storage("invalid admitted approval id".into()))?,
                    ),
                    bytes,
                )
            }
            EventPayload::InteractionResolved { resolution }
                if matches!(
                    resolution.outcome,
                    InteractionOutcome::Approved {} | InteractionOutcome::Declined {}
                ) =>
            {
                let id = InteractionId::from_bytes(*resolution.id.as_bytes());
                let before = io.owned_bytes()?;
                let read = self.owner.read(id, Some(io)).await?;
                let hold = io.guard_owned(io.owned_since(before)?)?;
                let (ticket, _) = read
                    .as_ref()
                    .ok_or_else(|| Error::Storage("decision has no request".into()))?;
                if ticket.kind != InteractionKind::Approval {
                    return Ok(None);
                }
                let detail = resolution
                    .detail
                    .as_ref()
                    .ok_or_else(|| Error::Storage("approval decision detail is missing".into()))?;
                let response_bytes = self.owner.read_file(detail, Some(io)).await?;
                let response_hold = io.guard_bytes(&response_bytes)?;
                let (response, response_custody) = io.decode(
                    std::marker::PhantomData::<InteractionResponse>,
                    &response_bytes,
                    ErrorCode::Storage,
                )?;
                let InteractionResponse::Approval { approved, reason } = response else {
                    return Err(Error::Storage("approval detail is not a decision".into()));
                };
                if approved != matches!(resolution.outcome, InteractionOutcome::Approved {}) {
                    return Err(Error::Storage(
                        "approval detail differs from its admitted outcome".into(),
                    ));
                }
                let decision = ApprovalDecisionFile {
                    request_sha256: *ticket.request.descriptor().sha256(),
                    expected_version: resolution.expected_version,
                    approved,
                    reason,
                };
                let bytes = io.encode(&decision, self.owner.maximum_bytes, false)?;
                let key = TaskFile::ApprovalDecision {
                    request: Component::new(resolution.id.hyphenated().to_string())
                        .map_err(|_| Error::Storage("invalid admitted approval id".into()))?,
                    principal: Component::new(event.scope.id()).map_err(|_| {
                        Error::Storage("approval principal is not a file component".into())
                    })?,
                };
                drop((
                    decision,
                    response_custody,
                    response_bytes,
                    response_hold,
                    read,
                    hold,
                ));
                (key, bytes)
            }
            _ => return Ok(None),
        };
        Ok(Some(FileValue {
            key: FileKey::Task {
                task: self.namespace.clone(),
                file: key,
            },
            channel: None,
            publication: Some(event.operation_id),
            bytes,
        }))
    }

    async fn event_file(&self, record: &Record, io: &PreparationIo) -> Result<Option<FileValue>> {
        let (event, retained) = with_history_io(Some(io), |work| {
            crate::store::history::verify_history_record_with_retained_quota(
                &self.owner.verifier,
                self.owner.verifier.audience(),
                record.sequence,
                record,
                work,
            )
        })?;
        let hold = io.guard_owned(retained)?;
        let result = self.project(&event, io).await;
        drop((event, hold));
        result
    }
}

async fn record<P: StreamProvider>(
    provider: &P,
    path: &StreamPath,
    position: u64,
) -> task_volume::Result<Record> {
    let mut records = provider
        .read(ReadRequest {
            path: path.clone(),
            from: position,
            limit: 1,
        })
        .await?;
    let record = records
        .next()
        .await
        .ok_or(task_volume::Error::ResetRequired)??;
    if record.sequence != position {
        return Err(task_volume::Error::ResetRequired);
    }
    Ok(record)
}

impl<P, A, O> FileQuerySource for InteractionFiles<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn observe_files(
        &self,
        query: Query,
        supplied: Option<Cursor>,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> BoxProviderFuture<'_, task_volume::Result<Observation>> {
        Box::pin(async move {
            self.select(&query)?;
            if maximum_files == 0 || maximum_bytes == 0 {
                return Err(task_volume::Error::LimitExceeded);
            }
            // The existing aggregate read checks the conversation's exact private-volume owner.
            drop(self.owner.aggregate(None).await?);
            let path = StreamPath::new(self.owner.verifier.audience().stream_path(&mut None)?)?;
            let provider = self.owner.stream.as_ref();
            let genesis = record(provider, &path, 0).await?;
            let watermarks = provider.watermarks(path.clone()).await?;
            if watermarks.accepted == 0 || watermarks.durable > watermarks.accepted {
                return Err(task_volume::Error::ResetRequired);
            }
            let mut identity = blake3::Hasher::new();
            identity.update(path.as_str().as_bytes());
            identity.update(genesis.commit_id.as_bytes());
            identity.update(self.namespace.as_str().as_bytes());
            let volume = *identity.finalize().as_bytes();
            let epoch = Uuid::from_bytes(
                genesis.commit_id.as_bytes()[..16]
                    .try_into()
                    .map_err(|_| task_volume::Error::ResetRequired)?,
            );
            let query_id = task_volume::query_id(&query)?;
            let mut resume = false;
            let mut cursor = Cursor {
                volume,
                epoch,
                schema: SCHEMA,
                query: query_id,
                position: watermarks.accepted,
                accepted: watermarks.accepted,
                durable: watermarks.durable,
                commit: [0; 32],
            };
            if let Some(prior) = &supplied
                && prior.volume == volume
                && prior.epoch == epoch
                && prior.schema == SCHEMA
                && prior.query == query_id
                && prior.position > 0
                && prior.position <= watermarks.accepted
                && prior.accepted >= prior.position
                && prior.accepted <= watermarks.accepted
                && prior.durable <= prior.accepted
                && prior.durable <= watermarks.durable
                && record(provider, &path, prior.position - 1)
                    .await?
                    .commit_id
                    .as_bytes()
                    == &prior.commit
            {
                cursor = prior.clone();
                resume = true;
            }
            if !resume {
                cursor.commit = *record(provider, &path, cursor.position - 1)
                    .await?
                    .commit_id
                    .as_bytes();
            }
            let snapshot = if resume {
                None
            } else {
                let io =
                    PreparationIo::from_budget(self.budget, &acyclic_fs::CancellationToken::new())?;
                let mut files = BTreeMap::new();
                let mut bytes = 0usize;
                for position in 0..cursor.position {
                    let record = record(provider, &path, position).await?;
                    if let Some(file) = self.event_file(&record, &io).await?
                        && query.matches(&file.key)
                    {
                        bytes = bytes
                            .checked_add(file.bytes.len())
                            .ok_or(task_volume::Error::LimitExceeded)?;
                        if files.len() == maximum_files || bytes > maximum_bytes {
                            return Err(task_volume::Error::LimitExceeded);
                        }
                        files.insert(file.key.clone(), file);
                    }
                }
                Some(Snapshot {
                    files: files.into_values().collect(),
                    generation: Generation {
                        volume,
                        epoch,
                        position: cursor.position,
                        commit: cursor.commit,
                    },
                    cursor: cursor.clone(),
                })
            };
            let records = provider
                .follow_checked(
                    path,
                    cursor.position,
                    acyclic_stream::CommitId::from_bytes(cursor.commit),
                )
                .await?;
            let adapter = self.copy();
            let changes = Box::pin(futures::stream::try_unfold(
                (records, cursor),
                move |(mut records, mut cursor)| {
                    let adapter = adapter.copy();
                    let query = query.clone();
                    async move {
                        let record = records
                            .next()
                            .await
                            .ok_or(task_volume::Error::ResetRequired)??;
                        if record.sequence != cursor.position {
                            return Err(task_volume::Error::ResetRequired);
                        }
                        let io = PreparationIo::from_budget(
                            adapter.budget,
                            &acyclic_fs::CancellationToken::new(),
                        )?;
                        let file = adapter
                            .event_file(&record, &io)
                            .await?
                            .filter(|file| query.matches(&file.key));
                        if file
                            .as_ref()
                            .is_some_and(|file| file.bytes.len() > maximum_bytes)
                        {
                            return Err(task_volume::Error::LimitExceeded);
                        }
                        cursor.position = cursor
                            .position
                            .checked_add(1)
                            .ok_or(task_volume::Error::LimitExceeded)?;
                        cursor.accepted = cursor.accepted.max(cursor.position);
                        cursor.commit = *record.commit_id.as_bytes();
                        Ok(Some((
                            task_volume::Change {
                                file,
                                files: vec![],
                                removed: vec![],
                                cursor: cursor.clone(),
                            },
                            (records, cursor),
                        )))
                    }
                },
            ));
            Ok(Observation {
                snapshot,
                reset: supplied.is_some() && !resume,
                changes,
            })
        })
    }
}

impl crate::filesystem::PreparationRecord for InteractionResponse {
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::InteractionResponse
    }
}

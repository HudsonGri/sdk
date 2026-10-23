//! Approval requests and decisions followed as task files.
//!
//! The interaction owner is the only settlement authority. These files are
//! verbatim copies of its admitted request and staged decision. Execution
//! observes a verdict only as a [`PublishedDecision`], which exists only after
//! the decision's mandatory file validated against its ticket, the provider
//! issued a durable receipt for the conversation events that committed it and,
//! for a task journal, for that exact followed file. An acknowledgement never
//! stands in for a receipt, so nothing written under deferred durability
//! reaches execution until the provider's barrier covers it.
use super::*;
use crate::{
    executor::with_history_io,
    interaction::{ApprovalDecisionFile, InteractionKind, principal_hex},
    task_volume::{Component, DurableFile, FileKey, FileValue, TaskFile},
};

/// Awaits one host read, keeping its retained bytes guarded for the rest of
/// the enclosing scope.
macro_rules! guarded_read {
    ($read:expr, $io:expr, $guard:ident) => {{
        let before = $io
            .map(PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let read = $read.await?;
        $guard = $io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        read
    }};
}

fn admitted<T>(read: Option<T>, io: Option<&PreparationIo>) -> Result<T> {
    read.ok_or_else(|| {
        canonical_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "published approval was not admitted",
        )
    })
}

/// Execution's only view of an approval verdict. Its single constructor is
/// [`FilesystemExecutionJournal::published_decision`].
pub(super) struct PublishedDecision {
    approved: bool,
    /// The conversation events that admitted the request and committed this
    /// resolution are in the provider's durable prefix.
    _resolution: acyclic_stream::DurableReceipt,
    /// For a task journal, its followed decision file is durable too. A
    /// conversation journal is itself the interaction owner and has no
    /// task-file surface.
    _file: Option<DurableFile>,
}

impl PublishedDecision {
    pub(super) fn outcome(&self) -> InteractionOutcome {
        if self.approved {
            InteractionOutcome::Approved {}
        } else {
            InteractionOutcome::Declined {}
        }
    }

    pub(super) fn approved(&self) -> bool {
        self.approved
    }
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits `approvals/<id>/decisions/<principal>.json` through the owner's
    /// authenticated compare-and-set resolve of exactly the submitted binding,
    /// then publishes the owner's copy durably.
    pub async fn write_approval_decision(&self, file: &FileValue, responder: &Scope) -> Result<()> {
        let task_id = self.approval_task_id().ok_or_else(|| {
            Error::Unsupported("approval decisions require a task journal".into())
        })?;
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, principal },
        } = &file.key
        else {
            return Err(Error::Unauthorized(
                "only approval decisions are approver files".into(),
            ));
        };
        if task.as_str() != task_id.to_string()
            || principal.as_str() != ApprovalDecisionFile::principal_file(responder.id())
            || file.channel.is_some()
            || file.requester.is_some()
            || file.publication.is_some()
        {
            return Err(Error::Unauthorized(
                "approval decision principal or task differs".into(),
            ));
        }
        let id: uuid::Uuid = request
            .as_str()
            .parse()
            .map_err(|_| Error::Invalid("invalid approval request identity".into()))?;
        if id.hyphenated().to_string() != request.as_str() {
            return Err(Error::Invalid(
                "noncanonical approval request identity".into(),
            ));
        }
        let id = InteractionId::from_bytes(id.into_bytes());
        let decision = ApprovalDecisionFile::decode(&file.bytes, self.maximum_payload_bytes)?;
        let host = self.interactions()?;
        // The passive route can submit only an already published request in
        // this task. Its bytes must be the interaction owner's exact request.
        let mut transaction = host.transaction(None).await?;
        let ticket = host
            .read_at(&transaction.aggregate.0, id, None)?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?
            .0;
        let key = FileKey::Task {
            task: task.clone(),
            file: TaskFile::ApprovalRequest(request.clone()),
        };
        let published = self
            .approval_files()?
            .snapshot(&crate::task_volume::Query::File(key), 1)
            .await?;
        let admitted = host.read_file(&ticket.request, None).await?;
        if !matches!(published.files.as_slice(), [file] if file.bytes == admitted) {
            return Err(Error::Unauthorized(
                "approval request is not owned by this task".into(),
            ));
        }
        let owner = self.owner.as_ref().map(|(owner, _)| owner);
        if let Some(owner) = owner {
            owner.require_interaction_grant(&mut None)?;
            owner.verify(true, &mut None).await?;
        }
        host.resolve_decision(id, decision, responder, owner, &mut transaction)
            .await?;
        self.published_decision_from(id, None, transaction.aggregate, None)
            .await?;
        if let Some(owner) = owner {
            owner.verify(true, &mut None).await?;
        }
        Ok(())
    }

    /// Publishes the owner's admitted approval request into the task's files.
    pub(super) async fn publish_approval_request(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let Some((owner, _)) = &self.owner else {
            return Ok(());
        };
        let host = self.interactions()?;
        let _read;
        let (ticket, _) = admitted(guarded_read!(host.read(id, io), io, _read), io)?;
        if ticket.kind != InteractionKind::Approval {
            return Ok(());
        }
        let files = self.approval_files()?;
        let bytes = host.read_file(&ticket.request, io).await?;
        let _request = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let value = FileValue::new(
            FileKey::Task {
                task: Component::new(owner.task_id().to_string())?,
                file: TaskFile::ApprovalRequest(Component::new(ticket.id.to_string())?),
            },
            bytes,
        );
        Ok(files.publish_owned(&value, native_work(io)).await?)
    }

    /// Validates the committed decision's mandatory file and proves its
    /// resolution and, for a task journal, the request and decision files
    /// durable before any verdict reaches execution.
    pub(super) async fn published_decision(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<PublishedDecision> {
        self.published_decision_for(id, None, io).await
    }

    pub(super) async fn published_decision_for(
        &self,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        io: Option<&PreparationIo>,
    ) -> Result<PublishedDecision> {
        let host = self.interactions()?;
        let aggregate = host.aggregate(io).await?;
        self.published_decision_from(id, binding, aggregate, io)
            .await
    }

    pub(super) async fn published_decision_from<'io>(
        &self,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        aggregate: (
            StreamAggregate<P>,
            Option<super::super::DecodeReservation<'io>>,
        ),
        io: Option<&'io PreparationIo>,
    ) -> Result<PublishedDecision> {
        let host = self.interactions()?;
        let (read, receipt, _read) = host.read_durable(&aggregate.0, id, binding, io).await?;
        drop(aggregate);
        let (ticket, resolution) = admitted(read, io)?;
        let (Some(resolution), InteractionKind::Approval) = (&resolution, ticket.kind) else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval has no committed decision",
            ));
        };
        let detail = resolution.detail.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "approval decision has no decision file",
            )
        })?;
        let request_bytes = if binding.is_some() || self.approval_task_id().is_some() {
            Some(host.read_file(&ticket.request, io).await?)
        } else {
            None
        };
        let _request_bytes = request_bytes
            .as_ref()
            .map(|bytes| io.map(|io| io.guard_bytes(bytes)).transpose())
            .transpose()?
            .flatten()
            .flatten();
        let _request = if binding.is_some() {
            let bytes = request_bytes.as_ref().ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "approval request is missing",
                )
            })?;
            crate::filesystem::interaction_host::admit_interaction(bytes, io)?;
            let decoded = with_history_io(io, |work| ticket.validate_request_bytes(bytes, work))?;
            let hold = io.map(|io| io.guard_owned(decoded.1)).transpose()?;
            Some((decoded, hold))
        } else {
            None
        };
        let bytes = host.read_file(detail, io).await?;
        let _detail = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if binding.is_some() {
            crate::filesystem::interaction_host::admit_interaction(&bytes, io)?;
        }
        let principal = with_history_io(io, |work| {
            ticket.validate_decision_bytes(resolution, None, detail, &bytes, work)
        })?;
        // The validation above bound the file's verdict to this outcome.
        let approved = matches!(resolution.outcome, InteractionOutcome::Approved {});
        let resolution = receipt.ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval decision awaits durability",
            )
        })?;
        let Some(task_id) = self.approval_task_id() else {
            return Ok(PublishedDecision {
                approved,
                _resolution: resolution,
                _file: None,
            });
        };
        let proof = self
            .publish_decision_files(task_id, id, &principal, request_bytes, bytes, io)
            .await?;
        Ok(PublishedDecision {
            approved,
            _resolution: resolution,
            _file: Some(proof),
        })
    }

    async fn publish_decision_files(
        &self,
        task_id: crate::TaskId,
        id: InteractionId,
        principal: &[u8; 16],
        request_bytes: Option<acyclic_stream::OwnedBytes>,
        bytes: acyclic_stream::OwnedBytes,
        io: Option<&PreparationIo>,
    ) -> Result<DurableFile> {
        let files = self.approval_files()?;
        let task = Component::new(task_id.to_string())?;
        let request = Component::new(id.to_string())?;
        let request_bytes = request_bytes.ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "approval request is missing",
            )
        })?;
        let work = native_work(io);
        files
            .publish_durable(
                &FileValue::new(
                    FileKey::Task {
                        task: task.clone(),
                        file: TaskFile::ApprovalRequest(request.clone()),
                    },
                    request_bytes,
                ),
                work,
            )
            .await?;
        let key = FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision {
                request,
                principal: Component::new(principal_hex(principal))?,
            },
        };
        let proof = files
            .publish_durable(&FileValue::new(key.clone(), bytes), work)
            .await?;
        if proof.key() != &key {
            return Err(Error::Storage("durable proof names another file".into()));
        }
        Ok(proof)
    }

    fn approval_task_id(&self) -> Option<crate::TaskId> {
        self.owner
            .as_ref()
            .map(|(owner, _)| owner.task_id())
            .or(self.approval_task)
    }

    /// A task journal bound to an interaction owner always carries its files.
    fn approval_files(&self) -> Result<&crate::task_volume::TaskFiles<P>> {
        self.task_files
            .as_deref()
            .ok_or_else(|| Error::Invalid("task approvals require private task files".into()))
    }
}

fn native_work(io: Option<&PreparationIo>) -> crate::NativeWork<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    return io.and_then(PreparationIo::original_native_work);
    #[cfg(target_arch = "wasm32")]
    io.and(None)
}

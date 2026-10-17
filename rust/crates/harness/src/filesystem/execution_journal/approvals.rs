//! Approval requests and decisions followed as task files.
//!
//! The interaction owner is the only settlement authority. These files are
//! verbatim copies of its admitted request and staged decision. Execution
//! observes a verdict only as a [`PublishedDecision`], which exists only after
//! the decision's mandatory file validated against its ticket and, for a task
//! journal, the provider confirmed that exact file durable.
use super::*;
use crate::{
    executor::with_history_io,
    interaction::{ApprovalDecisionFile, InteractionKind, principal_hex},
    task_volume::{Component, DurableFile, FileKey, FileValue, TaskFile},
};

/// Reads the admitted ticket and resolution, keeping the read's retained
/// bytes guarded for the rest of the enclosing scope.
macro_rules! read_admitted {
    ($host:expr, $id:expr, $io:expr, $guard:ident) => {{
        let before = $io
            .map(PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let read = $host.read($id, $io).await?;
        $guard = $io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        read.ok_or_else(|| {
            canonical_boundary(
                $io,
                crate::wire::ErrorCode::Storage,
                "published approval was not admitted",
            )
        })?
    }};
}

/// Execution's only view of an approval verdict. Its single constructor is
/// [`FilesystemExecutionJournal::published_decision`].
pub(super) struct PublishedDecision {
    approved: bool,
    _record: DecisionRecord,
}

enum DecisionRecord {
    /// The task's followed decision file is durable.
    TaskFile { _proof: DurableFile },
    /// A conversation journal is itself the interaction owner; its committed
    /// resolution is the record and it has no task-file surface.
    Conversation,
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
        let Some((owner, _)) = &self.owner else {
            return Err(Error::Unsupported(
                "approval decisions require a task journal".into(),
            ));
        };
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, principal },
        } = &file.key
        else {
            return Err(Error::Unauthorized(
                "only approval decisions are approver files".into(),
            ));
        };
        if task.as_str() != owner.task_id().to_string()
            || principal.as_str() != ApprovalDecisionFile::principal_file(responder.id())
            || file.channel.is_some()
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
        owner.require_interaction_grant(&mut None)?;
        owner.verify(true, &mut None).await?;
        host.resolve_decision(id, decision, responder, Some(owner))
            .await?;
        self.published_decision(id, None).await?;
        owner.verify(true, &mut None).await
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
        let (ticket, _) = read_admitted!(host, id, io, _read);
        if ticket.kind != InteractionKind::Approval {
            return Ok(());
        }
        let files = self.approval_files()?;
        let bytes = host.read_file(&ticket.request, io).await?;
        let _request = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let value = FileValue {
            key: FileKey::Task {
                task: Component::new(owner.task_id().to_string())?,
                file: TaskFile::ApprovalRequest(Component::new(ticket.id.to_string())?),
            },
            channel: None,
            publication: None,
            bytes,
        };
        Ok(files.publish_owned(&value, native_work(io)).await?)
    }

    /// Validates the committed decision's mandatory file and, for a task
    /// journal, proves the request and decision files durable before any
    /// verdict reaches execution.
    pub(super) async fn published_decision(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<PublishedDecision> {
        let host = self.interactions()?;
        let _read;
        let (ticket, resolution) = read_admitted!(host, id, io, _read);
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
        let bytes = host.read_file(detail, io).await?;
        let _detail = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let principal = with_history_io(io, |work| {
            ticket.validate_decision_bytes(resolution, None, detail, &bytes, work)
        })?;
        let approved = ApprovalDecisionFile::decode(&bytes, self.maximum_payload_bytes)?.approved();
        if approved != matches!(resolution.outcome, InteractionOutcome::Approved {}) {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "approval decision file differs from its verdict",
            ));
        }
        let Some((owner, _)) = &self.owner else {
            return Ok(PublishedDecision {
                approved,
                _record: DecisionRecord::Conversation,
            });
        };
        let files = self.approval_files()?;
        let task = Component::new(owner.task_id().to_string())?;
        let request = Component::new(ticket.id.to_string())?;
        let request_bytes = host.read_file(&ticket.request, io).await?;
        let _request = io
            .map(|io| io.guard_bytes(&request_bytes))
            .transpose()?
            .flatten();
        let work = native_work(io);
        files
            .publish_durable(
                &FileValue {
                    key: FileKey::Task {
                        task: task.clone(),
                        file: TaskFile::ApprovalRequest(request.clone()),
                    },
                    channel: None,
                    publication: None,
                    bytes: request_bytes,
                },
                work,
            )
            .await?;
        let key = FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision {
                request,
                principal: Component::new(principal_hex(&principal))?,
            },
        };
        let proof = files
            .publish_durable(
                &FileValue {
                    key: key.clone(),
                    channel: None,
                    publication: None,
                    bytes,
                },
                work,
            )
            .await?;
        if proof.key() != &key {
            return Err(Error::Storage("durable proof names another file".into()));
        }
        Ok(PublishedDecision {
            approved,
            _record: DecisionRecord::TaskFile { _proof: proof },
        })
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

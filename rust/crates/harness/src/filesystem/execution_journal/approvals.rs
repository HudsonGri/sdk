//! Approval requests and decisions followed as task files.
//!
//! The interaction owner is the only settlement authority. These files are
//! verbatim copies of its admitted request and staged decision, published by the
//! task journal after each commit and again before execution observes a
//! decision, so every decided approval leaves the same followed trail.
use super::*;
use crate::{
    executor::with_history_io,
    interaction::{ApprovalDecisionFile, InteractionKind, principal_hex},
    task_volume::{Component, FileKey, FileValue, TaskFile, TaskFiles},
};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits `approvals/<id>/decisions/<principal>.json` through the owner's
    /// authenticated resolve transaction, then publishes the owner's copy.
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
        let (ticket, prior) = self
            .interactions()?
            .read(id, None)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        let version = match &prior {
            Some(prior) if prior.outcome.is_terminal() => prior.expected_version,
            Some(prior) => crate::contract::next_revision(prior.expected_version)?,
            None => 1,
        };
        if ticket.kind != InteractionKind::Approval
            || decision.principal != responder.id()
            || !decision.binds(ticket.request.descriptor().sha256(), version)
        {
            return Err(Error::Conflict("approval decision binding differs".into()));
        }
        let approved = decision.approved();
        self.resolve_interaction(
            id,
            InteractionResponse::Approval {
                approved,
                reason: decision.reason,
            },
            responder,
        )
        .await
    }

    /// Publishes the owner's admitted request and committed decision, if any.
    /// Every step is an idempotent append of immutable owner bytes.
    pub(super) async fn publish_approval_files(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let (Some(files), Some((owner, _)), Some(host)) =
            (&self.task_files, &self.owner, &self.interactions)
        else {
            return Ok(());
        };
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let read = host.read(id, io).await?;
        let _read = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        let Some((ticket, resolution)) = &read else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "published approval was not admitted",
            ));
        };
        if ticket.kind != InteractionKind::Approval {
            return Ok(());
        }
        let task = Component::new(owner.task_id().to_string())?;
        let request = Component::new(ticket.id.to_string())?;
        let bytes = host.read_file(&ticket.request, io).await?;
        let _request = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        publish(
            files,
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::ApprovalRequest(request.clone()),
            },
            bytes,
            io,
        )
        .await?;
        let Some((resolution, detail)) = resolution
            .as_ref()
            .and_then(|resolution| Some((resolution, resolution.detail.as_ref()?)))
        else {
            return Ok(());
        };
        let bytes = host.read_file(detail, io).await?;
        let _detail = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let principal = with_history_io(io, |work| {
            ticket.validate_decision_bytes(resolution, None, detail, &bytes, work)
        })?;
        publish(
            files,
            FileKey::Task {
                task,
                file: TaskFile::ApprovalDecision {
                    request,
                    principal: Component::new(principal_hex(&principal))?,
                },
            },
            bytes,
            io,
        )
        .await
    }
}

async fn publish<P: StreamProvider>(
    files: &TaskFiles<P>,
    key: FileKey,
    bytes: acyclic_stream::OwnedBytes,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let value = FileValue {
        key,
        channel: None,
        publication: None,
        bytes,
    };
    #[cfg(not(target_arch = "wasm32"))]
    let work = io.and_then(PreparationIo::original_native_work);
    #[cfg(target_arch = "wasm32")]
    let work = io.and(None);
    Ok(files.publish_owned(&value, work).await?)
}

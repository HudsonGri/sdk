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
use crate::filesystem::interaction_host::{PublishedDecision, approval_submission};
use crate::{
    interaction::InteractionKind,
    task_volume::{Component, FileKey, FileValue, TaskFile},
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
        let (id, decision) = approval_submission(
            owner.task_id(),
            file,
            responder.id(),
            self.maximum_payload_bytes,
        )?;
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
        let publication = self
            .owner
            .as_ref()
            .map(|(owner, _)| self.approval_files().map(|files| (files, owner.task_id())))
            .transpose()?;
        self.interactions()?
            .published_decision(id, binding, publication, io)
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
        let publication = self
            .owner
            .as_ref()
            .map(|(owner, _)| self.approval_files().map(|files| (files, owner.task_id())))
            .transpose()?;
        self.interactions()?
            .published_decision_from(id, binding, publication, aggregate, io)
            .await
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

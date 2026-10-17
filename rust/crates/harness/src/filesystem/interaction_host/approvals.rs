use super::*;
use crate::{
    TaskId,
    interaction::{ApprovalDecisionFile, InteractionKind, principal_hex},
    task_volume::{Component, DurableFile, FileKey, FileValue, TaskFile, TaskFiles},
};

/// Execution's only view of an approval verdict. Its single constructor is
/// [`FilesystemInteractionHost::published_decision`].
pub(in crate::filesystem) struct PublishedDecision {
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
    pub(in crate::filesystem) fn outcome(&self) -> InteractionOutcome {
        if self.approved {
            InteractionOutcome::Approved {}
        } else {
            InteractionOutcome::Declined {}
        }
    }

    pub(in crate::filesystem) fn approved(&self) -> bool {
        self.approved
    }
}

pub(in crate::filesystem) fn approval_submission(
    task_id: TaskId,
    file: &FileValue,
    principal_id: &str,
    maximum_bytes: u64,
) -> Result<(InteractionId, ApprovalDecisionFile)> {
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
        || principal.as_str() != ApprovalDecisionFile::principal_file(principal_id)
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
    let decision = ApprovalDecisionFile::decode(&file.bytes, maximum_bytes)?;
    Ok((id, decision))
}

fn admitted<T>(read: Option<T>, io: Option<&PreparationIo>) -> Result<T> {
    read.ok_or_else(|| {
        interaction_boundary(
            io,
            ErrorCode::Storage,
            "published approval was not admitted",
        )
    })
}

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn published_decision(
        &self,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        publication: Option<(&TaskFiles<P>, TaskId)>,
        io: Option<&PreparationIo>,
    ) -> Result<PublishedDecision> {
        let aggregate = self.aggregate(io).await?;
        self.published_decision_from(id, binding, publication, aggregate, io)
            .await
    }

    pub(in crate::filesystem) async fn published_decision_from<'io>(
        &self,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        publication: Option<(&TaskFiles<P>, TaskId)>,
        aggregate: (StreamAggregate<P>, Option<DecodeReservation<'io>>),
        io: Option<&'io PreparationIo>,
    ) -> Result<PublishedDecision> {
        let _read;
        let (read, receipt, hold) = self.read_durable(&aggregate.0, id, binding, io).await?;
        drop(aggregate);
        _read = hold;
        let (ticket, resolution) = admitted(read, io)?;
        let (Some(resolution), InteractionKind::Approval) = (&resolution, ticket.kind) else {
            return Err(interaction_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval has no committed decision",
            ));
        };
        let detail = resolution.detail.as_ref().ok_or_else(|| {
            interaction_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "approval decision has no decision file",
            )
        })?;
        let request_bytes = if binding.is_some() || publication.is_some() {
            Some(self.read_file(&ticket.request, io).await?)
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
                interaction_boundary(
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
        let bytes = self.read_file(detail, io).await?;
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
            interaction_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval decision awaits durability",
            )
        })?;
        let Some((files, task)) = publication else {
            return Ok(PublishedDecision {
                approved,
                _resolution: resolution,
                _file: None,
            });
        };
        let task = Component::new(task.to_string())?;
        let request = Component::new(ticket.id.to_string())?;
        let request_bytes = request_bytes.ok_or_else(|| {
            interaction_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "approval request is missing",
            )
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
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
                principal: Component::new(principal_hex(&principal))?,
            },
        };
        let proof = files
            .publish_durable(&FileValue::new(key.clone(), bytes), work)
            .await?;
        if proof.key() != &key {
            return Err(Error::Storage("durable proof names another file".into()));
        }
        Ok(PublishedDecision {
            approved,
            _resolution: resolution,
            _file: Some(proof),
        })
    }
}

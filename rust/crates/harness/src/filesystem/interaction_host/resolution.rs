use super::*;
use crate::executor::with_history_io;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P, A, O> InteractionResolver for FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn inspect(
        &self,
        scope: Scope,
        id: InteractionId,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        self.verifier.verify(&scope, &mut None)?;
        let result = self.read(id, None).await?;
        if let Some((ticket, _)) = &result
            && !scope.capabilities().contains(&ticket.viewer_grant())
            && !scope.capabilities().contains(&ticket.responder_grant())
        {
            return Err(Error::Unauthorized(
                "scope lacks interaction view grant".into(),
            ));
        }
        Ok(result)
    }

    async fn resolve_answer(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> Result<ResolutionReceipt> {
        if matches!(response, InteractionResponse::Approval { .. }) {
            return Err(Error::Invalid("approval requires a decision".into()));
        }
        self.resolve_response((operation_id, id, expected_version), &scope, response, None)
            .await
    }

    async fn resolve_approval(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> Result<ResolutionReceipt> {
        self.resolve_response(
            (operation_id, id, expected_version),
            &scope,
            InteractionResponse::Approval { approved, reason },
            None,
        )
        .await
    }
}

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn resolve_latest(
        &self,
        id: InteractionId,
        response: InteractionResponse,
        responder: &Scope,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
    ) -> Result<()> {
        self.verify_responder_scope(id, responder, None)?;
        let (_, prior) = self
            .read(id, None)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        if let Some(previous) = &prior
            && previous.outcome.is_terminal()
        {
            let detail = matches!(
                previous.outcome,
                InteractionOutcome::Approved {} | InteractionOutcome::Declined {}
            );
            let stored = self.read_resolution_file(id, detail, None).await?;
            return if let Some(bytes) = stored
                && crate::contract::json_from_slice::<InteractionResponse>(&bytes)
                    .map_err(|error| Error::Storage(error.to_string()))?
                    == response
            {
                Ok(())
            } else {
                Err(Error::Conflict("interaction was already resolved".into()))
            };
        }
        let version = prior
            .as_ref()
            .map_or(Ok(1), |prior| next_revision(prior.expected_version))?;
        let digest = blake3::hash(format!("interaction:{id}:resolve-{version}").as_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        self.resolve_response(
            (OperationId::from_bytes(bytes), id, version),
            responder,
            response,
            owner,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn resolve_response(
        &self,
        binding: (OperationId, InteractionId, u64),
        scope: &Scope,
        response: InteractionResponse,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
    ) -> Result<ResolutionReceipt> {
        let (operation_id, id, expected_version) = binding;
        let ticket = self.open_resolution(scope, id, expected_version).await?;
        let request = self
            .read_request(id, None)
            .await?
            .ok_or_else(|| Error::Storage("admitted interaction has no request".into()))?;
        request.validate_response(&response, &mut None)?;
        let staged = self
            .stage_answer(id, expected_version, operation_id, &response, None)
            .await?;
        let (outcome, detail) = match response {
            InteractionResponse::Approval { approved: true, .. } => {
                (InteractionOutcome::Approved {}, Some(staged))
            }
            InteractionResponse::Approval {
                approved: false, ..
            } => (InteractionOutcome::Declined {}, Some(staged)),
            _ => (
                InteractionOutcome::Answered {
                    answer: Box::new(staged),
                },
                None,
            ),
        };
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version,
            outcome,
            detail,
        };
        resolution.validate(&ticket)?;
        ResolutionReceipt::from_apply_result(
            self.execute(
                operation_id,
                scope,
                Action::ResolveInteraction { resolution },
                owner,
                None,
            )
            .await?,
        )
    }

    pub(in crate::filesystem) fn verify_responder_scope(
        &self,
        id: InteractionId,
        scope: &Scope,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        with_history_io(io, |work| self.verifier.verify(scope, work))?;
        let grant = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("interaction:respond:{id}"), work)
        })?;
        let hold = io
            .map(|io| io.guard_owned(grant.capacity() as u64))
            .transpose()?;
        let examined = acyclic_fs::WorkCounters {
            items_examined: 2,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(examined)?;
        }
        let allowed = scope
            .capabilities()
            .contains(capability::INTERACTION_RESOLVE)
            && scope.capabilities().contains(&grant);
        if let Some(io) = io {
            io.charge(examined)?;
        }
        drop((grant, hold));
        if !allowed {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "responder lacks its exact grant",
            ));
        }
        Ok(())
    }

    async fn open_resolution(
        &self,
        scope: &Scope,
        id: InteractionId,
        expected_version: u64,
    ) -> Result<InteractionTicket> {
        self.verify_responder_scope(id, scope, None)?;
        let (ticket, prior) = self
            .read(id, None)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        let next = match &prior {
            Some(prior) if prior.outcome.is_terminal() => prior.expected_version,
            Some(prior) => next_revision(prior.expected_version)?,
            None => 1,
        };
        if expected_version != next {
            return Err(Error::Conflict(
                "interaction resolution version mismatch".into(),
            ));
        }
        Ok(ticket)
    }

    pub(in crate::filesystem) async fn verify_approval(
        &self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let aggregate = self.aggregate(io).await?;
        let Some((ticket, Some(resolution))) = aggregate
            .0
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "tool installation lacks a resolved approval",
            ));
        };
        if resolution.outcome != (InteractionOutcome::Approved {})
            || !ticket.approval.as_ref().is_some_and(|binding| {
                binding.operation_id == operation && binding.action_digest == digest
            })
        {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "approval does not authorize this definition",
            ));
        }
        let bytes = self.read_file(&ticket.request, io).await?;
        let hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        super::content::admit_interaction(&bytes, io)?;
        let (request, retained, custody) =
            with_history_io(io, |work| ticket.validate_request_bytes(&bytes, work))?;
        let request_hold = io.map(|io| io.guard_owned(retained)).transpose()?;
        let request = (request, custody, request_hold);
        drop((bytes, hold));
        if let Some(detail) = &resolution.detail {
            let bytes = self.read_file(detail, io).await?;
            let _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            super::content::admit_interaction(&bytes, io)?;
            with_history_io(io, |work| {
                ticket.validate_decision_bytes(
                    &request.0,
                    &resolution.outcome,
                    detail,
                    &bytes,
                    work,
                )
            })?;
        }
        Ok(())
    }
}

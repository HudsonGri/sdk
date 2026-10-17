use super::*;
use crate::{
    IdempotencyKey, InteractionId,
    core::{Action, Command, SchemaRegistry},
    store::StreamAggregate,
};

impl LocalHarness {
    /// Principal authenticated by the standalone local host.
    pub const APPROVAL_PRINCIPAL: &str = "local-user";

    /// Writes a decision as the authenticated user of this local composition.
    pub async fn write_approval_decision(
        &self,
        task: TaskId,
        id: InteractionId,
        file: &crate::task_volume::FileValue,
    ) -> Result<()> {
        let issuer = self.interaction_issuer.as_ref().ok_or_else(|| {
            Error::Unsupported("embedded runtimes supply their own responder authority".into())
        })?;
        let responder = issuer.issue(
            Self::APPROVAL_PRINCIPAL,
            &Capabilities::new([
                crate::contract::capability::INTERACTION_RESOLVE.to_owned(),
                crate::contract::capability::interaction_respond(id),
            ]),
            self.signed.agent(),
            None,
            &mut None,
        )?;
        self.runtime
            .write_approval_decision(task, file, &responder)
            .await
    }
}

pub(super) async fn open(
    stream: &Arc<LocalStream>,
    issuer: &AuthorityIssuer,
    agent: AgentId,
    volume: &VolumeRef,
) -> Result<(AuthorityVerifier, Scope)> {
    let verifier = issuer.verifier(&mut None)?;
    let scope = issuer.issue(
        "interaction-owner",
        &Capabilities::new([
            crate::contract::capability::INTERACTION_OPEN.to_owned(),
            "conversation:bind".to_owned(),
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    let mut aggregate = StreamAggregate::open(
        stream,
        (
            verifier.audience().clone(),
            verifier.copy(&mut None)?,
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    match aggregate
        .reducer()
        .conversation()
        .and_then(|state| state.agent)
    {
        Some(bound) if bound == agent => return Ok((verifier, scope)),
        Some(_) => {
            return Err(Error::Conflict(
                "local interaction conversation belongs to another agent".into(),
            ));
        }
        None => {}
    }
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes(agent.into_bytes()),
                idempotency_key: IdempotencyKey::new("local-interaction-owner")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            None,
            &mut None,
        )
        .await?;
    Ok((verifier, scope))
}

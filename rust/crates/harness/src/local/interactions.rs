use super::*;
use crate::{
    IdempotencyKey,
    core::{Action, Command, SchemaRegistry},
    store::StreamAggregate,
};

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

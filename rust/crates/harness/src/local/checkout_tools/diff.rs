//! The diff baseline of each selected workspace: its head generation when the
//! workspace's first command began. The command records it as a turn effect,
//! so it commits into the agent's state with that turn and is read back from
//! there on reopen; this map is the live copy the diff readers follow.
use super::*;
use crate::agent::{AgentState, DiffBase, TurnEffect, TurnEffects};

/// Call key of the recorded baseline: at most one per turn.
const RECORD: &str = "diff-base";

#[derive(Clone, Copy)]
pub(in crate::local) struct Retained {
    /// `None` when the checkout held a source file the capture could not read.
    pub(in crate::local) generation: Option<acyclic_fs::GenerationId>,
    /// Captured by this process and not yet read back from an agent's state.
    pending: bool,
}

/// Baselines by workspace; published once each, followed by diff readers.
pub(in crate::local) type Baselines =
    tokio::sync::watch::Sender<std::collections::BTreeMap<acyclic_fs::WorkspaceId, Retained>>;

impl CheckoutTools {
    pub(super) async fn retain_diff_base(
        &self,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        unreadable_source: bool,
    ) -> Result<()> {
        let id = workspace.id();
        // Commands on one workspace are serialized by its guard, so only the
        // first one reads the head.
        if self.workspace.diff_bases.borrow().contains_key(&id) {
            return Ok(());
        }
        let generation = if unreadable_source {
            None
        } else {
            Some(
                workspace
                    .head(
                        acyclic_fs::performance::BOUNDARY_BUDGET,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map_err(storage)?
                    .value
                    .id(),
            )
        };
        insert(
            &self.workspace.diff_bases,
            id,
            Retained {
                generation,
                pending: true,
            },
        );
        Ok(())
    }

    /// Records `workspace`'s baseline as an effect of the calling turn while no
    /// agent state holds it yet.
    pub(super) async fn record_diff_base(
        &self,
        context: &ToolContext,
        workspace: acyclic_fs::WorkspaceId,
    ) -> Result<()> {
        let Some(effects) = context.turn_effects() else {
            return Ok(());
        };
        let retained = self.workspace.diff_bases.borrow().get(&workspace).copied();
        match retained {
            Some(retained) if retained.pending => {
                record(&**effects, workspace, retained.generation).await
            }
            _ => Ok(()),
        }
    }
}

fn insert(bases: &Baselines, workspace: acyclic_fs::WorkspaceId, retained: Retained) {
    // A committed baseline is never replaced; a live one only by a committed one.
    bases.send_if_modified(|bases| {
        let keep = bases
            .get(&workspace)
            .is_some_and(|held| !held.pending || retained.pending);
        if !keep {
            bases.insert(workspace, retained);
        }
        !keep
    });
}

async fn record(
    effects: &dyn TurnEffects,
    workspace: acyclic_fs::WorkspaceId,
    generation: Option<acyclic_fs::GenerationId>,
) -> Result<()> {
    let effect = TurnEffect::DiffBase(DiffBase {
        workspace,
        generation,
    });
    effects.record(RECORD, &effect).await.map(drop)
}

/// At the open of `agent`'s turn: reads its committed baseline back, or
/// records a live baseline no agent state holds yet (a capture whose turn was
/// cancelled, or a file write's) into this turn.
pub(in crate::local) async fn open_turn(
    bases: &Baselines,
    agent: &AgentState,
    effects: Option<&dyn TurnEffects>,
) -> Result<()> {
    if let Some(base) = agent.diff_base {
        restore(bases, base);
        return Ok(());
    }
    let pending = bases
        .borrow()
        .iter()
        .find(|(_, retained)| retained.pending)
        .map(|(workspace, retained)| (*workspace, retained.generation));
    match (pending, effects) {
        (Some((workspace, generation)), Some(effects)) => {
            record(effects, workspace, generation).await
        }
        _ => Ok(()),
    }
}

/// Holds `base`, read back from an agent's state, as its workspace's baseline.
pub(in crate::local) fn restore(bases: &Baselines, base: DiffBase) {
    insert(
        bases,
        base.workspace,
        Retained {
            generation: base.generation,
            pending: false,
        },
    );
}

/// Waits for `workspace`'s baseline without capturing or changing it.
pub(in crate::local) async fn baseline(
    bases: &Baselines,
    workspace: acyclic_fs::WorkspaceId,
) -> Result<Retained> {
    let mut bases = bases.subscribe();
    let bases = bases
        .wait_for(|bases| bases.contains_key(&workspace))
        .await
        .map_err(|_| Error::Conflict("diff baseline owner closed".into()))?;
    bases
        .get(&workspace)
        .copied()
        .ok_or_else(|| Error::Conflict("missing diff baseline".into()))
}

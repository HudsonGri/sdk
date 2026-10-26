//! The diff baseline of each selected workspace: its head generation when the
//! workspace's first command began, held by the workspace owner in memory.
use super::*;

#[derive(Clone, Copy)]
pub(in crate::local) enum Baseline {
    Captured(acyclic_fs::GenerationId),
    UnreadableSource,
}

/// Baselines by workspace; published once each, followed by diff readers.
pub(in crate::local) type Baselines =
    tokio::sync::watch::Sender<std::collections::BTreeMap<acyclic_fs::WorkspaceId, Baseline>>;

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
        let baseline = if unreadable_source {
            Baseline::UnreadableSource
        } else {
            let generation = workspace
                .head(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(storage)?
                .value;
            Baseline::Captured(generation.id())
        };
        self.workspace.diff_bases.send_if_modified(|bases| {
            if bases.contains_key(&id) {
                return false;
            }
            bases.insert(id, baseline);
            true
        });
        Ok(())
    }
}

/// Waits for `workspace`'s baseline without capturing or changing it.
pub(in crate::local) async fn baseline(
    bases: &Baselines,
    workspace: acyclic_fs::WorkspaceId,
) -> Result<Baseline> {
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

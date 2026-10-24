use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Lists deleted source versions under a bounded result count.
    pub async fn source_tombstones(
        &self,
        maximum: usize,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<(String, SourceVersion)>, KernelOwnedAllocation), LazyWorkspaceError> {
        let state = self.state(ctx).await?;
        let mut pending = Owned::allocate(1, ctx)?;
        pending.value.push(state.overlay);
        let mut output = Owned::allocate(0, ctx)?;
        let result = async {
            while let Some(id) = pending.value.pop() {
                let mut node = self
                    .store
                    .load_lazy_overlay(id, ctx)
                    .await?
                    .ok_or(LazyWorkspaceError::NotFound)?;
                let prepared = (|| {
                    if let LazyOverlay::Node {
                        left,
                        right,
                        change,
                        ..
                    } = &node.0
                    {
                        for id in [*left, *right] {
                            pending.ensure_for_push(usize::MAX, ctx)?;
                            pending.value.push(id);
                        }
                        if matches!(change, LazyOverlayChange::Tombstone { removed: Some(_) }) {
                            output.ensure_for_push(maximum, ctx)?;
                            output.allocation.absorb(&mut node.1)?;
                        }
                    }
                    Ok::<_, LazyWorkspaceError>(())
                })();
                if let Err(error) = prepared {
                    retire_node(node, ctx)?;
                    return Err(error);
                }
                match node.0 {
                    LazyOverlay::Node {
                        path,
                        change:
                            LazyOverlayChange::Tombstone {
                                removed: Some(version),
                            },
                        ..
                    } => {
                        output.bytes += path.capacity() as u64;
                        output.value.push((path, version));
                    }
                    value => retire_node((value, node.1), ctx)?,
                }
            }
            crate::kernel::sort_by_accounted(&mut output.value, ctx, |a, b| a.0.cmp(&b.0))?;
            Ok::<_, LazyWorkspaceError>(())
        }
        .await;
        pending.retire(ctx)?;
        if let Err(error) = result {
            output.retire(ctx)?;
            return Err(error);
        }
        Ok((output.value, output.allocation))
    }
    /// Reads the live source version without consulting retained observations.
    /// A source epoch may advance, but a replacement source identity cannot
    /// authorize a deletion recorded against this workspace's original source.
    pub async fn current_source_version(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        let source = self.state(ctx).await?.source;
        match self.source_lookup(source, path, ctx).await {
            Ok(node) => Ok(node.map(|n| n.version)),
            Err(LazyWorkspaceError::Demand(DemandError::Absent | DemandError::NotDirectory)) => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Checks the version and authored-view fences for carrying a source deletion.
    ///
    /// The merge coordinator must separately reject paths changed by the parent
    /// or its ancestors and publish under its original generation fence. This
    /// check creates no authored files, observations or tombstones, reads no
    /// source content and performs no directory scans. Source epochs follow the
    /// shared binding fence.
    /// Absence is established only by `NotFound`; denied or failed observations
    /// never authorize deletion. All observations use the original accounting.
    pub async fn source_deletion_is_unmodified<
        PD: DemandSource + 'static,
        PS: LazyWorkspaceStore,
    >(
        &self,
        path: &str,
        deleted: SourceVersion,
        child_head: &crate::Generation<A, O>,
        parent: Option<&LazyWorkspace<A, O, PD, PS>>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        if child_head.workspace_id() != self.workspace.id() {
            return Err(WorkspaceError::ForeignGeneration.into());
        }
        match accounted!(ctx, |work, cancel| child_head.stat(path, work, cancel)) {
            Ok(_) => return Ok(false),
            Err(LazyWorkspaceError::WorkspaceCause(WorkspaceError::NotFound)) => {}
            Err(error) => return Err(error),
        }
        if self.current_source_version(path, ctx).await? != Some(deleted) {
            return Ok(false);
        }
        let Some(parent) = parent else {
            return Ok(true);
        };
        let lookup = match parent.inspect(path, ctx).await {
            Ok(lookup) => lookup,
            Err(LazyWorkspaceError::NotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        let unmodified = matches!(
            &lookup.0,
            LazyLookup::Source { node, .. } if node.version == deleted
        );
        parent.retire_lookup(lookup, ctx)?;
        Ok(unmodified)
    }
    pub(super) async fn removed_source_version(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        match self.overlay_fact(state.overlay, path, ctx).await? {
            Some(LazyOverlayChange::Observe { node, .. }) => Ok(Some(node.version)),
            Some(LazyOverlayChange::Tombstone { removed }) => Ok(removed),
            None => match self.source_lookup(state.source, path, ctx).await {
                Ok(node) => Ok(node.map(|n| n.version)),
                Err(LazyWorkspaceError::Demand(
                    DemandError::Absent | DemandError::NotDirectory,
                )) => Ok(None),
                Err(error) => Err(error),
            },
        }
    }
}

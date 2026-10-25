use super::*;

pub(crate) async fn prepare_writeback(
    root: &PiWritebackRoot,
    generations: &PiWritebackGenerations<'_>,
    state: &acyclic_fs::LocalCoreStateStore,
    options: &acyclic_fs::MaterializeOptions,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::MaterializationJournal> {
    root.verify(native_work).await?;
    let from = &generations.from;
    let candidate = generations.candidate();
    if generations.original_snapshot.workspace_id != root.workspace {
        return Err(selected_unauthorized(
            "writeback candidate is not the original selected root workspace",
        ));
    }
    let journal = root
        .publication
        .prepare_selected(
            from,
            candidate,
            state,
            options,
            native_work,
            (generations.paths, writeback_context(root, generations)),
        )
        .await?;
    root.verify(native_work).await?;
    Ok(journal)
}

/// Publishes only the exact user-confirmed preparation.
pub(crate) fn publish_writeback<'a>(
    root: &'a PiWritebackRoot,
    generations: &'a PiWritebackGenerations<'a>,
    journal: &'a acyclic_fs::MaterializationJournal,
    options: &'a acyclic_fs::MaterializeOptions,
    state: &'a acyclic_fs::LocalCoreStateStore,
    approval: (
        &'a str,
        &'a dyn acyclic_harness::tool::ToolApprovalVerifier,
        acyclic_harness::InteractionId,
    ),
    native_work: &'a acyclic_fs::NativeMaterializationWork,
) -> impl std::future::Future<Output = Result<acyclic_fs::MaterializationJournal>> + Send + 'a {
    async move {
        root.verify(native_work).await?;
        if generations.original_snapshot.workspace_id != root.workspace {
            return Err(selected_unauthorized(
                "writeback lacks exact original-root user approval",
            ));
        }
        let confirmation = crate::pi_operator::parse_digest(approval.0)?;
        writeback_digest_value(root, generations, journal)?
            .authorize(approval.1, approval.2, &confirmation, native_work)
            .await?
            .publish(state, options, || root.verify(native_work))
            .await
    }
}

/// A replay can acknowledge only the SDK's authenticated completed original journal, with the same exact user digest.
pub(crate) async fn validate_applied_writeback(
    root: &PiWritebackRoot,
    generations: &PiWritebackGenerations<'_>,
    journal: &acyclic_fs::MaterializationJournal,
    approval: (
        &str,
        &dyn acyclic_harness::tool::ToolApprovalVerifier,
        acyclic_harness::InteractionId,
    ),
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    if journal.phase != acyclic_fs::MaterializationPhase::Applied
        || generations.original_snapshot.workspace_id != root.workspace
    {
        return Err(selected_unauthorized(
            "completed writeback differs from its original root and generations",
        ));
    }
    writeback_digest_value(root, generations, journal)?
        .authorize(
            approval.1,
            approval.2,
            &crate::pi_operator::parse_digest(approval.0)?,
            native_work,
        )
        .await?;
    Ok(())
}

pub(crate) fn writeback_digest_value<'a>(
    root: &'a PiWritebackRoot,
    generations: &'a PiWritebackGenerations<'_>,
    journal: &'a acyclic_fs::MaterializationJournal,
) -> Result<acyclic_harness::filesystem::CheckoutApproval<'a, impl Serialize + 'a>> {
    root.publication.approval_value(
        &generations.from,
        generations.candidate(),
        generations.paths,
        journal,
        writeback_context(root, generations),
    )
}

fn writeback_context<'a>(
    root: &'a PiWritebackRoot,
    generations: &'a PiWritebackGenerations<'_>,
) -> impl Serialize + 'a {
    (
        root.task,
        &root.volume,
        root.workspace,
        generations.original_snapshot,
        generations.imported,
    )
}

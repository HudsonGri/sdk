use super::*;
use crate::{contract::capability, filesystem::PreparationIo};

#[tokio::test]
async fn parent_operations_recheck_retained_evidence_without_reauthenticating() -> Result<()> {
    let fixture = fixture().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let owned_before = io.owned_bytes()?;
    let controller = ParentProjectController::new(
        &fixture.host,
        fixture.parent.reducer(),
        &fixture.issuer.verifier(&mut None)?,
        &fixture.scope,
        &fixture.project,
        Some(&io),
    )?;
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            for io in [None, Some(&io)] {
                controller.require(capability::FORK_PUBLISH, VolumeOperation::Read, io)?;
                controller.require(capability::PROJECT_MERGE, VolumeOperation::Write, io)?;
                assert!(
                    controller
                        .require("not-granted", VolumeOperation::Read, io)
                        .is_err()
                );
            }
            Ok(())
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "parent controller reauthenticated immutable scope evidence"
    );
    let foreign = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    assert!(
        controller
            .require(
                capability::FORK_PUBLISH,
                VolumeOperation::Read,
                Some(&PreparationIo::from_native_work(&foreign)?)
            )
            .is_err()
    );
    let before = work.receipt().allocation_operations;
    cancellation.cancel();
    assert!(
        controller
            .require(capability::FORK_PUBLISH, VolumeOperation::Read, Some(&io))
            .is_err()
    );
    assert_eq!(work.receipt().allocation_operations, before);
    drop(controller);
    assert_eq!(io.owned_bytes()?, owned_before);
    Ok(())
}

#[tokio::test]
async fn parent_revocation_rejects_pending_fork_import_and_merge() -> Result<()> {
    for native in [false, true] {
        revoked_parent(native).await?;
    }
    Ok(())
}

async fn revoked_parent(native: bool) -> Result<()> {
    let fixture = fixture().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = native
        .then(|| {
            acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
        })
        .transpose()
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = original
        .as_ref()
        .map(PreparationIo::from_native_work)
        .transpose()?;
    let controller = ParentProjectController::new(
        &fixture.host,
        fixture.parent.reducer(),
        &fixture.issuer.verifier(&mut None)?,
        &fixture.scope,
        &fixture.project,
        io.as_ref(),
    )?;
    let adapter = crate::filesystem::FilesystemProjectWorkspaces::new(
        &fixture.host,
        fixture.parent.reducer(),
        &fixture.issuer.verifier(&mut None)?,
        &fixture.scope,
        fixture.project.clone(),
    )?;
    let cloned_adapter = adapter.clone();
    let plan = controller
        .prepare_project_import(&fixture.source, &fixture.source_generation, None)
        .await?;
    let key = IdempotencyKey::new("revoked-fork")?;
    let fork = controller.fork_project(&fixture.source_generation, &fixture.source, &key);
    let import =
        controller.prepare_project_import(&fixture.source, &fixture.source_generation, None);
    let merge =
        controller.apply_project_merge(&plan, OperationId::from_bytes([93; 16]), BTreeMap::new());
    let adapter_fork = crate::merge::ProjectWorkspaceProvider::fork_project(
        &adapter,
        &fixture.scope,
        &fixture.source_generation,
        &fixture.source,
        &key,
    );
    let cloned_fork = crate::merge::ProjectWorkspaceProvider::fork_project(
        &cloned_adapter,
        &fixture.scope,
        &fixture.source_generation,
        &fixture.source,
        &key,
    );
    fixture.issuer.revoke_workload(&fixture.scope)?;
    assert!(matches!(
        controller.require(capability::FORK_PUBLISH, VolumeOperation::Read, None),
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(fork.await, Err(Error::Unauthorized(_))));
    assert!(matches!(import.await, Err(Error::Unauthorized(_))));
    assert!(matches!(merge.await, Err(Error::Unauthorized(_))));
    assert!(matches!(adapter_fork.await, Err(Error::Unauthorized(_))));
    assert!(matches!(cloned_fork.await, Err(Error::Unauthorized(_))));
    Ok(())
}

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

#[tokio::test]
async fn native_publication_owner_releases_every_partial_allocation() -> Result<()> {
    let fixture = fixture().await?;
    check_metadata_cuts(&fixture, true)
}

#[tokio::test]
async fn native_merge_metadata_releases_every_partial_allocation() -> Result<()> {
    let fixture = fixture().await?;
    check_metadata_cuts(&fixture, false)
}

fn check_metadata_cuts(fixture: &Fixture, publication: bool) -> Result<()> {
    let verifier = fixture.issuer.verifier(&mut None)?;
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let cancellation = acyclic_fs::CancellationToken::new();
    let make_io = |budget| -> Result<PreparationIo> {
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        PreparationIo::from_native_work(&work)
    };
    let construct =
        |parent: &ParentProjectController<'_, MemoryAuthorityBackend, MemoryObjectBackend>,
         io: &PreparationIo|
         -> Result<()> {
            if publication {
                drop(parent.publication_authority(io)?);
            } else {
                drop(parent.merge_metadata(&fixture.source, Some(io))?);
            }
            Ok(())
        };
    let probe = make_io(budget)?;
    let parent = ParentProjectController::new(
        &fixture.host,
        fixture.parent.reducer(),
        &verifier,
        &fixture.scope,
        &fixture.project,
        Some(&probe),
    )?;
    let setup = budget.allocation_operations - probe.remaining()?.allocation_operations;
    let owned = probe.owned_bytes()?;
    construct(&parent, &probe)?;
    assert_eq!(probe.owned_bytes()?, owned);
    let required = budget.allocation_operations - setup - probe.remaining()?.allocation_operations;
    assert!(
        required > 1,
        "publication owner must reach partial metadata copies"
    );
    drop(parent);
    let mut leaks = Vec::new();
    for cut in 0..required {
        let mut limited = budget;
        limited.allocation_operations = setup + cut;
        let io = make_io(limited)?;
        io.retain_owned(17)?;
        let parent = ParentProjectController::new(
            &fixture.host,
            fixture.parent.reducer(),
            &verifier,
            &fixture.scope,
            &fixture.project,
            Some(&io),
        )?;
        let owned = io.owned_bytes()?;
        let result = construct(&parent, &io);
        assert!(
            result.is_err(),
            "publication allocation cut {cut}/{required}"
        );
        drop(result);
        let after = io.owned_bytes()?;
        if after != owned {
            leaks.push((cut, owned, after));
        }
        drop(parent);
        io.release(17)?;
    }
    assert!(
        leaks.is_empty(),
        "native parent metadata charges leaked (publication={publication}): {leaks:?}"
    );
    Ok(())
}

#[tokio::test]
async fn native_publication_owner_rechecks_trust_cancellation_and_original_custody() -> Result<()> {
    for cancel in [false, true] {
        let fixture = fixture().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let parent = ParentProjectController::new(
            &fixture.host,
            fixture.parent.reducer(),
            &fixture.issuer.verifier(&mut None)?,
            &fixture.scope,
            &fixture.project,
            Some(&io),
        )?;
        let owner = parent.publication_authority(&io)?;
        let (result, signatures) =
            acyclic_test_support::differential::observe(|| (), |()| owner.require(&work));
        result?;
        let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
        assert!(
            signatures.is_empty(),
            "publication guard reauthenticated immutable evidence"
        );
        let foreign = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(owner.require(&foreign).is_err());
        if cancel {
            cancellation.cancel();
        } else {
            fixture.issuer.revoke_workload(&fixture.scope)?;
        }
        let before = work.receipt().allocation_operations;
        assert!(owner.require(&work).is_err());
        assert_eq!(work.receipt().allocation_operations, before);
    }
    Ok(())
}

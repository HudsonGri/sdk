use super::*;
use crate::durable_host::tests::created_fixture::CreationFixture;
use acyclic_test_support::differential::{self, Operation};

fn binding_count(trace: Vec<Operation>) -> usize {
    trace
        .into_iter()
        .filter(|(name, _)| *name == "created request binding")
        .count()
}

fn compare_binding<'a>(
    runtime: &tokio::runtime::Runtime,
    fixture: &CreationFixture,
    request: &'a ResourcePreparationRequest,
    proof: &'a ResourceCreationProofRef,
    io: &PreparationIo,
    owner: &Authority,
) -> Result<CreationRequest<'a>> {
    let (bound, trace) = differential::observe::<_, _, Operation>(
        || (),
        |_| CreationRequest::new(&fixture.filesystem, request, proof, io),
    );
    let bound = bound?;
    assert_eq!(binding_count(trace), 1);
    let (outcomes, trace) = differential::observe::<_, _, Operation>(
        || (),
        |_| {
            runtime.block_on(async {
                let mut volumes = Vec::new();
                for _ in 0..2 {
                    volumes.push(
                        bound
                            .authenticate(&fixture.filesystem, owner, Some(io))
                            .await?
                            .volume,
                    );
                }
                Result::Ok(volumes)
            })
        },
    );
    let outcomes = outcomes?;
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(
        binding_count(trace),
        0,
        "reads must consume the original binding"
    );
    let (old, trace) = differential::observe::<_, _, Operation>(
        || (),
        |_| {
            runtime.block_on(async {
                let repeated = CreationRequest::new(&fixture.filesystem, request, proof, io)?;
                repeated
                    .authenticate(&fixture.filesystem, owner, Some(io))
                    .await
                    .map(|v| v.volume)
            })
        },
    );
    assert_eq!(old?, outcomes[0]);
    assert_eq!(
        binding_count(trace),
        1,
        "negative control reconstructs the immutable input"
    );

    Ok(bound)
}

fn retained_creation_request(native: bool) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let fixture = runtime.block_on(CreationFixture::new())?;
    let created = runtime.block_on(fixture.create_filesystem_child())?;
    let request = &created.request;
    let proof = request
        .child
        .resource_proofs
        .first()
        .ok_or_else(|| Error::Invalid("fixture has no creation proof".into()))?;
    let cancel = CancellationToken::new();
    let work = native
        .then(|| {
            acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024),
                &cancel,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
        })
        .transpose()?;
    let io = match &work {
        Some(work) => PreparationIo::from_native_work(work)?,
        None => PreparationIo::new(request, &cancel)?,
    };
    let owner = &Authority {
        kind: crate::core::AggregateKind::Task,
        id: "created-owner".into(),
    };
    let bound = compare_binding(&runtime, &fixture, request, proof, &io, owner)?;
    let mut foreign = (*owner).clone();
    foreign.id.push_str("-foreign");
    assert!(matches!(
        runtime.block_on(bound.authenticate(&fixture.filesystem, &foreign, Some(&io))),
        Err(Error::Unauthorized(_))
    ));
    let other = FilesystemHost::new(acyclic_fs::Fs::memory(), &fixture.filesystem_provider, None)?;
    assert!(
        matches!(
            runtime.block_on(bound.authenticate(&other, owner, Some(&io))),
            Err(Error::Unauthorized(_))
        ),
        "retained parsing must never stand in for the protected journal"
    );
    let mut forged = request.clone();
    forged.child.operation_id = OperationId::from_bytes([7; 16]);
    let forged = CreationRequest::new(&fixture.filesystem, &forged, proof, &io)?;
    assert!(matches!(
        runtime.block_on(forged.authenticate(&fixture.filesystem, owner, Some(&io))),
        Err(Error::Unauthorized(_))
    ));
    let mut forged = proof.clone();
    forged.evidence = forged.resource.clone();
    assert!(matches!(
        CreationRequest::new(&fixture.filesystem, request, &forged, &io),
        Err(Error::Unauthorized(_))
    ));
    io.remaining()?;
    cancel.cancel();
    assert!(
        runtime
            .block_on(bound.authenticate(&fixture.filesystem, owner, Some(&io)))
            .is_err()
    );
    Ok(())
}

#[test]
fn created_request_has_one_owner_and_rechecks_live_evidence() -> Result<()> {
    retained_creation_request(false)
}

#[test]
fn native_created_request_has_one_owner_and_rechecks_live_evidence() -> Result<()> {
    retained_creation_request(true)
}

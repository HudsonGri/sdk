use super::*;
use crate::access::{Action, Capability, Context, Generations, Grant, PathTrie, Policy,
    Principal, PrincipalKind, Resource, ResourcePattern};

struct Reload {
    source: std::sync::Weak<MemorySource>,
}

impl crate::native_mount::NativeMountCallbackGuard for Reload {
    fn check<'a>(&'a self, work: &'a crate::NativeMaterializationWork)
    -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        crate::native_mount::NativeMountCallbackFuture::new(work, || async move {
            let source = self.source.upgrade().ok_or(MountSourceError::Stale)?;
            let mut checkout = source.checkout.volume.checkout(GenerationSelector::Head,
                CheckoutMode::read_only_pinned(), work, work.cancellation()).await.map_err(MountSourceError::from)?;
            let (bytes, revision) = checkout.read_access_policy(work, work.cancellation()).await.map_err(MountSourceError::from)?;
            let policy: Policy = serde_json::from_slice(&bytes.ok_or(MountSourceError::AccessDenied)?.bytes)
                .map_err(|_| MountSourceError::AccessDenied)?;
            let volume = uuid::Uuid::from_bytes(source.volume_id()?.into_bytes()).to_string();
            let principal = Principal { kind: PrincipalKind::Service, id: "mount-test".into(),
                lineage: vec![], groups: Default::default(), roles: Default::default(),
                tags: Default::default(), attributes: Default::default() };
            let capability = Capability { layers: vec![vec![Grant {
                actions: Action::ALL.into_iter().collect(), resource: ResourcePattern {
                    lineage_root: None, volume: None, generations: Generations::All, path: "**".into(),
                },
            }]], expires_at: None };
            let view = PathTrie::compile(&policy, &principal, Resource {
                config: source.config, lineage: &[], volume: &volume, generation: 1, head: 1, path: "",
            }, Context { now: access::access_time()?, approvals: 0 }, &capability)
                .map_err(|_| MountSourceError::AccessDenied)?
                .with_policy_revision(checkout.generation_id(), revision);
            source.access_view.as_ref().ok_or(MountSourceError::AccessDenied)?
                .publish(view).map_err(|_| MountSourceError::AccessDenied)?;
            Ok(())
        })
    }

    fn check_write<'a>(&'a self, work: &'a crate::NativeMaterializationWork)
    -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        self.check(work)
    }
}

fn publish(source: &MemorySource, policy: &Policy) -> Result<(), Box<dyn std::error::Error>> {
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    invoke(&source.runtime, &work, || async {
        let mut checkout = source.checkout.volume.checkout(GenerationSelector::Head,
            CheckoutMode::tracking_transaction(), &work, work.cancellation()).await?;
        checkout.write_access_policy(&serde_json::to_vec(policy).unwrap(),
            crate::NamedAttributeWriteMode::Upsert, &work, work.cancellation()).await?;
        checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted,
            BOUNDARY_BUDGET, work.cancellation(), |_| async {
                Ok::<_, crate::OperationFailure<FsError>>(crate::OperationReceipt {
                    value: (), work: crate::WorkCounters::UNCHARGED,
                })
            }).await.map_err(|failure| MountSourceError::from(failure.error))?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn cached_denial_observes_owner_policy_publication_before_host_probes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let denied = crate::access::fixtures::denied(Action::Read, "file");
    publish(&source, &denied)?;
    let source = bind_test_access(source, vec![deny("restricted", &[Action::Read], "file")], None)?;
    let source = Arc::new_cyclic(|weak| source.with_access_refresh(Arc::new(Reload { source: weak.clone() })));
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("file");
    assert!(source.require_paths(&[(Action::Read, &path)], &work).is_err());
    let mut allowed: Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    allowed.version = denied.version + 1;
    publish(&source, &allowed)?;
    source.require_paths(&[(Action::Read, &path)], &work)?;
    assert!(source.access_view.as_ref().unwrap().current()?.decide(Action::Read, "file", access::access_time()?).allowed);
    Ok(())
}

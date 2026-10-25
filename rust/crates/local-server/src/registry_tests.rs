//! An artifact supplies one identity at admission, never two mutable samples.
use super::*;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Default)]
struct ChangingIdentity(AtomicU8);
impl ChangingIdentity {
    fn next(&self) -> [u8; 32] {
        [self.0.fetch_add(1, Ordering::Relaxed); 32]
    }
}
impl Package for ChangingIdentity {
    fn digest(&self) -> [u8; 32] {
        self.next()
    }
    fn deploy(&self, _: Scope) -> BoxProviderFuture<'_, Result<Instance, Status>> {
        Box::pin(async { Err(Status::unimplemented("not deployed")) })
    }
}
impl Worker for ChangingIdentity {
    fn digest(&self) -> [u8; 32] {
        self.next()
    }
    fn fetch(
        &self,
        _: Scope,
        _: wire::InvokeWorkerRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>> {
        Box::pin(async { Err(Status::unimplemented("not invoked")) })
    }
}

#[test]
fn package_registration_pins_the_checked_digest_and_preserves_its_original_artifact() {
    let mut registry = Registry::default();
    let first: Arc<dyn Package> = Arc::new(ChangingIdentity::default());
    registry.package(first.clone()).unwrap();
    let replacement = Arc::new(ChangingIdentity::default());
    assert_eq!(
        registry.package(replacement).unwrap_err().code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(registry.packages.len(), 1);
    assert!(Arc::ptr_eq(
        registry.packages.get(&[0; 32]).unwrap(),
        &first
    ));
}

#[test]
fn worker_registration_pins_the_checked_digest_and_preserves_its_original_artifact() {
    let mut registry = Registry::default();
    let first: Arc<dyn Worker> = Arc::new(ChangingIdentity::default());
    registry.worker(first.clone()).unwrap();
    let replacement = Arc::new(ChangingIdentity::default());
    assert_eq!(
        registry.worker(replacement).unwrap_err().code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(registry.workers.len(), 1);
    assert!(Arc::ptr_eq(registry.workers.get(&[0; 32]).unwrap(), &first));
}

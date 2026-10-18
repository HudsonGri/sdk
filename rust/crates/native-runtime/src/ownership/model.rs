use super::*;
use crate::sync::Mutex;
use loom::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Cleanup(Arc<AtomicBool>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
        loom::thread::yield_now();
    }
}

fn check(reversed: bool) {
    loom::model(move || {
        // One lock: reopening must wait until the former owner's cleanup ends.
        let fence = Arc::new(Mutex::new(()));
        let exists = Arc::new(AtomicBool::new(true));
        let guard = fence.lock().unwrap();
        let owner = OwnershipAnchor::new(Cleanup(exists.clone()));
        let other_fence = fence.clone();
        let other_exists = exists.clone();
        let opener = loom::thread::spawn(move || {
            let _guard = other_fence.lock().unwrap();
            other_exists.store(true, Ordering::SeqCst);
        });
        if reversed {
            // Former tuple order: a waiting opener can publish before cleanup.
            drop((guard, owner));
        } else {
            // The exact production drop implementation, with the real shim lock.
            let shared = Arc::new(SharedOwner::new(owner));
            let retained = shared.clone();
            shared.retain(guard);
            drop(shared);
            loom::thread::yield_now();
            drop(retained);
        }
        opener.join().unwrap();
        assert!(
            exists.load(Ordering::SeqCst),
            "cleanup deleted a reopened store"
        );
    });
}

#[test]
fn cleanup_precedes_reopen_under_every_interleaving() {
    check(false);
}

#[test]
#[should_panic(expected = "cleanup deleted a reopened store")]
fn reversed_release_order_is_detected() {
    check(true);
}

struct Fence(Arc<AtomicBool>);
impl Drop for Fence {
    fn drop(&mut self) {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "fence released before shared cleanup"
        );
    }
}

fn registration(discard: bool) {
    loom::model(move || {
        let exists = Arc::new(AtomicBool::new(true));
        let owner = Arc::new(SharedOwner::new(OwnershipAnchor::new(Cleanup(
            exists.clone(),
        ))));
        let first = owner.clone();
        let second = owner.clone();
        let first_fence = Fence(exists.clone());
        let second_fence = Fence(exists.clone());
        let a = loom::thread::spawn(move || {
            if discard {
                drop(first_fence);
            } else {
                first.retain(first_fence);
            }
        });
        let b = loom::thread::spawn(move || {
            second.retain(second_fence);
        });
        drop(owner);
        a.join().unwrap();
        b.join().unwrap();
        assert!(!exists.load(Ordering::SeqCst));
    });
}

#[test]
fn concurrent_fence_registration_survives_last_store_release() {
    registration(false);
}

#[test]
#[should_panic(expected = "fence released before shared cleanup")]
fn discarded_fence_is_detected() {
    registration(true);
}

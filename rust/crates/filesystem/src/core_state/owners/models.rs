use super::*;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn race<const FENCE: bool>() {
    let registry = Arc::new(Owners::new(BTreeMap::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let peers: Vec<_> = (0..2)
        .map(|_| {
            let registry = registry.clone();
            let created = created.clone();
            loom::thread::spawn(move || {
                join::<_, FENCE, true>(&registry, "namespace".into(), [1; 16], true, || {
                    let id = created.fetch_add(1, Ordering::SeqCst);
                    loom::thread::yield_now();
                    Ok(Arc::new(id))
                })
                .unwrap()
            })
        })
        .collect();
    let mut owners = Vec::new();
    for peer in peers {
        match peer.join() {
            Ok(owner) => owners.push(owner),
            Err(error) => std::panic::resume_unwind(error),
        }
    }
    assert_eq!(
        created.load(Ordering::SeqCst),
        1,
        "owner initialization escaped registry lock"
    );
    assert!(Arc::ptr_eq(
        owners[0].0.as_ref().unwrap(),
        owners[1].0.as_ref().unwrap()
    ));
    let reopened = join::<_, FENCE, true>(&registry, "namespace".into(), [1; 16], true, || {
        panic!("reopen ignored a live owner")
    })
    .unwrap();
    assert!(Arc::ptr_eq(
        owners[0].0.as_ref().unwrap(),
        reopened.0.as_ref().unwrap()
    ));
    drop((owners, reopened));
    join::<_, FENCE, true>(&registry, "namespace".into(), [2; 16], true, || {
        created.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(2))
    })
    .unwrap();
    assert_eq!(created.load(Ordering::SeqCst), 2);
}
#[test]
fn competing_joins_and_reopen_share_the_live_owner() {
    loom::model(race::<true>);
}
#[test]
#[should_panic(expected = "owner initialization escaped registry lock")]
fn negative_control_detects_initialization_outside_the_registry_lock() {
    loom::model(race::<false>);
}

#[test]
fn concurrent_reopen_preserves_the_joined_owner() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let created = Arc::new(AtomicUsize::new(0));
        let first = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            Ok(Arc::new(created.fetch_add(1, Ordering::SeqCst)))
        })
        .unwrap();
        let release = loom::thread::spawn(move || {
            loom::thread::yield_now();
            drop(first);
        });
        let opener = {
            let registry = registry.clone();
            let created = created.clone();
            loom::thread::spawn(move || {
                join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
                    loom::thread::yield_now();
                    Ok(Arc::new(created.fetch_add(1, Ordering::SeqCst)))
                })
                .unwrap()
            })
        };
        let current = opener.join().unwrap();
        release.join().unwrap();
        let again = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            panic!("lost the owner retained by the reopening caller")
        })
        .unwrap();
        assert!(Arc::ptr_eq(
            current.0.as_ref().unwrap(),
            again.0.as_ref().unwrap()
        ));
        assert!(matches!(created.load(Ordering::SeqCst), 1 | 2));
    });
}

struct NamespaceLock {
    locked: Arc<AtomicBool>,
    tearing_down: Arc<AtomicBool>,
}

impl Drop for NamespaceLock {
    fn drop(&mut self) {
        // The namespace is retiring its final owner inside the lifecycle gate.
        self.tearing_down.store(true, Ordering::SeqCst);
        loom::thread::yield_now();
        self.locked.store(false, Ordering::SeqCst);
    }
}

fn release_race<const FENCE: bool>() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let locked = Arc::new(AtomicBool::new(false));
        let tearing_down = Arc::new(AtomicBool::new(false));
        let overlap = AtomicBool::new(false);
        let create = || {
            overlap.fetch_or(locked.swap(true, Ordering::SeqCst), Ordering::SeqCst);
            Ok(Arc::new(NamespaceLock {
                locked: locked.clone(),
                tearing_down: tearing_down.clone(),
            }))
        };
        let first =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        let releasing = {
            loom::thread::spawn(move || {
                let mut first = first;
                release::<_, FENCE, true>(&first.1, &mut first.0);
            })
        };
        // Force the final-strong-count window rather than letting reopen retain
        // the first owner before its destructor starts. With the fence, reopen
        // must wait for teardown; without it, loom must find an overlapping lock.
        while !tearing_down.load(Ordering::SeqCst) {
            loom::thread::yield_now();
        }
        let current =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        releasing.join().unwrap();
        assert!(
            !overlap.load(Ordering::SeqCst),
            "owner teardown raced reopen"
        );
        let again = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            panic!("reopen lost its owner")
        })
        .unwrap();
        assert!(Arc::ptr_eq(
            current.0.as_ref().unwrap(),
            again.0.as_ref().unwrap()
        ));
        drop(current);
        assert!(locked.load(Ordering::SeqCst));
        drop(again);
        assert!(!locked.load(Ordering::SeqCst));
    });
}

#[test]
fn last_release_and_reopen_share_the_registry_fence() {
    release_race::<true>();
}

#[test]
#[should_panic(expected = "owner teardown raced reopen")]
fn negative_control_detects_last_release_outside_the_registry_fence() {
    release_race::<false>();
}

fn nonfinal_locks<const FAST: bool>() {
    loom::model(|| {
        let registry = Owners::new(BTreeMap::new());
        let first = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            Ok(Arc::new(1))
        })
        .unwrap();
        let mut second = first.clone();
        let before = first.1.locks.load(Ordering::Relaxed);
        release::<_, true, FAST>(&second.1, &mut second.0);
        assert_eq!(
            first.1.locks.load(Ordering::Relaxed) - before,
            0,
            "nonfinal release acquired a namespace lock"
        );
    });
}

#[test]
fn nonfinal_release_is_atomic_and_lock_free() {
    nonfinal_locks::<true>();
}

#[test]
#[should_panic(expected = "nonfinal release acquired a namespace lock")]
fn negative_control_detects_locking_nonfinal_releases() {
    nonfinal_locks::<false>();
}

#[test]
fn simultaneous_nonfinal_releases_cannot_bypass_teardown() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let locked = Arc::new(AtomicBool::new(false));
        let create = || {
            assert!(
                !locked.swap(true, Ordering::SeqCst),
                "owner teardown raced reopen"
            );
            Ok(Arc::new(NamespaceLock {
                locked: locked.clone(),
                tearing_down: Arc::new(AtomicBool::new(false)),
            }))
        };
        let first =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        let second = first.clone();
        let peer = loom::thread::spawn(move || drop(first));
        drop(second);
        let current =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        peer.join().unwrap();
        assert!(locked.load(Ordering::SeqCst));
        drop(current);
        assert!(!locked.load(Ordering::SeqCst));
    });
}

fn exclusive_admission<const RETAIN: bool>() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let exclusive =
            join::<_, true, RETAIN>(&registry, "namespace".into(), [1; 16], false, || {
                Ok(Arc::new(1))
            })
            .unwrap();
        let joining = {
            let registry = registry.clone();
            loom::thread::spawn(move || {
                join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
                    panic!("created over a live exclusive admission")
                })
            })
        };
        assert!(
            matches!(
                joining.join().unwrap(),
                Err(LocalCoreStateStoreError::OwnershipConflict(_))
            ),
            "exclusive admission became shareable"
        );
        drop(exclusive);
        join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            Ok(Arc::new(2))
        })
        .unwrap();
    });
}

#[test]
fn exclusive_admissions_cannot_be_joined() {
    exclusive_admission::<true>();
}

#[test]
#[should_panic(expected = "exclusive admission became shareable")]
fn negative_control_detects_erasing_exclusive_admission() {
    exclusive_admission::<false>();
}

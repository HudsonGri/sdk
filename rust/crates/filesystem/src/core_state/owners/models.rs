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
                join::<_, FENCE>(&registry, "namespace".into(), [1; 16], || {
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
    assert!(Arc::ptr_eq(&owners[0], &owners[1]));
    let reopened = join::<_, FENCE>(&registry, "namespace".into(), [1; 16], || {
        panic!("reopen ignored a live owner")
    })
    .unwrap();
    assert!(Arc::ptr_eq(&owners[0], &reopened));
    drop((owners, reopened));
    join::<_, FENCE>(&registry, "namespace".into(), [2; 16], || {
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
        let first = join::<_, true>(&registry, "namespace".into(), [1; 16], || {
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
                join::<_, true>(&registry, "namespace".into(), [1; 16], || {
                    loom::thread::yield_now();
                    Ok(Arc::new(created.fetch_add(1, Ordering::SeqCst)))
                })
                .unwrap()
            })
        };
        let current = opener.join().unwrap();
        release.join().unwrap();
        let again = join::<_, true>(&registry, "namespace".into(), [1; 16], || {
            panic!("lost the owner retained by the reopening caller")
        })
        .unwrap();
        assert!(Arc::ptr_eq(&current, &again));
        assert!(matches!(created.load(Ordering::SeqCst), 1 | 2));
    });
}

struct NamespaceLock {
    locked: Arc<AtomicBool>,
    tearing_down: Arc<AtomicBool>,
}

impl Drop for NamespaceLock {
    fn drop(&mut self) {
        // Arc's strong count is already zero when native teardown starts.
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
        let first = join::<_, true>(&registry, "namespace".into(), [1; 16], create).unwrap();
        let releasing = {
            let registry = registry.clone();
            loom::thread::spawn(move || release::<_, FENCE>(&registry, &mut Some(first)))
        };
        // Force the final-strong-count window rather than letting reopen retain
        // the first owner before its destructor starts. With the fence, reopen
        // must wait for teardown; without it, loom must find an overlapping lock.
        while !tearing_down.load(Ordering::SeqCst) {
            loom::thread::yield_now();
        }
        let current = join::<_, true>(&registry, "namespace".into(), [1; 16], create).unwrap();
        releasing.join().unwrap();
        assert!(
            !overlap.load(Ordering::SeqCst),
            "owner teardown raced reopen"
        );
        let again = join::<_, true>(&registry, "namespace".into(), [1; 16], || {
            panic!("reopen lost its owner")
        })
        .unwrap();
        assert!(Arc::ptr_eq(&current, &again));
        release::<_, true>(&registry, &mut Some(current));
        assert!(locked.load(Ordering::SeqCst));
        release::<_, true>(&registry, &mut Some(again));
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

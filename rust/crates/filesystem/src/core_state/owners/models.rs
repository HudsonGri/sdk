use super::*;
use loom::sync::atomic::{AtomicUsize, Ordering};

use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};

#[derive(Debug)]
struct Seen {
    created: usize,
    expected: usize,
    shared: bool,
}

fn initialize<const FENCE: bool>(
    registry: &Owners<usize>,
    created: &AtomicUsize,
    ready: loom::sync::mpsc::Sender<()>,
    peer: loom::sync::mpsc::Receiver<()>,
) -> Arc<usize> {
    join::<_, FENCE>(registry, "namespace".into(), [1; 16], || {
        let id = created.fetch_add(1, Ordering::SeqCst);
        if !FENCE {
            // Witness the unlocked initialization window with both constructors
            // in flight. Channels coordinate the control, not registry state.
            ready.send(()).unwrap();
            peer.recv().unwrap();
        }
        loom::thread::yield_now();
        Ok(Arc::new(id))
    })
    .unwrap()
}

fn race<const FENCE: bool>(see: &mut Observer<'_, Seen>) -> Result<(), Stop> {
    let registry = Arc::new(Owners::new(BTreeMap::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let (first_ready, first_seen) = loom::sync::mpsc::channel();
    let (second_ready, second_seen) = loom::sync::mpsc::channel();
    let peer = {
        let registry = registry.clone();
        let created = created.clone();
        loom::thread::spawn(move || {
            initialize::<FENCE>(&registry, &created, first_ready, second_seen)
        })
    };
    // The model thread is the second actor: both joins still race, while each
    // independent oracle run needs only one spawned peer.
    let first = initialize::<FENCE>(&registry, &created, second_ready, first_seen);
    let owners = [first, peer.join().unwrap()];
    see.see(Seen {
        created: created.load(Ordering::SeqCst),
        expected: 1,
        shared: Arc::ptr_eq(&owners[0], &owners[1]),
    })?;
    let reopened = join::<_, FENCE>(&registry, "namespace".into(), [1; 16], || {
        panic!("reopen ignored a live owner")
    })
    .unwrap();
    see.see(Seen {
        created: created.load(Ordering::SeqCst),
        expected: 1,
        shared: Arc::ptr_eq(&owners[0], &reopened),
    })?;
    drop((owners, reopened));
    join::<_, FENCE>(&registry, "namespace".into(), [2; 16], || {
        created.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(2))
    })
    .unwrap();
    see.see(Seen {
        created: created.load(Ordering::SeqCst),
        expected: 2,
        shared: true,
    })
}

#[test]
fn competing_joins_and_reopen_share_the_live_owner() {
    loom::model(|| {
        let oracle = Oracle::declare(|_: &(), fault: Option<&()>, see| {
            if fault.is_some() {
                race::<false>(see)
            } else {
                race::<true>(see)
            }
        })
        .invariant("registry-owner", |seen| {
            if seen.created == seen.expected && seen.shared {
                Ok(())
            } else {
                Err(format!("{seen:?}"))
            }
        })
        .control("registry-owner", (), ())
        .build();
        oracle.check(&());
    });
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

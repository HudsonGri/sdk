use super::*;
use crate::lock_order::{Mutex as OrderedMutex, OPERATION};
use std::sync::{OnceLock, Weak};

type Owners<T> = OrderedMutex<BTreeMap<PathBuf, ([u8; 16], Weak<T>, Arc<Retirement>)>, OPERATION>;

/// The weak count reaches zero before the last owner's fields finish dropping.
/// A registry entry keeps this completion cell, never the owner or its OS lock.
#[derive(Debug, Default)]
struct Retirement {
    done: crate::sync::Mutex<bool>,
    changed: crate::sync::Condvar,
    #[cfg(test)]
    waiter: crate::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
}
impl Retirement {
    fn wait(&self) {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        while !*done {
            #[cfg(test)]
            if let Some(waiter) = self
                .waiter
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
            {
                let _ = waiter.send(());
            }
            done = self
                .changed
                .wait(done)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}
#[derive(Debug)]
pub(super) struct Retiring(Arc<Retirement>);
impl Drop for Retiring {
    fn drop(&mut self) {
        *self.0.done.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.0.changed.notify_all();
    }
}

// Registry -> OS namespace lock. Neither the record log nor provider/view locks
// are acquired while holding this registry. The constructor uses try-lock only.
fn join<T, const FENCE: bool>(
    owners: &Owners<T>,
    root: PathBuf,
    identity: [u8; 16],
    create: impl FnOnce(Retiring) -> Result<Arc<T>, LocalCoreStateStoreError>,
) -> Result<Arc<T>, LocalCoreStateStoreError> {
    join_with_retirement::<T, FENCE, true>(owners, root, identity, create)
}

fn join_with_retirement<T, const FENCE: bool, const WAIT: bool>(
    owners: &Owners<T>,
    root: PathBuf,
    identity: [u8; 16],
    create: impl FnOnce(Retiring) -> Result<Arc<T>, LocalCoreStateStoreError>,
) -> Result<Arc<T>, LocalCoreStateStoreError> {
    let mut guard = Some(owners.lock().unwrap_or_else(PoisonError::into_inner));
    if let Some((original, owner, retirement)) = guard.as_ref().unwrap().get(&root) {
        if let Some(owner) = owner.upgrade() {
            return if *original == identity {
                Ok(owner)
            } else {
                Err(LocalCoreStateStoreError::OwnershipConflict(root))
            };
        }
        // No live owner remains, but its destructor may still hold owner.lock.
        // Retirement never takes the registry, so waiting cannot invert locks.
        if WAIT {
            retirement.wait();
        }
    }
    // FENCE=false is the deliberately broken initialization race.
    if !FENCE {
        guard.take();
    }
    let retirement = Arc::new(Retirement::default());
    let owner = create(Retiring(retirement.clone()))?;
    let mut guard = guard.unwrap_or_else(|| owners.lock().unwrap_or_else(PoisonError::into_inner));
    guard.insert(root, (identity, Arc::downgrade(&owner), retirement));
    // Weak entries never keep an owner, its file lock, or its WAL alive.
    // A dead weak owner can still be retiring; removing that entry would lose
    // its completion fence. Reclaim only after physical retirement is complete.
    guard.retain(|_, (_, owner, retirement)| {
        owner.strong_count() != 0
            || !*retirement
                .done
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
    });
    Ok(owner)
}

impl LocalCoreStateStore {
    /// Joins this process's existing owner of a canonical private namespace.
    /// Other processes remain excluded until the last owned handle drops.
    pub fn open_shared_owner(root: impl Into<PathBuf>) -> Result<Self, LocalCoreStateStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        let held =
            crate::native_host::HostRoot::open(&root, &mut crate::native_host::ordinary_work())
                .map_err(|error| LocalCoreStateStoreError::LazyWork(error.into()))?;
        static OWNERS: OnceLock<Owners<Owner>> = OnceLock::new();
        let owner = join::<_, true>(
            OWNERS.get_or_init(|| OrderedMutex::new(BTreeMap::new())),
            root.clone(),
            held.identity().to_bytes(),
            |retiring| {
                let store = Self::open_owned(&root)?;
                let Ownership::Owned(mut owner) = store.ownership else {
                    unreachable!()
                };
                let original = Arc::get_mut(&mut owner).expect("new owner");
                original.cache_limit = Some(super::contexts::OWNED_CHECKPOINT as usize);
                original.retiring = Some(retiring);
                Ok(owner)
            },
        )?;
        Ok(Self {
            root: Arc::new(root),
            ownership: Ownership::Owned(owner),
            deferral: None,
            journal_work: None,
            ownership_anchor: None,
        })
    }
}

#[cfg(all(test, acyclic_fs_loom))]
mod models;
#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn bounded_cache(peak: usize) -> proptest::test_runner::TestCaseResult {
        prop_assert!(peak <= super::super::contexts::OWNED_CHECKPOINT as usize);
        Ok(())
    }

    async fn query_misses(store: &LocalCoreStateStore, queries: usize) -> usize {
        let Ownership::Owned(owner) = &store.ownership else {
            panic!("not owned")
        };
        let value = super::super::tests::crash_lineage();
        assert!(
            WorkspaceLineageStore::compare_and_swap(store, value.workspace_id, 0, value.clone())
                .await
                .unwrap()
        );
        let mut peak = 0;
        for index in 0..queries {
            assert!(
                WorkspaceLineageStore::load(
                    store,
                    WorkspaceId::from_bytes([128 + index as u8; 16])
                )
                .await
                .unwrap()
                .is_none()
            );
            peak = peak.max(owner.residents.read().unwrap().0.len());
        }
        assert_eq!(
            WorkspaceLineageStore::load(store, value.workspace_id)
                .await
                .unwrap(),
            Some(value)
        );
        peak
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4))]
        #[test]
        fn joinable_cache_work_is_bounded_across_distinct_queries(queries in 4usize..16) {
            let (bounded, old) = tokio::runtime::Builder::new_current_thread().enable_all()
                .build().unwrap().block_on(async {
                    let root = tempfile::tempdir().unwrap();
                    let bounded = LocalCoreStateStore::open_shared_owner(root.path().join("bounded")).unwrap();
                    let old = LocalCoreStateStore::open_owned(root.path().join("old")).unwrap();
                    let peaks = (query_misses(&bounded, queries).await, query_misses(&old, queries).await);
                    drop((bounded, old));
                    let reopened = LocalCoreStateStore::open_shared_owner(root.path().join("bounded")).unwrap();
                    let value = super::super::tests::crash_lineage();
                    assert_eq!(WorkspaceLineageStore::load(&reopened, value.workspace_id).await.unwrap(), Some(value));
                    peaks
                });
            bounded_cache(bounded)?;
            prop_assert!(bounded_cache(old).is_err(), "unbounded-cache control escaped the same assertion");
        }
    }

    fn reopen_while_last_owner_is_dropping<const WAIT: bool>() -> bool {
        type DropGate = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
        struct DelayedOwner {
            _lock: OwnerLock,
            gate: std::sync::Mutex<Option<DropGate>>,
            _retiring: Retiring,
        }
        impl Drop for DelayedOwner {
            fn drop(&mut self) {
                if let Some((entered, release)) = self.gate.get_mut().unwrap().take() {
                    entered.send(()).unwrap();
                    release.recv().unwrap();
                }
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_owned();
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let (entered, dropping) = std::sync::mpsc::channel();
        let (release, held) = std::sync::mpsc::channel();
        let (waiting, observed_wait) = std::sync::mpsc::channel();
        let first = join::<_, true>(&registry, root.clone(), [1; 16], |retiring| {
            *retiring.0.waiter.lock().unwrap() = Some(waiting);
            Ok(Arc::new(DelayedOwner {
                _lock: OwnerLock::acquire(&root, LockMode::Exclusive)?,
                gate: std::sync::Mutex::new(Some((entered, held))),
                _retiring: retiring,
            }))
        })
        .unwrap();
        let weak = Arc::downgrade(&first);
        let dropping_owner = std::thread::spawn(move || drop(first));
        dropping.recv().unwrap();
        assert!(
            weak.upgrade().is_none(),
            "last strong owner has not begun dropping"
        );
        let (finished, completion) = std::sync::mpsc::channel();
        let opener = std::thread::spawn(move || {
            let result = join_with_retirement::<_, true, WAIT>(
                &registry,
                root.clone(),
                [1; 16],
                |retiring| {
                    Ok(Arc::new(DelayedOwner {
                        _lock: OwnerLock::acquire(&root, LockMode::Exclusive)?,
                        gate: std::sync::Mutex::new(None),
                        _retiring: retiring,
                    }))
                },
            );
            finished.send(result.is_ok()).unwrap();
        });
        let result = if WAIT {
            observed_wait.recv().unwrap();
            assert!(matches!(
                completion.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            release.send(()).unwrap();
            completion.recv().unwrap()
        } else {
            let result = completion.recv().unwrap();
            release.send(()).unwrap();
            result
        };
        dropping_owner.join().unwrap();
        opener.join().unwrap();
        result
    }

    #[test]
    fn reopen_waits_for_physical_retirement_after_the_last_strong_owner() {
        assert!(
            reopen_while_last_owner_is_dropping::<true>(),
            "same-process reopen raced owner retirement"
        );
        assert!(
            !reopen_while_last_owner_is_dropping::<false>(),
            "old weak-only join escaped the physical-lock detector"
        );
    }

    #[test]
    fn joins_canonical_aliases_without_extending_the_owner_lifetime() {
        let directory = tempfile::tempdir().unwrap();
        let exclusive = LocalCoreStateStore::open_owned(directory.path()).unwrap();
        assert!(LocalCoreStateStore::open_shared_owner(directory.path()).is_err());
        drop(exclusive);
        let first = LocalCoreStateStore::open_shared_owner(directory.path()).unwrap();
        let second = LocalCoreStateStore::open_shared_owner(directory.path().join(".")).unwrap();
        let (Ownership::Owned(a), Ownership::Owned(b)) = (&first.ownership, &second.ownership)
        else {
            panic!("joined store is not owned")
        };
        assert!(Arc::ptr_eq(a, b));
        assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
        drop(first);
        assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
        drop(second);
        LocalCoreStateStore::open_owned(directory.path()).unwrap();
    }
    #[test]
    fn a_live_owner_cannot_be_joined_after_its_directory_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("namespace");
        let first = LocalCoreStateStore::open_shared_owner(&root).unwrap();
        std::fs::rename(&root, directory.path().join("retired")).unwrap();
        std::fs::create_dir(&root).unwrap();
        assert!(matches!(
            LocalCoreStateStore::open_shared_owner(&root),
            Err(LocalCoreStateStoreError::OwnershipConflict(_))
        ));
        drop(first);
        LocalCoreStateStore::open_shared_owner(&root).unwrap();
    }
}

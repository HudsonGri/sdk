use super::*;
use crate::lock_order::{Mutex as OrderedMutex, OPERATION};
use crate::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

#[derive(Debug, PartialEq)]
enum Sharing {
    Exclusive,
    Shared([u8; 16]),
}

type Owners<T> = OrderedMutex<BTreeMap<PathBuf, Arc<Lifecycle<T>>>, OPERATION>;

#[derive(Debug)]
struct Lifecycle<T> {
    current: OrderedMutex<Option<(Sharing, Arc<T>)>, OPERATION>,
    references: AtomicUsize,
    #[cfg(test)]
    locks: AtomicUsize,
    #[cfg(test)]
    registry_locks: AtomicUsize,
}

impl<T> Lifecycle<T> {
    fn new() -> Self {
        Self {
            current: OrderedMutex::new(None),
            references: AtomicUsize::new(0),
            #[cfg(test)]
            locks: AtomicUsize::new(0),
            #[cfg(test)]
            registry_locks: AtomicUsize::new(0),
        }
    }

    fn lock(&self) -> crate::lock_order::MutexGuard<'_, Option<(Sharing, Arc<T>)>> {
        #[cfg(test)]
        self.locks.fetch_add(1, Ordering::Relaxed);
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn registry() -> &'static Owners<Owner> {
    static OWNERS: OnceLock<Owners<Owner>> = OnceLock::new();
    OWNERS.get_or_init(|| OrderedMutex::new(BTreeMap::new()))
}

#[derive(Debug)]
pub(super) struct OwnerRef<T = Owner>(Option<Arc<T>>, Arc<Lifecycle<T>>);

impl<T> Clone for OwnerRef<T> {
    fn clone(&self) -> Self {
        self.1.references.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone(), self.1.clone())
    }
}

impl<T> std::ops::Deref for OwnerRef<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0.as_deref().expect("live owner")
    }
}

impl<T> Drop for OwnerRef<T> {
    fn drop(&mut self) {
        if self.0.is_some() {
            release::<_, true, true>(&self.1, &mut self.0);
        }
    }
}

fn release<T, const FENCE: bool, const FAST: bool>(
    lifecycle: &Lifecycle<T>,
    owner: &mut Option<Arc<T>>,
) {
    // Retire the handle's Arc before its count. The namespace keeps the final
    // Arc until all handles have retired, so teardown runs inside its own gate.
    drop(owner.take());
    if FAST {
        let mut references = lifecycle.references.load(Ordering::Relaxed);
        while references > 1 {
            match lifecycle.references.compare_exchange_weak(
                references,
                references - 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => references = current,
            }
        }
    }
    let mut guard = lifecycle.lock();
    if lifecycle.references.fetch_sub(1, Ordering::AcqRel) == 1 {
        let retired = guard.take();
        if !FENCE {
            drop(guard); // Negative control: reopen overtakes native teardown.
            drop(retired);
        } else {
            drop(retired);
        }
    }
}

fn create(root: &Path, cache_limit: Option<usize>) -> Result<Arc<Owner>, LocalCoreStateStoreError> {
    Ok(Arc::new(Owner {
        _lock: OwnerLock::acquire(root, LockMode::Exclusive)?,
        residents: Arc::default(),
        log: Mutex::default(),
        cache_limit,
    }))
}

fn canonical(root: &Path) -> Result<(PathBuf, [u8; 16]), LocalCoreStateStoreError> {
    std::fs::create_dir_all(root)?;
    let root = std::fs::canonicalize(root)?;
    let held = crate::native_host::HostRoot::open(&root, &mut crate::native_host::ordinary_work())
        .map_err(|error| LocalCoreStateStoreError::LazyWork(error.into()))?;
    let identity = held.identity().to_bytes();
    Ok((root, identity))
}

pub(super) fn exclusive(root: &Path) -> Result<OwnerRef, LocalCoreStateStoreError> {
    let (root, identity) = canonical(root)?;
    join::<_, true, true>(registry(), root.clone(), identity, false, || {
        create(&root, None)
    })
}

// Registry and namespace gates are never nested. Creation/teardown holds only
// the namespace gate, then the OS lock; no record-log or provider lock is taken.
fn join<T, const FENCE: bool, const RETAIN_ADMISSION: bool>(
    owners: &Owners<T>,
    root: PathBuf,
    identity: [u8; 16],
    shared: bool,
    create: impl FnOnce() -> Result<Arc<T>, LocalCoreStateStoreError>,
) -> Result<OwnerRef<T>, LocalCoreStateStoreError> {
    let lifecycle = {
        let mut registry = owners.lock().unwrap_or_else(PoisonError::into_inner);
        registry.retain(|_, lifecycle| Arc::strong_count(lifecycle) != 1);
        let lifecycle = registry
            .entry(root.clone())
            .or_insert_with(|| Arc::new(Lifecycle::new()))
            .clone();
        #[cfg(test)]
        lifecycle.registry_locks.fetch_add(1, Ordering::Relaxed);
        lifecycle
    };
    let mut guard = Some(lifecycle.lock());
    if let Some((original, owner)) = guard.as_ref().unwrap().as_ref() {
        if !shared || *original != Sharing::Shared(identity) {
            return Err(LocalCoreStateStoreError::OwnershipConflict(root));
        }
        lifecycle.references.fetch_add(1, Ordering::Relaxed);
        return Ok(OwnerRef(Some(owner.clone()), lifecycle.clone()));
    }
    if !FENCE {
        guard.take(); // Negative control: duplicate initialization.
    }
    let owner = create()?;
    let mut guard = guard.unwrap_or_else(|| lifecycle.lock());
    let sharing = if shared || !RETAIN_ADMISSION {
        Sharing::Shared(identity)
    } else {
        Sharing::Exclusive
    };
    *guard = Some((sharing, owner.clone()));
    lifecycle.references.store(1, Ordering::Relaxed);
    Ok(OwnerRef(Some(owner), lifecycle.clone()))
}

impl LocalCoreStateStore {
    /// Joins this process's existing owner of a canonical private namespace.
    /// Other processes remain excluded until the last owned handle drops.
    pub fn open_shared_owner(root: impl Into<PathBuf>) -> Result<Self, LocalCoreStateStoreError> {
        let (root, identity) = canonical(&root.into())?;
        let owner = join::<_, true, true>(registry(), root.clone(), identity, true, || {
            create(&root, Some(super::contexts::OWNED_CHECKPOINT as usize))
        })?;
        Ok(Self {
            root: Arc::new(root),
            ownership: Ownership::Owned(owner),
            deferral: None,
            journal_work: None,
        })
    }
}

#[cfg(all(test, acyclic_fs_loom))]
mod models;
#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[tokio::test]
    async fn owned_transactions_and_clones_acquire_no_lifecycle_locks() {
        for shared in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = if shared {
                LocalCoreStateStore::open_shared_owner(directory.path())
            } else {
                LocalCoreStateStore::open_owned(directory.path())
            }
            .unwrap();
            let Ownership::Owned(owner) = &store.ownership else {
                unreachable!()
            };
            let namespace_before = owner.1.locks.load(Ordering::Relaxed);
            let registry_before = owner.1.registry_locks.load(Ordering::Relaxed);
            drop(store.clone());
            let value = super::super::tests::crash_lineage();
            assert!(
                WorkspaceLineageStore::compare_and_swap(&store, value.workspace_id, 0, value)
                    .await
                    .unwrap()
            );
            assert_eq!(
                owner.1.locks.load(Ordering::Relaxed) - namespace_before,
                0,
                "nonfinal release acquired a namespace lock"
            );
            assert_eq!(
                owner.1.registry_locks.load(Ordering::Relaxed) - registry_before,
                0,
                "nonfinal release acquired a registry lock"
            );
        }
    }

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
        assert!(Arc::ptr_eq(a.0.as_ref().unwrap(), b.0.as_ref().unwrap()));
        assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
        drop(first);
        assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
        drop(second);
        LocalCoreStateStore::open_owned(directory.path()).unwrap();
    }
    #[test]
    fn final_owner_release_unlocks_inherited_file_descriptions() {
        for shared in [false, true] {
            for duplicate_count in 1..=3 {
                let directory = tempfile::tempdir().unwrap();
                let store = if shared {
                    LocalCoreStateStore::open_shared_owner(directory.path())
                } else {
                    LocalCoreStateStore::open_owned(directory.path())
                }
                .unwrap();
                // dup and fork share the same flock file description. Hold the
                // duplicates past the logical owner's release, as a child can
                // do between fork and exec even with CLOEXEC set.
                let duplicates: Vec<_> = match &store.ownership {
                    Ownership::Owned(owner) => (0..duplicate_count)
                        .map(|_| owner._lock.0.as_ref().unwrap().try_clone().unwrap())
                        .collect(),
                    Ownership::Shared => unreachable!(),
                };
                let clone = store.clone();
                drop(store);
                assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
                drop(clone);
                let reopened = LocalCoreStateStore::open_owned(directory.path())
                    .expect("released owner left an inherited lock");
                drop((reopened, duplicates));
            }
        }
    }

    #[test]
    fn explicit_unlock_cannot_release_a_later_admission_on_a_duplicate() {
        let directory = tempfile::tempdir().unwrap();
        let mut lock = OwnerLock::acquire(directory.path(), LockMode::Exclusive).unwrap();
        let duplicate = lock.0.as_ref().unwrap().try_clone().unwrap();
        lock.unlock().unwrap();
        duplicate.try_lock_exclusive().unwrap();
        // Drop must consume no second unlock, which could release a newer lock
        // sharing the old file description after the explicit release.
        drop(lock);
        assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
        FileExt::unlock(&duplicate).unwrap();
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

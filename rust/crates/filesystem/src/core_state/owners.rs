use super::*;
use crate::lock_order::{Mutex as OrderedMutex, OPERATION};
use std::sync::{OnceLock, Weak};

type Owners<T> = OrderedMutex<BTreeMap<PathBuf, ([u8; 16], Weak<T>)>, OPERATION>;

fn registry() -> &'static Owners<Owner> {
    static OWNERS: OnceLock<Owners<Owner>> = OnceLock::new();
    OWNERS.get_or_init(|| OrderedMutex::new(BTreeMap::new()))
}

// Every strong owner, including transaction admissions, releases under the same
// registry lock as open/join. A dead Weak therefore implies completed teardown.
#[derive(Clone, Debug)]
pub(super) struct OwnerRef(Option<Arc<Owner>>);

impl OwnerRef {
    fn new(owner: Arc<Owner>) -> Self {
        Self(Some(owner))
    }
}

impl std::ops::Deref for OwnerRef {
    type Target = Owner;

    fn deref(&self) -> &Owner {
        self.0.as_deref().expect("live owner")
    }
}

impl Drop for OwnerRef {
    fn drop(&mut self) {
        release::<_, true>(registry(), &mut self.0);
    }
}

fn release<T, const FENCE: bool>(owners: &Owners<T>, owner: &mut Option<Arc<T>>) {
    let _guard = FENCE.then(|| owners.lock().unwrap_or_else(PoisonError::into_inner));
    drop(owner.take());
}

fn create(root: &Path, cache_limit: Option<usize>) -> Result<Arc<Owner>, LocalCoreStateStoreError> {
    Ok(Arc::new(Owner {
        _lock: OwnerLock::acquire(root, LockMode::Exclusive)?,
        residents: Arc::default(),
        log: Mutex::default(),
        cache_limit,
    }))
}

pub(super) fn exclusive(root: &Path) -> Result<OwnerRef, LocalCoreStateStoreError> {
    let _guard = registry().lock().unwrap_or_else(PoisonError::into_inner);
    Ok(OwnerRef::new(create(root, None)?))
}

// Registry -> OS namespace lock. Neither the record log nor provider/view locks
// are acquired while holding this registry. The constructor uses try-lock only.
fn join<T, const FENCE: bool>(
    owners: &Owners<T>,
    root: PathBuf,
    identity: [u8; 16],
    create: impl FnOnce() -> Result<Arc<T>, LocalCoreStateStoreError>,
) -> Result<Arc<T>, LocalCoreStateStoreError> {
    let mut guard = Some(owners.lock().unwrap_or_else(PoisonError::into_inner));
    if let Some((original, owner)) = guard.as_ref().unwrap().get(&root)
        && let Some(owner) = owner.upgrade()
    {
        return if *original == identity {
            Ok(owner)
        } else {
            Err(LocalCoreStateStoreError::OwnershipConflict(root))
        };
    }
    // FENCE=false is the deliberately broken initialization race.
    if !FENCE {
        guard.take();
    }
    let owner = create()?;
    let mut guard = guard.unwrap_or_else(|| owners.lock().unwrap_or_else(PoisonError::into_inner));
    guard.insert(root, (identity, Arc::downgrade(&owner)));
    // Weak entries never keep an owner, its file lock, or its WAL alive.
    guard.retain(|_, (_, owner)| owner.strong_count() != 0);
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
        let owner = join::<_, true>(registry(), root.clone(), held.identity().to_bytes(), || {
            create(&root, Some(super::contexts::OWNED_CHECKPOINT as usize))
        })?;
        Ok(Self {
            root: Arc::new(root),
            ownership: Ownership::Owned(OwnerRef::new(owner)),
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

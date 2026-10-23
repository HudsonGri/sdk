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
            release::<_, true, ATOMIC>(&self.1, &mut self.0);
        }
    }
}

// Release strategies. Production always uses `ATOMIC`; every other mode is a
// loom mutation whose model control must fail (owners/models.rs).
const ATOMIC: u8 = 0;
/// Control: every release, final or not, takes the namespace gate.
#[cfg(test)]
#[cfg(acyclic_fs_loom)]
mod models;
#[cfg(test)]
#[cfg(not(acyclic_fs_loom))]
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
    fn joining_a_replaced_identity_never_returns_the_live_owner() {
        let owners = OrderedMutex::new(BTreeMap::new());
        let path = PathBuf::from("namespace");
        let first = join::<_, true>(&owners, path.clone(), [1; 16], || Ok(Arc::new(1))).unwrap();
        let alias = join::<_, true>(&owners, path.clone(), [1; 16], || {
            panic!("an existing identity must be joined")
        })
        .unwrap();
        assert!(Arc::ptr_eq(&first, &alias));
        assert!(matches!(
            join::<_, true>(&owners, path.clone(), [2; 16], || {
                panic!("replacement cannot initialize while the owner is live")
            }),
            Err(LocalCoreStateStoreError::OwnershipConflict(_))
        ));
        drop(first);
        drop(alias);
        assert_eq!(
            *join::<_, true>(&owners, path, [2; 16], || Ok(Arc::new(2))).unwrap(),
            2
        );
    }

    #[test]
    fn a_live_owner_cannot_be_joined_after_its_directory_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("namespace");
        let first = LocalCoreStateStore::open_shared_owner(&root).unwrap();
        let retired = directory.path().join("retired");
        #[cfg(windows)]
        {
            // Windows pins the namespace while its owner holds the directory.
            let error = std::fs::rename(&root, &retired).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            assert!(root.is_dir() && !retired.exists());
            let joined = LocalCoreStateStore::open_shared_owner(&root).unwrap();
            let (Ownership::Owned(a), Ownership::Owned(b)) = (&first.ownership, &joined.ownership)
            else {
                panic!("joined store is not owned")
            };
            assert!(Arc::ptr_eq(a.0.as_ref().unwrap(), b.0.as_ref().unwrap()));
            drop(joined);
            drop(first);
        }
        std::fs::rename(&root, &retired).unwrap();
        std::fs::create_dir(&root).unwrap();
        #[cfg(not(windows))]
        {
            assert!(matches!(
                LocalCoreStateStore::open_shared_owner(&root),
                Err(LocalCoreStateStoreError::OwnershipConflict(_))
            ));
            drop(first);
        }
        LocalCoreStateStore::open_shared_owner(&root).unwrap();
    }
}

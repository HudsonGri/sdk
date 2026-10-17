use crate::sync::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

// Volume ids may conservatively share a process fence across independent
// embedded stores. ProjFS admits only process-owned authorities; remote
// authorities need a distributed publication lease and are refused.
/// Lock order: registry -> fence. No fence operation takes the registry lock;
/// guards retain state, never a lock, across I/O and physical driver lifetime.
#[derive(Default)]
struct ProjectionPolicies(Mutex<BTreeMap<crate::VolumeId, Arc<Fence>>>);
impl ProjectionPolicies {
    pub(crate) fn get(&self, volume: crate::VolumeId) -> Result<Arc<Fence>, crate::FsError> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| crate::FsError::MutationNotAllowed)?;
        entries.retain(|_, fence| Arc::strong_count(fence) > 1);
        Ok(Arc::clone(entries.entry(volume).or_default()))
    }
}
#[cfg(not(acyclic_fs_loom))]
static POLICIES: std::sync::OnceLock<ProjectionPolicies> = std::sync::OnceLock::new();
#[cfg(acyclic_fs_loom)]
loom::lazy_static! { static ref POLICIES: ProjectionPolicies = ProjectionPolicies::default(); }

pub(crate) fn policy_fence(volume: crate::VolumeId) -> Result<Arc<Fence>, crate::FsError> {
    #[cfg(not(acyclic_fs_loom))]
    let policies = POLICIES.get_or_init(ProjectionPolicies::default);
    #[cfg(acyclic_fs_loom)]
    let policies = &*POLICIES;
    policies.get(volume)
}

#[derive(Default)]
pub(crate) struct Fence(Mutex<State>);
#[derive(Default)]
struct State {
    projections: usize,
    editors: usize,
}
pub(crate) struct Lease {
    fence: Arc<Fence>,
    projection: bool,
}
impl Fence {
    pub(crate) fn enter(self: &Arc<Self>, projection: bool) -> Result<Lease, crate::FsError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| crate::FsError::MutationNotAllowed)?;
        if if projection {
            state.editors != 0
        } else {
            state.projections != 0
        } {
            return Err(crate::FsError::MutationNotAllowed);
        }
        let count = if projection {
            &mut state.projections
        } else {
            &mut state.editors
        };
        *count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        Ok(Lease {
            fence: Arc::clone(self),
            projection,
        })
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self
            .fence
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = if self.projection {
            &mut state.projections
        } else {
            &mut state.editors
        };
        *count = count
            .checked_sub(1)
            .expect("policy lease belongs to its fence");
    }
}
#[cfg(all(test, acyclic_fs_loom))]
mod model {
    use super::*;
    fn run(omit_editor_lease: bool) {
        loom::model(move || {
            let volume = crate::VolumeId::from_bytes([1; 16]);
            let fence = policy_fence(volume).unwrap();
            let projecting = Arc::new(crate::sync::atomic::AtomicBool::new(false));
            let editor = loom::thread::spawn({
                let projecting = Arc::clone(&projecting);
                move || {
                    let fence = policy_fence(volume).unwrap();
                    let lease = if omit_editor_lease {
                        None
                    } else {
                        match fence.enter(false) {
                            Ok(lease) => Some(lease),
                            Err(_) => return,
                        }
                    };
                    assert!(
                        !projecting.load(crate::sync::atomic::Ordering::SeqCst),
                        "policy changed during projection"
                    );
                    drop(lease);
                }
            });
            if let Ok(lease) = fence.enter(true) {
                projecting.store(true, crate::sync::atomic::Ordering::SeqCst);
                loom::thread::yield_now();
                projecting.store(false, crate::sync::atomic::Ordering::SeqCst);
                drop(lease);
            }
            editor.join().unwrap();
            assert!(fence.enter(false).is_ok());
        });
    }
    #[test]
    fn policy_publication_and_projection_are_exclusive() {
        run(false);
    }
    #[test]
    #[should_panic(expected = "policy changed during projection")]
    fn missing_policy_lease_negative_control() {
        run(true);
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod properties {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn fresh_owner_handles_share_exclusion_until_the_last_lease_retires(steps in prop::collection::vec(0u8..4, 0..48)) {
            let volume = crate::VolumeId::new();
            let mut projections = Vec::new();
            let mut editors = Vec::new();
            for step in steps {
                let fence = policy_fence(volume).map_err(|error| TestCaseError::fail(error.to_string()))?;
                match step {
                    0 | 1 => {
                        let projection = step == 0;
                        let allowed = if projection { editors.is_empty() } else { projections.is_empty() };
                        let entered = fence.enter(projection);
                        prop_assert_eq!(entered.is_ok(), allowed);
                        if let Ok(lease) = entered { if projection { projections.push(lease); } else { editors.push(lease); } }
                    }
                    2 => { projections.pop(); }
                    _ => { editors.pop(); }
                }
            }
            drop(projections); drop(editors);
            prop_assert!(policy_fence(volume).unwrap().enter(false).is_ok());
        }
    }
}

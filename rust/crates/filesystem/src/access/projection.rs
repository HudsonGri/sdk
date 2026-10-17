use crate::sync::{Arc, Mutex};
use std::collections::BTreeMap;

// A volume id denotes one authority. Distinct embedded stores reusing an id
// conservatively share exclusion. Remote authorities need a distributed lease
// and are refused by ProjFS admission.
#[derive(Default)]
struct ProjectionPolicies {
    // Terminal lock: neither backend I/O nor another lock runs under it.
    // Leases retain registration, never this lock, across publication/retirement.
    entries: Mutex<BTreeMap<crate::VolumeId, State>>,
}
#[derive(Default)]
struct State {
    projections: usize,
    editors: usize,
}
pub(crate) struct Lease {
    owner: Arc<ProjectionPolicies>,
    volume: crate::VolumeId,
    projection: bool,
}

#[cfg(not(acyclic_fs_loom))]
static POLICIES: std::sync::OnceLock<Arc<ProjectionPolicies>> = std::sync::OnceLock::new();
#[cfg(acyclic_fs_loom)]
loom::lazy_static! { static ref POLICIES: Arc<ProjectionPolicies> = Arc::new(ProjectionPolicies::default()); }

pub(crate) fn enter(volume: crate::VolumeId, projection: bool) -> Result<Lease, crate::FsError> {
    #[cfg(not(acyclic_fs_loom))]
    let policies = POLICIES.get_or_init(|| Arc::new(ProjectionPolicies::default()));
    #[cfg(acyclic_fs_loom)]
    let policies = &*POLICIES;
    ProjectionPolicies::enter(policies, volume, projection)
}
impl ProjectionPolicies {
    fn enter(
        owner: &Arc<Self>,
        volume: crate::VolumeId,
        projection: bool,
    ) -> Result<Lease, crate::FsError> {
        let mut entries = owner
            .entries
            .lock()
            .map_err(|_| crate::FsError::MutationNotAllowed)?;
        if let Some(state) = entries.get(&volume)
            && if projection {
                state.editors != 0
            } else {
                state.projections != 0
            }
        {
            return Err(crate::FsError::MutationNotAllowed);
        }
        let state = entries.entry(volume).or_default();
        let count = if projection {
            &mut state.projections
        } else {
            &mut state.editors
        };
        *count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        Ok(Lease {
            owner: Arc::clone(owner),
            volume,
            projection,
        })
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut entries = self
            .owner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = entries
            .get_mut(&self.volume)
            .expect("registered policy lease");
        let count = if self.projection {
            &mut state.projections
        } else {
            &mut state.editors
        };
        *count = count
            .checked_sub(1)
            .expect("policy lease belongs to its owner");
        if state.projections == 0 && state.editors == 0 {
            entries.remove(&self.volume);
        }
        if entries.is_empty() {
            *entries = BTreeMap::new();
        }
    }
}
#[cfg(all(test, acyclic_fs_loom))]
mod model {
    use super::*;
    fn run(omit_editor_lease: bool) {
        // Collect outcomes only; all schedule-dependent state uses the shim.
        // Report after Loom retires its lazy registry, including failing schedules.
        let violated = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = std::sync::Arc::clone(&violated);
        loom::model(move || {
            let volume = crate::VolumeId::from_bytes([1; 16]);
            let projecting = Arc::new(crate::sync::atomic::AtomicBool::new(false));
            let editor = loom::thread::spawn({
                let projecting = Arc::clone(&projecting);
                move || {
                    let lease = if omit_editor_lease {
                        None
                    } else {
                        match enter(volume, false) {
                            Ok(lease) => Some(lease),
                            Err(_) => return false,
                        }
                    };
                    let overlap = projecting.load(crate::sync::atomic::Ordering::SeqCst);
                    drop(lease);
                    overlap
                }
            });
            if let Ok(lease) = enter(volume, true) {
                projecting.store(true, crate::sync::atomic::Ordering::SeqCst);
                loom::thread::yield_now();
                projecting.store(false, crate::sync::atomic::Ordering::SeqCst);
                drop(lease);
            }
            if editor.join().unwrap() {
                observed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            assert!(enter(volume, false).is_ok());
        });
        assert!(
            !violated.load(std::sync::atomic::Ordering::Relaxed),
            "policy changed during projection"
        );
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
    #[cfg(all(feature = "memory", not(target_arch = "wasm32")))]
    #[test]
    fn unknown_authorities_cannot_claim_a_process_policy_owner() {
        let unknown = crate::Fs::new((), (), crate::EmbeddedCapabilities::MEMORY);
        assert!(!unknown.has_process_policy_owner());
        assert!(crate::Fs::memory().has_process_policy_owner());
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn fresh_owner_handles_share_exclusion_until_the_last_lease_retires(steps in prop::collection::vec(0u8..4, 0..48)) {
            let volume = crate::VolumeId::from_bytes([1; 16]);
            let policies = Arc::new(ProjectionPolicies::default());
            let mut projections = Vec::new();
            let mut editors = Vec::new();
            for step in steps {
                match step {
                    0 | 1 => {
                        let projection = step == 0;
                        let allowed = if projection { editors.is_empty() } else { projections.is_empty() };
                        let entered = ProjectionPolicies::enter(&policies, volume, projection);
                        prop_assert_eq!(entered.is_ok(), allowed);
                        if let Ok(lease) = entered { if projection { projections.push(lease); } else { editors.push(lease); } }
                    }
                    2 => { projections.pop(); }
                    _ => { editors.pop(); }
                }
            }
            drop(projections); drop(editors);
            prop_assert!(policies.entries.lock().unwrap().is_empty());
            prop_assert!(ProjectionPolicies::enter(&policies, volume, false).is_ok());
        }
    }
}

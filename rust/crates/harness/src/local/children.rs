use super::*;
use crate::{runtime::TaskSpawner, scheduler::LeaseFence};

#[cfg(feature = "native-execution")]
mod admission;
#[cfg(feature = "native-execution")]
mod commands;
#[cfg(feature = "native-execution")]
mod controller;
#[cfg(test)]
#[cfg(all(feature = "native-execution", not(acyclic_harness_loom)))]
mod cuts;
#[cfg(feature = "native-execution")]
mod lazy;
#[cfg(feature = "native-execution")]
mod preparation;

/// Admission's budget is live across these frames; each is charged or says why not.
#[cfg(all(test, feature = "native-execution"))]
#[test]
fn admission_makes_no_unpaid_box() {
    acyclic_test_support::boxes::assert_all_paid(&[
        (
            "children/admission.rs",
            include_str!("children/admission.rs"),
        ),
        (
            "children/preparation.rs",
            include_str!("children/preparation.rs"),
        ),
    ]);
}

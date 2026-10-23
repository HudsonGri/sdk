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

/// Every source under `local/children`, and every other source in this
/// crate that names a live allocation budget, charges each heap move or
/// states why it needs no charge.
#[cfg(all(test, feature = "native-execution"))]
#[test]
fn admission_makes_no_unpaid_box() {
    use acyclic_test_support::boxes;
    let root = std::path::Path::new(crate::build_metadata::MANIFEST_DIR).join("src");
    let mut sources = boxes::sources(&root.join("local/children"));
    sources.extend(boxes::sources(&root).into_iter().filter(|(name, source)| {
        boxes::budgeted(source) && !std::path::Path::new(name).starts_with("local/children")
    }));
    boxes::assert_sources_paid(&sources, 20);
}

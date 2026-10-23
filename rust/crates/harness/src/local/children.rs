use super::*;
use crate::{runtime::TaskSpawner, scheduler::LeaseFence};

#[cfg(any(test, feature = "native-execution"))]
mod admission;
#[cfg(any(test, feature = "native-execution"))]
mod commands;
#[cfg(any(test, feature = "native-execution"))]
mod controller;
#[cfg(test)]
#[cfg(all(any(test, feature = "native-execution"), not(acyclic_harness_loom)))]
mod cuts;
#[cfg(any(test, feature = "native-execution"))]
mod lazy;
#[cfg(any(test, feature = "native-execution"))]
mod preparation;

/// Set in a scale run's negative control: settled children keep their task
/// views, so the run's bounds must see each retained view.
#[cfg(test)]
pub(in crate::local) const RETAIN_CHILD_VIEWS: &str = "ACYCLIC_TEST_RETAIN_CHILD_VIEWS";

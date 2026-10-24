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

/// Set in a scale run's negative control: settled children keep their task
/// views, so the run's bounds must see each retained view.
#[cfg(test)]
pub(in crate::local) const RETAIN_CHILD_VIEWS: &str = "ACYCLIC_TEST_RETAIN_CHILD_VIEWS";

/// Set in a scale run's negative control: each task view mounts its own
/// native instance instead of a view of the process's shared one.
#[cfg(test)]
pub(in crate::local) const UNSHARED_TASK_VIEWS: &str = "ACYCLIC_TEST_UNSHARED_TASK_VIEWS";

//! One manifest: each owning fix removes its entry after rebasing onto DST.
use super::Error;

pub(super) const EXPECTED_FAILS: &[(&str, u64, fn(&Error) -> bool)] = &[(
    "F3",
    904,
    |error| matches!(error, Error::Conflict(cause) if cause == "local agent already has an admitted turn"),
)];

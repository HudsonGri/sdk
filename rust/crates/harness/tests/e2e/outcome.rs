//! The outcome a scripted case expects. Standalone so the fuzz targets that
//! include one case schema need not include the others.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedOutcome {
    #[default]
    Succeeded,
    Failed,
    Indeterminate,
}

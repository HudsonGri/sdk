use super::*;

/// Immutable decision written to `approvals/<id>/decisions/<principal>.json`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecisionFile {
    /// SHA-256 of the exact admitted request bytes, including its action binding.
    pub request_sha256: [u8; 32],
    /// Open interaction version being resolved.
    pub expected_version: u64,
    /// Whether this principal approves the bound operation.
    pub approved: bool,
    /// Participant explanation retained in the owner's immutable decision.
    pub reason: Option<String>,
}

impl ApprovalDecisionFile {
    /// Decodes a bounded decision body before its authenticated owner admits it.
    pub fn decode(bytes: &[u8], maximum_bytes: u64) -> Result<Self> {
        if bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid(
                "approval decision exceeds its file bound".into(),
            ));
        }
        let value: Self = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if value.expected_version == 0 || value.request_sha256 == [0; 32] {
            return Err(Error::Invalid(
                "approval decision has no request binding".into(),
            ));
        }
        Ok(value)
    }
}

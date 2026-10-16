use crate::Adapter;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Authentication required by a registered endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Authentication {
    /// A provider API key is required.
    ApiKey,
    /// Prefer an API key, otherwise reuse native Codex CLI credentials on the built-in Responses route.
    ApiKeyOrCodexLogin,
    /// The endpoint accepts anonymous requests; use an API key when configured.
    Optional,
}

/// Declared image support and finite bounds for one exact model.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImageInput {
    /// Maximum raw bytes in one image.
    pub maximum_bytes: u64,
    /// Maximum combined raw image bytes in one request.
    pub maximum_total_bytes: u64,
    /// Maximum image occurrences, including prior turns and tool results.
    pub maximum_images: u32,
    /// Maximum width or height accepted before provider preprocessing.
    pub maximum_dimension: u32,
    /// Model-specific upper bound for one image's input tokens.
    pub maximum_tokens: u32,
}

/// Endpoint configuration independent of immutable model identity.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSpec {
    /// Protocol dialect.
    pub adapter: Adapter,
    /// Credential policy supplied by the catalog, independent of its provider ID.
    pub authentication: Authentication,
    /// HTTPS API root, or loopback HTTP API root.
    pub base_url: String,
    /// Exact model capacities, including local server configurations.
    #[serde(default)]
    pub models: BTreeMap<String, crate::ModelSpec>,
    /// Verified admission counter. Required for automatic compaction; generic
    /// compatible endpoints must configure their own tokenizer contract.
    #[serde(default)]
    pub token_counter: Option<crate::TokenCounterSpec>,
    /// Exact image input limits; omitted models receive a fallback note.
    #[serde(default)]
    pub images: BTreeMap<String, ImageInput>,
    /// Maximum bytes in one SSE event and aggregate retained protocol state.
    pub max_event_bytes: usize,
}

//! One exchange format. Headers have names only; body files are exact wire bytes.
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    io::{self, Read},
    path::Path,
};

/// Protocol identity supplied by the model adapter, never inferred from credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Wire adapter.
    pub adapter: String,
    /// Catalog provider identity.
    pub provider: String,
    /// Selected member model.
    pub model: String,
}

/// Why the consumer stopped observing the HTTP body. A protocol terminal can
/// cancel a healthy HTTP stream before EOF; the captured prefix remains exact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseEnd {
    /// The transport returned clean EOF.
    Eof,
    /// The consumer dropped the body, including after a protocol terminal.
    Cancelled,
    /// The transport returned an error after the captured prefix.
    Interrupted,
}

/// Published only within one exclusively allocated exchange directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    /// Format revision.
    pub version: u32,
    /// UTC Unix timestamp in nanoseconds.
    pub timestamp_unix_nanos: u128,
    /// Adapter, provider and selected model.
    pub identity: Identity,
    /// Running executable SHA256.
    pub binary_sha256: String,
    /// Commit from the compiled source.
    pub source_commit: String,
    /// HTTP method.
    pub method: String,
    /// URL path only. No authority, user info, query or fragment.
    pub path: String,
    /// Header names whose values were withheld. No header values are accepted.
    pub redacted_headers: Vec<String>,
    /// SHA256 of request.body.
    pub request_sha256: String,
    /// Status, absent if headers never arrived.
    pub status: Option<u16>,
    /// Response media type.
    pub content_type: Option<String>,
    /// SHA256 of the exact received prefix in response.body.
    pub response_sha256: String,
    /// Original transport chunk lengths, including SSE framing bytes.
    pub chunks: Vec<u64>,
    /// Absent while receiving. Settled prefixes replay without inventing bytes.
    pub end: Option<ResponseEnd>,
}

/// Verified replay input. Created only by loading a settled, digest-checked exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exchange {
    provenance: Provenance,
    request: Vec<u8>,
    chunks: Vec<Vec<u8>>,
}
impl Exchange {
    /// Checks the manifest, every body digest and the exact transport boundaries.
    pub fn load(directory: &Path) -> io::Result<Self> {
        Self::load_checked(directory).map_err(|error| io::Error::new(error.kind(), format!(
            "STEP admit provider recording failed at {}: {error}; recovery: ACYCLIC_PROVIDER_RECORD=provider-captures graphcoder -p <original-prompt>, then load the newly completed exchange", directory.display())))
    }
    fn load_checked(directory: &Path) -> io::Result<Self> {
        let provenance: Provenance =
            serde_json::from_slice(&std::fs::read(directory.join("provenance.json"))?)
                .map_err(io::Error::other)?;
        let hex = |text: &str, sizes: &[usize]| {
            sizes.contains(&text.len()) && text.bytes().all(|b| b.is_ascii_hexdigit())
        };
        if provenance.version != 1
            || provenance.end.is_none()
            || provenance.method != "POST"
            || provenance
                .status
                .is_none_or(|status| !(100..600).contains(&status))
            || !hex(&provenance.binary_sha256, &[64])
            || !hex(&provenance.source_commit, &[40, 64])
        {
            return Err(io::Error::other(
                "incomplete or invalid provider provenance",
            ));
        }
        let request = std::fs::read(directory.join("request.body"))?;
        let response = std::fs::read(directory.join("response.body"))?;
        let total = provenance
            .chunks
            .iter()
            .try_fold(0usize, |n, c| n.checked_add(usize::try_from(*c).ok()?));
        if sha256(&request) != provenance.request_sha256
            || sha256(&response) != provenance.response_sha256
            || total != Some(response.len())
        {
            return Err(io::Error::other(format!(
                "STEP admit provider recording failed at {}; recovery: recapture a complete exchange with ACYCLIC_PROVIDER_RECORD",
                directory.display()
            )));
        }
        let mut remaining = response.as_slice();
        let chunks = provenance
            .chunks
            .iter()
            .map(|length| {
                let length = usize::try_from(*length).map_err(io::Error::other)?;
                let (chunk, tail) = remaining
                    .split_at_checked(length)
                    .ok_or_else(|| io::Error::other("recorded chunk exceeds the response body"))?;
                remaining = tail;
                Ok(chunk.to_vec())
            })
            .collect::<io::Result<_>>()?;
        Ok(Self {
            provenance,
            request,
            chunks,
        })
    }
    /// Verified provenance.
    pub fn provenance(&self) -> &Provenance {
        &self.provenance
    }
    /// Exact original request, useful for replay guards.
    pub fn request(&self) -> &[u8] {
        &self.request
    }
    /// Exact chunks; the replay host must not regenerate SSE.
    pub fn chunks(&self) -> &[Vec<u8>] {
        &self.chunks
    }
}
/// SHA256 of an in-memory body, without text decoding.
pub fn sha256(bytes: &[u8]) -> String {
    let mut hash = BodyHash::default();
    hash.update(bytes);
    hash.hex()
}
/// Shared incremental body hash; neither capture nor replay has a separate digest implementation.
#[derive(Clone, Default)]
pub struct BodyHash(Sha256);
impl BodyHash {
    /// Include exact raw bytes.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
    /// Digest the current prefix without consuming it.
    pub fn hex(&self) -> String {
        hex::encode(self.0.clone().finalize())
    }
}
/// SHA256 of a stream without loading the binary into memory.
pub fn sha256_reader(mut input: impl Read) -> io::Result<String> {
    let mut digest = BodyHash::default();
    let mut buffer = [0; 8192];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(
            buffer
                .get(..n)
                .ok_or_else(|| io::Error::other("reader exceeded its buffer"))?,
        );
    }
    Ok(digest.hex())
}

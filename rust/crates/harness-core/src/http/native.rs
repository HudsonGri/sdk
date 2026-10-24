//! One native HTTP implementation; protocol adapters classify the retained send state.
use crate::{Error, ModelTransportFailure, OperationId};
use acyclic_stream::BoxProviderStream;
use bytes::Bytes;
use futures::StreamExt as _;
use std::{collections::BTreeMap, time::Duration};

/// Whether a failed exchange could have reached the server's HTTP handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendState {
    /// Request construction or connection establishment failed before dispatch.
    NeverSent,
    /// HTTP bytes may have left this host. A lost response is not a refusal.
    MayHaveSent,
}

/// Network failure with conservative, transport-owned dispatch evidence.
pub struct Failure {
    kind: ModelTransportFailure,
    state: SendState,
    source: reqwest::Error,
}

impl Failure {
    fn new(error: reqwest::Error, body: bool) -> Self {
        let (root, tls) = root(&error);
        let _ = root;
        let kind = if tls {
            ModelTransportFailure::Tls
        } else if error.is_timeout() {
            ModelTransportFailure::Timeout
        } else if error.is_connect() {
            ModelTransportFailure::Connect
        } else {
            ModelTransportFailure::Interrupted
        };
        let state = if !body && (error.is_builder() || error.is_connect()) {
            SendState::NeverSent
        } else {
            SendState::MayHaveSent
        };
        Self {
            kind,
            state,
            source: error,
        }
    }
    /// True only when local request construction failed before dispatch.
    #[must_use]
    pub fn request_invalid(&self) -> bool {
        self.source.is_builder()
    }
    /// Transport classification shared by model dispatch and MCP.
    #[must_use]
    pub const fn kind(&self) -> ModelTransportFailure {
        self.kind
    }
    /// Evidence used to decide whether resending an effect is safe.
    #[must_use]
    pub const fn send_state(&self) -> SendState {
        self.state
    }
    /// Deepest cause, excluding the URL-bearing reqwest context.
    pub fn cause(&self) -> &(dyn std::error::Error + 'static) {
        root(&self.source).0
    }
    /// Preserves uncertain delivery; a JSON-RPC id is not an idempotency receipt.
    #[must_use]
    pub fn into_mcp_error(self, operation: OperationId) -> Error {
        if self.request_invalid() {
            return Error::Invalid("invalid HTTP request".into());
        }
        match self.state {
            SendState::NeverSent => Error::HttpNotSent {
                operation,
                failure: self.kind,
            },
            SendState::MayHaveSent => Error::Indeterminate(operation),
        }
    }
}

fn root(error: &reqwest::Error) -> (&(dyn std::error::Error + 'static), bool) {
    let mut current: &(dyn std::error::Error + 'static) = error;
    let mut tls = false;
    loop {
        tls |= current.is::<rustls::Error>();
        let next = current
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|wrapped| wrapped as &(dyn std::error::Error + 'static))
            .or_else(|| current.source());
        match next {
            Some(next) => current = next,
            None => return (current, tls),
        }
    }
}

/// Streaming exchange input. Deliberately not Debug: headers and URLs can contain secrets.
pub struct Request {
    /// HTTP method, selected by the protocol adapter.
    pub method: reqwest::Method,
    /// Pinned target URL.
    pub url: String,
    /// Explicit headers, including any host-selected credentials.
    pub headers: BTreeMap<String, String>,
    /// Exact immutable request body.
    pub body: Bytes,
    /// Optional whole-exchange deadline; model adapters also own idle deadlines.
    pub timeout: Option<Duration>,
}

/// Unparsed HTTP response shared by all native protocol adapters.
pub struct Response {
    /// HTTP response status.
    pub status: u16,
    /// Original response headers.
    pub headers: reqwest::header::HeaderMap,
    /// Lazy body; every body failure is conservatively MayHaveSent.
    pub body: BoxProviderStream<'static, std::result::Result<Bytes, Failure>>,
}

/// The sole client policy: pooled HTTP/2, no redirects and no implicit replay.
#[must_use]
pub fn client_builder() -> ClientBuilder {
    ClientBuilder(reqwest::Client::builder())
}

/// Opaque pooled client. All requests pass through the non-replaying exchange API.
pub struct Client(reqwest::Client);

/// Safe client configuration; redirect and automatic retry policy cannot be overridden.
pub struct ClientBuilder(reqwest::ClientBuilder);

impl ClientBuilder {
    /// Adds host-selected default headers.
    #[must_use]
    pub fn default_headers(self, headers: reqwest::header::HeaderMap) -> Self {
        Self(self.0.default_headers(headers))
    }
    /// Disables environment proxy discovery for explicitly direct connections.
    #[must_use]
    pub fn no_proxy(self) -> Self {
        Self(self.0.no_proxy())
    }
    /// Adds a host-selected TLS trust root.
    #[must_use]
    pub fn add_root_certificate(self, root: reqwest::Certificate) -> Self {
        Self(self.0.add_root_certificate(root))
    }
    /// Selects HTTP/1 for hosts which require it.
    #[must_use]
    pub fn http1_only(self) -> Self {
        Self(self.0.http1_only())
    }
    /// Applies the sole dispatch policy at construction, after all permitted configuration.
    pub fn build(self) -> std::result::Result<Client, reqwest::Error> {
        self.0
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map(Client)
    }
}

/// Sends exactly once. Dispatch evidence is produced here rather than guessed by callers.
pub async fn exchange(client: &Client, request: Request) -> std::result::Result<Response, Failure> {
    let mut builder = client
        .0
        .request(request.method, request.url)
        .body(request.body);
    if let Some(timeout) = request.timeout {
        builder = builder.timeout(timeout);
    }
    for (name, value) in request.headers {
        builder = builder.header(name, value);
    }
    let response = builder
        .send()
        .await
        .map_err(|error| Failure::new(error, false))?;
    Ok(Response {
        status: response.status().as_u16(),
        headers: response.headers().clone(),
        body: Box::pin(
            response
                .bytes_stream()
                .map(|chunk| chunk.map_err(|error| Failure::new(error, true))),
        ),
    })
}

//! Shared identity secrets client and admission. Transports carry bytes; they
//! never interpret responses or expose response bodies as diagnostics.
use crate::contract::*;
use crate::credential::CredentialProvider;
use serde::{Serialize, de::DeserializeOwned};
use std::{future::Future, pin::Pin};
use zeroize::Zeroizing;
pub mod conformance;

/// Decode an exact protobuf request, rejecting discarded fields (including an
/// account selector), duplicate fields and noncanonical encodings. All browser
/// request entry points use this admission before resolving credentials.
pub fn decode_request<P, R>(input: &[u8]) -> Result<R, Error>
where
    P: prost::Message + Default,
    R: TryFrom<P>,
{
    let wire = P::decode(input).map_err(|_| Error::InvalidRequest)?;
    let canonical = Zeroizing::new(wire.encode_to_vec());
    if canonical.as_slice() != input {
        return Err(Error::InvalidRequest);
    }
    R::try_from(wire).map_err(|_| Error::InvalidRequest)
}

/// Redacted failures. Neither provider errors nor their source chains survive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Invalid or unsupported request shape.
    #[error("secrets: invalid request")]
    InvalidRequest,
    /// Provider reply violates the contract, including protected value leakage.
    #[error("secrets: invalid response")]
    InvalidResponse,
    /// No independent secrets permission covers this operation.
    #[error("secrets: permission denied")]
    PermissionDenied,
    /// Scope generation changed before the mutation.
    #[error("secrets: generation conflict; list the scope before submitting a new mutation")]
    Conflict,
    /// Credential resolution failed; never retain the callback's diagnostic.
    #[error("secrets: credential unavailable")]
    Credential,
    /// Transport failure, without URL, headers, body or source diagnostic.
    #[error("secrets: transport failed")]
    Transport,
    /// Non-success status; response body is never included.
    #[error("secrets: HTTP status {0}")]
    Http(u16),
}

impl Error {
    /// Stable diagnostic code, without provider-controlled text.
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidResponse => "invalid_response",
            Self::PermissionDenied => "permission_denied",
            Self::Conflict => "generation_conflict",
            Self::Credential => "credential_unavailable",
            Self::Transport => "transport_failed",
            Self::Http(_) => "http_error",
        }
    }
}

fn label(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_control)
}
fn require(valid: bool) -> Result<(), crate::Error> {
    if valid {
        Ok(())
    } else {
        Err(crate::Error::Invalid)
    }
}
pub(crate) fn validate_grant(value: &SecretsGrant) -> Result<(), crate::Error> {
    require(
        matches!(
            value.operation,
            SecretsOperation::Put | SecretsOperation::List | SecretsOperation::Delete
        ) && label(&value.scope)
            && value.name.as_deref().is_none_or(label),
    )
}
pub(crate) fn validate_put(value: &SecretPutRequest) -> Result<(), crate::Error> {
    require(
        label(&value.scope)
            && label(&value.name)
            && matches!(value.kind, SecretKind::Plain | SecretKind::Protected),
    )
}
pub(crate) fn validate_list(value: &SecretListRequest) -> Result<(), crate::Error> {
    require(label(&value.scope))
}
pub(crate) fn validate_delete(value: &SecretDeleteRequest) -> Result<(), crate::Error> {
    require(label(&value.scope) && label(&value.name))
}

/// Request facts for the shared authority intersection. There is no account
/// field: the authenticated issuer chain selects the owning account.
#[derive(Clone, Copy, Debug)]
pub struct Target<'a> {
    /// Exact operation.
    pub operation: SecretsOperation,
    /// Exact scope.
    pub scope: &'a str,
    /// None for scope listing; exact variable for mutations.
    pub name: Option<&'a str>,
}

impl Target<'_> {
    pub(crate) fn valid(self) -> bool {
        label(self.scope)
            && match self.operation {
                SecretsOperation::List => self.name.is_none(),
                SecretsOperation::Put | SecretsOperation::Delete => self.name.is_some_and(label),
                SecretsOperation::Unspecified => false,
            }
    }
    pub(crate) fn allowed_by(self, grant: &SecretsGrant) -> bool {
        self.valid()
            && validate_grant(grant).is_ok()
            && self.operation == grant.operation
            && self.scope == grant.scope
            && grant
                .name
                .as_deref()
                .is_none_or(|name| self.name == Some(name))
    }
}

/// Wire reply. Debug deliberately does not include the bytes.
pub struct Response {
    /// Actual HTTP status.
    pub status: u16,
    /// Caller-owned transport bytes, scrubbed after decoding.
    pub body: Zeroizing<Vec<u8>>,
}

/// Native request future; cancellation drops the transport request.
#[cfg(not(target_arch = "wasm32"))]
pub type TransportFuture<'a> = Pin<Box<dyn Future<Output = Result<Response, Error>> + Send + 'a>>;
/// Browser request future, with JavaScript's single-threaded ownership.
#[cfg(target_arch = "wasm32")]
pub type TransportFuture<'a> = Pin<Box<dyn Future<Output = Result<Response, Error>> + 'a>>;

/// Thin HTTP adapter. Implementations must use the fixed path unchanged, carry
/// credentials only as a sensitive header, reject redirects and suppress body
/// diagnostics. No retries, interpretation or authority logic lives here.
pub trait Transport {
    /// Send exactly one POST. Borrowed inputs remain owned until completion.
    fn post<'a>(
        &'a self,
        path: &'static str,
        bearer: &'a str,
        body: &'a [u8],
    ) -> TransportFuture<'a>;
}

/// One client implementation used by native callers, the CLI and WASM.
pub struct Client<T> {
    transport: T,
    credentials: CredentialProvider,
}

impl<T: Transport> Client<T> {
    /// Use an existing credential provider, resolved freshly for each call.
    pub fn new(transport: T, credentials: CredentialProvider) -> Self {
        Self {
            transport,
            credentials,
        }
    }

    async fn post<R: DeserializeOwned>(
        &self,
        path: &'static str,
        body: &impl Serialize,
    ) -> Result<R, Error> {
        let body = Zeroizing::new(serde_json::to_vec(body).map_err(|_| Error::InvalidRequest)?);
        let bearer = self
            .credentials
            .resolve()
            .await
            .map_err(|_| Error::Credential)?;
        let response = self.transport.post(path, &bearer, &body).await?;
        match response.status {
            200..=299 => serde_json::from_slice(&response.body).map_err(|_| Error::InvalidResponse),
            401 | 403 => Err(Error::PermissionDenied),
            409 => Err(Error::Conflict),
            status => Err(Error::Http(status)),
        }
    }

    /// Create or replace exactly at the supplied scope generation. No replay or
    /// automatic retry can turn a conflict into a successful overwrite.
    pub async fn put(&self, request: &SecretPutRequest) -> Result<SecretMutationReply, Error> {
        validate_put(request).map_err(|_| Error::InvalidRequest)?;
        let reply: SecretMutationReply = self.post("/v1/identity/secrets/put", request).await?;
        if reply.name != request.name
            || reply.kind != request.kind
            || reply.generation <= request.expected_generation
            || reply.version == 0
        {
            return Err(Error::InvalidResponse);
        }
        Ok(reply)
    }

    /// Return plain values and protected metadata. A protected response carrying
    /// any value field is unrepresentable and rejected by the canonical decoder.
    pub async fn list(&self, request: &SecretListRequest) -> Result<SecretListReply, Error> {
        validate_list(request).map_err(|_| Error::InvalidRequest)?;
        let reply: SecretListReply = self.post("/v1/identity/secrets/list", request).await?;
        let mut names = std::collections::HashSet::with_capacity(reply.variables.len());
        for variable in &reply.variables {
            let (name, version) = match &variable.kind {
                SecretVariableKind::Plain(row) => (&row.name, row.version),
                SecretVariableKind::Protected(row) => (&row.name, row.version),
            };
            if !label(name) || version == 0 || !names.insert(name) {
                return Err(Error::InvalidResponse);
            }
        }
        Ok(reply)
    }

    /// Delete at the supplied scope generation, returning metadata only.
    pub async fn delete(
        &self,
        request: &SecretDeleteRequest,
    ) -> Result<SecretMutationReply, Error> {
        validate_delete(request).map_err(|_| Error::InvalidRequest)?;
        let reply: SecretMutationReply = self.post("/v1/identity/secrets/delete", request).await?;
        if reply.name != request.name
            || reply.kind != SecretKind::Deleted
            || reply.generation <= request.expected_generation
            || reply.version == 0
        {
            return Err(Error::InvalidResponse);
        }
        Ok(reply)
    }
}

/// Native HTTPS adapter for the shared client. Local HTTP is restricted to
/// loopback, allowing real in-process conformance providers without bearer leaks.
#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
pub struct HttpTransport {
    endpoint: url::Url,
    client: reqwest::Client,
}

#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
impl HttpTransport {
    /// Create a redirect-free HTTP adapter. Origin validation happens before
    /// any credential resolution or network operation.
    pub fn new(origin: &str) -> Result<Self, Error> {
        Ok(Self {
            endpoint: crate::credential::endpoint(origin).map_err(|_| Error::InvalidRequest)?,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|_| Error::Transport)?,
        })
    }
}

#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
impl Transport for HttpTransport {
    fn post<'a>(
        &'a self,
        path: &'static str,
        bearer: &'a str,
        body: &'a [u8],
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut authorization =
                reqwest::header::HeaderValue::from_str(&format!("Bearer {bearer}"))
                    .map_err(|_| Error::Credential)?;
            authorization.set_sensitive(true);
            let response = self
                .client
                .post(
                    self.endpoint
                        .join(path)
                        .map_err(|_| Error::InvalidRequest)?,
                )
                .header(reqwest::header::AUTHORIZATION, authorization)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_vec())
                .send()
                .await
                .map_err(|_| Error::Transport)?;
            let status = response.status().as_u16();
            // Never consume an error body: even a malicious provider cannot
            // attach a planted value to a propagated error or source chain.
            let body = if (200..300).contains(&status) {
                response
                    .bytes()
                    .await
                    .map_err(|_| Error::Transport)?
                    .to_vec()
            } else {
                Vec::new()
            };
            Ok(Response {
                status,
                body: Zeroizing::new(body),
            })
        })
    }
}

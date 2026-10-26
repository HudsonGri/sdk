use acyclic_harness_core::Result;
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, ProviderPlatform};
use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use futures::StreamExt as _;
use std::collections::BTreeMap;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod owned_bytes;
#[cfg(not(target_arch = "wasm32"))]
mod recording;

/// One streaming JSON request. Headers may contain credentials; deliberately not Debug.
pub struct HttpRequest {
    /// Model-only recording identity. OAuth credential exchanges leave this absent.
    #[cfg(not(target_arch = "wasm32"))]
    pub recording: Option<std::sync::Arc<acyclic_harness_core::http::recording::Identity>>,
    /// Fully resolved endpoint.
    pub url: String,
    /// Protocol and authentication headers.
    pub headers: BTreeMap<String, String>,
    /// Exact encoded JSON.
    pub body: Bytes,
}

/// Response whose body remains lazy until polled.
pub struct HttpResponse {
    /// HTTP status.
    pub status: u16,
    /// Incremental body. Dropping it cancels I/O.
    pub body: BoxProviderStream<'static, Result<Bytes>>,
    /// The response's `Retry-After` delay, when it gave one in seconds.
    pub retry_after: Option<std::time::Duration>,
}

#[cfg(not(target_arch = "wasm32"))]
impl HttpResponse {
    /// Admits the concrete body stream before boxing it on the issued ledger.
    /// Charges received bytes once; downstream codecs charge their own copies/decoding.
    pub fn with_native_body<S>(
        status: u16,
        stream: S,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self>
    where
        S: futures::Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        Ok(Self {
            status,
            body: native::body(stream, Some(work))?,
            retry_after: None,
        })
    }
}

/// Platform HTTP boundary, also used by scripted fixtures.
pub trait HttpTransport: ProviderPlatform {
    /// Whether model exchanges should carry recording identity. Off by default.
    #[cfg(not(target_arch = "wasm32"))]
    fn recording_enabled(&self) -> bool {
        false
    }
    /// Sends exactly one request without retries.
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>>;
    /// Waits `duration` on the platform timer: a redelivery backoff, or a deadline's wake-up.
    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, ()>;
    /// Monotonic time on the platform clock that `delay` waits on, from a fixed origin.
    /// Deadlines are measured on it; timers only wake their measurement.
    fn now(&self) -> std::time::Duration;
    /// `delay` admitted on the original invocation ledger before it exists.
    #[cfg(not(target_arch = "wasm32"))]
    fn delay_with_native_work(
        &self,
        _: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        work.remaining()?;
        Err(acyclic_harness_core::Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native timer",
        })
    }
    /// Whether this transport funds its native callback frames before allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }
    /// Issues one HTTP exchange on the original invocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self,
        _: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'a, HttpResponse>> {
        work.remaining()?;
        Err(acyclic_harness_core::Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native producer",
        })
    }
}

#[cfg(target_arch = "wasm32")]
mod browser;
#[cfg(target_arch = "wasm32")]
pub use browser::BrowserTransport;

pub(crate) fn platform() -> Result<std::sync::Arc<dyn HttpTransport>> {
    #[cfg(not(target_arch = "wasm32"))]
    let transport = ReqwestTransport::new()?;
    #[cfg(target_arch = "wasm32")]
    let transport = BrowserTransport::global()?;
    Ok(std::sync::Arc::new(transport))
}

/// Tokio's monotonic clock (which a paused test runtime drives) from a process-wide origin.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_now() -> std::time::Duration {
    static ORIGIN: std::sync::OnceLock<tokio::time::Instant> = std::sync::OnceLock::new();
    let origin = *ORIGIN.get_or_init(tokio::time::Instant::now);
    tokio::time::Instant::now().saturating_duration_since(origin)
}

/// Pooled native HTTP with redirects disabled.
#[cfg(not(target_arch = "wasm32"))]
pub struct ReqwestTransport {
    client: acyclic_harness_core::http::native::Client,
    recording: Option<recording::Recorder>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ReqwestTransport {
    async fn exchange(
        &self,
        request: HttpRequest,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<HttpResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let mut capture = match (&self.recording, request.recording.as_ref()) {
            (Some(recorder), Some(identity)) => Some(recorder.begin(
                identity,
                &request.url,
                request.headers.keys(),
                &request.body,
                work,
            )?),
            _ => None,
        };
        let response = acyclic_harness_core::http::native::exchange(
            &self.client,
            acyclic_harness_core::http::native::Request {
                method: reqwest::Method::POST,
                url: request.url,
                headers: {
                    let mut headers = request.headers;
                    headers
                        .entry("content-type".into())
                        .or_insert_with(|| "application/json".into());
                    headers
                },
                body: request.body,
                timeout: None,
            },
        )
        .await
        .map_err(|failure| {
            if failure.request_invalid() {
                crate::invalid("invalid HTTP request")
            } else {
                crate::transport_failure(native, failure.kind(), failure.cause())
            }
        })?;
        let status = response.status;
        if let Some(capture) = capture.as_mut() {
            let content_type = response.headers.get(reqwest::header::CONTENT_TYPE).map(|v| v.to_str()).transpose()
                .map_err(|_| crate::invalid("STEP record response content type failed; recovery: recapture a provider response with a valid Content-Type"))?;
            capture.headers(status, content_type, || {
                response
                    .headers
                    .keys()
                    .map(reqwest::header::HeaderName::as_str)
            })?;
        }
        let retry_after = response
            .headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(crate::retry::retry_after);
        let body = response.body.map(move |chunk| {
            chunk.map_err(|failure| {
                crate::transport_failure(native, failure.kind(), failure.cause())
            })
        });
        #[cfg(not(target_arch = "wasm32"))]
        let body = native::body(body, work)?;
        let body = recording::body(body, capture)?;
        #[cfg(target_arch = "wasm32")]
        let body = Box::pin(body);
        Ok(HttpResponse {
            status,
            body,
            retry_after,
        })
    }
    /// Creates the platform transport. Credentials are never forwarded across redirects.
    pub fn new() -> Result<Self> {
        let builder = Self::client_builder();
        let client = builder
            .build()
            .map_err(|_| super::invalid("HTTP client initialization failed"))?;
        let recording = std::env::var_os("ACYCLIC_PROVIDER_RECORD")
            .map(recording::Recorder::new)
            .transpose()?;
        Ok(Self { client, recording })
    }

    /// Explicit recording configuration, shared by embedders and tests. Does not change routing.
    pub fn with_recording(directory: impl Into<std::path::PathBuf>) -> Result<Self> {
        Ok(Self {
            client: Self::client_builder()
                .build()
                .map_err(|_| super::invalid("HTTP client initialization failed"))?,
            recording: Some(recording::Recorder::new(directory.into())?),
        })
    }

    fn client_builder() -> acyclic_harness_core::http::native::ClientBuilder {
        acyclic_harness_core::http::native::client_builder()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "http/http2_tests.rs"]
mod http2_tests;

#[cfg(not(target_arch = "wasm32"))]
impl HttpTransport for ReqwestTransport {
    fn recording_enabled(&self) -> bool {
        self.recording.is_some()
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(self.exchange(
            request,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        ))
    }

    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, ()> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn now(&self) -> std::time::Duration {
        native_now()
    }

    fn delay_with_native_work(
        &self,
        duration: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        acyclic_harness_core::NativeFutureBundle::new(work, move || async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness_core::NativeFutureBundle::new(work, || self.exchange(request, Some(work)))
    }
}

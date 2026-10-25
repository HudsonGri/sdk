//! Native HTTP I/O; only transport-proven unsent requests can be redelivered.

use super::{McpHttpProvider, McpHttpRequest, McpHttpResponse};
use crate::{Error, OperationId, Result};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use std::time::Duration;

/// Native network capability with explicit host credentials.
pub struct NativeMcpHttpProvider(crate::http::native::Client);

impl NativeMcpHttpProvider {
    /// Creates a provider with redirects and implicit HTTP retries disabled.
    pub fn new(headers: reqwest::header::HeaderMap) -> Result<Self> {
        let client = crate::http::native::client_builder()
            .default_headers(headers)
            .build()
            .map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(Self(client))
    }
}

impl McpHttpProvider for NativeMcpHttpProvider {
    fn exchange<'a>(
        &'a self,
        operation: OperationId,
        request: McpHttpRequest,
    ) -> BoxProviderFuture<'a, Result<McpHttpResponse>> {
        Box::pin(async move {
            if request.timeout_ms == 0 || request.maximum_response_bytes == 0 {
                return Err(Error::Invalid("HTTP exchange needs finite bounds".into()));
            }
            let method = reqwest::Method::from_bytes(request.method.as_bytes())
                .map_err(|e| Error::Invalid(e.to_string()))?;
            let deadline =
                tokio::time::Instant::now() + Duration::from_millis(u64::from(request.timeout_ms));
            let body = bytes::Bytes::from(request.body);
            let mut retry = acyclic_stream::runtime::backoff::Backoff::new(
                acyclic_stream::runtime::backoff::Policy {
                    attempts: 3,
                    base_delay: Duration::from_millis(10),
                    max_delay: Duration::from_millis(100),
                },
                &operation.into_bytes(),
            );
            let response = loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    // Every path back to this boundary was proven unsent by the transport.
                    return Err(Error::HttpNotSent {
                        operation,
                        failure: crate::ModelTransportFailure::Timeout,
                    });
                }
                let result = crate::http::native::exchange(
                    &self.0,
                    crate::http::native::Request {
                        method: method.clone(),
                        url: request.endpoint.clone(),
                        headers: request.headers.clone(),
                        body: body.clone(),
                        // One absolute budget covers every connection, wait and response body.
                        timeout: Some(remaining),
                    },
                )
                .await;
                match result {
                    Ok(response) => break response,
                    Err(failure) => {
                        let safe = failure.send_state()
                            == crate::http::native::SendState::NeverSent
                            && !failure.request_invalid()
                            && failure.kind() != crate::ModelTransportFailure::Tls;
                        let remaining =
                            deadline.saturating_duration_since(tokio::time::Instant::now());
                        let wait = safe.then(|| retry.next(None, remaining)).flatten();
                        let error = failure.into_mcp_error(operation);
                        let Some(wait) = wait else {
                            return Err(error);
                        };
                        tokio::time::sleep(wait).await;
                        if tokio::time::Instant::now() >= deadline {
                            return Err(error);
                        }
                    }
                }
            };
            let status = response.status;
            let headers = response
                .headers
                .iter()
                .map(|(key, value)| {
                    value
                        .to_str()
                        .map(|value| (key.as_str().to_owned(), value.to_owned()))
                        .map_err(|e| Error::Invalid(e.to_string()))
                })
                .collect::<Result<_>>()?;
            let mut received = 0_usize;
            let body = response.body.map(move |chunk| {
                let chunk = chunk.map_err(|failure| failure.into_mcp_error(operation))?;
                received = received.saturating_add(chunk.len());
                if received as u64 > u64::from(request.maximum_response_bytes) {
                    return Err(Error::Indeterminate(operation));
                }
                Ok(chunk)
            });
            Ok(McpHttpResponse {
                status,
                headers,
                body: Box::pin(body),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>> {
        // HTTP request IDs do not constitute a server idempotency/receipt API.
        Box::pin(async { Ok(None) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::{McpToolTransport, http::HttpMcpTransport};
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    struct Recovering {
        address: std::net::SocketAddr,
        resolutions: AtomicUsize,
    }
    impl reqwest::dns::Resolve for Recovering {
        fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
            let failed = self.resolutions.fetch_add(1, Ordering::SeqCst) == 0;
            let address = self.address;
            Box::pin(async move {
                if failed {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "fixture unsent resolution",
                    )
                    .into())
                } else {
                    Ok(Box::new(std::iter::once(address)) as reqwest::dns::Addrs)
                }
            })
        }
    }

    #[tokio::test]
    async fn real_local_http_json_sse_and_lost_response_never_repost() -> Result<()> {
        for direct in [false, true] {
            for mode in [
                "json",
                "sse",
                "lost",
                "redirect",
                "recover-json",
                "recover-sse",
            ] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let address = listener
                    .local_addr()
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let operation = OperationId::from_bytes([9; 16]);
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener
                        .accept()
                        .await
                        .map_err(|e| Error::Storage(e.to_string()))?;
                    let mut input = Vec::new();
                    let mut buffer = [0_u8; 512];
                    loop {
                        let count = socket
                            .read(&mut buffer)
                            .await
                            .map_err(|e| Error::Storage(e.to_string()))?;
                        if count == 0 {
                            return Err(Error::Invalid("fixture request disconnected".into()));
                        }
                        input.extend_from_slice(&buffer[..count]);
                        if input.len() > 8192 {
                            return Err(Error::Invalid("fixture request bound".into()));
                        }
                        if let Some(end) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                            let header = std::str::from_utf8(&input[..end])
                                .map_err(|e| Error::Invalid(e.to_string()))?;
                            let length = header
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|n| n.trim().parse::<usize>().ok())
                                })
                                .ok_or_else(|| Error::Invalid("fixture length absent".into()))?;
                            if input.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    let request =
                        String::from_utf8(input).map_err(|e| Error::Invalid(e.to_string()))?;
                    assert!(request.contains("mcp-session-id: session"));
                    assert!(request.contains("mcp-protocol-version: 2025-11-25"));
                    assert!(request.contains("tools/call"));
                    if mode == "lost" {
                        drop(socket);
                        return Ok(listener);
                    }
                    let result=json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"content":[{"type":"text","text":"local"}],"isError":false}}).to_string();
                    let response = if mode == "redirect" {
                        format!(
                            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                    } else {
                        let body = if mode.ends_with("sse") {
                            format!("data: {result}\r\n\r\n")
                        } else {
                            result
                        };
                        let media = if mode.ends_with("sse") {
                            "text/event-stream"
                        } else {
                            "application/json"
                        };
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: {media}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                    };
                    socket
                        .write_all(response.as_bytes())
                        .await
                        .map_err(|e| Error::Storage(e.to_string()))?;
                    socket
                        .shutdown()
                        .await
                        .map_err(|e| Error::Storage(e.to_string()))?;
                    Ok::<_, Error>(listener)
                });
                let resolver = Arc::new(Recovering {
                    address,
                    resolutions: AtomicUsize::new(0),
                });
                let (provider, endpoint) = if mode.starts_with("recover-") {
                    let client = crate::http::native::client_builder()
                        .no_proxy()
                        .dns_resolver(resolver.clone())
                        .build()
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    (
                        Arc::new(NativeMcpHttpProvider(client)),
                        format!("http://recover.invalid:{}/mcp", address.port()),
                    )
                } else {
                    (
                        Arc::new(NativeMcpHttpProvider::new(
                            reqwest::header::HeaderMap::new(),
                        )?),
                        format!("http://{address}/mcp"),
                    )
                };
                let result = if direct {
                    let result = provider.exchange(operation, McpHttpRequest {
                    endpoint, method: "POST".into(),
                    headers: [("mcp-session-id".into(), "session".into()), ("mcp-protocol-version".into(), "2025-11-25".into())].into(),
                    body: json!({"jsonrpc":"2.0", "id":operation.to_string(), "method":"tools/call", "params":{"name":"echo","arguments":{}}}).to_string().into_bytes(),
                    timeout_ms:1000, maximum_response_bytes:8192,
                }).await;
                    match result {
                        Ok(mut response) => {
                            if mode == "redirect" {
                                assert_eq!(response.status, 307);
                            } else {
                                assert_eq!(response.status, 200);
                                let mut body = Vec::new();
                                while let Some(chunk) = response.body.next().await {
                                    body.extend_from_slice(&chunk?);
                                }
                                assert!(
                                    String::from_utf8(body)
                                        .map_err(|error| Error::Invalid(error.to_string()))?
                                        .contains("local")
                                );
                            }
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    let transport = HttpMcpTransport::new(
                        provider,
                        endpoint,
                        Some("session".into()),
                        8192,
                        1000,
                        None,
                    )?;
                    let result = transport.call(operation, "echo", json!({}), None).await;
                    assert!(transport.reconcile(operation, None).await?.is_none());
                    result.map(|result| {
                        assert_eq!(result.content, [rmcp::model::ContentBlock::text("local")]);
                    })
                };
                if mode.starts_with("recover-") {
                    assert_eq!(resolver.resolutions.load(Ordering::SeqCst), 2);
                }
                // The exchange future has finished and owns no detached dispatch task.
                // Inspect the retained listener's backlog without a timer or a race with server shutdown.
                let listener = server.await.map_err(|e| Error::Storage(e.to_string()))??;
                assert!(
                    matches!(listener.into_std().map_err(|error| Error::Storage(error.to_string()))?.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
                );
                if mode != "lost" && (mode != "redirect" || direct) {
                    result?;
                } else {
                    assert!(matches!(result, Err(Error::Indeterminate(_))));
                }
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn failed_resolution_is_never_sent_not_indeterminate() -> Result<()> {
        struct Refused(AtomicUsize);
        impl reqwest::dns::Resolve for Refused {
            fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "fixture resolver returned no address",
                    )
                    .into())
                })
            }
        }
        // A bound, non-listening socket does not imply an immediate refusal on
        // every OS. Failing before an address exists proves no HTTP bytes can
        // have been dispatched, without a port race or a network deadline.
        let resolver = Arc::new(Refused(AtomicUsize::new(0)));
        let endpoint = "http://refused.invalid/mcp";
        let operation = OperationId::from_bytes([21; 16]);
        let provider = Arc::new(NativeMcpHttpProvider(
            crate::http::native::client_builder()
                .no_proxy()
                .dns_resolver(resolver.clone())
                .build()
                .map_err(|error| Error::Invalid(error.to_string()))?,
        ));
        let result = provider
            .exchange(
                operation,
                McpHttpRequest {
                    endpoint: endpoint.into(),
                    method: "POST".into(),
                    headers: Default::default(),
                    body: b"{}".to_vec(),
                    timeout_ms: 1000,
                    maximum_response_bytes: 1024,
                },
            )
            .await;
        assert!(matches!(result, Err(Error::HttpNotSent { operation: actual,
            failure: crate::ModelTransportFailure::Connect }) if actual == operation));
        let transport = HttpMcpTransport::new(
            provider,
            endpoint.into(),
            Some("session".into()),
            1024,
            1000,
            None,
        )?;
        assert!(
            matches!(transport.call(operation, "echo", json!({}), None).await,
            Err(Error::HttpNotSent { operation: actual, failure: crate::ModelTransportFailure::Connect })
                if actual == operation)
        );
        assert_eq!(resolver.0.load(Ordering::SeqCst), 8);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn connection_redelivery_consumes_one_absolute_exchange_budget() -> Result<()> {
        struct Slow(AtomicUsize);
        impl reqwest::dns::Resolve for Slow {
            fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "unsent fixture",
                    )
                    .into())
                })
            }
        }
        for direct in [false, true] {
            let resolver = Arc::new(Slow(AtomicUsize::new(0)));
            let provider = Arc::new(NativeMcpHttpProvider(
                crate::http::native::client_builder()
                    .no_proxy()
                    .dns_resolver(resolver.clone())
                    .build()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ));
            let started = tokio::time::Instant::now();
            let operation = OperationId::from_bytes([31; 16]);
            if direct {
                assert!(
                    provider
                        .exchange(
                            operation,
                            McpHttpRequest {
                                endpoint: "http://slow.invalid/mcp".into(),
                                method: "POST".into(),
                                headers: Default::default(),
                                body: b"{}".to_vec(),
                                timeout_ms: 1000,
                                maximum_response_bytes: 1024,
                            }
                        )
                        .await
                        .is_err()
                );
            } else {
                let transport = HttpMcpTransport::new(
                    provider,
                    "http://slow.invalid/mcp".into(),
                    None,
                    1024,
                    1000,
                    None,
                )?;
                assert!(
                    transport
                        .call(operation, "echo", json!({}), None)
                        .await
                        .is_err()
                );
            }
            assert!(
                resolver.0.load(Ordering::SeqCst) >= 2,
                "no unsent redelivery occurred"
            );
            assert!(
                tokio::time::Instant::now() - started <= Duration::from_millis(1000),
                "exchange restarted its elapsed budget"
            );
        }
        Ok(())
    }
}

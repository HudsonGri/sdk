//! Optional native HTTP I/O, without request replay or a second receipt journal.

use super::{McpHttpProvider, McpHttpRequest, McpHttpResponse};
use crate::{Error, OperationId, Result};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use std::time::Duration;

/// Native network capability with explicit host credentials.
pub struct NativeMcpHttpProvider(crate::http::native::Client);

impl NativeMcpHttpProvider {
    /// Creates a provider with redirects and retries disabled.
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
            let response = crate::http::native::exchange(
                &self.0,
                crate::http::native::Request {
                    method,
                    url: request.endpoint,
                    headers: request.headers,
                    body: request.body.into(),
                    timeout: Some(Duration::from_millis(u64::from(request.timeout_ms))),
                },
            )
            .await
            .map_err(|failure| failure.into_mcp_error(operation))?;
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
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[tokio::test]
    async fn real_local_http_json_sse_and_lost_response_never_repost() -> Result<()> {
        for mode in ["json", "sse", "lost", "redirect"] {
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
                    let body = if mode == "sse" {
                        format!("data: {result}\r\n\r\n")
                    } else {
                        result
                    };
                    let media = if mode == "sse" {
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
            let provider = Arc::new(NativeMcpHttpProvider::new(
                reqwest::header::HeaderMap::new(),
            )?);
            let transport = HttpMcpTransport::new(
                provider,
                format!("http://{address}/mcp"),
                Some("session".into()),
                8192,
                1000,
                None,
            )?;
            let result = transport.call(operation, "echo", json!({}), None).await;
            assert!(transport.reconcile(operation, None).await?.is_none());
            // The exchange future has finished and owns no detached dispatch task.
            // Inspect the retained listener's backlog without a timer or a race with server shutdown.
            let listener = server.await.map_err(|e| Error::Storage(e.to_string()))??;
            assert!(
                matches!(listener.into_std().map_err(|error| Error::Storage(error.to_string()))?.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
            );
            if matches!(mode, "json" | "sse") {
                assert_eq!(result?.content, [rmcp::model::ContentBlock::text("local")]);
            } else {
                assert!(matches!(result, Err(Error::Indeterminate(_))));
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn a_reserved_unlistening_endpoint_is_never_sent_not_indeterminate() -> Result<()> {
        let socket =
            tokio::net::TcpSocket::new_v4().map_err(|error| Error::Storage(error.to_string()))?;
        socket
            .bind("127.0.0.1:0".parse().unwrap())
            .map_err(|error| Error::Storage(error.to_string()))?;
        let address = socket
            .local_addr()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let operation = OperationId::from_bytes([21; 16]);
        let provider = Arc::new(NativeMcpHttpProvider::new(
            reqwest::header::HeaderMap::new(),
        )?);
        let result = provider
            .exchange(
                operation,
                McpHttpRequest {
                    endpoint: format!("http://{address}/mcp"),
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
            format!("http://{address}/mcp"),
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
        drop(socket);
        Ok(())
    }
}

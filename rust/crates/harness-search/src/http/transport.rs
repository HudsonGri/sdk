pub(crate) const MAXIMUM_RESPONSE_BYTES: usize = 1_048_576;
use crate::tool::edit::{charge, reserve_string};
use crate::url_fetch::{MAXIMUM_URL_BYTES, rejection};
use crate::{Error, NativeCustody, NativeWork, Result};
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::TokioIo;
use reqwest::{Url, header::HeaderValue};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub(crate) mod socket;

pub(crate) const HTTP_BUFFER_BYTES: usize = 32_768;

/// Funds the bounded HTTP parser buffer and request/header copies before connecting.
pub(crate) fn request_scratch(endpoint: &Url, work: NativeWork<'_>) -> Result<NativeCustody> {
    let copies = endpoint
        .as_str()
        .len()
        .checked_mul(4)
        .ok_or_else(|| super::super::error(work, "HTTP request ownership overflow"))?;
    let bytes = copies
        .checked_add(HTTP_BUFFER_BYTES + 1024)
        .ok_or_else(|| super::super::error(work, "HTTP request ownership overflow"))?;
    let held = crate::tool::edit::allocation(work, bytes)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 8,
            bytes_copied: copies as u64,
            items_examined: endpoint.as_str().len() as u64,
            ..Default::default()
        })?;
    }
    Ok(held)
}

pub(crate) fn tls(endpoint: &Url) -> Result<Option<Arc<rustls::ClientConfig>>> {
    if endpoint.scheme() != "https" {
        return Ok(None);
    }
    tls_config().map(Some)
}

pub(crate) fn tls_config() -> Result<Arc<rustls::ClientConfig>> {
    use rustls_platform_verifier::BuilderVerifierExt as _;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .and_then(|builder| builder.with_platform_verifier())
    .map_err(|_| crate::Error::Invalid("cannot create search TLS client".into()))?
    .with_no_client_auth();
    Ok(Arc::new(config))
}

pub(crate) fn copy_text(text: &str, held: &mut NativeCustody) -> Result<String> {
    let mut output = String::new();
    reserve_string(held, &mut output, text.len())?;
    charge(held, text.len() as u64, 1)?;
    output.push_str(text);
    Ok(output)
}
pub(crate) fn public_address(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
                || a == 0
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 192 && b == 0 && c == 0)
                || (a == 198 && (b == 18 || b == 19)))
        }
        IpAddr::V6(ip) => {
            // Permit global unicast only. IPv4-mapped addresses inherit IPv4 policy.
            if let Some(ip) = ip.to_ipv4_mapped() {
                return public_address(IpAddr::V4(ip));
            }
            let segments = ip.segments();
            (segments[0] & 0xe000) == 0x2000
                && !(segments[0] == 0x2001 && (segments[1] <= 0x01ff || segments[1] == 0x0db8))
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && (segments[1] & 0xf000) == 0)
        }
    }
}
pub(crate) async fn await_io<F: std::future::Future>(
    future: F,
    work: crate::NativeWork<'_>,
) -> Result<F::Output> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.remaining()?;
        return tokio::select! {
            biased;
            _ = work.cancellation().cancelled() => Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into()),
            result = future => Ok(result),
        };
    }
    let _ = work;
    Ok(future.await)
}

pub(crate) fn transport_error(work: crate::NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason,
        };
    }
    let _ = work;
    Error::Storage(reason.into())
}

pub(crate) async fn search(
    method: http::Method,
    endpoint: &Url,
    key: &HeaderValue,
    body: bytes::Bytes,
    tls: Option<&Arc<rustls::ClientConfig>>,
    work: NativeWork<'_>,
    failure: &mut Option<crate::Error>,
) -> Result<(Vec<u8>, NativeCustody)> {
    let explicitly_local = explicitly_local(endpoint);
    match send(
        Request {
            method,
            endpoint,
            key: Some(key),
            body,
            tls,
            require_public_address: !explicitly_local,
            redirects: false,
        },
        work,
        failure,
    )
    .await?
    {
        Response::Content { bytes, held, .. } => Ok((bytes, held)),
        Response::Redirect(..) => Err(rejection(work, "search redirects are forbidden")),
    }
}

pub(crate) enum Response {
    Redirect(String, NativeCustody),
    Content {
        bytes: Vec<u8>,
        held: NativeCustody,
        status: u16,
        content_type: Option<String>,
    },
}

pub(crate) struct Request<'a> {
    pub(crate) method: http::Method,
    pub(crate) endpoint: &'a Url,
    pub(crate) key: Option<&'a HeaderValue>,
    pub(crate) body: bytes::Bytes,
    pub(crate) tls: Option<&'a Arc<rustls::ClientConfig>>,
    pub(crate) require_public_address: bool,
    pub(crate) redirects: bool,
}

pub(crate) async fn send(
    request: Request<'_>,
    work: NativeWork<'_>,
    failure: &mut Option<crate::Error>,
) -> Result<Response> {
    let Request {
        method,
        endpoint,
        key,
        body,
        tls,
        require_public_address,
        redirects,
    } = request;
    let host = endpoint
        .host_str()
        .ok_or_else(|| rejection(work, "invalid fetch host"))?;
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| rejection(work, "invalid fetch port"))?;
    // Keep request/header/URI temporaries funded until the connection has dropped.
    let scratch = request_scratch(endpoint, work)?;
    let addresses = tokio::net::lookup_host((host.trim_matches(['[', ']']), port))
        .await
        .map_err(|_| transport_error(work, "cannot resolve fetch source"))?;
    let socket = validated_connect(addresses, require_public_address, work).await?;
    let socket = socket::Socket::new(socket, work, failure)?;
    let host_header = match endpoint.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };
    let mut target = endpoint.path().to_owned();
    if let Some(query) = endpoint.query() {
        target.push('?');
        target.push_str(query);
    }
    let mut builder = http::Request::builder()
        .method(method)
        .uri(target)
        .header(http::header::HOST, host_header)
        .header(http::header::CONNECTION, "close")
        .header(http::header::ACCEPT_ENCODING, "identity");
    if let Some(key) = key {
        builder = builder
            .header("x-api-key", key.clone())
            .header(http::header::CONTENT_TYPE, "application/json");
    }
    let request = builder
        .body(Full::new(body))
        .map_err(|_| rejection(work, "cannot construct fetch request"))?;
    let result = if endpoint.scheme() == "https" {
        let tls = tls.ok_or_else(|| rejection(work, "HTTP TLS configuration missing"))?;
        let name =
            rustls::pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_owned())
                .map_err(|_| rejection(work, "invalid fetch TLS name"))?;
        let socket = tokio_rustls::TlsConnector::from(tls.clone())
            .connect(name, socket)
            .await
            .map_err(|_| transport_error(work, "fetch TLS handshake failed"))?;
        exchange(socket, request, redirects, work).await
    } else {
        exchange(socket, request, redirects, work).await
    };
    drop(scratch);
    result
}

async fn validated_connect(
    addresses: impl Iterator<Item = std::net::SocketAddr>,
    require_public_address: bool,
    work: NativeWork<'_>,
) -> Result<tokio::net::TcpStream> {
    let (validated, _held) = validated_addresses(addresses, require_public_address, work)?;
    // Retain ordinary address fallback without a second hostname resolution.
    tokio::net::TcpStream::connect(validated.as_slice())
        .await
        .map_err(|_| transport_error(work, "cannot connect fetch source"))
}

fn validated_addresses(
    addresses: impl Iterator<Item = std::net::SocketAddr>,
    require_public_address: bool,
    work: NativeWork<'_>,
) -> Result<(Vec<std::net::SocketAddr>, NativeCustody)> {
    let mut held =
        crate::tool::edit::allocation(work, std::mem::size_of::<Vec<std::net::SocketAddr>>())?;
    let mut validated = Vec::new();
    for address in addresses {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        if require_public_address && !public_address(address.ip()) {
            return Err(rejection(
                work,
                "fetch redirect targets a non-public address",
            ));
        }
        let length = validated.len() + 1;
        crate::tool::edit::reserve_vec(&mut held, &mut validated, length)?;
        crate::tool::edit::charge(&held, std::mem::size_of_val(&address) as u64, 0)?;
        validated.push(address);
    }
    if validated.is_empty() {
        return Err(transport_error(work, "cannot resolve fetch source"));
    }
    Ok((validated, held))
}

async fn exchange<I: AsyncRead + AsyncWrite + Unpin>(
    socket: I,
    request: http::Request<Full<bytes::Bytes>>,
    redirects: bool,
    work: NativeWork<'_>,
) -> Result<Response> {
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(HTTP_BUFFER_BYTES)
        .handshake(TokioIo::new(socket))
        .await
        .map_err(|_| transport_error(work, "cannot initialize fetch connection"))?;
    let exchange = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|_| transport_error(work, "fetch HTTP request failed"))?;
        let status = response.status();
        if redirects && matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            let location = response
                .headers()
                .get(http::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .filter(|location| !location.is_empty() && location.len() <= MAXIMUM_URL_BYTES)
                .ok_or_else(|| rejection(work, "fetch redirect has no bounded location"))?;
            let mut held = crate::tool::edit::allocation(work, std::mem::size_of::<String>())?;
            let location = copy_text(location, &mut held)?;
            return Ok(Response::Redirect(location, held));
        }
        if !status.is_success() {
            return Err(rejection(work, "fetch HTTP request rejected"));
        }
        if response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
        {
            return Err(rejection(work, "fetch response exceeds byte limit"));
        }
        for encoding in response.headers().get_all(http::header::CONTENT_ENCODING) {
            if !encoding
                .to_str()
                .is_ok_and(|value| value.eq_ignore_ascii_case("identity"))
            {
                return Err(rejection(
                    work,
                    "fetch response uses unsupported content encoding",
                ));
            }
        }
        let mut held = crate::tool::edit::allocation(work, std::mem::size_of::<Vec<u8>>())?;
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .map(|value| {
                let value = value
                    .to_str()
                    .map_err(|_| rejection(work, "invalid fetch content type"))?;
                copy_text(value, &mut held)
            })
            .transpose()?;
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| transport_error(work, "cannot read fetch response"))?;
            let Ok(chunk) = frame.into_data() else {
                continue;
            };
            if chunk.len() > MAXIMUM_RESPONSE_BYTES - bytes.len() {
                return Err(rejection(work, "fetch response exceeds byte limit"));
            }
            let length = bytes.len() + chunk.len();
            crate::tool::edit::reserve_vec(&mut held, &mut bytes, length)?;
            crate::tool::edit::charge(&held, chunk.len() as u64, 1)?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(Response::Content {
            bytes,
            held,
            status: status.as_u16(),
            content_type,
        })
    };
    tokio::pin!(exchange, connection);
    tokio::select! { biased;
        result = &mut exchange => result,
        result = &mut connection => {
            result.map_err(|_| transport_error(work, "fetch connection failed"))?;
            exchange.await
        }
    }
}

#[cfg(test)]
mod tests;

pub(crate) fn explicitly_local(endpoint: &Url) -> bool {
    endpoint.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| !public_address(ip))
    })
}

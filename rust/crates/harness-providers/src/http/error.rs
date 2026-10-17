use super::*;
use futures::StreamExt as _;

pub(crate) async fn response_error(
    mut response: HttpResponse,
    maximum: usize,
    #[cfg(not(target_arch = "wasm32"))] work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<acyclic_harness::Error> {
    #[cfg(not(target_arch = "wasm32"))]
    let _custody = work.map(|work| crate::native::reserve(work, maximum as u64, 1, 0)).transpose()?;
    let mut body = Vec::with_capacity(maximum);
    let mut interruption = None;
    while body.len() < maximum {
        match response.body.next().await {
            Some(Ok(bytes)) => {
                let length = bytes.len().min(maximum - body.len());
                body.extend_from_slice(&bytes[..length]);
            }
            Some(Err(error)) => {
                #[cfg(not(target_arch = "wasm32"))]
                if !matches!(error, acyclic_harness::Error::Storage(_) | acyclic_harness::Error::Invalid(_) | acyclic_harness::Error::Unauthorized(_) | acyclic_harness::Error::NativeBoundary { .. } | acyclic_harness::Error::NativeDiagnostic { .. }) { return Err(error); }
                interruption = Some(error);
                break;
            }
            None => break,
        }
    }
    let detail = String::from_utf8_lossy(&body);
    let diagnostic = |message: std::fmt::Arguments<'_>| {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            let (message, custody) = work.display(message)?;
            return Ok(acyclic_harness::Error::NativeDiagnostic {
                code: crate::http_error_code(response.status), message, custody,
            });
        }
        Ok(crate::http_error_from_message(response.status, message.to_string()))
    };
    match interruption.as_ref() {
        Some(error) => diagnostic(format_args!("model HTTP status {}: {}; {}", response.status, detail, error)),
        None if detail.is_empty() => diagnostic(format_args!("model HTTP status {}", response.status)),
        None => diagnostic(format_args!("model HTTP status {}: {}", response.status, detail)),
    }
}

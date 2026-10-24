use super::{HttpRequest, HttpResponse, HttpTransport};
use crate::invalid;
use acyclic_harness_core::Result;
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use js_sys::{Function, Promise, Reflect, Uint8Array};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;

use acyclic_harness_core::http::browser::timer;

/// Browser HTTP with an explicitly owned fetch function.
pub struct BrowserTransport(Function);

struct Body {
    receiver: JsValue,
    read: Function,
    cancel: Function,
}
impl Drop for Body {
    fn drop(&mut self) {
        let _ = self.cancel.call0(&self.receiver);
    }
}

impl BrowserTransport {
    /// Injects platform I/O without replacing any protocol implementation.
    pub fn new(fetch: Function) -> Self {
        Self(fetch)
    }

    /// Selects fetch from the current browser or worker realm.
    pub fn global() -> Result<Self> {
        let fetch = Reflect::get(&js_sys::global(), &JsValue::from_str("fetch"))
            .map_err(|_| invalid("browser fetch is unavailable"))?
            .dyn_into::<Function>()
            .map_err(|_| invalid("browser fetch must be a function"))?;
        Ok(Self::new(fetch))
    }
}

fn interrupted() -> acyclic_harness_core::Error {
    crate::transport_failure(
        false,
        acyclic_harness_core::ModelTransportFailure::Interrupted,
        "browser HTTP exchange interrupted",
    )
}

impl HttpTransport for BrowserTransport {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let mut headers = request.headers;
            headers
                .entry("content-type".into())
                .or_insert_with(|| "application/json".into());
            let receiver: JsValue = acyclic_harness_core::http::browser::start(
                self.0.clone(),
                acyclic_harness_core::http::browser::Request {
                    method: "POST".into(),
                    url: request.url,
                    headers,
                    body: request.body.into(),
                    timeout_ms: None,
                },
                None,
            )?
            .into();
            let function = |name: &str| -> Result<Function> {
                Reflect::get(&receiver, &JsValue::from_str(name))
                    .map_err(|_| invalid("browser exchange missing function"))?
                    .dyn_into::<Function>()
                    .map_err(|_| invalid("invalid browser exchange function"))
            };
            let body = Body {
                read: function("read")?,
                cancel: function("cancel")?,
                receiver,
            };
            let head = Reflect::get(&body.receiver, &JsValue::from_str("response"))
                .map_err(|_| interrupted())?;
            let head = JsFuture::from(Promise::resolve(&head))
                .await
                .map_err(|_| interrupted())?;
            let status: u16 = serde_wasm_bindgen::from_value(
                Reflect::get(&head, &JsValue::from_str("status")).map_err(|_| interrupted())?,
            )
            .map_err(|_| interrupted())?;
            let headers =
                Reflect::get(&head, &JsValue::from_str("headers")).map_err(|_| interrupted())?;
            let retry_after = Reflect::get(&headers, &JsValue::from_str("retry-after"))
                .ok()
                .and_then(|value| value.as_string())
                .and_then(|value| crate::retry::retry_after(&value));
            let stream = futures::stream::try_unfold(body, |body| async move {
                let value = body.read.call0(&body.receiver).map_err(|_| interrupted())?;
                let value = JsFuture::from(Promise::resolve(&value))
                    .await
                    .map_err(|_| interrupted())?;
                if value.is_null() {
                    return Ok(None);
                }
                let value = value
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| invalid("browser stream chunk must be bytes"))?;
                Ok(Some((Bytes::from(value.to_vec()), body)))
            });
            Ok(HttpResponse {
                status,
                retry_after,
                body: Box::pin(stream),
            })
        })
    }

    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, ()> {
        Box::pin(timer::delay(duration))
    }

    fn now(&self) -> std::time::Duration {
        timer::now()
    }
}

#[cfg(test)]
#[path = "browser_tests.rs"]
mod tests;

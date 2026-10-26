//! Byte custody adapter over the shared SSE grammar.
use super::*;

/// Incremental subscription decoder. A chunk is drained before another is admitted.
#[wasm_bindgen(js_name = SseDecoder)]
pub struct SseDecoder {
    decoder: acyclic_harness_core::http::sse::Decoder,
    chunk: Vec<u8>,
    offset: usize,
    maximum: usize,
}

#[wasm_bindgen(js_class = SseDecoder)]
impl SseDecoder {
    /// Creates a decoder with a per-event byte bound.
    #[wasm_bindgen(constructor)]
    pub fn new(
        #[wasm_bindgen(unchecked_param_type = "number")] maximum: JsValue,
    ) -> Result<SseDecoder, JsValue> {
        let maximum: u64 = from_js(maximum)?;
        let maximum = usize::try_from(maximum)
            .ok()
            .filter(|value| *value != 0)
            .ok_or_else(|| {
                JsValue::from(js_sys::RangeError::new(
                    "SSE bound exceeds this host or is zero",
                ))
            })?;
        Ok(Self {
            decoder: acyclic_harness_core::http::sse::Decoder::subscription(),
            chunk: Vec::new(),
            offset: 0,
            maximum,
        })
    }

    /// Retains one host chunk; previous input must have been consumed.
    pub fn push(&mut self, chunk: js_sys::Uint8Array) -> Result<(), JsValue> {
        if self.offset != self.chunk.len() {
            return Err(JsValue::from_str("SSE chunk is not drained"));
        }
        self.chunk = super::bytes::snapshot(&chunk, |_| Ok(()))?.0;
        self.offset = 0;
        Ok(())
    }

    /// Decodes lazily so cancellation can stop before the next buffered event.
    #[wasm_bindgen(js_name = nextFrame)]
    pub fn next_frame(&mut self) -> Result<JsValue, JsValue> {
        while let Some(&byte) = self.chunk.get(self.offset) {
            self.offset += 1;
            if let Some(frame) = self.decoder.push(byte, self.maximum).map_err(sse_error)? {
                return sse_frame(frame);
            }
        }
        self.chunk.clear();
        self.offset = 0;
        Ok(JsValue::UNDEFINED)
    }

    /// Rejects a disconnect inside an event without publishing its cursor.
    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        self.decoder
            .finish()
            .map_err(sse_error)?
            .map_or(Ok(JsValue::UNDEFINED), sse_frame)
    }
}

fn sse_frame(frame: acyclic_harness_core::http::sse::Frame) -> Result<JsValue, JsValue> {
    // Struct fields have one object representation at the JavaScript boundary.
    // A serde_json::Value object is a map and becomes a JavaScript Map instead.
    #[derive(Serialize)]
    struct SubscriptionFrame<'a> {
        id: &'a str,
        event: &'a str,
        data: &'a str,
    }
    let metadata = frame
        .metadata
        .ok_or_else(|| JsValue::from_str("subscription metadata missing"))?;
    to_js(&SubscriptionFrame {
        id: &metadata.id,
        event: &metadata.event,
        data: &frame.data,
    })
}

fn sse_error(error: acyclic_harness_core::Error) -> JsValue {
    match error {
        acyclic_harness_core::Error::Invalid(reason) => js_sys::RangeError::new(&reason).into(),
        other => js_sys::Error::new(&other.to_string()).into(),
    }
}

/// Creates the protocol-neutral exchange used by browser HTTP adapters.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = browserHttpExchange)]
pub fn browser_http_exchange(
    fetch: js_sys::Function,
    method: String,
    url: String,
    headers: JsValue,
    #[wasm_bindgen(unchecked_param_type = "Uint8Array | Blob")] body: JsValue,
    timeout_ms: JsValue,
    signal: Option<web_sys::AbortSignal>,
) -> Result<JsValue, JsValue> {
    let headers = from_js(headers)?;
    let timeout_ms = if timeout_ms.is_undefined() || timeout_ms.is_null() {
        None
    } else {
        Some(from_js(timeout_ms)?)
    };
    use acyclic_harness_core::http::browser::Body;
    use wasm_bindgen::JsCast as _;
    let body = if let Some(bytes) = body.dyn_ref::<js_sys::Uint8Array>() {
        Body::Bytes(super::bytes::snapshot(bytes, |_| Ok(()))?.0.into())
    } else {
        Body::Blob(body.dyn_into::<web_sys::Blob>().map_err(|_| {
            JsValue::from(js_sys::TypeError::new(
                "HTTP body must be Uint8Array or Blob",
            ))
        })?)
    };
    acyclic_harness_core::http::browser::start(
        fetch,
        acyclic_harness_core::http::browser::Request {
            method,
            url,
            headers,
            body,
            timeout_ms,
        },
        signal,
    )
    .map(JsValue::from)
    .map_err(js_error)
}

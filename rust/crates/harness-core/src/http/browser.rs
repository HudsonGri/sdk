//! Fetch I/O shared by browser model and MCP adapters. No protocol or replay policy here.
use crate::{Error, Result};

/// Platform timers shared by browser HTTP and model retry adapters.
pub mod timer;
use js_sys::{Function, Object, Promise, Reflect, Uint8Array};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};
use wasm_bindgen::{JsCast as _, JsValue, closure::Closure};
use wasm_bindgen_futures::{JsFuture, future_to_promise};
use web_sys::{
    AbortController, AbortSignal, Headers, ReadableStreamDefaultReader, RequestCredentials,
    RequestInit, RequestRedirect, Response,
};

/// Explicit browser request. Credentials and redirect policy are owned by this layer.
pub struct Request {
    /// Pinned HTTP method.
    pub method: String,
    /// Pinned endpoint.
    pub url: String,
    /// Explicit headers.
    pub headers: BTreeMap<String, String>,
    /// Exact request body.
    pub body: bytes::Bytes,
    /// Optional whole-exchange deadline, retained through body consumption.
    pub timeout_ms: Option<u32>,
}

struct State {
    abort: AbortController,
    reader: RefCell<Option<ReadableStreamDefaultReader>>,
    pending: Cell<bool>,
    timer: RefCell<Option<(timer::Timeout, Closure<dyn FnMut()>)>>,
    signal: RefCell<Option<(AbortSignal, Closure<dyn FnMut()>)>>,
}

impl State {
    fn arm(self: &Rc<Self>, remaining: u32) -> Result<()> {
        let interval = remaining.min(i32::MAX as u32);
        let weak = Rc::downgrade(self);
        let callback = Closure::new(move || {
            if let Some(state) = weak.upgrade() {
                if remaining > interval {
                    if state.arm(remaining - interval).is_err() {
                        state.cancel();
                    }
                } else {
                    state.cancel();
                }
            }
        });
        let timeout = timer::schedule(callback.as_ref(), interval).ok_or_else(invalid)?;
        *self.timer.borrow_mut() = Some((timeout, callback));
        Ok(())
    }
    fn finish(&self) {
        self.timer.borrow_mut().take();
        if let Some((signal, callback)) = self.signal.borrow_mut().take() {
            let _ = signal
                .remove_event_listener_with_callback("abort", callback.as_ref().unchecked_ref());
        }
        if !self.pending.get() {
            if let Some(reader) = self.reader.borrow_mut().take() {
                reader.release_lock();
            }
        }
    }
    fn cancel(&self) {
        self.abort.abort();
        if let Some(reader) = self.reader.borrow_mut().take() {
            // An injected fetch may own a body independently of its AbortSignal.
            // Cancel the admitted reader too, including an outstanding read.
            let cancelled = reader.cancel();
            reader.release_lock();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = JsFuture::from(cancelled).await;
            });
        }
        self.finish();
    }
}
impl Drop for State {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn failure() -> JsValue {
    js_sys::Error::new("browser HTTP exchange interrupted").into()
}
fn invalid() -> Error {
    Error::Invalid("invalid browser HTTP host or request".into())
}
fn set(object: &Object, name: &str, value: &JsValue) -> Result<()> {
    Reflect::set(object, &JsValue::from_str(name), value)
        .map(|_| ())
        .map_err(|_| invalid())
}

/// Starts one fetch, returning a cancellable lazy exchange. Never follows redirects or retries.
pub fn start(fetch: Function, request: Request, signal: Option<AbortSignal>) -> Result<Object> {
    let state = Rc::new(State {
        abort: AbortController::new().map_err(|_| invalid())?,
        reader: RefCell::new(None),
        pending: Cell::new(false),
        timer: RefCell::new(None),
        signal: RefCell::new(None),
    });
    let init = RequestInit::new();
    init.set_method(&request.method);
    init.set_credentials(RequestCredentials::Omit);
    init.set_redirect(RequestRedirect::Error);
    init.set_signal(Some(&state.abort.signal()));
    if !request.body.is_empty() {
        init.set_body(Uint8Array::from(request.body.as_ref()).as_ref());
    }
    let headers = Headers::new().map_err(|_| invalid())?;
    for (name, value) in request.headers {
        headers.set(&name, &value).map_err(|_| invalid())?;
    }
    init.set_headers(headers.as_ref());
    if let Some(milliseconds) = request.timeout_ms {
        state.arm(milliseconds)?;
    }
    if let Some(signal) = signal {
        let weak = Rc::downgrade(&state);
        let callback = Closure::new(move || {
            if let Some(state) = weak.upgrade() {
                state.cancel();
            }
        });
        signal
            .add_event_listener_with_callback("abort", callback.as_ref().unchecked_ref())
            .map_err(|_| invalid())?;
        let aborted = signal.aborted();
        *state.signal.borrow_mut() = Some((signal, callback));
        if aborted {
            state.cancel();
        }
    }
    let head_state = state.clone();
    let response = future_to_promise(async move {
        if head_state.abort.signal().aborted() {
            return Err(failure());
        }
        let result = async {
            let value = fetch
                .call2(
                    &js_sys::global(),
                    &JsValue::from_str(&request.url),
                    init.as_ref(),
                )
                .map_err(|_| failure())?;
            let response = JsFuture::from(Promise::resolve(&value))
                .await
                .map_err(|_| failure())?
                .dyn_into::<Response>()
                .map_err(|_| failure())?;
            if head_state.abort.signal().aborted() {
                return Err(failure());
            }
            if let Some(body) = response.body() {
                let reader = body
                    .get_reader()
                    .dyn_into::<ReadableStreamDefaultReader>()
                    .map_err(|_| failure())?;
                *head_state.reader.borrow_mut() = Some(reader);
            }
            let headers = Object::new();
            let entries = js_sys::try_iter(response.headers().as_ref())
                .map_err(|_| failure())?
                .ok_or_else(failure)?;
            for entry in entries {
                let entry = js_sys::Array::from(&entry.map_err(|_| failure())?);
                Reflect::set(&headers, &entry.get(0), &entry.get(1)).map_err(|_| failure())?;
            }
            let head = Object::new();
            Reflect::set(
                &head,
                &JsValue::from_str("status"),
                &JsValue::from_f64(response.status().into()),
            )
            .map_err(|_| failure())?;
            Reflect::set(&head, &JsValue::from_str("headers"), &headers).map_err(|_| failure())?;
            Ok(head.into())
        }
        .await;
        if result.is_err() {
            head_state.cancel();
        }
        result
    });
    let read_state = state.clone();
    let head = response.clone();
    let read: Closure<dyn FnMut() -> Promise> = Closure::new(move || -> Promise {
        let state = read_state.clone();
        let head = head.clone();
        future_to_promise(async move {
            JsFuture::from(head).await?;
            if state.abort.signal().aborted() {
                return Err(failure());
            }
            if state.pending.replace(true) {
                return Err(js_sys::Error::new("concurrent browser body reads").into());
            }
            let reader = state.reader.borrow().clone();
            let result = async {
                let Some(reader) = reader else {
                    return Ok(JsValue::NULL);
                };
                let chunk = JsFuture::from(reader.read()).await.map_err(|_| failure())?;
                if state.abort.signal().aborted() {
                    return Err(failure());
                }
                let done =
                    Reflect::get(&chunk, &JsValue::from_str("done")).map_err(|_| failure())?;
                if done.as_bool() == Some(true) {
                    return Ok(JsValue::NULL);
                }
                Reflect::get(&chunk, &JsValue::from_str("value"))
                    .map_err(|_| failure())?
                    .dyn_into::<Uint8Array>()
                    .map(JsValue::from)
                    .map_err(|_| failure())
            }
            .await;
            state.pending.set(false);
            if result.as_ref().is_ok_and(JsValue::is_null) {
                state.finish();
            } else if result.is_err() {
                state.cancel();
            }
            result
        })
    });
    let cancel: Closure<dyn FnMut()> = Closure::new(move || state.cancel());
    let exchange = Object::new();
    set(&exchange, "response", response.as_ref())?;
    set(&exchange, "read", &read.into_js_value())?;
    set(&exchange, "cancel", &cancel.into_js_value())?;
    Ok(exchange)
}

//! Browser timer ownership shared by HTTP deadlines and model backoff.
use js_sys::{Function, Promise, Reflect};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;

/// Own the original handle: browsers use numbers, Node uses objects. Cancellation must clear
/// the timer before releasing its future, rather than retain a resolver for up to an hour.
/// Owned host timer; dropping it synchronously clears the original handle.
pub struct Timeout {
    global: JsValue,
    clear: Function,
    handle: JsValue,
}

impl Drop for Timeout {
    fn drop(&mut self) {
        let _ = self.clear.call1(&self.global, &self.handle);
    }
}

/// Waits with cancellation-owned host timers and platform-supported intervals.
pub async fn delay(duration: std::time::Duration) {
    // setTimeout takes signed 32-bit milliseconds. Split longer waits, and round fractional
    // milliseconds up so Retry-After is never shortened by the conversion.
    let mut remaining = duration.as_millis() + u128::from(duration.subsec_nanos() % 1_000_000 != 0);
    while remaining != 0 {
        let interval = remaining.min(i32::MAX as u128) as u32;
        let mut timeout = None;
        let promise = Promise::new(&mut |resolve, reject| {
            timeout = schedule(resolve.as_ref(), interval);
            if timeout.is_none() {
                let _ = reject.call0(&JsValue::UNDEFINED);
            }
        });
        let elapsed = JsFuture::from(promise).await;
        drop(timeout);
        if elapsed.is_err() {
            return;
        }
        remaining -= u128::from(interval);
    }
}

/// The platform's monotonic clock, `performance.now()`, in the realm's own origin. A realm
/// without it falls back to the wall clock, which still only grows in practice.
pub fn now() -> std::time::Duration {
    let global = js_sys::global();
    let millis = Reflect::get(&global, &JsValue::from_str("performance"))
        .ok()
        .and_then(|performance| {
            let now = Reflect::get(&performance, &JsValue::from_str("now"))
                .ok()?
                .dyn_into::<Function>()
                .ok()?;
            now.call0(&performance).ok()?.as_f64()
        })
        .unwrap_or_else(js_sys::Date::now);
    // `max` maps NaN to zero and `min` keeps the conversion finite.
    std::time::Duration::from_secs_f64(millis.max(0.0).min(1e15) / 1000.0)
}

/// Schedules one platform-supported interval; callers split longer deadlines.
#[must_use]
pub fn schedule(callback: &JsValue, milliseconds: u32) -> Option<Timeout> {
    let global = js_sys::global();
    let function = |name| {
        Reflect::get(&global, &JsValue::from_str(name))
            .ok()?
            .dyn_into::<Function>()
            .ok()
    };
    let set = function("setTimeout")?;
    let clear = function("clearTimeout")?;
    let handle = set
        .call2(&global, callback, &JsValue::from_f64(milliseconds.into()))
        .ok()?;
    Some(Timeout {
        global: global.into(),
        clear,
        handle,
    })
}

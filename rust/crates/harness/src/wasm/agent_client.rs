//! Thin browser ownership of the same AgentClient as the native agent adapter.

use super::json::to_js;
use crate::{
    agent::AgentPath,
    client::{AgentClient, AgentClientError},
};
use acyclic_stream::WasmStream;
use js_sys::JsString;
use std::{
    cell::{Cell, RefCell},
    task::{Poll, Waker},
};
use wasm_bindgen::prelude::*;

fn error(code: &str, message: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&message.to_string());
    error.set_name(code);
    error.into()
}

fn client_error(cause: AgentClientError) -> JsValue {
    let code = match &cause {
        AgentClientError::Session(cause) => cause.code(),
        AgentClientError::Record(_) => "invalid_agent_record",
        AgentClientError::Closed(_) => "agent_stream_closed",
    };
    error(code, cause)
}

struct Reading<'a> {
    busy: &'a Cell<bool>,
    waker: &'a RefCell<Option<Waker>>,
}

impl Drop for Reading<'_> {
    fn drop(&mut self) {
        self.waker.borrow_mut().take();
        self.busy.set(false);
    }
}

/// The focused agent's bounded conversation/status fold. The platform supplies
/// its existing authenticated pool; this object creates no transport or queue.
#[wasm_bindgen]
pub struct WasmAgentClient {
    client: RefCell<Option<AgentClient>>,
    reading: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

#[wasm_bindgen]
impl WasmAgentClient {
    /// Explicit cursors select a bounded page or retained live edge. The slot
    /// must already be authenticated on the supplied stream's session pool.
    #[wasm_bindgen(constructor)]
    pub fn new(
        stream: &WasmStream,
        agent: JsString,
        conversation_from: u64,
        status_from: u64,
        credential: u32,
    ) -> Result<Self, JsValue> {
        // Admit UTF-16 length before copying an untrusted JavaScript string.
        if agent.length() as usize > acyclic_stream::MAX_PATH_BYTES {
            return Err(error("invalid_agent_path", "agent path exceeds byte bound"));
        }
        let agent = AgentPath::try_from(agent.as_string()
            .ok_or_else(|| error("invalid_agent_path", "agent path is not a string"))?)
            .map_err(|cause| error("invalid_agent_path", cause))?;
        let pool = stream.session_client()
            .ok_or_else(|| error("client_no_session", "provider has no authenticated session"))?;
        let client = AgentClient::new(pool, agent, conversation_from, status_from, credential)
            .map_err(client_error)?;
        Ok(Self {
            client: RefCell::new(Some(client)),
            reading: Cell::new(false),
            waker: RefCell::new(None),
        })
    }

    /// One transition decoded and folded by the exact native Rust core. There
    /// can be one pending read; a slow browser never creates a second queue.
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        if self.reading.replace(true) {
            return Err(error("client_read_busy", "an agent read is already pending"));
        }
        let _reading = Reading { busy: &self.reading, waker: &self.waker };
        let event = futures::future::poll_fn(|context| {
            let mut client = self.client.borrow_mut();
            let Some(client) = client.as_mut() else {
                return Poll::Ready(Err(error("client_closed", "agent client is closed")));
            };
            *self.waker.borrow_mut() = Some(context.waker().clone());
            client.poll_next(context).map(|result| result.map_err(client_error))
        }).await?;
        // Exact 64-bit sequences cross as BigInt through the shared codec.
        to_js(&event)
    }

    /// Release follows and their retained views. Wake a pending read after all
    /// interior borrows are released; it reports closure, never a fake status.
    pub fn close(&self) {
        let client = self.client.borrow_mut().take();
        let waker = self.waker.borrow_mut().take();
        drop(client);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Read the shared fold's revision without copying conversation history.
    #[wasm_bindgen(js_name = viewRevision)]
    pub fn view_revision(&self) -> Result<u64, JsValue> {
        self.with_client(|client| client.conversation().revision())
    }

    /// The live-tail eviction edge, distinct from its recovery cursor.
    #[wasm_bindgen(js_name = firstSequence)]
    pub fn first_sequence(&self) -> Result<u64, JsValue> {
        self.with_client(|client| client.conversation().first_sequence())
    }

    /// Retained displayed turns, capped by the shared Rust StreamView policy.
    #[wasm_bindgen(js_name = recordCount)]
    pub fn record_count(&self) -> Result<usize, JsValue> {
        self.with_client(|client| client.conversation().records().len())
    }

    /// Decode and copy one visible turn through the same native reader.
    /// An index beyond the retained window returns null.
    pub fn record(&self, index: usize) -> Result<JsValue, JsValue> {
        let entry = self.with_client(|client| client.conversation_entry(index))?
            .map_err(client_error)?;
        to_js(&entry)
    }
}

impl WasmAgentClient {
    fn with_client<T>(&self, read: impl FnOnce(&AgentClient) -> T) -> Result<T, JsValue> {
        let client = self.client.borrow();
        let client = client.as_ref()
            .ok_or_else(|| error("client_closed", "agent client is closed"))?;
        Ok(read(client))
    }
}

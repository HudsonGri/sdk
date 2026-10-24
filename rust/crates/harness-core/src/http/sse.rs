//! Bounded SSE framing shared by model, MCP and host adapters.
use crate::{Error, Result};

fn invalid(reason: impl Into<String>) -> Error {
    Error::Invalid(reason.into())
}
use acyclic_stream::BoxProviderStream;
use bytes::Bytes;
use futures::{Stream, StreamExt as _};

/// One fully terminated SSE event, retaining its native buffer custody.
pub struct Frame {
    /// Optional event metadata, retained only when requested by a subscription.
    pub metadata: Option<Metadata>,
    /// Joined data fields without the final framing newline.
    pub data: String,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Clone, Default, serde::Serialize)]
/// Subscription cursor and event name; ids never pass through a numeric representation.
pub struct Metadata {
    /// Last event id, including exact u64 spelling.
    pub id: String,
    /// SSE event type, defaulting to message.
    pub event: String,
}

/// Incremental per-event decoder; input is bounded before retaining a byte.
#[derive(Default)]
pub struct Decoder {
    line: Vec<u8>,
    data: String,
    bytes: usize,
    cr: bool,
    metadata: Option<Metadata>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

impl Decoder {
    /// Portable subscription decoder, retaining event names and exact cursors.
    #[must_use]
    pub fn subscription() -> Self {
        Self {
            metadata: Some(Metadata::default()),
            ..Self::default()
        }
    }

    fn failure(&self, reason: &'static str) -> crate::Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.custody.is_some() {
            return crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason,
            };
        }
        invalid(reason)
    }
    /// The body ended inside an event: the connection was cut off, not the stream malformed.
    fn truncated(&self) -> crate::Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = self.custody.is_some();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native = false;
        Error::ModelUnavailable {
            failure: crate::ModelTransportFailure::Interrupted,
            excerpt: if native {
                String::new()
            } else {
                "truncated SSE event".into()
            },
        }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Creates a decoder funded by the caller's original allocation ledger.
    pub fn native(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        Ok(Self {
            custody: Some(work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?),
            ..Self::default()
        })
    }

    fn line_byte(&mut self, byte: u8) -> Result<()> {
        let target = self.line.len() + 1;
        let old = self.line.capacity();
        if target > old {
            let capacity = target.max(old.saturating_mul(2)).max(8);
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let mut allocation = self
                .custody
                .as_ref()
                .map(|owner| {
                    owner.work().claim(
                        capacity as u64,
                        1,
                        acyclic_fs::WorkCounters {
                            bytes_copied: self.line.len() as u64,
                            ..Default::default()
                        },
                    )
                })
                .transpose()?;
            self.line
                .try_reserve_exact(capacity - self.line.len())
                .map_err(|_| self.failure("SSE buffer allocation failed"))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let (Some(owner), Some(allocation)) = (&mut self.custody, &mut allocation) {
                allocation.reconcile_capacity(self.line.capacity() as u64)?;
                owner.release(old as u64)?;
                owner.absorb(allocation)?;
            }
        }
        self.line.push(byte);
        Ok(())
    }

    fn finish_line(&mut self) -> Result<()> {
        let text = std::str::from_utf8(&self.line).map_err(|_| self.failure("SSE is not UTF-8"))?;
        let (field, value) = text.split_once(':').unwrap_or((text, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        if field == "data" {
            let target = self
                .data
                .len()
                .checked_add(value.len())
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| self.failure("SSE size overflow"))?;
            let old = self.data.capacity();
            if target > old {
                let capacity = target.max(old.saturating_mul(2)).max(8);
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let mut allocation = self
                    .custody
                    .as_ref()
                    .map(|owner| {
                        owner.work().claim(
                            capacity as u64,
                            1,
                            acyclic_fs::WorkCounters {
                                bytes_copied: self.data.len() as u64,
                                ..Default::default()
                            },
                        )
                    })
                    .transpose()?;
                self.data
                    .try_reserve_exact(capacity - self.data.len())
                    .map_err(|_| self.failure("SSE buffer allocation failed"))?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let (Some(owner), Some(allocation)) = (&mut self.custody, &mut allocation) {
                    allocation.reconcile_capacity(self.data.capacity() as u64)?;
                    owner.release(old as u64)?;
                    owner.absorb(allocation)?;
                }
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(owner) = &self.custody {
                owner.work().charge(acyclic_fs::WorkCounters {
                    bytes_copied: value.len() as u64,
                    ..Default::default()
                })?;
            }
            self.data.push_str(value);
            self.data.push('\n');
        }
        if let Some(metadata) = &mut self.metadata {
            match field {
                "id" if !value.contains('\0') => {
                    metadata.id.clear();
                    metadata.id.push_str(value);
                }
                "event" => {
                    metadata.event.clear();
                    metadata.event.push_str(value);
                }
                _ => {}
            }
        }
        self.line.clear();
        Ok(())
    }
    fn count_byte(&mut self, maximum: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(1)
            .ok_or_else(|| self.failure("SSE size overflow"))?;
        if self.bytes > maximum {
            return Err(self.failure("SSE event exceeds configured byte limit"));
        }
        Ok(())
    }

    fn end_line(&mut self) -> Result<Option<Frame>> {
        if !self.line.is_empty() {
            self.finish_line()?;
            return Ok(None);
        }
        self.bytes = 0;
        let metadata = self.metadata.as_mut().map(|metadata| Metadata {
            id: if self.data.is_empty() {
                String::new()
            } else {
                metadata.id.clone()
            },
            event: {
                let event = std::mem::take(&mut metadata.event);
                if event.is_empty() {
                    "message".into()
                } else {
                    event
                }
            },
        });
        if self.data.is_empty() {
            return Ok(None);
        }
        self.data.pop();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = self
            .custody
            .as_mut()
            .map(|owner| owner.split_off(self.data.capacity() as u64))
            .transpose()?;
        Ok(Some(Frame {
            data: std::mem::take(&mut self.data),
            metadata,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _custody: custody,
        }))
    }

    /// Consumes a physical wire byte. CR is held until CRLF can be counted atomically.
    pub fn push(&mut self, byte: u8, maximum: usize) -> Result<Option<Frame>> {
        let mut frame = None;
        if self.cr {
            self.cr = false;
            if byte == b'\n' {
                self.count_byte(maximum)?;
                return self.end_line();
            }
            frame = self.end_line()?;
        }
        self.count_byte(maximum)?;
        match byte {
            b'\r' => self.cr = true,
            b'\n' => return self.end_line(),
            _ => self.line_byte(byte)?,
        }
        Ok(frame)
    }

    /// Completes a bare CR delimiter at EOF, otherwise rejects unfinished events.
    pub fn finish(&mut self) -> Result<Option<Frame>> {
        let frame = if self.cr {
            self.cr = false;
            self.end_line()?
        } else {
            None
        };
        if self.bytes != 0 || !self.line.is_empty() || !self.data.is_empty() {
            return Err(self.truncated());
        }
        Ok(frame)
    }
}

/// Decodes complete events and rejects a disconnect inside an event.
pub fn frames<'a>(
    mut body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
    mut decoder: Decoder,
) -> impl Stream<Item = Result<Frame>> + acyclic_stream::ProviderTask + 'a {
    async_stream::try_stream! {
        while let Some(chunk) = body.next().await {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(owner) = &decoder.custody { owner.work().remaining()?; }
            for byte in chunk? {
                if let Some(frame) = decoder.push(byte, maximum)? { yield frame; }
            }
        }
        if let Some(frame) = decoder.finish()? { yield frame; }
    }
}

/// Portable data-only stream over the same bounded decoder.
pub fn decode<'a>(
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
) -> BoxProviderStream<'a, Result<String>> {
    Box::pin(frames(body, maximum, Decoder::default()).map(|frame| frame.map(|frame| frame.data)))
}

#[cfg(test)]
mod tests;

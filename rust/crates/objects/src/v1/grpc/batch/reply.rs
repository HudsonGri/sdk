use super::super::super::NativeObjectsError;
use super::*;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use bytes::Buf;
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use std::io;

#[cfg(all(test, not(objects_loom)))]
mod tests;

pub(super) struct Exchange {
    pub(super) reply: std::sync::Mutex<Option<Reply>>,
    pub(super) sizes: Guarded<Vec<u64>>,
    completed: Arc<Completion>,
    metadata: std::sync::Mutex<Guarded<Vec<Option<wire::ObjectMetadata>>>>,
}

impl Exchange {
    pub(super) fn keep_metadata(
        &self,
        header: &mut wire::PutObjectsHeader,
        meter: &std::sync::Arc<dyn NativeReadMeter>,
    ) -> Result<(), NativeObjectsError> {
        let mut metadata = self
            .metadata
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !metadata.value.is_empty() || header.objects.len() != self.sizes.value.len() {
            return Err(invalid());
        }
        for object in &mut header.objects {
            let info = wire::ObjectInfo {
                metadata: object.metadata.take(),
                ..Default::default()
            };
            let mut credit = ReadAllocation::adopt(meter, info_capacity(&info)?)?;
            metadata
                ._allocation
                .as_mut()
                .ok_or_else(invalid)?
                .absorb(&mut credit)?;
            metadata.value.push(info.metadata);
        }
        Ok(())
    }
}

pub(super) type Received = std::sync::Arc<Guarded<Exchange>>;

pub(super) fn received(
    meter: Option<&std::sync::Arc<dyn NativeReadMeter>>,
    sizes: Guarded<Vec<u64>>,
    completed: Arc<Completion>,
) -> Result<Received, Error> {
    let metadata = batch_vector(sizes.value.len(), meter)?;
    let allocation = meter
        .map(|meter| ReadAllocation::new(meter, std::mem::size_of::<Guarded<Exchange>>()))
        .transpose()
        .map_err(|_| protocol())?;
    Ok(std::sync::Arc::new(Guarded {
        value: Exchange {
            reply: std::sync::Mutex::new(None),
            sizes,
            completed,
            metadata: std::sync::Mutex::new(metadata),
        },
        _allocation: allocation,
    }))
}

pub(super) struct Reply {
    pub(super) response: Guarded<wire::PutObjectsResponse>,
    pub(super) failure: Option<NativeObjectsError>,
}

impl Reply {
    pub(super) fn record_write(&mut self, meter: &dyn NativeReadMeter) {
        let completed = self
            .response
            .value
            .results
            .iter()
            .filter_map(|result| match &result.result {
                Some(wire::put_object_result::Result::Object(object)) => Some(object.size as usize),
                _ => None,
            })
            .sum();
        let recorded = meter.record_write(completed);
        if self.failure.is_none() {
            self.failure = recorded.err().map(Into::into);
        }
    }

    pub(super) fn validate(&mut self, sizes: &[u64], completion: Result<(), Error>) {
        let mut failure = completion.err();
        if self.response.value.results.len() != sizes.len() {
            failure.get_or_insert_with(protocol);
        }
        for (result, size) in self.response.value.results.iter_mut().zip(sizes) {
            let valid = match &result.result {
                Some(wire::put_object_result::Result::Object(object)) => {
                    info(object).is_ok() && object.size == *size
                }
                Some(wire::put_object_result::Result::Error(error)) => item_error(error).is_ok(),
                None => false,
            };
            if !valid {
                result.result = None;
                failure.get_or_insert_with(protocol);
            }
        }
        if self.failure.is_none() {
            self.failure = failure.map(Into::into);
        }
    }
}

pub(super) struct Decoder {
    pub(super) prepared: Option<Reply>,
    pub(super) expected: usize,
    pub(super) meter: Option<std::sync::Arc<dyn NativeReadMeter>>,
    pub(super) received: Received,
}

fn invalid() -> NativeObjectsError {
    io::Error::from(io::ErrorKind::InvalidData).into()
}

fn message<'a>(kind: WireType, input: &mut &'a [u8]) -> Result<&'a [u8], NativeObjectsError> {
    if kind != WireType::LengthDelimited {
        return Err(invalid());
    }
    let length =
        usize::try_from(decode_varint(input).map_err(|_| invalid())?).map_err(|_| invalid())?;
    if length > input.len() {
        return Err(invalid());
    }
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}

fn fields<'a>(
    mut input: &'a [u8],
    mut visit: impl FnMut(u32, WireType, &mut &'a [u8]) -> Result<(), NativeObjectsError>,
) -> Result<(), NativeObjectsError> {
    while !input.is_empty() {
        let (tag, kind) = decode_key(&mut input).map_err(|_| invalid())?;
        visit(tag, kind, &mut input)?;
    }
    Ok(())
}

fn unknown(tag: u32, kind: WireType, input: &mut &[u8]) -> Result<(), NativeObjectsError> {
    skip_field(kind, tag, input, DecodeContext::default()).map_err(|_| invalid())
}

fn text<'a>(kind: WireType, input: &mut &'a [u8]) -> Result<&'a str, NativeObjectsError> {
    std::str::from_utf8(message(kind, input)?).map_err(|_| invalid())
}

fn verify_user_map(
    expected: Option<&wire::ObjectMetadata>,
    bytes: &[u8],
) -> Result<bool, NativeObjectsError> {
    const MAX: usize = wire::ObjectsLimit::MaxUserMetadataBytes as usize;
    let mut keys = [""; MAX];
    let count = expected.map_or(0, |metadata| metadata.user.len());
    if count > MAX {
        return Err(invalid());
    }
    if let Some(metadata) = expected {
        for (slot, key) in keys.iter_mut().zip(metadata.user.keys()) {
            *slot = key;
        }
    }
    let mut matched = [false; MAX];
    let mut present = false;
    fields(bytes, |tag, kind, input| {
        if tag != 3 {
            return unknown(tag, kind, input);
        }
        present = true;
        fields(message(kind, input)?, |tag, kind, input| {
            if tag != 2 {
                return unknown(tag, kind, input);
            }
            let (mut key, mut value) = ("", "");
            fields(message(kind, input)?, |tag, kind, input| match tag {
                1 => {
                    key = text(kind, input)?;
                    Ok(())
                }
                2 => {
                    value = text(kind, input)?;
                    Ok(())
                }
                _ => unknown(tag, kind, input),
            })?;
            let index = keys[..count].binary_search(&key).map_err(|_| invalid())?;
            matched[index] = expected
                .and_then(|metadata| metadata.user.get(key))
                .is_some_and(|expected| expected == value);
            Ok(())
        })
    })?;
    if present && matched[..count].iter().any(|matched| !matched) {
        return Err(invalid());
    }
    Ok(present)
}

struct Decode<'a> {
    meter: Option<&'a std::sync::Arc<dyn NativeReadMeter>>,
    allocation: Option<&'a mut ReadAllocation>,
    copied: u64,
}

impl Decode<'_> {
    fn text(
        &mut self,
        target: &mut String,
        kind: WireType,
        input: &mut &[u8],
    ) -> Result<(), NativeObjectsError> {
        let bytes = message(kind, input)?;
        let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
        if let Some(meter) = self.meter {
            meter.admit_codec(0, text.len() as u64, 0)?;
        }
        if target.capacity() < text.len() {
            let previous = target.capacity();
            let mut credit = self
                .meter
                .map(|meter| ReadAllocation::new(meter, text.len() - previous))
                .transpose()?;
            target
                .try_reserve_exact(text.len().saturating_sub(target.len()))
                .map_err(|error| {
                    self.meter.map_or_else(
                        || io::Error::from(io::ErrorKind::OutOfMemory),
                        |meter| meter.allocation_failed(error),
                    )
                })?;
            if let Some(credit) = credit.as_mut() {
                credit.reconcile_capacity(target.capacity() - previous)?;
            }
            if let (Some(owner), Some(credit)) = (self.allocation.as_mut(), credit.as_mut()) {
                owner.absorb(credit)?;
            }
        }
        target.clear();
        target.push_str(text);
        self.copied += text.len() as u64;
        if let Some(meter) = self.meter {
            meter.record_codec(0, text.len() as u64, 0)?;
        }
        Ok(())
    }

    fn metadata(
        &mut self,
        target: &mut wire::ObjectMetadata,
        bytes: &[u8],
    ) -> Result<(), NativeObjectsError> {
        fields(bytes, |tag, kind, input| {
            let text = match tag {
                1 => Some(&mut target.content_type),
                3 => Some(&mut target.content_encoding),
                4 => Some(&mut target.cache_control),
                5 => Some(&mut target.content_disposition),
                6 => Some(&mut target.content_language),
                _ => None,
            };
            if let Some(text) = text {
                return self.text(text, kind, input);
            }
            if tag == 2 {
                if self.meter.is_some() {
                    // The borrowed pass verified these entries against transferred storage.
                    message(kind, input)?;
                    return Ok(());
                }
                let mut key = String::new();
                let mut value = String::new();
                fields(message(kind, input)?, |tag, kind, input| match tag {
                    1 => self.text(&mut key, kind, input),
                    2 => self.text(&mut value, kind, input),
                    _ => unknown(tag, kind, input),
                })?;
                target.user.insert(key, value);
                Ok(())
            } else {
                target
                    .merge_field(tag, kind, input, DecodeContext::default())
                    .map_err(|_| invalid())
            }
        })
    }

    fn object(
        &mut self,
        bytes: &[u8],
        mut metadata: Option<wire::ObjectMetadata>,
    ) -> Result<wire::ObjectInfo, NativeObjectsError> {
        if self.meter.is_some() {
            if !verify_user_map(metadata.as_ref(), bytes)? {
                metadata = None;
            }
            if let Some(metadata) = metadata.as_mut() {
                metadata.content_type.clear();
                metadata.content_encoding.clear();
                metadata.cache_control.clear();
                metadata.content_disposition.clear();
                metadata.content_language.clear();
                metadata.expires_unix_seconds = None;
            }
        }
        let mut object = wire::ObjectInfo {
            metadata,
            ..Default::default()
        };
        fields(bytes, |tag, kind, input| match tag {
            1 => self.text(&mut object.etag, kind, input),
            3 => self.metadata(
                object.metadata.get_or_insert_default(),
                message(kind, input)?,
            ),
            _ => object
                .merge_field(tag, kind, input, DecodeContext::default())
                .map_err(|_| invalid()),
        })?;
        Ok(object)
    }

    fn result(
        &mut self,
        bytes: &[u8],
        mut metadata: Option<wire::ObjectMetadata>,
    ) -> Result<wire::PutObjectResult, NativeObjectsError> {
        let mut result = wire::PutObjectResult::default();
        fields(bytes, |tag, kind, input| {
            if matches!(tag, 1 | 2) && result.result.is_some() {
                return Err(invalid());
            }
            result.result = Some(match tag {
                1 => wire::put_object_result::Result::Object(
                    self.object(message(kind, input)?, metadata.take())?,
                ),
                2 => {
                    let mut error = wire::ErrorDetail::default();
                    fields(message(kind, input)?, |tag, kind, input| match tag {
                        2 => self.text(&mut error.request_id, kind, input),
                        _ => error
                            .merge_field(tag, kind, input, DecodeContext::default())
                            .map_err(|_| invalid()),
                    })?;
                    wire::put_object_result::Result::Error(error)
                }
                _ => return unknown(tag, kind, input),
            });
            Ok(())
        })?;
        Ok(result)
    }
}

impl Decoder {
    fn accept(&self, mut reply: Reply) {
        reply.validate(
            &self.received.value.sizes.value,
            self.received.value.completed.validate(),
        );
        // Account before Tonic can wait for trailers or the caller can drop its future.
        if let Some(meter) = self.meter.as_ref() {
            reply.record_write(meter.as_ref());
        }
        *self
            .received
            .value
            .reply
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(reply);
    }

    pub(super) fn bytes(&mut self, bytes: &[u8]) -> Result<Reply, tonic::Status> {
        let mut reply = self
            .prepared
            .take()
            .ok_or_else(|| tonic::Status::internal("duplicate batch reply"))?;
        let mut decode = Decode {
            meter: self.meter.as_ref(),
            allocation: reply.response._allocation.as_mut(),
            copied: 0,
        };
        let mut metadata = self
            .received
            .value
            .metadata
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let decoded = (|| {
            if let Some(meter) = decode.meter {
                meter.admit_codec(bytes.len() as u64, 0, 0)?;
            }
            fields(bytes, |tag, kind, input| {
                if tag == 1 {
                    if reply.response.value.results.len() == self.expected {
                        return Err(invalid());
                    }
                    let metadata = if self.meter.is_some() {
                        let index = reply.response.value.results.len();
                        let value = metadata.value.get_mut(index).ok_or_else(invalid)?.take();
                        let info = wire::ObjectInfo {
                            metadata: value,
                            ..Default::default()
                        };
                        let mut credit = metadata
                            ._allocation
                            .as_mut()
                            .ok_or_else(invalid)?
                            .split(info_capacity(&info)?)?;
                        decode
                            .allocation
                            .as_mut()
                            .ok_or_else(invalid)?
                            .absorb(&mut credit)?;
                        info.metadata
                    } else {
                        None
                    };
                    let item = message(kind, input)?;
                    match decode.result(item, metadata) {
                        Ok(result) => reply.response.value.results.push(result),
                        Err(error) => {
                            reply.failure.get_or_insert(error);
                            reply
                                .response
                                .value
                                .results
                                .push(wire::PutObjectResult::default());
                        }
                    }
                    Ok(())
                } else {
                    unknown(tag, kind, input)
                }
            })
        })();
        reply.failure = reply
            .failure
            .take()
            .or_else(|| decoded.err())
            .or_else(|| (reply.response.value.results.len() != self.expected).then(invalid));
        if let Some(meter) = self.meter.as_ref() {
            let recorded = meter.record_codec(bytes.len() as u64, 0, 0);
            if reply.failure.is_none() {
                reply.failure = recorded.err().map(Into::into);
            }
        }
        Ok(reply)
    }
}

impl tonic::codec::Decoder for Decoder {
    type Item = ();
    type Error = tonic::Status;
    fn decode(
        &mut self,
        buffer: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<()>, tonic::Status> {
        let length = buffer.remaining();
        if buffer.chunk().len() != length {
            return Err(tonic::Status::internal("fragmented batch envelope"));
        }
        let reply = self.bytes(buffer.chunk())?;
        self.accept(reply);
        buffer.advance(length);
        Ok(Some(()))
    }
}

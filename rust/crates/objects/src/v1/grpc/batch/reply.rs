use super::*;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use bytes::Buf;
use super::super::super::{NativeObjectsError, ObjectsPutReceipt};
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use std::io;

pub(super) struct Reply {
    pub(super) response: Guarded<wire::PutObjectsResponse>,
    pub(super) failure: Option<NativeObjectsError>,
}

pub(super) struct Decoder {
    pub(super) prepared: Option<Reply>,
    pub(super) expected: usize,
    pub(super) meter: Option<std::sync::Arc<dyn NativeReadMeter>>,
}

fn invalid() -> NativeObjectsError { io::Error::from(io::ErrorKind::InvalidData).into() }

fn message<'a>(kind: WireType, input: &mut &'a [u8]) -> Result<&'a [u8], NativeObjectsError> {
    if kind != WireType::LengthDelimited { return Err(invalid()); }
    let length = usize::try_from(decode_varint(input).map_err(|_| invalid())?).map_err(|_| invalid())?;
    if length > input.len() { return Err(invalid()); }
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}

fn fields(mut input: &[u8], mut visit: impl FnMut(u32, WireType, &mut &[u8]) -> Result<(), NativeObjectsError>) -> Result<(), NativeObjectsError> {
    while !input.is_empty() {
        let (tag, kind) = decode_key(&mut input).map_err(|_| invalid())?;
        visit(tag, kind, &mut input)?;
    }
    Ok(())
}

fn unknown(tag: u32, kind: WireType, input: &mut &[u8]) -> Result<(), NativeObjectsError> {
    skip_field(kind, tag, input, DecodeContext::default()).map_err(|_| invalid())
}

fn count(input: &[u8], expected: usize) -> Result<(), NativeObjectsError> {
    let mut count = 0;
    fields(input, |tag, kind, input| {
        if tag == 1 {
            message(kind, input)?;
            count += 1;
            if count > expected { return Err(invalid()); }
            Ok(())
        } else { unknown(tag, kind, input) }
    })?;
    if count != expected { return Err(invalid()); }
    Ok(())
}

struct Decode<'a> {
    meter: Option<&'a std::sync::Arc<dyn NativeReadMeter>>,
    allocation: Option<&'a mut ReadAllocation>,
    copied: u64,
}

impl Decode<'_> {
    fn text(&mut self, target: &mut String, kind: WireType, input: &mut &[u8]) -> Result<(), NativeObjectsError> {
        let bytes = message(kind, input)?;
        let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
        if let Some(meter) = self.meter { meter.admit_codec(0, text.len() as u64, 0)?; }
        if target.capacity() < text.len() {
            let previous = target.capacity();
            let mut credit = self.meter.map(|meter| ReadAllocation::new(meter, text.len() - previous)).transpose()?;
            target.try_reserve_exact(text.len().saturating_sub(target.len())).map_err(|error| {
                self.meter.map_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory), |meter| meter.allocation_failed(error))
            })?;
            if let Some(credit) = credit.as_mut() { credit.reconcile_capacity(target.capacity() - previous)?; }
            if let (Some(owner), Some(credit)) = (self.allocation.as_mut(), credit.as_mut()) { owner.absorb(credit)?; }
        }
        target.clear();
        target.push_str(text);
        self.copied += text.len() as u64;
        if let Some(meter) = self.meter { meter.record_codec(0, text.len() as u64, 0)?; }
        Ok(())
    }

    fn metadata(&mut self, target: &mut wire::ObjectMetadata, bytes: &[u8]) -> Result<(), NativeObjectsError> {
        fields(bytes, |tag, kind, input| {
            let text = match tag {
                1 => Some(&mut target.content_type),
                3 => Some(&mut target.content_encoding),
                4 => Some(&mut target.cache_control),
                5 => Some(&mut target.content_disposition),
                6 => Some(&mut target.content_language),
                _ => None,
            };
            if let Some(text) = text { return self.text(text, kind, input); }
            if tag == 2 {
                let mut key = String::new();
                let mut value = String::new();
                fields(message(kind, input)?, |tag, kind, input| match tag {
                    1 => self.text(&mut key, kind, input),
                    2 => self.text(&mut value, kind, input),
                    _ => unknown(tag, kind, input),
                })?;
                if !target.user.contains_key(&key) {
                    let mut entry = self.meter.map(|meter| ReadAllocation::adopt(meter, std::mem::size_of::<(String, String)>())).transpose()?;
                    if let (Some(owner), Some(entry)) = (self.allocation.as_mut(), entry.as_mut()) { owner.absorb(entry)?; }
                }
                target.user.insert(key, value);
                Ok(())
            } else {
                target.merge_field(tag, kind, input, DecodeContext::default()).map_err(|_| invalid())
            }
        })
    }

    fn object(&mut self, bytes: &[u8]) -> Result<wire::ObjectInfo, NativeObjectsError> {
        let mut object = wire::ObjectInfo::default();
        fields(bytes, |tag, kind, input| match tag {
            1 => self.text(&mut object.etag, kind, input),
            3 => self.metadata(object.metadata.get_or_insert_default(), message(kind, input)?),
            _ => object.merge_field(tag, kind, input, DecodeContext::default()).map_err(|_| invalid()),
        })?;
        Ok(object)
    }

    fn result(&mut self, bytes: &[u8]) -> Result<wire::PutObjectResult, NativeObjectsError> {
        let mut result = wire::PutObjectResult::default();
        fields(bytes, |tag, kind, input| {
            result.result = Some(match tag {
                1 => wire::put_object_result::Result::Object(self.object(message(kind, input)?)?),
                2 => {
                    let mut error = wire::ErrorDetail::default();
                    fields(message(kind, input)?, |tag, kind, input| match tag {
                        2 => self.text(&mut error.request_id, kind, input),
                        _ => error.merge_field(tag, kind, input, DecodeContext::default()).map_err(|_| invalid()),
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
    pub(super) fn bytes(&mut self, bytes: &[u8]) -> Result<Reply, tonic::Status> {
        // Count borrowed envelopes before allocating or invoking any generated decoder.
        count(bytes, self.expected).map_err(|_| tonic::Status::resource_exhausted("batch result count"))?;
        let mut reply = self.prepared.take().ok_or_else(|| tonic::Status::internal("duplicate batch reply"))?;
        let mut decode = Decode { meter: self.meter.as_ref(), allocation: reply.response._allocation.as_mut(), copied: 0 };
        let decoded = (|| {
            if let Some(meter) = decode.meter { meter.admit_codec(bytes.len() as u64, 0, 0)?; }
            fields(bytes, |tag, kind, input| {
                if tag == 1 {
                    reply.response.value.results.push(decode.result(message(kind, input)?)?);
                    Ok(())
                } else { unknown(tag, kind, input) }
            })
        })();
        reply.failure = decoded.err();
        if let Some(meter) = self.meter.as_ref() {
            let recorded = meter.record_codec(bytes.len() as u64, 0, 0);
            if reply.failure.is_none() { reply.failure = recorded.err().map(Into::into); }
        }
        Ok(reply)
    }
}

impl tonic::codec::Decoder for Decoder {
    type Item = Reply;
    type Error = tonic::Status;
    fn decode(&mut self, buffer: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<Reply>, tonic::Status> {
        let length = buffer.remaining();
        if buffer.chunk().len() != length { return Err(tonic::Status::internal("fragmented batch envelope")); }
        let reply = self.bytes(buffer.chunk())?;
        buffer.advance(length);
        Ok(Some(reply))
    }
}

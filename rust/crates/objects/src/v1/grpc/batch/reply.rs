use super::super::super::NativeObjectsError;
use super::*;
use crate::sync::Mutex;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation, ReadAllocationReservation};
use bytes::Buf;
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use std::io;

#[cfg(all(test, not(objects_loom)))]
mod tests;

pub(super) struct Exchange {
    pub(super) reply: Mutex<Option<Reply>>,
    pub(super) sizes: Guarded<Vec<u64>>,
    completed: Arc<Completion>,
    metadata: Mutex<Guarded<Vec<Option<wire::ObjectMetadata>>>>,
    resources: Mutex<Resources, 2>,
}

const REPLY_BYTES: u64 = 16 * 1024 * 1024;
const TEXT_BYTES: usize = 8192;
type TextCredits = [Option<ReadAllocationReservation>; 7];

struct CodecCredit {
    meter: std::sync::Arc<dyn NativeReadMeter>,
    encoded: u64,
    copied: u64,
    failure: Option<io::Error>,
}
impl CodecCredit {
    fn record(&mut self, encoded: u64, copied: u64) -> Result<(), NativeObjectsError> {
        if encoded > self.encoded || copied > self.copied {
            return Err(invalid());
        }
        self.encoded -= encoded;
        self.copied -= copied;
        if let Err(error) = self.meter.record_reserved_codec(encoded, copied) {
            self.failure.get_or_insert(error);
        }
        Ok(())
    }
}
impl Drop for CodecCredit {
    fn drop(&mut self) {
        self.meter.cancel_reserved_codec(self.encoded, self.copied);
    }
}
struct Resources {
    texts: Guarded<Vec<TextCredits>>,
    codec: Option<CodecCredit>,
}

impl Exchange {
    pub(super) fn keep_metadata(
        &self,
        header: &mut wire::PutObjectsHeader,
        meter: Option<&std::sync::Arc<dyn NativeReadMeter>>,
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
            if let Some(meter) = meter {
                let mut credit = ReadAllocation::adopt(meter, info_capacity(&info)?)?;
                metadata
                    ._allocation
                    .as_mut()
                    .ok_or_else(invalid)?
                    .absorb(&mut credit)?;
            }
            metadata.value.push(info.metadata);
        }
        Ok(())
    }
}

pub(super) type Received = Arc<Guarded<Exchange>>;

pub(super) fn received(
    meter: Option<&std::sync::Arc<dyn NativeReadMeter>>,
    sizes: Guarded<Vec<u64>>,
    completed: Arc<Completion>,
) -> Result<Received, Error> {
    let metadata = batch_vector(sizes.value.len(), meter)?;
    let mut texts = batch_vector(sizes.value.len(), meter)?;
    let codec = if let Some(meter) = meter {
        meter
            .reserve_codec(REPLY_BYTES, REPLY_BYTES)
            .map_err(|_| protocol())?;
        Some(CodecCredit {
            meter: meter.clone(),
            encoded: REPLY_BYTES,
            copied: REPLY_BYTES,
            failure: None,
        })
    } else {
        None
    };
    for _ in &sizes.value {
        let mut credits: TextCredits = std::array::from_fn(|_| None);
        if let Some(meter) = meter {
            for credit in &mut credits {
                *credit = Some(
                    ReadAllocationReservation::new(meter, TEXT_BYTES as u64)
                        .map_err(|_| protocol())?,
                );
            }
        }
        texts.value.push(credits);
    }
    let allocation = meter
        .map(|meter| ReadAllocation::new(meter, std::mem::size_of::<Guarded<Exchange>>()))
        .transpose()
        .map_err(|_| protocol())?;
    Ok(Arc::new(Guarded {
        value: Exchange {
            reply: Mutex::new(None),
            sizes,
            completed,
            metadata: Mutex::new(metadata),
            resources: Mutex::new(Resources { texts, codec }),
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
                let retained = match &result.result {
                    Some(wire::put_object_result::Result::Object(object)) => {
                        info_capacity(object).unwrap_or(0)
                    }
                    Some(wire::put_object_result::Result::Error(error)) => {
                        error.request_id.capacity()
                    }
                    None => 0,
                };
                result.result = None;
                if let Some(allocation) = self.response._allocation.as_mut() {
                    drop(allocation.split(retained));
                }
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
    credits: std::array::IntoIter<Option<ReadAllocationReservation>, 7>,
    codec: Option<&'a mut CodecCredit>,
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
        if let Some(codec) = self.codec.as_mut() {
            if text.len() > TEXT_BYTES || text.len() as u64 > codec.copied {
                return Err(invalid());
            }
        }
        if target.capacity() < text.len() {
            let previous = target.capacity();
            let capacity = if self.meter.is_some() {
                TEXT_BYTES
            } else {
                text.len()
            };
            let mut credit = if self.meter.is_some() {
                let mut credit = self
                    .credits
                    .next()
                    .flatten()
                    .ok_or_else(invalid)?
                    .allocate()?;
                credit.resize_retained(capacity - previous)?;
                Some(credit)
            } else {
                None
            };
            target
                .try_reserve_exact(capacity.saturating_sub(target.len()))
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
        if let Some(codec) = self.codec.as_mut() {
            codec.record(0, text.len() as u64)?;
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
        let mut metadata = self
            .received
            .value
            .metadata
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut resources = self
            .received
            .value
            .resources
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let decoded = (|| {
            if let Some(codec) = resources.codec.as_mut() {
                codec.record(bytes.len() as u64, 0)?;
            }
            fields(bytes, |tag, kind, input| {
                if tag == 1 {
                    if reply.response.value.results.len() == self.expected {
                        return Err(invalid());
                    }
                    let index = reply.response.value.results.len();
                    let mut item_credit = self.meter.as_ref().map(ReadAllocation::empty);
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
                        item_credit
                            .as_mut()
                            .ok_or_else(invalid)?
                            .absorb(&mut credit)?;
                        info.metadata
                    } else {
                        None
                    };
                    let item = message(kind, input)?;
                    let credits = resources
                        .texts
                        .value
                        .get_mut(index)
                        .map(|credits| std::mem::replace(credits, std::array::from_fn(|_| None)))
                        .ok_or_else(invalid)?;
                    let mut decode = Decode {
                        meter: self.meter.as_ref(),
                        allocation: item_credit.as_mut(),
                        copied: 0,
                        credits: credits.into_iter(),
                        codec: resources.codec.as_mut(),
                    };
                    let decoded = decode.result(item, metadata).and_then(|result| {
                        match &result.result {
                            Some(wire::put_object_result::Result::Object(object)) => {
                                info(object).map_err(NativeObjectsError::from)?;
                            }
                            Some(wire::put_object_result::Result::Error(error)) => {
                                item_error(error).map_err(NativeObjectsError::from)?;
                            }
                            None => return Err(invalid()),
                        }
                        Ok(result)
                    });
                    drop(decode);
                    match decoded {
                        Ok(result) => {
                            if let Some(credit) = item_credit.as_mut() {
                                let retained = match &result.result {
                                    Some(wire::put_object_result::Result::Object(object)) => {
                                        info_capacity(object)?
                                    }
                                    Some(wire::put_object_result::Result::Error(error)) => {
                                        error.request_id.capacity()
                                    }
                                    None => 0,
                                };
                                credit.resize_retained(retained)?;
                            }
                            if let (Some(owner), Some(credit)) =
                                (reply.response._allocation.as_mut(), item_credit.as_mut())
                            {
                                owner.absorb(credit)?;
                            }
                            reply.response.value.results.push(result);
                        }
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
        if let Some(codec) = resources.codec.as_mut() {
            let recorded = codec.failure.take();
            if reply.failure.is_none() {
                reply.failure = recorded.map(Into::into);
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

#[cfg(all(test, objects_loom))]
mod model {
    use super::*;
    fn history(early: bool) {
        let counts = std::sync::Arc::new(crate::test_meter::CountingMeter::default());
        let meter: std::sync::Arc<dyn NativeReadMeter> = counts;
        let mut sizes = batch_vector(1, Some(&meter)).unwrap();
        sizes.value.push(7);
        let received =
            received(Some(&meter), sizes, Completion::new(Some(&meter)).unwrap()).unwrap();
        let writer = {
            let meter = meter.clone();
            let received = received.clone();
            loom::thread::Builder::new()
                .stack_size(2 * 1024 * 1024)
                .spawn(move || {
                    if early {
                        received.value.completed.sent();
                    }
                    let mut header = wire::PutObjectsHeader {
                        objects: vec![wire::PutObjectHeader {
                            metadata: Some(wire::ObjectMetadata {
                                user: [("key".into(), "value".into())].into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                    };
                    received
                        .value
                        .keep_metadata(&mut header, Some(&meter))
                        .unwrap();
                    assert!(header.objects[0].metadata.is_none());
                    received.value.completed.sent();
                })
                .unwrap()
        };
        let reader = {
            let received = received.clone();
            loom::thread::Builder::new()
                .stack_size(2 * 1024 * 1024)
                .spawn(move || {
                    if received.value.completed.validate().is_err() {
                        return;
                    }
                    {
                        let metadata = received.value.metadata.lock().unwrap();
                        assert_eq!(
                            metadata.value.len(),
                            1,
                            "completion published before metadata transfer"
                        );
                        assert_eq!(metadata.value[0].as_ref().unwrap().user["key"], "value");
                    }
                    let response = batch_vector(1, Some(&meter)).unwrap();
                    let mut decoder = Decoder {
                        prepared: Some(Reply {
                            response: Guarded {
                                value: wire::PutObjectsResponse {
                                    results: response.value,
                                },
                                _allocation: response._allocation,
                            },
                            failure: None,
                        }),
                        expected: 1,
                        meter: Some(meter),
                        received: received.clone(),
                    };
                    let reply = wire::PutObjectsResponse {
                        results: vec![wire::PutObjectResult {
                            result: Some(wire::put_object_result::Result::Object(
                                wire::ObjectInfo {
                                    etag: "receipt".into(),
                                    size: 7,
                                    last_modified: Some(prost_types::Timestamp::default()),
                                    metadata: Some(wire::ObjectMetadata {
                                        user: [("key".into(), "value".into())].into(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            )),
                        }],
                    };
                    let reply = decoder.bytes(&reply.encode_to_vec()).unwrap();
                    decoder.accept(reply);
                    assert!(received.value.metadata.lock().unwrap().value[0].is_none());
                    drop(decoder);
                    let settled = received.value.reply.lock().unwrap().take().unwrap();
                    assert!(settled.failure.is_none());
                    assert_eq!(settled.response.value.results.len(), 1);
                    assert!(received.value.reply.lock().unwrap().take().is_none());
                })
                .unwrap()
        };
        writer.join().unwrap();
        reader.join().unwrap();
    }
    #[test]
    fn batch_exchange_models_metadata_transfer_decode_settlement_and_drop() {
        loom::model(|| history(false));
    }
    #[test]
    fn batch_exchange_rejects_publication_before_metadata_transfer() {
        assert!(std::panic::catch_unwind(|| loom::model(|| history(true))).is_err());
    }
}
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn fuzz_reply(
    bytes: &[u8],
    expected: usize,
    counts: std::sync::Arc<crate::test_meter::CountingMeter>,
    cancel: bool,
    allocation_budget: Option<u64>,
) -> Result<(), NativeObjectsError> {
    use std::sync::atomic::Ordering;
    assert!(expected <= 8);
    let meter: std::sync::Arc<dyn NativeReadMeter> = counts.clone();
    {
        let reference = wire::PutObjectsResponse::decode(bytes).ok();
        let mut sizes = batch_vector(expected, Some(&meter))?;
        let mut header = wire::PutObjectsHeader::default();
        for index in 0..expected {
            let object = reference
                .as_ref()
                .and_then(|reply| reply.results.get(index))
                .and_then(|item| match &item.result {
                    Some(wire::put_object_result::Result::Object(object)) => Some(object),
                    _ => None,
                });
            sizes
                .value
                .push(object.map_or(0, |object| object.size.min(1 << 40)));
            header.objects.push(wire::PutObjectHeader {
                metadata: object.and_then(|object| object.metadata.clone()),
                ..Default::default()
            });
        }
        let completed = Completion::new(Some(&meter))?;
        completed.sent();
        let received = received(Some(&meter), sizes, completed)?;
        received.value.keep_metadata(&mut header, Some(&meter))?;
        let response = batch_vector(expected, Some(&meter))?;
        let mut decoder = Decoder {
            prepared: Some(Reply {
                response: Guarded {
                    value: wire::PutObjectsResponse {
                        results: response.value,
                    },
                    _allocation: response._allocation,
                },
                failure: None,
            }),
            expected,
            meter: Some(meter),
            received,
        };
        if let Some(budget) = allocation_budget {
            counts.allocation_limit.store(
                counts.allocations.load(Ordering::SeqCst) + budget,
                Ordering::SeqCst,
            );
        }
        counts.cancelled.store(cancel, Ordering::SeqCst);
        let reply = decoder.bytes(bytes).map_err(|_| invalid())?;
        assert!(reply.response.value.results.len() <= expected);
        let retained = reply
            .response
            .value
            .results
            .iter()
            .try_fold(0usize, |total, item| {
                Ok::<_, NativeObjectsError>(
                    total
                        + match &item.result {
                            Some(wire::put_object_result::Result::Object(object)) => {
                                info_capacity(object)?
                            }
                            Some(wire::put_object_result::Result::Error(error)) => {
                                error.request_id.capacity()
                            }
                            None => 0,
                        },
                )
            })?
            + reply.response.value.results.capacity()
                * std::mem::size_of::<wire::PutObjectResult>();
        assert_eq!(
            reply
                .response
                ._allocation
                .as_ref()
                .ok_or_else(invalid)?
                .bytes(),
            retained as u64
        );
        if reply.failure.is_none() {
            assert_eq!(Some(&reply.response.value), reference.as_ref());
        }
        decoder.accept(reply);
        let reply = decoder
            .received
            .value
            .reply
            .lock()
            .map_err(|_| invalid())?
            .take()
            .ok_or_else(invalid)?;
        let accepted: u64 = reply
            .response
            .value
            .results
            .iter()
            .filter_map(|item| match &item.result {
                Some(wire::put_object_result::Result::Object(object)) => Some(object.size),
                _ => None,
            })
            .sum();
        assert_eq!(counts.written.load(Ordering::SeqCst), accepted);
        assert!(decoder.bytes(bytes).is_err());
    }
    assert_eq!(counts.retained(), 0);
    Ok(())
}

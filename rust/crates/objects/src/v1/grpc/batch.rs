use super::super::Guarded;
use super::*;
use crate::sync::{Arc, AtomicBool, Ordering};

struct PutCodec;
struct PutEncoder;

impl tonic::codec::Encoder for PutEncoder {
    type Item = Guarded<wire::PutObjectsRequest>;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        buffer: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        item.value
            .encode(buffer)
            .map_err(|_| tonic::Status::internal("batch encoding"))
    }
}

impl tonic::codec::Codec for PutCodec {
    type Encode = Guarded<wire::PutObjectsRequest>;
    type Decode = wire::PutObjectsResponse;
    type Encoder = PutEncoder;
    type Decoder = <tonic_prost::ProstCodec<wire::PutObjectsRequest, wire::PutObjectsResponse> as tonic::codec::Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        PutEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        tonic::codec::Codec::decoder(&mut tonic_prost::ProstCodec::<
            wire::PutObjectsRequest,
            wire::PutObjectsResponse,
        >::default())
    }
}

#[derive(Default)]
struct Completion {
    sent: AtomicBool,
    failed: AtomicBool,
}

impl Completion {
    fn sent(&self) {
        self.sent.store(true, Ordering::Release);
    }

    fn validate(&self) -> Result<(), Error> {
        if self.sent.load(Ordering::Acquire) && !self.failed.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(protocol())
        }
    }
}

fn item_error(detail: &wire::ErrorDetail) -> Result<Error, Error> {
    wire::ErrorCode::try_from(detail.code)
        .ok()
        .filter(|code| *code != wire::ErrorCode::Unspecified)
        .map(Error::from)
        .ok_or_else(protocol)
}

fn put_frames(
    bodies: Vec<OwnedBytes>,
    header: wire::PutObjectsHeader,
    completion: Arc<Completion>,
    meter: Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
) -> impl futures::Stream<Item = Guarded<wire::PutObjectsRequest>> + Send + 'static {
    let source = completion.clone();
    let items = stream::iter(bodies.into_iter().zip(0_u32..)).flat_map(move |(body, index)| {
        let meter = meter.clone();
        let source = source.clone();
        stream::unfold((Some(body), false), move |(pending, complete)| {
            let meter = meter.clone();
            let source = source.clone();
            async move {
                use wire::put_objects_chunk::Frame;
                let frame = if let Some(body) = pending.filter(|body| !body.is_empty()) {
                    let size = body.len().min(65_536);
                    let (chunk, remaining) = if size == body.len() {
                        (body, None)
                    } else {
                        (body.slice(..size), Some(body.slice(size..)))
                    };
                    let body = if let Some(meter) = meter.as_ref() {
                        match upload::native_body_bytes(chunk, meter) {
                            Ok(body) => body,
                            Err(_) => {
                                source.failed.store(true, Ordering::Release);
                                return None;
                            }
                        }
                    } else {
                        Guarded {
                            value: upload::body_bytes(chunk),
                            _allocation: None,
                        }
                    };
                    Some((
                        Frame::Body(body.value),
                        (remaining, false),
                        body._allocation,
                    ))
                } else if !complete {
                    Some((Frame::Complete(true), (None, true), None))
                } else {
                    None
                };
                frame.map(|(frame, next, allocation)| {
                    (
                        Guarded {
                            value: wire::PutObjectsRequest {
                                frame: Some(wire::put_objects_request::Frame::Chunk(
                                    wire::PutObjectsChunk {
                                        index,
                                        frame: Some(frame),
                                    },
                                )),
                            },
                            _allocation: allocation,
                        },
                        next,
                    )
                })
            }
        })
    });
    stream::once(async move {
        Guarded {
            value: wire::PutObjectsRequest {
                frame: Some(wire::put_objects_request::Frame::Header(header)),
            },
            _allocation: None,
        }
    })
    .chain(items)
    .chain(stream::once(async move {
        completion.sent();
        Guarded {
            value: wire::PutObjectsRequest {
                frame: Some(wire::put_objects_request::Frame::Complete(true)),
            },
            _allocation: None,
        }
    }))
}

impl GrpcObjects {
    pub(super) async fn put_objects(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        self.put_objects_with_custody(requests, (), None).await
    }

    async fn put_objects_with_custody<G: Send + Sync + 'static>(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
        custody: G,
        meter: Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        let result = async {
            let mut objects = batch_vector(count, meter.as_ref())?;
            let mut bodies = batch_vector(count, meter.as_ref())?;
            let mut sizes = batch_vector(count, meter.as_ref())?;
            for (header, body) in requests {
                sizes.value.push(body.len() as u64);
                objects.value.push(header);
                bodies.value.push(body);
            }
            let header = wire::PutObjectsHeader {
                objects: std::mem::take(&mut objects.value),
            };
            request::put_batch(&header)?;
            for size in &sizes.value {
                request::upload_length(*size)?;
            }
            let completion_custody = meter
                .as_ref()
                .map(|meter| {
                    acyclic_native_runtime::ReadAllocation::new(
                        meter,
                        std::mem::size_of::<Completion>(),
                    )
                })
                .transpose()
                .map_err(|_| protocol())?;
            let completed = Arc::new(Completion::default());
            let body_frames = put_frames(
                std::mem::take(&mut bodies.value),
                header,
                completed.clone(),
                meter.clone(),
            );
            let retire = stream::once(async move {
                drop((custody, objects, bodies, completion_custody));
                None::<Guarded<wire::PutObjectsRequest>>
            })
            .filter_map(futures::future::ready);
            let frames = body_frames.chain(retire).take_while(move |_| {
                futures::future::ready(meter.as_ref().is_none_or(|meter| meter.check().is_ok()))
            });
            let input = self.authenticated(frames, self.transfer_timeout).await?;
            let (mut client, path) = self
                .streaming("/acyclic.objects.v1.ObjectsService/PutObjects")
                .await?;
            let reply: wire::PutObjectsResponse = client
                .client_streaming(input, path, PutCodec)
                .await
                .map(ok)
                .map_err(|status| error_from_status(&status))?;
            completed.validate()?;
            if reply.results.len() != count {
                return Err(protocol());
            }
            reply
                .results
                .into_iter()
                .zip(sizes.value.drain(..))
                .map(|(result, size)| match result.result {
                    Some(wire::put_object_result::Result::Object(object)) => {
                        info(&object)?;
                        if object.size != size {
                            return Err(protocol());
                        }
                        Ok(Ok(object))
                    }
                    Some(wire::put_object_result::Result::Error(error)) => {
                        Ok(Err(item_error(&error)?))
                    }
                    None => Err(protocol()),
                })
                .collect::<Result<Vec<_>, Error>>()
        };
        match tokio::time::timeout(self.transfer_timeout, result).await {
            Ok(Ok(results)) => results,
            Ok(Err(error)) => vec![Err(error); count],
            Err(_) => vec![Err(wire::ErrorCode::Unavailable.into()); count],
        }
    }

    pub(super) fn put_objects_native(
        &self,
        mut input: super::super::ObjectsNativeRequest<Vec<(wire::PutObjectHeader, OwnedBytes)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        super::super::ObjectsNativeFuture<
            '_,
            super::super::ObjectsBatch<super::super::ObjectsPutReceipt>,
        >,
        super::super::NativeObjectsError,
    > {
        use super::super::{NativeObjectsError, ObjectsBatch, ObjectsPutReceipt};
        use acyclic_native_runtime::ReadAllocation;
        super::super::native_input_meter(&meter, &input)?;
        super::super::native_future(&meter.clone(), async move {
            let count = input.data().len();
            let bytes = count
                .checked_mul(std::mem::size_of::<
                    Result<ObjectsPutReceipt, NativeObjectsError>,
                >())
                .ok_or(std::io::ErrorKind::OutOfMemory)?;
            let mut allocation = ReadAllocation::new(&meter, bytes)?;
            let mut results = Vec::new();
            results
                .try_reserve_exact(count)
                .map_err(|error| meter.allocation_failed(error))?;
            allocation.reconcile_capacity(
                results.capacity()
                    * std::mem::size_of::<Result<ObjectsPutReceipt, NativeObjectsError>>(),
            )?;
            if count == 0 {
                return ObjectsBatch::new(results, allocation);
            }
            let maximum = input.data().iter().try_fold(0usize, |total, (_, body)| {
                total
                    .checked_add(body.len())
                    .ok_or(std::io::ErrorKind::InvalidInput)
            })?;
            meter.admit_write(maximum)?;
            let requests = std::mem::take(&mut input.data);
            let mut values = super::super::Guarded {
                value: self
                    .put_objects_with_custody(requests, input, Some(meter.clone()))
                    .await,
                _allocation: Some(ReadAllocation::empty(&meter)),
            };
            let capacity =
                values.value.capacity() * std::mem::size_of::<Result<wire::ObjectInfo, Error>>();
            if let Some(custody) = values._allocation.as_mut() {
                custody.reconcile_capacity(capacity)?;
            }
            let completed = values
                .value
                .iter()
                .filter_map(|result| result.as_ref().ok())
                .try_fold(0usize, |total, info| {
                    let size =
                        usize::try_from(info.size).map_err(|_| std::io::ErrorKind::InvalidData)?;
                    total
                        .checked_add(size)
                        .ok_or(std::io::ErrorKind::InvalidData)
                })?;
            let recorded = meter.record_write(completed);
            for value in values.value.drain(..) {
                results.push(match value {
                    Ok(info) => {
                        let mut custody = ReadAllocation::empty(&meter);
                        let retained = info_capacity(&info)?;
                        let claim = custody.reconcile_capacity(retained);
                        let receipt = ObjectsPutReceipt::new(info, Some(custody));
                        match claim {
                            Ok(()) => Ok(receipt),
                            Err(error) => {
                                let mut failure = NativeObjectsError::from(error);
                                failure.receipt = Some(Box::new(receipt));
                                Err(failure)
                            }
                        }
                    }
                    Err(error) => Err(error.into()),
                });
            }
            drop(values);
            recorded?;
            ObjectsBatch::new(results, allocation)
        })
    }

    pub(super) async fn get_objects(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        let result = async {
            let query = wire::GetObjectsRequest {
                objects: requests
                    .iter()
                    .map(|(request, maximum)| wire::GetObjectSelection {
                        request: Some(request.clone()),
                        maximum_bytes: *maximum,
                    })
                    .collect(),
            };
            request::get_batch(&query)?;
            let input = self.authenticated(query, self.transfer_timeout).await?;
            let (mut client, path) = self
                .streaming("/acyclic.objects.v1.ObjectsService/GetObjects")
                .await?;
            let mut frames: tonic::Streaming<wire::GetObjectsResponse> = client
                .server_streaming(input, path, tonic_prost::ProstCodec::default())
                .await
                .map(ok)
                .map_err(|status| error_from_status(&status))?;
            let mut results = Vec::with_capacity(count);
            for (index, (query, maximum)) in requests.into_iter().enumerate() {
                let mut header = None;
                let mut remaining = 0;
                let mut body = bytes::BytesMut::new();
                loop {
                    use wire::get_objects_response::Frame;
                    let frame = frames
                        .message()
                        .await
                        .map_err(|status| error_from_status(&status))?
                        .ok_or_else(protocol)?;
                    if usize::try_from(frame.index).ok() != Some(index) {
                        return Err(protocol());
                    }
                    match frame.frame {
                        Some(Frame::Header(value)) if header.is_none() => {
                            remaining = response::get_header(&value, &query.range, maximum)?;
                            header = Some(value);
                        }
                        Some(Frame::Body(value)) if header.is_some() => {
                            let chunk = response::body(value, remaining)?;
                            remaining -= chunk.len() as u64;
                            body.extend_from_slice(&chunk);
                        }
                        Some(Frame::Complete(true)) if header.is_some() && remaining == 0 => {
                            results.push(Ok(Object::new(
                                header.take().ok_or_else(protocol)?,
                                body.freeze().into(),
                            )));
                            break;
                        }
                        Some(Frame::Error(error)) => {
                            results.push(Err(item_error(&error)?));
                            break;
                        }
                        _ => return Err(protocol()),
                    }
                }
            }
            if frames
                .message()
                .await
                .map_err(|status| error_from_status(&status))?
                .is_some()
            {
                return Err(protocol());
            }
            Ok(results)
        };
        let error = match tokio::time::timeout(self.transfer_timeout, result).await {
            Ok(Ok(results)) => return results,
            Ok(Err(error)) => error,
            Err(_) => wire::ErrorCode::Unavailable.into(),
        };
        (0..count).map(|_| Err(error)).collect()
    }
}

fn batch_vector<T>(
    count: usize,
    meter: Option<&std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
) -> Result<super::super::Guarded<Vec<T>>, Error> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(protocol)?;
    let mut allocation = meter
        .map(|meter| acyclic_native_runtime::ReadAllocation::new(meter, bytes))
        .transpose()
        .map_err(|_| protocol())?;
    let mut value = Vec::new();
    value.try_reserve_exact(count).map_err(|error| {
        if let Some(meter) = meter {
            let _ = meter.allocation_failed(error);
        }
        protocol()
    })?;
    if let Some(allocation) = allocation.as_mut() {
        allocation
            .reconcile_capacity(value.capacity() * std::mem::size_of::<T>())
            .map_err(|_| protocol())?;
    }
    Ok(super::super::Guarded {
        value,
        _allocation: allocation,
    })
}

fn info_capacity(info: &wire::ObjectInfo) -> Result<usize, std::io::Error> {
    let mut bytes = info.etag.capacity();
    if let Some(metadata) = &info.metadata {
        for value in [
            &metadata.content_type,
            &metadata.content_encoding,
            &metadata.cache_control,
            &metadata.content_disposition,
            &metadata.content_language,
        ] {
            bytes = bytes
                .checked_add(value.capacity())
                .ok_or(std::io::ErrorKind::OutOfMemory)?;
        }
        for (key, value) in &metadata.user {
            bytes = bytes
                .checked_add(std::mem::size_of::<(String, String)>())
                .and_then(|bytes| bytes.checked_add(key.capacity()))
                .and_then(|bytes| bytes.checked_add(value.capacity()))
                .ok_or(std::io::ErrorKind::OutOfMemory)?;
        }
    }
    Ok(bytes)
}

#[cfg(all(test, not(objects_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn headers(count: usize) -> wire::PutObjectsHeader {
        wire::PutObjectsHeader {
            objects: (0..count)
                .map(|index| wire::PutObjectHeader {
                    bucket: Some(wire::BucketRef {
                        name: "batch".into(),
                    }),
                    object_key: format!("key-{index}"),
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn batch_upload_moves_unique_headers_and_chunks() {
        for length in [1, 65_536] {
            let body = vec![7; length];
            let body_pointer = body.as_ptr();
            let header = headers(1);
            let key_pointer = header.objects[0].object_key.as_ptr();
            let completion = Arc::new(Completion::default());
            let frames = put_frames(vec![body.into()], header, completion.clone(), None);
            futures::pin_mut!(frames);
            let first = frames.next().await.unwrap();
            let Some(wire::put_objects_request::Frame::Header(header)) = first.value.frame else {
                panic!("header")
            };
            assert_eq!(header.objects[0].object_key.as_ptr(), key_pointer);
            let frame = frames.next().await.unwrap();
            let Some(wire::put_objects_request::Frame::Chunk(wire::PutObjectsChunk {
                frame: Some(wire::put_objects_chunk::Frame::Body(body)),
                ..
            })) = frame.value.frame
            else {
                panic!("body")
            };
            assert_eq!(body.as_ptr(), body_pointer);
            assert!(completion.validate().is_err());
            while frames.next().await.is_some() {}
            completion.validate().unwrap();
        }
    }

    #[tokio::test]
    async fn dropping_upload_before_final_marker_rejects_early_success() {
        let completion = Arc::new(Completion::default());
        let mut frames = Box::pin(put_frames(
            vec![vec![1; 65_537].into()],
            headers(1),
            completion.clone(),
            None,
        ));
        frames.next().await.unwrap();
        frames.next().await.unwrap();
        assert!(completion.validate().is_err());
        drop(frames);
        assert!(completion.validate().is_err());
    }

    proptest! {
        #[test]
        fn batch_upload_round_trips_and_rejects_every_truncated_prefix(
            bodies in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..1024), 1..8),
            split in any::<bool>(),
        ) {
            let header = headers(bodies.len());
            let owned: Vec<_> = bodies.iter().map(|body| {
                let body = OwnedBytes::from(body.clone());
                if split { body.slice(..) } else { body }
            }).collect();
            let frames = futures::executor::block_on(put_frames(owned, header.clone(), Arc::new(Completion::default()), None).collect::<Vec<_>>());
            let mut reconstructed = vec![Vec::new(); bodies.len()];
            for prefix in 0..frames.len() {
                let mut parser = request::BatchUploadFraming::new(&header).unwrap();
                for frame in frames.iter().skip(1).take(prefix.saturating_sub(1)) {
                    let decoded = wire::PutObjectsRequest::decode(frame.value.encode_to_vec().as_slice()).unwrap();
                    parser.accept(&decoded).unwrap();
                }
                prop_assert!(parser.finish().is_err());
            }
            let mut parser = request::BatchUploadFraming::new(&header).unwrap();
            for frame in frames.iter().skip(1) {
                let decoded = wire::PutObjectsRequest::decode(frame.value.encode_to_vec().as_slice()).unwrap();
                parser.accept(&decoded).unwrap();
                if let Some(wire::put_objects_request::Frame::Chunk(wire::PutObjectsChunk {
                    index, frame: Some(wire::put_objects_chunk::Frame::Body(body)),
                })) = decoded.frame { reconstructed[index as usize].extend(body); }
            }
            parser.finish().unwrap();
            prop_assert_eq!(reconstructed, bodies);
        }
    }
}

#[cfg(all(test, objects_loom))]
mod model {
    use super::*;

    fn history(early_completion: bool) {
        let completion = Arc::new(Completion::default());
        let sent = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let writer = {
            let (completion, sent, cancelled) =
                (completion.clone(), sent.clone(), cancelled.clone());
            loom::thread::spawn(move || {
                if early_completion {
                    completion.sent();
                }
                if !cancelled.load(Ordering::Acquire) {
                    sent.store(true, Ordering::Release);
                    completion.sent();
                }
            })
        };
        let observer = loom::thread::spawn(move || {
            if completion.validate().is_ok() {
                assert!(
                    sent.load(Ordering::Acquire),
                    "success before final upload frame"
                );
            }
            cancelled.store(true, Ordering::Release);
        });
        writer.join().unwrap();
        observer.join().unwrap();
    }

    #[test]
    fn batch_completion_models_cancellation_and_early_replies() {
        loom::model(|| history(false));
    }

    #[test]
    fn batch_completion_rejects_the_early_success_control() {
        assert!(std::panic::catch_unwind(|| loom::model(|| history(true))).is_err());
    }
}

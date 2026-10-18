use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn item_error(detail: &wire::ErrorDetail) -> Result<Error, Error> {
    wire::ErrorCode::try_from(detail.code)
        .ok()
        .filter(|code| *code != wire::ErrorCode::Unspecified)
        .map(Error::from)
        .ok_or_else(protocol)
}

fn put_frames(
    requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    header: wire::PutObjectsHeader,
    completion: Arc<AtomicBool>,
) -> impl futures::Stream<Item = wire::PutObjectsRequest> + Send + 'static {
    let items = stream::iter(requests.into_iter().zip(0_u32..)).flat_map(|((_, body), index)| {
        stream::unfold((body, 0), move |(body, offset)| async move {
            use wire::put_objects_chunk::Frame;
            let frame = if offset < body.len() {
                let end = offset.saturating_add(65_536).min(body.len());
                Some((Frame::Body(body.slice(offset..end).to_vec()), end))
            } else if offset == body.len() {
                Some((Frame::Complete(true), offset + 1))
            } else {
                None
            };
            frame.map(|(frame, next)| {
                (
                    wire::PutObjectsRequest {
                        frame: Some(wire::put_objects_request::Frame::Chunk(
                            wire::PutObjectsChunk {
                                index,
                                frame: Some(frame),
                            },
                        )),
                    },
                    (body, next),
                )
            })
        })
    });
    stream::once(async move {
        wire::PutObjectsRequest {
            frame: Some(wire::put_objects_request::Frame::Header(header)),
        }
    })
    .chain(items)
    .chain(stream::once(async move {
        completion.store(true, Ordering::Release);
        wire::PutObjectsRequest {
            frame: Some(wire::put_objects_request::Frame::Complete(true)),
        }
    }))
}

impl GrpcObjects {
    pub(super) async fn put_objects(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        let result = async {
            let header = wire::PutObjectsHeader {
                objects: requests.iter().map(|(header, _)| header.clone()).collect(),
            };
            request::put_batch(&header)?;
            let sizes: Vec<_> = requests.iter().map(|(_, body)| body.len() as u64).collect();
            for size in &sizes {
                request::upload_length(*size)?;
            }
            let completed = Arc::new(AtomicBool::new(false));
            let frames = put_frames(requests, header, completed.clone());
            let input = self.authenticated(frames, self.transfer_timeout).await?;
            let (mut client, path) = self
                .streaming("/acyclic.objects.v1.ObjectsService/PutObjects")
                .await?;
            let reply: wire::PutObjectsResponse = client
                .client_streaming(input, path, tonic_prost::ProstCodec::default())
                .await
                .map(ok)
                .map_err(|status| error_from_status(&status))?;
            if !completed.load(Ordering::Acquire) || reply.results.len() != count {
                return Err(protocol());
            }
            reply
                .results
                .into_iter()
                .zip(sizes)
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

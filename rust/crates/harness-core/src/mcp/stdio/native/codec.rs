use crate::mcp::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use bytes::{BufMut as _, BytesMut};
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde::{Serialize, de::DeserializeOwned};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

pub(super) struct BoundedCodec<T> {
    decoder: JsonRpcMessageCodec<T>,
    maximum: Arc<AtomicU32>,
    decoder_maximum: u32,
}

impl<T> BoundedCodec<T> {
    pub(super) fn new(maximum: Arc<AtomicU32>) -> Self {
        let decoder_maximum = maximum.load(Ordering::Acquire);
        Self {
            decoder: JsonRpcMessageCodec::new_with_max_length(decoder_maximum as usize),
            maximum,
            decoder_maximum,
        }
    }

    fn refresh_decoder_bound(&mut self) {
        let maximum = self.maximum.load(Ordering::Acquire);
        if maximum != self.decoder_maximum {
            self.decoder = JsonRpcMessageCodec::new_with_max_length(maximum as usize);
            self.decoder_maximum = maximum;
        }
    }
}

impl<T: DeserializeOwned> Decoder for BoundedCodec<T> {
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        self.refresh_decoder_bound();
        self.decoder.decode(buffer).map_err(Into::into)
    }

    fn decode_eof(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        self.refresh_decoder_bound();
        self.decoder.decode_eof(buffer).map_err(Into::into)
    }
}

impl<T: Serialize> Encoder<T> for BoundedCodec<T> {
    type Error = io::Error;

    fn encode(&mut self, item: T, buffer: &mut BytesMut) -> io::Result<()> {
        let start = buffer.len();
        let maximum = u64::from(self.maximum.load(Ordering::Acquire));
        let result = crate::executor::json::json_to_writer_bounded(
            &item,
            &mut (&mut *buffer).writer(),
            crate::executor::json::JsonBounds {
                maximum_bytes: maximum,
                maximum_items: maximum,
                maximum_depth: 256,
            },
            &mut |_| Ok(()),
        );
        if let Err(error) = result {
            buffer.truncate(start);
            return Err(io::Error::new(io::ErrorKind::InvalidData, error));
        }
        buffer.put_u8(b'\n');
        Ok(())
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod bounded_encoder_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn wire_bytes_are_unchanged_and_failed_messages_preserve_the_prefix(text in ".{0,256}") {
            let value = serde_json::json!({"message": text, "values": [null, true, 17]});
            let encoded = serde_json::to_vec(&value).unwrap();
            let length = u32::try_from(encoded.len()).unwrap();
            let limit = Arc::new(AtomicU32::new(length));
            let mut codec = BoundedCodec::new(Arc::clone(&limit));
            let prefix = b"an earlier message\n";
            let mut buffer = BytesMut::from(&prefix[..]);
            codec.encode(value.clone(), &mut buffer).unwrap();
            prop_assert_eq!(&buffer[prefix.len()..buffer.len() - 1], encoded.as_slice());
            prop_assert_eq!(buffer.last(), Some(&b'\n'));
            let completed = buffer.clone();
            limit.store(length - 1, Ordering::Release);
            prop_assert!(codec.encode(value, &mut buffer).is_err());
            prop_assert_eq!(buffer, completed);
        }
    }

    #[test]
    fn transport_cannot_bypass_the_shared_nesting_admission() {
        let mut value = serde_json::Value::Null;
        for _ in 0..257 {
            value = serde_json::Value::Array(vec![value]);
        }
        // Actual former serde writer admitted this message under its byte bound.
        assert!(serde_json::to_vec(&value).unwrap().len() < 4096);
        let mut codec = BoundedCodec::new(Arc::new(AtomicU32::new(4096)));
        let mut buffer = BytesMut::from(&b"retained\n"[..]);
        assert!(codec.encode(value, &mut buffer).is_err());
        assert_eq!(buffer.as_ref(), b"retained\n");
    }
}

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod tests {
    use super::*;
    use loom::sync::mpsc;

    fn handoff_model(omit_publication: bool) {
        loom::model(move || {
            let limit = Arc::new(AtomicU32::new(16));
            let follower_limit = Arc::clone(&limit);
            let (ready_send, ready_receive) = mpsc::channel();
            let (tool_send, tool_receive) = mpsc::channel();
            let follower = loom::thread::spawn(move || {
                let mut codec = BoundedCodec::<serde_json::Value>::new(follower_limit);
                ready_send.send(()).unwrap();
                tool_receive.recv().unwrap();
                let value = serde_json::json!("x".repeat(32));
                let mut bytes = BytesMut::new();
                codec
                    .encode(value.clone(), &mut bytes)
                    .expect("tool bound must reach encoder");
                assert_eq!(codec.decode(&mut bytes).unwrap(), Some(value));
            });
            ready_receive.recv().unwrap();
            // Negative control omits the catalog-to-tool bound publication.
            if !omit_publication {
                limit.store(64, Ordering::Release);
            }
            tool_send.send(()).unwrap();
            follower.join().unwrap();
        });
    }

    #[test]
    fn catalog_to_tool_handoff_updates_the_real_encoder_and_decoder() {
        handoff_model(false);
    }

    #[test]
    #[should_panic(expected = "tool bound must reach encoder")]
    fn negative_control_missing_publication_is_detected() {
        handoff_model(true);
    }
}

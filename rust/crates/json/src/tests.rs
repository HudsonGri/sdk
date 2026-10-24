use super::*;
use proptest::prelude::*;
use serde_json::json;
fn bounds(bytes: usize) -> Bounds {
    Bounds {
        maximum_bytes: bytes as u64,
        maximum_items: bytes as u64,
        maximum_depth: 32,
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn every_sink_and_counter_share_wire_bytes_and_limits(text in ".{0,128}", pretty in any::<bool>()) {
        let value = json!({"escaped": text, "items": [1, true, null]});
        let expected = if pretty { serde_json::to_vec_pretty(&value).unwrap() } else { serde_json::to_vec(&value).unwrap() };
        let mut output = Vec::new();
        prop_assert_eq!(to_writer(&value, &mut output, bounds(expected.len()), pretty).unwrap(), expected.len() as u64);
        prop_assert_eq!(output, expected.clone());
        prop_assert!(matches!(to_writer(&value, &mut Vec::new(), bounds(expected.len() - 1), pretty), Err(EncodeError::Admission(Admission::Bytes))));
        prop_assert_eq!(count(&value, bounds(expected.len())).unwrap(), serde_json::to_vec(&value).unwrap().len() as u64);
    }
}

#[test]
fn byte_only_writer_cannot_satisfy_the_structural_boundary() {
    let value = json!([1, 2, 3]);
    let bounds = Bounds {
        maximum_bytes: 1024,
        maximum_items: 3,
        maximum_depth: 32,
    };
    assert!(matches!(
        count(&value, bounds),
        Err(EncodeError::Admission(Admission::Items))
    ));
    // Exact old byte-only policy admits this document under the same byte cap.
    assert!(serde_json::to_vec(&value).unwrap().len() < bounds.maximum_bytes as usize);
    let nested = json!([[[[0]]]]);
    assert!(matches!(
        count(
            &nested,
            Bounds {
                maximum_items: 1024,
                maximum_depth: 2,
                ..bounds
            }
        ),
        Err(EncodeError::Admission(Admission::Depth))
    ));
}

#[test]
fn serializers_and_sinks_keep_their_original_error_class() {
    struct Invalid;
    impl serde::Serialize for Invalid {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("fixture serialization refusal"))
        }
    }
    assert!(matches!(
        count(&Invalid, bounds(1024)),
        Err(EncodeError::Serialization(_))
    ));
    struct Closed;
    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert!(matches!(to_writer(&[1], &mut Closed, bounds(1024), false),
        Err(EncodeError::Output(error)) if error.kind() == io::ErrorKind::BrokenPipe));
}

//! Independent decoder oracle shared by shrinking properties and fuzzing.
use super::*;

fn legacy_json<'a, T: PreparationRecord>(
    bytes: &[u8],
    io: Option<&'a PreparationIo>,
) -> Result<(T, Option<crate::conversation::ContentHold<'a>>)> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        {
            let parsed = io.decode(
                std::marker::PhantomData::<Value>,
                bytes,
                crate::wire::ErrorCode::Storage,
            )?;
            let _canonical_hold;
            let canonical = io.encode(&parsed.0, bytes.len() as u64, true)?;
            _canonical_hold = io.guard_bytes(&canonical)?;
            if canonical.as_ref() != bytes {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "execution journal JSON is not canonical",
                ));
            }
        }
        let (value, hold) = io.decode(
            std::marker::PhantomData::<T>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        return Ok((value, Some(hold)));
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    let parsed: Value = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Storage(format!("execution journal JSON is invalid: {error}")))?;
    if crate::contract::canonical_json_bytes(&parsed)? != bytes {
        return Err(Error::Storage(
            "execution journal JSON is not canonical".into(),
        ));
    }
    let value = crate::contract::json_from_slice(bytes).map_err(|error| {
        Error::Storage(format!("execution journal content is invalid: {error}"))
    })?;
    Ok((value, None))
}

fn check<T: PreparationRecord + std::fmt::Debug + PartialEq>(bytes: &[u8]) {
    let expected = legacy_json::<T>(bytes, None)
        .map(|(value, _)| value)
        .map_err(|e| (e.kind(), e.to_string()));
    let actual = decode_json::<T>(bytes, None)
        .map(|(value, _)| value)
        .map_err(|e| (e.kind(), e.to_string()));
    assert_eq!(actual, expected);
}

/// Compares complete decoded values and exact errors with the pre-change path.
pub fn fuzz_execution_json(bytes: &[u8]) {
    check::<Value>(bytes);
    check::<ModelEvent>(bytes);
    check::<ModelRequest>(bytes);
    if let Ok(value) = legacy_json::<Value>(bytes, None).map(|(value, _)| value) {
        let encoded = crate::contract::canonical_json_bytes(&value);
        assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            assert_eq!(
                decode_json::<Value>(&encoded, None)
                    .map(|(value, _)| value)
                    .map_err(|error| (error.kind(), error.to_string())),
                Ok(value)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn journal_decode_keeps_only_one_tree_live(length in 0usize..65537) {
            for value in [Value::String("x".repeat(length)), serde_json::json!({"z":["\0\n\"\\é".repeat(length / 8)], "a":length})] {
                let bytes = crate::contract::canonical_json_bytes(&value)?;
                let mut parsed = None;
                let one = acyclic_test_support::measure(|| parsed = Some(crate::contract::json_from_slice::<Value>(&bytes).unwrap()));
                drop(parsed.take());
                let mut decoded = None;
                let actual = acyclic_test_support::measure(|| decoded = Some(decode_json::<Value>(&bytes, None).unwrap().0));
                prop_assert_eq!(decoded, Some(value));
                prop_assert_eq!(actual.count_total, 2 * one.count_total);
                prop_assert_eq!(actual.bytes_total, 2 * one.bytes_total);
                prop_assert_eq!(actual.bytes_max, one.bytes_max);
                prop_assert_eq!(actual.bytes_current, one.bytes_current);
                let before = acyclic_test_support::measure(|| { std::hint::black_box(legacy_json::<Value>(&bytes, None).unwrap().0); });
                prop_assert!(before.count_total > actual.count_total);
                prop_assert!(before.bytes_max > actual.bytes_max, "old growing encoding/tree clone passed the same bound");
            }
        }

        #[test]
        fn journal_decode_round_trips_and_preserves_errors(
            text in ".{0,512}", arbitrary in prop::collection::vec(any::<u8>(), 0..2048),
        ) {
            for value in [serde_json::json!({"kind":"content", "delta": text}),
                serde_json::json!({"max_output_tokens":null,"messages":[{"content":text,"role":"user"}],
                    "model":{"name":"model","options":null,"provider":"test","revision":"1"},"tools":[]})] {
                let encoded = crate::contract::canonical_json_bytes(&value)?;
                fuzz_execution_json(&encoded);
                let mut noncanonical = encoded; noncanonical.push(b' ');
                fuzz_execution_json(&noncanonical);
            }
            fuzz_execution_json(&arbitrary);
        }
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod accounted {
    use super::*;
    use crate::test_support::differential;
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn accounted_decoder_preserves_full_outputs_traces_and_custody(
            text in ".{0,256}", budget in 1u64..131073, cancelled in any::<bool>(),
            malformed in any::<bool>(),
        ) {
            let mut bytes = crate::contract::canonical_json_bytes(&serde_json::json!({"z":[text],"a":true}))?;
            if malformed { bytes.push(b' '); }
            for allowance in [budget, 1 << 26] {
                for native in [false, true] {
                    let [before, after] = differential::pair(|| bytes.clone(), |bytes, old| {
                        differential::accounted(native, allowance, cancelled, |io| {
                            let result = if old { legacy_json::<Value>(&bytes, Some(io)) }
                                else { decode_json::<Value>(&bytes, Some(io)) };
                            result.map(|(value, hold)| {
                                let observed = crate::contract::canonical_json_bytes(&value).unwrap();
                                drop(value); drop(hold); observed
                            }).map_err(|error| (error.kind(), error.to_string()))
                        })
                    });
                    prop_assert_eq!(before, after);
                }
            }
        }
    }
}

//! Exact aggregate item boundaries on the canonical serializer's native path.
thread_local! {
    pub(super) static RESTORE_BYTE_CHARGE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[test]
fn encoding_denial_stops_visiting_the_value_on_every_sink() {
    use super::*;
    use serde::ser::SerializeSeq as _;
    use std::cell::Cell;
    struct Sequence<'a>(&'a Cell<usize>);
    impl serde::Serialize for Sequence<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            let mut seq = serializer.serialize_seq(Some(1000))?;
            for _ in 0..1000 {
                self.0.set(self.0.get() + 1);
                seq.serialize_element(&0)?;
            }
            seq.end()
        }
    }
    let visits = Cell::new(0);
    let bounds = JsonBounds {
        maximum_bytes: 4096,
        maximum_items: 1,
        maximum_depth: 8,
    };
    assert!(json_count_bounded(&Sequence(&visits), bounds).is_err());
    assert_eq!(visits.replace(0), 1);
    assert!(json_bytes_bounded(&Sequence(&visits), bounds, None, &mut |_| Ok(())).is_err());
    assert_eq!(visits.replace(0), 1);
    assert!(
        json_to_writer_bounded(&Sequence(&visits), &mut Vec::new(), bounds, &mut |_| Ok(()))
            .is_err()
    );
    assert_eq!(visits.replace(0), 1);
    // The former deferred-failure writer acknowledged every write after its
    // first denial. It still visited the complete value despite bounded output.
    struct Deferred;
    impl std::io::Write for Deferred {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Deferred, &Sequence(&visits)).unwrap();
    assert_eq!(visits.get(), 1000);
}

#[test]
fn external_sink_uses_the_owned_encoder_item_and_cancellation_boundary() {
    use super::*;
    let value = serde_json::json!([1, 2, 3]);
    let bounds = JsonBounds {
        maximum_bytes: 128,
        maximum_items: 3,
        maximum_depth: 8,
    };
    // Counting, owned encoding, and an externally owned transport sink must
    // reject the same fourth item. The former byte-only sink accepts it.
    let mut old = Vec::new();
    serde_json::to_writer(&mut old, &value).unwrap();
    assert_eq!(old, b"[1,2,3]");
    assert!(json_bytes_bounded(&value, bounds, None, &mut |_| Ok(())).is_err());
    assert!(count_json_bytes_trusted(&value, bounds, None, &mut |_| Ok(())).is_err());
    let mut output = Vec::new();
    assert!(json_to_writer_bounded(&value, &mut output, bounds, &mut |_| Ok(())).is_err());
    let mut output = Vec::new();
    assert!(
        json_to_writer_bounded(&value, &mut output, bounds, &mut |_| {
            Err(Error::Conflict("original operation cancelled".into()))
        })
        .is_err()
    );
    assert!(
        output.is_empty(),
        "a denied original operation reached the transport"
    );
    let bytes = json_to_writer_bounded(
        &value,
        &mut output,
        JsonBounds {
            maximum_items: 4,
            ..bounds
        },
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(bytes, u64::try_from(old.len()).unwrap());
    assert_eq!(output, old);
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native {
    use super::*;
    use crate::executor::json::canonical_json_digest;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters, WorkError};
    use proptest::prelude::*;

    const LIMIT: u64 = 65_536;
    // One array and one string, examined by serialization and index construction.
    const ITEMS: u64 = 2 * 2;

    fn check(prior: u64, restore: bool) -> std::result::Result<(), String> {
        let old = RESTORE_BYTE_CHARGE.replace(restore);
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                RESTORE_BYTE_CHARGE.set(self.0);
            }
        }
        let _reset = Reset(old);
        let native = NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(LIMIT, 4 * 1024 * 1024),
            &CancellationToken::new(),
        )
        .unwrap();
        native
            .charge(WorkCounters {
                items_examined: prior,
                ..WorkCounters::UNCHARGED
            })
            .unwrap();
        let baseline = native.live_allocation_bytes();
        let value = serde_json::json!(["x".repeat(4096)]);
        let expected = serde_json::to_vec(&value).unwrap();
        let mut history = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native).unwrap(),
        );
        let result = canonical_json_digest(&value, LIMIT, &mut Some(&mut history));
        if prior + ITEMS <= LIMIT {
            let digest = result
                .map_err(|error| format!("admitted total {} rejected: {error:?}", prior + ITEMS))?;
            if digest != *blake3::hash(&expected).as_bytes()
                || native.receipt().items_examined != prior + ITEMS
                || native.receipt().bytes_hashed != expected.len() as u64
            {
                return Err("canonical bytes or exact native work differ".into());
            }
        } else {
            match result {
                Err(crate::Error::NativeWork {
                    cause:
                        acyclic_fs::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                            counter: "items_examined",
                            observed,
                            maximum: LIMIT,
                        }),
                    ..
                }) if observed > LIMIT && observed <= prior + ITEMS => {}
                other => {
                    return Err(format!(
                        "exhausted item budget did not retain its cause: {other:?}"
                    ));
                }
            }
            // Each admission is atomic: no rejected work is charged.
            if native.receipt().items_examined > LIMIT {
                return Err("denied work changed the original ledger".into());
            }
            if prior + ITEMS == LIMIT + 1 && native.receipt().items_examined != prior + 2 {
                return Err("denied index scan changed the admitted serialization prefix".into());
            }
        }
        drop(history);
        if native.live_allocation_bytes() != baseline {
            return Err("canonical digest leaked native custody".into());
        }
        Ok(())
    }

    #[test]
    fn canonical_array_item_budget_accepts_65536_and_rejects_65537() {
        check(LIMIT - ITEMS, false).unwrap();
        check(LIMIT + 1 - ITEMS, false).unwrap();
    }

    #[test]
    fn restored_byte_charge_fails_the_same_native_item_boundary_oracle() {
        // A roomy budget reaches the exact-work equality; the tight budget
        // independently proves that byte overcharging rejects an admitted call.
        let error = check(0, true).unwrap_err();
        assert_eq!(error, "canonical bytes or exact native work differ");
        let error = check(LIMIT - ITEMS, true).unwrap_err();
        assert!(error.contains("admitted total 65536 rejected"), "{error}");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn every_native_item_prefix_preserves_exact_work_and_denial(prior in 0u64..=LIMIT) {
            check(prior, false).map_err(TestCaseError::fail)?;
        }
    }
}

#[test]
fn unobserved_size_count_uses_the_same_depth_admission_as_owned_encoding() {
    use super::{JsonBounds, json_byte_length, json_bytes_bounded};
    let mut value = serde_json::Value::Null;
    for _ in 0..257 {
        value = serde_json::Value::Array(vec![value]);
    }
    assert!(json_byte_length(&value, 4096, &mut None).is_err());
    let bounds = JsonBounds {
        maximum_bytes: 4096,
        maximum_items: 4096,
        maximum_depth: 256,
    };
    assert!(json_bytes_bounded(&value, bounds, None, &mut |_| Ok(())).is_err());
    // The former standalone serde counter has no nesting admission.
    let old = serde_json::to_vec(&value).unwrap();
    assert!(old.len() < 4096);
    assert_eq!(
        json_byte_length(&serde_json::json!({"x": [1, true]}), 14, &mut None).unwrap(),
        14
    );
}

#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| {
    if let Ok(snapshot) = acyclic_harness::file_citations::FileCitations::decode(bytes, 65_536) {
        let encoded = serde_json::to_vec(&snapshot).unwrap();
        assert_eq!(
            acyclic_harness::file_citations::FileCitations::decode(&encoded, encoded.len() as u64)
                .unwrap(),
            snapshot
        );
    }
});

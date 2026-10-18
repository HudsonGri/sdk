#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(records) = loom_lock_scan::records::decode(bytes) {
        let mut encoded = (records.len() as u32).to_be_bytes().to_vec();
        for source in records {
            encoded.extend((source.len() as u32).to_be_bytes());
            encoded.extend(source.as_bytes());
            let _ = loom_lock_scan::bounds::bounded(source);
        }
        assert_eq!(encoded, bytes);
    }
});

#![no_main]
#[path = "../../src/native_host/macos_record.rs"]
mod macos_record;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let start = bytes.first().map_or(0, |byte| usize::from(*byte) * 8);
    for start in [0, start] {
        if let Ok(record) = macos_record::record(bytes, start) {
            assert!(start + record.len() <= bytes.len());
            if let Ok(name) = macos_record::name(record, 144) {
                assert!(!name.to_bytes().is_empty());
                assert!(!name.to_bytes().contains(&b'/'));
            }
        }
    }
});

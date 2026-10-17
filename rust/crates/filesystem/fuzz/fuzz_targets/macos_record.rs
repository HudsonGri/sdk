#![no_main]
#[path = "../../src/native_host/macos_record.rs"]
mod macos_record;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let start = bytes.first().map_or(0, |byte| usize::from(*byte) * 8);
    for start in [0, start] {
        if let Ok(record) = macos_record::record(bytes, start) {
            assert!(start + record.len() <= bytes.len());
            for at in [4, 8, 12, 16, 32, 36, 104, 108, 112, 116, 128, 132] {
                assert_eq!(
                    macos_record::u32_at(record, at).is_ok(),
                    at + 4 <= record.len()
                );
            }
            for at in [40, 48, 56, 64, 72, 80, 88, 96, 120, 136] {
                assert_eq!(
                    macos_record::i64_at(record, at).is_ok(),
                    at + 8 <= record.len()
                );
            }
            if let Ok(name) = macos_record::name(record, 144) {
                assert!(!name.to_bytes().is_empty());
                assert!(!name.to_bytes().contains(&b'/'));
            }
        }
    }
});

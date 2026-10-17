use super::macos_record::{name, record};
use proptest::prelude::*;
const FIXED: usize = 144;
fn encode(name: &[u8]) -> Vec<u8> {
    let length = (FIXED + name.len() + 1).next_multiple_of(8);
    let mut bytes = vec![0; length];
    bytes[..4].copy_from_slice(&(length as u32).to_ne_bytes());
    bytes[4..8].copy_from_slice(&1_u32.to_ne_bytes());
    bytes[24..28].copy_from_slice(&((FIXED - 24) as i32).to_ne_bytes());
    bytes[28..32].copy_from_slice(&((name.len() + 1) as u32).to_ne_bytes());
    bytes[FIXED..FIXED + name.len()].copy_from_slice(name);
    bytes
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn names_round_trip(value in "[a-zA-Z0-9_.-]{1,255}", padding in 0_usize..8,
        facts in prop::collection::vec(any::<u8>(), 112)) {
        let mut bytes = encode(value.as_bytes());
        bytes[32..144].copy_from_slice(&facts);
        let mut records = vec![0; padding * 8];
        records.extend_from_slice(&bytes);
        let parsed = record(&records, padding * 8).unwrap();
        prop_assert_eq!(name(parsed, FIXED).unwrap().to_bytes(), value.as_bytes());
        prop_assert_eq!(parsed.len(), bytes.len());
        for at in [32, 36, 104, 108, 112, 116, 128, 132] {
            prop_assert_eq!(super::macos_record::u32_at(parsed, at).unwrap().to_ne_bytes(), facts[at - 32..at - 28]);
        }
        for at in [40, 48, 56, 64, 72, 80, 88, 96, 120, 136] {
            prop_assert_eq!(super::macos_record::i64_at(parsed, at).unwrap().to_ne_bytes(), facts[at - 32..at - 24]);
        }
        // Negative control: an offset into the fixed metadata cannot name an entry.
        let mut broken = bytes.clone();
        broken[24..28].copy_from_slice(&0_i32.to_ne_bytes());
        prop_assert!(name(&broken, FIXED).is_err());
    }
    #[test]
    fn arbitrary_records_never_escape_the_initialized_prefix(bytes in prop::collection::vec(any::<u8>(), 0..1024), start in any::<usize>()) {
        if let Ok(parsed) = record(&bytes, start) {
            prop_assert!(start + parsed.len() <= bytes.len());
            if let Ok(value) = name(parsed, FIXED) {
                prop_assert!(!value.to_bytes().is_empty());
                prop_assert!(!value.to_bytes().contains(&b'/'));
            }
        }
    }
}
#[test]
fn truncated_records_and_names_are_rejected() {
    let bytes = encode(b"entry");
    for end in 0..bytes.len() {
        assert!(record(&bytes[..end], 0).is_err());
    }
    let mut broken = bytes.clone();
    broken[FIXED + 5] = b'x';
    assert!(name(&broken, FIXED).is_err());
    broken[24..28].copy_from_slice(&(-1_i32).to_ne_bytes());
    assert!(name(&broken, FIXED).is_err());
    broken[24..28].copy_from_slice(&i32::MAX.to_ne_bytes());
    assert!(name(&broken, FIXED).is_err());
}

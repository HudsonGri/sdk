#![no_main]

#[path = "../../src/cache_record.rs"]
mod cache_record;
pub use acyclic_fuzz_support as fuzz_support;
use sha2::{Digest, Sha256};

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let digest = format!("sha256:{:x}", Sha256::digest(bytes));
    let encoded = cache_record::encode(&digest, &digest);
    let _ = fuzz_support::round_trip_option(&digest, cache_record::decode(&encoded, &digest));
    if let Some(split) = bytes.iter().position(|byte| *byte == b'\n') {
        if let Some(key) = fuzz_input
            .slice(0..split)
            .and_then(fuzz_support::FuzzInput::text)
        {
            if let Some(value) = fuzz_input
                .slice(split + 1..bytes.len())
                .and_then(|fuzz_input| fuzz_input.optional_with(key, cache_record::decode))
            {
                let replay = cache_record::encode(key, &value);
                let _ = fuzz_support::round_trip_option(&value, cache_record::decode(&replay, key));
            }
        }
    }
});

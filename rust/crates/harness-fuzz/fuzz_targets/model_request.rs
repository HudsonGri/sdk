#![no_main]
use acyclic_harness::{
    conversation::Limits,
    model::{ModelPrefix, ModelRequest, PreparedModelRequest},
};

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(request) = serde_json::from_slice::<ModelRequest>(bytes) {
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(serde_json::from_slice::<ModelRequest>(&encoded).unwrap(), request);
        if let Ok(prepared) = PreparedModelRequest::prepare(request.clone(), Limits::default(), None) {
            let decoded = PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None).unwrap();
            assert_eq!(decoded.request(), &request);
            assert_eq!(decoded.request_digest(), prepared.request_digest());
        }
    }
    if let Ok(prefix) = serde_json::from_slice::<ModelPrefix>(bytes) {
        let encoded = prefix.canonical_bytes(None).unwrap();
        let decoded: ModelPrefix = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, prefix);
    }
});

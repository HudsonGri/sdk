#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{
    conversation::Limits,
    model::{ModelRequest, PreparedModelRequest},
};

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(request) = fuzz_input.parse(serde_json::from_slice::<ModelRequest>) else {
        return;
    };
    let encoded = serde_json::to_vec(&request).unwrap();
    let _ = fuzz_support::round_trip(&(request), serde_json::from_slice::<ModelRequest>(&encoded));
    if let Ok(prepared) = acyclic_harness::model::PreparedModelRequest::prepare(
        request.clone(),
        Limits::default(),
        None,
    ) {
        let _ = fuzz_support::round_trip_by(
            &(request.clone(), prepared.request_digest().to_owned()),
            PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None),
            |decoded| {
                Ok::<_, std::convert::Infallible>((
                    decoded.request().clone(),
                    decoded.request_digest().to_owned(),
                ))
            },
        );
    }
});

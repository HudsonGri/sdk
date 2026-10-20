#![no_main]

use acyclic_harness::tool::files::*;
use libfuzzer_sys::fuzz_target;
use serde::{Serialize, de::DeserializeOwned};

fn round_trip<T: Serialize + DeserializeOwned>(input: &[u8]) {
    if let Ok(value) = serde_json::from_slice::<T>(input) {
        let encoded = serde_json::to_vec(&value).unwrap();
        let recovered: T = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(serde_json::to_vec(&recovered).unwrap(), encoded);
    }
}

fuzz_target!(|input: &[u8]| {
    acyclic_harness::goal::fuzz_request_head(input);
    acyclic_harness::local::fuzz_owner_record(input);
    round_trip::<acyclic_fs::GitCompatState>(input);
    round_trip::<ReadFileInput>(input);
    round_trip::<WriteFileInput>(input);
    round_trip::<EditFileInput>(input);
    round_trip::<acyclic_harness::Outcome<serde_json::Value>>(input);
    round_trip::<acyclic_harness::scheduler::SchedulerEvent>(input);
    round_trip::<acyclic_harness::scheduler::OperationState>(input);
});

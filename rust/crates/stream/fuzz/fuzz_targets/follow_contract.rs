#![no_main]

use acyclic_stream::test_support::follow_contract as checks;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    checks::check(input, checks::Fault::None).expect("follow wire contract");
});

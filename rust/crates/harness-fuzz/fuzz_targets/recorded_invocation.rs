#![no_main]

use acyclic_harness::test_support::fuzz_cases::recorded_invocation_input;
libfuzzer_sys::fuzz_target!(|data: &[u8]| recorded_invocation_input::check(data));

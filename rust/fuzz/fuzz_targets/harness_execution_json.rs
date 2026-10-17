//! Fuzz journal canonical JSON decoding against its independent pre-change oracle.
#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| acyclic_harness::executor::fuzz_execution_json(bytes));

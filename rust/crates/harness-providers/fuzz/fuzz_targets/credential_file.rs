#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness_providers::Credentials::assert_file_invariants(input);
});

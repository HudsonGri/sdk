#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness::hooks::fuzz_output(bytes);
});

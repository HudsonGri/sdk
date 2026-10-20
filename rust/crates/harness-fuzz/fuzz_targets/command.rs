#![no_main]

libfuzzer_sys::fuzz_target!(|input: &str| {
    acyclic_harness::tools::fuzz_command(input);
});

#![no_main]

use libfuzzer_sys::fuzz_target;
use acyclic_harness::test_support::fuzz_cases::goal_scenario_input;

fuzz_target!(|data: &[u8]| goal_scenario_input::check(data));

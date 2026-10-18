//! Constructor admission requires the same transport feature as SwarmClient.
#[path = "../../harness-fuzz/support/swarm_limits.rs"]
mod oracle;

proptest::proptest! {
    #[test]
    fn swarm_bounds_round_trip_and_reject_unknown_metadata(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..128)) {
        oracle::exercise(&bytes, false);
    }
}

#[test]
fn negative_control_omitting_a_swarm_bound_is_rejected() {
    assert!(std::panic::catch_unwind(|| oracle::exercise(b"", true)).is_err());
}

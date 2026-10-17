#![cfg(all(acyclic_harness_loom, acyclic_loom))]

#[test]
fn complete_prepared_identity_survives_unrelated_writer_with_negative_control() {
    loom::model(|| acyclic_harness::distributed::verify_prepared_journal_identity(true));
    let failure = std::panic::catch_unwind(|| {
        loom::model(|| acyclic_harness::distributed::verify_prepared_journal_identity(false))
    })
    .expect_err("changed conditions reused a cached identity");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("prepared retry reused a changed transaction identity"),
        "{text}"
    );
}

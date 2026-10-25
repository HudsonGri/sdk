//! The Windows full-flush syscall has exactly one implementation.
fn violations<'a>(sources: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    sources.into_iter().filter_map(|(path, text)| {
        let count = text.matches("FlushFileBuffers(").count();
        let expected = usize::from(path == "rust/crates/native-runtime/src/held_sync.rs");
        (count != expected).then(|| format!("{path}: expected {expected} native full flush calls, found {count}"))
    }).collect()
}

fn sources() -> impl Iterator<Item = (&'static str, &'static str)> {
    acyclic_test_support::repository_rust_sources().iter().copied().filter(|(path, _)| {
        *path != "rust/crates/native-runtime/src/held_sync_sources_tests.rs"
    })
}

#[test]
fn every_windows_full_flush_uses_the_shared_borrowed_handle_primitive() {
    let found = violations(sources());
    assert!(found.is_empty(), "duplicate full flush implementations: {found:#?}");
    assert!(sources().count() > 3000);
}

#[test]
fn negative_control_a_second_host_flush_implementation_is_rejected() {
    for target in ["rust/crates/filesystem/src/native_host/record.rs", "rust/crates/native-runtime/src/windows/io.rs"] {
        let found = violations(sources().map(|(path, text)| (path, if path == target { "FlushFileBuffers(handle)" } else { text })));
        assert!(found.iter().any(|violation| violation.starts_with(target)), "{target}: {found:?}");
    }
}

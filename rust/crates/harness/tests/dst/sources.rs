//! Every fresh identity, wall-clock read and entropy draw in the simulated
//! crates crosses a seeded seam. A new direct source fails this scan.
//!
//! Monotonic waits are not listed: simulated paths use Tokio's paused clock, and
//! host-only waits (process trees, mounts, lock retries) sit behind the process
//! provider or outside the simulated stores.
use std::{collections::BTreeMap, path::Path};

/// Crates linked into the DST closure whose sources reach simulated paths.
const CRATES: [&str; 6] = [
    "harness",
    "filesystem",
    "stream",
    "objects",
    "native-runtime",
    "actors",
];

/// Direct nondeterministic sources. Aliased imports must still name the crate.
const SOURCES: [&str; 17] = [
    "Uuid::new_v4(",
    "Uuid::now_v7(",
    "Uuid::new_v7(",
    "SystemTime::now(",
    "getrandom::fill(",
    "getrandom::getrandom(",
    "getrandom::u32(",
    "getrandom::u64(",
    "thread_rng(",
    "rand::rng(",
    "rand::random",
    "OsRng",
    "fastrand::",
    "Timestamp::now(",
    "Utc::now(",
    "Local::now(",
    "OffsetDateTime::now",
];

/// Exact remaining direct sources: (file, source, count).
const ALLOWED: [(&str, &str, usize); 17] = [
    // Seam fallbacks, reached only when no seeded scope is active.
    ("harness/src/identity.rs", "Uuid::new_v4(", 1),
    ("harness/src/local/owner.rs", "getrandom::fill(", 1),
    ("filesystem/src/identity.rs", "Uuid::new_v4(", 1),
    ("filesystem/src/identity.rs", "Uuid::now_v7(", 1),
    ("stream/src/lib/domain.rs", "SystemTime::now(", 1),
    ("stream/src/lib/stream.rs", "Uuid::new_v4(", 1),
    ("objects/src/v1/memory/mod.rs", "getrandom::fill(", 1),
    ("objects/src/v1/memory/mod.rs", "SystemTime::now(", 1),
    // cfg(test) poll-sweep fixture; wasm32 builds compile no simulation.
    (
        "harness/src/local/admission/poll_sweep/selected_history.rs",
        "Uuid::new_v4(",
        1,
    ),
    ("stream/src/wasm/remote.rs", "Uuid::new_v4(", 1),
    // Kernel-originated mount callbacks run on driver threads outside every
    // simulation scope; simulated processes never write through a mount.
    ("filesystem/src/native_mount/mod.rs", "SystemTime::now(", 1),
    (
        "filesystem/src/native_mount/adapter/mod.rs",
        "SystemTime::now(",
        1,
    ),
    (
        "filesystem/src/native_mount/fuse/attributes.rs",
        "SystemTime::now(",
        1,
    ),
    (
        "filesystem/src/native_mount/darwin_mount/paths.rs",
        "SystemTime::now(",
        1,
    ),
    // Probe directory names, removed on drop; never durable state.
    (
        "filesystem/src/native_mount/storage_accelerations.rs",
        "SystemTime::now(",
        1,
    ),
    (
        "filesystem/src/native_mount/storage_accelerations_linux.rs",
        "SystemTime::now(",
        1,
    ),
    (
        "filesystem/src/native_mount/storage_accelerations_macos.rs",
        "SystemTime::now(",
        1,
    ),
];

// In-crate unit-test modules by repository naming convention.
fn test_only(relative: &str) -> bool {
    let mut parts = relative.rsplit('/');
    let name = parts.next().unwrap_or_default();
    name == "tests.rs"
        || name.ends_with("_tests.rs")
        || name.starts_with("tests_")
        || parts.any(|part| part == "tests" || part.ends_with("_tests"))
}

/// Compares direct source counts with the exact allowance, in both directions.
fn violations<'a>(files: impl IntoIterator<Item = (String, &'a str)>) -> Vec<String> {
    let mut found = BTreeMap::new();
    for (path, text) in files {
        for source in SOURCES {
            let count = text.matches(source).count();
            if count > 0 {
                found.insert((path.clone(), source), count);
            }
        }
    }
    let mut violations = Vec::new();
    for (path, source, count) in ALLOWED {
        match found.remove(&(path.to_owned(), source)) {
            Some(actual) if actual == count => (),
            actual => violations.push(format!(
                "{path}: {source} expected {count}, found {}",
                actual.unwrap_or(0)
            )),
        }
    }
    violations.extend(
        found
            .into_iter()
            .map(|((path, source), count)| format!("{path}: unseeded {source} x{count}")),
    );
    violations
}

// Both scans share one read of the tree.
fn sources() -> &'static [(String, String)] {
    static FILES: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    FILES.get_or_init(|| read_sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("..")))
}

fn read_sources(root: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    for name in CRATES {
        let mut pending = vec![root.join(name).join("src")];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).expect("source directory") {
                let path = entry.expect("source entry").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path.extension().is_none_or(|extension| extension != "rs") {
                    continue;
                }
                let relative = path
                    .strip_prefix(root)
                    .expect("crate source")
                    .to_str()
                    .expect("UTF-8 source path")
                    .replace('\\', "/");
                if !test_only(&relative) {
                    files.push((relative, std::fs::read_to_string(&path).expect("source")));
                }
            }
        }
    }
    files
}

#[test]
fn unseeded_sources_stay_behind_their_seams() {
    let files = sources();
    assert!(files.len() > 1000, "source scan lost its roots");
    let found = violations(
        files
            .iter()
            .map(|(path, text)| (path.clone(), text.as_str())),
    );
    assert!(found.is_empty(), "unseeded sources: {found:#?}");
}

#[test]
fn planted_unseeded_sources_fail_the_scan() {
    let files = sources();
    let scan = |planted: Option<(&str, &str)>| {
        violations(files.iter().map(|(path, text)| match planted {
            Some((target, plant)) if path == target => (path.clone(), plant),
            _ => (path.clone(), text.as_str()),
        }))
    };
    // A new production file drawing host entropy directly.
    let planted = violations(
        files
            .iter()
            .map(|(path, text)| (path.clone(), text.as_str()))
            .chain([(
                "harness/src/planted.rs".into(),
                "let id = uuid::Uuid::new_v4();",
            )]),
    );
    assert_eq!(
        planted,
        ["harness/src/planted.rs: unseeded Uuid::new_v4( x1"]
    );
    // A second direct read beside an allowed seam fallback.
    let identity = files
        .iter()
        .find(|(path, _)| path == "filesystem/src/identity.rs")
        .expect("filesystem identity seam");
    let doubled = format!(
        "{}\nstd::time::SystemTime::now();uuid::Uuid::now_v7()",
        identity.1
    );
    assert_eq!(
        scan(Some(("filesystem/src/identity.rs", &doubled))),
        [
            "filesystem/src/identity.rs: Uuid::now_v7( expected 1, found 2",
            "filesystem/src/identity.rs: unseeded SystemTime::now( x1",
        ]
    );
    // Restoring a routed wall-clock read in production admission code.
    let admission = "let now = std::time::SystemTime::now();";
    assert_eq!(
        scan(Some(("harness/src/store/admission.rs", admission))),
        ["harness/src/store/admission.rs: unseeded SystemTime::now( x1"]
    );
    // A removed seam leaves a stale allowance rather than silently passing.
    assert_eq!(
        scan(Some(("stream/src/lib/stream.rs", ""))),
        ["stream/src/lib/stream.rs: Uuid::new_v4( expected 1, found 0"]
    );
    assert!(test_only("harness/src/client/tests/history.rs"));
    assert!(!test_only("harness/src/local/owner.rs"));
}

#[test]
fn fresh_transport_identities_do_not_copy_domain_keys() {
    // This is a structural allocation proof: the transport owns the generator's
    // Bytes directly. A domain key is only constructed for the domain consumer.
    let routes = [
        (
            include_str!("../../../stream/src/grpc/client.rs"),
            "crate::new_identity_bytes,",
            "|| Bytes::copy_from_slice(crate::new_idempotency_key().as_bytes()),",
        ),
        (
            include_str!("../../../stream/src/session/client.rs"),
            "identity: crate::new_identity_bytes(),",
            "identity: Bytes::copy_from_slice(crate::new_idempotency_key().as_bytes()),",
        ),
    ];
    let owns_generated_bytes =
        |text: &str, route: &str| text.contains(route) && !text.contains("new_idempotency_key()");
    for (source, route, copying_mutant) in routes {
        assert!(owns_generated_bytes(source, route));
        // Restoring either original call must fail the same allocation gate.
        let broken = source.replace(route, copying_mutant);
        assert!(!owns_generated_bytes(&broken, route));
    }
}

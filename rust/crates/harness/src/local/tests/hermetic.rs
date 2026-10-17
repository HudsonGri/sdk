//! Tests never share machine state: no unit test can reach the per-user harness store, the user's home or
//! CODEX_HOME, and every test composition owns its state root. The seams are `owner::user_state_root`,
//! `settings::user_home` and `native_settings::configured_codex_home`; the scan keeps them the only way in.
use super::*;
use std::path::PathBuf;

#[test]
fn unit_tests_cannot_resolve_the_per_user_store() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let unrooted = LocalHarnessConfig::new(checkout.path(), (model()?, text()));
    assert!(
        matches!(super::super::owner::user_state_root(), Err(Error::Unsupported(message))
            if message == super::super::owner::UNIT_TESTS_INJECT_STATE)
    );
    assert!(
        matches!(unrooted.resolved_state_directory(), Err(Error::Unsupported(message))
            if message == super::super::owner::UNIT_TESTS_INJECT_STATE)
    );
    // Positive control: the injected directory is the one resolution, and the test helper always injects one.
    let injected = tempfile::tempdir().map_err(super::super::storage)?;
    let rooted = LocalHarnessConfig::new(checkout.path(), (model()?, text()))
        .state_directory(injected.path());
    assert_eq!(rooted.resolved_state_directory()?, injected.path());
    let helper = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?;
    let owned = helper.resolved_state_directory()?;
    assert!(!owned.starts_with(checkout.path()));
    assert_ne!(
        owned,
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .resolved_state_directory()?
    );
    Ok(())
}

#[tokio::test]
async fn opening_without_an_injected_root_fails_before_any_state_exists() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let opened =
        LocalHarness::open(LocalHarnessConfig::new(checkout.path(), (model()?, text()))).await;
    assert!(
        matches!(&opened, Err(Error::Unsupported(message)) if message.starts_with("unit tests must inject")),
        "{:?}",
        opened.as_ref().err()
    );
    Ok(())
}

#[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
#[test]
fn unit_tests_cannot_resolve_the_user_home_or_codex_home() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let project = super::super::LocalProject::Workspace(checkout.path().to_path_buf());
    assert!(
        matches!(super::super::settings::configuration(&project, None), Err(Error::Unsupported(message))
            if message == super::super::settings::UNIT_TESTS_INJECT_SETTINGS)
    );
    Ok(())
}

#[test]
fn default_state_is_one_directory_per_project_under_the_root() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let projects = tempfile::tempdir().map_err(super::super::storage)?;
    let [first, second] = ["first", "second"].map(|name| projects.path().join(name));
    for path in [&first, &second] {
        std::fs::create_dir(path).map_err(super::super::storage)?;
    }
    let at = |path: &PathBuf| {
        super::super::owner::state_directory_in(
            root.path(),
            &super::super::LocalProject::Workspace(path.clone()),
        )
    };
    let state = at(&first)?;
    assert_eq!(state.parent(), Some(root.path()));
    assert_eq!(state, at(&first.join("."))?);
    assert_ne!(state, at(&second)?);
    // Negative control: a root inside the checkout would put owner state in the agent's view.
    assert!(matches!(
        super::super::owner::state_directory_in(
            &first.join("nested"),
            &super::super::LocalProject::Workspace(first.clone())
        ),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

/// Production seams that may consult machine-wide locations, by crate-relative path.
const SEAMS: [(&str, &str); 3] = [
    ("src/local/owner.rs", "data_local_dir"),
    ("src/local/settings.rs", "home_dir"),
    ("src/native_settings.rs", concat!("CODEX", "_HOME")),
];

/// Sources allowed to build a composition without the injecting test helper: the type itself, this gate (which
/// proves such a composition fails), the helper, and the production binary.
const UNROOTED: [&str; 4] = [
    "src/local/config.rs",
    "src/local/tests/hermetic.rs",
    "src/test_support/mod.rs",
    "src/bin/",
];

/// Every reach for a machine-wide location or an unrooted composition outside its seam, as "path:line: text".
fn escapes(relative: &str, source: &str) -> Vec<String> {
    let unrooted = concat!("LocalHarnessConfig", "::new(");
    let mut found: Vec<_> = source
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            !UNROOTED.iter().any(|path| relative.starts_with(path)) && line.contains(unrooted)
        })
        .map(|(index, line)| format!("{relative}:{}: {}", index + 1, line.trim()))
        .collect();
    if !SEAMS.iter().any(|(path, _)| relative == *path) {
        let compact: String = source.chars().filter(|c| !c.is_whitespace()).collect();
        if compact.contains(concat!("dirs", "::")) {
            found.push(format!("{relative}: direct directory discovery"));
        }
        for name in [
            "HO".to_owned() + "ME",
            "USER".to_owned() + "PROFILE",
            "LOCAL".to_owned() + "APPDATA",
            "APP".to_owned() + "DATA",
            "CODEX".to_owned() + "_HOME",
        ] {
            for read in ["var", "var_os", "env!", "option_env!"] {
                let token = format!("{read}(\"{name}\")");
                if compact.match_indices(&token).any(|(offset, _)| {
                    offset == 0 || !matches!(compact.as_bytes()[offset - 1], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
                }) {
                    found.push(format!("{relative}: direct environment discovery: {token}"));
                }
            }
        }
    }
    found
}

fn rust_sources(directory: &Path, found: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(directory).map_err(super::super::storage)? {
        let path = entry.map_err(super::super::storage)?.path();
        if path.is_dir() {
            rust_sources(&path, found)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
    Ok(())
}

#[test]
fn only_the_seams_reach_machine_wide_state() -> Result<()> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    for directory in ["src", "tests"] {
        rust_sources(&crate_root.join(directory), &mut sources)?;
    }
    let mut found = Vec::new();
    for path in &sources {
        let relative = path
            .strip_prefix(crate_root)
            .map_err(super::super::storage)?
            .to_string_lossy()
            .replace('\\', "/");
        found.extend(escapes(
            &relative,
            &std::fs::read_to_string(path).map_err(super::super::storage)?,
        ));
    }
    assert!(
        sources.len() > 400,
        "scanned only {} sources",
        sources.len()
    );
    assert!(
        found.is_empty(),
        "machine-wide state outside its seam:\n{}",
        found.join("\n")
    );
    // Each seam still names the location it owns, so the scan cannot pass by a seam moving away.
    for (path, token) in SEAMS {
        let seam = std::fs::read_to_string(crate_root.join(path)).map_err(super::super::storage)?;
        assert!(seam.contains(token), "{path} no longer owns {token}");
    }
    Ok(())
}

#[test]
fn the_scan_rejects_each_escape_shape() {
    for (path, line) in [
        (
            "src/local/tests/x.rs",
            concat!("let root = dirs", "::data_local_dir();"),
        ),
        (
            "tests/e2e/x.rs",
            concat!("let home = dirs", "::home_dir();"),
        ),
        (
            "src/local/mod.rs",
            concat!("std::env::var_os(\"CODEX", "_HOME\")"),
        ),
        (
            "tests/e2e/agents.rs",
            concat!(
                "LocalHarness::open(LocalHarnessConfig",
                "::new(dir, model))"
            ),
        ),
        (
            "src/local/tests.rs",
            concat!("LocalHarnessConfig", "::new(dir, model)"),
        ),
    ] {
        assert_eq!(escapes(path, line).len(), 1, "{path}: {line}");
    }
    for (path, line) in [
        ("src/local/owner.rs", concat!("dirs", "::data_local_dir()")),
        (
            "src/test_support/mod.rs",
            concat!("LocalHarnessConfig", "::new(workspace, model)"),
        ),
        (
            "src/bin/graphcoder.rs",
            concat!("LocalHarnessConfig", "::new(&args.workspace, binding)"),
        ),
    ] {
        assert!(escapes(path, line).is_empty(), "{path}: {line}");
    }
}

#[test]
fn the_scan_rejects_direct_environment_discovery() {
    for name in [
        "HOME",
        "USERPROFILE",
        "LOCALAPPDATA",
        "APPDATA",
        "CODEX_HOME",
    ] {
        for read in ["var", "var_os", "env!", "option_env!"] {
            let source = format!("std::env::{read}( \n \"{name}\" \n )");
            assert_eq!(
                escapes("src/local/tests/escape.rs", &source).len(),
                1,
                "{source}"
            );
            assert!(escapes("src/native_settings.rs", &source).is_empty());
        }
    }
    assert!(
        escapes(
            "src/local/tests/fixture.rs",
            "std::env::var_os(\"HARNESS_TEST_CHILD\")"
        )
        .is_empty()
    );
}

#[cfg(feature = "native-execution")]
async fn retained_store_lifetime(order: Vec<usize>, retain: bool) -> Result<bool> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let mut config =
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .without_default_tools();
    let root = config.resolved_state_directory()?;
    let released = config.test_state.as_ref().unwrap().release_barrier();
    // Negative control reproduces the old harness-only owner on the real stores.
    let premature = if retain {
        None
    } else {
        config.test_state.take()
    };
    let harness = LocalHarness::open(config).await?;
    let mut stores: Vec<Option<Box<dyn std::any::Any>>> = vec![
        Some(Box::new(harness.filesystem().clone())),
        Some(Box::new(harness.task_runtime().clone())),
        Some(Box::new(harness.task_files().clone())),
        Some(Box::new(
            harness
                .workspace
                .as_ref()
                .unwrap()
                .store
                .clone()
                .with_ownership_anchor(None),
        )),
    ];
    harness.close().await?;
    drop(premature);
    let mut valid = root.is_dir();
    for index in order {
        valid &= root.is_dir();
        drop(stores[index].take());
    }
    // Existing ownership barriers observe completion of provider work without polling.
    acyclic_native_runtime::run_blocking_io(move || released.wait(), None)
        .map_err(super::super::storage)?
        .await
        .map_err(super::super::storage)?;
    Ok(valid && !root.exists())
}

#[cfg(feature = "native-execution")]
mod state_lifetime_properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(12))]
        #[test]
        fn every_retained_store_keeps_the_state_root(order in Just(vec![0usize, 1, 2, 3]).prop_shuffle()) {
            // Property: any drop order keeps the backing root until the final store releases it.
            prop_assert!(WORKERS.block_on(retained_store_lifetime(order, true)).unwrap());
        }
    }

    #[tokio::test]
    async fn harness_only_owner_fails_the_same_lifetime_oracle() -> Result<()> {
        assert!(!retained_store_lifetime(vec![0, 1, 2, 3], false).await?);
        assert!(retained_store_lifetime(vec![0, 1, 2, 3], true).await?);
        Ok(())
    }
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn binding_supplied_stores_without_a_checkout_never_resolves_user_state() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let config = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .without_default_tools();
    let root = config.resolved_state_directory()?;
    let harness = LocalHarness::open(config).await?;
    let storage = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(root.join("filesystem")))
        .await
        .map_err(super::super::storage)?;
    let stream = harness.task_stream();
    let volume = harness.task_files().volume().clone();
    let verifier = harness.verifier.clone();
    let signed = harness.signed.clone();
    let LocalHarness { runtime, files, .. } = harness;
    let runtime = Arc::try_unwrap(runtime)
        .map_err(|_| Error::Conflict("fixture runtime is still shared".into()))?;
    let mut config =
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .volume(volume)
            .without_default_tools();
    config.state_directory = None;
    config.test_state = None;
    assert!(config.resolved_state_directory().is_err());
    let bound = LocalHarness::bind(
        config,
        runtime,
        files,
        (storage, stream),
        (&verifier, &signed),
        None,
    )
    .await?;
    bound.close().await
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn an_existing_volume_keeps_its_callers_store_ownership() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let volume = super::volume(directory.path()).await?;
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(super::super::storage)?;
    let config = crate::test_support::local_harness_config(
        directory.path().join("filesystem"),
        (model()?, text()),
    )?
    .volume(volume)
    .without_default_tools();
    let state = config.resolved_state_directory()?;
    let released = config.test_state.as_ref().unwrap().release_barrier();
    let harness = LocalHarness::open(config).await?;
    harness.close().await?;
    acyclic_native_runtime::run_blocking_io(move || released.wait(), None)
        .map_err(super::super::storage)?
        .await
        .map_err(super::super::storage)?;
    // The temporary companion state closes independently of the caller's live provider.
    assert!(!state.exists());
    assert!(
        !filesystem
            .local_root_release_barrier()
            .unwrap()
            .is_released()
    );
    Ok(())
}

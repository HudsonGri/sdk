//! Test compositions own their roots; Clippy resolves and rejects ambient discovery outside the native seam.
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

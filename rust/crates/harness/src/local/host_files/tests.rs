use super::*;

#[tokio::test(flavor = "current_thread")]
async fn workspace_projection_uses_shared_results_without_client_state() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner")),
        )
        .await?,
    );
    let owner = tokio::spawn(Box::pin(serve(Arc::clone(&local))));
    let descriptor = checkout.path().join(DIRECTORY).join("owner.json");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !descriptor.is_file() {
            assert!(
                !owner.is_finished(),
                "workspace owner stopped before readiness"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(storage)?;
    assert!(
        serve(Arc::clone(&local)).await.is_err(),
        "a second workspace owner must not dispatch the same requests"
    );
    let requests = checkout.path().join(DIRECTORY).join("requests");
    std::fs::write(requests.join("malformed.json"), b"{not a request").map_err(storage)?;
    std::fs::write(
        requests.join("oversized.json"),
        vec![b'x'; usize::try_from(MAXIMUM_BYTES).map_err(storage)? + 1],
    )
    .map_err(storage)?;
    let foreign = OperationId::new();
    std::fs::write(
        requests.join(format!("{foreign}.json")),
        serde_json::to_vec(&Request {
            session: uuid::Uuid::new_v4(),
            operation: foreign,
            action: HostAction::Open {
                host: "codex".into(),
                session: "must-not-dispatch".into(),
            },
        })
        .map_err(storage)?,
    )
    .map_err(storage)?;
    #[cfg(unix)]
    let outside = {
        let outside = tempfile::NamedTempFile::new().map_err(storage)?;
        std::fs::write(outside.path(), b"outside remains untouched").map_err(storage)?;
        std::os::unix::fs::symlink(outside.path(), requests.join("symlink.json"))
            .map_err(storage)?;
        outside
    };
    let cwd = checkout.path().to_path_buf();
    let result = tokio::task::spawn_blocking(move || {
        let result = request(
            &cwd,
            HostAction::Open {
                host: "codex".into(),
                session: "fixture-native-session".into(),
            },
        )?;
        let child = request(
            &cwd,
            HostAction::Fork {
                parent: serde_json::from_value(result["task"].clone()).map_err(storage)?,
                reference: "endpoint-boundary".into(),
            },
        )?;
        let child = PathBuf::from(child["cwd"].as_str().unwrap());
        assert_eq!(workspace_root(&child)?, cwd);
        assert!(!child.join(DIRECTORY).join("owner.json").exists());
        let task = serde_json::from_value(result["task"].clone()).map_err(storage)?;
        // The reference fits a request, but the byte-array query encoding does
        // not fit a response. The shared command consumer settles it without
        // stranding the claim or terminating the workspace owner.
        request(&cwd, HostAction::Hook {
            host: "pi".into(), event: "NativeSession".into(),
            input: serde_json::json!({"task":task,"native_session":{"id":"fixture","file":cwd.join("x".repeat(20_000))}}),
        })?;
        assert!(request(&cwd, HostAction::Query {
            task, query: Query::File(FileKey::Task {
                task: Component::new(task.to_string()).map_err(storage)?,
                file: TaskFile::Context(Component::new("native-session").map_err(storage)?),
            }),
        }).is_err(), "oversized live queries must settle as omitted output");
        assert_eq!(request(&cwd, HostAction::Open {
            host: "codex".into(), session: "fixture-native-session".into(),
        })?["task"], result["task"]);
        Ok::<_, Error>(result)
    })
    .await
    .map_err(storage)??;
    assert_eq!(
        result.get("cwd").unwrap_or(&Value::Null),
        checkout.path().to_string_lossy().as_ref()
    );
    assert!(result.get("task").is_some_and(Value::is_string));
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::fs::read_dir(&requests)
            .map_err(storage)?
            .next()
            .is_some()
        {
            assert!(!owner.is_finished(), "invalid request stopped the owner");
            tokio::task::yield_now().await;
        }
        Ok::<_, Error>(())
    })
    .await
    .map_err(storage)??;
    assert!(
        !owner.is_finished(),
        "invalid writable projections must not stop the owner"
    );
    #[cfg(unix)]
    assert_eq!(
        std::fs::read(outside.path()).map_err(storage)?,
        b"outside remains untouched"
    );
    assert_eq!(
        std::fs::read_dir(checkout.path().join(DIRECTORY).join("requests"))
            .map_err(storage)?
            .count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(checkout.path().join(DIRECTORY).join("results"))
            .map_err(storage)?
            .count(),
        0
    );
    assert!(local.agent.is_none());
    assert!(local.tools().definitions(None)?.is_empty());
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    Arc::try_unwrap(local)
        .map_err(|_| Error::Conflict("workspace owner retained after shutdown".into()))?
        .disconnect()
        .await?;
    Ok(())
}

#[test]
fn absent_owner_never_creates_a_service_or_state_directory() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    assert!(matches!(
        request(
            checkout.path(),
            HostAction::Open {
                host: "codex".into(),
                session: "fixture".into()
            }
        ),
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        std::fs::read_dir(checkout.path()).map_err(storage)?.count(),
        0
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn endpoint_projection_cannot_follow_a_workspace_symlink() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let outside = tempfile::tempdir().map_err(storage)?;
    std::os::unix::fs::symlink(outside.path(), checkout.path().join(".acyclic-sdk"))
        .map_err(storage)?;
    assert!(discover(checkout.path()).is_err());
    assert_eq!(
        std::fs::read_dir(outside.path()).map_err(storage)?.count(),
        0
    );
    Ok(())
}

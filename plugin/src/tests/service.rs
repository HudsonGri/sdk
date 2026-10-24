use super::*;

#[tokio::test(start_paused = true)]
async fn service_transition_handoff_waits_for_publication_and_release_without_a_timer() {
    let fixture = Fixture::new();
    let startup = service_startup_lock(fixture.data.clone()).await.unwrap();
    let owner = acquire_service_lock(&fixture.data).unwrap().unwrap();
    let marker = ServiceMarker {
        instance_id: "transition-owner".to_owned(),
        binary_identity: "test-binary".to_owned(),
    };
    let stops = StopRequests::open(&fixture.data, &marker.instance_id).unwrap();
    let before = tokio::time::Instant::now();
    let mut draining = std::pin::pin!(drain_service(&fixture.data, Some(&marker.instance_id)));
    // Poll the actual drain before publication. It must wait on the gate;
    // the former implementation instead armed its 20ms sampling timer.
    assert!(futures::poll!(draining.as_mut()).is_pending());
    let published = PublishedServiceMarker::create(&fixture.data, &marker).unwrap();
    drop(startup);
    let retire = async {
        let request = stops.next().await;
        write_service_drain_completion(
            &fixture.data,
            &marker.instance_id,
            &request,
            &Ok(SessionCloseState::Closed),
        )
        .unwrap();
        drop(published);
        drop(owner);
    };
    let (handoff, ()) = tokio::join!(draining, retire);
    let handoff = handoff.unwrap();
    assert_eq!(
        tokio::time::Instant::now() - before,
        std::time::Duration::ZERO
    );
    assert!(acquire_service_lock(&fixture.data).unwrap().is_none());
    drop(handoff);
    // A second requester is authenticated by the original durable drain,
    // even though the stopped service has withdrawn its marker.
    drop(
        drain_service(&fixture.data, Some(&marker.instance_id))
            .await
            .unwrap(),
    );
    assert!(
        drain_service(&fixture.data, Some("another-owner"))
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn service_transition_missing_publication_is_an_error_not_a_polling_window() {
    let fixture = Fixture::new();
    let _owner = acquire_service_lock(&fixture.data).unwrap().unwrap();
    let before = tokio::time::Instant::now();
    let error = drain_service(&fixture.data, None).await.err().unwrap();
    assert!(error.contains("without a published identity"));
    assert_eq!(
        tokio::time::Instant::now() - before,
        std::time::Duration::ZERO
    );
}

#[tokio::test]
async fn service_transition_does_not_accept_a_failed_or_foreign_drain_receipt() {
    let fixture = Fixture::new();
    assert!(claim_stopped_service(&fixture.data, Some("owner")).is_err());
    write_service_drain_completion(
        &fixture.data,
        "owner",
        "drain",
        &Ok(SessionCloseState::Failed),
    )
    .unwrap();
    assert!(claim_stopped_service(&fixture.data, Some("owner")).is_err());
    write_service_drain_completion(
        &fixture.data,
        "owner",
        "drain",
        &Ok(SessionCloseState::Closed),
    )
    .unwrap();
    fs::copy(
        service_drain_completion_path(&fixture.data, "owner"),
        service_drain_completion_path(&fixture.data, "foreign"),
    )
    .unwrap();
    assert!(claim_stopped_service(&fixture.data, Some("foreign")).is_err());
    assert!(
        claim_stopped_service(&fixture.data, Some("owner"))
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn service_transition_gate_holds_the_canonical_os_lock_until_release() {
    let fixture = Fixture::new();
    let gate = service_startup_lock(fixture.data.clone()).await.unwrap();
    let open = || {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(
                fixture
                    .data
                    .parent()
                    .unwrap()
                    .join(".acyclic-service-startup.lock"),
            )
            .unwrap()
    };
    let contender = acyclic_native_runtime::FileLock::try_acquire(
        open(),
        acyclic_native_runtime::FileLockMode::Exclusive,
    );
    assert!(contender.is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock));
    drop(gate);
    drop(
        acyclic_native_runtime::FileLock::try_acquire(
            open(),
            acyclic_native_runtime::FileLockMode::Exclusive,
        )
        .unwrap(),
    );
}

#[cfg(any(unix, windows))]
#[test]
fn binary_handoff_drains_live_sessions_before_reopen() {
    run_large_stack("service-handoff", || async {
        let fixture = Fixture::new();
        let data = fixture.data.clone();
        let service = tokio::spawn(async move {
            run_service_with_identity(data, Some("older-service-binary".into()), None).await
        });
        for _ in 0..250 {
            if fixture.data.join("service.identity").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        send_control_request(
            &fixture.data,
            &native_hook_request("codex", "SessionStart", "live", &fixture.root, json!({})),
        )
        .await
        .unwrap();
        assert!(
            !service_is_ready_for_identity(&fixture.data, "replacement-service-binary")
                .await
                .unwrap()
        );
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), service)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let (_lock, reopened) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        assert!(reopened.catalog.lock().await.active.is_empty());
        let _ = reopened.shutdown().await.unwrap();
    });
}

#[cfg(any(unix, windows))]
#[test]
fn durable_drain_is_identity_bound_and_answers_concurrent_requesters() {
    run_large_stack("durable-drain", || async {
        let fixture = Fixture::new();
        let data = fixture.data.clone();
        let service = tokio::spawn(async move { run_service(data).await });
        for _ in 0..250 {
            if fixture.data.join("service.identity").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let identity = ServiceMarker::read(&fixture.data).unwrap().instance_id;
        assert!(
            drain_service(&fixture.data, Some("replacement-service"))
                .await
                .is_err()
        );
        assert!(acquire_service_lock(&fixture.data).unwrap().is_none());
        // Each drain returns an exclusive handoff fence. Release each fence
        // in its own requester, so one completed request cannot prevent the
        // other from claiming the same stopped service.
        let drain = || async {
            drop(drain_service(&fixture.data, Some(&identity)).await?);
            Ok::<(), String>(())
        };
        let (first, second) = tokio::join!(drain(), drain());
        first.unwrap();
        second.unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), service)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(ServiceMarker::read(&fixture.data).is_none());
        assert!(service_drain_completion_path(&fixture.data, &identity).exists());
        assert!(acquire_service_lock(&fixture.data).unwrap().is_some());
    });
}

#[cfg(any(unix, windows))]
#[test]
fn failed_startup_releases_the_service_and_root() {
    run_large_stack("failed-service-startup", || async {
        let fixture = Fixture::new();
        fs::create_dir(fixture.data.join("service.identity")).unwrap();
        assert!(
            run_service_with_identity(fixture.data.clone(), Some("service".into()), None)
                .await
                .is_err()
        );
        let fence = acquire_service_lock(&fixture.data).unwrap().unwrap();
        drop(fence);
        let (_lock, reopened) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        let _ = reopened.shutdown().await.unwrap();
    });
}

#[test]
fn sessions_are_independent_and_old_handles_are_fenced() {
    run_large_stack("session-isolation", || async {
        let fixture = Fixture::new();
        let other = fixture.directory("other");
        let (_lock, service) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        for (session, root) in [("a", &fixture.root), ("b", &other)] {
            service
                .dispatch_request(
                    native_hook_request("claude-code", "SessionStart", session, root, json!({})),
                    &mut None,
                )
                .await
                .unwrap();
        }
        let old = service.session("a").await.unwrap();
        let resume = old.pause().await;
        let request = ControlRequest {
            version: 1,
            command: ControlCommand::Agents,
            cwd: other,
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            service.dispatch_request(request, &mut None),
        )
        .await
        .unwrap()
        .unwrap();
        resume.send(()).unwrap();
        assert_eq!(
            service.end_session("a", true, &mut None).await.unwrap(),
            SessionCloseState::Closed
        );
        service
            .dispatch_request(
                native_hook_request("claude-code", "SessionStart", "a", &fixture.root, json!({})),
                &mut None,
            )
            .await
            .unwrap();
        assert!(!old.is_active());
        assert!(service.session("a").await.unwrap().key.epoch > old.key.epoch);
        assert_eq!(
            service.drain_sessions(true).await.unwrap(),
            SessionCloseState::Closed
        );
    });
}

#[test]
fn accepted_operation_survives_waiter_cancellation() {
    run_large_stack("waiter-cancellation", || async {
        let fixture = Fixture::new();
        let (_lock, service) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        let service = Arc::new(service);
        service
            .dispatch_request(
                native_hook_request("codex", "SessionStart", "session", &fixture.root, json!({})),
                &mut None,
            )
            .await
            .unwrap();
        let handle = service.session("session").await.unwrap();
        let resume = handle.pause().await;
        let request_service = Arc::clone(&service);
        let root = fixture.root.clone();
        let waiter = tokio::spawn(async move {
            request_service
                .dispatch_request(
                    native_hook_request(
                        "codex",
                        "UserPromptSubmit",
                        "session",
                        &root,
                        json!({"turn_id":"durable-turn"}),
                    ),
                    &mut None,
                )
                .await
        });
        tokio::task::yield_now().await;
        waiter.abort();
        resume.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !handle.with_snapshot(|snapshot| matches!(snapshot,
                Some(crate::service::SessionSnapshot::HostManaged(state)) if state.root_turns.contains("durable-turn"))).unwrap() {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert_eq!(
            service.drain_sessions(true).await.unwrap(),
            SessionCloseState::Closed
        );
    });
}

#[test]
fn close_is_fifo_idempotent_and_durable() {
    run_large_stack("session-close", || async {
        let fixture = Fixture::new();
        let (_lock, service) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        let service = Arc::new(service);
        service
            .dispatch_request(
                native_hook_request("codex", "SessionStart", "session", &fixture.root, json!({})),
                &mut None,
            )
            .await
            .unwrap();
        let handle = service.session("session").await.unwrap();
        let resume = handle.pause().await;
        let operation_service = Arc::clone(&service);
        let root = fixture.root.clone();
        let operation = tokio::spawn(async move {
            operation_service
                .dispatch_request(
                    native_hook_request(
                        "codex",
                        "UserPromptSubmit",
                        "session",
                        &root,
                        json!({"turn_id":"before-close"}),
                    ),
                    &mut None,
                )
                .await
        });
        tokio::task::yield_now().await;
        let close_service = Arc::clone(&service);
        let close =
            tokio::spawn(
                async move { close_service.end_session("session", true, &mut None).await },
            );
        tokio::task::yield_now().await;
        let duplicate_service = Arc::clone(&service);
        let duplicate = tokio::spawn(async move {
            duplicate_service
                .end_session("session", true, &mut None)
                .await
        });
        resume.send(()).unwrap();
        operation.await.unwrap().unwrap();
        assert_eq!(close.await.unwrap().unwrap(), SessionCloseState::Closed);
        assert_eq!(duplicate.await.unwrap().unwrap(), SessionCloseState::Closed);
        assert_eq!(
            service
                .end_session("session", true, &mut None)
                .await
                .unwrap(),
            SessionCloseState::Closed
        );
        let state = load_saved_state(
            &fixture
                .data
                .join("sessions")
                .join(blake3::hash(b"session").to_hex().as_str()),
        )
        .unwrap();
        assert!(!state.active);
        assert!(state.root_turns.contains("before-close"));
    });
}

#[test]
fn saturated_data_queue_still_accepts_close() {
    run_large_stack("saturated-close", || async {
        let fixture = Fixture::new();
        let (_lock, service) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        let service = Arc::new(service);
        service
            .dispatch_request(
                native_hook_request("codex", "SessionStart", "session", &fixture.root, json!({})),
                &mut None,
            )
            .await
            .unwrap();
        let handle = service.session("session").await.unwrap();
        let resume = handle.pause().await;
        let request = ControlRequest {
            version: 1,
            command: ControlCommand::Ping,
            cwd: fixture.root.clone(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        };
        let mut operations = Vec::new();
        for _ in 0..SESSION_DATA_CAPACITY {
            let handle = Arc::clone(&handle);
            let request = request.clone();
            operations.push(tokio::spawn(async move {
                handle.dispatch(request, &mut None).await
            }));
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while handle.data_slots.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(handle.dispatch(request, &mut None).await.is_err());
        let close =
            tokio::spawn(async move { service.end_session("session", true, &mut None).await });
        tokio::task::yield_now().await;
        resume.send(()).unwrap();
        for operation in operations {
            operation.await.unwrap().unwrap();
        }
        assert_eq!(close.await.unwrap().unwrap(), SessionCloseState::Closed);
    });
}

#[test]
fn drain_waits_for_busy_sessions_and_saves_terminal_state() {
    run_large_stack("service-drain", || async {
        let fixture = Fixture::new();
        let (_lock, service) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        service
            .dispatch_request(
                native_hook_request("codex", "SessionStart", "session", &fixture.root, json!({})),
                &mut None,
            )
            .await
            .unwrap();
        let handle = service.session("session").await.unwrap();
        let resume = handle.pause().await;
        service.begin_drain();
        let drain = tokio::spawn(async move { service.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        resume.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), drain)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            SessionCloseState::Closed
        );
        let state = load_saved_state(
            &fixture
                .data
                .join("sessions")
                .join(blake3::hash(b"session").to_hex().as_str()),
        )
        .unwrap();
        assert!(!state.active);
    });
}

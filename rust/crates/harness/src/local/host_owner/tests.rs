use super::super::host_workspace::tests::native_write;
use super::*;
use crate::task_volume::{ChildFile, ChildState, ChildStatus, Component, FileKey, FileValue};

struct Fixture {
    checkout: tempfile::TempDir,
    _state: tempfile::TempDir,
    owner: HostWorkspaceOwner,
    parent: TaskId,
}

impl Fixture {
    async fn open() -> Result<Self> {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(checkout.path().join("source.txt"), b"source").map_err(storage)?;
        let local = Arc::new(
            LocalHarness::open(
                LocalHarnessConfig::native_host(checkout.path())
                    .state_directory(state.path().join("owner")),
            )
            .await?,
        );
        let mut owner = HostWorkspaceOwner::new(local)?;
        let root = owner
            .dispatch(
                OperationId::new(),
                HostAction::Open {
                    host: "pi".into(),
                    session: "native-root".into(),
                },
            )
            .await?;
        let parent = serde_json::from_value(root["task"].clone()).map_err(storage)?;
        Ok(Self {
            checkout,
            _state: state,
            owner,
            parent,
        })
    }
    async fn session(&mut self, reference: &str, action: SessionAction) -> Result<Value> {
        self.owner
            .dispatch(
                OperationId::new(),
                HostAction::Session {
                    parent: self.parent,
                    reference: reference.into(),
                    action,
                },
            )
            .await
    }
}

// Negative control: this same lifecycle on the previous per-tool ledger leaves
// a pending write with no PostToolUse and refuses SessionEnd/capture/merge.
#[tokio::test]
async fn failed_and_cancelled_native_tools_leave_capturable_mergeable_views() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    for reference in ["failed-write", "cancelled-write"] {
        let location = fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Fork {
                    parent: fixture.parent,
                    reference: reference.into(),
                },
            )
            .await?;
        let child = serde_json::from_value(location["task"].clone()).map_err(storage)?;
        let cwd = PathBuf::from(
            location["cwd"]
                .as_str()
                .ok_or_else(|| Error::Invalid("view absent".into()))?,
        );
        assert!(
            cwd.starts_with(
                std::fs::canonicalize(fixture.checkout.path())
                    .map_err(storage)?
                    .join(host_workspace::HOST_VIEW_ROOT)
            )
        );
        let parent = fixture.owner.attached(fixture.parent)?;
        let attached = fixture.owner.attached(child)?;
        parent
            .workspace
            .name_child(
                &parent.context,
                &attached.context,
                &attached.workspace,
                reference,
            )
            .await?;
        fixture.owner.dispatch(OperationId::new(),HostAction::Hook {host:"pi".into(),event:"PreToolUse".into(),input:json!({"task":child,"session_id":"native-root","cwd":cwd,"tool_name":"bash","tool_use_id":reference,"tool_input":{"command":"write then fail or cancel"}})}).await?;
        native_write(
            &cwd.join(format!("{reference}.txt")),
            b"partial native write",
        )?;
        // The native host has stopped the failed/cancelled tool. No post hook.
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Hook {
                    host: "pi".into(),
                    event: "SessionEnd".into(),
                    input: json!({"task":child,"session_id":"native-root","cwd":cwd}),
                },
            )
            .await?;
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Fs {
                    task: fixture.parent,
                    command: format!("merge agents/{reference}"),
                },
            )
            .await?;
        assert_eq!(
            std::fs::read(fixture.checkout.path().join(format!("{reference}.txt")))
                .map_err(storage)?,
            b"partial native write"
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_sessions_use_shared_ordered_mailboxes_and_resume_after_recreation() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    std::fs::write(
        fixture.checkout.path().join("instructions.md"),
        b"discovered in child view",
    )
    .map_err(storage)?;
    fixture
        .session(
            "child",
            SessionAction::Spawn {
                prompt: "initial".into(),
            },
        )
        .await?;
    fixture
        .session(
            "child",
            SessionAction::Send {
                prompt: "follow-up".into(),
            },
        )
        .await?;
    let first = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(first["prompt"], "initial");
    assert_eq!(first["turn"], 1);
    assert!(fixture.session("child", SessionAction::Next).await.is_err());
    assert!(
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Fs {
                    task: fixture.parent,
                    command: "merge agents/child".into()
                }
            )
            .await
            .is_err()
    );
    let child: TaskId = serde_json::from_value(first["task"].clone()).map_err(storage)?;
    let cwd = PathBuf::from(
        first["cwd"]
            .as_str()
            .ok_or_else(|| Error::Invalid("view absent".into()))?,
    );
    assert_eq!(
        std::fs::read(cwd.join("instructions.md")).map_err(storage)?,
        b"discovered in child view"
    );
    fixture
        .session(
            "child",
            SessionAction::SaveSession {
                id: "opaque-native-resume-id".into(),
            },
        )
        .await?;
    native_write(&cwd.join("child.txt"), b"native")?;
    fixture
        .session(
            "child",
            SessionAction::Complete {
                turn: 1,
                outcome: crate::Outcome::Failed {
                    message: "native failed after write".into(),
                },
            },
        )
        .await?;
    let local = Arc::clone(&fixture.owner.local);
    drop(fixture.owner);
    fixture.owner = HostWorkspaceOwner::new(local)?;
    fixture
        .owner
        .dispatch(OperationId::new(), HostAction::Reopen { task: child })
        .await?;
    let next = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(next["turn"], 2);
    assert_eq!(next["prompt"], "follow-up");
    assert_eq!(
        next.pointer("/session/id"),
        Some(&json!("opaque-native-resume-id"))
    );
    let recreated = PathBuf::from(
        next["cwd"]
            .as_str()
            .ok_or_else(|| Error::Invalid("view absent".into()))?,
    );
    assert_eq!(
        std::fs::read(recreated.join("child.txt")).map_err(storage)?,
        b"native"
    );
    fixture
        .session(
            "child",
            SessionAction::Control {
                control: crate::task_volume::Control::Interrupt,
            },
        )
        .await?;
    let settled = fixture
        .session(
            "child",
            SessionAction::Complete {
                turn: 2,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    assert_eq!(settled.pointer("/status/settled"), Some(&json!(true)));
    assert_eq!(
        settled.pointer("/status/state"),
        Some(&json!("Interrupted"))
    );
    fixture
        .session(
            "child",
            SessionAction::Control {
                control: crate::task_volume::Control::Resume,
            },
        )
        .await?;
    assert!(
        fixture
            .session("child", SessionAction::Next)
            .await?
            .is_null()
    );
    fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Fs {
                task: fixture.parent,
                command: "merge agents/child".into(),
            },
        )
        .await?;
    assert_eq!(
        std::fs::read(fixture.checkout.path().join("child.txt")).map_err(storage)?,
        b"native"
    );
    Ok(())
}

#[tokio::test]
async fn native_claim_recovery_does_not_replay_unacknowledged_input_or_replace_results()
-> Result<()> {
    let mut fixture = Fixture::open().await?;
    let child = fixture
        .session(
            "recover",
            SessionAction::Spawn {
                prompt: "claimed".into(),
            },
        )
        .await?;
    let task = serde_json::from_value(child["task"].clone()).map_err(storage)?;
    let reference = Component::new("recover").map_err(storage)?;
    // Simulate an owner crash after Status and before Delivered publication.
    fixture
        .owner
        .publish(
            fixture.parent,
            FileValue::new(
                FileKey::Child {
                    reference,
                    file: ChildFile::Status,
                },
                serde_json::to_vec(&ChildStatus {
                    task,
                    state: ChildState::Running,
                    settled: false,
                    inputs: 1,
                })
                .map_err(storage)?,
            ),
        )
        .await?;
    let recovered = fixture.session("recover", SessionAction::Recover).await?;
    assert_eq!(recovered.pointer("/status/settled"), Some(&json!(true)));
    assert!(
        fixture
            .session("recover", SessionAction::Next)
            .await?
            .is_null()
    );
    fixture
        .session(
            "recover",
            SessionAction::Send {
                prompt: "second".into(),
            },
        )
        .await?;
    let next = fixture.session("recover", SessionAction::Next).await?;
    assert_eq!(next["turn"], 2);
    assert_eq!(next["prompt"], "second");
    fixture
        .session(
            "recover",
            SessionAction::Complete {
                turn: 2,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    assert!(
        fixture
            .session("recover", SessionAction::Next)
            .await?
            .is_null()
    );
    Ok(())
}

#[tokio::test]
async fn native_live_observation_keeps_latest_result_and_retains_shared_history() -> Result<()> {
    const TURNS: u64 = 3;
    let mut fixture = Fixture::open().await?;
    fixture
        .session(
            "history",
            SessionAction::Spawn {
                prompt: "initial".into(),
            },
        )
        .await?;
    // Complete real ordered inputs. Retained history exceeds the endpoint
    // bound, but the live result must remain independent of that history.
    for turn in 1..=TURNS {
        if turn > 1 {
            fixture
                .session(
                    "history",
                    SessionAction::Send {
                        prompt: "follow-up".into(),
                    },
                )
                .await?;
        }
        let claimed = fixture.session("history", SessionAction::Next).await?;
        assert_eq!(claimed["turn"], turn);
        fixture
            .session(
                "history",
                SessionAction::Complete {
                    turn,
                    outcome: crate::Outcome::Failed {
                        message: "x".repeat(24 * 1024),
                    },
                },
            )
            .await?;
    }
    let observed = fixture.session("history", SessionAction::Observe).await?;
    assert_eq!(
        observed["results"].as_array().map(Vec::len),
        Some(1),
        "live observations must not accumulate historical answers"
    );
    assert_eq!(observed.pointer("/results/0/turn"), Some(&json!(TURNS)));
    assert!(serde_json::to_vec(&observed).map_err(storage)?.len() < 65_536);
    let reference = Component::new("history").map_err(storage)?;
    let files = fixture.owner.files(fixture.parent).await?;
    let mut retained_bytes = 0;
    for turn in 1..=TURNS {
        let retained = files
            .snapshot(
                &Query::File(FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Result(turn),
                }),
                1,
            )
            .await
            .map_err(storage)?;
        assert_eq!(retained.files.len(), 1);
        retained_bytes += retained
            .files
            .iter()
            .map(|file| file.bytes.len())
            .sum::<usize>();
    }
    assert!(retained_bytes > 65_536);
    Ok(())
}

#[tokio::test]
async fn native_builtin_subagents_and_tool_events_do_not_create_harness_forks() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    for event in [
        "PreToolUse",
        "PostToolUse",
        "PostToolUseFailure",
        "SubagentStart",
        "SubagentStop",
        "WorktreeCreate",
        "WorktreeRemove",
    ] {
        assert_eq!(
            fixture
                .owner
                .dispatch(
                    OperationId::new(),
                    HostAction::Hook {
                        host: "codex".into(),
                        event: event.into(),
                        input: json!({"agent_id":"native-only"})
                    }
                )
                .await?,
            json!({})
        );
    }
    assert_eq!(fixture.owner.views.len(), 1);
    assert_eq!(
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Sessions {
                    parent: fixture.parent
                }
            )
            .await?,
        json!([])
    );
    Ok(())
}

#[tokio::test]
async fn native_cli_cancel_signals_the_process_owner_and_archive_settles_unstarted_inputs()
-> Result<()> {
    let mut fixture = Fixture::open().await?;
    fixture
        .session(
            "active",
            SessionAction::Spawn {
                prompt: "work".into(),
            },
        )
        .await?;
    let turn = fixture.session("active", SessionAction::Next).await?;
    let task = serde_json::from_value(turn["task"].clone()).map_err(storage)?;
    let intent = fixture
        .owner
        .dispatch(OperationId::new(), HostAction::Cancel { task })
        .await?;
    assert_eq!(intent["control"], "interrupt");
    assert_eq!(intent.pointer("/status/settled"), Some(&json!(false)));
    fixture
        .session(
            "active",
            SessionAction::Complete {
                turn: 1,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    fixture
        .session(
            "active",
            SessionAction::SaveSession {
                id: "retained-session".into(),
            },
        )
        .await?;
    fixture
        .session(
            "unstarted",
            SessionAction::Spawn {
                prompt: "never launched".into(),
            },
        )
        .await?;
    let archived = fixture
        .session(
            "unstarted",
            SessionAction::Control {
                control: crate::task_volume::Control::Archive,
            },
        )
        .await?;
    assert_eq!(archived.pointer("/status/state"), Some(&json!("Archived")));
    assert_eq!(archived.pointer("/status/settled"), Some(&json!(true)));
    assert!(
        fixture
            .session(
                "unstarted",
                SessionAction::Control {
                    control: crate::task_volume::Control::Resume
                }
            )
            .await
            .is_err()
    );
    Ok(())
}

use super::*;
use crate::{
    client::{CommandResolution, CommandTool},
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
use acyclic_test_support::differential;

async fn fixture() -> (tempfile::TempDir, LocalHarness, FileValue) {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("delegated answer".into())])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
            }),
    );
    let model = Model::new(
        "scripted".into(),
        "admission-work".into(),
        "1".into(),
        serde_json::Value::Null,
    )
    .unwrap();
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider)).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        harness
            .dispatch_command(
                OperationId::new(),
                None,
                CommandResolution::Tool {
                    tool: CommandTool::Spawn,
                    input: serde_json::json!({"task":"delegate this task","name":"delegate"})
                }
            )
            .await
            .unwrap(),
        serde_json::json!({"agent":"delegate"})
    );
    let request = harness
        .task_files
        .inner
        .child_request(&crate::task_volume::Component::new("delegate").unwrap())
        .await
        .unwrap();
    (directory, harness, request)
}

fn workspace_work(trace: &[(&str, u64)]) -> [u64; 2] {
    ["view-items", "view-allocations"].map(|kind| {
        trace
            .iter()
            .filter(|(operation, _)| *operation == kind)
            .map(|(_, amount)| amount)
            .sum()
    })
}

fn no_view_work(trace: &[(&str, u64)], assert_property: bool) {
    if assert_property {
        assert_eq!(
            workspace_work(trace),
            [0, 0],
            "controller reopened an unused child workspace: {trace:?}"
        );
    }
}

#[test]
fn replay_admission_preserves_results_and_errors_with_zero_workspace_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pair = differential::pair(
        || runtime.block_on(fixture()),
        |(_directory, harness, request), old| {
            runtime.block_on(async {
                let child = if old {
                    harness
                        .admit_child_request(&harness.task_files, &request)
                        .await
                        .unwrap()
                        .0
                } else {
                    harness
                        .admit_child_task(&harness.task_files, &request)
                        .await
                        .unwrap()
                };
                let record = harness
                    .runtime
                    .task_host()
                    .observe_admission(child)
                    .await
                    .unwrap();
                assert_eq!(record.parent, request.requester());
                let reference = crate::task_volume::Component::new("delegate").unwrap();
                let result = harness
                    .task_files
                    .inner
                    .snapshot(
                        &Query::File(FileKey::Child {
                            reference,
                            file: ChildFile::Result(1),
                        }),
                        1,
                    )
                    .await
                    .unwrap();
                let answer =
                    crate::task_volume::ChildTurnResult::parse(&result.files[0].bytes).unwrap();
                assert_eq!(answer.task, child);
                let mut forged = request;
                forged.bytes = acyclic_stream::OwnedBytes::from_static(b"forged");
                let error = match harness.admit_child_task(&harness.task_files, &forged).await {
                    Ok(_) => panic!("forged request admitted"),
                    Err(error) => error.to_string(),
                };
                let admission_bytes =
                    crate::contract::canonical_json_bytes(&record.canonical_value())
                        .unwrap()
                        .len() as u64;
                let output = (
                    serde_json::to_vec(&answer.outcome).unwrap(),
                    error,
                    admission_bytes,
                );
                harness.disconnect().await.unwrap();
                output
            })
        },
    );
    assert_eq!(
        (&pair[0].0.0, &pair[0].0.1),
        (&pair[1].0.0, &pair[1].0.1),
        "admission changed output bytes or its error"
    );
    for ((_, _, admission_bytes), trace) in &pair {
        let observations: Vec<_> = trace
            .iter()
            .copied()
            .filter(|(kind, _)| *kind != "view-items" && *kind != "view-allocations")
            .collect();
        // Independent stores have different digest bytes in their canonical
        // admissions. Check every operation against its own exact input size.
        let admission_read = [
            ("read", *admission_bytes),
            ("verify", *admission_bytes),
            ("retain", 0),
            ("verify", *admission_bytes),
        ];
        assert_eq!(observations, admission_read.repeat(2));
    }
    no_view_work(&pair[1].1, true);
    assert!(pair[0].1.iter().any(|(_, amount)| *amount > 0));
    std::panic::catch_unwind(|| no_view_work(&pair[0].1, true))
        .expect_err("redundant view work was not detected");
    no_view_work(&pair[0].1, false);
}

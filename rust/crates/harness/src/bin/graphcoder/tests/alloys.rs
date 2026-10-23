//! The same data cases qualify the shipping binary under the serialized Windows helper.
use super::*;
use axum::{body::Body, extract::State, http::StatusCode, response::Response};
use harness_mock_model::{Engine, Script, bind_result, events_with_usage};
use std::sync::Mutex;

struct Fixture {
    engine: Engine,
    records: Vec<Value>,
    killed: std::path::PathBuf,
}

async fn reply(
    State(state): State<Arc<Mutex<Fixture>>>,
    uri: axum::http::Uri,
    Json(body): Json<Value>,
) -> Response {
    let path = uri
        .path()
        .trim_start_matches('/')
        .strip_prefix("v1/")
        .unwrap_or_default();
    let mut state = state.lock().unwrap();
    let tokens = state.engine.input_tokens("alloy", &body).unwrap();
    let (step, status, bytes, hold) = match state.engine.take("alloy", path, &body) {
        Some((index, step)) => {
            let step = bind_result(step, &body).unwrap();
            let bytes = step.json.as_ref().map_or_else(
                || events_with_usage(path, &step, tokens).unwrap().concat(),
                Value::to_string,
            );
            (Some(index), step.status.unwrap_or(200), bytes, step.hold)
        }
        None if path == "count" => (None, 200, json!({"input_tokens":tokens}).to_string(), false),
        None => (None, 409, "unmatched alloy request".into(), false),
    };
    state
        .records
        .push(json!({"path":path,"step":step,"status":status,"body":body}));
    if status == 409 {
        eprintln!(
            "unmatched alloy tool result: {}",
            harness_mock_model::last_content(&body, true)
        );
    }
    if hold {
        std::fs::write(&state.killed, b"accepted").unwrap();
    }
    let body = if hold {
        Body::from_stream(futures::stream::pending::<
            Result<axum::body::Bytes, std::io::Error>,
        >())
    } else {
        Body::from(bytes)
    };
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header(
            "content-type",
            if path == "count" {
                "application/json"
            } else {
                "text/event-stream"
            },
        )
        .body(body)
        .unwrap()
}

fn expand(value: &mut Value, url: &str) {
    match value {
        Value::String(text) => *text = text.replace("{{url}}", url),
        Value::Array(values) => values.iter_mut().for_each(|value| expand(value, url)),
        Value::Object(values) => values.values_mut().for_each(|value| expand(value, url)),
        _ => {}
    }
}

fn subset(actual: &Value, expected: &Value) -> bool {
    match expected {
        Value::Object(fields) => fields
            .iter()
            .all(|(name, value)| subset(&actual[name], value)),
        _ => actual == expected,
    }
}

async fn case(document: &str) {
    let mut case: Value = serde_json::from_str(document).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    for path in [&workspace, &settings] {
        std::fs::create_dir(path).unwrap();
    }
    if let Some(files) = case["settings"].as_object() {
        for (path, text) in files {
            let path = settings.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text.as_str().unwrap()).unwrap();
        }
    }
    let killed = directory.path().join("kill-ready");
    let state = Arc::new(Mutex::new(Fixture {
        engine: Engine::new(Script {
            cases: [(
                "alloy".into(),
                serde_json::from_value(case["model"].clone()).unwrap(),
            )]
            .into(),
        }),
        records: Vec::new(),
        killed: killed.clone(),
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    expand(
        &mut case["catalog"],
        &format!("http://{}/v1", listener.local_addr().unwrap()),
    );
    let app = Router::new().fallback(reply).with_state(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, case["catalog"].to_string()).unwrap();
    for input in case["inputs"].as_array().unwrap() {
        let command = case_command(&case, input, &workspace, &settings, &catalog);
        let killed = input.get("kill_on").map(|_| killed.clone());
        let result = run(command, killed.clone()).await;
        if killed.is_some() {
            assert!(result.unwrap_err().starts_with("killed"));
            continue;
        }
        let output = result.unwrap();
        assert_eq!(
            output.status.code(),
            Some(input["exit"].as_i64().unwrap_or(0) as i32),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        for expected in input["stderr_contains"].as_array().into_iter().flatten() {
            assert!(String::from_utf8_lossy(&output.stderr).contains(expected.as_str().unwrap()));
        }
        if output.status.success() {
            let outcomes = results(&output.stdout);
            let outcome = outcomes.last().unwrap();
            assert_eq!(outcome["ok"]["text"], input["answer"]);
            if let Some(usage) = case["assert"].get("member_usage") {
                assert!(
                    subset(&outcome["ok"]["metadata"]["members"], usage),
                    "{outcome}"
                );
            }
        }
    }
    verify(&case, &state.lock().unwrap().records, &workspace);
    server.abort();
}

fn case_command(
    case: &Value,
    input: &Value,
    workspace: &Path,
    settings: &Path,
    catalog: &Path,
) -> Command {
    let operation = "00000000-0000-0000-0000-000000000001";
    let mut command = Command::new(build_metadata::executable());
    command
        .current_dir(workspace)
        .arg("--workspace")
        .arg(workspace)
        .arg("--settings-root")
        .arg(settings)
        .arg("--catalog")
        .arg(catalog)
        .args([
            "--operation",
            operation,
            "-p",
            input["text"].as_str().unwrap(),
        ])
        .env("HOME", settings)
        .env("USERPROFILE", settings)
        .env("CODEX_HOME", settings.join(".codex"))
        .env("XDG_DATA_HOME", settings)
        .env("LOCALAPPDATA", settings)
        .env_remove("GRAPHCODER_MODEL")
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(selection) = case["selection"].as_str() {
        command.args(["--model", selection]);
    }
    command
}

fn verify(case: &Value, records: &[Value], workspace: &Path) {
    let generations: Vec<_> = records
        .iter()
        .filter(|record| record["path"] != "count")
        .collect();
    assert_eq!(
        generations.len() as u64,
        case["assert"]["requests"].as_u64().unwrap()
    );
    assert!(
        records.iter().all(|record| record["status"] != 409),
        "{records:?}"
    );
    if let Some(paths) = case["assert"].get("generation_paths") {
        assert_eq!(
            Value::Array(
                generations
                    .iter()
                    .map(|record| record["path"].clone())
                    .collect()
            ),
            *paths
        );
    }
    if let Some(counts) = case["assert"]["generation_counts"].as_object() {
        for (path, count) in counts {
            assert_eq!(
                generations
                    .iter()
                    .filter(|record| record["path"] == *path)
                    .count() as u64,
                count.as_u64().unwrap()
            );
        }
    }
    if let Some(period) = case["assert"]["generation_period"].as_u64() {
        let period = usize::try_from(period).unwrap();
        assert!(
            generations
                .iter()
                .enumerate()
                .all(|(index, record)| record["path"] == generations[index % period]["path"])
        );
    }
    let mut consumed: Vec<_> = records
        .iter()
        .filter_map(|record| record["step"].as_u64())
        .collect();
    consumed.sort_unstable();
    assert_eq!(
        consumed,
        (0..case["model"]["steps"].as_array().unwrap().len() as u64).collect::<Vec<_>>()
    );
    for expected in case["assert"]["http"].as_array().into_iter().flatten() {
        assert!(
            records.iter().any(|record| subset(record, expected)),
            "{expected}"
        );
    }
    if let Some(files) = case["assert"]["files"].as_object() {
        for (path, text) in files {
            assert_eq!(
                std::fs::read_to_string(workspace.join(path)).unwrap(),
                text.as_str().unwrap()
            );
        }
    }
}

#[tokio::test]
async fn alloy_round_robin() {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-round-robin.json"
    ))
    .await;
}
#[tokio::test]
async fn alloy_weighted() {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-weighted.json"
    ))
    .await;
}
#[tokio::test]
async fn alloy_per_role() {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-per-role.json"
    ))
    .await;
}
#[tokio::test]
async fn alloy_settings() {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-settings.json"
    ))
    .await;
}
#[tokio::test]
async fn alloy_capacity() {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-capacity.json"
    ))
    .await;
}
#[tokio::test]
async fn alloy_retry() {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-retry.json"
    ))
    .await;
}
#[tokio::test]
async fn alloy_resume() {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-resume.json"
    ))
    .await;
}

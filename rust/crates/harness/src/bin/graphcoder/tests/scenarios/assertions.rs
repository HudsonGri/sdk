use super::{TestResult, json_matchers, provider::Recording, wire};
use acyclic_harness::{
    OperationId,
    client::jsonl,
    task_volume::{FileKey, TaskFile},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, process::Output};

pub fn output(
    case: &Value,
    operation: OperationId,
    output: &Output,
    replay: bool,
) -> TestResult<Vec<Value>> {
    let indeterminate =
        case["expect"]["outcome"] == "indeterminate" || case["expect"]["indeterminate"] == true;
    let expected_code = if indeterminate { 4 } else { 0 };
    if output.status.code() != Some(expected_code) {
        return Err(format!(
            "exit {:?}, expected {expected_code}\nstdout: {}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let text = std::str::from_utf8(&output.stdout)?;
    let mut lines = text.lines();
    let header_line = lines.next().ok_or("no JSONL header")?;
    let header: Value = serde_json::from_str(header_line)?;
    if header[0]["TaskSession"]["session"] != operation.to_string() {
        return Err(format!("wrong session header: {header}").into());
    }
    let mut results = Vec::new();
    let mut statuses = Vec::new();
    let mut files = Vec::new();
    let (_, snapshot) = jsonl::parse_snapshot(header_line)?;
    let mut previous = snapshot.cursor.position;
    for file in snapshot.files {
        let bytes: Vec<_> = file
            .segments
            .iter()
            .flat_map(|bytes| bytes.iter().copied())
            .collect();
        observe(file.key, &bytes, &mut results, &mut statuses, &mut files)?;
    }
    for line in lines {
        let change = jsonl::parse_change(line)?;
        if change.reset || change.cursor.position <= previous {
            return Err("JSONL follow reset or repeated its cursor".into());
        }
        previous = change.cursor.position;
        for file in change.file.into_iter().chain(change.files) {
            observe(
                file.key,
                &file.bytes,
                &mut results,
                &mut statuses,
                &mut files,
            )?;
        }
    }
    if results.len() != 1 {
        return Err(format!("expected exactly one JSONL execution result, got {results:?}").into());
    }
    let expected = if indeterminate {
        json!({"completion":"indeterminate"})
    } else {
        json!({"completion":"completed","ok":{"text":case["expect"]["text"]}})
    };
    if !json_matchers::subset(&expected, &results[0]) {
        return Err(format!(
            "JSONL result differs: expected {expected}, got {}",
            results[0]
        )
        .into());
    }
    if let Some(expected) = case["expect"]["statuses"].as_array() {
        let expected = if replay {
            &expected[expected.len().saturating_sub(1)..]
        } else {
            expected
        };
        json_matchers::ordered(expected, &statuses, "JSONL task statuses")?;
    }
    Ok(files)
}

fn observe(
    key: FileKey,
    bytes: &[u8],
    results: &mut Vec<Value>,
    statuses: &mut Vec<Value>,
    files: &mut Vec<Value>,
) -> TestResult {
    if matches!(
        key,
        FileKey::Session {
            file: TaskFile::ExecutionResult(_),
            ..
        }
    ) {
        results.push(serde_json::from_slice::<Value>(bytes)?);
    }
    if matches!(
        key,
        FileKey::Task {
            file: TaskFile::Status,
            ..
        }
    ) {
        statuses.push(serde_json::from_slice::<Value>(bytes)?);
    }
    files.push(json!({"path":key.path(), "bytes":bytes}));
    Ok(())
}

pub fn recording(case: &Value, protocol: &str, recording: &Recording) -> TestResult {
    super::history::check(case, &recording.requests, protocol)?;
    for text in case["expect"]["request_text"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let text = text.as_str().ok_or("request_text must contain strings")?;
        if !recording
            .requests
            .values()
            .flatten()
            .any(|body| json_matchers::subset(&json!({"$contains":text}), body))
        {
            return Err(format!("HTTP model requests omitted {text:?}").into());
        }
    }
    for (name, remaining) in &recording.remaining {
        if *remaining != 0 {
            return Err(format!("{name}: {remaining} unused HTTP replies").into());
        }
    }
    for (name, requests) in &recording.requests {
        let expected = case["expect"]["requests"]
            .get(name)
            .and_then(Value::as_u64)
            .or_else(|| case["expect"]["request_count"].as_u64())
            .or_else(|| {
                case["models"]
                    .get(name)
                    .and_then(Value::as_array)
                    .map(|turns| turns.len() as u64)
            })
            .or_else(|| {
                case.get("responses")
                    .or_else(|| case.get("turns"))
                    .and_then(Value::as_array)
                    .map(|turns| turns.len() as u64)
            });
        if let Some(expected) = expected
            && requests.len() as u64 != expected
        {
            return Err(format!(
                "{name}: expected {expected} HTTP requests, got {}",
                requests.len()
            )
            .into());
        }
        if let Some(expected) = case["expect"]["toolsets"][name].as_array() {
            let mut expected = expected.clone();
            expected.sort_by_key(Value::to_string);
            for request in requests {
                let mut actual: Vec<_> = request["tools"]
                    .as_array()
                    .ok_or("HTTP tools missing")?
                    .iter()
                    .map(|tool| tool["name"].clone())
                    .collect();
                actual.sort_by_key(Value::to_string);
                if actual != expected {
                    return Err(format!(
                        "HTTP toolset {name}: expected {expected:?}, got {actual:?}"
                    )
                    .into());
                }
            }
        }
        let adapter = if protocol == "anthropic" {
            acyclic_harness_providers::Adapter::Anthropic
        } else {
            acyclic_harness_providers::Adapter::Responses
        };
        for request in requests {
            let violations = acyclic_harness_providers::validate_input(adapter, request);
            if !violations.is_empty() {
                return Err(
                    format!("invalid {protocol} HTTP request: {violations:?}: {request}").into(),
                );
            }
            if request["stream"] != true {
                return Err("HTTP request did not select streaming".into());
            }
        }
        if case["driver"] == "provider" {
            let expected = case["expect"]["requests"]
                .as_array()
                .ok_or("provider request expectations missing")?;
            if expected.len() != requests.len() {
                return Err("provider request count differs".into());
            }
            for (expected, actual) in expected.iter().zip(requests) {
                for (pointer, expected) in expected
                    .as_object()
                    .ok_or("provider request assertion missing")?
                {
                    if !actual
                        .pointer(pointer)
                        .is_some_and(|actual| json_matchers::subset(expected, actual))
                    {
                        return Err(format!(
                            "HTTP request {pointer}: expected {expected}, got {actual}"
                        )
                        .into());
                    }
                }
            }
        }
        if name == "parent"
            && let Some(expected) = case["expect"]["results"].as_object()
        {
            let request = requests.last().ok_or("no final HTTP request")?;
            let outputs: BTreeMap<_, _> = wire::outputs(request, protocol)?
                .into_iter()
                .filter(|(id, _)| !id.starts_with("binary_seed_"))
                .collect();
            if expected.keys().ne(outputs.keys()) {
                return Err(format!(
                    "HTTP tool result identities differ: expected {expected:?}, got {outputs:?}"
                )
                .into());
            }
            for (call, expected) in expected {
                if let Some(content) = expected.get("content") {
                    let actual = if content["kind"] == "parts" {
                        json!({"kind":"parts","parts":[{"kind":"text","text":outputs[call]}]})
                    } else {
                        json!({"kind":"json","value":outputs[call]})
                    };
                    if !json_matchers::subset(content, &actual) {
                        return Err(format!(
                            "HTTP tool result {call}: expected {content}, got {actual}"
                        )
                        .into());
                    }
                }
            }
        }
    }
    if let Some(expected) = case["expect"]["task_files"].as_object() {
        let audits = recording
            .audits
            .get("parent")
            .ok_or("missing parent HTTP audit")?;
        if audits.len() != 1 {
            return Err("child files were not inspected through the shipped fs tool".into());
        }
        let outputs: BTreeMap<_, _> = wire::outputs(&audits[0], protocol)?.into_iter().collect();
        for (index, (path, expected)) in expected.iter().enumerate() {
            let id = format!("binary_audit_{index}");
            let result = outputs
                .get(&id)
                .ok_or_else(|| format!("missing child file result {path}"))?;
            let actual: Value = serde_json::from_str(
                result["stdout"]
                    .as_str()
                    .ok_or("child fs read returned no stdout")?,
            )?;
            if result["success"] != true || !json_matchers::subset(expected, &actual) {
                return Err(format!("child {path}: expected {expected}, got {actual}").into());
            }
        }
    }
    for (name, attempts) in &recording.attempts {
        let turns = case["models"].get(name).or_else(|| case.get("turns"));
        let prepares = usize::from(
            turns
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_array)
                .flatten()
                .any(|event| {
                    event["arguments"]["file"].is_string()
                        && event["name"].as_str().is_some_and(|name| {
                            matches!(name.split('@').next(), Some("read" | "edit"))
                        })
                }),
        );
        let expected = recording.requests[name].len() + prepares + recording.audits[name].len();
        if *attempts != expected {
            return Err(
                format!("{name}: expected {expected} total HTTP attempts, got {attempts}").into(),
            );
        }
    }
    Ok(())
}

pub fn files(case: &Value, workspace: &Path) -> TestResult {
    for (path, expected) in case["expect"]["files"].as_object().into_iter().flatten() {
        let actual = match std::fs::read(workspace.join(path)) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if actual.as_deref() != expected.as_str().map(str::as_bytes) {
            return Err(format!("workspace {path}: expected {expected}, got {actual:?}").into());
        }
    }
    Ok(())
}

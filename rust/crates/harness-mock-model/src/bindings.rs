use crate::{Step, last_content};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonSelection {
    text: Value,
    pointer: String,
    #[serde(default)]
    set: BTreeMap<String, Value>,
}

/// Resolve exact JSON leaves `{"$request":"/pointer"}` or
/// `{"$tool_result":"/pointer"}`, and string insertions `{{request:/pointer}}`
/// or `{{tool_result:/pointer}}` throughout a response. Strings insert raw text;
/// other values insert compact JSON. Pointers use RFC 6901 escaping. Missing or
/// malformed templates fail explicitly. Copied values are never re-expanded.
/// `{"$json":{"text":{"$request":"/pointer"},"pointer":"/selection",
/// "set":{"/existing/leaf":"replacement"}}}` selects from JSON-encoded request
/// text. Optional mutations replace existing selected leaves only; copied data
/// remains opaque, including strings which resemble binding expressions.
pub fn bind_result(step: &Step, body: &Value) -> Result<Step, String> {
    if step.replay.is_some() {
        return Ok(step.clone());
    }
    let result = serde_json::from_str(&last_content(body, true)).unwrap_or(Value::Null);
    let mut value = serde_json::to_value(step).map_err(|error| error.to_string())?;
    // Guards have already matched; they are request data, not reply templates.
    value
        .as_object_mut()
        .ok_or("step is not an object")?
        .remove("when");
    bind(&mut value, body, &result)?;
    let mut bound: Step = serde_json::from_value(value).map_err(|error| error.to_string())?;
    bound.when = step.when.clone();
    Ok(bound)
}

fn lookup<'a>(source: &'a Value, pointer: &str) -> Result<&'a Value, String> {
    source
        .pointer(pointer)
        .ok_or_else(|| format!("missing template pointer {pointer}"))
}

fn bind(value: &mut Value, request: &Value, result: &Value) -> Result<(), String> {
    match value {
        Value::Object(fields) if fields.len() == 1 && fields.contains_key("$json") => {
            let mut selection: JsonSelection = serde_json::from_value(fields["$json"].clone())
                .map_err(|error| format!("invalid JSON selection: {error}"))?;
            bind(&mut selection.text, request, result)?;
            let text = selection.text.as_str().ok_or("JSON selection needs text")?;
            let decoded: Value = serde_json::from_str(text)
                .map_err(|error| format!("invalid selected JSON text: {error}"))?;
            let mut selected = lookup(&decoded, &selection.pointer)?.clone();
            for (pointer, mut replacement) in selection.set {
                bind(&mut replacement, request, result)?;
                *selected
                    .pointer_mut(&pointer)
                    .ok_or_else(|| format!("missing mutation pointer {pointer}"))? = replacement;
            }
            *value = selected;
        }
        Value::Object(fields)
            if fields.len() == 1
                && (fields.contains_key("$request") || fields.contains_key("$tool_result")) =>
        {
            let (key, source) = if fields.contains_key("$request") {
                ("$request", request)
            } else {
                ("$tool_result", result)
            };
            let pointer = fields[key]
                .as_str()
                .ok_or("template pointer must be a string")?;
            *value = lookup(source, pointer)?.clone();
        }
        Value::Object(fields) => {
            for child in fields.values_mut() {
                bind(child, request, result)?;
            }
        }
        Value::Array(items) => {
            for child in items {
                bind(child, request, result)?;
            }
        }
        Value::String(text) => {
            let mut rest = text.as_str();
            let mut output = String::new();
            loop {
                let candidate = [("{{request:", request), ("{{tool_result:", result)]
                    .into_iter()
                    .filter_map(|(prefix, source)| {
                        rest.find(prefix).map(|index| (index, prefix, source))
                    })
                    .min_by_key(|(index, _, _)| *index);
                let Some((index, prefix, source)) = candidate else {
                    break;
                };
                output.push_str(&rest[..index]);
                rest = &rest[index + prefix.len()..];
                let end = rest.find("}}").ok_or("unterminated request template")?;
                let copied = lookup(source, &rest[..end])?;
                match copied.as_str() {
                    Some(text) => output.push_str(text),
                    None => output.push_str(&copied.to_string()),
                }
                rest = &rest[end + 2..];
            }
            output.push_str(rest);
            *text = output;
        }
        _ => {}
    }
    Ok(())
}

/// Loads scripts and the single shared provider recording format before serving.
/// Raw captures never pass through string templates or SSE generation.
pub fn load_script(path: &std::path::Path) -> std::io::Result<crate::Script> {
    let mut script: crate::Script =
        serde_json::from_slice(&std::fs::read(path)?).map_err(std::io::Error::other)?;
    let root = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    for case in script.cases.values_mut() {
        for step in &mut case.steps {
            if let Some(recording) = &step.recording {
                if !step.text.is_empty()
                    || !step.tools.is_empty()
                    || !step.custom_tools.is_empty()
                    || step.json.is_some()
                    || step.status.is_some()
                    || step.hold
                {
                    return Err(std::io::Error::other(
                        "STEP load recording failed: raw and generated responses are mutually exclusive; recovery: keep only the recording and request guards in this script step",
                    ));
                }
                step.replay = Some(acyclic_harness_core::http::recording::Exchange::load(
                    &root.join(recording),
                )?);
            }
        }
    }
    Ok(script)
}

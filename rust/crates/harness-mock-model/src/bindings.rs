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

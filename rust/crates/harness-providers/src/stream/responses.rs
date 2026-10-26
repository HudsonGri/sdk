use super::{ModelEvent, Result, State, Value, index, invalid, string, text};

pub(super) fn parse(state: &mut State, value: &Value, data: &str) -> Result<Vec<ModelEvent>> {
    if state.projection {
        projection_coverage(state, value)?;
    }
    let mut events = Vec::new();
    match value["type"].as_str() {
        Some("response.output_text.delta" | "response.refusal.delta") => {
            text(&mut events, &value["delta"], false)
        }
        Some("response.reasoning_summary_text.delta" | "response.reasoning_text.delta") => {
            text(&mut events, &value["delta"], true)
        }
        Some("response.output_item.added" | "response.output_item.done") => {
            let item = &value["item"];
            if value["type"] == "response.output_item.done" && item["type"] == "message" {
                state.complete_message(index(value, "output_index")?, item)?;
            }
            if item["type"] == "reasoning" {
                #[derive(serde::Deserialize)]
                struct Event<'a> {
                    #[serde(borrow)]
                    item: &'a serde_json::value::RawValue,
                }
                let event: Event<'_> =
                    serde_json::from_str(data).map_err(|_| invalid("invalid reasoning item"))?;
                state.raw_part(
                    index(value, "output_index")?,
                    item,
                    Some(event.item.get()),
                    false,
                )?;
            }
            if item["type"] == "function_call" {
                let (call, budget) = state.call(index(value, "output_index")?)?;
                budget.string(&mut call.id, string(&item["call_id"])?)?;
                budget.string(&mut call.name, string(&item["name"])?)?;
                if let Some(arguments) = item["arguments"].as_str() {
                    budget.string(&mut call.arguments, arguments)?;
                }
            }
        }
        Some("response.function_call_arguments.delta") => {
            let index = index(value, "output_index")?;
            if !state.calls.contains_key(&index) {
                return Err(invalid("tool delta without item"));
            }
            let (call, budget) = state.call(index)?;
            budget.append(&mut call.arguments, value.get("delta"))?;
        }
        Some("response.function_call_arguments.done") => {
            let index = index(value, "output_index")?;
            if !state.calls.contains_key(&index) {
                return Err(invalid("tool done without item"));
            }
            let (call, budget) = state.call(index)?;
            budget.string(
                &mut call.arguments,
                value["arguments"]
                    .as_str()
                    .ok_or_else(|| invalid("tool arguments must be a string"))?,
            )?;
        }
        Some("response.completed" | "response.incomplete") => {
            let response = &value["response"];
            if response["status"]
                .as_str()
                .is_some_and(|status| !matches!(status, "completed" | "incomplete"))
            {
                return Err(invalid("Responses generation did not complete"));
            }
            if let Some(output) = response.get("output") {
                let output = output
                    .as_array()
                    .ok_or_else(|| invalid("response output must be an array"))?;
                if let Some((_, text)) = &mut state.completed_message {
                    state.budget.string(text, "")?;
                }
                state.completed_message = Some((0, String::new()));
                for (position, item) in output.iter().enumerate() {
                    if item["type"] == "message" {
                        state.complete_message(
                            u64::try_from(position)
                                .map_err(|_| invalid("response output index overflow"))?,
                            item,
                        )?;
                    }
                }
            }
            state.identity(response)?;
            state.usage(&response["usage"])?;
            if value["type"] == "response.incomplete" || response["status"] == "incomplete" {
                if response["incomplete_details"]["reason"] != "max_output_tokens" {
                    return Err(invalid("Responses generation was incomplete"));
                }
                state.finish_reason("length")?;
            }
            state.terminal = true;
        }
        _ => {}
    }
    Ok(events)
}

// Coverage belongs at the canonical decoder, before normalization discards any
// provider-specific semantics. This is intentionally narrower than live decoding.
fn projection_coverage(state: &State<'_>, value: &Value) -> Result<()> {
    if state.terminal {
        return Err(invalid("recording has events after its terminal response"));
    }
    projection_event_fields(value)?;
    match value["type"].as_str() {
        Some("response.output_item.added" | "response.output_item.done") => {
            projection_item(&value["item"])?;
        }
        Some("response.content_part.added" | "response.content_part.done") => {
            projection_text(&value["part"])?;
        }
        Some("response.created" | "response.in_progress" | "response.queued") => {
            if value["response"]
                .get("output")
                .is_some_and(|output| output.as_array().is_none_or(|items| !items.is_empty()))
            {
                return Err(invalid("recording has nonempty preliminary output"));
            }
        }
        Some(
            "response.output_text.delta"
            | "response.output_text.done"
            | "response.function_call_arguments.delta"
            | "response.function_call_arguments.done",
        ) => {}
        Some("response.completed") => {
            let response = &value["response"];
            if response["status"] != "completed" {
                return Err(invalid("recording projection requires completed output"));
            }
            let output = response["output"]
                .as_array()
                .ok_or_else(|| invalid("recording projection requires authoritative output"))?;
            let mut calls = 0usize;
            let mut messages = 0usize;
            for (position, item) in output.iter().enumerate() {
                projection_item(item)?;
                if item
                    .get("status")
                    .is_some_and(|status| status != "completed")
                {
                    return Err(invalid("recording terminal item was not completed"));
                }
                if item["type"] == "message" {
                    messages += 1;
                } else {
                    calls += 1;
                    let position = u64::try_from(position)
                        .map_err(|_| invalid("recording output index overflow"))?;
                    let call = state
                        .calls
                        .get(&position)
                        .ok_or_else(|| invalid("recording terminal call was not streamed"))?;
                    if item["call_id"].as_str() != Some(call.id.as_str())
                        || item["name"].as_str() != Some(call.name.as_str())
                        || item["arguments"].as_str() != Some(call.arguments.as_str())
                    {
                        return Err(invalid(
                            "recording terminal call differs from streamed call",
                        ));
                    }
                }
            }
            if messages > 1 || calls != state.calls.len() {
                return Err(invalid(
                    "recording output cannot be represented without loss",
                ));
            }
        }
        _ => return Err(invalid("unsupported recording projection event")),
    }
    Ok(())
}

fn projection_event_fields(value: &Value) -> Result<()> {
    let fields: &[&str] = match value["type"].as_str() {
        Some("response.output_item.added" | "response.output_item.done") => {
            &["type", "sequence_number", "output_index", "item"]
        }
        Some("response.content_part.added" | "response.content_part.done") => &[
            "type",
            "sequence_number",
            "item_id",
            "output_index",
            "content_index",
            "part",
        ],
        Some(
            "response.created" | "response.in_progress" | "response.queued" | "response.completed",
        ) => &["type", "sequence_number", "response"],
        Some("response.output_text.delta") => &[
            "type",
            "sequence_number",
            "item_id",
            "output_index",
            "content_index",
            "delta",
            "logprobs",
        ],
        Some("response.output_text.done") => &[
            "type",
            "sequence_number",
            "item_id",
            "output_index",
            "content_index",
            "text",
            "logprobs",
        ],
        Some("response.function_call_arguments.delta") => &[
            "type",
            "sequence_number",
            "item_id",
            "output_index",
            "delta",
        ],
        Some("response.function_call_arguments.done") => &[
            "type",
            "sequence_number",
            "item_id",
            "output_index",
            "arguments",
        ],
        _ => return Err(invalid("unsupported recording projection event")),
    };
    projection_fields(value, fields)?;
    if value
        .get("logprobs")
        .is_some_and(|value| value.as_array().is_none_or(|values| !values.is_empty()))
    {
        return Err(invalid("unsupported recording projection logprobs"));
    }
    Ok(())
}

fn projection_fields(value: &Value, fields: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("recording output must be an object"))?;
    if object.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(invalid("unsupported recording projection field"));
    }
    Ok(())
}

fn projection_item(item: &Value) -> Result<()> {
    match item["type"].as_str() {
        Some("function_call") => projection_fields(
            item,
            &["type", "id", "status", "call_id", "name", "arguments"],
        ),
        Some("message") => {
            projection_fields(item, &["type", "id", "status", "role", "content"])?;
            let parts = item["content"]
                .as_array()
                .ok_or_else(|| invalid("recording message content must be an array"))?;
            for part in parts {
                projection_text(part)?;
            }
            Ok(())
        }
        _ => Err(invalid("unsupported recording projection output item")),
    }
}

fn projection_text(part: &Value) -> Result<()> {
    projection_fields(part, &["type", "text", "annotations", "logprobs"])?;
    if part["type"] != "output_text"
        || !part["text"].is_string()
        || ["annotations", "logprobs"].iter().any(|field| {
            part.get(field)
                .is_some_and(|value| value.as_array().is_none_or(|values| !values.is_empty()))
        })
    {
        return Err(invalid("unsupported recording projection text semantics"));
    }
    Ok(())
}

impl State<'_> {
    fn complete_message(&mut self, position: u64, item: &Value) -> Result<()> {
        if item["role"] != "assistant" {
            return Err(invalid("response message must be assistant-authored"));
        }
        if self
            .completed_message
            .as_ref()
            .is_some_and(|(last, _)| *last > position)
        {
            return Ok(());
        }
        let content = item["content"]
            .as_array()
            .ok_or_else(|| invalid("response message content must be an array"))?;
        let (last, text) = self
            .completed_message
            .get_or_insert_with(|| (position, String::new()));
        self.budget.string(text, "")?;
        for part in content {
            let value = match part["type"].as_str() {
                Some("output_text") => &part["text"],
                Some("refusal") => &part["refusal"],
                _ => return Err(invalid("unsupported assistant message content")),
            };
            self.budget.append_str(
                text,
                value
                    .as_str()
                    .ok_or_else(|| invalid("assistant message text must be a string"))?,
            )?;
        }
        *last = position;
        Ok(())
    }
}

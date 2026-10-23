//! Resident JSON maps retain their unused `BTreeMap` node slots, too.
use super::inventory::add;
use super::*;

fn map_slack(value: &Value) -> Result<u64> {
    crate::executor::json::json_value_retained_owned_bytes(value)?
        .checked_sub(crate::executor::json::json_value_logical_owned_bytes(
            value,
        )?)
        .ok_or_else(super::inventory::overflow)
}

fn part_slack(part: &ModelContentPart) -> Result<u64> {
    match part {
        ModelContentPart::ToolCall(call) => {
            add(map_slack(&call.arguments)?, map_slack(&call.metadata)?)
        }
        ModelContentPart::ToolResult {
            content: ToolResultContent::Json { value },
            ..
        } => map_slack(value),
        _ => Ok(0),
    }
}

impl ModelRequest {
    pub(super) fn retained_payload_bytes(&self) -> Result<u64> {
        let mut bytes = add(self.payload_bytes()?, map_slack(&self.model.options)?)?;
        for tool in &self.tools {
            for schema in [
                &tool.input_schema,
                &tool.output_schema,
                &tool.projection_schema,
            ] {
                bytes = add(bytes, map_slack(schema)?)?;
            }
        }
        for message in &self.messages {
            for part in message.content.parts() {
                bytes = add(bytes, part_slack(part)?)?;
            }
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_request_inventory_covers_sparse_maps_in_every_json_slot() -> Result<()> {
        let sparse = serde_json::json!({"outer": [{"inner": "value"}]});
        let slack = map_slack(&sparse)?;
        assert!(slack > 0, "logical entry slots omit sparse node capacity");
        let request = ModelRequest {
            model: Model::new(
                "test".into(),
                "inventory".into(),
                "1".into(),
                sparse.clone(),
            )?,
            messages: vec![ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Parts(vec![
                    ModelContentPart::ToolCall(
                        ToolCall::new("call".into(), "tool".into(), sparse.clone())
                            .with_metadata(sparse.clone()),
                    ),
                    ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "tool".into(),
                        content: ToolResultContent::Json {
                            value: sparse.clone(),
                        },
                    },
                ]),
            }],
            tools: vec![crate::tool::ToolDefinition {
                name: "tool".into(),
                revision: "1".into(),
                description: "test".into(),
                input_schema: sparse.clone(),
                output_schema: sparse.clone(),
                projection_schema: sparse,
            }],
            max_output_tokens: None,
        };
        // Seven JSON positions; the old logical inventory is the negative control.
        assert_eq!(
            request.retained_payload_bytes()? - request.payload_bytes()?,
            7 * slack
        );
        Ok(())
    }
}

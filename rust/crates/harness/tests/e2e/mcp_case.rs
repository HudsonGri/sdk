use acyclic_harness::mcp::{McpSchemaExposure, McpToolDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct McpFixture {
    pub server: String,
    pub pages: BTreeMap<String, Page>,
    pub replies: Vec<Reply>,
    #[serde(default)]
    pub schema_exposure: McpSchemaExposure,
    pub enabled_tools: Option<Vec<String>>,
    #[serde(default)]
    pub disabled_tools: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Page {
    pub tools: Vec<McpToolDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reply {
    pub name: String,
    pub arguments: Value,
    pub result: Value,
    #[serde(default)]
    pub files: BTreeMap<String, String>,
}

impl McpFixture {
    pub(super) fn reply(&self, name: &str, arguments: &Value) -> Result<&Reply, &'static str> {
        let mut matches = self
            .replies
            .iter()
            .filter(|reply| reply.name == name && reply.arguments == *arguments);
        let reply = matches
            .next()
            .ok_or("unexpected MCP tool name or arguments")?;
        if matches.next().is_some() {
            return Err("ambiguous MCP tool reply");
        }
        Ok(reply)
    }
}

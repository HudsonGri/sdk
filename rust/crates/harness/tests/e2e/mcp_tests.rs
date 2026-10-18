use super::{TestResult, local_case::LocalCase};
use proptest::prelude::*;
use serde_json::json;

const CASE: &[u8] = include_bytes!("../../../harness-fuzz/corpus/client_diff/local-mcp");

proptest! {
    #[test]
    fn native_fixture_codec_and_reply_binding_preserve_exact_json(text in ".{0,64}") {
        let mut case: LocalCase = serde_json::from_slice(CASE)?;
        let fixture = case.mcp.as_mut().unwrap();
        fixture.replies[0].arguments = json!({"nested":{"text":text}, "array":[text]});
        let arguments = fixture.replies[0].arguments.clone();
        prop_assert!(fixture.reply("echo", &arguments).is_ok());
        prop_assert!(fixture.reply("different", &arguments).is_err());
        let mut inserted = arguments.clone();
        inserted["nested"]["injected"] = true.into();
        prop_assert!(fixture.reply("echo", &inserted).is_err());
        inserted = arguments.clone();
        inserted["array"].as_array_mut().unwrap().push("injected".into());
        prop_assert!(fixture.reply("echo", &inserted).is_err());
        let mut duplicate: super::mcp_case::McpFixture =
            serde_json::from_value(serde_json::to_value(&fixture)?)?;
        duplicate.replies.push(serde_json::from_value(serde_json::to_value(&fixture.replies[0])?)?);
        prop_assert!(duplicate.reply("echo", &arguments).is_err());
        let encoded = serde_json::to_vec(&case)?;
        let restored: LocalCase = serde_json::from_slice(&encoded)?;
        prop_assert_eq!(serde_json::to_value(case)?, serde_json::to_value(restored)?);
    }
}

#[tokio::test]
async fn corrupted_real_http_reply_fails_unchanged_model_and_file_expectations() -> TestResult {
    let baseline: LocalCase = serde_json::from_slice(CASE)?;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        super::local_execution::run(baseline),
    )
    .await??;
    let mut broken: LocalCase = serde_json::from_slice(CASE)?;
    broken
        .mcp
        .as_mut()
        .ok_or("missing native MCP fixture")?
        .replies[0]
        .result["content"][0]["text"] = "CORRUPTED".into();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        super::local_execution::run(broken),
    )
    .await?
    .expect_err("corrupted HTTP response passed unchanged expectations");
    assert!(
        error.to_string().contains("local model requests"),
        "{error}"
    );
    Ok(())
}

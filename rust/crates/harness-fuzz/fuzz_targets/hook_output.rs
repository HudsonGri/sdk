#![no_main]

use acyclic_harness::{Error, Result};
use acyclic_harness::{TaskId, tool};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

// Include the production pure decoder and normalized types. No native runner,
// model provider, process, or public testing API is needed by this target.
#[path = "../../harness/src/hooks/spec.rs"]
mod spec;
use spec::{HookEvent, HookFailure};
#[path = "../../harness/src/hooks/output.rs"]
mod output;
use output::HookOutput;
#[path = "../../harness/src/hooks/input.rs"]
mod input;
use input::HookInput;

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 4096 {
        return;
    }
    if let Ok(fields) = serde_json::from_slice::<Value>(bytes) {
        let invocation = tool::ToolInvocation {
            operation_id: acyclic_harness::OperationId::from_bytes([1; 16]),
            call_id: "fuzz-call".into(),
            name: "fuzz-tool".into(),
            arguments: fields.clone(),
        };
        let response = tool::ToolResult {
            value: fields.clone(),
        };
        for post in [false, true] {
            let event = if post {
                HookEvent::PostToolUse
            } else {
                HookEvent::PreToolUse
            };
            let input = HookInput::Tool {
                invocation: &invocation,
                response: post.then_some(&response),
                session_id: None,
            };
            let encoded = input
                .encode(event, std::path::Path::new("workspace"))
                .unwrap();
            let decoded: Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(&decoded["tool_input"], &fields);
            assert_eq!(decoded.get("tool_response"), post.then_some(&fields));
            assert_eq!(decoded["hook_event_name"], event.name());
            assert_eq!(decoded["cwd"], "workspace");
        }
        let encoded = HookInput::Fields(&fields)
            .encode(HookEvent::PreToolUse, std::path::Path::new("workspace"));
        if fields.is_object() {
            let decoded: Value = serde_json::from_slice(&encoded.unwrap()).unwrap();
            assert_eq!(decoded["cwd"], "workspace");
            assert_eq!(decoded["hook_event_name"], "PreToolUse");
            for (key, value) in fields.as_object().unwrap() {
                if !matches!(key.as_str(), "cwd" | "hook_event_name") {
                    assert_eq!(&decoded[key], value);
                }
            }
        } else {
            assert!(encoded.is_err());
        }
    }
    if let Ok(output) = serde_json::from_slice::<HookOutput>(bytes) {
        let encoded = serde_json::to_vec(&output).unwrap();
        let decoded: HookOutput = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(output, decoded);
    }
    for event in [
        HookEvent::SessionStart,
        HookEvent::PreToolUse,
        HookEvent::PostToolUse,
        HookEvent::Stop,
    ] {
        let mut reports = HookOutput::default();
        reports.apply(
            event,
            HookFailure::Report,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        let mut merged = HookOutput {
            approval: Some("earlier approval".into()),
            ..HookOutput::default()
        };
        merged.apply(
            event,
            HookFailure::Report,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        assert_eq!(merged.approval.as_deref(), Some("earlier approval"));
        assert_eq!(merged.blocked, reports.blocked);
        let mut blocks = HookOutput::default();
        blocks.apply(
            event,
            HookFailure::Block,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        assert_eq!(reports.diagnostics.len(), blocks.diagnostics.len());
        for (report, block) in reports.diagnostics.iter().zip(&blocks.diagnostics) {
            assert_eq!(report.kind, block.kind);
            assert_eq!(report.exit_code, block.exit_code);
            assert_eq!(report.message, block.message);
            assert_eq!(block.blocking, event != HookEvent::Stop);
        }
        if reports.diagnostics.is_empty() || event == HookEvent::Stop {
            assert_eq!(reports, blocks);
        } else {
            assert!(blocks.blocks());
            assert!(reports.blocked.is_none());
            assert!(reports.additional_context.is_empty());
            assert!(reports.updated_input.is_none());
            assert!(reports.approval.is_none());
        }
        if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
            let canonical = serde_json::to_vec(&value).unwrap();
            let mut round_trip = HookOutput::default();
            round_trip.apply(event, HookFailure::Report, Ok((Some(0), canonical, vec![])));
            // Syntax diagnostics contain byte positions; compare semantics
            // only when the structured input was accepted.
            if reports.diagnostics.is_empty() {
                if event == HookEvent::PreToolUse
                    && value
                        .pointer("/hookSpecificOutput/permissionDecision")
                        .and_then(Value::as_str)
                        == Some("ask")
                    && let Some(reason) = value
                        .pointer("/hookSpecificOutput/permissionDecisionReason")
                        .and_then(Value::as_str)
                {
                    assert_eq!(reports.approval.as_deref(), Some(reason));
                }
                assert_eq!(reports, round_trip);
            }
        }
    }
    if let Ok(rule) = serde_json::from_slice::<spec::HookRule>(bytes) {
        let encoded = serde_json::to_vec(&rule).unwrap();
        let decoded: spec::HookRule = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(rule, decoded);
        for (original, restored) in rule.hooks.iter().zip(&decoded.hooks) {
            assert_eq!(original.validate().is_ok(), restored.validate().is_ok());
        }
    }
});

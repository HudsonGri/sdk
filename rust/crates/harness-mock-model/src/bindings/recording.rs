use super::{JsonSelection, bind};
use crate::{Step, Tool};
use acyclic_harness_core::model::ModelEvent;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

/// One declared captured tool-argument selector and its request-bound replacement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedBinding {
    /// Original provider call identity, not a newly minted execution identity.
    pub call_id: String,
    /// Exact original tool name.
    pub name: String,
    /// JSON pointer within the captured tool arguments.
    pub pointer: String,
    /// Pure selection from the captured request, without mutations.
    pub captured: JsonSelection,
    /// Existing binder expression evaluated against the fresh request.
    pub replacement: Value,
}

/// Explicit derivation policy. The original capture always remains immutable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingProjection {
    /// Existing selected provider's event bound, never an implicit larger bound.
    pub maximum_event_bytes: usize,
    /// Nonoverlapping, exact tool selectors; an unmatched binding refuses serving.
    pub bindings: Vec<CapturedBinding>,
}

pub(super) fn decode_recording(step: &Step) -> Result<Step, String> {
    let projection = step
        .recording_projection
        .as_ref()
        .ok_or("missing projection")?;
    if projection.maximum_event_bytes == 0 || projection.bindings.is_empty() {
        return Err("projection requires an event bound and at least one binding".into());
    }
    let replay = step.replay.as_ref().ok_or("missing admitted exchange")?;
    if replay.provenance().identity.adapter != "responses"
        || replay.provenance().status != Some(200)
    {
        return Err("selector projection requires a successful Responses exchange".into());
    }
    let events = futures::executor::block_on(acyclic_harness_providers::decode_recording(
        replay,
        projection.maximum_event_bytes,
    ))
    .map_err(|error| error.to_string())?;
    let mut decoded = Step::default();
    let mut message = None;
    for event in events {
        match event {
            ModelEvent::Content { delta } => decoded.text.push(delta),
            ModelEvent::MessageCompleted { text } => message = Some(text),
            ModelEvent::ToolCall(call) => decoded.tools.push(Tool {
                id: call
                    .metadata
                    .pointer("/provider_call/id")
                    .and_then(Value::as_str)
                    .ok_or("captured tool has no original provider call identity")?
                    .into(),
                name: call.name,
                arguments: call.arguments,
                namespace: None,
            }),
            ModelEvent::Completed { metadata } => {
                if metadata
                    .get("finish_reason")
                    .is_some_and(|value| value == "length")
                {
                    return Err("captured model output is incomplete".into());
                }
            }
            ModelEvent::Reasoning { .. } | ModelEvent::ReasoningBlock { .. } => {
                return Err("semantic projection cannot discard captured reasoning; use raw replay for this exchange".into());
            }
        }
    }
    if let Some(text) = message {
        decoded.text = vec![text];
    }
    // Validate every captured selector during admission, before the listener exists.
    bind_captured(step, &decoded, None)?;
    Ok(decoded)
}

pub(super) fn bind_recording(step: &Step, request: &Value) -> Result<Step, String> {
    if step.recording_projection.is_none() {
        return Ok(step.clone());
    }
    let decoded = step
        .decoded_replay
        .as_deref()
        .ok_or("projection was not admitted")?;
    let derived = bind_captured(step, decoded, Some(request))?;
    let mut bound = step.clone();
    bound.derived_replay = Some(Box::new(derived));
    Ok(bound)
}

fn bind_captured(step: &Step, decoded: &Step, request: Option<&Value>) -> Result<Step, String> {
    let replay = step.replay.as_ref().ok_or("missing admitted exchange")?;
    let original: Value =
        serde_json::from_slice(replay.request()).map_err(|error| error.to_string())?;
    let projection = step
        .recording_projection
        .as_ref()
        .ok_or("missing projection")?;
    bind_selectors(projection, &original, decoded, request)
}

fn bind_selectors(
    projection: &RecordingProjection,
    original: &Value,
    decoded: &Step,
    request: Option<&Value>,
) -> Result<Step, String> {
    let mut selected = BTreeSet::<(String, String)>::new();
    let mut derived = decoded.clone();
    for binding in &projection.bindings {
        if !binding.captured.set.is_empty() || binding.pointer.is_empty() {
            return Err("captured selection must be pure and target a tool-argument member".into());
        }
        if selected.iter().any(|(id, pointer)| {
            id == &binding.call_id
                && (pointer == &binding.pointer
                    || pointer
                        .strip_prefix(&binding.pointer)
                        .is_some_and(|tail| tail.starts_with('/'))
                    || binding
                        .pointer
                        .strip_prefix(pointer)
                        .is_some_and(|tail| tail.starts_with('/')))
        }) {
            return Err("captured selector bindings overlap".into());
        }
        selected.insert((binding.call_id.clone(), binding.pointer.clone()));
        let mut expected = json!({"$json":binding.captured});
        bind(&mut expected, original, &Value::Null)?;
        let index = {
            let mut targets = derived
                .tools
                .iter()
                .enumerate()
                .filter(|(_, tool)| tool.id == binding.call_id && tool.name == binding.name);
            let (index, _) = targets
                .next()
                .ok_or("captured call selector is unmatched")?;
            if targets.next().is_some() {
                return Err("captured call selector is ambiguous".into());
            }
            index
        };
        let target = &mut derived.tools[index];
        if target.arguments.pointer(&binding.pointer) != Some(&expected) {
            return Err("captured tool selector differs from its original request".into());
        }
        if let Some(request) = request {
            let mut replacement = binding.replacement.clone();
            bind(&mut replacement, request, &Value::Null)?;
            *target
                .arguments
                .pointer_mut(&binding.pointer)
                .ok_or("captured pointer disappeared")? = replacement;
        }
    }
    Ok(derived)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn policy() -> RecordingProjection {
        serde_json::from_value(json!({"maximum_event_bytes":65536,"bindings":[{
            "call_id":"read-call","name":"read","pointer":"/source",
            "captured":{"text":{"$request":"/catalog"},"pointer":"/source"},
            "replacement":{"$json":{"text":{"$request":"/catalog"},"pointer":"/source"}}
        }]}))
        .unwrap()
    }
    fn call(source: Value) -> Step {
        Step {
            tools: vec![Tool {
                id: "read-call".into(),
                name: "read".into(),
                namespace: None,
                arguments: json!({"source":source}),
            }],
            ..Step::default()
        }
    }

    proptest! {
        #[test]
        fn captured_selectors_bind_once_without_rewriting_originals(old in "[a-z]{1,20}", fresh in "[A-Z]{1,20}") {
            // These are internal binder DTOs, not provider/OS wire fixtures.
            let before = json!({"generation":old,"literal":{"$request":"/never-evaluate"}});
            let after = json!({"generation":fresh,"literal":{"$request":"/never-evaluate"}});
            let original = json!({"catalog":json!({"source":before}).to_string()});
            let request = json!({"catalog":json!({"source":after}).to_string()});
            let decoded = call(before.clone());
            let policy = policy();
            let bound = bind_selectors(&policy,&original,&decoded,Some(&request)).unwrap();
            prop_assert_eq!(&bound.tools[0].arguments["source"],&after);
            prop_assert_eq!(&decoded.tools[0].arguments["source"],&before);
            prop_assert_ne!(&decoded.tools[0].arguments["source"],&after,
                "negative: omitting binding retains the stale selector");
            prop_assert!(bind_selectors(&policy,&original,&call(after),Some(&request)).is_err());
        }
    }

    #[test]
    fn stale_ambiguous_overlapping_and_missing_capture_bindings_refuse() {
        let source = json!({"generation":"retained"});
        let original = json!({"catalog":json!({"source":source}).to_string()});
        let decoded = call(source);
        let policy = policy();
        assert!(bind_selectors(&policy, &original, &decoded, None).is_ok());
        let mut duplicate = decoded.clone();
        duplicate.tools.extend(decoded.tools.clone());
        assert!(bind_selectors(&policy, &original, &duplicate, None).is_err());
        assert!(bind_selectors(&policy, &original, &Step::default(), None).is_err());
        let mut overlapping = policy.clone();
        overlapping.bindings.extend(policy.bindings.clone());
        assert!(bind_selectors(&overlapping, &original, &decoded, None).is_err());
        let mut wrong = policy.clone();
        wrong.bindings[0].pointer = "/absent".into();
        assert!(bind_selectors(&wrong, &original, &decoded, None).is_err());
        let mut mutated = policy;
        mutated.bindings[0]
            .captured
            .set
            .insert("/generation".into(), json!("other"));
        assert!(bind_selectors(&mutated, &original, &decoded, None).is_err());
    }

    #[test]
    fn nested_approval_selectors_share_the_file_binding_contract() {
        // Internal request/argument DTOs, not fabricated external provider bytes.
        let before = json!({"task":"old-task","request":"old-request",
            "request_sha256":"old-digest","parent":{"principal":"old-parent"}});
        let after = json!({"task":"fresh-task","request":"fresh-request",
            "request_sha256":"fresh-digest","parent":{"principal":"fresh-parent"}});
        let original = json!({"approval":before.to_string()});
        let request = json!({"approval":after.to_string()});
        let decoded = Step {
            tools: vec![Tool {
                id: "approval-call".into(),
                name: "review".into(),
                namespace: None,
                arguments: before.clone(),
            }],
            ..Step::default()
        };
        let mut projection = RecordingProjection {
            maximum_event_bytes: 65536,
            bindings: vec![],
        };
        for pointer in ["/task", "/request", "/request_sha256", "/parent/principal"] {
            projection.bindings.push(CapturedBinding {
                call_id: "approval-call".into(),
                name: "review".into(),
                pointer: pointer.into(),
                captured: JsonSelection {
                    text: json!({"$request":"/approval"}),
                    pointer: pointer.into(),
                    set: Default::default(),
                },
                replacement: json!({"$json":{"text":{"$request":"/approval"},"pointer":pointer}}),
            });
        }
        let bound = bind_selectors(&projection, &original, &decoded, Some(&request)).unwrap();
        assert_eq!(bound.tools[0].arguments, after);
        assert_eq!(decoded.tools[0].arguments, before);
        assert!(bind_selectors(&projection, &original, &decoded, Some(&json!({}))).is_err());
        projection.bindings[0].captured.pointer = "/request".into();
        assert!(bind_selectors(&projection, &original, &decoded, Some(&request)).is_err());
    }

    #[test]
    fn a_declared_projection_cannot_fall_back_to_an_unbound_reply() {
        let step = Step {
            recording_projection: Some(policy()),
            text: vec!["unbound".into()],
            ..Step::default()
        };
        assert!(crate::reply_with_usage("responses", &step, 0).is_none());
        let ordinary = Step {
            recording_projection: None,
            ..step
        };
        assert!(crate::reply_with_usage("responses", &ordinary, 0).is_some());
    }
}

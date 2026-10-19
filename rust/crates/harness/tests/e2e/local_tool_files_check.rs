//! Pure identity, payload and output verification shared with the fuzz target.
use crate::TestResult;
use acyclic_harness::{
    OperationId, TaskId,
    model::ToolCall,
    task_volume::{Component, FileKey, HeadFile, TaskFile, ToolFile},
    tool::ToolInvocation,
};
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) fn check(
    files: &BTreeMap<FileKey, HeadFile>,
    task: TaskId,
    operation: OperationId,
    calls: &[(u64, ToolCall)],
    results: &BTreeMap<String, Value>,
) -> TestResult {
    let task = Component::new(task.to_string())?;
    let mut expected = BTreeMap::new();
    for (step, call) in calls {
        let invocation = ToolInvocation::for_model_call(
            operation,
            u32::try_from(*step)?,
            call.call_id.clone(),
            call.name.clone(),
            call.arguments.clone(),
            None,
        )?;
        let identity = Component::new(invocation.operation_id.to_string())?;
        let result = results
            .get(&call.call_id)
            .ok_or("missing expected bash result")?;
        let value = result["content"]
            .get("value")
            .ok_or("missing expected bash value")?;
        if expected
            .insert(identity, (*step, serde_json::to_value(invocation)?, value))
            .is_some()
        {
            return Err("duplicate expected bash identity".into());
        }
    }
    let mut requests = 0;
    let mut completed = 0;
    let mut output: BTreeMap<_, (u64, Vec<u8>)> = BTreeMap::new();
    let mut step_turns = BTreeMap::new();
    let mut turn_steps = BTreeMap::new();
    for (key, value) in files {
        if key != &value.key {
            return Err("task tool map and file identities differ".into());
        }
        let FileKey::Task {
            task: owner,
            file: TaskFile::Tool { turn, call, file },
        } = key
        else {
            continue;
        };
        let (step, request, result) = expected.get(call).ok_or("unexpected task tool identity")?;
        // Turns are accepted publication ordinals, not raw model steps. The
        // full derived call identity binds the original operation and step;
        // all of its files and every peer call in that step share one turn.
        if step_turns
            .insert(*step, *turn)
            .is_some_and(|old| old != *turn)
            || turn_steps
                .insert(*turn, *step)
                .is_some_and(|old| old != *step)
        {
            return Err("task tool model-turn grouping differs".into());
        }
        if owner != &task || value.channel.is_some() {
            return Err("task tool file lost its owning task or channel".into());
        }
        let [bytes] = value.segments.as_slice() else {
            return Err("tool file must retain its single immutable segment".into());
        };
        if value.length != bytes.len() as u64 {
            return Err("tool file length differs from its segment".into());
        }
        match file {
            ToolFile::Request => {
                if serde_json::from_slice::<Value>(bytes)? != *request {
                    return Err("published tool request payload differs".into());
                }
                requests += 1;
            }
            ToolFile::Result => {
                if serde_json::from_slice::<Value>(bytes)? != **result {
                    return Err("published tool result differs".into());
                }
                completed += 1;
            }
            ToolFile::Stdout(sequence) | ToolFile::Stderr(sequence) => {
                if bytes.is_empty() {
                    return Err("empty task output publication".into());
                }
                let channel = if matches!(file, ToolFile::Stdout(_)) {
                    "stdout"
                } else {
                    "stderr"
                };
                let (next, assembled) = output.entry((*turn, call.clone(), channel)).or_default();
                if *sequence != *next {
                    return Err("task output sequence has a gap".into());
                }
                *next += 1;
                assembled.extend_from_slice(bytes);
            }
            ToolFile::Arguments(_) => return Err("unexpected bash argument fragment".into()),
        }
    }
    if requests != expected.len() || completed != expected.len() {
        return Err("missing published tool request or result".into());
    }
    let mut previous = None;
    for turn in step_turns.values() {
        if previous.is_some_and(|old| old >= *turn) {
            return Err("task tool model turns are not in accepted order".into());
        }
        previous = Some(*turn);
    }
    for (call, (step, _, result)) in expected {
        let turn = step_turns[&step];
        for channel in ["stdout", "stderr"] {
            let expected = result[channel]
                .as_str()
                .ok_or("missing expected bash output")?;
            let actual = output
                .remove(&(turn, call.clone(), channel))
                .unwrap_or_default()
                .1;
            if actual != expected.as_bytes() {
                return Err("published task output bytes differ".into());
            }
        }
    }
    Ok(())
}

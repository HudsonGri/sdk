use super::TestResult;
use acyclic_harness::{
    OperationId, TaskId,
    model::ToolCall,
    task_volume::{Component, FileKey, HeadFile, TaskFile, ToolFile},
    tool::ToolInvocation,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

type Files = BTreeMap<FileKey, HeadFile>;
type Calls = Vec<(u64, ToolCall)>;
type Results = BTreeMap<String, Value>;

fn head_file(key: FileKey, bytes: Vec<u8>) -> HeadFile {
    HeadFile {
        key,
        channel: None,
        publication: None,
        length: bytes.len() as u64,
        segments: vec![bytes.into()],
        through_sequence: None,
    }
}

pub(super) fn fixture(
    task: TaskId,
    operation: OperationId,
    stdout: &str,
    stderr: &str,
) -> TestResult<(Files, Calls, Results)> {
    grouped_fixture(task, operation, &[(0, 17, "call")], stdout, stderr)
}

fn grouped_fixture(
    task: TaskId,
    operation: OperationId,
    groups: &[(u64, u64, &str)],
    stdout: &str,
    stderr: &str,
) -> TestResult<(Files, Calls, Results)> {
    let mut files = BTreeMap::new();
    let mut calls = Vec::new();
    let mut results = BTreeMap::new();
    for (step, turn, name) in groups {
        let call = ToolCall::new((*name).into(), "bash".into(), json!({"command":"unused"}));
        let invocation = ToolInvocation::for_model_call(
            operation,
            u32::try_from(*step)?,
            call.call_id.clone(),
            call.name.clone(),
            call.arguments.clone(),
            None,
        )?;
        let key = |file| {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(FileKey::Task {
                task: Component::new(task.to_string())?,
                file: TaskFile::Tool {
                    turn: *turn,
                    call: Component::new(invocation.operation_id.to_string())?,
                    file,
                },
            })
        };
        let value =
            json!({"stdout":stdout,"stderr":stderr,"exit_code":0,"success":true,"stop":null});
        for (file, bytes) in [
            (ToolFile::Request, serde_json::to_vec(&invocation)?),
            (ToolFile::Result, serde_json::to_vec(&value)?),
            (ToolFile::Stdout(0), stdout.as_bytes().to_vec()),
            (ToolFile::Stderr(0), stderr.as_bytes().to_vec()),
        ] {
            if !bytes.is_empty() {
                let key = key(file)?;
                files.insert(key.clone(), head_file(key, bytes));
            }
        }
        calls.push((*step, call));
        results.insert(
            (*name).into(),
            json!({"name":"bash","content":{"kind":"json","value":value}}),
        );
    }
    Ok((files, calls, results))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_tool_files_check::check;
    use proptest::prelude::*;

    fn reassign(files: &Files, call: &Component, turn: u64) -> Files {
        files
            .iter()
            .map(|(key, value)| {
                let mut key = key.clone();
                if let FileKey::Task {
                    file:
                        TaskFile::Tool {
                            turn: old,
                            call: identity,
                            ..
                        },
                    ..
                } = &mut key
                    && identity == call
                {
                    *old = turn;
                }
                let mut value = value.clone();
                value.key = key.clone();
                (key, value)
            })
            .collect()
    }

    #[test]
    fn accepted_ordinals_preserve_full_call_identity_and_step_groups() -> TestResult {
        let task = TaskId::from_bytes([41; 16]);
        let operation = OperationId::from_bytes([42; 16]);
        let (files, calls, results) = grouped_fixture(
            task,
            operation,
            &[(0, 17, "first"), (0, 17, "peer"), (1, 31, "next")],
            "out",
            "err",
        )?;
        check(&files, task, operation, &calls, &results)?;
        let identity = |index: usize| -> TestResult<Component> {
            let (step, call) = &calls[index];
            Ok(Component::new(
                ToolInvocation::for_model_call(
                    operation,
                    u32::try_from(*step)?,
                    call.call_id.clone(),
                    call.name.clone(),
                    call.arguments.clone(),
                    None,
                )?
                .operation_id
                .to_string(),
            )?)
        };
        // One step cannot split across ordinals; different steps cannot share
        // an ordinal or reverse their accepted order, even with exact payloads.
        for (index, turn, message) in [
            (1, 18, "model-turn grouping differs"),
            (2, 17, "model-turn grouping differs"),
            (2, 11, "not in accepted order"),
        ] {
            let broken = reassign(&files, &identity(index)?, turn);
            let error = check(&broken, task, operation, &calls, &results)
                .expect_err("invalid accepted-turn group passed");
            assert!(error.to_string().contains(message), "{error}");
        }
        // A peer operation can reuse both step 0 and the provider call ID. Its
        // genuinely encoded request still cannot replace one expected call.
        let (foreign, _, _) = grouped_fixture(
            task,
            OperationId::from_bytes([43; 16]),
            &[(0, 17, "first")],
            "out",
            "err",
        )?;
        let first = identity(0)?;
        let mut mixed = files.clone();
        mixed.retain(|key, _| {
            !matches!(key, FileKey::Task {
            file: TaskFile::Tool { call, .. }, ..
        } if call == &first)
        });
        mixed.extend(foreign);
        let error = check(&mixed, task, operation, &calls, &results)
            .expect_err("foreign operation reused an expected raw step and call ID");
        assert!(
            error.to_string().contains("unexpected task tool identity"),
            "{error}"
        );
        Ok(())
    }

    proptest! {
        #[test]
        fn task_tool_wire_binds_payload_output_sequence_and_terminal(
            identity in any::<[u8;16]>(), stdout in ".{0,24}", stderr in ".{0,24}",
        ) {
            let task = TaskId::from_bytes(identity);
            let operation = OperationId::from_bytes(identity);
            let (files,calls,results) = fixture(task,operation,&stdout,&stderr)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert!(check(&files,task,operation,&calls,&results).is_ok());
            let request = files.keys().find(|key| matches!(key, FileKey::Task {file:TaskFile::Tool {file:ToolFile::Request,..},..})).unwrap();
            let result = files.keys().find(|key| matches!(key, FileKey::Task {file:TaskFile::Tool {file:ToolFile::Result,..},..})).unwrap();
            for field in ["name","arguments"] {
                let mut broken = files.clone();
                let mut payload: Value = serde_json::from_slice(&broken[request].segments[0])?;
                if field == "name" {payload[field]="fs".into();}
                else {payload[field]["injected"]=true.into();}
                broken.insert(request.clone(),head_file(request.clone(),serde_json::to_vec(&payload)?));
                prop_assert!(check(&broken,task,operation,&calls,&results).is_err());
            }
            let mut missing = files.clone();missing.remove(result);
            prop_assert!(check(&missing,task,operation,&calls,&results).is_err());
            let mut broken = files.clone();
            let mut payload: Value = serde_json::from_slice(&broken[result].segments[0])?;
            payload["injected"]=true.into();
            broken.insert(result.clone(),head_file(result.clone(),serde_json::to_vec(&payload)?));
            prop_assert!(check(&broken,task,operation,&calls,&results).is_err());
            let FileKey::Task {task:owner,file:TaskFile::Tool {turn,call,..}}=request else {unreachable!()};
            let mut changed=stdout.as_bytes().to_vec();changed.push(b'X');
            for (sequence,bytes) in [(0,changed),(1,Vec::new())] {
                let mut broken=files.clone();
                let key=FileKey::Task {task:owner.clone(),file:TaskFile::Tool {turn:*turn,call:call.clone(),file:ToolFile::Stdout(sequence)}};
                broken.insert(key.clone(),head_file(key,bytes));
                prop_assert!(check(&broken,task,operation,&calls,&results).is_err());
            }
            let mut duplicate=calls.clone();duplicate.push(calls[0].clone());
            prop_assert!(check(&files,task,operation,&duplicate,&results).is_err());
            let mut foreign=identity;foreign[0]^=1;
            prop_assert!(check(&files,TaskId::from_bytes(foreign),operation,&calls,&results).is_err());
            prop_assert!(check(&files,task,OperationId::from_bytes(foreign),&calls,&results).is_err());
        }
    }
}

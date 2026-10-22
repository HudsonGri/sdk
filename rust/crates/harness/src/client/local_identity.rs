//! Launch identity is an ordinary file in the authorized workspace private volume.
use crate::task_volume::{Component, FileKey, FileValue, Query, TaskFile};
use crate::{Error, OperationId, Result, local::LocalHarness};

/// Reopen a retained session by clearing only its presentation exit flag.
/// Selection, prompts, outcomes and admission identities remain in their files.
pub async fn resume_local_identity(
    local: &LocalHarness,
    explicit: Option<OperationId>,
) -> Result<OperationId> {
    let files = local.task_files();
    let operation = match explicit {
        Some(operation) => operation,
        None => {
            let key = FileKey::parse(".ui/workspace/interactive.json")?;
            let saved = files.snapshot(&Query::File(key), 1).await?;
            let file = saved
                .files
                .first()
                .ok_or_else(|| Error::NotFound("interactive conversation".into()))?;
            serde_json::from_slice(&file.bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?
        }
    };
    let key = FileKey::parse(&format!(".ui/session-{operation}/layout.json"))?;
    loop {
        let saved = files.snapshot(&Query::File(key.clone()), 1).await?;
        let file = saved
            .files
            .first()
            .ok_or_else(|| Error::NotFound("interactive session layout".into()))?;
        let mut layout: serde_json::Value = serde_json::from_slice(&file.bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let object = layout
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("session layout must be an object".into()))?;
        if object.remove("exit_requested").is_none() {
            return Ok(operation);
        }
        let value = FileValue::new(
            key.clone(),
            serde_json::to_vec(&layout).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        match files.write(saved.generation, &value).await {
            Ok(_) => return Ok(operation),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

/// Reopen an unfinished launch; completed prompts allocate a fresh request.
/// The private volume supplies the workspace identity. Prompt bytes supply the
/// remaining input identity; argv identities are only explicit overrides.
pub async fn local_identity(
    local: &LocalHarness,
    prompt: Option<&str>,
    explicit: Option<OperationId>,
) -> Result<OperationId> {
    if let Some(operation) = explicit {
        return Ok(operation);
    }
    let name = prompt.map_or_else(
        || "interactive.json".to_owned(),
        |text| format!("prompt-{}.json", blake3::hash(text.as_bytes()).to_hex()),
    );
    let key = FileKey::Ui {
        directory: Component::new("workspace")?,
        name: Component::new(name)?,
    };
    let files = local.task_files();
    loop {
        let snapshot = files.snapshot(&Query::File(key.clone()), 1).await?;
        if let Some(file) = snapshot.files.first() {
            let operation: OperationId = serde_json::from_slice(&file.bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let session = Component::new(operation.to_string())?;
            let completed = if prompt.is_some() {
                let result = FileKey::Session {
                    session,
                    file: TaskFile::ExecutionResult(Component::new(operation.to_string())?),
                };
                !files
                    .snapshot(&Query::File(result), 1)
                    .await?
                    .files
                    .is_empty()
            } else {
                let layout = FileKey::Ui {
                    directory: Component::new(format!("session-{operation}"))?,
                    name: Component::new("layout.json")?,
                };
                files
                    .snapshot(&Query::File(layout), 1)
                    .await?
                    .files
                    .first()
                    .map(|file| serde_json::from_slice::<serde_json::Value>(&file.bytes))
                    .transpose()
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .is_some_and(|layout| layout["exit_requested"] == true)
            };
            if !completed {
                return Ok(operation);
            }
        }
        let operation = OperationId::new();
        let value = FileValue::new(
            key.clone(),
            serde_json::to_vec(&operation).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        match files.write(snapshot.generation, &value).await {
            Ok(_) => return Ok(operation),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

use super::*;
use crate::{
    task_volume::{Component, FileKey, TaskFile, ToolFile, Write},
    tool::{RecordedInvocation, ToolInvocation, ToolResult},
};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn publish_tool_value<T: Serialize>(
        &self,
        operation: OperationId,
        step: u32,
        call: &str,
        leaf: ToolFile,
        value: &T,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let Some(files) = &self.task_files else {
            return Ok(());
        };
        let (owner, _) = self.owner.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "tool publication requires its owner",
            )
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
        let identity = ToolInvocation::model_operation(operation, step, call, work)?;
        let turn = <Self as ExecutionJournal>::model_file_turn(self, operation, step, io)?.await?;
        let task = Component::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?)?;
        let call = Component::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{identity}"), work)
        })?)?;
        // Match the existing native tool request/result byte representation so
        // its later publication is an identical immutable retry.
        let bytes = match io {
            Some(io) => io.encode(value, files.maximum_bytes(), false)?,
            None => serde_json::to_vec(value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .into(),
        };
        let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if let ToolFile::Arguments(sequence) = leaf {
            for (part, offset) in (0..bytes.len())
                .step_by(crate::task_volume::MODEL_CHUNK_BYTES)
                .enumerate()
            {
                let end = (offset + crate::task_volume::MODEL_CHUNK_BYTES).min(bytes.len());
                let part = u64::try_from(part).map_err(|_| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "tool argument part overflow",
                    )
                })?;
                if part >= crate::task_volume::MODEL_CHUNK_STRIDE {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "tool argument part overflow",
                    ));
                }
                let write = Write::new(
                    FileKey::Task {
                        task: task.clone(),
                        file: TaskFile::Tool {
                            turn,
                            call: call.clone(),
                            file: ToolFile::Arguments(sequence + part),
                        },
                    },
                    bytes.slice(offset..end),
                );
                self.publish_model_file(&write, io).await?;
            }
            return Ok(());
        }
        let write = Write::new(
            FileKey::Task {
                task,
                file: TaskFile::Tool {
                    turn,
                    call,
                    file: leaf,
                },
            },
            bytes,
        );
        self.publish_model_file(&write, io).await
    }

    pub(super) async fn publish_tool_event_file(
        &self,
        operation: OperationId,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if self.task_files.is_none() {
            return Ok(());
        }
        match event {
            ExecutionEvent::ToolStarted {
                step,
                call_id,
                invocation,
            } => {
                let (record, _hold) =
                    crate::executor::load_json::<RecordedInvocation>(self, invocation, io).await?;
                self.publish_tool_value(
                    operation,
                    *step,
                    call_id,
                    ToolFile::Request,
                    &record.invocation,
                    io,
                )
                .await
            }
            ExecutionEvent::ToolCompleted {
                step,
                call_id,
                result,
                ..
            } => {
                let (result, _hold) =
                    crate::executor::load_json::<ToolResult>(self, result, io).await?;
                self.publish_tool_value(
                    operation,
                    *step,
                    call_id,
                    ToolFile::Result,
                    &result.value,
                    io,
                )
                .await
            }
            ExecutionEvent::ToolFailed {
                step,
                call_id,
                reason,
            } => {
                #[derive(Serialize)]
                struct Failure<'a> {
                    error: &'a str,
                }
                self.publish_tool_value(
                    operation,
                    *step,
                    call_id,
                    ToolFile::Error,
                    &Failure {
                        error: reason.message(),
                    },
                    io,
                )
                .await
            }
            _ => Ok(()),
        }
    }

    pub(super) async fn publish_tool_arguments(
        &self,
        operation: OperationId,
        step: u32,
        sequence: usize,
        call: &crate::model::ToolCall,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        // ModelEvent::ToolCall currently arrives as one complete argument value.
        let sequence = u64::try_from(sequence)
            .ok()
            .and_then(|sequence| sequence.checked_mul(crate::task_volume::MODEL_CHUNK_STRIDE))
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "tool argument sequence overflow",
                )
            })?;
        self.publish_tool_value(
            operation,
            step,
            &call.call_id,
            ToolFile::Arguments(sequence),
            &call.arguments,
            io,
        )
        .await
    }
}

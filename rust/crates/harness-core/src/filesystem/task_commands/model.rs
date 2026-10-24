use super::*;
use crate::conversation::BoundedContentFuture;
use crate::executor::{ExecutionProgress, TurnOutput};
use crate::filesystem::task_runtime::observed::boundary;
use crate::filesystem::{FilesystemExecutionJournal, PreparationIo};
use crate::wire::ErrorCode;

struct ModelRun<P, A, O> {
    context: Arc<TaskContext>,
    command: ModelTaskCommand,
    steering: Option<(crate::workflow::WorkflowRecord, OperationId)>,
    journal: Option<Arc<FilesystemExecutionJournal<P, A, O>>>,
    operation: OperationId,
}

impl<P, A, O> ModelRun<P, A, O> {
    fn journal(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<&Arc<FilesystemExecutionJournal<P, A, O>>> {
        self.journal
            .as_ref()
            .ok_or_else(|| boundary(io, ErrorCode::Storage, "model journal is absent"))
    }
}

struct ModelExecution<P, A, O> {
    context: Arc<TaskContext>,
    journal: Option<Arc<FilesystemExecutionJournal<P, A, O>>>,
    operation: OperationId,
}

impl<P, A, O> ModelExecution<P, A, O> {
    fn journal(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<&Arc<FilesystemExecutionJournal<P, A, O>>> {
        self.journal
            .as_ref()
            .ok_or_else(|| boundary(io, ErrorCode::Storage, "model journal is absent"))
    }
}

struct PreparedModel {
    pipeline: ContextPipeline,
    tools: crate::tool::ToolRegistry,
    #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
    hooks: Option<Arc<crate::hooks::BoundHooks>>,
    input: TurnInput,
    conversation_id: crate::ConversationId,
    summary_source: Option<Box<FileRef>>,
    role: Option<String>,
    selected_output_volume: Option<crate::conversation::VolumeRef>,
    max_output_tokens: Option<u32>,
}

type ModelPreparation = std::ops::ControlFlow<String, PreparedModel>;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn model_command(
        &self,
        invocation: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let (mut run, binding) = self.prepare_model_run(invocation, payload).await?;
        let CommandInvocation {
            fence,
            reconciliation_only,
            io,
            ..
        } = invocation;
        if reconciliation_only
            && !was_dispatched(
                run.journal(io)?.as_ref(),
                run.operation,
                MODEL_TASK_COMMAND_KIND,
                io,
            )
            .await?
        {
            self.reconcile_prompt_hooks(invocation, &run.context, run.operation)
                .await?;
            return mail::reconciled(io);
        }
        if let Some(steering) = run.steering.take() {
            BoundedContentFuture::new(io, || {
                self.steer(
                    &run.context,
                    fence,
                    &mut run.command,
                    steering,
                    reconciliation_only,
                    io,
                )
            })?
            .await?;
        }
        if let Some(io) = io {
            io.retain_owned(
                std::mem::size_of::<TurnInput>()
                    .saturating_sub(std::mem::size_of::<ModelTaskCommand>()) as u64,
            )?;
        }
        let command = run.command;
        let prepared = match self
            .prepare_model_input(invocation, &run.context, run.operation, command)
            .await?
        {
            ModelPreparation::Continue(prepared) => prepared,
            ModelPreparation::Break(message) => {
                return ready(Outcome::<Value>::Failed { message }, io);
            }
        };
        // The command has moved into the prepared input. Retain only the journal
        // and authenticated task context across execution.
        let execution = ModelExecution {
            context: run.context,
            journal: run.journal,
            operation: run.operation,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            return self
                .execute_native_model(invocation, execution, prepared, binding, io)
                .await;
        }
        self.execute_model(invocation, execution, prepared, binding)
            .await
    }

    async fn prepare_model_run(
        &self,
        invocation: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<(ModelRun<P, A, O>, &ModelBinding)> {
        let CommandInvocation {
            context,
            admission,
            fence,
            command: workflow,
            reconciliation_only,
            io,
        } = invocation;
        let task = admission.task_id();
        let context = if io.is_none() && context.owned_mailbox.is_none() {
            Arc::new(self.runtime.task_context(task, fence.clone()).await?)
        } else {
            Arc::clone(context)
        };
        let turn = workflow.operation_id;
        let command: ModelTaskCommand = mail::decode_command(payload, io)?;
        if command
            .role
            .as_deref()
            .is_some_and(|role| role.trim().is_empty())
        {
            return Err(boundary(io, ErrorCode::Invalid, "agent role is empty"));
        }
        let binding = self.model.as_ref().ok_or_else(|| {
            boundary(
                io,
                ErrorCode::Unsupported,
                "model command provider is not bound",
            )
        })?;
        let stock = admission.task.name == "acyclic.stock_turn"
            || super::super::child_agent::is_child(admission);
        let steering = self
            .model_steering(invocation, &context, &command, stock)
            .await?;
        let (journal, operation) = if io.is_some() || reconciliation_only {
            let (journal, operation) = BoundedContentFuture::new(io, || {
                self.command_journal(task, fence, turn, &context, io)
            })?
            .await?;
            (Some(journal), operation)
        } else {
            (
                None,
                crate::filesystem::task_runtime::execution_operation(task, turn, io)?,
            )
        };
        Ok((
            ModelRun {
                context,
                command,
                steering,
                journal,
                operation,
            },
            binding,
        ))
    }

    async fn model_steering(
        &self,
        invocation: CommandInvocation<'_>,
        context: &Arc<TaskContext>,
        command: &ModelTaskCommand,
        stock: bool,
    ) -> Result<Option<(crate::workflow::WorkflowRecord, OperationId)>> {
        let CommandInvocation {
            admission,
            fence,
            command: workflow,
            io,
            ..
        } = invocation;
        let task = admission.task_id();
        let turn = workflow.operation_id;
        Ok(if stock || command.mailbox_operation.is_some() {
            BoundedContentFuture::new(io, || async {
                let journal = self
                    .command_workflow_journal(context, task, fence, io)
                    .await?;
                let latest = journal.observe_latest_record(io).await?.ok_or_else(|| {
                    boundary(io, ErrorCode::Unauthorized, "model checkpoint is absent")
                })?;
                if !latest.transition.commands.iter().any(|command| {
                    command.operation_id == turn && command.kind == MODEL_TASK_COMMAND_KIND
                }) {
                    return Err(boundary(
                        io,
                        ErrorCode::Unauthorized,
                        "model is outside the committed outbox",
                    ));
                }
                let recorded = if stock
                    && latest
                        .transition
                        .state
                        .get("mail")
                        .is_some_and(|mail| !mail.is_null())
                {
                    Some(
                        OperationId::deserialize(
                            latest
                                .transition
                                .state
                                .get("receive")
                                .and_then(|receive| receive.get("operation_id"))
                                .unwrap_or(&Value::Null),
                        )
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    )
                } else {
                    None
                };
                Ok(command
                    .mailbox_operation
                    .or(recorded)
                    .map(|receive| (latest, receive)))
            })?
            .await?
        } else {
            None
        })
    }

    async fn reconcile_prompt_hooks(
        &self,
        invocation: CommandInvocation<'_>,
        context: &TaskContext,
        operation: OperationId,
    ) -> Result<()> {
        #[cfg(not(all(feature = "hooks", not(target_arch = "wasm32"))))]
        let _ = (invocation, context, operation);
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        {
            let CommandInvocation {
                admission,
                fence,
                io,
                ..
            } = invocation;
            let task = admission.task_id();
            if io.is_none()
                && let Some(executor) = &context.harness.workspace_tools
            {
                // Hook dispatch precedes provider dispatch. Cancellation must
                // retain a claimed hook even when no model request was sent.
                let owned = Arc::new(self.runtime.task_context(task, fence.clone()).await?);
                for event in crate::hooks::HookEvent::at(crate::hooks::HookSite::Prompt) {
                    let issued = ToolContext::new(
                        Arc::clone(&owned),
                        operation,
                        super::hooks::prompt_hook_call(event),
                        None,
                    )?;
                    executor
                        .run_hooks(
                            (&crate::hooks::Hooks::default(), [0; 32]),
                            event,
                            (&Value::Null).into(),
                            &crate::tool::ToolRegistry::default(),
                            issued,
                            true,
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn prepare_model_input(
        &self,
        invocation: CommandInvocation<'_>,
        context: &Arc<TaskContext>,
        operation: OperationId,
        command: ModelTaskCommand,
    ) -> Result<ModelPreparation> {
        let io = invocation.io;
        let task = invocation.admission.task_id();
        #[cfg(not(all(feature = "hooks", not(target_arch = "wasm32"))))]
        let _ = operation;
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        let manifest_digest = *command.context_manifest.descriptor().sha256();
        let (pipeline, tools) = BoundedContentFuture::new(io, || async {
            if command.summary_source.is_some() {
                Ok((
                    crate::context::ContextPipeline::default(),
                    crate::tool::ToolRegistry::default(),
                ))
            } else {
                self.model_context(command.context_manifest, context, io)
                    .await
            }
        })?
        .await?;
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        let (pipeline, hooks) = if command.summary_source.is_none() {
            match BoundedContentFuture::new(io, || {
                self.prompt_hooks(
                    pipeline,
                    &tools,
                    Arc::clone(context),
                    (
                        invocation.fence,
                        operation,
                        invocation.reconciliation_only,
                        manifest_digest,
                    ),
                    &command.input,
                    io,
                )
            })?
            .await?
            {
                super::hooks::PromptHooks::Ready(pipeline, hooks) => (pipeline, hooks),
                super::hooks::PromptHooks::Blocked(message) => {
                    return Ok(ModelPreparation::Break(message));
                }
            }
        } else {
            (pipeline, None)
        };
        let input = TurnInput {
            operation_id: operation,
            input: command.input,
            selected_context: command.selected_context,
            max_steps: command.max_steps,
        };
        let conversation_id = command
            .conversation_id
            .unwrap_or_else(|| crate::ConversationId::from_bytes(task.into_bytes()));
        Ok(ModelPreparation::Continue(PreparedModel {
            pipeline,
            tools,
            #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
            hooks,
            input,
            conversation_id,
            summary_source: command.summary_source,
            role: command.role,
            selected_output_volume: command.selected_output_volume,
            max_output_tokens: command.max_output_tokens,
        }))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn execute_native_model(
        &self,
        invocation: CommandInvocation<'_>,
        run: ModelExecution<P, A, O>,
        prepared: PreparedModel,
        binding: &ModelBinding,
        io: &PreparationIo,
    ) -> Result<TaskCommandProgress> {
        let CommandInvocation {
            admission,
            fence,
            reconciliation_only,
            ..
        } = invocation;
        let task = admission.task_id();
        let PreparedModel {
            pipeline,
            tools,
            input,
            conversation_id,
            summary_source,
            role,
            selected_output_volume,
            max_output_tokens,
            ..
        } = prepared;
        let ModelExecution {
            context,
            journal,
            operation,
        } = run;
        let compaction = if summary_source.is_some() {
            &crate::context::CompactionPolicy::Disabled
        } else {
            &binding.compaction
        };
        BoundedContentFuture::new(Some(io), || async {
            let work = io.original_native_work().ok_or_else(|| {
                boundary(
                    Some(io),
                    ErrorCode::Unsupported,
                    "model requires original native work",
                )
            })?;
            let context = match &selected_output_volume {
                Some(volume) => context.with_output_volume(volume).await?,
                None => context,
            };
            let journal = journal
                .as_ref()
                .ok_or_else(|| boundary(Some(io), ErrorCode::Storage, "model journal is absent"))?;
            let executor = self
                .runtime
                .stock_executor_with_native_work(
                    task,
                    fence,
                    operation,
                    &context,
                    &binding.model,
                    binding.provider_for(&context),
                    &pipeline,
                    tools,
                    journal,
                    work,
                    compaction,
                    max_output_tokens,
                )?
                .with_conversation_id(conversation_id)
                .with_summary_source(summary_source)
                .with_role(role);
            let executor = if reconciliation_only {
                executor.into_reconciliation()
            } else {
                executor
            };
            let result = executor
                .execute_progress_with_native_context(&context, &input, journal.as_ref(), work)?
                .await;
            drop(executor);
            drop(context);
            if reconciliation_only {
                return Self::reconcile_model(result.map(drop), journal, operation, Some(io)).await;
            }
            let (progress, custody) = result?;
            io.absorb_native_custody(custody)?;
            Self::model_progress(progress, Some(io))
        })?
        .await
    }

    async fn execute_model(
        &self,
        invocation: CommandInvocation<'_>,
        run: ModelExecution<P, A, O>,
        prepared: PreparedModel,
        binding: &ModelBinding,
    ) -> Result<TaskCommandProgress> {
        let CommandInvocation {
            command: workflow,
            fence,
            reconciliation_only,
            io,
            ..
        } = invocation;
        let turn = workflow.operation_id;
        let operation = run.operation;
        let context = &run.context;
        let PreparedModel {
            pipeline,
            tools,
            input,
            conversation_id,
            summary_source,
            role,
            selected_output_volume,
            max_output_tokens,
            #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
            hooks,
        } = prepared;
        let compaction = if summary_source.is_some() {
            &crate::context::CompactionPolicy::Disabled
        } else {
            &binding.compaction
        };
        BoundedContentFuture::new(io, || async {
            let execution = self
                .runtime
                .stock_execution_with_context(
                    Arc::clone(context),
                    fence.clone(),
                    turn,
                    binding.model.clone(),
                    binding.provider_for(context),
                    pipeline,
                    tools,
                    selected_output_volume.as_ref(),
                )
                .await?
                .with_conversation_id(conversation_id)
                .with_compaction_policy(compaction.clone())
                .with_summary_source(summary_source)
                .with_role(role);
            #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
            let execution = execution.with_hooks(hooks);
            let execution = match max_output_tokens {
                Some(maximum) => execution.with_max_output_tokens(maximum)?,
                None => execution,
            };
            if reconciliation_only {
                let result = execution.reconcile_progress(input).await;
                return Self::reconcile_model(result.map(drop), run.journal(io)?, operation, io)
                    .await;
            }
            Self::model_progress(execution.execute_progress(input).await?, io)
        })?
        .await
    }

    fn model_progress(
        progress: ExecutionProgress<TurnOutput>,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        match progress {
            ExecutionProgress::Ready(output) => ready(Outcome::Succeeded(output), io),
            ExecutionProgress::Pending(_) => Ok(TaskCommandProgress::Pending),
            ExecutionProgress::Rejected(reason) => rejected(reason, io),
            // A recorded tool failure settles the turn with its stable message.
            ExecutionProgress::Failed(reason) => ready(
                Outcome::<Value>::Failed {
                    message: crate::executor::stock_string_copy(reason.message(), io)?,
                },
                io,
            ),
        }
    }

    async fn reconcile_model(
        result: Result<()>,
        journal: &FilesystemExecutionJournal<P, A, O>,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        match result {
            Ok(()) | Err(Error::InteractionRejected(crate::InteractionRejection::Cancelled)) => {}
            Err(Error::Indeterminate(id)) if id == operation => {}
            Err(Error::Indeterminate(_)) => return Ok(TaskCommandProgress::Indeterminate),
            Err(error) => return Err(error),
        }
        if journal.is_quiescent(io).await? {
            mail::reconciled(io)
        } else {
            Ok(TaskCommandProgress::Indeterminate)
        }
    }

    async fn steer(
        &self,
        context: &TaskContext,
        fence: &LeaseFence,
        command: &mut ModelTaskCommand,
        (latest, receive): (crate::workflow::WorkflowRecord, OperationId),
        reconciliation_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let task = context.durable_task_id().ok_or_else(|| {
            boundary(io, ErrorCode::Unauthorized, "steering has no admitted task")
        })?;
        let results = latest
            .input
            .get("commands")
            .and_then(Value::as_array)
            .ok_or_else(|| boundary(io, ErrorCode::Unauthorized, "steering result is absent"))?;
        let [result] = results.as_slice() else {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "steering requires one result",
            ));
        };
        let result: CommittedMailResult = match io {
            Some(io) => crate::filesystem::retain_decoded(
                io.value_decode_preparation(result, ErrorCode::Unauthorized)?,
            )?,
            None => serde_json::from_value(result.clone())
                .map_err(|error| Error::Unauthorized(error.to_string()))?,
        };
        let Outcome::Succeeded(item) = result.value else {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "steering receive did not succeed",
            ));
        };
        drop(latest);
        let reader = context;
        // The durable reader authenticates the committed receive, cursor and original parcel.
        let bytes = reader
            .read_received_mail(
                receive,
                &item,
                &mut io
                    .map(PreparationIo::history_io)
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::history::HistoryIo),
            )
            .await?;
        let operation = super::super::recursive::derive_operation(
            b"acyclic.harness.received-copy.v1\0",
            &[task.into_bytes(), receive.into_bytes()],
            io,
        )?;
        let path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("mailbox/{receive}/parcel"), work)
        })?;
        let _path = io
            .map(|io| io.guard_owned(path.capacity() as u64))
            .transpose()?;
        let (retained, _) = reader.observe_staged_file_once(operation, io).await?;
        let copy = match retained {
            Some(file) => file,
            None if reconciliation_only => {
                return Err(boundary(
                    io,
                    ErrorCode::Storage,
                    "started steered model has no retained receiver parcel",
                ));
            }
            None => {
                self.runtime.owner_observed(task, fence, false, io).await?;
                let (copy, _) = reader
                    .stage_file_once(
                        operation,
                        &path,
                        &bytes,
                        item.payload.descriptor().media_type(),
                        item.payload.display_name(),
                        io,
                    )
                    .await?;
                self.runtime.owner_observed(task, fence, false, io).await?;
                copy
            }
        };
        if copy.path() != path
            || copy.descriptor() != item.payload.descriptor()
            || copy.display_name() != item.payload.display_name()
        {
            return Err(boundary(
                io,
                ErrorCode::Conflict,
                "receiver parcel differs from original mail",
            ));
        }
        let mut parts = Vec::new();
        match std::mem::replace(&mut command.input, ModelContent::Parts(Vec::new())) {
            ModelContent::Parts(existing) => parts = existing,
            ModelContent::Text(text) => crate::executor::stock_vec_push(
                &mut parts,
                crate::model::ModelContentPart::Text { text },
                io,
            )?,
            ModelContent::Part(part) => crate::executor::stock_vec_push(&mut parts, part, io)?,
        }
        crate::executor::stock_vec_push(
            &mut parts,
            crate::model::ModelContentPart::File {
                file: copy,
                policy: crate::model::FileProjectionPolicy::BoundedFull,
            },
            io,
        )?;
        command.input = ModelContent::Parts(parts);
        Ok(())
    }
}

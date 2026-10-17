use super::*;
use crate::{
    context::{Context, ContextPlacement, SourceStage},
    hooks::{BoundHooks, HookEvent, Hooks},
    model::{ModelMessage, ModelRole},
};

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn prompt_hooks(
        &self,
        pipeline: ContextPipeline,
        tools: &crate::tool::ToolRegistry,
        context: Arc<TaskContext>,
        (fence, operation, reconcile_only, digest): (&LeaseFence, OperationId, bool, [u8; 32]),
        prompt: &ModelContent,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<(ContextPipeline, Option<Arc<BoundHooks>>)> {
        if !pipeline
            .configuration_sources()
            .any(|file| file.descriptor().media_type() == Hooks::MEDIA_TYPE)
        {
            return Ok((pipeline, None));
        }
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "hooks require ordinary task execution",
            ));
        }
        let mut hooks = None::<Hooks>;
        for source in pipeline.configuration_sources() {
            if source.descriptor().media_type() != Hooks::MEDIA_TYPE {
                continue;
            }
            let bytes = context.read_file(source, None).await?;
            let next: Hooks = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match &mut hooks {
                Some(hooks) => hooks.append(next),
                None => hooks = Some(next),
            }
        }
        let bound = Arc::new(BoundHooks {
            rules: hooks.unwrap_or_default(),
            manifest: digest,
        });
        let session_id = context.durable_task_id().map(|task| task.to_string());
        let mut messages = Vec::new();
        for (event, input) in [
            (
                HookEvent::SessionStart,
                serde_json::json!({"source":"startup", "session_id": session_id}),
            ),
            (
                HookEvent::UserPromptSubmit,
                serde_json::json!({"prompt": prompt_text(prompt), "session_id": session_id}),
            ),
        ] {
            let output = self
                .prompt_hook(&bound, event, &input, tools, &context, (fence, operation, reconcile_only))
                .await?;
            messages.extend(output.into_iter().map(|text| ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text),
            }));
        }
        if messages.is_empty() {
            return Ok((pipeline, Some(bound)));
        }
        let source = Context {
            messages,
            ..Default::default()
        };
        Ok((
            pipeline.with(Arc::new(SourceStage::new(
                "hooks.prompt",
                "1",
                Arc::new(source),
                ContextPlacement::Prepend,
            ))),
            Some(bound),
        ))
    }

    /// Runs one prompt-scoped event once per model command, retaining its outcome.
    /// Returns only the context it adds; an explicit or failure-policy block rejects
    /// the command before any provider request.
    async fn prompt_hook(
        &self,
        bound: &BoundHooks,
        event: HookEvent,
        input: &serde_json::Value,
        tools: &crate::tool::ToolRegistry,
        context: &Arc<TaskContext>,
        (fence, operation, reconcile_only): (&LeaseFence, OperationId, bool),
    ) -> Result<Vec<String>> {
        if bound.rules.commands(event, input, tools)?.next().is_none() {
            return Ok(Vec::new());
        }
        let executor =
            context.harness.workspace_tools.as_ref().ok_or_else(|| {
                Error::Unsupported("hook workspace execution is not bound".into())
            })?;
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("hook dispatch has no task".into()))?;
        let owned = Arc::new(self.runtime.task_context(task, fence.clone()).await?);
        let issued = ToolContext::new(owned, operation, prompt_hook_call(event), None)?;
        let output = executor
            .run_hooks(
                (&bound.rules, bound.manifest),
                event,
                input.into(),
                tools,
                issued,
                reconcile_only,
            )
            .await?
            .ok_or_else(|| Error::Invalid("started model has no prompt hook outcome".into()))?;
        crate::hooks::report(event, &output);
        let output = output.into_result()?;
        if let Some(reason) = output.blocked {
            return Err(Error::Unauthorized(reason));
        }
        Ok(output.additional_context)
    }
}

/// Distinct retained claim identities for each prompt-scoped event of one command.
pub(super) fn prompt_hook_call(event: HookEvent) -> &'static str {
    match event {
        HookEvent::UserPromptSubmit => "user-prompt-submit",
        _ => "session-start",
    }
}

/// The submitted text a UserPromptSubmit hook inspects; attachments are not inlined.
fn prompt_text(prompt: &ModelContent) -> String {
    match prompt {
        ModelContent::Text(text) => text.clone(),
        content => content
            .parts()
            .iter()
            .filter_map(|part| match part {
                crate::model::ModelContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("
"),
    }
}


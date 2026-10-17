use super::*;
use crate::hooks::{HookEvent, HookInput, HookOutput};

impl StockExecution<'_> {
    pub(super) fn append_hook_context(
        &self,
        messages: &mut Vec<ModelMessage>,
        mut output: HookOutput,
    ) -> Result<()> {
        let blocked = output.take_block_reason();
        for text in blocked.into_iter().chain(output.additional_context) {
            let message = ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text),
            };
            message
                .content
                .validate_limits(self.stock.limits, &mut None)?;
            stock_vec_push(messages, message, self.io)?;
        }
        Ok(())
    }

    pub(super) async fn tool_hook(
        &self,
        operation: OperationId,
        event: HookEvent,
        invocation: &ToolInvocation,
        response: Option<&ToolResult>,
        retained_only: bool,
    ) -> Result<HookOutput> {
        let Some(hooks) = &self.stock.hooks else {
            return Ok(HookOutput::default());
        };
        let input = HookInput::Tool {
            invocation,
            response,
            session_id: self.stock.task.as_ref().map(|(_, task, _)| *task),
        };
        if hooks
            .rules
            .commands(event, input, &self.stock.tools)?
            .next()
            .is_none()
        {
            return Ok(HookOutput::default());
        }
        if self.io.is_some() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Unsupported,
                "hooks require ordinary task execution",
            ));
        }
        let (task, _) = self
            .context
            .ok_or_else(|| Error::Unauthorized("tool hook has no issued task context".into()))?;
        let executor = task.harness.workspace_tools.as_ref().ok_or_else(|| {
            Error::Unsupported("tool hook workspace execution is not bound".into())
        })?;
        let phase = phase_operation(invocation.operation_id, event);
        let context =
            crate::runtime::ToolContext::new(Arc::clone(task), phase, &invocation.call_id, None)?;
        match executor
            .run_hooks(
                (&hooks.rules, hooks.manifest),
                event,
                input,
                &self.stock.tools,
                context,
                retained_only || self.stock.reconciliation_only,
            )
            .await?
        {
            Some(output) => {
                crate::hooks::report(event, &output);
                Ok(output)
            }
            None if retained_only || self.stock.reconciliation_only => {
                Err(Error::Indeterminate(operation))
            }
            None => Err(Error::Invalid(
                "started tool has no retained hook outcome".into(),
            )),
        }
    }

    pub(super) async fn hook_approval(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        invocation: &ToolInvocation,
        tool: &crate::tool::Tool,
        prompt: String,
        started: bool,
    ) -> Result<Option<ExecutionProgress<()>>> {
        let hooks =
            self.stock.hooks.as_ref().ok_or_else(|| {
                Error::Invalid("hook approval has no pinned configuration".into())
            })?;
        let task = self.stock.task.as_ref().map_or(
            TaskId::from_bytes(operation.into_bytes()),
            |(_, task, _)| *task,
        );
        let identity = ComponentIdentity {
            name: "hooks.pre-tool".into(),
            version: "1".into(),
            digest: hooks.manifest,
        };
        let ((approval, request), _hold) = tool_approval_request(
            task,
            &identity,
            &tool.definition,
            invocation,
            (prompt, None),
            None,
        )?;
        self.settle_approval(journal, operation, approval, &request, invocation, started)
            .await
    }
}

pub(crate) fn phase_operation(tool: OperationId, event: HookEvent) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:tool-hook:v1\0");
    hasher.update(&tool.into_bytes());
    hasher.update(event.name().as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(id)
}

use super::*;

impl StockExecution<'_> {
    pub(super) async fn execute_progress(
        &self,
        input: &TurnInput,
        journal: &dyn ExecutionJournal,
    ) -> Result<ExecutionProgress<TurnOutput>> {
        self.verify_task_context_binding(input.operation_id)?;
        self.verify_execution_owner().await?;
        self.validate_turn_input(journal, input).await?;
        if let Some(source) = self.stock.summary_source.as_deref() {
            return crate::conversation::BoundedContentFuture::new(self.io, || {
                self.summarize_file(journal, input, source)
            })?
            .await;
        }
        self.ensure_started(journal, input).await?;
        let mut prior_messages = Vec::new();
        for step in 0..input.max_steps {
            let model_events = self
                .run_model_step(
                    journal,
                    input,
                    step,
                    &prior_messages,
                    ModelPurpose::Response,
                )?
                .await?;
            let (calls, text, metadata) =
                self.collect_step_events(input, step, model_events, &mut prior_messages)?;
            if calls.is_empty() {
                return Ok(ExecutionProgress::Ready(TurnOutput {
                    text,
                    attachments: Vec::new(),
                    metadata,
                    steps: step + 1,
                }));
            }
            #[cfg(feature = "filesystem")]
            let text_hold = self
                .io
                .filter(|_| text.capacity() != 0)
                .map(|io| io.guard_owned(text.capacity() as u64))
                .transpose()?;
            drop(text);
            #[cfg(feature = "filesystem")]
            drop(text_hold);
            for (invocation, invocation_index) in calls {
                match self
                    .resolve_tool_call(
                        journal,
                        input.operation_id,
                        step,
                        invocation,
                        invocation_index,
                        &mut prior_messages,
                    )
                    .await?
                {
                    ExecutionProgress::Ready(()) => {}
                    ExecutionProgress::Pending(wait) => {
                        return Ok(ExecutionProgress::Pending(wait));
                    }
                    ExecutionProgress::Rejected(reason) => {
                        return Ok(ExecutionProgress::Rejected(reason));
                    }
                }
            }
        }
        Err(stock_boundary(
            self.io,
            crate::wire::ErrorCode::Conflict,
            "executor step limit reached",
        ))
    }

    fn collect_step_events(
        &self,
        input: &TurnInput,
        step: u32,
        model_events: Vec<ModelEvent>,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<(Vec<(ToolInvocation, usize)>, String, Value)> {
        let mut calls = Vec::new();
        let mut text = String::new();
        let mut completed = Value::Null;
        for event in model_events {
            match event {
                ModelEvent::Content { delta } => {
                    stock_string_push(
                        &mut text,
                        &delta,
                        self.stock.limits.file_bytes,
                        "assistant output exceeds file limit",
                        self.io,
                    )?;
                }
                ModelEvent::ToolCall(crate::model::ToolCall {
                    call_id,
                    name,
                    arguments,
                    metadata,
                }) => {
                    let invocation = ToolInvocation::for_model_call(
                        input.operation_id,
                        step,
                        call_id,
                        name,
                        arguments,
                        self.native_work(),
                    )?;
                    let call = crate::model::ToolCall::new(
                        stock_string_copy(&invocation.call_id, self.io)?,
                        stock_string_copy(&invocation.name, self.io)?,
                        crate::model::clone_value(&invocation.arguments, self.io)?,
                    )
                    .with_metadata(metadata);
                    let index = prior_messages.len();
                    self.append_assistant_part(prior_messages, ModelContentPart::ToolCall(call))?;
                    stock_vec_push(&mut calls, (invocation, index), self.io)?;
                }
                ModelEvent::Completed { metadata } => completed = metadata,
                ModelEvent::ReasoningBlock { block } => {
                    self.append_assistant_part(
                        prior_messages,
                        ModelContentPart::Reasoning { block },
                    )?;
                }
                ModelEvent::Reasoning { .. } => {}
            }
        }
        Ok((calls, text, completed))
    }

    pub(super) fn append_assistant_part(
        &self,
        messages: &mut Vec<ModelMessage>,
        part: ModelContentPart,
    ) -> Result<()> {
        let message = ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(part),
        };
        with_history_io(self.io, |work| {
            message.content.validate_limits(self.stock.limits, work)
        })?;
        stock_vec_push(messages, message, self.io)
    }
}

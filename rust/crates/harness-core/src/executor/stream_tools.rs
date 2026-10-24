use super::*;
use std::{future::Future as _, pin::Pin, task::{Context, Poll}};

type ToolFuture<'a> = crate::conversation::BoundedContentFuture<'a, ExecutionProgress<()>>;

struct StreamCall<'a> {
    future: Option<ToolFuture<'a>>,
    read: bool,
    started: bool,
}

/// Futures belong to this model step. No detached work survives its settlement.
pub(super) struct StreamTools<'a> {
    calls: Vec<StreamCall<'a>>,
    head: usize,
    blocked: bool,
    failure: Option<Error>,
}

impl<'a> StreamTools<'a> {
    pub(super) fn new() -> Self {
        Self { calls: Vec::new(), head: 0, blocked: false, failure: None }
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> bool {
        let mut preceding_settled = true;
        for index in self.head..self.calls.len() {
            let call = &mut self.calls[index];
            if self.blocked && !call.started {
                call.future = None;
                continue;
            }
            let Some(future) = call.future.as_mut() else { continue };
            if !call.read && !preceding_settled { break; }
            call.started = true;
            let read = call.read;
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    self.calls[index].future = None;
                    self.record(result);
                }
                Poll::Pending => {
                    preceding_settled = false;
                    if !read { break; }
                }
            }
        }
        while self.head < self.calls.len() && self.calls[self.head].future.is_none() {
            self.head += 1;
        }
        if self.head == self.calls.len() {
            self.calls.clear();
            self.head = 0;
            true
        } else {
            false
        }
    }

    fn record(&mut self, result: Result<ExecutionProgress<()>>) {
        match result {
            Ok(ExecutionProgress::Ready(())) => {}
            Ok(_) => self.blocked = true,
            Err(error) => {
                self.blocked = true;
                if self.failure.is_none() { self.failure = Some(error); }
            }
        }
    }

    pub(super) async fn during<T>(&mut self, future: impl std::future::Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        futures::future::poll_fn(|cx| {
            self.poll(cx);
            future.as_mut().poll(cx)
        }).await
    }

    pub(super) fn stop(&mut self) { self.blocked = true; }

    pub(super) fn finish(&mut self) -> Result<()> {
        match self.failure.take() { Some(error) => Err(error), None => Ok(()) }
    }

    pub(super) fn admit(
        &mut self,
        execution: &'a StockExecution<'_>,
        journal: &'a dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        call: &crate::model::ToolCall,
    ) -> Result<()> {
        if self.blocked { return Ok(()) }
        let read = execution.parallel_read(call)?;
        let future = execution.stream_tool(journal, operation, step, call)?;
        stock_vec_push(&mut self.calls, StreamCall { future: Some(future), read, started: false }, execution.io)?;
        Ok(())
    }
}

pub(super) struct StreamControl<'a> {
    pub(super) active: &'a std::sync::atomic::AtomicBool,
    pub(super) stop: &'a std::sync::atomic::AtomicBool,
}

impl StockExecution<'_> {
    fn parallel_read(&self, call: &crate::model::ToolCall) -> Result<bool> {
        // Hook commands can have effects even when their wrapped tool only reads.
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        if self.stock.hooks.is_some() { return Ok(false) }
        Ok(self.stock.tools.get(&call.name, None, self.io)?
            .is_some_and(|tool| tool.executor.execution_class() == crate::tool::ToolExecutionClass::Read))
    }

    fn stream_tool<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        call: &crate::model::ToolCall,
    ) -> Result<ToolFuture<'a>> {
        let invocation = ToolInvocation::for_model_call(operation, step,
            stock_string_copy(&call.call_id, self.io)?, stock_string_copy(&call.name, self.io)?,
            crate::model::clone_value(&call.arguments, self.io)?, self.native_work())?;
        let call = crate::model::ToolCall::new(stock_string_copy(&call.call_id, self.io)?,
            stock_string_copy(&call.name, self.io)?, crate::model::clone_value(&call.arguments, self.io)?)
            .with_metadata(crate::model::clone_value(&call.metadata, self.io)?);
        crate::conversation::BoundedContentFuture::new(self.io, || async move {
            let mut messages = Vec::new();
            let mut hooks = Vec::new();
            self.append_assistant_part(&mut messages, ModelContentPart::ToolCall(call))?;
            self.resolve_tool_call(journal, operation, step, invocation, 0, &mut messages, &mut hooks).await
        })
    }
}

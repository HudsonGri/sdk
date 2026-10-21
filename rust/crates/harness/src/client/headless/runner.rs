//! One headless filesystem contract for every client backend.
use super::HeadlessOutcome;
use crate::{OperationId, Result, task_volume::FileValue};
use futures::Stream;
use std::{future::Future, io::Write};

/// A backend consuming request files and emitting snapshot/cursor-follow JSONL.
pub trait HeadlessClient {
    /// Runs one prompt while publishing its followed records.
    fn prompt<W: Write>(
        &mut self,
        command: FileValue,
        target: OperationId,
        output: &mut W,
    ) -> impl Future<Output = Result<HeadlessOutcome>>;
    /// Runs bounded JSONL input alongside the same followed records.
    fn input<W: Write>(
        &mut self,
        target: OperationId,
        input: impl Stream<Item = Result<String>> + Unpin,
        output: &mut W,
    ) -> impl Future<Output = Result<HeadlessOutcome>>;
}

/// Submits through the client and follows its exact operation to settlement.
pub fn run_headless<'a, C: HeadlessClient + 'a, W: Write + 'a>(
    client: &'a mut C,
    command: FileValue,
    target: OperationId,
    output: &'a mut W,
) -> impl Future<Output = Result<HeadlessOutcome>> + 'a {
    // Forward the child itself; an awaiting adapter retains another copy of its
    // initial future alongside the suspended await state.
    client.prompt(command, target, output)
}

/// Reads this client's canonical JSONL input and drains followed output before exit.
pub fn run_jsonl<'a, C: HeadlessClient + 'a, W: Write + 'a>(
    client: &'a mut C,
    target: OperationId,
    input: impl Stream<Item = Result<String>> + Unpin + 'a,
    output: &'a mut W,
) -> impl Future<Output = Result<HeadlessOutcome>> + 'a {
    client.input(target, input, output)
}

#[cfg(test)]
mod tests;

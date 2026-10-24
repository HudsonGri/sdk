//! Local headless clients use the same request files and atomic query follow.
use super::{
    ClientQuery, CommandRequest, CommandResolution, HeadlessClient, HeadlessOutcome,
    QueryViewLimits,
};
use crate::{
    Error, OperationId, Result,
    local::LocalHarness,
    task_volume::{Change, Component, FileKey, FileValue, Generation, Query, TaskFile},
};
use futures::{FutureExt, Stream, StreamExt};
use std::{future::Future, io::Write, pin::Pin, sync::Arc};

const MAXIMUM_BYTES: usize = super::jsonl::MAXIMUM_REQUEST_BYTES;
const MAXIMUM_FILES: usize = 4096;
// Request size is a per-file admission bound, independent of retained display.
const OBSERVATION_BYTES: u64 = 4 * 1024 * 1024;

fn session(operation: OperationId) -> Result<Component> {
    Ok(Component::new(
        uuid::Uuid::from_bytes(operation.into_bytes()).to_string(),
    )?)
}
fn selection(operation: OperationId) -> Result<Query> {
    Ok(Query::TaskSession {
        task: Component::new(crate::runtime::task_id_for_admission(operation).to_string())?,
        session: session(operation)?,
    })
}
fn key(operation: OperationId, make: fn(Component) -> TaskFile) -> Result<FileKey> {
    Ok(FileKey::Session {
        session: session(operation)?,
        file: make(Component::new(operation.to_string())?),
    })
}

/// Builds an immutable prompt request without admitting or executing it.
pub fn prepare_local_prompt(operation: OperationId, prompt: &str) -> Result<FileValue> {
    if prompt.is_empty() {
        return Err(Error::Invalid("empty local prompt".into()));
    }
    let bytes = serde_json::to_vec(&CommandResolution::Prompt {
        text: prompt.into(),
        source: None,
        mentions: Vec::new(),
    })
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let file = FileValue::new(key(operation, TaskFile::Request)?, bytes);
    validate(&file, operation)?;
    Ok(file)
}

fn validate(file: &FileValue, operation: OperationId) -> Result<()> {
    if file.key != key(operation, TaskFile::Request)? || file.channel.is_some() {
        return Err(Error::Unauthorized(
            "stdin request targets another session or execution owner".into(),
        ));
    }
    let mut claim = file.clone();
    claim.key = key(operation, TaskFile::ExecutionClaim)?;
    if file.bytes.len() > MAXIMUM_BYTES
        || crate::task_volume::record_bytes(file)? > acyclic_stream::MAX_RECORD_BYTES
        || crate::task_volume::record_bytes(&claim)? > acyclic_stream::MAX_RECORD_BYTES
    {
        return Err(Error::Invalid("local request exceeds byte bound".into()));
    }
    serde_json::from_slice::<CommandResolution>(&file.bytes)
        .or_else(|_| {
            serde_json::from_slice::<CommandRequest>(&file.bytes)
                .map(|request| CommandResolution::Submit { request })
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}

/// Decodes an existing task-file value, retaining its exact retry identity.
/// Only this connection's caller-owned request file is admissible.
pub fn parse_local_request(line: &str, operation: OperationId) -> Result<FileValue> {
    if line.len() > MAXIMUM_BYTES {
        return Err(Error::Invalid("stdin line exceeds byte bound".into()));
    }
    let file = super::jsonl::parse_request(line)?;
    validate(&file, operation)?;
    Ok(file)
}

/// Request admission adapter. Execution belongs to the package runtime;
/// this client owns only its disposable query and output publication.
pub trait RequestAdmission {
    /// Publish exactly this immutable request using the selected host transport.
    fn submit(
        &self,
        operation: OperationId,
        file: FileValue,
    ) -> futures::future::LocalBoxFuture<'_, Result<()>>;
    /// Observe runtime settlement independently of the file transport. This
    /// reports owner failure even when no terminal file could be published.
    fn settled(&self, operation: OperationId) -> futures::future::LocalBoxFuture<'_, Result<()>>;
    /// Final source cut to drain before reporting an unsaved runtime failure.
    fn drain_cut(&self, operation: OperationId)
    -> futures::future::LocalBoxFuture<'_, Result<u64>>;
}

enum Admission {
    Local(Arc<LocalHarness>),
    Deployed(Arc<dyn RequestAdmission>),
}
impl Admission {
    fn local(&self) -> Option<&Arc<LocalHarness>> {
        match self {
            Self::Local(local) => Some(local),
            Self::Deployed(_) => None,
        }
    }
}
/// One request-file and completion state machine for embedded and deployed clients.
pub struct FileHeadlessClient {
    admission: Admission,
    operation: OperationId,
    query: ClientQuery,
    initial: bool,
    submitted: bool,
    host: Pin<Box<dyn Future<Output = Result<()>>>>,
    attachment: Option<futures::channel::oneshot::Sender<()>>,
    host_done: bool,
    host_error: Option<Error>,
    drain_through: u64,
}

impl FileHeadlessClient {
    pub(super) async fn open(local: Arc<LocalHarness>, operation: OperationId) -> Result<Self> {
        let query = ClientQuery::open(
            &local.task_files().inner,
            selection(operation)?,
            None,
            QueryViewLimits {
                files: MAXIMUM_FILES,
                segments: MAXIMUM_FILES,
                bytes: OBSERVATION_BYTES,
            },
        )
        .await?;
        let mut subscription = crate::package::harness::CommandSubscription::start(
            local.clone(),
            &acyclic_stream::runtime::PlatformExecutor,
            operation,
            true,
        );
        let attachment = subscription.take_attachment();
        Ok(Self {
            host: Box::pin(subscription),
            attachment,
            admission: Admission::Local(local),
            operation,
            query,
            initial: true,
            submitted: false,
            host_done: false,
            host_error: None,
            drain_through: 0,
        })
    }

    /// Observe the same task-file protocol over a deployed admission adapter.
    pub fn deployed(
        operation: OperationId,
        query: ClientQuery,
        admission: Box<dyn RequestAdmission>,
    ) -> Self {
        let admission: Arc<dyn RequestAdmission> = Arc::from(admission);
        let observer = admission.clone();
        let host = Box::pin(async move { observer.settled(operation).await });
        Self {
            admission: Admission::Deployed(admission),
            operation,
            query,
            initial: true,
            submitted: false,
            host,
            attachment: None,
            host_done: false,
            host_error: None,
            drain_through: 0,
        }
    }

    /// Drain admitted execution before retiring its native mounts, including on
    /// malformed input or a failed output sink. Dropping an awaiting future
    /// cannot join a native capture worker.
    pub async fn finish(mut self, outcome: Result<HeadlessOutcome>) -> Result<HeadlessOutcome> {
        let requested = self.submitted
            || self
                .query
                .file(&key(self.operation, TaskFile::Request)?)
                .is_some();
        let mut settlement = Ok(());
        if self.admission.local().is_some()
            && requested
            && !self.host_done
            && self.outcome()?.is_none()
        {
            // The one-request headless owner returns after saving its outcome.
            // Keep polling it until native work and publication have settled.
            settlement = self.host.as_mut().await;
            self.host_done = true;
        }
        // Even a published outcome precedes worker teardown. Stop idle follow
        // and join before the process can shut down its executor.
        drop(self.attachment.take());
        if self.admission.local().is_some() && !self.host_done {
            settlement = self.host.as_mut().await;
        }
        let Self {
            admission,
            host,
            query,
            ..
        } = self;
        drop(host);
        drop(query);
        if let Admission::Local(local) = admission {
            super::disconnect_local(local).await?;
        }
        match outcome {
            Err(error) => Err(error),
            Ok(value) => settlement.map(|()| value),
        }
    }

    fn initial(&mut self, output: &mut impl Write) -> Result<()> {
        if self.initial {
            let snapshot = self.query.snapshot();
            for file in &snapshot.files {
                if matches!(
                    file.key,
                    FileKey::Session {
                        file: TaskFile::ExecutionResult(_),
                        ..
                    }
                ) {
                    let bytes = file
                        .segments
                        .first()
                        .ok_or_else(|| Error::Invalid("outcome has no segment".into()))?;
                    super::command_output::completion(bytes)?;
                }
            }
            emit(
                output,
                super::jsonl::snapshot(&selection(self.operation)?, &snapshot)?,
            )?;
            self.initial = false;
        }
        Ok(())
    }

    #[inline(never)]
    fn submit<'a>(
        &'a mut self,
        file: &'a FileValue,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            if let Admission::Deployed(admission) = &self.admission {
                admission.submit(self.operation, file.clone()).await?;
                self.submitted = true;
                return Ok(());
            }
            let local = self
                .admission
                .local()
                .ok_or_else(|| Error::Invalid("missing request admission".into()))?;
            let cursor = self.query.cursor();
            let expected = Generation {
                volume: cursor.volume,
                epoch: cursor.epoch,
                position: cursor.position,
                commit: cursor.commit,
            };
            admit_request(local, self.operation, file, expected).await?;
            self.submitted = true;
            Ok(())
        })
    }

    fn outcome(&self) -> Result<Option<HeadlessOutcome>> {
        self.query
            .file(&key(self.operation, TaskFile::ExecutionResult)?)
            .map(|file| {
                let bytes = file
                    .segments
                    .first()
                    .ok_or_else(|| Error::Invalid("outcome has no segment".into()))?;
                super::command_output::completion(bytes)
            })
            .transpose()
    }
}

/// Publish a request through the one owner-authorized task-file admission path.
/// A concurrent publisher can advance the cut, but cannot change this request's
/// immutable identity or bypass `TaskFiles`' write authorization.
#[inline(never)]
pub fn admit_request<'a>(
    local: &'a LocalHarness,
    operation: OperationId,
    file: &'a FileValue,
    mut expected: Generation,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let normalized = resolve_request(local, operation, file).await?;
        loop {
            match local.task_files().write(expected, &normalized).await {
                Ok(_) => return Ok(()),
                Err(crate::task_volume::Error::Conflict(next))
                    if next.position > expected.position =>
                {
                    expected = next;
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
}

/// The stock package's single request admission policy, shared by embedded and
/// deployed entry points. Resolution pins exact input before execution starts.
#[inline(never)]
fn resolve_request<'a>(
    local: &'a LocalHarness,
    operation: OperationId,
    file: &'a FileValue,
) -> futures::future::BoxFuture<'a, Result<FileValue>> {
    Box::pin(async move {
        validate(file, operation)?;
        // Both -p and JSONL enter through this boundary. Already pinned prompt
        // resolutions are data, so template expansion is never parsed twice.
        let normalized;
        let file = match serde_json::from_slice::<CommandResolution>(&file.bytes) {
            Ok(CommandResolution::Prompt {
                text,
                source: None,
                mentions,
            }) if mentions.is_empty() => {
                let resolution = local.resolve_input(operation, &text).await?;
                let mut captured = file.clone();
                captured.bytes = serde_json::to_vec(&resolution)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into();
                normalized = captured;
                validate(&normalized, operation)?;
                &normalized
            }
            _ => file,
        };
        let resolution = serde_json::from_slice::<CommandResolution>(&file.bytes)
            .or_else(|_| {
                serde_json::from_slice::<CommandRequest>(&file.bytes)
                    .map(|request| CommandResolution::Submit { request })
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
        match resolution {
            CommandResolution::Select { .. }
            | CommandResolution::Confirm { .. }
            | CommandResolution::Sdk { .. }
            | CommandResolution::Submit {
                request:
                    CommandRequest::Quit | CommandRequest::Theme { .. } | CommandRequest::CycleMode,
            } => {
                return Err(Error::Unsupported(
                    "command requires an interactive client".into(),
                ));
            }
            _ => {}
        }
        Ok(file.clone())
    })
}

impl FileHeadlessClient {
    // Prompt and JSONL share this owner loop. Construct its storage/follow state
    // on a separate frame so it is not retained as scratch space in prompt's
    // poll while the same thread is driving a model and staging its journal.
    #[inline(never)]
    fn run<'a>(
        &'a mut self,
        operation: OperationId,
        mut input: impl Stream<Item = Result<String>> + Unpin + 'a,
        mut input_open: bool,
        output: &'a mut impl Write,
    ) -> Pin<Box<dyn Future<Output = Result<HeadlessOutcome>> + 'a>> {
        Box::pin(async move {
            if operation != self.operation {
                return Err(Error::Unauthorized("headless operation differs".into()));
            }
            self.initial(output)?;
            loop {
                let settled = self.outcome()?;
                if settled.is_none()
                    && self.host_done
                    && self.query.cursor().position >= self.drain_through
                {
                    let error = self
                        .host_error
                        .take()
                        .unwrap_or(Error::Indeterminate(operation));
                    return if HeadlessOutcome::from_error(&error) == HeadlessOutcome::Indeterminate
                    {
                        Ok(HeadlessOutcome::Indeterminate)
                    } else {
                        Err(error)
                    };
                }
                if let Some(outcome) = settled {
                    if input_open {
                        match input.next().now_or_never() {
                            Some(Some(line)) => {
                                self.submit(&parse_local_request(&line?, operation)?)
                                    .await?;
                                continue;
                            }
                            Some(None) | None => {}
                        }
                    }
                    return Ok(outcome);
                }
                let submitted = self.submitted
                    || self
                        .query
                        .file(&key(operation, TaskFile::Request)?)
                        .is_some();
                enum Next {
                    Input(Option<Result<String>>),
                    File(Result<()>),
                    Host(Result<()>),
                }
                let next = tokio::select! {
                    biased;
                    line = input.next(), if input_open => Next::Input(line),
                    change = self.query.publish_next(async |change| publish(output, change)) => Next::File(change),
                    result = &mut self.host, if !self.host_done => Next::Host(result),
                };
                match next {
                    Next::Input(Some(line)) => {
                        self.submit(&parse_local_request(&line?, operation)?)
                            .await?
                    }
                    Next::Input(None) => {
                        input_open = false;
                        if !submitted {
                            return Err(Error::NotFound("operation was not submitted".into()));
                        }
                    }
                    Next::File(result) => {
                        result?;
                    }
                    Next::Host(result) => {
                        self.host_done = true;
                        self.host_error = result.err();
                        self.drain_through = match &self.admission {
                            Admission::Local(local) => {
                                local
                                    .task_files()
                                    .inner
                                    .snapshot(&selection(operation)?, MAXIMUM_FILES)
                                    .await?
                                    .cursor
                                    .position
                            }
                            Admission::Deployed(admission) => {
                                admission.drain_cut(operation).await?
                            }
                        };
                    }
                }
            }
        })
    }
}

fn emit(output: &mut impl Write, line: String) -> Result<()> {
    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    output
        .write_all(&bytes)
        .and_then(|()| output.flush())
        .map_err(|error| Error::Storage(error.to_string()))
}
fn publish(output: &mut impl Write, change: &Change) -> Result<()> {
    for file in change.file.iter().chain(&change.files) {
        if matches!(
            file.key,
            FileKey::Session {
                file: TaskFile::ExecutionResult(_),
                ..
            }
        ) {
            super::command_output::completion(&file.bytes)?;
        }
    }
    emit(output, super::jsonl::change(change)?)
}

impl HeadlessClient for FileHeadlessClient {
    async fn prompt<W: Write>(
        &mut self,
        command: FileValue,
        operation: OperationId,
        output: &mut W,
    ) -> Result<HeadlessOutcome> {
        if operation != self.operation {
            return Err(Error::Unauthorized("headless operation differs".into()));
        }
        validate(&command, operation)?;
        self.initial(output)?;
        self.submit(&command).await?;
        Box::pin(self.run(operation, futures::stream::empty(), false, output)).await
    }
    async fn input<W: Write>(
        &mut self,
        operation: OperationId,
        input: impl Stream<Item = Result<String>> + Unpin,
        output: &mut W,
    ) -> Result<HeadlessOutcome> {
        Box::pin(self.run(operation, input, true, output)).await
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;

//! Crash-cut enumeration over a real on-disk `LocalStream`.
//!
//! A clean run counts the operation's durable provider writes (`append`,
//! `fork` and committed `commit`s; `LocalStream` publishes each only after its
//! journal flush). Every cut then replays the operation in a fresh directory
//! and crashes at one boundary:
//! - `BeforeWrite(k)`: writes `1..=k` landed, along with every non-durable
//!   effect performed before write `k + 1`, which never starts;
//! - `LostAck(k)`: write `k` landed but the process died before seeing it.
//!
//! After a crash every provider call fails. All handles are dropped, an
//! independent provider reopens the directory, and `recover` observes it.
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest, ChildrenRequest,
    CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest,
    IdempotencyKey, IdempotencyObservation, LocalStream, ReadRequest, RecordStream, StreamBounds,
    StreamError, StreamPath, StreamProvider, StreamWatermarks,
};
use std::{
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

type Storage<T> = Result<T, StreamError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Cut {
    BeforeWrite(usize),
    LostAck(usize),
}

pub(crate) struct CrashingStream {
    inner: Arc<LocalStream>,
    cut: Option<Cut>,
    writes: AtomicUsize,
    crashed: AtomicBool,
}

impl CrashingStream {
    fn alive(&self) -> Storage<()> {
        if self.crashed.load(Ordering::SeqCst) {
            return Err(StreamError::Unavailable);
        }
        Ok(())
    }

    async fn write<T>(
        &self,
        landed: impl FnOnce(&T) -> bool,
        write: impl Future<Output = Storage<T>>,
    ) -> Storage<T> {
        self.alive()?;
        let before = self.writes.load(Ordering::SeqCst);
        if self.cut == Some(Cut::BeforeWrite(before)) {
            self.crashed.store(true, Ordering::SeqCst);
            return Err(StreamError::Unavailable);
        }
        let output = write.await?;
        if landed(&output) {
            self.writes.store(before + 1, Ordering::SeqCst);
            if self.cut == Some(Cut::LostAck(before + 1)) {
                self.crashed.store(true, Ordering::SeqCst);
                return Err(StreamError::Unavailable);
            }
        }
        Ok(output)
    }
}

#[async_trait::async_trait]
impl StreamProvider for CrashingStream {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Storage<Option<IdempotencyObservation>> {
        self.alive()?;
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Storage<u64> {
        self.alive()?;
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Storage<StreamBounds> {
        self.alive()?;
        self.inner.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Storage<StreamWatermarks> {
        self.alive()?;
        self.inner.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> Storage<AppendOutcome> {
        self.write(
            |outcome| matches!(outcome, AppendOutcome::Committed(_)),
            self.inner.append(request),
        )
        .await
    }
    async fn fork(&self, request: ForkRequest) -> Storage<ForkReceipt> {
        self.write(|_| true, self.inner.fork(request)).await
    }
    async fn read(&self, request: ReadRequest) -> Storage<RecordStream> {
        self.alive()?;
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Storage<RecordStream> {
        self.alive()?;
        self.inner.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Storage<RecordStream> {
        self.alive()?;
        self.inner.follow_checked(path, from, preceding).await
    }
    async fn children(&self, request: ChildrenRequest) -> Storage<ChildStream> {
        self.alive()?;
        self.inner.children(request).await
    }
    async fn children_page(&self, request: ChildrenPageRequest) -> Storage<ChildrenPage> {
        self.alive()?;
        self.inner.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Storage<CommitOutcome> {
        self.write(
            |outcome| matches!(outcome, CommitOutcome::Committed(_)),
            self.inner.commit(request),
        )
        .await
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Storage<CommitOutcome> {
        self.write(
            |outcome| matches!(outcome, CommitOutcome::Committed(_)),
            self.inner.commit_before(request, deadline_unix_millis),
        )
        .await
    }
    async fn read_commit(&self, id: CommitId) -> Storage<CommittedEnvelope> {
        self.alive()?;
        self.inner.read_commit(id).await
    }
}

pub(crate) struct CrashCuts<O> {
    /// Durable writes made by the uncrashed operation.
    pub(crate) writes: usize,
    /// Recovery observation after the uncrashed operation.
    pub(crate) clean: O,
    /// Every cut, in boundary order, with its recovery observation.
    pub(crate) cuts: Vec<(Cut, O)>,
}

async fn open(root: &Path) -> Arc<LocalStream> {
    Arc::new(
        LocalStream::open(root, Default::default())
            .await
            .expect("crash-cut provider opens; every prior handle must be dropped"),
    )
}

async fn once<O, S, SF, R, RF, V, VF>(
    cut: Option<Cut>,
    setup: &S,
    run: &R,
    recover: &V,
) -> (usize, O)
where
    S: Fn(Arc<LocalStream>) -> SF,
    SF: Future<Output = ()>,
    R: Fn(Arc<CrashingStream>) -> RF,
    RF: Future<Output = ()>,
    V: Fn(Arc<LocalStream>) -> VF,
    VF: Future<Output = O>,
{
    let directory = tempfile::tempdir().expect("crash-cut directory");
    let inner = open(directory.path()).await;
    setup(inner.clone()).await;
    let crashing = Arc::new(CrashingStream {
        inner,
        cut,
        writes: AtomicUsize::new(0),
        crashed: AtomicBool::new(false),
    });
    run(crashing.clone()).await;
    let writes = crashing.writes.load(Ordering::SeqCst);
    let Ok(crashing) = Arc::try_unwrap(crashing) else {
        panic!("operation retained the crashed provider");
    };
    drop(crashing);
    let observed = recover(open(directory.path()).await).await;
    (writes, observed)
}

/// Runs `setup` uncounted, then `run` once cleanly and once per durable cut.
/// `run` must ignore its own result; `recover` reopens and observes.
pub(crate) async fn enumerate<O, S, SF, R, RF, V, VF>(
    setup: S,
    run: R,
    recover: V,
) -> CrashCuts<O>
where
    S: Fn(Arc<LocalStream>) -> SF,
    SF: Future<Output = ()>,
    R: Fn(Arc<CrashingStream>) -> RF,
    RF: Future<Output = ()>,
    V: Fn(Arc<LocalStream>) -> VF,
    VF: Future<Output = O>,
{
    let (writes, clean) = once(None, &setup, &run, &recover).await;
    assert!(writes > 0, "operation made no durable write to cut");
    let mut cuts = Vec::with_capacity(2 * writes + 1);
    for landed in 0..=writes {
        let mut boundary = vec![Cut::BeforeWrite(landed)];
        if landed > 0 {
            boundary.insert(0, Cut::LostAck(landed));
        }
        for cut in boundary {
            let (reached, observed) = once(Some(cut), &setup, &run, &recover).await;
            // Every cut lands exactly its own writes. BeforeWrite(writes)
            // never fires, so it replays the clean run's durable state.
            let expected = match cut {
                Cut::BeforeWrite(k) | Cut::LostAck(k) => k,
            };
            assert_eq!(reached, expected, "{cut:?} did not reach its boundary");
            cuts.push((cut, observed));
        }
    }
    CrashCuts {
        writes,
        clean,
        cuts,
    }
}

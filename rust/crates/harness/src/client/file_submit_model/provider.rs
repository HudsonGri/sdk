//! External durability prefixes over the actual modeled MemoryStream.
use acyclic_stream::*;
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub(super) struct Cuts {
    pub(super) outbox: AtomicU64,
    pub(super) target: AtomicU64,
    pub(super) intent_position: AtomicU64,
    pub(super) target_position: AtomicU64,
    pub(super) armed: AtomicBool,
    pub(super) crossed: AtomicBool,
}
impl Cuts {
    pub(super) fn new() -> Self {
        Self {
            outbox: AtomicU64::new(u64::MAX),
            target: AtomicU64::new(u64::MAX),
            intent_position: AtomicU64::new(0),
            target_position: AtomicU64::new(0),
            armed: AtomicBool::new(false),
            crossed: AtomicBool::new(false),
        }
    }
}

pub(super) struct Provider {
    pub(super) inner: MemoryStream,
    pub(super) cuts: Arc<Cuts>,
    pub(super) outbox: bool,
}
impl Provider {
    fn cap(&self) -> &AtomicU64 {
        if self.outbox {
            &self.cuts.outbox
        } else {
            &self.cuts.target
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for Provider {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> std::result::Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn watermarks(
        &self,
        path: StreamPath,
    ) -> std::result::Result<StreamWatermarks, StreamError> {
        let accepted = self.inner.tail(path).await?;
        Ok(StreamWatermarks {
            accepted,
            durable: accepted.min(self.cap().load(Ordering::SeqCst)),
        })
    }
    async fn append(
        &self,
        request: AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
    ) -> std::result::Result<RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> std::result::Result<RecordStream, StreamError> {
        self.inner.follow_checked(path, from, preceding).await
    }
    async fn children(
        &self,
        request: ChildrenRequest,
    ) -> std::result::Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn commit(
        &self,
        request: CommitRequest,
    ) -> std::result::Result<CommitOutcome, StreamError> {
        if self.cuts.armed.load(Ordering::SeqCst) {
            // After setup, this sole client commits only its request to target
            // and its acknowledgement to outbox. Observe before publication;
            // a later watermark must not repair an earlier violation.
            let confirmed = if self.outbox {
                self.cuts.target.load(Ordering::SeqCst)
                    >= self.cuts.target_position.load(Ordering::SeqCst)
            } else {
                self.cuts.outbox.load(Ordering::SeqCst)
                    >= self.cuts.intent_position.load(Ordering::SeqCst)
            };
            if !confirmed {
                self.cuts.crossed.store(true, Ordering::SeqCst);
            }
        }
        // The wrapper owns no lock. TaskVolume releases its index before I/O;
        // MemoryStream acquires only its actual crate-local provider mutex.
        self.inner.commit(request).await
    }
    async fn read_commit(
        &self,
        id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

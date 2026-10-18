use super::{Arc, Barrier, Mutex};
use acyclic_stream::*;

pub(super) struct ScheduledStream<P> {
    pub(super) inner: Arc<P>,
    // Fixture lock order: never retain the slot lock across a wait/provider call.
    pub(super) barrier: Arc<Mutex<Option<Barrier>>>,
}

#[async_trait::async_trait]
impl<P: StreamProvider> StreamProvider for ScheduledStream<P> {
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let barrier = self.barrier.lock().unwrap().take();
        if let Some(barrier) = barrier {
            barrier.entered.send(()).unwrap();
            barrier.release.await.unwrap();
        }
        self.inner.commit(request).await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }

    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
}

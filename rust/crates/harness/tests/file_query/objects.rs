use acyclic_fs::{
    AsyncObjectStore, AuthorityAllocation, AuthorityWork, MemoryObjectBackend, ObjectId,
    ObjectKind, ObjectRead, ObjectReadRequest, ObjectStoreError, storage::ObjectWrite,
};
use acyclic_stream::OwnedBytes;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
type ReadPause = (
    ObjectKind,
    futures::channel::oneshot::Sender<()>,
    futures::channel::oneshot::Receiver<()>,
);

#[derive(Clone)]
pub(super) struct CountingObjects {
    inner: Arc<MemoryObjectBackend>,
    pub(super) reads: Arc<AtomicUsize>,
    pause: Arc<Mutex<Option<ReadPause>>>,
}

impl Default for CountingObjects {
    fn default() -> Self {
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self {
            inner: Arc::new(MemoryObjectBackend::new(Arc::new(objects), bucket)),
            reads: Arc::default(),
            pause: Arc::default(),
        }
    }
}

impl CountingObjects {
    pub(super) fn pause(
        &self,
        kind: ObjectKind,
    ) -> (
        futures::channel::oneshot::Receiver<()>,
        futures::channel::oneshot::Sender<()>,
    ) {
        let (entered, arrival) = futures::channel::oneshot::channel();
        let (release, released) = futures::channel::oneshot::channel();
        *self.pause.lock().unwrap() = Some((kind, entered, released));
        (arrival, release)
    }

    async fn pause_read(&self, id: ObjectId, bytes: &[u8]) {
        let pause = {
            let mut gate = self.pause.lock().unwrap();
            if gate.as_ref().is_some_and(|(kind, _, _)| {
                *kind == id.kind && (*kind != ObjectKind::BlobChunk || bytes.starts_with(b"- ["))
            }) {
                gate.take()
            } else {
                None
            }
        };
        if let Some((_, entered, released)) = pause {
            entered.send(()).unwrap();
            released.await.unwrap();
        }
    }
}

impl AsyncObjectStore for CountingObjects {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_many(writes, work).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        if id.kind == ObjectKind::BlobChunk {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        let read = self.inner.read(id, maximum, work).await?;
        self.pause_read(id, &read.bytes).await;
        Ok(read)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, AuthorityAllocation), ObjectStoreError> {
        self.reads.fetch_add(
            requests
                .iter()
                .filter(|request| request.object_id.kind == ObjectKind::BlobChunk)
                .count(),
            Ordering::SeqCst,
        );
        let result = self.inner.read_many(requests, work).await?;
        for (request, read) in requests.iter().zip(&result.0) {
            self.pause_read(request.object_id, &read.bytes).await;
        }
        Ok(result)
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}

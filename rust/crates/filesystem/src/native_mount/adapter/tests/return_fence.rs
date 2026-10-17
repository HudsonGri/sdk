use super::*;
use std::cell::Cell;
use std::sync::{atomic::AtomicUsize, mpsc};

thread_local! {
    static OMIT: Cell<bool> = const { Cell::new(false) };
}

pub(in crate::native_mount::adapter) fn omitted() -> bool {
    OMIT.get()
}

struct Omit(bool);
impl Omit {
    fn enter(value: bool) -> Self {
        Self(OMIT.replace(value))
    }
}
impl Drop for Omit {
    fn drop(&mut self) {
        OMIT.set(self.0);
    }
}

pub(in crate::native_mount) fn with_omitted<T>(broken: bool, run: impl FnOnce() -> T) -> T {
    let _old = Omit::enter(broken);
    run()
}

struct Pause {
    remaining: usize,
    entered: mpsc::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

struct Objects {
    inner: crate::MemoryObjectBackend,
    reads: AtomicUsize,
    // Terminal fixture lock: release before either channel operation or await.
    pause: crate::sync::Mutex<Option<Pause>>,
}

impl Default for Objects {
    fn default() -> Self {
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self {
            inner: crate::MemoryObjectBackend::new(Arc::new(objects), bucket),
            reads: AtomicUsize::new(0),
            pause: crate::sync::Mutex::new(None),
        }
    }
}

impl Objects {
    async fn before_read(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let pause = {
            let mut slot = self.pause.lock().unwrap();
            if let Some(pause) = slot.as_mut() {
                pause.remaining -= 1;
                if pause.remaining == 0 {
                    slot.take()
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.entered.send(()).unwrap();
            pause.release.await.unwrap();
        }
    }
}

impl AsyncObjectStore for Objects {
    fn supports_decoded_cache(&self) -> bool {
        self.inner.supports_decoded_cache()
    }
    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_hashed(object, work).await
    }
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_many(writes, work).await
    }
    async fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_many_hashed(objects, work).await
    }
    async fn flush_before_publish(
        &self,
        scope: crate::PublicationScope<'_>,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, crate::ObjectStoreError> {
        self.inner.flush_before_publish(scope, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        self.before_read().await;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        for _ in requests {
            self.before_read().await;
        }
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}

type Source = CheckoutMountSource<crate::MemoryAuthorityBackend, Arc<Objects>>;

fn fixture() -> Result<(Arc<Source>, Arc<Objects>), Box<dyn std::error::Error>> {
    let objects = Arc::new(Objects::default());
    let fs = Fs::new(
        crate::StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default())),
        objects.clone(),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let profile = if cfg!(unix) {
        FilesystemProfile::Posix
    } else {
        FilesystemProfile::Portable
    };
    let config = VolumeConfig {
        profile,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cancel = CancellationToken::new();
    let checkout = runtime.block_on(async {
        fs.create_volume(config, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map(|r| r.value)
    })?;
    let source = access::bind_test_access(
        CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?,
        vec![],
        None,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("file");
    let file = source.create_file(&path, metadata(), None, &work)?;
    source
        .open_created(&path, &file)?
        .write_range(0, Bytes::from(vec![7; 4096]))?;
    #[cfg(unix)]
    source.write_attribute(
        &path,
        b"user.secret",
        crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"attribute"), 0),
        MountAttributeWriteMode::Create,
        &work,
    )?;
    source.sync()?;
    Ok((Arc::new(source), objects))
}

#[derive(Clone, Copy, Debug)]
enum Read {
    Bytes,
    Metadata,
    Directory,
    Open,
    Detached,
    Seek,
    #[cfg(unix)]
    Attribute,
    #[cfg(unix)]
    Attributes,
}

fn read(source: &Source, op: Read) -> Result<(), MountSourceError> {
    let path = native_test_path("file");
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    match op {
        Read::Bytes => assert_eq!(source.read_range(&path, 0, 4096)?.as_ref(), vec![7; 4096]),
        Read::Metadata => assert_eq!(
            source.lookup(&path)?.unwrap().node.kind,
            MountNodeKind::Regular
        ),
        Read::Directory => assert_eq!(
            source
                .read_directory(&MountPath::root(), None, 8)?
                .entries
                .len(),
            1
        ),
        Read::Open => {
            source.open_file_with_lookup_with_meter(&path, MountOpenAccess::Read, &work)?;
        }
        Read::Detached => {
            source.detach_file_with_meter(&path, &work)?;
        }
        Read::Seek => assert_eq!(source.seek(&path, 0, MountSeekTarget::Data)?, Some(0)),
        #[cfg(unix)]
        Read::Attribute => assert_eq!(
            source
                .read_attribute(&path, b"user.secret", &work)?
                .unwrap()
                .bytes
                .as_ref(),
            b"attribute"
        ),
        #[cfg(unix)]
        Read::Attributes => assert_eq!(
            source.list_attributes(&path, None, 8)?.names,
            vec![b"user.secret".to_vec()]
        ),
    }
    Ok(())
}

fn revoked_return(
    op: Read,
    broken: bool,
    persisted: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (source, objects) = fixture()?;
    // Calibrate the last real storage read of this exact operation without the
    // return fence. Pausing that read establishes authorization-before-storage,
    // rather than pausing the policy lookup that precedes authorization.
    objects.reads.store(0, Ordering::Relaxed);
    {
        let _old = Omit::enter(true);
        read(&source, op)?;
    }
    let remaining = objects.reads.load(Ordering::Relaxed);
    assert_ne!(remaining, 0);
    let (entered, pending) = mpsc::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *objects.pause.lock().unwrap() = Some(Pause {
        remaining,
        entered,
        release: released,
    });
    let worker = std::thread::spawn({
        let source = source.clone();
        move || {
            let _old = Omit::enter(broken);
            read(&source, op)
        }
    });
    pending.recv_timeout(Duration::from_secs(2))?;
    let policy = crate::access::fixtures::denied(crate::access::Action::Read, "**");
    let publication = (|| -> Result<(), Box<dyn std::error::Error>> {
        if persisted {
            super::policy_refresh::publish(&source, &policy)?;
        } else {
            let view = access::compiled(
                &source,
                2,
                vec![access::deny("revoked", &crate::access::Action::ALL, "**")],
                None,
            )?;
            source.access_view.as_ref().unwrap().publish(view)?;
        }
        Ok(())
    })();
    release.send(()).unwrap();
    let result = worker.join().map_err(|_| "native read worker panicked")?;
    publication?;
    assert!(
        matches!(result, Err(MountSourceError::AccessDenied)),
        "native result escaped a revoked cut: {op:?} {result:?}"
    );
    Ok(())
}

#[test]
fn every_native_result_rechecks_authority_after_its_storage_read()
-> Result<(), Box<dyn std::error::Error>> {
    for op in [
        Read::Bytes,
        Read::Metadata,
        Read::Directory,
        Read::Open,
        Read::Detached,
        Read::Seek,
        #[cfg(unix)]
        Read::Attribute,
        #[cfg(unix)]
        Read::Attributes,
    ] {
        revoked_return(op, false, true)?;
        revoked_return(op, false, false)?;
    }
    Ok(())
}

#[test]
#[should_panic(expected = "native result escaped a revoked cut")]
fn removed_return_fence_negative_control() {
    revoked_return(Read::Bytes, true, true).unwrap();
}

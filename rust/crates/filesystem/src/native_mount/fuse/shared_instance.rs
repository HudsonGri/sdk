//! The process's shared FUSE instances, one per shared root directory.
//!
//! An instance exists only while a view holds it: the registry keeps weak
//! references, every view keeps a strong one, and dropping the last view
//! unmounts the instance, stops its threads and releases its fence. A
//! failed start unwinds through the same teardown. A process that dies with
//! live views leaves its root fenced by a lock nobody holds; the next start
//! for that root detaches the dead owner's mount before mounting again.

use super::shared::{SharedDispatch, SharedViewEntry, SharedViews, run_shared_invalidator};
use super::*;
use crate::NativeMaterializationWork;
use crate::native_mount::{
    MountDestinationGuard, detach_native_mount_destination_after_crash, native_mount_control_paths,
};
use std::path::PathBuf;
use std::sync::{Condvar, Weak};

static INSTANCES: OnceLock<Mutex<HashMap<PathBuf, Slot>>> = OnceLock::new();

#[cfg(test)]
thread_local! {
    /// Negative control: starts without reaping a crash-left root.
    pub(super) static SKIP_REAP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct Slot {
    instance: Weak<SharedFuseInstance>,
    retired: Arc<Retired>,
}

/// Signalled once an instance's teardown is complete.
#[derive(Default)]
struct Retired {
    done: Mutex<bool>,
    changed: Condvar,
}

impl Retired {
    fn wait(&self) {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        while !*done {
            done = self
                .changed
                .wait(done)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn finish(&self) {
        *self.done.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_all();
    }
}

fn instances() -> &'static Mutex<HashMap<PathBuf, Slot>> {
    INSTANCES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(in crate::native_mount) struct SharedFuseInstance {
    pub(super) views: Arc<SharedViews>,
    root: PathBuf,
    path: Vec<u8>,
    state: Mutex<InstanceState>,
    retired: Arc<Retired>,
    // The instance's own meter: it outlives the view that started it.
    work: NativeMaterializationWork,
}

#[derive(Default)]
struct InstanceState {
    mount: super::mount::Mount,
    device: Option<OwnedFd>,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    invalidator: Option<std::thread::JoinHandle<()>>,
    guard: Option<MountDestinationGuard>,
}

/// One view of a shared instance. Dropping it retires the view, and the last
/// view's drop tears the instance down.
pub(in crate::native_mount) struct SharedFuseView {
    // Field order: the view retires before its instance reference drops.
    pub(super) entry: SharedViewEntry,
    pub(super) _instance: Arc<SharedFuseInstance>,
}

impl SharedFuseInstance {
    /// Returns the live instance for `root`, starting one if none is live.
    pub(super) fn acquire(root: &Path) -> Result<Arc<Self>, NativeMountError> {
        // Canonicalize the parent only: a crash-left root is a dead mount
        // whose own stat fails, and the reaper must still find it.
        let (Some(parent), Some(name)) = (root.parent(), root.file_name()) else {
            return Err(NativeMountError::InvalidDestination);
        };
        let root = parent
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?
            .join(name);
        loop {
            let mut registry = instances().lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(slot) = registry.get(&root) {
                if let Some(instance) = slot.instance.upgrade() {
                    return Ok(instance);
                }
                // The last view dropped and teardown is running; a new start
                // would race its unmount and fence release.
                let retired = Arc::clone(&slot.retired);
                drop(registry);
                retired.wait();
                continue;
            }
            // Starting under the registry lock serializes starts per root.
            let instance = Arc::new(Self::start(&root)?);
            registry.insert(
                root,
                Slot {
                    instance: Arc::downgrade(&instance),
                    retired: Arc::clone(&instance.retired),
                },
            );
            return Ok(instance);
        }
    }

    fn start(root: &Path) -> Result<Self, NativeMountError> {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        // A fence whose lock is free belongs to a dead owner: detach its
        // mount first, so a crash never leaves a root mounted for good.
        let [fence, ..] = native_mount_control_paths(root)?;
        #[cfg(test)]
        let reap = !SKIP_REAP.with(std::cell::Cell::get);
        #[cfg(not(test))]
        let reap = true;
        if reap && fence.try_exists()? {
            detach_native_mount_destination_after_crash(root)?;
        }
        let metadata = std::fs::metadata(root)?;
        let mut path = root.as_os_str().as_bytes().to_vec();
        path.push(0);
        let mut instance = Self {
            views: SharedViews::new(
                std::os::unix::fs::MetadataExt::uid(&metadata),
                std::os::unix::fs::MetadataExt::gid(&metadata),
            ),
            root: root.to_path_buf(),
            path,
            state: Mutex::new(InstanceState::default()),
            retired: Arc::new(Retired::default()),
            work,
        };
        // Every failure below drops `instance`, whose teardown undoes the
        // steps already taken.
        let views = Arc::clone(&instance.views);
        let work = instance.work.clone();
        let state = instance
            .state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        MountDestinationGuard::acquire_with_work(root, true, &work, &mut state.guard, false)?;
        super::session::map_read_buffers();
        let mut config = Config::default();
        config.mount_options = vec![
            fuser::MountOption::FSName("acyclic-fs".to_owned()),
            fuser::MountOption::DefaultPermissions,
            fuser::MountOption::Exec,
            fuser::MountOption::NoAtime,
        ];
        config.n_threads = Some(crate::native_mount::CALLBACK_THREADS as usize);
        config.clone_fd = false;
        state.mount.open_into(
            root,
            true,
            || super::lifecycle::fuse_recovery_helper(&work),
            &mut state.device,
        )?;
        let descriptor = state
            .device
            .as_ref()
            .ok_or(NativeMountError::FuseShutdownFailed)?
            .try_clone()?;
        let session = fuser::Session::from_fd(
            SharedDispatch(Arc::clone(&views)),
            descriptor,
            fuser::SessionACL::Owner,
            config,
        )?;
        let notifier = session.notifier();
        views.set_notifier(notifier.clone());
        state.worker = Some(
            std::thread::Builder::new()
                .name("acyclic-fuse".to_owned())
                .spawn(move || session.run())?,
        );
        state.invalidator = Some(
            std::thread::Builder::new()
                .name("acyclic-fuse-invalidator".to_owned())
                .spawn(move || run_shared_invalidator(&views, &notifier))?,
        );
        Ok(instance)
    }

    fn teardown(&mut self) {
        self.views.close();
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        let Ok(path) = std::ffi::CStr::from_bytes_with_nul(&self.path) else {
            return;
        };
        let mut detached = !state.mount.pending();
        if !detached {
            let device = state.device.as_ref().map(std::os::fd::AsFd::as_fd);
            match state.mount.stop(path, device) {
                Ok(()) => {
                    detached = device
                        .is_none_or(|device| state.mount.device_detached(device).unwrap_or(false))
                        || !state.mount.pending();
                }
                Err(error) => {
                    tracing::warn!(%error, root = %self.root.display(), "shared FUSE unmount failed");
                }
            }
        }
        if !detached {
            // Busy: detach the namespace now; the kernel ends the connection,
            // and with it the worker, once the last open file closes.
            match nix::mount::umount2(path, nix::mount::MntFlags::MNT_DETACH) {
                Ok(()) => detached = true,
                Err(error) => {
                    tracing::warn!(%error, root = %self.root.display(), "shared FUSE detach failed");
                }
            }
            drop(state.worker.take());
        }
        if let Some(worker) = state.worker.take() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "shared FUSE session ended with an error");
                }
                Err(_) => tracing::warn!("shared FUSE session panicked"),
            }
        }
        self.views.stop_invalidator();
        if let Some(invalidator) = state.invalidator.take()
            && invalidator.join().is_err()
        {
            tracing::warn!("shared FUSE invalidator panicked");
        }
        state.device.take();
        if let Some(mut guard) = state.guard.take() {
            if detached {
                if let Err(error) = guard.release_cleanup(None) {
                    tracing::warn!(%error, "shared FUSE fence release failed");
                }
            }
            // An undetached root keeps its fence file; the next start for the
            // root detaches it.
            drop(guard);
        }
    }
}

impl Drop for SharedFuseInstance {
    fn drop(&mut self) {
        self.teardown();
        {
            let mut registry = instances().lock().unwrap_or_else(PoisonError::into_inner);
            if registry
                .get(&self.root)
                .is_some_and(|slot| Arc::ptr_eq(&slot.retired, &self.retired))
            {
                registry.remove(&self.root);
            }
        }
        self.retired.finish();
    }
}

impl SharedFuseView {
    /// Lists a view named after `destination`'s last component in the shared
    /// instance at its parent, starting the instance if none is live.
    pub(super) fn start(
        destination: &Path,
        build: impl FnOnce(u64) -> Result<Arc<ProjectionCore>, NativeMountError>,
    ) -> Result<Self, NativeMountError> {
        let (Some(root), Some(name)) = (destination.parent(), destination.file_name()) else {
            return Err(NativeMountError::InvalidDestination);
        };
        let instance = SharedFuseInstance::acquire(root)?;
        let entry = instance.views.insert(name.as_bytes(), build)?;
        Ok(Self {
            entry,
            _instance: instance,
        })
    }

    pub(super) fn core(&self) -> &Arc<ProjectionCore> {
        self.entry.core()
    }

    /// Retires the view; the instance stays up while other views hold it.
    pub(super) fn retire(&mut self) -> bool {
        self.entry.retire()
    }
}

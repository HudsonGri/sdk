//! Logical volume bindings acquire an OS projection only for native execution.
use super::*;
use crate::sync::SyncMutex;
use crate::sync::owned::{OwnedMutexGuard, lock_owned};

pub(in crate::local) struct Native(
    pub(super) acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>,
);

impl std::ops::Deref for Native {
    type Target = acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for Native {
    fn drop(&mut self) {
        // Keep callbacks and the destination fence inside the last owner.
        if let Err(error) = self.0.abandon() {
            tracing::error!(?error, "workspace mount retirement failed");
        }
    }
}

// Lock order: checkout state -> one volume projection -> filesystem/provider.
// No callback reacquires either outer lock. Hold this guard through logical
// writes, so first projection cannot pin a head preceding an unpublished write.
pub(super) async fn initialize<T>(
    slot: &StateArc<AsyncMutex<Option<T>>>,
    create: impl AsyncFnOnce() -> Result<T>,
) -> Result<OwnedMutexGuard<Option<T>>> {
    let mut slot = lock_owned(StateArc::clone(slot)).await;
    if slot.is_none() {
        *slot = Some(create().await?);
    }
    Ok(slot)
}

/// One live native execution scope's claim on a projection. Dropping it,
/// including by cancellation, releases the claim; only `leave` retires.
pub(super) struct Entered<'a>(&'a SyncMutex<usize>);

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        let mut users = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *users = users.saturating_sub(1);
    }
}

// Claims are added only under the projection lock, and the last scope retires
// under that same lock, so a projection never retires under a running command
// and a new scope never adopts one that is being retired.
pub(super) async fn enter<'a, T>(
    slot: &StateArc<AsyncMutex<Option<T>>>,
    users: &'a SyncMutex<usize>,
    create: impl AsyncFnOnce() -> Result<T>,
) -> Result<Entered<'a>> {
    let slot = initialize(slot, create).await?;
    *users
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
    drop(slot);
    Ok(Entered(users))
}

pub(super) async fn leave<T>(
    slot: &StateArc<AsyncMutex<Option<T>>>,
    users: &SyncMutex<usize>,
    entered: Entered<'_>,
    retire: impl AsyncFnOnce(&mut Option<T>) -> Result<()>,
) -> Result<()> {
    drop(entered);
    let mut slot = lock_owned(StateArc::clone(slot)).await;
    if *users
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        != 0
    {
        return Ok(());
    }
    retire(&mut slot).await
}

impl MountedVolume {
    pub(in crate::local) async fn native(&self) -> OwnedMutexGuard<Option<Native>> {
        lock_owned(StateArc::clone(&self.native)).await
    }

    pub(super) async fn enter(
        &self,
        root: &Path,
        relative: &str,
        read_only: bool,
    ) -> Result<Entered<'_>> {
        enter(&self.native, &self.users, async || {
            volumes::prepare_mount_directory(root, Path::new(relative))?;
            let options = if read_only {
                acyclic_fs::MountOptions::read_only()
            } else {
                acyclic_fs::MountOptions::read_write()
                    .publication(acyclic_fs::MountPublication::Manual)
            };
            self.workspace
                .workspace()
                .mount(&self.destination, options)
                .await
                .map(Native)
                .map_err(Error::from)
        })
        .await
    }

    pub(super) async fn leave(&self, entered: Entered<'_>) -> Result<()> {
        leave(&self.native, &self.users, entered, async |slot| {
            self.retire_locked(slot).await
        })
        .await
    }

    pub(super) async fn retire(&self) -> Result<()> {
        self.retire_locked(&mut *self.native().await).await
    }

    // The single retirement path: publish, detach, then remove the residue
    // projection created. A failed detach keeps the owner for a later retry.
    async fn retire_locked(&self, slot: &mut Option<Native>) -> Result<()> {
        if let Some(native) = slot.as_ref() {
            native.sync().await?;
            native.advance_to_head().await?;
            native.unmount().await?;
        }
        *slot = None;
        volumes::remove_residue(&self.destination)
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod tests;

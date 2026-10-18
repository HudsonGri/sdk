//! Removes host bytes a saved binding imported under SDK namespaces.
//!
//! Before every path took its exclusions from one policy, a binding could
//! import host entries under the reserved names: demand promoted host
//! nodes into the authored head, and older capture read those names too.
//! Nothing refreshes them now, so a reopened head would serve the imported
//! bytes forever. Every head records the policy it was last reconciled under
//! at [`MARKER`], so forks inherit it. While that record differs from the
//! current policy, opening a binding removes every head entry under those
//! names outside the SDK-owned subtrees, and within them every entry whose
//! identity derives from the host node at the same path.
use crate::{Error, Result};
use acyclic_fs::demand::DemandSource as _;
use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::kernel::{FileKind, NamespacePath};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FileId, Generation, IdempotencyKey,
    LazyWorkspace, LazyWorkspaceStore, NativeRootIdentity, PublicationPermit, TransactionCommit,
    WorkBudget, WorkspaceError,
};
use std::path::Path;

/// Head path recording the policy the head was last reconciled under.
pub(super) const MARKER: &str = ".system/reserved-names";

const PAGE: u32 = 256;

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(format!("reserved-name reconciliation: {error}"))
}

/// Head entries under one name, every child before its parent.
async fn authored<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    head: &Generation<A, O>,
    name: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<(String, FileId, FileKind)>> {
    let root = match head
        .stat(&format!("/{name}"), WorkBudget::UNBOUNDED, cancellation)
        .await
    {
        Ok(stat) => stat.value,
        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {
            return Ok(Vec::new());
        }
        Err(failure) => return Err(storage(failure.error)),
    };
    let mut entries = vec![(name.to_owned(), root.file_id, root.kind)];
    let mut index = 0;
    while index < entries.len() {
        if entries[index].2 == FileKind::Directory {
            let directory = entries[index].0.clone();
            let mut after = None;
            loop {
                let page = head
                    .list_directory(
                        &format!("/{directory}"),
                        after.as_ref(),
                        PAGE,
                        WorkBudget::UNBOUNDED,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| storage(failure.error))?
                    .value
                    .0;
                for entry in &page.entries {
                    let child = entry
                        .name
                        .unicode_text(WorkBudget::UNBOUNDED, cancellation)
                        .map_err(|failure| storage(failure.error))?
                        .value;
                    entries.push((format!("{directory}/{child}"), entry.file_id, entry.kind));
                }
                after = page.entries.last().map(|entry| entry.name.clone());
                if !page.has_more || after.is_none() {
                    break;
                }
            }
        }
        index += 1;
    }
    // Breadth-first order lists parents first; reversed, children come first.
    entries.reverse();
    Ok(entries)
}

/// The checkout's source with nothing excluded, at the binding's reference.
async fn open_host(
    root: &Path,
    identity: NativeRootIdentity,
    config: acyclic_fs::VolumeConfig,
    reference: acyclic_fs::demand::SourceReference,
    cancellation: &CancellationToken,
) -> Result<NativeDemandSource> {
    let host = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(root),
        config.profile,
        config.limits,
        Some(reference),
        None,
        WorkBudget::UNBOUNDED,
        cancellation,
    )
    .await
    .map_err(|failure| storage(failure.error))?
    .value;
    if host.root_identity() != identity {
        return Err(Error::Unauthorized(
            "checkout root was replaced during reconciliation".into(),
        ));
    }
    Ok(host)
}

/// Reconciles `view`'s head with the current reserved-name policy.
///
/// `root` is the binding's physical checkout and its pinned identity. Its
/// unfiltered source identifies entries demand imported under SDK-owned
/// subtrees.
pub async fn reconcile_reserved_names<A, O, S>(
    view: &LazyWorkspace<A, O, NativeDemandSource, S>,
    root: &Path,
    identity: NativeRootIdentity,
) -> Result<()>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    S: LazyWorkspaceStore,
{
    let cancellation = CancellationToken::new();
    let policy = super::reserved::fingerprint();
    let head = view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| storage(failure.error))?
        .value;
    match head
        .read(
            &format!("/{MARKER}"),
            policy.len() as u64 + 1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
    {
        Ok(recorded) if recorded.value.as_ref() == policy.as_bytes() => return Ok(()),
        Ok(_) => {}
        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {}
        Err(failure) => return Err(storage(failure.error)),
    }
    let config = view.workspace().volume_config();
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let reference = view.state(&mut accounting).await.map_err(storage)?.source;
    // Opened only for a head that holds SDK-owned entries.
    let mut host = None;
    let mut visited = false;
    for name in super::RESERVED_NAMES {
        for (relative, file_id, kind) in authored(&head, name, &cancellation).await? {
            visited = true;
            let imported = if !super::reserved::sdk_owned(&relative) {
                true
            } else if kind == FileKind::Directory || relative == MARKER {
                // An owned directory holds SDK state; it is never removed.
                false
            } else {
                let path =
                    acyclic_fs::path::PortablePath::parse(&format!("/{relative}"), config.limits)
                        .map_err(storage)?;
                let path =
                    NamespacePath::from_portable_in_profile(&path, config.profile, config.limits)
                        .map_err(storage)?;
                if host.is_none() {
                    host = Some(open_host(root, identity, config, reference, &cancellation).await?);
                }
                let source = host
                    .as_ref()
                    .ok_or_else(|| storage("unfiltered checkout source is unavailable"))?;
                match source
                    .lookup(
                        source.reference(),
                        &path,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(|failure| storage(failure.error))?
                    .value
                {
                    Some(node) => {
                        view.source_file_id(&node.file_identity, &mut accounting)
                            .map_err(storage)?
                            == file_id
                    }
                    None => false,
                }
            };
            if imported {
                view.remove(&format!("/{relative}"), Some(file_id), &mut accounting)
                    .await
                    .map_err(storage)?;
            }
        }
    }
    // A head with nothing under the reserved names has nothing to reconcile,
    // and recording the policy would cost every new binding a generation.
    if !visited {
        return Ok(());
    }
    // Record the policy only after every removal committed: a crash before
    // this point reconciles again, and reconciling is idempotent.
    match view
        .write(
            &format!("/{MARKER}"),
            bytes::Bytes::from(policy).into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut accounting,
        )
        .await
        .map_err(storage)?
    {
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => Ok(()),
        _ => Err(Error::Conflict(
            "reserved-name reconciliation raced with another writer".into(),
        )),
    }
}

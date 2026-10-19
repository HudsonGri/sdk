//! Discovered checkout skills, listed like Codex's `<skills_instructions>`.
//!
//! Only frontmatter metadata enters the context. Bodies stay in the checkout,
//! where the agent reads one with its ordinary file tools before following it.
use super::*;
use crate::context::{CatalogSkill, SKILL_ROOTS};

/// Directory entries examined per page.
const PAGE: u32 = 64;

mod catalog;
pub(in crate::local) use catalog::listed;

impl LocalHarness {
    /// Lists `<root>/<name>/SKILL.md` frontmatter from the selected checkout view.
    pub(in crate::local) async fn checkout_skills(
        &self,
        workspace: &LocalWorkspace,
        maximum: u64,
    ) -> Result<Vec<CatalogSkill>> {
        let (selected, pending) = workspace.selected_view().await?;
        if let Some((operation, _)) = pending {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                operation.into_bytes(),
            )));
        }
        let view = selected.as_ref().unwrap_or_else(|| workspace.view());
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let mut skills: Vec<CatalogSkill> = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        for root in SKILL_ROOTS {
            let mut directories = Vec::new();
            let mut cursor = None;
            loop {
                let page = match view
                    .list_directory(&format!("/{root}"), cursor, PAGE, &mut accounting)
                    .await
                {
                    Ok((page, _)) => page,
                    Err(error) if absent(&error) => break,
                    Err(error) => return Err(error.into()),
                };
                for entry in &page.entries {
                    if entry.kind == acyclic_fs::demand::SourceNodeKind::Directory
                        && let Ok(name) = std::str::from_utf8(entry.name.as_bytes())
                    {
                        directories.push(name.to_owned());
                    }
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            // Pages arrive in the view's order; list by name for a stable prompt.
            directories.sort_unstable();
            for directory in directories {
                let path = format!("{root}/{directory}/SKILL.md");
                let bytes = match view
                    .read_latest_prefix(&format!("/{path}"), maximum, &mut accounting)
                    .await
                {
                    Ok(bytes) => bytes,
                    Err(acyclic_fs::LazyWorkspaceError::NotRegularFile) => continue,
                    Err(error) if absent(&error) => continue,
                    Err(error) => return Err(error.into()),
                };
                if let Some(skill) = listed(bytes.bytes.as_ref(), path)
                    && names.insert(skill.name.clone())
                {
                    skills.push(skill);
                }
            }
        }
        Ok(skills)
    }
}

/// A missing skill root or file is no skill, whether the authored view or the
/// native source reports the absence.
fn absent(error: &acyclic_fs::LazyWorkspaceError) -> bool {
    use acyclic_fs::{FsError, LazyWorkspaceError};
    matches!(
        error,
        LazyWorkspaceError::NotFound
            | LazyWorkspaceError::NotDirectory
            | LazyWorkspaceError::Filesystem(FsError::NotFound | FsError::NotDirectory)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_absence_is_skipped() {
        use acyclic_fs::{FsError, LazyWorkspaceError};
        for error in [
            LazyWorkspaceError::NotFound,
            LazyWorkspaceError::NotDirectory,
            LazyWorkspaceError::Filesystem(FsError::NotFound),
            LazyWorkspaceError::Filesystem(FsError::NotDirectory),
        ] {
            assert!(absent(&error), "{error}");
        }
        // Negative controls: a stale or cancelled view fails the turn.
        for error in [
            LazyWorkspaceError::StaleSource,
            LazyWorkspaceError::Cancelled,
        ] {
            assert!(!absent(&error), "{error}");
        }
    }
}

//! Discovered checkout skills, listed like Codex's `<skills_instructions>`.
//!
//! Only frontmatter metadata enters the context. Bodies stay in the checkout,
//! where the agent reads one with its ordinary file tools before following it.
use super::*;

/// Repository skill roots, in precedence order for duplicate names.
pub(super) const ROOTS: [&str; 2] = [".agents/skills", ".claude/skills"];
/// Skills listed per turn; the rest stay discoverable on disk.
const MAXIMUM_SKILLS: usize = 128;
/// Directory entries examined per page.
const PAGE: u32 = 64;

#[derive(serde::Deserialize)]
struct Header {
    name: String,
    description: String,
}

/// One listed skill: validated name, single-line description and file path.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Listed {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) path: String,
}

/// Parses frontmatter; a skill without a valid name and description is skipped.
pub(super) fn listed(bytes: &[u8], path: String) -> Option<Listed> {
    let header: Header = crate::context::parse_frontmatter(bytes).ok()?;
    let valid_name = !header.name.is_empty()
        && !header.name.starts_with('-')
        && !header.name.ends_with('-')
        && !header.name.contains("--")
        && header
            .name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    let description = header.description.split_whitespace().collect::<Vec<_>>();
    (valid_name && !description.is_empty()).then(|| Listed {
        name: header.name,
        description: description.join(" "),
        path,
    })
}

/// Exact block text, or `None` when no skill was discovered.
pub(super) fn frame(skills: &[Listed]) -> Option<String> {
    if skills.is_empty() {
        return None;
    }
    let mut text = String::from(
        "<skills_instructions>\n## Skills\nA skill is a set of local instructions to follow that is stored in a `SKILL.md` file. Below is the list of skills that can be used. Each entry includes a name, description, and a path relative to the working directory. Read a skill's file before following it.\n### Available skills\n",
    );
    for skill in skills {
        text.push_str("- ");
        text.push_str(&skill.name);
        text.push_str(": ");
        text.push_str(&skill.description);
        text.push_str(" (file: ");
        text.push_str(&skill.path);
        text.push_str(")\n");
    }
    text.push_str("</skills_instructions>");
    Some(text)
}

impl LocalHarness {
    /// Lists `<root>/<name>/SKILL.md` frontmatter from the selected checkout view.
    pub(super) async fn checkout_skills(
        &self,
        workspace: &LocalWorkspace,
        maximum: u64,
    ) -> Result<Vec<Listed>> {
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
        let mut skills: Vec<Listed> = Vec::new();
        for root in ROOTS {
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
                if cursor.is_none() || directories.len() >= MAXIMUM_SKILLS {
                    break;
                }
            }
            // Pages arrive in the view's order; list by name for a stable prompt.
            directories.sort_unstable();
            for directory in directories {
                if skills.len() == MAXIMUM_SKILLS {
                    return Ok(skills);
                }
                let path = format!("{root}/{directory}/SKILL.md");
                let bytes = match view
                    .read_latest(&format!("/{path}"), maximum, &mut accounting)
                    .await
                {
                    Ok(bytes) => bytes,
                    Err(
                        acyclic_fs::LazyWorkspaceError::NotRegularFile
                        | acyclic_fs::LazyWorkspaceError::TooLarge,
                    ) => continue,
                    Err(error) if absent(&error) => continue,
                    Err(error) => return Err(error.into()),
                };
                if let Some(skill) = listed(bytes.bytes.as_ref(), path)
                    && !skills.iter().any(|listed| listed.name == skill.name)
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

    #[test]
    fn frontmatter_lists_valid_skills_on_one_line() {
        let skill = listed(
            b"---\nname: release-notes\ndescription: |\n  Draft notes\n  from merged PRs.\nlicense: MIT\n---\nBODY",
            ".agents/skills/release-notes/SKILL.md".into(),
        );
        assert_eq!(
            frame(skill.as_slice()).as_deref(),
            Some(
                "<skills_instructions>\n## Skills\nA skill is a set of local instructions to follow that is stored in a `SKILL.md` file. Below is the list of skills that can be used. Each entry includes a name, description, and a path relative to the working directory. Read a skill's file before following it.\n### Available skills\n- release-notes: Draft notes from merged PRs. (file: .agents/skills/release-notes/SKILL.md)\n</skills_instructions>"
            )
        );
        assert!(!frame(skill.as_slice()).unwrap_or_default().contains("BODY"));
    }

    #[test]
    fn skills_without_a_valid_name_or_description_are_not_listed() {
        for bytes in [
            &b"no frontmatter"[..],
            b"---\nname: Bad Name\ndescription: x\n---\n",
            b"---\nname: ok\ndescription: \"  \"\n---\n",
            b"---\nname: ok\n---\n",
        ] {
            assert_eq!(listed(bytes, "p".into()), None);
        }
        assert_eq!(frame(&[]), None);
    }
}

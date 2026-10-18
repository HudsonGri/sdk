//! The mount point is harness state, never a user change: Git ignores it
//! through the repository's own `info/exclude`, not the user's `.gitignore`.
use std::path::{Path, PathBuf};

const MARKER: &str = "# acyclic harness volume mount point";

/// The `info/exclude` that governs `root`, and `root` relative to the
/// working tree's top, or `None` outside a Git working tree.
fn governing(root: &Path) -> std::io::Result<Option<(PathBuf, PathBuf)>> {
    let mut top = root;
    loop {
        let dot_git = top.join(".git");
        let git_dir = match std::fs::symlink_metadata(&dot_git) {
            Ok(entry) if entry.is_dir() => dot_git,
            Ok(entry) if entry.is_file() => {
                // A linked worktree or submodule: `gitdir: <path>`.
                let text = std::fs::read_to_string(&dot_git)?;
                let Some(path) = text.trim_end().strip_prefix("gitdir: ") else {
                    return Ok(None);
                };
                top.join(path)
            }
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(parent) = top.parent() else {
                    return Ok(None);
                };
                top = parent;
                continue;
            }
            Err(error) => return Err(error),
        };
        // Exclusions live in the common directory shared by all worktrees.
        let common = match std::fs::read_to_string(git_dir.join("commondir")) {
            Ok(path) => git_dir.join(path.trim_end()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => git_dir,
            Err(error) => return Err(error),
        };
        let relative = root
            .strip_prefix(top)
            .unwrap_or(Path::new(""))
            .to_path_buf();
        return Ok(Some((common.join("info").join("exclude"), relative)));
    }
}

/// The anchored pattern for `name` under `relative`, in Git's `/` syntax.
fn pattern(relative: &Path, name: &str) -> Option<String> {
    let mut pattern = String::new();
    for part in relative.components() {
        let std::path::Component::Normal(part) = part else {
            return None;
        };
        pattern.push('/');
        pattern.push_str(&glob_literal(part.to_str()?));
    }
    pattern.push('/');
    pattern.push_str(&glob_literal(name));
    pattern.push('/');
    Some(pattern)
}

fn glob_literal(name: &str) -> String {
    let mut literal = String::with_capacity(name.len());
    for character in name.chars() {
        if matches!(character, '*' | '?' | '[' | '\\' | '!' | '#') || character.is_whitespace() {
            literal.push('\\');
        }
        literal.push(character);
    }
    literal
}

/// Appends the mount point's pattern once; existing user lines are kept.
pub(super) fn exclude_mount_point(root: &Path, name: &str) -> std::io::Result<()> {
    let Some((exclude, relative)) = governing(root)? else {
        return Ok(());
    };
    let Some(pattern) = pattern(&relative, name) else {
        return Ok(());
    };
    let existing = match std::fs::read_to_string(&exclude) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };
    if existing.lines().any(|line| line.trim_end() == pattern) {
        return Ok(());
    }
    if let Some(parent) = exclude.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let separator = if existing.is_empty() || existing.ends_with('\n') {
        ""
    } else {
        "\n"
    };
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&exclude)?
        .write_all(format!("{separator}{MARKER}\n{pattern}\n").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(root: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    }

    #[test]
    fn the_mount_point_is_excluded_once_without_touching_user_ignores() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        let exclude = repository.join(".git/info/exclude");
        std::fs::write(&exclude, "user-pattern").unwrap();
        let nested = repository.join("sub dir");
        for root in [&repository, &nested] {
            std::fs::create_dir_all(root.join(".volumes/scratch")).unwrap();
            std::fs::write(root.join(".volumes/scratch/note"), "x").unwrap();
            exclude_mount_point(root, ".volumes").unwrap();
            exclude_mount_point(root, ".volumes").unwrap();
        }
        std::fs::write(nested.join("tracked"), "x").unwrap();
        let text = std::fs::read_to_string(&exclude).unwrap();
        assert!(text.starts_with("user-pattern\n"));
        assert_eq!(text.matches("/.volumes/\n").count(), 2);
        assert_eq!(text.matches("/sub\\ dir/.volumes/\n").count(), 1);
        assert!(!repository.join(".gitignore").exists());
        let status = git(
            &repository,
            &["status", "--porcelain", "--untracked-files=all"],
        );
        assert_eq!(status, "?? \"sub dir/tracked\"\n");
        // Negative control: without the exclusion both mount points show.
        std::fs::write(&exclude, "user-pattern\n").unwrap();
        let status = git(
            &repository,
            &["status", "--porcelain", "--untracked-files=all"],
        );
        assert!(status.contains(".volumes/scratch/note"), "{status}");
    }

    #[test]
    fn linked_worktrees_share_the_common_exclude() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(
            &repository,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "c",
            ],
        );
        let linked = directory.path().join("linked");
        git(
            &repository,
            &["worktree", "add", "-q", linked.to_str().unwrap()],
        );
        std::fs::create_dir_all(linked.join(".volumes")).unwrap();
        std::fs::write(linked.join(".volumes/note"), "x").unwrap();
        exclude_mount_point(&linked, ".volumes").unwrap();
        assert!(
            std::fs::read_to_string(repository.join(".git/info/exclude"))
                .unwrap()
                .contains("\n/.volumes/\n")
        );
        assert_eq!(git(&linked, &["status", "--porcelain"]), "");
    }

    #[test]
    fn a_directory_outside_git_is_left_alone() {
        let directory = tempfile::tempdir().unwrap();
        // The temporary root may sit inside a repository on a developer box;
        // only assert that nothing is created inside the directory itself.
        exclude_mount_point(directory.path(), ".volumes").unwrap();
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

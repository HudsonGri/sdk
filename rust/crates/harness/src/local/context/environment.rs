//! Task-binding environment facts, framed like Codex's `<environment_context>`.
//!
//! The block is rendered once into the turn's context manifest, so a resumed
//! or replayed turn sees the facts that were true when it was admitted.
use std::path::Path;
use std::time::Duration;

/// Porcelain bytes summarized; larger repositories report a partial count.
const GIT_STATUS_BYTES: u64 = 64 * 1024;
/// A slow `git status` only drops the summary, never delays the turn further.
const GIT_STATUS_TIMEOUT: Duration = Duration::from_secs(2);

/// Renders the environment block for a checkout rooted at `cwd`.
pub(super) async fn render(cwd: &Path, now_unix_millis: u64) -> String {
    let git = git_status(cwd).await;
    frame(
        &display(cwd),
        std::env::consts::OS,
        now_unix_millis,
        git.as_deref(),
    )
}

/// The path as a user would type it: Windows verbatim prefixes are dropped.
pub(in crate::local) fn display(path: &Path) -> String {
    let text = path.to_string_lossy();
    if let Some(share) = text.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{share}")
    } else if let Some(local) = text
        .strip_prefix(r"\\?\")
        .filter(|local| local.as_bytes().get(1) == Some(&b':'))
    {
        local.to_owned()
    } else {
        text.into_owned()
    }
}

/// Exact block text. The shell is the one the `bash` tool always runs.
pub(super) fn frame(cwd: &str, os: &str, now_unix_millis: u64, git: Option<&str>) -> String {
    let mut text = String::from("<environment_context>\n");
    for (tag, value) in [
        ("cwd", Some(cwd)),
        ("shell", Some("bash")),
        ("os", Some(os)),
        ("current_date", Some(date(now_unix_millis).as_str())),
        ("timezone", Some("UTC")),
        ("git_status", git),
    ] {
        let Some(value) = value else { continue };
        text.push_str("  <");
        text.push_str(tag);
        text.push('>');
        for character in value.chars() {
            match character {
                '&' => text.push_str("&amp;"),
                '<' => text.push_str("&lt;"),
                '>' => text.push_str("&gt;"),
                '\n' | '\r' => text.push(' '),
                character => text.push(character),
            }
        }
        text.push_str("</");
        text.push_str(tag);
        text.push_str(">\n");
    }
    text.push_str("</environment_context>");
    text
}

/// Proleptic Gregorian UTC date, `YYYY-MM-DD`.
pub(super) fn date(unix_millis: u64) -> String {
    // Hinnant's days-to-civil over the 400-year era; u64 days never go negative.
    let days = unix_millis / 86_400_000 + 719_468;
    let era = days / 146_097;
    let day_of_era = days % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// One-line summary of `git status`, or `None` outside a work tree.
async fn git_status(cwd: &Path) -> Option<String> {
    use tokio::io::AsyncReadExt as _;
    let mut child = tokio::process::Command::new(which::which("git").ok()?)
        .current_dir(cwd)
        .args([
            "status",
            "--porcelain=v1",
            "--branch",
            "-z",
            "--untracked-files=normal",
        ])
        // Read-only: never take the index lock another git process may need.
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let collected = tokio::time::timeout(GIT_STATUS_TIMEOUT, async {
        let mut bytes = Vec::new();
        stdout
            .take(GIT_STATUS_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .ok()?;
        let truncated = bytes.len() as u64 > GIT_STATUS_BYTES;
        if truncated {
            child.start_kill().ok()?;
            child.wait().await.ok()?;
        } else if !child.wait().await.ok()?.success() {
            return None;
        }
        Some((bytes, truncated))
    })
    .await
    .ok()??;
    Some(summarize(&collected.0, collected.1))
}

/// Summarizes NUL-separated porcelain v1 records with a branch header.
pub(super) fn summarize(porcelain: &[u8], truncated: bool) -> String {
    let mut records = porcelain.split(|byte| *byte == 0);
    let mut branch = None;
    let [mut staged, mut modified, mut untracked, mut conflicted] = [0_u64; 4];
    let mut complete = !truncated;
    while let Some(record) = records.next() {
        if let Some(header) = record.strip_prefix(b"## ") {
            branch = Some(String::from_utf8_lossy(header).into_owned());
            continue;
        }
        let [index, tree, b' ', ..] = record else {
            // An empty tail follows the final NUL; anything else is a cut record.
            complete &= record.is_empty();
            continue;
        };
        match (index, tree) {
            (b'?', b'?') => untracked += 1,
            (b'!', b'!') => {}
            (b'D', b'D') | (b'A', b'A') | (b'U', _) | (_, b'U') => conflicted += 1,
            _ => {
                staged += u64::from(*index != b' ');
                modified += u64::from(*tree != b' ');
            }
        }
        // A rename or copy carries its source path as the next record.
        if matches!(index, b'R' | b'C') {
            records.next();
        }
    }
    let mut summary = branch.map_or_else(String::new, |branch| format!("branch {branch}"));
    let mut counts = Vec::new();
    for (count, label) in [
        (staged, "staged"),
        (modified, "modified"),
        (untracked, "untracked"),
        (conflicted, "conflicted"),
    ] {
        if count != 0 {
            counts.push(format!("{count} {label}"));
        }
    }
    if !summary.is_empty() {
        summary.push_str("; ");
    }
    if counts.is_empty() && complete {
        summary.push_str("clean");
    } else {
        summary.push_str(&counts.join(", "));
    }
    if !complete {
        summary.push_str(if counts.is_empty() { "" } else { ", " });
        summary.push_str("more changes not counted");
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_follow_the_gregorian_calendar_in_utc() {
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(951_782_400_000), "2000-02-29");
        assert_eq!(date(1_792_324_800_000), "2026-10-18");
        assert_eq!(date(1_792_367_999_999), "2026-10-18");
        assert_eq!(date(1_792_368_000_000), "2026-10-19");
        assert_eq!(date(4_000_000_000_000), "2096-10-02");
    }

    #[test]
    fn porcelain_records_summarize_by_state() {
        let porcelain =
            b"## main...origin/main [ahead 1]\0M  staged.rs\0 M edited.rs\0MM both.rs\0R  new.rs\0old.rs\0?? scratch.txt\0UU conflict.rs\0!! ignored\0";
        assert_eq!(
            summarize(porcelain, false),
            "branch main...origin/main [ahead 1]; 3 staged, 2 modified, 1 untracked, 1 conflicted"
        );
        assert_eq!(summarize(b"## main\0", false), "branch main; clean");
        assert_eq!(
            summarize(b"## main\0?? a\0?? b", true),
            "branch main; 2 untracked, more changes not counted"
        );
        // A rename's source path is not a change, even when it reads like one.
        assert_eq!(summarize(b"R  new.rs\0?? old\0", false), "1 staged");
        assert_eq!(summarize(b"?? old\0", false), "1 untracked");
    }

    #[test]
    fn verbatim_windows_prefixes_are_not_shown() {
        for (path, shown) in [
            (r"\\?\C:\work", r"C:\work"),
            (r"\\?\UNC\server\share", r"\\server\share"),
            (r"\\?\Volume{1}\work", r"\\?\Volume{1}\work"),
            ("/tmp/work", "/tmp/work"),
        ] {
            assert_eq!(display(Path::new(path)), shown);
        }
    }

    #[test]
    fn the_block_escapes_values_and_omits_absent_git_status() {
        assert_eq!(
            frame("/w/<a&b>", "linux", 0, Some("branch main; clean")),
            "<environment_context>\n  <cwd>/w/&lt;a&amp;b&gt;</cwd>\n  <shell>bash</shell>\n  <os>linux</os>\n  <current_date>1970-01-01</current_date>\n  <timezone>UTC</timezone>\n  <git_status>branch main; clean</git_status>\n</environment_context>"
        );
        assert!(!frame("/w", "linux", 0, None).contains("git_status"));
    }
}

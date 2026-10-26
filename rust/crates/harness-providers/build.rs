//! Register model checking and stamp captures with the compiled source identity.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(loom)");
    println!("cargo:rerun-if-env-changed=ACYCLIC_PROVIDER_SOURCE_COMMIT");
    // Resolve real Git paths: a worktree's .git is a file, not a directory.
    for name in ["HEAD".to_owned(), "index".to_owned()]
        .into_iter()
        .chain(git(&["symbolic-ref", "-q", "HEAD"]))
    {
        if let Some(path) = git(&["rev-parse", "--git-path", &name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    // Archive builders supply the exact source commit explicitly.
    let commit = std::env::var("ACYCLIC_PROVIDER_SOURCE_COMMIT")
        .ok()
        .or_else(|| git(&["rev-parse", "HEAD"]))
        .unwrap_or_default();
    if !commit.is_empty()
        && !((commit.len() == 40 || commit.len() == 64)
            && commit.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err(
            "STEP stamp provider source failed: invalid commit; recovery: unset ACYCLIC_PROVIDER_SOURCE_COMMIT and cargo build --locked -p acyclic-harness-providers"
                .into(),
        );
    }
    println!("cargo:rustc-env=ACYCLIC_PROVIDER_SOURCE_COMMIT={commit}");
    Ok(())
}

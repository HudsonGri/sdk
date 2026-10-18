use std::hash::{Hash, Hasher};
use std::path::Path;

pub(super) fn record(directory: &Path, args: &[String]) -> std::io::Result<()> {
    if !args.iter().any(|arg| arg == "--test") {
        return Ok(());
    }
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    args.hash(&mut hash);
    let name = format!("{:016x}", hash.finish());
    let cwd = std::env::current_dir()?;
    let mut command = cwd.to_string_lossy().into_owned();
    for arg in args {
        command.push('\0');
        command.push_str(arg);
    }
    std::fs::write(directory.join(format!("{name}.args")), command)?;
    let mut environment = String::new();
    for (key, value) in std::env::vars() {
        if matches!(
            key.as_str(),
            "OUT_DIR"
                | "CARGO_MANIFEST_DIR"
                | "CARGO_CRATE_NAME"
                | "CARGO_BIN_NAME"
                | "CARGO_PRIMARY_PACKAGE"
                | "HOST"
                | "TARGET"
                | "PROFILE"
                | "OPT_LEVEL"
        ) || key.starts_with("CARGO_PKG_")
            || key.starts_with("CARGO_FEATURE_")
            || key.starts_with("CARGO_CFG_")
        {
            environment.push_str(&key);
            environment.push('=');
            environment.push_str(&value);
            environment.push('\0');
        }
    }
    std::fs::write(directory.join(format!("{name}.env")), environment)
}

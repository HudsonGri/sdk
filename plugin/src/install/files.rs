//! Configuration file operations and locations.

use super::*;

pub(crate) fn read_json_object(path: &Path) -> Result<serde_json::Map<String, Value>, String> {
    if !path.exists() {
        return Ok(serde_json::Map::new());
    }
    serde_json::from_slice::<Value>(&fs::read(path).map_err(display)?)
        .map_err(display)?
        .as_object()
        .cloned()
        .ok_or_else(|| format!("{} must contain a JSON object", path.display()))
}

pub(crate) fn object_entry<'a>(
    document: &'a mut serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a mut serde_json::Map<String, Value>, String> {
    document
        .entry(key.to_owned())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| format!("configuration field '{key}' must be an object"))
}

pub(crate) fn write_json_with_backup(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
        None,
    )
    .map_err(display)
}

pub(crate) fn write_text_with_backup(path: &Path, value: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("txt")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(&temporary, value.as_bytes()).map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
        None,
    )
    .map_err(display)
}

pub(crate) fn executable_on_path(name: &str) -> bool {
    find_executable(name).is_some()
}

/// Uses the shared native search policy, including Windows package-manager
/// batch launchers. Discovery and execution select the same absolute path.
pub(crate) fn find_executable(name: &str) -> Option<PathBuf> {
    let cwd = env::current_dir().ok()?;
    acyclic_native_runtime::ExecutableSearch::from_environment(&cwd, |key| env::var_os(key))
        .resolve(name)
        .ok()
}

/// A command for the host CLI `name` as installed on PATH.
pub(crate) fn host_command(name: &str) -> Result<std::process::Command, String> {
    let cwd = env::current_dir().map_err(display)?;
    host_command_in(name, &cwd, |key| env::var_os(key))
}

pub(crate) fn host_command_in(
    name: &str,
    cwd: &Path,
    environment: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<std::process::Command, String> {
    acyclic_native_runtime::ExecutableSearch::from_environment(cwd, environment)
        .command(name)
        .map_err(display)
}

pub(crate) fn home_directory() -> Result<PathBuf, String> {
    env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .ok_or_else(|| "cannot locate the per-user configuration directory".to_owned())
}

pub(crate) fn config_directory() -> Result<PathBuf, String> {
    if cfg!(windows) {
        env::var_os("APPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| "APPDATA is unavailable".to_owned())
    } else {
        Ok(env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or(home_directory()?.join(".config")))
    }
}

pub(crate) fn claude_desktop_config() -> Result<PathBuf, String> {
    if cfg!(target_os = "macos") {
        Ok(home_directory()?.join("Library/Application Support/Claude/claude_desktop_config.json"))
    } else {
        Ok(config_directory()?.join("Claude/claude_desktop_config.json"))
    }
}

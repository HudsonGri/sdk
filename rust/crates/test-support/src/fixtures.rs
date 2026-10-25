use std::path::{Path, PathBuf};

/// Selects the exact companion executable admitted by the cross-test runner.
/// Ordinary Cargo runs use their `CARGO_BIN_EXE_*` compiler metadata. The
/// explicit fixture must be absolute; no pathname search can substitute a
/// different build or an executable installed on the test machine.
#[must_use]
pub fn binary_path(cargo_path: &str) -> PathBuf {
    resolve_binary(
        std::env::var_os("ACYCLIC_TEST_BINARY").map(PathBuf::from),
        cargo_path,
    )
}

fn resolve_binary(admitted: Option<PathBuf>, cargo_path: &str) -> PathBuf {
    let binary = admitted.unwrap_or_else(|| cargo_path.into());
    assert!(
        binary.is_absolute(),
        "test executable must be absolute: {}",
        binary.display()
    );
    binary
}

/// Locates a checked-in fixture relative to the SDK repository.
///
/// Cross-built test executables run against an explicitly supplied source
/// snapshot: set `ACYCLIC_TEST_SOURCE_ROOT` to its absolute repository root.
/// Ordinary Cargo tests use the source tree they were compiled from.
///
/// # Panics
///
/// If the explicit source root is relative, or the fixture path is absolute.
pub fn source_path(relative: impl AsRef<Path>) -> PathBuf {
    resolve(
        std::env::var_os("ACYCLIC_TEST_SOURCE_ROOT").map(PathBuf::from),
        relative.as_ref(),
    )
}

fn resolve(root: Option<PathBuf>, relative: &Path) -> PathBuf {
    let root = root.unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.."));
    assert!(
        root.is_absolute(),
        "test source root must be absolute: {}",
        root.display()
    );
    assert!(
        relative.is_relative(),
        "fixture path must be repository-relative"
    );
    root.join(relative)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_source_snapshot_replaces_the_build_hosts_path() {
        let root = std::env::temp_dir().join("cross-built-fixtures");
        let source = resolve(Some(root.clone()), Path::new("plugin/src/main.rs"));
        assert_eq!(source, root.join("plugin/src/main.rs"));
        assert!(source.is_absolute());
    }

    #[test]
    fn admitted_executable_replaces_foreign_compiler_metadata() {
        let binary = std::env::temp_dir().join("admitted-sdk-local-cli.exe");
        assert_eq!(
            resolve_binary(Some(binary.clone()), "/absent-build-host/sdk-local.exe"),
            binary
        );
        assert_ne!(
            PathBuf::from("/absent-build-host/sdk-local.exe"),
            binary,
            "the old compiler-only path is the negative control"
        );
    }

    #[test]
    #[should_panic(expected = "test executable must be absolute")]
    fn executable_override_cannot_search_the_host_path() {
        resolve_binary(Some("sdk-local.exe".into()), "/build-host/sdk-local.exe");
    }
}

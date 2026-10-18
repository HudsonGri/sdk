//! One process-environment seam; fixtures cannot discover native user locations.
use std::ffi::OsString;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
use std::path::{Path, PathBuf};

enum Read<'a> {
    Variable(&'a str),
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    Home,
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    State(Option<&'a Path>),
}

#[cfg(any(test, feature = "test-support"))]
fn user_location(name: &str) -> bool {
    [
        "HOME",
        "USERPROFILE",
        "LOCALAPPDATA",
        "APPDATA",
        "CODEX_HOME",
        "HOMEDRIVE",
        "HOMEPATH",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
    ]
    .iter()
    .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

pub fn variable(name: &str) -> Option<OsString> {
    resolve(Read::Variable(name))
}

/// Fixture readers never expose ambient home/configuration paths, including integration consumers.
#[cfg(feature = "test-support")]
pub fn fixture_variable(name: &str) -> Option<OsString> {
    if user_location(name) {
        None
    } else {
        variable(name)
    }
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn home() -> Option<PathBuf> {
    resolve(Read::Home).map(PathBuf::from)
}
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn state(injected: Option<&Path>) -> Option<PathBuf> {
    resolve(Read::State(injected)).map(PathBuf::from)
}

#[expect(
    clippy::disallowed_methods,
    reason = "the only native state/environment resolver; unit fixtures cannot discover user locations"
)]
fn resolve(read: Read<'_>) -> Option<OsString> {
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    if let Read::State(Some(directory)) = read {
        return Some(directory.as_os_str().to_owned());
    }
    #[cfg(all(
        feature = "test-support",
        feature = "filesystem-local",
        not(target_arch = "wasm32")
    ))]
    if let Read::State(None) = read {
        return None;
    }
    #[cfg(test)]
    match &read {
        Read::Variable(name) if !user_location(name) => {}
        _ => return None,
    }
    match read {
        Read::Variable(name) => std::env::var_os(name),
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        Read::Home => dirs::home_dir().map(PathBuf::into_os_string),
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        Read::State(None) => dirs::data_local_dir().map(PathBuf::into_os_string),
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        Read::State(Some(directory)) => Some(directory.as_os_str().to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn location_names_stay_isolated_under_arbitrary_casing(mask in any::<u16>()) {
            for name in ["HOME", "USERPROFILE", "LOCALAPPDATA", "APPDATA", "CODEX_HOME"] {
                let cased: String = name.chars().enumerate().map(|(index, letter)| {
                    if mask & (1 << index) != 0 { letter.to_ascii_lowercase() } else { letter }
                }).collect();
                prop_assert!(variable(&cased).is_none());
                #[cfg(feature = "test-support")]
                prop_assert!(fixture_variable(&cased).is_none());
            }
        }
    }
    #[test]
    fn native_user_locations_are_unreachable_from_unit_fixtures() {
        for name in [
            "HOME",
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
            "CODEX_HOME",
            "home",
            "XDG_CONFIG_HOME",
        ] {
            assert!(variable(name).is_none());
        }
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        {
            assert!(home().is_none());
            assert!(state(None).is_none());
            let directory = tempfile::tempdir().unwrap();
            assert_eq!(
                state(Some(directory.path())).as_deref(),
                Some(directory.path())
            );
        }
    }
}

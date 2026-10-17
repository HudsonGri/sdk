//! Untrusted policy, scope and path inputs through real audit serialization.
#![no_main]
#[path = "../../tests/access_input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    input::check(bytes);
    let Ok(input) = serde_json::from_slice::<input::Input>(bytes) else {
        return;
    };
    use acyclic_fs::model::{CaseSensitivity, FilesystemProfile};
    let mut config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral);
    config.profile = match input.profile % 4 {
        0 => FilesystemProfile::Portable,
        1 => FilesystemProfile::Browser,
        2 => FilesystemProfile::Posix,
        _ => FilesystemProfile::Windows,
    };
    if input.folded {
        config.case_sensitivity = CaseSensitivity::ProfileFolded;
    }
    acyclic_fs::access::audit::fuzzing::check(
        &input.policy,
        input.principal,
        input.capability,
        &input.path,
        config,
        input.now,
    );
});

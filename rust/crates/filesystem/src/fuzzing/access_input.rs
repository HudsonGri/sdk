//! Shared parser/decision oracle for proptest, corpus replay and cargo-fuzz.
use crate::access::*;
use serde::{Deserialize, Serialize};

/// Synthetic policy request used by property tests and mutation fuzzing.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    /// Policy evaluated by the production access boundary.
    pub policy: Policy,
    /// Delegation capability supplied with the request.
    pub capability: Capability,
    /// Principal requesting access.
    pub principal: Principal,
    /// Candidate portable path.
    pub path: String,
    /// Native path profile selector.
    pub profile: u8,
    /// Whether the request uses folded native names.
    pub folded: bool,
    /// Synthetic current time used for expiry checks.
    pub now: u64,
    /// Optional encoded native name to validate.
    #[serde(default)]
    pub native_name: Option<(u8, Vec<u8>)>,
}

/// Checks arbitrary input against the production implementation and independent invariants.
pub fn check(bytes: &[u8]) {
    // Scope labels are also accepted directly, including malformed JSON chains.
    if let Ok(label) = std::str::from_utf8(bytes)
        && let Ok(Some(capability)) = Capability::from_scope_label(label)
    {
        assert_eq!(
            Capability::from_scope_label(&capability.to_scope_label().expect("serialize scope"))
                .expect("parse scope"),
            Some(capability)
        );
    }
    let Ok(input) = serde_json::from_slice::<Input>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&input).expect("serialize access input");
    assert_eq!(
        serde_json::from_slice::<Input>(&encoded).expect("parse access input"),
        input
    );
    let label = input.capability.to_scope_label().expect("serialize scope");
    assert_eq!(
        Capability::from_scope_label(&label).expect("parse scope"),
        Some(input.capability.clone())
    );
    let mut config = crate::VolumeConfig::portable(crate::Lifecycle::Durable);
    config.profile = match input.profile % 4 {
        0 => crate::model::FilesystemProfile::Portable,
        1 => crate::model::FilesystemProfile::Browser,
        2 => crate::model::FilesystemProfile::Posix,
        _ => crate::model::FilesystemProfile::Windows,
    };
    config.unicode = crate::model::UnicodePolicy::Preserve;
    if input.folded {
        config.case_sensitivity = crate::model::CaseSensitivity::ProfileFolded;
    }
    let resource = Resource {
        config,
        lineage: &input.principal.lineage,
        volume: "project",
        generation: 10,
        head: 10,
        path: &input.path,
    };
    let context = Context {
        now: input.now,
        approvals: 2,
    };
    let Ok(trie) = PathTrie::compile(
        &input.policy,
        &input.principal,
        resource,
        context,
        &input.capability,
    ) else {
        return; // Invalid input and policies exceeding compilation budgets fail closed.
    };
    let prepared = trie.prepare(&input.path);
    for action in Action::ALL {
        let expected = decide(
            &input.policy,
            &input.principal,
            action,
            resource,
            context,
            &input.capability,
        );
        assert_eq!(trie.decide(action, &input.path, input.now), expected);
        if let Ok(path) = prepared {
            assert_eq!(path.decide(action, input.now), expected);
        }
    }
    if let Some((encoding, bytes)) = input.native_name {
        use crate::kernel::{LogicalName, NameEncoding};
        let encoding = match encoding % 3 {
            0 => NameEncoding::Utf8,
            1 => NameEncoding::PosixBytes,
            _ => NameEncoding::WindowsUtf16Le,
        };
        if let Ok(name) = LogicalName::new(
            encoding,
            bytes.clone(),
            config.limits.maximum_component_bytes,
        ) {
            assert_eq!(name.as_bytes(), bytes);
            let decoded = match encoding {
                NameEncoding::Utf8 | NameEncoding::PosixBytes => String::from_utf8(bytes).ok(),
                NameEncoding::WindowsUtf16Le => String::from_utf16(
                    &bytes
                        .chunks_exact(2)
                        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                        .collect::<Vec<_>>(),
                )
                .ok(),
            };
            let native = trie.prepare("").expect("root").child_name(&name);
            let expected_encoding = match config.profile {
                crate::model::FilesystemProfile::Portable
                | crate::model::FilesystemProfile::Browser => NameEncoding::Utf8,
                crate::model::FilesystemProfile::Posix => NameEncoding::PosixBytes,
                crate::model::FilesystemProfile::Windows => NameEncoding::WindowsUtf16Le,
            };
            assert_eq!(
                native.is_ok(),
                encoding == expected_encoding
                    && decoded
                        .as_ref()
                        .is_some_and(|text| trie.prepare(text).is_ok())
            );
            if let (Ok(native), Some(path)) = (native, decoded) {
                for action in Action::ALL {
                    assert_eq!(
                        native.decide(action, input.now),
                        decide(
                            &input.policy,
                            &input.principal,
                            action,
                            Resource {
                                path: &path,
                                ..resource
                            },
                            context,
                            &input.capability
                        )
                    );
                }
            }
        }
    }
}

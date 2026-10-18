//! The one policy for root names a host checkout reserves.
//!
//! Every demand, capture and publication path takes its exclusions from
//! [`RESERVED_NAMES`], derived from the single [`RESERVED`] table, so adding a
//! name updates every path at once.
//!
//! Every reserved name is hidden from source demand, never read by capture
//! and preserved on the host by publication, so no path can carry host bytes
//! that another path refuses to refresh. Host tools such as git read these
//! names on the host itself, never through a checkout head.

/// One reserved root name.
struct Reserved {
    name: &'static str,
    /// Subtrees the SDK itself writes into a checkout head under this name.
    /// Everything else a head holds under a reserved name was imported from
    /// the host and is removed when a binding is reconciled.
    owned: &'static [&'static str],
}

// The order is the watcher's fence placement preference: the first existing
// directory hosts fence cookies, and a root with none gains the first name.
const RESERVED: [Reserved; 4] = [
    // Host-side SDK staging, such as watcher fence cookies.
    Reserved {
        name: ".acyclic-sdk",
        owned: &[],
    },
    // The host's own repository. Bash runs host git against it in place.
    Reserved {
        name: ".git",
        owned: &[],
    },
    // SDK state, never checkout content.
    Reserved {
        name: ".system",
        owned: &[
            "execution",
            "harness-file-metadata",
            "harness-uploads",
            "inherited-conversation",
            "interactions",
            "reserved-names",
            "workflows",
        ],
    },
    // Volume projections, mounted over the checkout and never written into
    // its head.
    Reserved {
        name: ".volumes",
        owned: &[],
    },
];

const fn names() -> [&'static str; RESERVED.len()] {
    let mut names = [""; RESERVED.len()];
    let mut index = 0;
    while index < RESERVED.len() {
        names[index] = RESERVED[index].name;
        index += 1;
    }
    names
}

/// Every reserved root name: hidden from source demand, never read by
/// capture, and preserved on the host by publication.
pub const RESERVED_NAMES: [&str; RESERVED.len()] = names();

/// Whether the SDK owns `relative` in a checkout head: it lies within, or is
/// an ancestor of, an owned subtree of a reserved name. A name without owned
/// subtrees holds nothing the SDK wrote, so not even its own root.
#[cfg(feature = "native-execution")]
pub(crate) fn sdk_owned(relative: &str) -> bool {
    let (name, rest) = relative.split_once('/').unwrap_or((relative, ""));
    let Some(reserved) = RESERVED.iter().find(|reserved| reserved.name == name) else {
        return false;
    };
    (rest.is_empty() && !reserved.owned.is_empty())
        || reserved.owned.iter().any(|owned| {
            rest == *owned
                || rest
                    .strip_prefix(owned)
                    .is_some_and(|tail| tail.starts_with('/'))
        })
}

/// Canonical text of the whole policy. A head records the policy it was
/// reconciled under; any change to the table yields different text and
/// reconciles every head again on its next open.
#[cfg(feature = "native-execution")]
pub(super) fn fingerprint() -> String {
    let mut text = String::from("acyclic-checkout-reserved-names-v2\n");
    for reserved in &RESERVED {
        text.push_str(&format!(
            "{} owned={}\n",
            reserved.name,
            reserved.owned.join(",")
        ));
    }
    text
}

#[cfg(all(test, feature = "native-execution"))]
mod tests {
    use super::*;

    #[test]
    fn only_owned_subtrees_and_their_ancestors_are_sdk_owned() {
        for owned in [
            ".system",
            ".system/execution",
            ".system/execution/operation/digest.json",
            ".system/inherited-conversation/prefix.json",
            ".system/reserved-names",
        ] {
            assert!(sdk_owned(owned), "{owned}");
        }
        for imported in [
            ".volumes",
            ".volumes/scratch/file",
            ".acyclic-sdk/fence",
            ".system/existing",
            ".system/executions/file",
            ".git",
            ".git/config",
            "docs/.system/execution",
        ] {
            assert!(!sdk_owned(imported), "{imported}");
        }
    }
}

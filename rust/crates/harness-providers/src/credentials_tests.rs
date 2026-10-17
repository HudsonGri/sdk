use super::*;
use crate::{HttpRequest, HttpResponse, HttpTransport, Registry};
use acyclic_stream::BoxProviderFuture;
use proptest::prelude::*;
use std::{cell::Cell, path::Path, sync::Arc};

thread_local! {
    /// Configured keys copied out of their configuration on this thread.
    pub(super) static CONFIGURED_COPIES: Cell<usize> = const { Cell::new(0) };
}

struct Offline;

impl HttpTransport for Offline {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async { Err(crate::invalid("credential fixtures never send")) })
    }
}

fn io(error: std::io::Error) -> acyclic_harness::Error {
    crate::invalid(error.to_string())
}

fn map(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(name, value)| ((*name).into(), (*value).into()))
        .collect()
}

/// Fake keychain, environment and home: the only native sources a test build can hold.
fn isolated(
    configured: &[(&str, &str)],
    keychain: &[(&str, &str)],
    environment: &[(&str, &str)],
    home: &Path,
) -> Credentials {
    Credentials {
        native: Some(NativeSources(Sources::Isolated {
            keychain: map(keychain),
            environment: map(environment),
            home: home.into(),
        })),
        ..Credentials::explicit(map(configured))
    }
}

fn login(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root).map_err(io)?;
    std::fs::write(root.join("auth.json"), b"{}").map_err(io)
}

fn uses_codex_login(credentials: Credentials) -> Result<bool> {
    let resolved = Registry::with_transport(credentials, Arc::new(Offline)).resolve("openai/gpt-5");
    match resolved {
        Ok(model) => {
            assert!(model.key.is_none());
            assert_eq!(
                model.config.base_url,
                "https://chatgpt.com/backend-api/codex"
            );
            Ok(model.codex_login.is_some())
        }
        Err(acyclic_harness::Error::Invalid(reason)) => {
            assert_eq!(reason, "model provider API key is unavailable");
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

#[test]
fn native_sources_precede_configuration_and_blank_keys_fall_through() -> Result<()> {
    let home = tempfile::tempdir().map_err(io)?;
    for (provider, variable) in [
        ("anthropic", "ANTHROPIC_API_KEY"),
        ("openai", "OPENAI_API_KEY"),
        ("openrouter", "OPENROUTER_API_KEY"),
        ("gemini", "GEMINI_API_KEY"),
    ] {
        for (keychain, environment, configured, expected) in [
            (
                Some("keychain"),
                Some("env"),
                Some("config"),
                Some("keychain"),
            ),
            (None, Some("env"), Some("config"), Some("env")),
            (Some(" \n"), Some(""), Some("config"), Some("config")),
            (None, None, Some(" \t"), None),
            (None, None, None, None),
        ] {
            let credentials = isolated(
                &configured
                    .map(|key| (provider, key))
                    .into_iter()
                    .collect::<Vec<_>>(),
                &keychain
                    .map(|key| (provider, key))
                    .into_iter()
                    .collect::<Vec<_>>(),
                &environment
                    .map(|key| (variable, key))
                    .into_iter()
                    .collect::<Vec<_>>(),
                home.path(),
            );
            assert_eq!(credentials.api_key(provider)?.as_deref(), expected);
        }
    }
    let gemini = isolated(&[], &[], &[("GOOGLE_API_KEY", "google")], home.path());
    assert_eq!(gemini.api_key("gemini")?.as_deref(), Some("google"));
    // Variables belong to their provider; unknown providers read configuration only.
    let other = isolated(&[], &[], &[("OPENAI_API_KEY", "openai")], home.path());
    assert_eq!(other.api_key("anthropic")?, None);
    assert_eq!(other.api_key("custom")?, None);
    Ok(())
}

#[test]
fn codex_login_is_found_only_through_enabled_sources_and_codex_home_wins() -> Result<()> {
    let home = tempfile::tempdir().map_err(io)?;
    let codex_home = tempfile::tempdir().map_err(io)?;
    let codex_home_text = codex_home
        .path()
        .to_str()
        .ok_or_else(|| crate::invalid("path"))?;
    assert!(!uses_codex_login(isolated(&[], &[], &[], home.path()))?);
    login(&home.path().join(".codex"))?;
    assert!(uses_codex_login(isolated(&[], &[], &[], home.path()))?);
    // Same files on disk: explicit credentials have no sources, so nothing is discovered.
    assert!(!uses_codex_login(Credentials::explicit(BTreeMap::new()))?);
    // An empty CODEX_HOME hides the home login rather than falling back to it.
    let selected = [("CODEX_HOME", codex_home_text)];
    assert!(!uses_codex_login(isolated(
        &[],
        &[],
        &selected,
        home.path()
    ))?);
    login(codex_home.path())?;
    std::fs::remove_file(home.path().join(".codex/auth.json")).map_err(io)?;
    assert!(uses_codex_login(isolated(
        &[],
        &[],
        &selected,
        home.path()
    ))?);
    // An API key from native sources outranks the Codex login.
    let keyed = isolated(&[], &[("openai", "fixture-key")], &selected, home.path());
    let model = Registry::with_transport(keyed, Arc::new(Offline)).resolve("openai/gpt-5")?;
    assert_eq!(model.key.as_deref(), Some("fixture-key"));
    assert!(model.codex_login.is_none());
    Ok(())
}

#[test]
fn credential_files_hold_configured_keys_only() -> Result<()> {
    let directory = tempfile::tempdir().map_err(io)?;
    let missing = Credentials::from_file(&directory.path().join("missing.json"))?;
    assert!(missing.native_sources().is_none());
    assert_eq!(missing.api_key("openai")?, None);
    let path = directory.path().join("credentials.json");
    std::fs::write(&path, br#"{"api_keys":{"openai":"configured"}}"#).map_err(io)?;
    let configured = Credentials::from_file(&path)?;
    assert!(configured.native_sources().is_none());
    assert_eq!(configured.api_key("openai")?.as_deref(), Some("configured"));
    for invalid in [
        &br#"{"api_keys":{},"native_sources":"secret-value"}"#[..],
        br#"{}"#,
    ] {
        std::fs::write(&path, invalid).map_err(io)?;
        let Err(error) = Credentials::from_file(&path) else {
            return Err(crate::invalid("invalid credential file accepted"));
        };
        assert!(!error.to_string().contains("secret-value"));
    }
    Ok(())
}

fn copies<T>(lookup: impl FnOnce() -> T) -> (T, usize) {
    CONFIGURED_COPIES.with(|copies| copies.set(0));
    let value = lookup();
    (value, CONFIGURED_COPIES.with(Cell::get))
}

#[test]
fn blank_configured_keys_are_rejected_before_they_are_copied() -> Result<()> {
    let home = tempfile::tempdir().map_err(io)?;
    let configured = [
        ("openai", "configured"),
        (
            "anthropic",
            " 	
",
        ),
        ("gemini", ""),
    ];
    for credentials in [
        Credentials::explicit(map(&configured)),
        isolated(
            &configured,
            &[("openai", " ")],
            &[("OPENAI_API_KEY", "")],
            home.path(),
        ),
    ] {
        for provider in ["anthropic", "gemini", "missing"] {
            let (key, count) = copies(|| credentials.api_key(provider));
            assert_eq!((key?, count), (None, 0));
        }
        let (key, count) = copies(|| credentials.api_key("openai"));
        assert_eq!((key?.as_deref(), count), (Some("configured"), 1));
    }
    // Native keys are already owned: resolving one copies nothing from configuration.
    let native = isolated(&configured, &[("openai", "keychain")], &[], home.path());
    let (key, count) = copies(|| native.api_key("openai"));
    assert_eq!((key?.as_deref(), count), (Some("keychain"), 0));
    Ok(())
}

const SEEDS: [&[u8]; 8] = [
    include_bytes!("../fuzz/corpus/credential_file/keys"),
    include_bytes!("../fuzz/corpus/credential_file/key-as-map"),
    include_bytes!("../fuzz/corpus/credential_file/unquoted-key"),
    include_bytes!("../fuzz/corpus/credential_file/unknown-field"),
    include_bytes!("../fuzz/corpus/credential_file/duplicate-field"),
    include_bytes!("../fuzz/corpus/credential_file/empty"),
    include_bytes!("../fuzz/corpus/credential_file/invalid-utf8-secret"),
    include_bytes!("../fuzz/corpus/credential_file/key-list"),
];

#[test]
fn credential_file_seed_corpus_satisfies_the_oracle() {
    for seed in SEEDS {
        Credentials::assert_file_invariants(seed);
    }
    // The seeds reach both outcomes, with the secret inside rejected documents.
    let (secret, document) = oracle::case(SEEDS[0]);
    assert_eq!(secret, "sk-live-secret");
    assert!(Credentials::from_json(&document).is_ok());
    for seed in &SEEDS[1..5] {
        let (secret, document) = oracle::case(seed);
        assert!(
            document
                .windows(secret.len())
                .any(|w| w == secret.as_bytes())
        );
        assert!(Credentials::from_json(&document).is_err());
    }
}

fn vocabulary() -> String {
    crate::invalid(INVALID_FILE).to_string()
}

fn check(
    document: &[u8],
    secret: &str,
    load: &dyn Fn(&[u8]) -> std::result::Result<oracle::Loaded, String>,
) -> std::result::Result<(), String> {
    oracle::check(document, secret, &[&vocabulary()], load)
}

#[test]
fn the_oracle_rejects_leaky_lossy_and_native_loaders() {
    let violation =
        |document: &[u8], loader: &dyn Fn(&[u8]) -> std::result::Result<oracle::Loaded, String>| {
            match check(document, "sk-live-secret", loader) {
                Ok(()) => panic!("oracle accepted a faulty loader"),
                Err(violation) => violation,
            }
        };
    // An error built from the parser's message echoes the key.
    let leaky = |document: &[u8]| -> std::result::Result<oracle::Loaded, String> {
        serde_json::from_slice::<CredentialFile>(document).map_err(|error| error.to_string())?;
        Err("unreachable".into())
    };
    let leaked: &[u8] = br#"{"api_keys":"sk-live-secret"}"#;
    let leak = violation(leaked, &leaky);
    assert!(leak.contains("echoed key material"), "{leak}");
    // Dropping blank keys, enabling native sources or resolving blanks are all caught.
    let accepted: &[u8] = br#"{"api_keys":{"openai":"key","anthropic":"  "}}"#;
    let faulty = |edit: fn(&mut oracle::Loaded)| {
        move |document: &[u8]| {
            let mut loaded = load(document)?;
            edit(&mut loaded);
            Ok(loaded)
        }
    };
    let lossy = violation(
        accepted,
        &faulty(|loaded| loaded.keys.retain(|_, key| !key.trim().is_empty())),
    );
    assert!(lossy.contains("not preserved"), "{lossy}");
    let native = violation(accepted, &faulty(|loaded| loaded.native = true));
    assert!(native.contains("native"), "{native}");
    let blank = violation(
        accepted,
        &faulty(|loaded| {
            loaded
                .resolved
                .insert("anthropic".into(), Some("  ".into()));
        }),
    );
    assert!(blank.contains("lookup"), "{blank}");
    // The real loader passes the same documents.
    check(accepted, "sk-live-secret", &load).unwrap();
    check(leaked, "sk-live-secret", &load).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn configured_keys_round_trip_exactly(keys in prop::collection::btree_map("[a-z@ -]{0,8}", "[ \t\n\"@a-zA-Z0-9-]{0,16}", 0..5)) {
        let document = serde_json::to_vec(&serde_json::json!({ "api_keys": keys })).unwrap();
        check(&document, "", &load).unwrap();
        let loaded = load(&document).unwrap();
        prop_assert_eq!(loaded.keys, keys);
        prop_assert!(!loaded.native);
    }

    #[test]
    fn rejected_documents_never_echo_the_secret(
        secret in "[a-zA-Z0-9_-]{4,24}",
        template in prop::sample::select(vec![
            r#"{"api_keys":"@"}"#,
            r#"{"api_keys":{"openai":@}}"#,
            r#"{"api_keys":["@"]}"#,
            r#"{"api_keys":{"openai":"@"},"native":"@"}"#,
            r#"{"api_keys":{"openai":"@"},"api_keys":{}}"#,
            r#"{"api_keys":{"@":1}}"#,
            "@",
        ]),
    ) {
        let document = template.replace('@', &secret);
        prop_assert!(Credentials::from_json(document.as_bytes()).is_err());
        check(document.as_bytes(), &secret, &load).unwrap();
    }
}

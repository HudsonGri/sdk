use acyclic_harness::Result;
use std::collections::BTreeMap;

/// API keys from configuration, optionally preceded by the current user's native sources.
/// Anthropic accepts API keys only; no subscription login is read.
///
/// Native sources are never implicit: there is no `Default` or deserialization, so only
/// [`Credentials::native`] reads the keychain, environment or Codex CLI login.
///
/// ```compile_fail
/// let _: acyclic_harness_providers::Credentials = Default::default();
/// ```
///
/// ```compile_fail
/// let _: acyclic_harness_providers::Credentials = serde_json::from_str("{}").unwrap();
/// ```
pub struct Credentials {
    /// Provider ID to API key. Deliberately not Debug or Serialize.
    api_keys: BTreeMap<String, String>,
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<NativeSources>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    api_keys: BTreeMap<String, String>,
}

impl Credentials {
    /// Uses only supplied keys, with no keychain, environment or filesystem reads.
    pub fn explicit(api_keys: BTreeMap<String, String>) -> Self {
        Self {
            api_keys,
            #[cfg(not(target_arch = "wasm32"))]
            native: None,
        }
    }

    /// Consults the current user's keychain, then provider environment variables, ahead of
    /// configured keys, and lets the registry use `$CODEX_HOME` or `~/.codex` CLI logins.
    /// Not compiled into this crate's tests, whose fixtures use isolated sources instead.
    #[cfg(all(not(target_arch = "wasm32"), not(test)))]
    #[must_use]
    pub fn native(self) -> Self {
        Self {
            native: Some(NativeSources(Sources::Current)),
            ..self
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_sources(&self) -> Option<&NativeSources> {
        self.native.as_ref()
    }

    /// Resolves enabled native sources, then this configuration.
    pub fn api_key(&self, provider: &str) -> Result<Option<String>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            if let Some(key) = native.api_key(provider)? {
                return Ok(Some(key));
            }
        }
        Ok(self
            .api_keys
            .get(provider)
            .map(String::as_str)
            .filter(|key| usable(key))
            .map(owned))
    }

    /// Reads configured keys from a JSON credential file. Missing files mean no keys.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => Self::from_json(&bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::explicit(BTreeMap::new()))
            }
            Err(_) => Err(super::invalid("cannot read provider credential file")),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice::<CredentialFile>(bytes)
            .map(|file| Self::explicit(file.api_keys))
            .map_err(|_| super::invalid(INVALID_FILE))
    }

    /// Fuzz entry: loading any credential file keeps native sources disabled, preserves
    /// configured keys exactly, round-trips, and never echoes key material in errors.
    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
    #[doc(hidden)]
    pub fn assert_file_invariants(input: &[u8]) {
        let (secret, document) = oracle::case(input);
        let vocabulary = super::invalid(INVALID_FILE).to_string();
        if let Err(violation) = oracle::check(&document, &secret, &[&vocabulary], &load) {
            panic!("credential file oracle: {violation}");
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
const INVALID_FILE: &str = "invalid provider credential file";

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
#[path = "../../harness/src/search/config/credential_oracle.rs"]
mod oracle;

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
fn load(document: &[u8]) -> std::result::Result<oracle::Loaded, String> {
    let credentials = Credentials::from_json(document).map_err(|error| error.to_string())?;
    let resolved = credentials
        .api_keys
        .keys()
        .map(|name| Ok((name.clone(), credentials.api_key(name)?)))
        .collect::<Result<_>>()
        .map_err(|error| error.to_string())?;
    Ok(oracle::Loaded {
        encoded: serde_json::to_vec(&serde_json::json!({ "api_keys": credentials.api_keys }))
            .map_err(|error| error.to_string())?,
        keys: credentials.api_keys.clone(),
        native: credentials.native.is_some(),
        resolved,
    })
}

fn present(key: Option<String>) -> Option<String> {
    key.filter(|key| usable(key))
}

fn usable(key: &str) -> bool {
    !key.trim().is_empty()
}

/// The single copy of a configured key out of its configuration.
fn owned(key: &str) -> String {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    tests::CONFIGURED_COPIES.with(|copies| copies.set(copies.get() + 1));
    key.to_owned()
}

/// The only reader of host credential state. Test builds compile just the isolated
/// variant, so this crate's fixtures cannot reach a real keychain, environment or home.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeSources(Sources);

#[cfg(not(target_arch = "wasm32"))]
enum Sources {
    #[cfg(not(test))]
    Current,
    #[cfg(test)]
    Isolated {
        keychain: BTreeMap<String, String>,
        environment: BTreeMap<String, String>,
        home: std::path::PathBuf,
    },
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeSources {
    fn api_key(&self, provider: &str) -> Result<Option<String>> {
        if let Some(key) = present(self.keychain(provider)?) {
            return Ok(Some(key));
        }
        let variables: &[&str] = match provider {
            "anthropic" => &["ANTHROPIC_API_KEY"],
            "openai" => &["OPENAI_API_KEY"],
            "openrouter" => &["OPENROUTER_API_KEY"],
            "gemini" => &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
            _ => &[],
        };
        Ok(variables.iter().find_map(|variable| {
            present(
                self.variable(variable)
                    .and_then(|key| key.into_string().ok()),
            )
        }))
    }

    /// `$CODEX_HOME`, else `.codex` in the user's home directory.
    pub(crate) fn codex_home(&self) -> Option<std::path::PathBuf> {
        self.variable("CODEX_HOME")
            .map(Into::into)
            .or_else(|| self.home().map(|home| home.join(".codex")))
    }

    fn keychain(&self, provider: &str) -> Result<Option<String>> {
        match &self.0 {
            #[cfg(not(test))]
            Sources::Current => {
                let Ok(entry) = keyring::Entry::new("acyclic-harness", provider) else {
                    return Ok(None);
                };
                match entry.get_password() {
                    Ok(key) => Ok(Some(key)),
                    Err(
                        keyring::Error::NoEntry
                        | keyring::Error::NoStorageAccess(_)
                        | keyring::Error::PlatformFailure(_),
                    ) => Ok(None),
                    Err(_) => Err(super::invalid("cannot decode keychain credential")),
                }
            }
            #[cfg(test)]
            Sources::Isolated { keychain, .. } => Ok(keychain.get(provider).cloned()),
        }
    }

    fn variable(&self, name: &str) -> Option<std::ffi::OsString> {
        match &self.0 {
            #[cfg(not(test))]
            Sources::Current => std::env::var_os(name),
            #[cfg(test)]
            Sources::Isolated { environment, .. } => environment.get(name).map(Into::into),
        }
    }

    fn home(&self) -> Option<std::path::PathBuf> {
        match &self.0 {
            #[cfg(not(test))]
            Sources::Current => {
                std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).map(Into::into)
            }
            #[cfg(test)]
            Sources::Isolated { home, .. } => Some(home.clone()),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "credentials_tests.rs"]
mod tests;

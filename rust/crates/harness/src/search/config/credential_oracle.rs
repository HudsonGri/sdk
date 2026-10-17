//! Shared oracle for JSON credential loaders: the provider credential file and the search
//! configuration. Depends on `std` and `serde_json` only, so either crate includes it by path.
use std::collections::BTreeMap;

/// What a loader produced for a document, viewed through its public behaviour.
pub(crate) struct Loaded {
    /// Every configured key, exactly as stored.
    pub(crate) keys: BTreeMap<String, String>,
    /// Whether any native source (keychain, environment, Codex login) is enabled.
    pub(crate) native: bool,
    /// Lookups the loader resolved, by configured key name.
    pub(crate) resolved: BTreeMap<String, Option<String>>,
    /// The loaded configuration encoded back into the loader's document format.
    pub(crate) encoded: Vec<u8>,
}

/// Splits fuzz input into a secret and a document. The first byte selects the secret length
/// (mod 32); each `@` in the remaining template becomes the secret, so key material reaches
/// valid and malformed positions alike.
pub(crate) fn case(input: &[u8]) -> (String, Vec<u8>) {
    let Some((&length, rest)) = input.split_first() else {
        return (String::new(), Vec::new());
    };
    let (secret, template) = rest.split_at(usize::from(length % 32).min(rest.len()));
    let secret = String::from_utf8_lossy(secret).into_owned();
    let mut document = Vec::with_capacity(template.len());
    for &byte in template {
        if byte == b'@' {
            document.extend_from_slice(secret.as_bytes());
        } else {
            document.push(byte);
        }
    }
    (secret, document)
}

/// Checks one load: native sources stay disabled, accepted documents preserve configured keys
/// exactly and round-trip to a fixed point, lookups ignore blank keys, and rejections never
/// echo key material. `vocabulary` holds the loader's fixed error texts.
pub(crate) fn check(
    document: &[u8],
    secret: &str,
    vocabulary: &[&str],
    load: &dyn Fn(&[u8]) -> Result<Loaded, String>,
) -> Result<(), String> {
    match load(document) {
        Ok(loaded) => {
            accepted(document, &loaded)?;
            let replay = load(&loaded.encoded)
                .map_err(|error| format!("re-encoded configuration rejected: {error}"))?;
            accepted(&loaded.encoded, &replay)?;
            if replay.keys != loaded.keys || replay.encoded != loaded.encoded {
                return Err("re-encoding is not a fixed point".into());
            }
            Ok(())
        }
        Err(error) => sanitized(&error, document, secret, vocabulary),
    }
}

fn accepted(document: &[u8], loaded: &Loaded) -> Result<(), String> {
    if loaded.native {
        return Err("a loaded document enabled native sources".into());
    }
    let Ok(serde_json::Value::Object(fields)) = serde_json::from_slice(document) else {
        return Err("accepted a document that is not a JSON object".into());
    };
    let expected = match fields.get("api_keys") {
        None => BTreeMap::new(),
        Some(serde_json::Value::Object(keys)) => keys
            .iter()
            .map(|(name, key)| Some((name.clone(), key.as_str()?.to_owned())))
            .collect::<Option<_>>()
            .ok_or("accepted a non-string key")?,
        Some(_) => return Err("accepted non-object api_keys".into()),
    };
    if loaded.keys != expected {
        return Err("configured keys were not preserved exactly".into());
    }
    for (name, resolved) in &loaded.resolved {
        let key = loaded.keys.get(name).filter(|key| !key.trim().is_empty());
        if resolved.as_ref() != key {
            return Err(format!("lookup of {name:?} disagrees with configuration"));
        }
    }
    Ok(())
}

fn sanitized(
    error: &str,
    document: &[u8],
    secret: &str,
    vocabulary: &[&str],
) -> Result<(), String> {
    let mut material = vec![secret.to_owned()];
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(document) {
        strings(&value, &mut material);
    }
    let echoed = material.iter().find(|text| {
        text.chars().count() >= 4
            && error.contains(text.as_str())
            && !vocabulary.iter().any(|fixed| fixed.contains(text.as_str()))
    });
    match echoed {
        Some(text) => Err(format!("error echoed key material {text:?}")),
        None => Ok(()),
    }
}

fn strings(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::String(text) => out.push(text.clone()),
        serde_json::Value::Array(items) => items.iter().for_each(|item| strings(item, out)),
        serde_json::Value::Object(fields) => fields.values().for_each(|item| strings(item, out)),
        _ => {}
    }
}

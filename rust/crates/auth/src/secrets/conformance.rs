//! Portable cases for fresh, disposable scopes. Cloud supplies transports and
//! credentials; neither account selection nor an authority bypass is available.
use super::{Client, Error, Transport};
use crate::contract::*;

/// Canonical language-neutral issue inventory, not a captured Cloud response.
pub const SUITE: &[u8] = include_bytes!("../../conformance/secrets-v1.json");

/// Verify the real provider's CAS, visibility and independent permissions.
/// `allowed` needs all three secrets permissions for this fresh scope; `denied`
/// must carry existing product permissions and no secrets permission. Every
/// failure is fixed text so a failing provider cannot expose protected values.
pub async fn verify<T: Transport, U: Transport>(allowed: &Client<T>, denied: &Client<U>, scope: &str) -> Result<(), &'static str> {
    let initial = allowed.list(&SecretListRequest { scope: scope.into() }).await.map_err(|_| "secrets/list initial failed")?;
    if !initial.variables.is_empty() { return Err("secrets conformance requires a fresh scope"); }
    let protected = SecretPutRequest { scope: scope.into(), name: "protected".into(), kind: SecretKind::Protected,
        value: "conformance-protected-value-never-return".into(), expected_generation: initial.generation };
    let first = allowed.put(&protected).await.map_err(|_| "secrets/put protected failed")?;
    let plain = SecretPutRequest { scope: scope.into(), name: "plain".into(), kind: SecretKind::Plain,
        value: "conformance-plain-value".into(), expected_generation: first.generation };
    let second = allowed.put(&plain).await.map_err(|_| "secrets/put plain failed")?;
    let listed = allowed.list(&SecretListRequest { scope: scope.into() }).await.map_err(|_| "secrets/list visibility failed")?;
    let protected_row = listed.variables.iter().any(|row| matches!(&row.kind,
        SecretVariableKind::Protected(row) if row.name == protected.name && row.version == first.version));
    let plain_row = listed.variables.iter().any(|row| matches!(&row.kind,
        SecretVariableKind::Plain(row) if row.name == plain.name && row.value == plain.value && row.version == second.version));
    if listed.generation != second.generation || listed.variables.len() != 2 || !protected_row || !plain_row {
        return Err("secrets/list visibility or generation diverged");
    }
    let deletion = SecretDeleteRequest { scope: scope.into(), name: protected.name.clone(), expected_generation: initial.generation };
    if allowed.put(&protected).await != Err(Error::Conflict) || allowed.delete(&deletion).await != Err(Error::Conflict) {
        return Err("secrets stale generation did not conflict");
    }
    let mut deletion = deletion;
    deletion.expected_generation = second.generation;
    let mut denied_put = protected.clone();
    denied_put.expected_generation = second.generation;
    if denied.put(&denied_put).await != Err(Error::PermissionDenied)
        || denied.list(&SecretListRequest { scope: scope.into() }).await != Err(Error::PermissionDenied)
        || denied.delete(&deletion).await != Err(Error::PermissionDenied) {
        return Err("secrets existing product grants implied secrets authority");
    }
    let third = allowed.delete(&deletion).await.map_err(|_| "secrets/delete protected failed")?;
    allowed.delete(&SecretDeleteRequest { scope: scope.into(), name: plain.name, expected_generation: third.generation })
        .await.map_err(|_| "secrets/delete plain failed")?;
    Ok(())
}

//! Owner-bounded evidence of strict Ed25519 verification, never authorization.
use super::*;
use crate::verify::{AccountState, RevocationCheck, Trust};
use std::sync::Mutex;

#[derive(Default)]
struct Entries {
    digests: [Option<[u8; 32]>; 32],
    next: usize,
}

/// Fixed storage owned by one verifier. Only successful strict verifications
/// enter this cache. Entries bind the public key, signature and full transcript;
/// no token, secret, trust decision or validity interval is retained.
#[derive(Default)]
pub struct SignatureCache(Mutex<Entries>);

impl SignatureCache {
    /// Pair reusable signature evidence with the owner's current trust snapshot.
    /// The ordinary verification API still checks that snapshot and time each call.
    pub fn with_trust<'a>(&'a self, trust: &'a (impl Trust + ?Sized)) -> impl Trust + 'a {
        CachedTrust {
            signatures: self,
            trust,
        }
    }

    pub(crate) fn verify(
        &self,
        key: &VerifyingKey,
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), VerifyError> {
        // Fixed-width key/signature boundaries and message length make this
        // transcript unambiguous. This is the same SHA-256 identity assumption
        // used by capability block revocations.
        let signature = Signature::from_slice(signature).map_err(|_| VerifyError::Signature)?;
        let digest: [u8; 32] = Sha256::new()
            .chain_update(b"acyclic.auth.v1/verified-signature\0")
            .chain_update(key.as_bytes())
            .chain_update(signature.to_bytes())
            .chain_update((message.len() as u64).to_le_bytes())
            .chain_update(message)
            .finalize()
            .into();
        if self
            .0
            .lock()
            .map_err(|_| VerifyError::Trust)?
            .digests
            .contains(&Some(digest))
        {
            return Ok(());
        }
        // No owner lock is held across cryptography, and failed checks never
        // replace evidence. Concurrent misses may independently prove the same fact.
        verify_signature(key, message, &signature.to_bytes())?;
        let mut entries = self.0.lock().map_err(|_| VerifyError::Trust)?;
        let next = entries.next;
        entries.digests[next] = Some(digest);
        entries.next = (next + 1) % entries.digests.len();
        Ok(())
    }
}

struct CachedTrust<'a, T: Trust + ?Sized> {
    signatures: &'a SignatureCache,
    trust: &'a T,
}
impl<T: Trust + ?Sized> Trust for CachedTrust<'_, T> {
    fn environment_keys(&self, environment: &str) -> Result<&[BlockId], VerifyError> {
        self.trust.environment_keys(environment)
    }
    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
        self.trust.account(account)
    }
    fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError> {
        self.trust.revoked(account, check)
    }
    fn signature_cache(&self) -> Option<&SignatureCache> {
        Some(self.signatures)
    }
}

#[cfg(test)]
mod tests;

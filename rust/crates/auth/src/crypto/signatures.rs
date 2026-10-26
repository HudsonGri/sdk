//! Owner-bounded evidence of strict Ed25519 verification, never authorization.
use super::*;
use crate::verify::{AccountState, RevocationCheck, Trust};
use std::sync::Mutex;

#[derive(Default)]
struct Entries {
    evidence: [Option<Evidence>; 32],
}

#[derive(Clone, Copy)]
struct Evidence {
    digest: Option<[u8; 32]>,
    key: VerifyingKey,
}

impl Entries {
    fn promote(&mut self, digest: [u8; 32]) -> bool {
        let Some(index) = self
            .evidence
            .iter()
            .position(|entry| entry.is_some_and(|entry| entry.digest == Some(digest)))
        else {
            return false;
        };
        if let Some(recent) = self.evidence.get_mut(..=index) {
            recent.rotate_right(1);
        }
        true
    }
}

/// Fixed storage owned by one verifier. Entries hold canonical nonweak public
/// keys or successful strict signature proofs binding the key and transcript.
/// No token, secret, trust decision or validity interval is retained.
#[derive(Default)]
pub struct SignatureCache(Mutex<Entries>);

impl SignatureCache {
    /// Reuses successful canonical public-key decoding and weak-point checks.
    /// The same 32 evidence slots own decoded keys and signature transcripts.
    /// No trust decision or caller-provided decoded key is admitted here.
    pub fn public_key(&self, bytes: &[u8]) -> Result<VerifyingKey, VerifyError> {
        let bytes: &[u8; 32] = bytes.try_into().map_err(|_| VerifyError::Malformed)?;
        if let Some(key) = self
            .0
            .lock()
            .map_err(|_| VerifyError::Trust)?
            .evidence
            .iter()
            .flatten()
            .find_map(|entry| (entry.key.as_bytes() == bytes).then_some(entry.key))
        {
            return Ok(key);
        }
        let key = super::public_key(bytes)?;
        let mut entries = self.0.lock().map_err(|_| VerifyError::Trust)?;
        if !entries
            .evidence
            .iter()
            .flatten()
            .any(|entry| entry.key == key)
        {
            entries.evidence.rotate_right(1);
            entries.evidence[0] = Some(Evidence { digest: None, key });
        }
        Ok(key)
    }

    /// Pair reusable signature evidence with the owner's current trust snapshot.
    /// The ordinary verification API still checks that snapshot and time each call.
    pub fn with_trust<'a>(&'a self, trust: &'a (impl Trust + ?Sized)) -> impl Trust + 'a {
        CachedTrust {
            signatures: self,
            trust,
        }
    }

    /// Verify immutable signature evidence. Callers separately enforce their
    /// current trust, domain, audience and validity rules on every use.
    pub fn verify(
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
            .promote(digest)
        {
            return Ok(());
        }
        // No owner lock is held across cryptography, and failed checks never
        // replace evidence. Concurrent misses may independently prove the same fact.
        verify_signature(key, message, &signature.to_bytes())?;
        let mut entries = self.0.lock().map_err(|_| VerifyError::Trust)?;
        if !entries.promote(digest) {
            entries.evidence.rotate_right(1);
            entries.evidence[0] = Some(Evidence {
                digest: Some(digest),
                key: *key,
            });
        }
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

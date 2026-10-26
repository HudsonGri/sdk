//! Read-only Streams admission from the runtime's real public trust boundary.
use super::*;
use acyclic_auth::{contract::*, verify::VerifyError};
use acyclic_stream::{StreamPath, session};
use std::sync::Arc;

impl session::AuthorityState for runtime_signing::PublicAuthority {
    fn snapshot(&self) -> std::result::Result<session::TrustSnapshot, VerifyError> {
        let trust = self.trust.read().map_err(|_| VerifyError::Trust)?;
        Ok(session::TrustSnapshot {
            trust: Arc::new(trust.clone()),
            now_unix_seconds: workload::now(),
            revision: *self.revision.borrow(),
        })
    }

    fn changed(&self, revision: u64) -> acyclic_stream::BoxProviderFuture<'_, ()> {
        // Subscribe before comparing: a revoke between snapshot and this call
        // is observed immediately, and one after comparison wakes the waiter.
        let mut changed = self.revision.subscribe();
        Box::pin(async move {
            if *changed.borrow_and_update() != revision {
                return;
            }
            let _ = changed.changed().await;
        })
    }

    fn expires_at(&self, deadline: u64) -> acyclic_stream::BoxProviderFuture<'_, ()> {
        use acyclic_stream::runtime::Executor as _;
        Box::pin(async move {
            loop {
                let remaining = deadline.saturating_sub(workload::now());
                if remaining == 0 {
                    return;
                }
                let milliseconds =
                    u32::try_from(remaining.saturating_mul(1000)).unwrap_or(u32::MAX);
                acyclic_stream::runtime::PlatformExecutor
                    .sleep(milliseconds)
                    .await;
            }
        })
    }
}

impl AuthorityRoot {
    /// Binds read-only Streams observation at this trusted runtime bootstrap.
    /// The sealed credential carries the supplied scope's expiry and ancestor
    /// revocations. The returned authority retains public trust only.
    pub fn stream_session(
        &self,
        scope: &Scope,
        audience: String,
        prefix: &StreamPath,
    ) -> Result<(Arc<session::Authority>, bytes::Bytes)> {
        let verifier = self.verifier(&mut None)?;
        let timestamp = workload::now();
        let parent = workload::verify(&verifier, &self.audience, scope, timestamp, &mut None)?;
        let authority = session::Authority::new(
            self.runtime.public.clone(),
            audience.clone(),
            Some(self.id.clone()),
        )
        .map_err(|error| Error::Unauthorized(error.to_string()))?;
        let grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: format!("streams:/{prefix}"),
                        prefix: true,
                    },
                    actions: vec![
                        "streams/follow".into(),
                        "streams/list".into(),
                        "streams/read".into(),
                    ],
                }),
            }],
        };
        let mut seed = zeroize::Zeroizing::new([0; 32]);
        seed[..16].copy_from_slice(crate::identity::fresh_uuid().as_bytes());
        seed[16..].copy_from_slice(crate::identity::fresh_uuid().as_bytes());
        let capability = workload::signer_with_grants(
            &self.id,
            &self.runtime.workload,
            timestamp,
            grants.clone(),
            Some(&parent),
        )?
        .mint(
            vec![
                Caveat {
                    kind: CaveatKind::Grants(grants),
                },
                Caveat {
                    kind: CaveatKind::Audience(ResourcePattern {
                        name: audience,
                        prefix: false,
                    }),
                },
            ],
            ed25519_dalek::SigningKey::from_bytes(&seed),
            &self.public_workload_trust()?,
            timestamp,
        )
        .and_then(|capability| capability.seal())
        .map_err(workload::unauthorized)?;
        Ok((
            Arc::new(authority),
            bytes::Bytes::copy_from_slice(&capability.encode()),
        ))
    }
}

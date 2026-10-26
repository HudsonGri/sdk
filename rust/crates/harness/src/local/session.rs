//! One authenticated canonical Streams session pool per local harness.
use super::*;
use acyclic_stream::session::{Authority as SessionAuthority, Client};

/// Reserved credential slot on the harness's shared Streams session.
pub const LOCAL_SESSION_CREDENTIAL_SLOT: u32 = 0;

/// Public local observation binding. The credential is sealed and read-only;
/// the authority owns public trust and never owns signing custody.
pub struct LocalSession {
    client: Client,
    authority: Arc<SessionAuthority>,
    credential: bytes::Bytes,
}

impl LocalSession {
    /// Shared canonical connection pool; cloning the client shares this pool.
    pub const fn client(&self) -> &Client {
        &self.client
    }

    /// Public trust, authenticated recipient and local account holder binding.
    pub const fn authority(&self) -> &Arc<SessionAuthority> {
        &self.authority
    }

    /// Sealed credential installed in [`LOCAL_SESSION_CREDENTIAL_SLOT`].
    pub const fn credential(&self) -> &bytes::Bytes {
        &self.credential
    }
}

impl LocalHarness {
    /// The one public observation binding for this harness's provider.
    /// Slot zero authorizes read-only observation under `harness`; trust
    /// revocation and expiry are enforced by the canonical session authority.
    pub async fn session_binding(&self) -> Result<&LocalSession> {
        self.session
            .get_or_try_init(|| async {
                let root = self.agent_authority.as_ref().ok_or_else(|| {
                    Error::Unsupported("embedded harness has no local session issuer".into())
                })?;
                let (authority, credential) = root.stream_session(
                    &self.signed,
                    format!("harness:/sessions/{}", self.agent),
                    &acyclic_stream::StreamPath::new("harness").map_err(storage)?,
                )?;
                let client = Client::in_process(
                    self.runtime.task_host().workflow_stream(),
                    authority.clone(),
                    Arc::new(acyclic_stream::runtime::PlatformExecutor),
                );
                client
                    .register_credential(LOCAL_SESSION_CREDENTIAL_SLOT, credential.clone())
                    .map_err(storage)?;
                Ok(LocalSession {
                    client,
                    authority,
                    credential,
                })
            })
            .await
    }

    /// Shared canonical Streams Client for WireSession and other local views.
    pub async fn session_client(&self) -> Result<&Client> {
        Ok(self.session_binding().await?.client())
    }
}

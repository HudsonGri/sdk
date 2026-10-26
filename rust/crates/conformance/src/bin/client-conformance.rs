//! Real endpoint runner; the host provisions the fixture through AgentHost.
use acyclic_conformance::client::{Fixture, verify};
use acyclic_harness::agent::{AgentId, AgentPath};
use acyclic_stream::grpc::Client;
use std::io::Read as _;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("ACYCLIC_CONFORMANCE_ENDPOINT")?;
    let token = std::env::var("ACYCLIC_CONFORMANCE_TOKEN")?;
    let namespace = AgentPath::root(&AgentId::try_from(std::env::var("ACYCLIC_CONFORMANCE_NAMESPACE")?)?);
    let path = std::env::var("ACYCLIC_CONFORMANCE_AGENT_FIXTURE")?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 { return Err("CLIENT_INPUT fixture exceeds 65536 bytes".into()); }
    let fixture: Fixture = serde_json::from_slice(&bytes)?;
    if fixture.agent != namespace && !fixture.agent.as_str().starts_with(&format!("{}/children/", namespace.as_str())) {
        return Err("CLIENT_INPUT fixture is outside the authorized namespace".into());
    }
    let credential = std::env::var("ACYCLIC_CONFORMANCE_SESSION_CREDENTIAL")?;
    if credential.len() > 131_072 { return Err("CLIENT_INPUT credential exceeds byte bound".into()); }
    let credential = hex::decode(credential)?;
    let provider = match std::env::var("ACYCLIC_CONFORMANCE_CA") {
        Ok(path) => Client::connect_eager_with_ca_certificate(&endpoint, &token, std::fs::read(path)?).await?,
        Err(std::env::VarError::NotPresent) => Client::connect_eager(&endpoint, &token).await?,
        Err(error) => return Err(error.into()),
    };
    let session = provider.session_client()?;
    session.register_credential(0, credential.into())?;
    let report = verify(&provider, session, 0, &fixture).await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

//! Real shared client + signed authority, against a disposable contract provider.
#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{authority::{PolicyDecision, Target as AuthorityTarget}, contract::*, credential::CredentialProvider,
    secrets::{self, Client, Error, Response, Transport, TransportFuture}, verify::{VerifiedCapability, verify}};
use futures::executor::block_on;
use prost::Message;
use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use zeroize::Zeroizing;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const PLANTED: &str = "PLANTED_SECRET_DO_NOT_LOG_6547";

fn grants() -> GrantSet {
    GrantSet { grants: [SecretsOperation::Put, SecretsOperation::List, SecretsOperation::Delete]
        .into_iter().map(|operation| Grant { kind: GrantKind::Secrets(SecretsGrant {
            operation, scope: "fixture".into(), name: None,
        }) }).collect() }
}
fn capability(allowed: bool) -> Result<VerifiedCapability, Box<dyn std::error::Error>> {
    let bytes = signed::root(|block| {
        if allowed && let Some(certs) = &mut block.certificates {
            for cert in &mut certs.keys { cert.key.grants = grants(); }
        }
    });
    Ok(verify(&bytes, &signed::Snapshot::default(), signed::NOW)?)
}

#[derive(Default)]
struct State { generation: u64, versions: BTreeMap<String, u64>, rows: BTreeMap<String, SecretPutRequest> }
struct Provider { state: Arc<Mutex<State>>, authority: VerifiedCapability, ignore_cas: bool, leak: bool }
fn response(status: u16, value: impl serde::Serialize) -> Result<Response, Error> {
    Ok(Response { status, body: Zeroizing::new(serde_json::to_vec(&value).map_err(|_| Error::InvalidResponse)?) })
}
impl Provider {
    fn call(&self, path: &str, body: &[u8]) -> Result<Response, Error> {
        let (operation, scope, name, expected, put) = match path {
            "/v1/identity/secrets/put" => {
                let request: SecretPutRequest = serde_json::from_slice(body).map_err(|_| Error::InvalidRequest)?;
                (SecretsOperation::Put, request.scope.clone(), Some(request.name.clone()), Some(request.expected_generation), Some(request))
            }
            "/v1/identity/secrets/list" => {
                let request: SecretListRequest = serde_json::from_slice(body).map_err(|_| Error::InvalidRequest)?;
                (SecretsOperation::List, request.scope, None, None, None)
            }
            "/v1/identity/secrets/delete" => {
                let request: SecretDeleteRequest = serde_json::from_slice(body).map_err(|_| Error::InvalidRequest)?;
                (SecretsOperation::Delete, request.scope, Some(request.name), Some(request.expected_generation), None)
            }
            _ => return Err(Error::InvalidRequest),
        };
        let attributes = RecordAttributes::default();
        let usage = signed::limits(None);
        let mut request = signed::request("fs:/fixture", &attributes, &usage);
        request.target = AuthorityTarget::Secrets(secrets::Target { operation, scope: &scope, name: name.as_deref() });
        if self.authority.authorize(&signed::Snapshot::default(), &request, PolicyDecision::Allow).is_err() {
            return response(403, PLANTED);
        }
        let mut state = self.state.lock().map_err(|_| Error::Transport)?;
        if expected.is_some_and(|generation| generation != state.generation) && !self.ignore_cas { return response(409, PLANTED); }
        if operation == SecretsOperation::List {
            if self.leak {
                return response(200, serde_json::json!({"generation":state.generation,"variables":[
                    {"kind":"protected","name":"protected","version":1,"value":PLANTED}]}));
            }
            let variables = state.rows.iter().map(|(name, row)| {
                let version = state.versions.get(name).copied().unwrap_or(0);
                SecretVariable { kind: match row.kind {
                    SecretKind::Plain => SecretVariableKind::Plain(PlainSecretVariable { name: name.clone(), version, value: row.value.clone() }),
                    _ => SecretVariableKind::Protected(ProtectedSecretVariable { name: name.clone(), version }),
                }}
            }).collect();
            return response(200, SecretListReply { generation: state.generation, variables });
        }
        let name = name.ok_or(Error::InvalidRequest)?;
        state.generation += 1;
        let version = state.versions.entry(name.clone()).or_default();
        *version += 1;
        let version = *version;
        let kind = if let Some(row) = put { let kind = row.kind; state.rows.insert(name.clone(), row); kind }
            else { state.rows.remove(&name); SecretKind::Deleted };
        response(200, SecretMutationReply { name, generation: state.generation, version, kind })
    }
}
impl Transport for Provider {
    fn post<'a>(&'a self, path: &'static str, _bearer: &'a str, body: &'a [u8]) -> TransportFuture<'a> {
        Box::pin(async move { self.call(path, body) })
    }
}
fn client(state: &Arc<Mutex<State>>, allowed: bool, ignore_cas: bool, leak: bool) -> Result<Client<Provider>, Box<dyn std::error::Error>> {
    Ok(Client::new(Provider { state: state.clone(), authority: capability(allowed)?, ignore_cas, leak },
        CredentialProvider::new(|| async { Ok("fixture-bearer".into()) })))
}

#[test]
fn secrets_provider_conformance_and_cas_authority_visibility_negative_controls() -> TestResult {
    block_on(async {
        for (ignore_cas, leak, should_pass) in [(false,false,true), (true,false,false), (false,true,false)] {
            let state = Arc::new(Mutex::new(State::default()));
            let allowed = client(&state, true, ignore_cas, leak)?;
            let denied = client(&state, false, false, false)?;
            let result = secrets::conformance::verify(&allowed, &denied, "fixture").await;
            assert_eq!(result.is_ok(), should_pass, "CAS/visibility mutant escaped provider conformance");
            if let Err(error) = result { assert!(!error.contains(PLANTED)); }
        }
        // Giving the supposedly denied client secrets grants is a second
        // negative control: the same provider oracle must detect widening.
        let state = Arc::new(Mutex::new(State::default()));
        let allowed = client(&state, true, false, false)?;
        let widened = client(&state, true, false, false)?;
        assert_eq!(secrets::conformance::verify(&allowed, &widened, "fixture").await,
            Err("secrets existing product grants implied secrets authority"));
        Ok(())
    })
}

#[test]
fn secrets_grants_intersect_operation_scope_name_owner_and_attenuation() -> TestResult {
    let authority = capability(true)?;
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::default();
    let usage = signed::limits(None);
    let mut request = signed::request("fs:/fixture", &attributes, &usage);
    for operation in [SecretsOperation::Put, SecretsOperation::List, SecretsOperation::Delete] {
        request.target = AuthorityTarget::Secrets(secrets::Target { operation, scope:"fixture", name: (operation != SecretsOperation::List).then_some("key") });
        assert!(authority.authorize(&trust, &request, PolicyDecision::Allow).is_ok());
        assert!(authority.authorize(&trust, &request, PolicyDecision::Deny).is_err());
        assert!(capability(false)?.authorize(&trust, &request, PolicyDecision::Allow).is_err());
        let narrowed = authority.attenuate(vec![Caveat { kind: CaveatKind::Grants(GrantSet {
            grants: vec![Grant { kind: GrantKind::Secrets(SecretsGrant { operation: SecretsOperation::Put, scope:"fixture".into(), name:Some("only".into()) }) }]
        }) }], &[99;32])?;
        assert!(narrowed.authorize(&trust, &request, PolicyDecision::Allow).is_err());
    }
    for (operation, scope, name, allowed) in [
        (SecretsOperation::Put,"fixture",Some("only"),true),
        (SecretsOperation::Put,"fixture-other",Some("only"),false),
        (SecretsOperation::Put,"fixture",None,false),
        (SecretsOperation::List,"fixture",Some("only"),false),
        (SecretsOperation::Unspecified,"fixture",None,false),
    ] {
        request.target = AuthorityTarget::Secrets(secrets::Target { operation, scope, name });
        assert_eq!(authority.authorize(&trust, &request, PolicyDecision::Allow).is_ok(), allowed);
    }
    Ok(())
}

struct Fault { status: u16, body: Vec<u8> }
impl Transport for Fault {
    fn post<'a>(&'a self, _path: &'static str, _bearer: &'a str, _body: &'a [u8]) -> TransportFuture<'a> {
        Box::pin(async move { Ok(Response { status:self.status, body: Zeroizing::new(self.body.clone()) }) })
    }
}
#[test]
fn secrets_redacts_requests_protobuf_and_every_provider_error() -> TestResult {
    let request = SecretPutRequest { scope:PLANTED.into(), name:PLANTED.into(), kind:SecretKind::Protected, value:PLANTED.into(), expected_generation:0 };
    let wire = SecretPutRequestProto::from(request.clone());
    assert!(!format!("{request:?} {wire:?}").contains(PLANTED), "semantic/protobuf debug exposed a value");
    assert!(String::from_utf8_lossy(&wire.encode_to_vec()).contains(PLANTED), "negative control must plant an actual value");
    for status in [200,400,401,403,409,500,302] {
        let client = Client::new(Fault { status, body:PLANTED.as_bytes().to_vec() }, CredentialProvider::new(|| async { Ok("fixture".into()) }));
        let Err(error) = block_on(client.put(&request)) else { return Err("malformed provider reply admitted".into()); };
        assert!(!format!("{error:?} {error}").contains(PLANTED));
        assert!(std::error::Error::source(&error).is_none(), "provider source chain must be erased");
    }
    let client = Client::new(Fault { status:200, body:Vec::new() }, CredentialProvider::new(|| async {
        Err(acyclic_auth::credential::CredentialError::Provider(std::io::Error::other(PLANTED).into()))
    }));
    let result = block_on(client.put(&request));
    assert_eq!(result, Err(Error::Credential));
    assert!(!format!("{result:?}").contains(PLANTED));
    Ok(())
}

#[test]
fn secrets_contract_rejects_account_selectors_and_protected_value_fields() -> TestResult {
    for request in [serde_json::json!({"scope":"fixture","account":PLANTED}),
        serde_json::json!({"scope":"fixture","reveal":true})] {
        assert!(serde_json::from_value::<SecretListRequest>(request).is_err());
    }
    let leaked = serde_json::json!({"generation":1,"variables":[{"kind":"protected","name":"key","version":1,"value":PLANTED}]});
    assert!(serde_json::from_value::<SecretListReply>(leaked.clone()).is_err());
    assert!(leaked.to_string().contains(PLANTED), "loose JSON negative control must expose the planted value");
    let valid = serde_json::json!({"generation":1,"variables":[{"kind":"protected","name":"key","version":1},
        {"kind":"plain","name":"visible","version":1,"value":"public"}]});
    let reply: SecretListReply = serde_json::from_value(valid.clone())?;
    assert_eq!(serde_json::to_value(reply.clone())?, valid);
    let wire = SecretListReplyProto::from(reply.clone()).encode_to_vec();
    assert_eq!(SecretListReply::try_from(SecretListReplyProto::decode(wire.as_slice())?)?, reply);
    Ok(())
}

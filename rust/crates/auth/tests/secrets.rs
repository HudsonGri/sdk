//! Real shared client + signed authority, against a disposable contract provider.
#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{
    authority::{PolicyDecision, Target as AuthorityTarget},
    contract::*,
    credential::CredentialProvider,
    secrets::{self, Client, Error, Response, Transport, TransportFuture},
    verify::{VerifiedCapability, verify},
};
use futures::executor::block_on;
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use zeroize::Zeroizing;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const PLANTED: &str = "PLANTED_SECRET_DO_NOT_LOG_6547";

fn grants() -> GrantSet {
    GrantSet {
        grants: [
            SecretsOperation::Put,
            SecretsOperation::List,
            SecretsOperation::Delete,
        ]
        .into_iter()
        .map(|operation| Grant {
            kind: GrantKind::Secrets(SecretsGrant {
                operation,
                scope: "fixture".into(),
                name: None,
            }),
        })
        .collect(),
    }
}
fn capability(allowed: bool) -> Result<VerifiedCapability, Box<dyn std::error::Error>> {
    let bytes = signed::root(|block| {
        if allowed && let Some(certs) = &mut block.certificates {
            for cert in &mut certs.keys {
                cert.key.grants = grants();
            }
        }
    });
    Ok(verify(&bytes, &signed::Snapshot::default(), signed::NOW)?)
}

#[derive(Default)]
struct State {
    generation: u64,
    versions: BTreeMap<String, u64>,
    rows: BTreeMap<String, SecretPutRequest>,
}
struct Provider {
    state: Arc<Mutex<State>>,
    authority: VerifiedCapability,
    ignore_cas: bool,
    leak: bool,
}
fn response(status: u16, value: impl serde::Serialize) -> Result<Response, Error> {
    Ok(Response {
        status,
        body: Zeroizing::new(serde_json::to_vec(&value).map_err(|_| Error::InvalidResponse)?),
    })
}
impl Provider {
    fn call(&self, path: &str, body: &[u8]) -> Result<Response, Error> {
        let (operation, scope, name, expected, put) = match path {
            "/v1/identity/secrets/put" => {
                let request: SecretPutRequest =
                    serde_json::from_slice(body).map_err(|_| Error::InvalidRequest)?;
                (
                    SecretsOperation::Put,
                    request.scope.clone(),
                    Some(request.name.clone()),
                    Some(request.expected_generation),
                    Some(request),
                )
            }
            "/v1/identity/secrets/list" => {
                let request: SecretListRequest =
                    serde_json::from_slice(body).map_err(|_| Error::InvalidRequest)?;
                (SecretsOperation::List, request.scope, None, None, None)
            }
            "/v1/identity/secrets/delete" => {
                let request: SecretDeleteRequest =
                    serde_json::from_slice(body).map_err(|_| Error::InvalidRequest)?;
                (
                    SecretsOperation::Delete,
                    request.scope,
                    Some(request.name),
                    Some(request.expected_generation),
                    None,
                )
            }
            _ => return Err(Error::InvalidRequest),
        };
        let attributes = RecordAttributes::default();
        let usage = signed::limits(None);
        let mut request = signed::request("fs:/fixture", &attributes, &usage);
        request.target = AuthorityTarget::Secrets(secrets::Target {
            operation,
            scope: &scope,
            name: name.as_deref(),
        });
        if self
            .authority
            .authorize(
                &signed::Snapshot::default(),
                &request,
                PolicyDecision::Allow,
            )
            .is_err()
        {
            return response(403, PLANTED);
        }
        let mut state = self.state.lock().map_err(|_| Error::Transport)?;
        if expected.is_some_and(|generation| generation != state.generation) && !self.ignore_cas {
            return response(409, PLANTED);
        }
        if operation == SecretsOperation::List {
            return self.list(&state);
        }
        let name = name.ok_or(Error::InvalidRequest)?;
        state.generation += 1;
        let version = state.versions.entry(name.clone()).or_default();
        *version += 1;
        let version = *version;
        let kind = if let Some(row) = put {
            let kind = row.kind;
            state.rows.insert(name.clone(), row);
            kind
        } else {
            state.rows.remove(&name);
            SecretKind::Deleted
        };
        response(
            200,
            SecretMutationReply {
                name,
                generation: state.generation,
                version,
                kind,
            },
        )
    }
    fn list(&self, state: &State) -> Result<Response, Error> {
        if self.leak {
            return response(
                200,
                serde_json::json!({"generation":state.generation,"variables":[
                    {"kind":"protected","name":"protected","version":1,"value":PLANTED}]}),
            );
        }
        let variables = state
            .rows
            .iter()
            .map(|(name, row)| {
                let version = state.versions.get(name).copied().unwrap_or(0);
                SecretVariable {
                    kind: match row.kind {
                        SecretKind::Plain => SecretVariableKind::Plain(PlainSecretVariable {
                            name: name.clone(),
                            version,
                            value: row.value.clone(),
                        }),
                        _ => SecretVariableKind::Protected(ProtectedSecretVariable {
                            name: name.clone(),
                            version,
                        }),
                    },
                }
            })
            .collect();
        response(
            200,
            SecretListReply {
                generation: state.generation,
                variables,
            },
        )
    }
}
impl Transport for Provider {
    fn post<'a>(
        &'a self,
        path: &'static str,
        _bearer: &'a str,
        body: &'a [u8],
    ) -> TransportFuture<'a> {
        Box::pin(async move { self.call(path, body) })
    }
}
fn client(
    state: &Arc<Mutex<State>>,
    allowed: bool,
    ignore_cas: bool,
    leak: bool,
) -> Result<Client<Provider>, Box<dyn std::error::Error>> {
    Ok(Client::new(
        Provider {
            state: state.clone(),
            authority: capability(allowed)?,
            ignore_cas,
            leak,
        },
        CredentialProvider::new(|| async { Ok("fixture-bearer".into()) }),
    ))
}

#[test]
fn secrets_provider_conformance_and_cas_authority_visibility_negative_controls() -> TestResult {
    block_on(async {
        for (ignore_cas, leak, should_pass) in [
            (false, false, true),
            (true, false, false),
            (false, true, false),
        ] {
            let state = Arc::new(Mutex::new(State::default()));
            let allowed = client(&state, true, ignore_cas, leak)?;
            let denied = client(&state, false, false, false)?;
            let result = secrets::conformance::verify(&allowed, &denied, "fixture").await;
            assert_eq!(
                result.is_ok(),
                should_pass,
                "CAS/visibility mutant escaped provider conformance"
            );
            if let Err(error) = result {
                assert!(!error.contains(PLANTED));
            }
        }
        // Giving the supposedly denied client secrets grants is a second
        // negative control: the same provider oracle must detect widening.
        let state = Arc::new(Mutex::new(State::default()));
        let allowed = client(&state, true, false, false)?;
        let widened = client(&state, true, false, false)?;
        assert_eq!(
            secrets::conformance::verify(&allowed, &widened, "fixture").await,
            Err("secrets existing product grants implied secrets authority")
        );
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
    for operation in [
        SecretsOperation::Put,
        SecretsOperation::List,
        SecretsOperation::Delete,
    ] {
        request.target = AuthorityTarget::Secrets(secrets::Target {
            operation,
            scope: "fixture",
            name: (operation != SecretsOperation::List).then_some("key"),
        });
        assert!(
            authority
                .authorize(&trust, &request, PolicyDecision::Allow)
                .is_ok()
        );
        assert!(
            authority
                .authorize(&trust, &request, PolicyDecision::Deny)
                .is_err()
        );
        assert!(
            capability(false)?
                .authorize(&trust, &request, PolicyDecision::Allow)
                .is_err()
        );
        let narrowed = authority.attenuate(
            vec![Caveat {
                kind: CaveatKind::Grants(GrantSet {
                    grants: vec![Grant {
                        kind: GrantKind::Secrets(SecretsGrant {
                            operation: SecretsOperation::Put,
                            scope: "fixture".into(),
                            name: Some("only".into()),
                        }),
                    }],
                }),
            }],
            &[99; 32],
        )?;
        assert!(
            narrowed
                .authorize(&trust, &request, PolicyDecision::Allow)
                .is_err()
        );
    }
    for (operation, scope, name, allowed) in [
        (SecretsOperation::Put, "fixture", Some("only"), true),
        (SecretsOperation::Put, "fixture-other", Some("only"), false),
        (SecretsOperation::Put, "fixture", None, false),
        (SecretsOperation::List, "fixture", Some("only"), false),
        (SecretsOperation::Unspecified, "fixture", None, false),
    ] {
        request.target = AuthorityTarget::Secrets(secrets::Target {
            operation,
            scope,
            name,
        });
        assert_eq!(
            authority
                .authorize(&trust, &request, PolicyDecision::Allow)
                .is_ok(),
            allowed
        );
    }
    Ok(())
}

struct Fault {
    status: u16,
    body: Vec<u8>,
}
impl Transport for Fault {
    fn post<'a>(
        &'a self,
        _path: &'static str,
        _bearer: &'a str,
        _body: &'a [u8],
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            Ok(Response {
                status: self.status,
                body: Zeroizing::new(self.body.clone()),
            })
        })
    }
}
#[test]
fn secrets_redacts_requests_protobuf_and_every_provider_error() -> TestResult {
    let request = SecretPutRequest {
        scope: PLANTED.into(),
        name: PLANTED.into(),
        kind: SecretKind::Protected,
        value: PLANTED.into(),
        expected_generation: 0,
    };
    let wire = SecretPutRequestProto::from(request.clone());
    assert!(
        !format!("{request:?} {wire:?}").contains(PLANTED),
        "semantic/protobuf debug exposed a value"
    );
    assert!(
        String::from_utf8_lossy(&wire.encode_to_vec()).contains(PLANTED),
        "negative control must plant an actual value"
    );
    for status in [200, 400, 401, 403, 409, 500, 302] {
        let client = Client::new(
            Fault {
                status,
                body: PLANTED.as_bytes().to_vec(),
            },
            CredentialProvider::new(|| async { Ok("fixture".into()) }),
        );
        let Err(error) = block_on(client.put(&request)) else {
            return Err("malformed provider reply admitted".into());
        };
        assert!(!format!("{error:?} {error}").contains(PLANTED));
        assert!(
            std::error::Error::source(&error).is_none(),
            "provider source chain must be erased"
        );
    }
    let client = Client::new(
        Fault {
            status: 200,
            body: Vec::new(),
        },
        CredentialProvider::new(|| async {
            Err(acyclic_auth::credential::CredentialError::Provider(
                std::io::Error::other(PLANTED).into(),
            ))
        }),
    );
    let result = block_on(client.put(&request));
    assert_eq!(result, Err(Error::Credential));
    assert!(!format!("{result:?}").contains(PLANTED));
    Ok(())
}

#[test]
fn secrets_contract_rejects_account_selectors_and_protected_value_fields() -> TestResult {
    for request in [
        serde_json::json!({"scope":"fixture","account":PLANTED}),
        serde_json::json!({"scope":"fixture","reveal":true}),
    ] {
        assert!(serde_json::from_value::<SecretListRequest>(request).is_err());
    }
    let leaked = serde_json::json!({"generation":1,"variables":[{"kind":"protected","name":"key","version":1,"value":PLANTED}]});
    assert!(serde_json::from_value::<SecretListReply>(leaked.clone()).is_err());
    assert!(
        leaked.to_string().contains(PLANTED),
        "loose JSON negative control must expose the planted value"
    );
    let valid = serde_json::json!({"generation":1,"variables":[{"kind":"protected","name":"key","version":1},
        {"kind":"plain","name":"visible","version":1,"value":"public"}]});
    let reply: SecretListReply = serde_json::from_value(valid.clone())?;
    assert_eq!(serde_json::to_value(reply.clone())?, valid);
    let wire = SecretListReplyProto::from(reply.clone()).encode_to_vec();
    assert_eq!(
        SecretListReply::try_from(SecretListReplyProto::decode(wire.as_slice())?)?,
        reply
    );
    Ok(())
}

#[test]
fn secrets_canonical_request_rejects_discarded_fields_before_dispatch() -> TestResult {
    let request = SecretListRequestProto {
        scope: "fixture".into(),
    };
    let mut bytes = request.encode_to_vec();
    let decoded: SecretListRequest = secrets::decode_request::<SecretListRequestProto, _>(&bytes)?;
    assert_eq!(decoded.scope, "fixture");
    // Wire field 2 is a planted account selector. Ordinary prost decoding
    // discards it; the shared canonical admission must refuse the same bytes.
    bytes.extend_from_slice(&[0x12, 3, b'b', b'a', b'd']);
    assert_eq!(SecretListRequestProto::decode(bytes.as_slice())?, request);
    assert_eq!(
        secrets::decode_request::<SecretListRequestProto, SecretListRequest>(&bytes),
        Err(Error::InvalidRequest)
    );
    Ok(())
}

#[cfg(feature = "client")]
mod http {
    use super::*;
    use axum::{
        Router,
        body::Bytes,
        extract::State as HttpState,
        http::{HeaderMap, StatusCode, Uri},
        routing::post,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LogWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("log capture poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct Server {
        allowed: Provider,
        denied: Provider,
        fault: AtomicUsize,
        calls: AtomicUsize,
    }
    async fn serve(
        HttpState(server): HttpState<Arc<Server>>,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> (StatusCode, HeaderMap, Vec<u8>) {
        server.calls.fetch_add(1, Ordering::SeqCst);
        let mut extra = HeaderMap::new();
        let response = if server.fault.load(Ordering::SeqCst) == 1 {
            extra.insert(
                "location",
                axum::http::HeaderValue::from_static("/redirect-target"),
            );
            response(302, PLANTED)
        } else if server.fault.load(Ordering::SeqCst) == 2 {
            response(500, PLANTED)
        } else if headers
            .get("content-type")
            .is_none_or(|value| value != "application/json")
        {
            response(400, PLANTED)
        } else {
            let provider = if headers
                .get("authorization")
                .is_some_and(|value| value == "Bearer allowed-fixture")
            {
                &server.allowed
            } else {
                &server.denied
            };
            provider.call(uri.path(), &body)
        };
        match response {
            Ok(response) => (
                StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                extra,
                response.body.to_vec(),
            ),
            Err(_) => (StatusCode::BAD_REQUEST, extra, Vec::new()),
        }
    }

    #[tokio::test]
    async fn secrets_native_http_runs_conformance_and_redacts_errors_without_following_redirects()
    -> TestResult {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let capture = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || LogWriter(capture.clone()))
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        tracing::error!(value = PLANTED, "unredacted logging negative control");
        {
            let mut captured = logs.lock().map_err(|_| "log capture poisoned")?;
            assert!(
                String::from_utf8_lossy(&captured).contains(PLANTED),
                "logging control did not emit its value"
            );
            captured.clear();
        }
        let state = Arc::new(Mutex::new(State::default()));
        let server = Arc::new(Server {
            allowed: Provider {
                state: state.clone(),
                authority: capability(true)?,
                ignore_cas: false,
                leak: false,
            },
            denied: Provider {
                state,
                authority: capability(false)?,
                ignore_cas: false,
                leak: false,
            },
            fault: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let router = Router::new()
            .route("/v1/identity/secrets/{operation}", post(serve))
            .with_state(server.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await });
        let resolutions = Arc::new(AtomicUsize::new(0));
        let counter = resolutions.clone();
        let allowed = Client::new(
            secrets::HttpTransport::new(&origin)?,
            CredentialProvider::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok("allowed-fixture".into()) }
            }),
        );
        let denied = Client::new(
            secrets::HttpTransport::new(&origin)?,
            CredentialProvider::new(|| async { Ok("denied-fixture".into()) }),
        );
        let result = secrets::conformance::verify(&allowed, &denied, "fixture").await;
        assert_eq!(result, Ok(()));
        assert_eq!(
            resolutions.load(Ordering::SeqCst) + 3,
            server.calls.load(Ordering::SeqCst),
            "every physical request needs fresh credentials"
        );
        for (fault, expected) in [(1, Error::Http(302)), (2, Error::Http(500))] {
            server.fault.store(fault, Ordering::SeqCst);
            let calls = server.calls.load(Ordering::SeqCst);
            let result = allowed
                .list(&SecretListRequest {
                    scope: "fixture".into(),
                })
                .await;
            assert_eq!(result, Err(expected));
            assert!(!format!("{result:?}").contains(PLANTED));
            assert_eq!(
                server.calls.load(Ordering::SeqCst),
                calls + 1,
                "redirect or retry dispatched a second request"
            );
        }
        task.abort();
        let captured = logs.lock().map_err(|_| "log capture poisoned")?;
        let captured = String::from_utf8_lossy(&captured);
        for value in [
            PLANTED,
            "conformance-protected-value-never-return",
            "allowed-fixture",
            "denied-fixture",
        ] {
            assert!(
                !captured.contains(value),
                "HTTP diagnostics emitted protected input or credentials"
            );
        }
        Ok(())
    }
}

#[test]
fn secrets_published_vectors_match_shared_request_and_reply_admission() -> TestResult {
    let suite: serde_json::Value = serde_json::from_slice(secrets::conformance::SUITE)?;
    let vectors = suite
        .get("vectors")
        .and_then(serde_json::Value::as_array)
        .ok_or("vectors missing")?;
    for vector in vectors {
        let request = vector.get("request").ok_or("request missing")?.clone();
        if vector.get("reject").and_then(serde_json::Value::as_str) == Some("invalid_request") {
            assert!(serde_json::from_value::<SecretListRequest>(request).is_err());
            continue;
        }
        let client = Client::new(
            Fault {
                status: 200,
                body: serde_json::to_vec(vector.get("response").ok_or("response missing")?)?,
            },
            CredentialProvider::new(|| async { Ok("fixture".into()) }),
        );
        match vector.get("operation").and_then(serde_json::Value::as_str) {
            Some("put") => {
                block_on(client.put(&serde_json::from_value(request)?))?;
            }
            Some("delete") => {
                block_on(client.delete(&serde_json::from_value(request)?))?;
            }
            Some("list") => {
                let result = block_on(client.list(&serde_json::from_value(request)?));
                if vector.get("reject").is_some() {
                    assert_eq!(result, Err(Error::InvalidResponse));
                } else {
                    result?;
                }
            }
            _ => return Err("unknown vector operation".into()),
        }
    }
    Ok(())
}

#[test]
fn secrets_and_identity_share_endpoint_admission_before_credentials() {
    for (origin, allowed) in [
        ("https://example.com", true),
        ("http://127.0.0.1:1234", true),
        ("http://[::1]:1234", true),
        ("http://localhost:1234", true),
        ("http://example.com", false),
        ("https://user:password@example.com", false),
        ("https://example.com/path", false),
        ("https://example.com?bearer=planted", false),
        ("https://example.com#planted", false),
    ] {
        assert!(
            url::Url::parse(origin).is_ok(),
            "negative control must admit the raw URL"
        );
        assert_eq!(acyclic_auth::credential::endpoint(origin).is_ok(), allowed);
    }
}

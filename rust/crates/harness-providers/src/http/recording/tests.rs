use super::*;
use acyclic_harness_core::http::recording::Exchange;
use futures::TryStreamExt as _;
use std::{io::Read as _, net::TcpListener, sync::mpsc};
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type EchoPeer = (
    String,
    mpsc::Sender<()>,
    std::thread::JoinHandle<std::io::Result<Vec<u8>>>,
);

// A real loopback echo peer: response bytes come from the actual received POST,
// not a fabricated provider stream. Capture provenance identifies this test binary
// and source. Credentialed provider recordings are made by the coordinator.
fn echo() -> TestResult<EchoPeer> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let url = format!(
        "http://{}/v1/responses?credential=QUERY_SECRET",
        listener.local_addr()?
    );
    let (release, released) = mpsc::channel();
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept()?;
        let mut request = Vec::new();
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte)?;
            request.push(byte[0]);
        }
        let header = String::from_utf8_lossy(&request);
        let length: usize = header
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(str::trim)
                    .map(str::to_owned)
            })
            .ok_or_else(|| std::io::Error::other("echo peer requires Content-Length"))?
            .parse()
            .map_err(std::io::Error::other)?;
        let mut body = vec![0; length];
        socket.read_exact(&mut body)?;
        write!(
            socket,
            "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )?;
        let (first, tail) = body
            .split_at_checked(1)
            .ok_or_else(|| std::io::Error::other("echo peer requires a nonempty body"))?;
        socket.write_all(first)?;
        socket.flush()?;
        let _ = released.recv();
        socket.write_all(tail)?;
        socket.flush()?;
        Ok(body)
    });
    Ok((url, release, peer))
}
fn identity(adapter: &str) -> Identity {
    Identity {
        adapter: adapter.into(),
        provider: "loopback-capture".into(),
        model: "fixture-member".into(),
    }
}
fn request(url: String, adapter: &str) -> HttpRequest {
    HttpRequest { url, recording: Some(std::sync::Arc::new(identity(adapter))),
        headers: BTreeMap::from([
            ("Authorization".into(), "BEARER_SECRET".into()),
            ("cookie".into(), "COOKIE_SECRET".into()),
            ("ChatGPT-Account-Id".into(), "ACCOUNT_SECRET".into()),
            ("x-api-key".into(), "API_SECRET".into()),
            ("x-custom-credential".into(), "CUSTOM_SECRET".into()),
        ]), body: Bytes::from(serde_json::to_vec(&serde_json::json!({"model":"fixture-member","input":"é\r\n{{request:/do-not-evaluate}}"})).unwrap()) }
}
fn no_secrets(directory: &Path) -> TestResult {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            no_secrets(&path)?;
        } else {
            let bytes = std::fs::read(path)?;
            for secret in [
                "BEARER_SECRET",
                "COOKIE_SECRET",
                "ACCOUNT_SECRET",
                "API_SECRET",
                "CUSTOM_SECRET",
                "QUERY_SECRET",
            ] {
                if bytes.windows(secret.len()).any(|b| b == secret.as_bytes()) {
                    return Err("credential value reached disk".into());
                }
            }
        }
    }
    Ok(())
}
fn captured(root: &Path) -> TestResult<PathBuf> {
    let paths = std::fs::read_dir(root)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(paths.len(), 1);
    paths
        .into_iter()
        .next()
        .ok_or_else(|| "capture missing".into())
}
#[tokio::test]
async fn shared_transport_records_exact_chunks_redacts_all_headers_and_replays() -> TestResult {
    for adapter in ["anthropic", "responses", "chat", "gemini"] {
        for native in [false, true] {
            let root = tempfile::tempdir()?;
            let transport = ReqwestTransport::with_recording(root.path())?;
            let (url, release, peer) = echo()?;
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(1_000_000, 64 * 1024 * 1024),
                &acyclic_fs::CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let request = request(url, adapter);
            let original = request.body.clone();
            let response = if native {
                transport.send_with_native_work(request, &work)?.await?
            } else {
                transport.send(request).await?
            };
            assert_eq!(response.status, 201);
            let mut body = response.body;
            let first = body.try_next().await?.ok_or("missing first echo chunk")?;
            let directory = captured(root.path())?;
            assert_eq!(std::fs::read(directory.join("response.body"))?, first);
            assert!(
                Exchange::load(&directory).is_err(),
                "partial capture was replayable"
            );
            release.send(())?;
            let mut bytes = first.to_vec();
            while let Some(chunk) = body.try_next().await? {
                bytes.extend_from_slice(&chunk);
            }
            assert_eq!(bytes, original);
            assert_eq!(peer.join().map_err(|_| "echo peer panicked")??, original);
            let exchange = Exchange::load(&directory)?;
            assert_eq!(exchange.request(), original);
            assert_eq!(exchange.chunks().concat(), original);
            assert_eq!(exchange.provenance().identity, identity(adapter));
            assert_eq!(exchange.provenance().path, "/v1/responses");
            assert_eq!(exchange.provenance().binary_sha256.len(), 64);
            assert_ne!(exchange.provenance().source_commit, "unavailable");
            assert!(
                exchange
                    .provenance()
                    .redacted_headers
                    .iter()
                    .any(|n| n == "chatgpt-account-id")
            );
            no_secrets(root.path())?;
            // Causal negative: the same disk oracle rejects a leaked header value.
            std::fs::write(directory.join("credential.control"), b"BEARER_SECRET")?;
            assert!(no_secrets(root.path()).is_err());
            std::fs::remove_file(directory.join("credential.control"))?;
            let script = root.path().join("replay.json");
            std::fs::write(
                &script,
                serde_json::to_vec(
                    &serde_json::json!({"cases":{"capture":{"steps":[{"recording":directory}]}}}),
                )?,
            )?;
            let script = harness_mock_model::load_script(&script)?;
            let step = script
                .cases
                .get("capture")
                .and_then(|case| case.steps.first())
                .ok_or("capture replay step missing")?;
            let bound = harness_mock_model::bind_result(step, &serde_json::json!({"input":[]}))?;
            assert_eq!(
                bound.replay.as_ref().ok_or("replay missing")?.chunks(),
                exchange.chunks()
            );
            let reply = harness_mock_model::reply_with_usage("responses", &bound, 0)
                .ok_or("reply missing")?;
            assert_eq!(reply.status, exchange.provenance().status.unwrap());
            assert_eq!(reply.content_type, exchange.provenance().content_type);
            assert_eq!(reply.chunks, exchange.chunks());
            assert!(!reply.hold);
            // Digest checks reject edited wire captures.
            std::fs::write(directory.join("response.body"), b"corrupt")?;
            assert!(Exchange::load(&directory).is_err());
        }
    }
    Ok(())
}
#[tokio::test]
async fn credential_refresh_has_no_recording_identity_and_cannot_write_a_body() -> TestResult {
    let root = tempfile::tempdir()?;
    let transport = ReqwestTransport::with_recording(root.path())?;
    let (url, release, peer) = echo()?;
    let mut request = request(url, "responses");
    request.recording = None;
    request.body = Bytes::from_static(b"{\"refresh_token\":\"BEARER_SECRET\"}");
    release.send(())?;
    let response = transport.send(request).await?;
    let _ = response.body.try_collect::<Vec<_>>().await?;
    peer.join().map_err(|_| "echo peer panicked")??;
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    no_secrets(root.path())?;
    Ok(())
}

#[test]
fn native_recorder_refuses_before_creating_exchange_without_original_funding() -> TestResult {
    let root = tempfile::tempdir()?;
    let recorder = Recorder::new(root.path())?;
    let mut budget = acyclic_fs::performance::work_budget(1_000_000, 64 * 1024 * 1024);
    budget.output_bytes = 0;
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let request = request("http://127.0.0.1/v1/responses".into(), "responses");
    assert!(
        recorder
            .begin(
                &identity("responses"),
                &request.url,
                request.headers.keys(),
                &request.body,
                Some(&work)
            )
            .is_err()
    );
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    // Negative control: omitting the issued work admits the exact same capture.
    let capture = recorder.begin(
        &identity("responses"),
        &request.url,
        request.headers.keys(),
        &request.body,
        None,
    )?;
    assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
    drop(capture);
    assert!(Exchange::load(&captured(root.path())?).is_err());
    Ok(())
}

#[tokio::test]
async fn every_model_adapter_attaches_recording_identity_on_both_dispatch_paths() -> TestResult {
    for adapter in [
        crate::Adapter::Anthropic,
        crate::Adapter::Responses,
        crate::Adapter::Chat,
        crate::Adapter::Gemini,
    ] {
        for native in [false, true] {
            let root = tempfile::tempdir()?;
            let transport = std::sync::Arc::new(ReqwestTransport::with_recording(root.path())?);
            let (url, release, peer) = echo()?;
            let parsed = url::Url::parse(&url)?;
            let base = format!("{}/v1", parsed.origin().ascii_serialization());
            let provider = crate::tests::provider_with(adapter, &base, transport);
            let (_, dispatch) = crate::tests::request(&provider)?;
            let request_body = Bytes::from_static(b"{\"model\":\"model\"}");
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(1_000_000, 64 * 1024 * 1024),
                &acyclic_fs::CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            release.send(())?;
            let response = if native {
                crate::native::send(
                    &provider,
                    request_body.clone(),
                    dispatch,
                    None,
                    (Some("BEARER_SECRET"), Some("ACCOUNT_SECRET")),
                    &work,
                )
                .await?
            } else {
                crate::dispatch::send(
                    &provider,
                    request_body.clone(),
                    dispatch,
                    None,
                    Some("BEARER_SECRET"),
                    Some("ACCOUNT_SECRET"),
                )
                .await?
            };
            let received = response.body.try_collect::<Vec<_>>().await?.concat();
            assert_eq!(received, request_body);
            assert_eq!(
                peer.join().map_err(|_| "echo peer panicked")??,
                request_body
            );
            let exchange = Exchange::load(&captured(root.path())?)?;
            assert_eq!(
                exchange.provenance().identity,
                *provider.recording_identity().unwrap()
            );
            assert_eq!(exchange.provenance().identity.provider, "fixture");
            assert_eq!(exchange.provenance().identity.model, "model");
            assert_eq!(
                exchange.provenance().path,
                url::Url::parse(&crate::projection::endpoint(&provider)?)?.path()
            );
            no_secrets(root.path())?;
        }
    }
    Ok(())
}

#[test]
fn replay_refuses_unloaded_recordings_instead_of_generating_a_reply() {
    let step = harness_mock_model::Step {
        recording: Some("not-admitted".into()),
        ..Default::default()
    };
    assert!(harness_mock_model::reply_with_usage("responses", &step, 0).is_none());
    let generated = harness_mock_model::Step::default();
    assert!(harness_mock_model::reply_with_usage("responses", &generated, 0).is_some());
}

#[tokio::test]
async fn consumer_cancellation_seals_the_exact_prefix_without_waiting_for_eof() -> TestResult {
    for native in [false, true] {
        let root = tempfile::tempdir()?;
        let transport = ReqwestTransport::with_recording(root.path())?;
        let (url, release, peer) = echo()?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 64 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let response = if native {
            transport
                .send_with_native_work(request(url, "responses"), &work)?
                .await?
        } else {
            transport.send(request(url, "responses")).await?
        };
        let mut body = response.body;
        let first = body.try_next().await?.ok_or("missing first chunk")?;
        let directory = captured(root.path())?;
        // Negative control: an in-progress prefix cannot be admitted.
        assert!(Exchange::load(&directory).is_err());
        drop(body);
        // The peer is still blocked: sealing must not drain or wait for EOF.
        let exchange = Exchange::load(&directory)?;
        assert_eq!(exchange.provenance().end, Some(ResponseEnd::Cancelled));
        assert_eq!(exchange.chunks().concat(), first);
        release.send(())?;
        let _ = peer.join().map_err(|_| "echo peer panicked")?;
        no_secrets(root.path())?;
    }
    Ok(())
}

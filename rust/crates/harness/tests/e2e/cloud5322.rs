//! Required Streams contracts since Cloud #5322 (857d16149af8ee24b307ad4627878c700dbce860).
use super::*;
use acyclic_harness::task_volume::{FileKey, TaskVolume, Write};
use acyclic_stream::{CommitConflict, CommitMutation, OwnedBytes, wire};
use futures::StreamExt as _;
use prost::Message as _;

async fn remote() -> TestResult<Arc<Client>> {
    let file = std::env::var_os("ACYCLIC_LOCAL_CLOUD").ok_or("missing local Cloud endpoints")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
    endpoints.streams.connect().await
}

fn report<T>(name: &str, result: TestResult<T>, errors: &mut Vec<String>) {
    match result {
        Ok(_) => eprintln!("Cloud5322 PASS {name}"),
        Err(error) => {
            let error = format!("{name}: {error}");
            eprintln!("Cloud5322 MISMATCH {error}");
            errors.push(error);
        }
    }
}

pub(super) async fn required_pass() -> TestResult {
    let remote = remote().await?;
    let cases = scenarios()?;
    let mut errors = Vec::new();
    // The hierarchy conformance phase needs an otherwise stable catalog.
    report(
        "Stream conformance",
        timed("Streams conformance", async {
            acyclic_stream::conformance::verify(remote.as_ref()).await?;
            TestResult::Ok(())
        })
        .await,
        &mut errors,
    );
    let probes = timed("required Session pins", async {
        let refused = refusals(remote.clone(), &remote, "probe").await?;
        if !refused.is_empty() {
            return Err(format!("expected no Unsupported pins, observed {refused:?}").into());
        }
        TestResult::Ok(())
    });
    let compared = async {
        let results = futures::future::join_all(cases.iter().map(|(path, case)| {
            let remote = remote.clone();
            async move {
                let relative = path.strip_prefix(root()).unwrap_or(path);
                let scenario_root =
                    format!("cloud5322-cases/{}", relative.display()).replace('.', "-");
                let phase = format!("scenario {}", relative.display());
                let result = timed(
                    &phase,
                    compare(
                        relative,
                        case,
                        Arc::new(MemoryStream::default()),
                        remote,
                        &scenario_root,
                    ),
                )
                .await;
                (relative.display().to_string(), result)
            }
        }))
        .await;
        results
    };
    let swarm = timed("required SwarmClient::follow", async {
        let session = remote.session_client()?;
        let baseline = Arc::new(MemoryStream::default());
        let executor = Arc::new(acyclic_stream::session::runtime::PlatformExecutor);
        let local = acyclic_stream::session::Client::in_process(baseline.clone(), executor);
        let expected = remote_swarm::follow(baseline, &local, "swarm", "Do it.").await?;
        let observed = remote_swarm::follow(remote.clone(), &session, "swarm", "Do it.").await?;
        if observed != expected {
            return Err(format!("Swarm views expected {expected}, observed {observed}").into());
        }
        TestResult::Ok(())
    });
    let resumed = timed("kill / resume / completed reopen", async {
        let file =
            std::env::var_os("ACYCLIC_LOCAL_CLOUD").ok_or("missing local Cloud endpoints")?;
        let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
        let local = loopback(Arc::new(MemoryStream::default())).await?;
        let (cloud, baseline, unjournaled) = futures::join!(
            resume::kill_and_resume(&endpoints.streams, remote.clone()),
            resume::kill_and_resume(&local.remote, local.client.clone()),
            resume::resume_unjournaled(remote.clone()),
        );
        let (cloud, baseline) = (cloud?, baseline?);
        unjournaled?;
        if cloud != baseline {
            return Err(format!("resumed journals differ: {cloud:?} vs {baseline:?}").into());
        }
        TestResult::Ok(())
    });
    let (probes, compared, swarm, resumed) = futures::join!(probes, compared, swarm, resumed);
    report(
        "Head publication / checked follow / Children / LATEST pins",
        probes,
        &mut errors,
    );
    report("SwarmClient::follow", swarm, &mut errors);
    report("kill / resume / completed reopen", resumed, &mut errors);
    for (name, result) in compared {
        report(&name, result, &mut errors);
    }
    if !errors.is_empty() {
        return Err(errors.join("\n").into());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs"]
async fn head_fenced_task_volume_publication_rejects_a_forged_head() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-fence/task")?;
    let volume = TaskVolume::open(remote.clone(), path.clone()).await?;
    let first = Write::new(FileKey::Goal, b"published".to_vec());
    let head = volume.append(volume.head().await?, &first).await?;
    let forged = CommitId::from_bytes([255; 32]);
    let request = CommitRequest {
        conditions: vec![CommitCondition::Head {
            path: path.clone(),
            expected: head.position,
            commit_id: forged,
        }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"must-not-commit")],
        }],
        idempotency_key: IdempotencyKey::new(b"cloud5322-forged-head".to_vec())?,
    };
    let observed = remote.commit(request.clone()).await?;
    let expected = CommitOutcome::Conflict(vec![CommitConflict::Head {
        path: path.clone(),
        expected: forged,
        actual: CommitId::from_bytes(head.commit),
    }]);
    if observed != expected {
        return Err(format!("Head conflict expected {expected:?}, observed {observed:?}").into());
    }
    if remote.commit(request).await? != expected {
        return Err("Head conflict retry changed".into());
    }
    if remote.tail(path).await? != head.position {
        return Err("forged Head changed the tail".into());
    }
    let second = Write::new(FileKey::Goal, b"accepted".to_vec());
    let next = volume.append(head, &second).await?;
    if next.position != head.position + 1 {
        return Err("valid fenced publication did not advance once".into());
    }
    eprintln!(
        "Cloud5322 PASS exact Head conflict, retry, unchanged tail and valid task publication"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs"]
async fn latest_payload_matches_exactly() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-errors/status")?;
    let latest = wire::SessionLatestValue {
        key: b"agent".to_vec().into(),
        value: b"ready".to_vec().into(),
        attention: true,
    };
    remote
        .append(AppendRequest {
            path: path.clone(),
            records: vec![latest.encode_to_vec().into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let mut data = remote
        .follow_data(
            wire::SessionFollow {
                path: path.as_str().into(),
                delivery: wire::SessionDelivery::Latest as i32,
                preceding_commit_id: Default::default(),
            },
            0,
        )
        .await?;
    let observed = wait(&mut 1, "LATEST value", data.next())
        .await?
        .ok_or("LATEST ended")??;
    if observed.item != Some(wire::session_data::Item::LatestValue(latest.clone())) {
        return Err(format!("LATEST expected {latest:?}, observed {observed:?}").into());
    }
    eprintln!("Cloud5322 PASS exact LATEST value");
    Ok(())
}

#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs"]
async fn checked_follow_close_codes() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-errors/checked")?;
    remote
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"anchor")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let session = remote.session_client()?;
    let mut errors = Vec::new();
    for (path, from, preceding, expected) in [
        (
            path.clone(),
            1,
            Some(CommitId::from_bytes([255; 32])),
            StreamError::PrefixNotRetained,
        ),
        (path, 2, None, StreamError::OutOfRange),
        (
            StreamPath::new("cloud5322-errors/missing")?,
            0,
            None,
            StreamError::NotFound,
        ),
    ] {
        let opened = match preceding {
            Some(preceding) => session.follow_checked(path, from, preceding).await,
            None => session.follow(path, from).await,
        };
        let observed = match opened {
            Err(error) => Some(error),
            Ok(mut follow) => {
                let item = wait(&mut 1, "checked follow close", follow.next()).await?;
                match item {
                    Some(Err(error)) => Some(error),
                    Some(Ok(record)) => {
                        eprintln!("unexpected checked-follow record: {record:?}");
                        Some(StreamError::Unavailable)
                    }
                    None => None,
                }
            }
        };
        if observed != Some(expected.clone()) {
            errors.push(format!(
                "Closed {} must map to {expected:?}, observed {observed:?}",
                expected.code()
            ));
        } else {
            eprintln!("Cloud5322 PASS Closed {} -> {expected:?}", expected.code());
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("\n").into());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs"]
async fn raw_session_closed_frames_use_stream_error_codes() -> TestResult {
    use wire::{session_frame::Body, session_subscription::Kind};
    let file = std::env::var_os("ACYCLIC_LOCAL_CLOUD").ok_or("missing local Cloud endpoints")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
    let remote = endpoints.streams.connect().await?;
    let path = StreamPath::new("cloud5322-raw/checked")?;
    remote
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"anchor")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let channel = tonic::transport::Endpoint::from_shared(endpoints.streams.endpoint)?
        .tls_config(tonic::transport::ClientTlsConfig::new().ca_certificate(
            tonic::transport::Certificate::from_pem(std::fs::read(endpoints.streams.ca_file)?),
        ))?
        .connect()
        .await?;
    let mut client = wire::stream_session_service_client::StreamSessionServiceClient::new(channel);
    let (send, receive) = tokio::sync::mpsc::channel(8);
    let frame = |body| wire::SessionFrame {
        channel_id: 0,
        generation: 0,
        body: Some(body),
    };
    send.send(frame(Body::Hello(wire::SessionHello {
        version: 1,
        client_id: b"cloud-local-5322".to_vec().into(),
        destination_node: String::new(),
        receive_bytes: 1 << 20,
        receive_frames: 100,
    })))
    .await?;
    let input = futures::stream::unfold(receive, |mut receive| async move {
        receive.recv().await.map(|frame| (frame, receive))
    });
    let mut request = tonic::Request::new(input);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", endpoints.streams.bearer).parse()?,
    );
    let mut output = client.session(request).await?.into_inner();
    let welcome = wait(&mut 1, "Session Welcome", output.message())
        .await??
        .ok_or("raw Session ended before Welcome")?;
    if !matches!(welcome.body, Some(Body::Welcome(_))) {
        return Err(format!("expected Welcome, observed {welcome:?}").into());
    }
    let subscriptions = [
        (1, path.as_str(), 1, vec![255; 32]),
        (3, path.as_str(), 2, vec![]),
        (5, "cloud5322-raw/missing", 0, vec![]),
        (7, path.as_str(), 0, vec![]),
    ]
    .into_iter()
    .map(|(id, path, next, preceding)| wire::SessionSubscription {
        channel: Some(wire::SessionChannel {
            channel_id: id,
            generation: 1,
        }),
        resume: Some(wire::SessionCursor {
            next,
            opaque: Default::default(),
        }),
        kind: Some(Kind::Follow(wire::SessionFollow {
            path: path.into(),
            delivery: wire::SessionDelivery::All as i32,
            preceding_commit_id: preceding.into(),
        })),
    })
    .collect();
    send.send(frame(Body::Interest(wire::SessionInterest {
        revision: 1,
        upsert: subscriptions,
        remove: vec![],
    })))
    .await?;
    let expected = std::collections::BTreeMap::from([
        (1, "prefix_not_retained"),
        (3, "out_of_range"),
        (5, "not_found"),
    ]);
    let mut closed = std::collections::BTreeMap::new();
    let mut healthy_opened = false;
    let mut healthy_delivered = false;
    // One InterestApplied, one Closed per refused sibling, and the healthy
    // sibling's Opened plus Data. Extra frames cannot extend the exchange.
    let mut remaining = 1 + expected.len() + 2;
    while closed.len() < expected.len() || !healthy_delivered {
        let frame = wait(&mut remaining, "Session sibling outcomes", output.message())
            .await??
            .ok_or("raw Session ended before Closed frames")?;
        eprintln!("Cloud5322 raw frame {frame:?}");
        if frame.channel_id != 0 && frame.generation != 1 {
            return Err(format!("wrong generation: {frame:?}").into());
        }
        match &frame.body {
            Some(Body::Closed(value)) => {
                if closed
                    .insert(frame.channel_id, value.code.clone())
                    .is_some()
                {
                    return Err(format!("duplicate Closed: {frame:?}").into());
                }
            }
            Some(Body::Opened(_)) if frame.channel_id == 7 => {
                if healthy_opened {
                    return Err("healthy sibling opened twice".into());
                }
                healthy_opened = true;
                send.send(wire::SessionFrame {
                    channel_id: 7,
                    generation: 1,
                    body: Some(Body::Window(wire::SessionWindow {
                        bytes: 1 << 20,
                        frames: 10,
                    })),
                })
                .await?;
            }
            Some(Body::Data(data)) if frame.channel_id == 7 && healthy_opened => {
                let Some(wire::session_data::Item::Record(record)) = &data.item else {
                    return Err(format!("healthy sibling omitted Record: {frame:?}").into());
                };
                if healthy_delivered
                    || record.sequence != 0
                    || record.value.as_ref() != b"anchor"
                    || data.after.as_ref().map(|cursor| cursor.next) != Some(1)
                {
                    return Err(format!("healthy sibling data differs: {frame:?}").into());
                }
                healthy_delivered = true;
            }
            Some(Body::InterestApplied(_)) => {}
            Some(Body::Opened(_)) => {
                return Err(format!("refused sibling opened: {frame:?}").into());
            }
            _ => {
                return Err(
                    format!("expected Opened/InterestApplied/Closed, observed {frame:?}").into(),
                );
            }
        }
    }
    if !closed
        .iter()
        .map(|(id, code)| (*id, code.as_str()))
        .eq(expected.into_iter())
    {
        return Err(format!("Closed codes expected 1=prefix_not_retained,3=out_of_range,5=not_found; observed {closed:?}").into());
    }
    eprintln!("Cloud5322 PASS exact raw Closed frames and healthy sibling Opened/data");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum WaitError {
    #[error("{operation}: Session response frame budget exhausted")]
    FramesExhausted { operation: &'static str },
    #[error("{operation}: {detail}")]
    Stalled {
        operation: &'static str,
        detail: String,
    },
}

/// Each expected response consumes a protocol frame, independently of host
/// speed. The shared runner guard attributes missing wakeups/frames and drains
/// accepted I/O; scenario semantics carry no clock or per-response deadline.
async fn wait<T>(
    remaining: &mut usize,
    operation: &'static str,
    response: impl std::future::Future<Output = T>,
) -> Result<T, WaitError> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or(WaitError::FramesExhausted { operation })?;
    super::super::stall::guarded(
        Path::new("cloud5322"),
        super::super::stall::waiting(operation, response),
    )
    .await
    .map_err(|detail| WaitError::Stalled { operation, detail })
}

#[tokio::test(start_paused = true)]
async fn response_waits_fail_with_typed_stalls_and_reject_an_unguarded_wait() {
    fn typed_stall(result: Result<Result<(), WaitError>, String>, operation: &str) -> bool {
        matches!(result, Ok(Err(WaitError::Stalled { operation: actual, detail }))
            if actual == operation && detail == format!("stalled: cloud5322 at {operation}"))
    }
    for operation in [
        "LATEST value",
        "checked follow close",
        "Session Welcome",
        "Session sibling outcomes",
    ] {
        // Register the independent observer before either wait starts. Both
        // guards then observe initial admission, rather than an empty outer
        // registry mistaking the inner guard's first quiet period for a stall.
        for guarded in [true, false] {
            let mut guard = super::super::progress_guard::Guard::new();
            let registry = guard.registry.clone();
            let result = guard
                .run(async {
                    let registration = registry.register(Path::new("wait-negative-control"));
                    drop(registration.polling());
                    if guarded {
                        wait(&mut 1, operation, std::future::pending::<()>()).await
                    } else {
                        // Removing the response guard must fail this same
                        // typed-error oracle without an external termination.
                        Ok(std::future::pending::<()>().await)
                    }
                })
                .await;
            assert_eq!(typed_stall(result, operation), guarded);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn response_budget_is_exact_and_rejects_a_restored_frame() {
    fn exhausted(result: Result<(), WaitError>) -> bool {
        matches!(
            result,
            Err(WaitError::FramesExhausted {
                operation: "frames"
            })
        )
    }
    for frames in 0..=8 {
        let mut remaining = frames;
        for delivered in 0..frames {
            assert_eq!(
                wait(&mut remaining, "frames", std::future::ready(delivered))
                    .await
                    .unwrap(),
                delivered
            );
        }
        assert!(exhausted(
            wait(&mut remaining, "frames", std::future::ready(())).await
        ));
        // Restoring a consumed frame must fail the same exact-budget oracle.
        remaining += 1;
        assert!(!exhausted(
            wait(&mut remaining, "frames", std::future::ready(())).await
        ));
    }
}

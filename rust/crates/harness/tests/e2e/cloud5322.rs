//! Unpushed pre-merge qualification of Cloud #5322 at 055987e839.
use super::*;
use acyclic_harness::task_volume::{FileKey, TaskVolume, Write};
use acyclic_stream::{CommitConflict, CommitMutation, OwnedBytes, wire};
use futures::StreamExt as _;
use prost::Message as _;
use std::time::Duration;

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
                let result = compare(
                    relative,
                    case,
                    Arc::new(MemoryStream::default()),
                    remote,
                    &scenario_root,
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
#[ignore = "pre-merge Cloud request 10 qualification at 055987e839"]
async fn head_fenced_task_volume_publication_rejects_a_forged_head() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-fence/task")?;
    let volume = TaskVolume::open(remote.clone(), path.clone()).await?;
    let first = Write::new(FileKey::parse("first.txt")?, b"published".to_vec());
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
    let second = Write::new(FileKey::parse("second.txt")?, b"accepted".to_vec());
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
#[ignore = "pre-merge Cloud request 10 qualification at 055987e839"]
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
    let observed = tokio::time::timeout(Duration::from_secs(10), data.next())
        .await?
        .ok_or("LATEST ended")??;
    if observed.item != Some(wire::session_data::Item::LatestValue(latest.clone())) {
        return Err(format!("LATEST expected {latest:?}, observed {observed:?}").into());
    }
    eprintln!("Cloud5322 PASS exact LATEST value");
    Ok(())
}

#[tokio::test]
#[ignore = "pre-merge Cloud request 10 qualification at 055987e839"]
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
                let item = match tokio::time::timeout(Duration::from_secs(10), follow.next()).await
                {
                    Ok(item) => item,
                    Err(_) => {
                        errors.push(format!(
                            "expected Closed {} -> {expected:?}; no terminal frame within 10s",
                            expected.code()
                        ));
                        continue;
                    }
                };
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

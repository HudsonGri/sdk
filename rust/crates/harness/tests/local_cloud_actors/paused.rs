use super::*;
use serde_json::json;
use std::process::Command;

const ID: &str = "sdk-paused-poison";
const SOURCE: &str = "e2e-root/sdk-paused-poison";
const POISON: &str = include_str!("poison.mjs");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn configuration() -> wire::CreateActorRequest {
    let mut request = create();
    request.code_sha256 = digest(POISON);
    request.idempotency_key = "sdk-create-paused-poison".into();
    request.subscriptions = vec![wire::SubscriptionSpec {
        subscription_id: "events".into(),
        stream_path: SOURCE.into(),
        start: Some(wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::CurrentHead(true)),
        }),
        placement_anchor: false,
    }];
    request
}

// Mint from the gate's newly generated synthetic account, never operator state.
fn bearer(request: &wire::CreateActorRequest) -> TestResult<String> {
    let account = PathBuf::from(
        std::env::var_os("ACYCLIC_LOCAL_CLOUD_ACCOUNT")
            .ok_or("fixture account directory missing")?,
    );
    let ctl = std::env::var_os("ACYCLIC_LOCAL_CLOUD_CTL").ok_or("fixture cloudctl missing")?;
    let temp = tempfile::tempdir()?;
    let permissions = temp.path().join("permissions.json");
    let key = temp.path().join("key");
    let token = temp.path().join("bearer");
    let scope = serde_json::to_string(&["staging", "00000000-0000-4000-8000-0000000000e1", ID])?;
    let execution: Vec<_> = [
        "actor_turn",
        "actor_checkpoint",
        "actor_restore",
        "actor_hibernate",
    ]
    .into_iter()
    .map(|operation| {
        json!({
            "operation": operation,
            "organization_id": "00000000-0000-4000-8000-0000000000e2",
            "project_id": "00000000-0000-4000-8000-0000000000e3",
            "policy_generation": 1,
            "workload": {"resource": "version", "workload_id": ID,
                "code_sha256": hex(&request.code_sha256),
                "configuration_sha256": hex(&Sha256::digest(request.encode_to_vec()))},
            "limits": {"cpu_time_us": 100000, "cpu_period_us": 100000,
                "memory_bytes": 256 << 20, "max_concurrency": 4, "reserved_capacity_units": 1},
            "usage_owner": {"product": "actors", "logical_id": scope}
        })
    })
    .collect();
    let observation = observation_path(ID)?;
    let mut streams: Vec<_> = ["follow", "tail"]
        .into_iter()
        .map(|operation| {
            json!({
                "operation": operation,
                "resource": {"resource": "path", "path": observation.as_str(), "descendants": false}
            })
        })
        .collect();
    streams.push(json!({"operation": "follow", "resource": {
        "resource": "path", "path": SOURCE, "descendants": false}}));
    std::fs::write(
        &permissions,
        serde_json::to_vec(&json!({"execution": execution, "streams": streams}))?,
    )?;
    let run = |command: &mut Command| -> TestResult {
        let output = command.output()?;
        if !output.status.success() {
            return Err(format!(
                "fixture credential mint failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    };
    run(Command::new(&ctl)
        .args(["account", "key", "mint", "--root"])
        .arg(account.join("root.key"))
        .arg("--birth")
        .arg(account.join("birth.cert"))
        .args(["--key-id", ID, "--permissions"])
        .arg(permissions)
        .args(["--days", "1", "--key-epoch", "1", "--output"])
        .arg(&key))?;
    run(Command::new(&ctl)
        .args(["account", "bearer", "--key"])
        .arg(key)
        .arg("--birth")
        .arg(account.join("birth.cert"))
        .args(["--allowance", "omit", "--output"])
        .arg(&token))?;
    Ok(std::fs::read_to_string(token)?.trim().to_owned())
}

fn paused() -> wire::SubscriptionObservation {
    wire::SubscriptionObservation {
        stream_path: SOURCE.into(),
        state: wire::SubscriptionState::Paused as i32,
        failed_cursor: Some(1),
        retry_count: 1,
        failure_code: "customer_exception".into(),
        ..subscription(1)
    }
}

// Cloud's additive public Configure RPC; no internal mutation or state injection.
#[derive(Clone, PartialEq, prost::Message)]
struct Configure {
    #[prost(string, tag = "1")]
    actor_id: String,
    #[prost(uint32, optional, tag = "2")]
    max_attempts: Option<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Configured {
    #[prost(uint32, tag = "1")]
    max_attempts: u32,
}

async fn configure(remote: &Remote, bearer: &str, max_attempts: u32) -> TestResult {
    let channel = tonic::transport::Endpoint::from_shared(remote.endpoint.clone())?
        .tls_config(tonic::transport::ClientTlsConfig::new().ca_certificate(
            tonic::transport::Certificate::from_pem(std::fs::read(&remote.ca_file)?),
        ))?
        .connect()
        .await?;
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await?;
    let mut request = tonic::Request::new(Configure {
        actor_id: ID.into(),
        max_attempts: Some(max_attempts),
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {bearer}").parse()?);
    let response: tonic::Response<Configured> = client
        .unary(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/acyclic.actors.v1.ActorsService/DeadLetters",
            ),
            tonic_prost::ProstCodec::default(),
        )
        .await?;
    assert_eq!(response.into_inner().max_attempts, max_attempts);
    Ok(())
}

pub(super) async fn qualify() -> TestResult {
    let endpoints = endpoints()?;
    let request = configuration();
    let bearer = bearer(&request)?;
    let ca = std::fs::read(&endpoints.streams.ca_file)?;
    let source = acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(
        &endpoints.streams.endpoint,
        &endpoints.streams.bearer,
        ca.clone(),
    )
    .await?;
    let observed = acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(
        &endpoints.streams.endpoint,
        &bearer,
        ca,
    )
    .await?;
    let append = |value: &'static [u8]| AppendRequest {
        path: StreamPath::new(SOURCE).unwrap(),
        records: vec![value.into()],
        if_tail: None,
        idempotency_key: None,
    };
    assert!(matches!(
        source.append(append(b"before-subscription")).await?,
        AppendOutcome::Committed(_)
    ));
    let objects = GrpcObjects::connect(
        &endpoints.objects.endpoint,
        &endpoints.objects.bearer,
        Some(&std::fs::read(&endpoints.objects.ca_file)?),
        None,
    )
    .await?;
    let bucket = objects
        .create_bucket(objects::CreateBucketRequest {
            name: "actors-code".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("code bucket omitted")?;
    objects
        .put(
            objects::PutObjectHeader {
                bucket: Some(bucket),
                object_key: format!("sha256/{}", hex(&request.code_sha256)),
                ..Default::default()
            },
            acyclic_stream::OwnedBytes::copy_from_slice(POISON.trim_end_matches('\n').as_bytes()),
        )
        .await?;
    let remote = &endpoints.actors.remote;
    let mut actors = grpc::connect(
        &remote.endpoint,
        &bearer,
        Some(&std::fs::read(&remote.ca_file)?),
    )
    .await?;
    let created = actors
        .create_actor(request)
        .await?
        .into_inner()
        .actor
        .ok_or("created actor missing")?;
    assert_eq!(created.actor_id, ID);
    configure(remote, &bearer, 1).await?;
    let boundary = observed.tail(observation_path(ID)?).await?;
    source.append(append(b"poison")).await?;
    let (before, next) = followed_observation(&observed, ID, boundary, |actor| {
        actor
            .subscriptions
            .iter()
            .any(|sub| sub.state == wire::SubscriptionState::Paused as i32)
    })
    .await?;
    subscriptions(&before, &[paused()])?;
    configure(remote, &bearer, 2).await?;
    // One public read after the acknowledged Configure, never an Inspect polling loop.
    let configured = actors
        .inspect_actor(wire::InspectActorRequest {
            actor_id: ID.into(),
        })
        .await?
        .into_inner()
        .actor
        .ok_or("configured actor missing")?;
    subscriptions(&configured, &[paused()])?;
    let mut expected = paused();
    expected.state = wire::SubscriptionState::Active as i32;
    expected.failed_cursor = None;
    expected.failure_code.clear();
    expected.retry_count = 0;
    let resume = wire::ResumeSubscriptionRequest {
        actor_id: ID.into(),
        subscription_id: "events".into(),
        idempotency_key: "sdk-resume-poison".into(),
    };
    let resumed = actors
        .resume_subscription(resume.clone())
        .await?
        .into_inner()
        .actor
        .ok_or("resume omitted actor")?;
    subscriptions(&resumed, &[expected])?;
    let retried = actors
        .resume_subscription(resume)
        .await?
        .into_inner()
        .actor
        .ok_or("resume retry omitted actor")?;
    assert_eq!(retried, resumed);
    followed_observation(&observed, ID, next, |actor| actor == &resumed).await?;
    actors
        .delete_actor(wire::DeleteActorRequest {
            actor_id: ID.into(),
            idempotency_key: "sdk-delete-poison".into(),
        })
        .await?;
    Ok(())
}

#[test]
fn paused_oracle_rejects_active_and_changed_failure_boundaries() -> TestResult {
    let expected = paused();
    subscriptions(
        &wire::ActorObservation {
            subscriptions: vec![expected.clone()],
            ..Default::default()
        },
        &[expected.clone()],
    )?;
    for field in 0..9 {
        let mut changed = expected.clone();
        match field {
            0 => changed.state = wire::SubscriptionState::Active as i32,
            1 => changed.failed_cursor = None,
            2 => changed.retry_count += 1,
            3 => changed.failure_code.clear(),
            4 => changed.completed_cursor += 1,
            5 => changed.delivered_cursor += 1,
            6 => changed.recoverable_cursor += 1,
            7 => changed.stream_path.push_str("-wrong"),
            _ => changed.subscription_id.push_str("-wrong"),
        }
        let actor = wire::ActorObservation {
            subscriptions: vec![changed],
            ..Default::default()
        };
        assert!(subscriptions(&actor, &[expected.clone()]).is_err());
    }
    Ok(())
}

use super::*;
use crate::conformance::{self, Failure, Package, Target};
use crate::guest::{AlarmChange, Checkpoint, Effect, GuestFuture, Manifest, Transition};
use crate::local::{PreparedPublish, Subscription};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, LocalStream, MemoryStream,
    StreamPath,
};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn root() -> Result<StreamPath, StreamError> {
    StreamPath::new("harness/actors")
}

fn key(value: &str) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(value.as_bytes().to_vec())
}

fn actor(id: &str) -> Result<ActorReference, Box<dyn std::error::Error>> {
    Ok(ActorReference::new(b"account", b"agents", id.as_bytes())?)
}

/// A running host and the task driving it.
type Running<P> = Option<(Arc<Runtime<P>>, tokio::task::JoinHandle<()>)>;

/// Hosts `local::Runtime` behind the conformance target surface.
struct LocalTarget<P: StreamProvider> {
    store: Store<P>,
    handler: Arc<dyn Handler>,
    running: std::sync::Mutex<Running<P>>,
}

impl<P: StreamProvider> LocalTarget<P> {
    fn start(provider: &Arc<P>, handler: Arc<dyn Handler>) -> Result<Self, StreamError> {
        let target = Self {
            store: Store::new(Arc::clone(provider), root()?),
            handler,
            running: std::sync::Mutex::new(None),
        };
        target.launch()?;
        Ok(target)
    }

    fn launch(&self) -> Result<(), StreamError> {
        let runtime = Arc::new(Runtime::new(
            Store::new(Arc::clone(self.store.provider()), root()?),
            Arc::clone(&self.handler),
            Arc::new(SystemClock),
        ));
        let host = Arc::clone(&runtime);
        let task = tokio::spawn(async move {
            let _outcome = host.run(|_, _| {}).await;
        });
        *self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((runtime, task));
        Ok(())
    }

    fn runtime(&self) -> Result<Arc<Runtime<P>>, Error> {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(runtime, _)| Arc::clone(runtime))
            .ok_or(Error::Invalid("runtime stopped"))
    }
}

impl<P: StreamProvider> Target for LocalTarget<P> {
    type Provider = P;
    fn store(&self) -> &Store<P> {
        &self.store
    }
    fn clock(&self) -> &dyn Clock {
        &SystemClock
    }
    fn pause<'a>(
        &'a self,
        actor: &'a ActorReference,
        paused: bool,
    ) -> BoxFuture<'a, Result<(), Error>> {
        async move { self.runtime()?.pause(actor, paused).await }.boxed()
    }
    fn restart(&self) -> BoxFuture<'_, Result<(), Error>> {
        async move {
            let stopped = self
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some((_, task)) = stopped {
                task.abort();
                // An aborted host is the crash being modelled.
                let _aborted = task.await;
            }
            self.launch()?;
            Ok(())
        }
        .boxed()
    }
}

#[tokio::test]
async fn conformance_suite_passes_on_memory_runtime() -> TestResult {
    let target = LocalTarget::start(
        &Arc::new(MemoryStream::default()),
        Arc::new(Package::new()?),
    )?;
    conformance::verify(&target, "memory").await?;
    Ok(())
}

#[tokio::test]
async fn conformance_suite_passes_on_durable_local_runtime() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let target = LocalTarget::start(&provider, Arc::new(Package::new()?))?;
    conformance::verify(&target, "local").await?;
    Ok(())
}

/// A host that loses the alarm half of a transition (the old independent
/// alarm write) is the broken behaviour the suite must reject.
struct DropsAlarmClears(Package);
impl Handler for DropsAlarmClears {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
    ) -> GuestFuture<'a, Transition> {
        async move {
            let mut transition = self.0.on_record(delivery, checkpoint).await?;
            if transition.alarm == AlarmChange::Clear {
                transition.alarm = AlarmChange::Keep;
            }
            Ok(transition)
        }
        .boxed()
    }
}

#[tokio::test]
async fn negative_control_conformance_rejects_a_host_that_loses_alarm_changes() -> TestResult {
    let target = LocalTarget::start(
        &Arc::new(MemoryStream::default()),
        Arc::new(DropsAlarmClears(Package::new()?)),
    )?;
    match conformance::verify(&target, "broken").await {
        Err(Failure::Violation {
            case: "cleared", ..
        }) => Ok(()),
        other => {
            Err(format!("the suite accepted a host that loses alarm clears: {other:?}").into())
        }
    }
}

struct Manual(AtomicU64);
impl Clock for Manual {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep_until(&self, _deadline_unix_ms: u64) -> BoxFuture<'static, ()> {
        futures::future::pending().boxed()
    }
}

fn ledger(outbox: &str) -> Result<Checkpoint, Box<dyn std::error::Error>> {
    Ok(Checkpoint {
        codec: conformance::codec(),
        bytes: serde_json::to_vec(&serde_json::json!({ "outbox": outbox, "published": 0 }))?.into(),
    })
}

async fn records<P: StreamProvider + ?Sized>(
    provider: &P,
    path: &str,
) -> Result<Vec<Bytes>, Box<dyn std::error::Error>> {
    let path = StreamPath::new(path)?;
    let tail = match provider.tail(path.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let stream = provider
        .read(acyclic_stream::ReadRequest {
            path,
            from: 0,
            limit: u32::try_from(tail)?.max(1),
        })
        .await?;
    Ok(stream
        .map(|record| record.map(|record| Bytes::copy_from_slice(&record.value)))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?)
}

/// Two actors from one manifest bind different inbox paths and first cursors
/// in a foreign commit; the runtime consumes exactly from each bound cursor.
#[tokio::test]
async fn instance_bound_subscriptions_created_in_a_foreign_commit() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let (first, second) = (actor("first")?, actor("second")?);
    // Five inherited records precede the first actor's cut.
    let prefix = vec![AppendRecord::from(Bytes::from_static(b"emit")); 5];
    PreparedPublish::settle(
        &provider
            .commit(CommitRequest {
                conditions: vec![CommitCondition::Absent {
                    path: StreamPath::new("agents/first")?,
                }],
                mutations: vec![CommitMutation::Append {
                    path: StreamPath::new("agents/first")?,
                    records: prefix,
                }],
                idempotency_key: key("prefix")?,
            })
            .await?,
    )?;
    let bind = |path: &str, cursor| -> Result<_, StreamError> {
        Ok(BTreeMap::from([(
            "inbox".to_owned(),
            Subscription {
                path: StreamPath::new(path)?,
                cursor,
            },
        )]))
    };
    let mut prepared = PreparedPublish::foreign(key("spawn")?);
    for (address, inbox, cursor, outbox) in [
        (&first, "agents/first", 5, "out/first"),
        (&second, "agents/second", 0, "out/second"),
    ] {
        for effect in store.create(address, bind(inbox, cursor)?, Some(ledger(outbox)?))? {
            prepared.join(effect)?;
        }
    }
    prepared.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: StreamPath::new("unrelated/log")?,
        }],
        mutation: CommitMutation::Append {
            path: StreamPath::new("unrelated/log")?,
            records: vec![Bytes::from_static(b"spawned").into()],
        },
    })?;
    PreparedPublish::settle(&provider.commit(prepared.into_request()).await?)?;
    PreparedPublish::settle(
        &provider
            .commit(CommitRequest {
                conditions: vec![CommitCondition::Tail {
                    path: StreamPath::new("agents/first")?,
                    expected: 5,
                }],
                mutations: vec![CommitMutation::Append {
                    path: StreamPath::new("agents/first")?,
                    records: vec![Bytes::from_static(b"emit").into()],
                }],
                idempotency_key: key("send")?,
            })
            .await?,
    )?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    runtime.track(first.clone())?;
    runtime.track(second.clone())?;
    let failures = runtime.drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    // A claim that reset the bound cursor would replay r0..r4 here.
    assert_eq!(
        records(&*provider, "out/first").await?,
        vec![Bytes::from_static(b"r5")]
    );
    assert!(records(&*provider, "out/second").await?.is_empty());
    let state = store.load(&first).await?;
    assert_eq!(
        state.state().and_then(|state| state.cursor("inbox")),
        Some(6)
    );
    assert_eq!(state.state().map(State::epoch), Some(1));
    let state = store.load(&second).await?;
    assert_eq!(
        state.state().and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    assert_eq!(
        runtime.counts(),
        (2, 0),
        "dormant actors hold no ready work"
    );
    Ok(())
}

/// Waits, without polling, until `path` exists. Each wake is a record on
/// `wake`, a stream appended in every commit that can create `path`.
async fn created<P: StreamProvider + ?Sized>(
    provider: &P,
    wake: StreamPath,
    path: StreamPath,
) -> TestResult {
    let mut wakes = provider.follow(wake, 0, FollowFilter::default()).await?;
    loop {
        match provider.tail(path.clone()).await {
            Ok(_) => return Ok(()),
            Err(StreamError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        wakes.next().await.ok_or("wake stream ended")??;
    }
}

/// Activation by the root's activation log: actors created by other parties'
/// commits get their first delivery with no placement call and no scan.
async fn foreign_creates_activate(count: usize) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let runtime = Arc::new(Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    ));
    let host = Arc::clone(&runtime);
    let task = tokio::spawn(async move {
        let _outcome = host.run(|_, _| {}).await;
    });
    for index in 0..count {
        let address = actor(&format!("a{index}"))?;
        let inbox = StreamPath::new(format!("agents/a{index}"))?;
        let mut prepared = PreparedPublish::foreign(key(&format!("create-{index}"))?);
        for effect in store.create(
            &address,
            BTreeMap::from([(
                "inbox".to_owned(),
                Subscription {
                    path: inbox.clone(),
                    cursor: 0,
                },
            )]),
            Some(ledger(&format!("out/a{index}"))?),
        )? {
            prepared.join(effect)?;
        }
        // The first input rides in the same commit as the creation.
        prepared.join(Effect {
            conditions: vec![CommitCondition::Absent {
                path: inbox.clone(),
            }],
            mutation: CommitMutation::Append {
                path: inbox,
                records: vec![Bytes::from_static(b"emit").into()],
            },
        })?;
        PreparedPublish::settle(&provider.commit(prepared.into_request()).await?)?;
    }
    for index in 0..count {
        let address = actor(&format!("a{index}"))?;
        created(
            &*provider,
            store.path(&address)?,
            StreamPath::new(format!("out/a{index}"))?,
        )
        .await?;
    }
    task.abort();
    let _aborted = task.await;
    assert_eq!(runtime.counts().0, count, "one registration per actor");
    Ok(())
}

#[tokio::test]
async fn foreign_created_actors_activate_without_placement_or_scan() -> TestResult {
    foreign_creates_activate(1_000).await
}

#[tokio::test]
#[ignore = "stress"]
async fn ten_thousand_foreign_created_actors_activate() -> TestResult {
    foreign_creates_activate(10_000).await
}

/// Pausing an actor that no host has claimed yet takes effect at epoch 0; the
/// first delivery waits for resume. The refused epoch-0 CAS spun forever.
#[tokio::test]
async fn an_unclaimed_actor_pauses_before_its_first_delivery() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = actor("paused")?;
    let inbox = StreamPath::new("agents/paused")?;
    let mut prepared = PreparedPublish::foreign(key("create")?);
    for effect in store.create(
        &address,
        BTreeMap::from([(
            "inbox".to_owned(),
            Subscription {
                path: inbox.clone(),
                cursor: 0,
            },
        )]),
        Some(ledger("out/paused")?),
    )? {
        prepared.join(effect)?;
    }
    prepared.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: inbox.clone(),
        }],
        mutation: CommitMutation::Append {
            path: inbox,
            records: vec![Bytes::from_static(b"emit").into()],
        },
    })?;
    PreparedPublish::settle(&provider.commit(prepared.into_request()).await?)?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        runtime.pause(&address, true),
    )
    .await??;
    assert_eq!(
        store.load(&address).await?.state().map(State::epoch),
        Some(0)
    );
    runtime.track(address.clone())?;
    assert!(runtime.drain().await.is_empty());
    assert!(records(&*provider, "out/paused").await?.is_empty());
    runtime.pause(&address, false).await?;
    assert!(runtime.drain().await.is_empty());
    assert_eq!(
        records(&*provider, "out/paused").await?,
        vec![Bytes::from_static(b"r0")]
    );
    Ok(())
}

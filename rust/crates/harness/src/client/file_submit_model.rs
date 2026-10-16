//! Real client/volume/provider paths; only the external durable prefixes vary.
use super::model_provider::{Cuts, Provider};
use super::{FileClient, FileClientDurability, FileClientError, FileClientLimits};
use crate::Error;
use crate::task_volume::{FileKey, Query, TaskVolume, Write};
use acyclic_stream::*;
use futures::StreamExt as _;
use loom::sync::atomic::Ordering;
use std::sync::Arc;

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn run<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

#[derive(Clone, Copy)]
enum Stage {
    Intent,
    Target,
    Checkpoint,
    Reopen,
}
impl Stage {
    fn message(self) -> &'static str {
        match self {
            Self::Intent => "target published before durable intent",
            Self::Target => "checkpoint published before durable target",
            Self::Checkpoint => "client retired before durable checkpoint",
            Self::Reopen => "client restored an unconfirmed checkpoint",
        }
    }
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    OmitGate,
    RejectAll,
    DuplicateTarget,
}

fn flush(
    client: &mut FileClient<Provider, Provider>,
    stage: Stage,
    fault: Fault,
) -> std::result::Result<usize, FileClientError> {
    run(async {
        match fault {
            Fault::RejectAll => {
                Err(Error::Conflict("deliberately refuse healthy intent".into()).into())
            }
            Fault::None | Fault::DuplicateTarget => client.flush().await,
            Fault::OmitGate => {
                client.catch_up().await?;
                match stage {
                    Stage::Intent => client.flush_pending::<false, true, true, true>().await,
                    Stage::Target => client.flush_pending::<true, false, true, true>().await,
                    Stage::Checkpoint => client.flush_pending::<true, true, false, true>().await,
                    Stage::Reopen => client.flush_pending::<true, true, true, true>().await,
                }
            }
        }
    })
}

fn scenario(stage: Stage, fault: Fault) {
    let cuts = Arc::new(Cuts::new());
    let local = Arc::new(Provider {
        inner: MemoryStream::default(),
        cuts: cuts.clone(),
        outbox: true,
    });
    let remote = Arc::new(Provider {
        inner: MemoryStream::default(),
        cuts: cuts.clone(),
        outbox: false,
    });
    let local_path = StreamPath::new("client/outbox").unwrap();
    let target_path = StreamPath::new("agent/inbox").unwrap();
    let outbox = Arc::new(run(TaskVolume::open(local.clone(), local_path.clone())).unwrap());
    let target = Arc::new(run(TaskVolume::open(remote.clone(), target_path.clone())).unwrap());
    let target_base = run(remote.inner.tail(target_path.clone())).unwrap();
    match stage {
        Stage::Intent => cuts.outbox.store(
            run(local.inner.tail(local_path.clone())).unwrap() + 1,
            Ordering::SeqCst,
        ),
        Stage::Target => cuts.target.store(target_base, Ordering::SeqCst),
        Stage::Checkpoint | Stage::Reopen => (),
    }
    let limits = FileClientLimits {
        commands: 1,
        bytes: 1,
        durability: FileClientDurability::Durable,
    };
    let mut client = run(FileClient::open(outbox.clone(), target.clone(), limits)).unwrap();
    let request = Write::new(
        FileKey::parse(".tasks/task/requests/op.json").unwrap(),
        vec![7],
    );
    run(client.enqueue(request.clone())).unwrap();
    let intent = run(local.inner.tail(local_path)).unwrap();
    cuts.intent_position.store(intent, Ordering::SeqCst);
    cuts.target_position
        .store(target_base + 1, Ordering::SeqCst);
    if matches!(stage, Stage::Checkpoint | Stage::Reopen) {
        // This equals the already-reported accepted prefix, so durability does
        // not regress; only the forthcoming checkpoint remains unconfirmed.
        cuts.outbox.store(intent, Ordering::SeqCst);
    }
    cuts.armed.store(true, Ordering::SeqCst);
    let writer = loom::thread::spawn({
        let cuts = cuts.clone();
        move || match stage {
            Stage::Target => cuts.target.store(u64::MAX, Ordering::SeqCst),
            _ => cuts.outbox.store(u64::MAX, Ordering::SeqCst),
        }
    });
    let first = flush(&mut client, stage, fault);
    let retired_early = client.pending() == 0 && cuts.outbox.load(Ordering::SeqCst) < intent + 1;
    let restored_early = if matches!(stage, Stage::Reopen) {
        let restored = if matches!(fault, Fault::OmitGate) {
            run(FileClient::open_checked::<false>(
                outbox.clone(),
                target.clone(),
                limits,
            ))
        } else {
            run(FileClient::open(outbox.clone(), target.clone(), limits))
        };
        restored.is_ok() && cuts.outbox.load(Ordering::SeqCst) < intent + 1
    } else {
        false
    };
    writer.join().unwrap();
    let retry = flush(
        &mut client,
        stage,
        if matches!(fault, Fault::RejectAll) {
            fault
        } else {
            Fault::None
        },
    );
    if matches!(fault, Fault::DuplicateTarget) {
        // Broken acknowledged retry: append the original immutable record
        // again, bypassing the client's production idempotent publication.
        let records = run(remote.inner.read(ReadRequest {
            path: target_path.clone(),
            from: target_base,
            limit: 1,
        }))
        .unwrap();
        let records = run(records.map(|record| record.unwrap().value).collect());
        run(remote.inner.append(AppendRequest {
            path: target_path.clone(),
            records,
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
    }
    let pending = client.pending();
    let acknowledged = client.cursor().position;
    drop(client);
    let restored = run(FileClient::open(outbox, target.clone(), limits));
    let snapshot = run(target.snapshot(&Query::Tasks, 1));
    let published_once = run(remote.inner.tail(target_path)).unwrap() == target_base + 1;
    let progress = first.unwrap_or(0) + retry.unwrap_or(0) == 1
        && pending == 0
        && acknowledged == intent
        && restored.is_ok_and(|client| client.pending() == 0 && client.cursor().position == intent)
        && snapshot.is_ok_and(|snapshot| snapshot.files == [request])
        && published_once;
    assert!(
        !cuts.crossed.load(Ordering::SeqCst) && !retired_early && !restored_early,
        "{}",
        stage.message()
    );
    assert!(
        progress,
        "durable client lost, duplicated, or refused accepted intent"
    );
}

fn detected(check: impl Fn() + Send + Sync + 'static, message: &str) {
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loom::model(check)))
        .expect_err("FileClient mutation survived");
    let text = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains(message), "{text}");
}

#[test]
fn each_durable_prefix_precedes_its_effect() {
    for stage in [
        Stage::Intent,
        Stage::Target,
        Stage::Checkpoint,
        Stage::Reopen,
    ] {
        loom::model(move || scenario(stage, Fault::None));
    }
}

#[test]
fn missing_durability_gate_controls() {
    for stage in [
        Stage::Intent,
        Stage::Target,
        Stage::Checkpoint,
        Stage::Reopen,
    ] {
        detected(move || scenario(stage, Fault::OmitGate), stage.message());
    }
}

#[test]
fn rejecting_all_intent_cannot_satisfy_safety_vacuously() {
    detected(
        || scenario(Stage::Intent, Fault::RejectAll),
        "durable client lost, duplicated, or refused accepted intent",
    );
}

#[test]
fn duplicate_accepted_request_control() {
    detected(
        || scenario(Stage::Target, Fault::DuplicateTarget),
        "durable client lost, duplicated, or refused accepted intent",
    );
}

fn stale_scenario(broken: bool) {
    let outbox_provider = Arc::new(MemoryStream::default());
    let target_provider = Arc::new(MemoryStream::default());
    let outbox_path = StreamPath::new("stale/outbox").unwrap();
    let target_path = StreamPath::new("stale/target").unwrap();
    let outbox = Arc::new(
        run(TaskVolume::open(
            outbox_provider.clone(),
            outbox_path.clone(),
        ))
        .unwrap(),
    );
    let target = Arc::new(
        run(TaskVolume::open(
            target_provider.clone(),
            target_path.clone(),
        ))
        .unwrap(),
    );
    let target_base = run(target_provider.tail(target_path.clone())).unwrap();
    let limits = FileClientLimits {
        commands: 2,
        bytes: 2,
        durability: FileClientDurability::Durable,
    };
    let mut current = run(FileClient::open(outbox.clone(), target.clone(), limits)).unwrap();
    let request = |id| {
        Write::new(
            FileKey::parse(&format!(".tasks/task/requests/op{id}.json")).unwrap(),
            vec![id],
        )
    };
    run(current.enqueue(request(1))).unwrap();
    let mut stale = run(FileClient::open(outbox.clone(), target.clone(), limits)).unwrap();
    run(stale.catch_up()).unwrap();
    // A real await boundary follows reconciliation. Freeze that actor at this
    // cut while its successor publishes and acknowledges B. If the checkpoint
    // changes after the later snapshot instead, the provider CAS fences it.
    run(current.enqueue(request(2))).unwrap();
    let accepted = run(outbox_provider.tail(outbox_path)).unwrap();
    run(current.flush()).unwrap();
    let result = if broken {
        run(stale.flush_pending::<true, true, true, false>())
    } else {
        run(stale.flush_pending::<true, true, true, true>())
    };
    drop(stale);
    let restored = run(FileClient::open(outbox, target, limits)).unwrap();
    let unchanged = result.is_err()
        && restored.pending() == 0
        && restored.cursor().position == accepted
        && run(target_provider.tail(target_path)).unwrap() == target_base + 2;
    assert!(unchanged, "stale opener rewound acknowledged checkpoint");
}

#[test]
fn stale_opener_cannot_rewind_the_reconciled_checkpoint() {
    loom::model(|| stale_scenario(false));
}

#[test]
fn missing_checkpoint_cursor_guard_control() {
    detected(
        || stale_scenario(true),
        "stale opener rewound acknowledged checkpoint",
    );
}

fn order_scenario(broken: bool) {
    let outbox = Arc::new(
        run(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("order/outbox").unwrap(),
        ))
        .unwrap(),
    );
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("order/target").unwrap();
    let target = Arc::new(run(TaskVolume::open(provider.clone(), path.clone())).unwrap());
    let base = run(provider.tail(path.clone())).unwrap();
    let limits = FileClientLimits {
        commands: 2,
        bytes: 2,
        durability: FileClientDurability::Durable,
    };
    let mut client = run(FileClient::open(outbox.clone(), target.clone(), limits)).unwrap();
    for id in [9, 1] {
        run(client.enqueue(Write::new(
            FileKey::parse(&format!(".tasks/task/requests/op{id}.json")).unwrap(),
            vec![id],
        )))
        .unwrap();
    }
    drop(client);
    let mut client = run(FileClient::open(outbox, target, limits)).unwrap();
    if broken {
        // Broken recovery from a key-sorted snapshot instead of publication order.
        client
            .pending
            .make_contiguous()
            .sort_by_key(|pending| pending.write.key.clone());
    }
    let result = run(client.flush());
    let records = run(provider.read(ReadRequest {
        path,
        from: base,
        limit: 2,
    }))
    .unwrap();
    let delivered = run(records
        .map(|record| {
            record
                .ok()
                .and_then(|record| {
                    crate::task_volume::FileValue::from_record(&record)
                        .ok()
                        .flatten()
                })
                .and_then(|file| file.bytes.first().copied())
        })
        .collect::<Vec<_>>());
    assert!(
        matches!(result, Ok(2)) && client.pending() == 0 && delivered == [Some(9), Some(1)],
        "outbox recovery changed input publication order"
    );
}

#[test]
fn recovered_intent_preserves_publication_order() {
    loom::model(|| order_scenario(false));
}

#[test]
fn sorted_recovery_control() {
    detected(
        || order_scenario(true),
        "outbox recovery changed input publication order",
    );
}

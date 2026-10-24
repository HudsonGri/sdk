use super::*;
use crate::local::LocalDurability;
use crate::test_meter::Meter;
use crate::{
    AppendOutcome, CommitCondition, CommitMutation, CommitOutcome, MemoryLimits, MemoryStream,
    OwnedBytes, StreamError, StreamPath, StreamProvider,
};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

const MAXIMUM: u64 = 2 * 1024 * 1024;

fn append(path: &StreamPath) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: path.clone(),
        records: vec![OwnedBytes::from_static(b"first").into()],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(b"append".to_vec())?),
    })
}

fn commit(path: &StreamPath) -> Result<CommitRequest, StreamError> {
    Ok(CommitRequest {
        idempotency_key: IdempotencyKey::new(b"commit".to_vec())?,
        conditions: vec![
            CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            },
            CommitCondition::Absent {
                path: StreamPath::new("tree/coordinated")?,
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: path.clone(),
                records: vec![OwnedBytes::from_static(b"second").into()],
            },
            CommitMutation::Fork {
                source: path.clone(),
                destination: StreamPath::new("tree/coordinated")?,
                at_tail: 1,
                records: vec![OwnedBytes::from_static(b"fork-next").into()],
            },
        ],
    })
}

fn execute_and_retry(
    validated: &Validated<'_>,
    store: &mut Store,
    expected: &IdempotencyOutcome,
) -> Result<(), Box<dyn std::error::Error>> {
    let accepted = validated.execute(store, MAXIMUM, || Ok(()), || 7, None)?;
    assert!(accepted.changed);
    assert_eq!(
        accepted.cut.as_ref().map(|cut| cut.state),
        Some(store.state()?),
        "accepted publication lost its immutable cut"
    );
    assert_eq!(&accepted.outcome.value, expected);
    drop(accepted);
    let before = store.state()?;
    let retried = validated.execute(
        store,
        MAXIMUM,
        || Err::<(), _>(LocalStreamError::Replay(StreamError::AccessDenied)),
        || panic!("retained retry sampled the clock"),
        Some(0),
    );
    assert!(
        retried.is_ok(),
        "retained retry revisited authority or conditions"
    );
    let retried = retried?;
    assert!(
        !retried.changed,
        "retained retry created another publication"
    );
    assert!(
        retried.cut.is_none(),
        "retained retry created a new read cut"
    );
    assert_eq!(
        &retried.outcome.value, expected,
        "retained retry revisited authority or conditions"
    );
    assert_eq!(store.state()?, before);
    Ok(())
}

#[tokio::test]
async fn one_engine_preserves_all_three_families_and_native_output_custody()
-> Result<(), Box<dyn std::error::Error>> {
    for native in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
        let meter = Arc::new(Meter::local());
        let work = native
            .then(|| crate::memory::Work::new(meter.original()))
            .transpose()?;
        let baseline = meter.held.load(Ordering::SeqCst);
        let clock = Arc::new(AtomicU64::new(7));
        let memory = MemoryStream::new_with_clock(MemoryLimits::default(), clock);
        let path = StreamPath::new("tree/source")?;
        let request = append(&path)?;
        let expected = memory.append(request.clone()).await?;
        execute_and_retry(
            &Validated::append(&request, work.clone())?,
            &mut store,
            &IdempotencyOutcome::Append(expected),
        )?;
        let mut request = commit(&path)?;
        let expected = memory.commit(request.clone()).await?;
        let CommitOutcome::Committed(envelope) = &expected else {
            return Err("unexpected coordinated conflict".into());
        };
        let envelope = envelope.clone();
        execute_and_retry(
            &Validated::commit(&mut request, work.clone())?,
            &mut store,
            &IdempotencyOutcome::Commit(expected),
        )?;
        let request = ForkRequest {
            source: path.clone(),
            destination: StreamPath::new("tree/standalone")?,
            at_tail: Some(1),
            idempotency_key: Some(IdempotencyKey::new(b"fork".to_vec())?),
        };
        let expected = memory.fork(request.clone()).await?;
        execute_and_retry(
            &Validated::fork(&request, work.clone())?,
            &mut store,
            &IdempotencyOutcome::Fork(expected),
        )?;
        assert_eq!(store.state()?.decision, 3);
        assert_eq!(store.durable_state()?.decision, 0);
        assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
        if native {
            assert!(
                meter.write_operations.load(Ordering::SeqCst) > 0,
                "cold mutation bypassed its original write ledger"
            );
        }
        let state = store.state()?;
        let mut reader = store.reader(work.clone());
        let output = retained::envelope(&mut reader, state, envelope.commit_id)?
            .ok_or("missing envelope")?;
        assert_eq!(output.value, envelope);
        // The owning output bridge must transfer its original allocation,
        // rather than retire it while the returned vector is still alive.
        let (value, allocation) = output.into_parts();
        if native {
            assert!(allocation.is_some());
            assert!(meter.held.load(Ordering::SeqCst) > baseline);
        }
        drop(value);
        drop(allocation);
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
        drop(reader);
        store.seal(work.clone())?;
        assert_eq!(store.durable_state()?, state);
        drop(store);
        drop(work);
        meter.assert_refunded();
        let mut reopened = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
        assert_eq!(reopened.state()?, state);
        let mut reader = reopened.reader(None);
        let head = query::head(&mut reader, state, &path)?;
        assert_eq!(head.tail, 2);
        assert_eq!(
            head.record(&mut reader, 0)?.ok_or("missing first")?.value,
            OwnedBytes::from_static(b"first")
        );
        assert_eq!(
            head.record(&mut reader, 1)?.ok_or("missing second")?.value,
            OwnedBytes::from_static(b"second")
        );
    }
    Ok(())
}

#[test]
fn new_decisions_check_authority_and_deadline_before_acceptance()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let path = StreamPath::new("source")?;
    let request = append(&path)?;
    let validated = Validated::append(&request, None)?;
    let initial = store.state()?;
    let sampled = Cell::new(false);
    let refused = validated.execute(
        &mut store,
        MAXIMUM,
        || Err::<(), _>(LocalStreamError::Replay(StreamError::AccessDenied)),
        || {
            sampled.set(true);
            7
        },
        None,
    );
    assert!(
        matches!(
            refused,
            Err(LocalStreamError::Replay(StreamError::AccessDenied))
        ),
        "new decision bypassed authority"
    );
    assert!(!sampled.get(), "refused authority sampled the clock");
    assert_eq!(store.state()?, initial);
    let accepted = validated.execute(&mut store, MAXIMUM, || Ok(()), || 7, None)?;
    assert!(matches!(
        accepted.outcome.value,
        IdempotencyOutcome::Append(AppendOutcome::Committed(_))
    ));
    drop(accepted);
    let before = store.state()?;
    assert_new_deadlines(&mut store, &path, before)?;
    Ok(())
}

fn assert_new_deadlines(
    store: &mut Store,
    path: &StreamPath,
    before: root::Root,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = commit(path)?;
    let validated = Validated::commit(&mut request, None)?;
    for [first, second] in [[10, 10], [7, 10]] {
        let sampled = Cell::new(false);
        let refused = validated.execute(
            store,
            MAXIMUM,
            || Ok(()),
            || {
                if sampled.replace(true) { second } else { first }
            },
            Some(10),
        );
        assert!(
            matches!(
                refused,
                Err(LocalStreamError::Replay(StreamError::DeadlineElapsed))
            ),
            "new decision bypassed its publication deadline"
        );
        assert_eq!(store.state()?, before);
    }
    Ok(())
}

#[test]
fn publication_deadlines_cover_time_spent_planning() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let path = StreamPath::new("source")?;
    let request = append(&path)?;
    let accepted =
        Validated::append(&request, None)?.execute(&mut store, MAXIMUM, || Ok(()), || 7, None)?;
    drop(accepted);
    let before = store.state()?;
    assert_new_deadlines(&mut store, &path, before)
}

#[test]
fn validation_preserves_the_original_hash_ledger_and_denials_for_every_family()
-> Result<(), Box<dyn std::error::Error>> {
    for denied in [false, true] {
        for family in 0..3 {
            let meter = Arc::new(Meter::local());
            let work = crate::memory::Work::new(meter.original())?;
            let path = StreamPath::new("source")?;
            let append = append(&path)?;
            let mut commit = commit(&path)?;
            let fork = ForkRequest {
                source: path,
                destination: StreamPath::new("destination")?,
                at_tail: None,
                idempotency_key: None,
            };
            meter.cancelled.store(denied, Ordering::SeqCst);
            let result = match family {
                0 => Validated::append(&append, Some(Arc::clone(&work))),
                1 => Validated::fork(&fork, Some(Arc::clone(&work))),
                _ => Validated::commit(&mut commit, Some(Arc::clone(&work))),
            };
            assert_eq!(
                result.is_ok(),
                !denied,
                "request validation bypassed the original ledger"
            );
            drop(result);
            if denied {
                assert_eq!(meter.hashed.load(Ordering::SeqCst), 0);
                assert!(
                    work.finish::<(), StreamError>(Err(StreamError::Unavailable))
                        .is_err()
                );
            } else {
                assert!(
                    meter.hashed.load(Ordering::SeqCst) > 0,
                    "request validation omitted its hash work"
                );
            }
            drop(work);
            meter.assert_refunded();
        }
    }
    Ok(())
}

#[test]
fn an_accepted_decision_retains_its_publication_guard_until_the_caller_releases_it()
-> Result<(), Box<dyn std::error::Error>> {
    struct Guard<'a>(&'a Cell<bool>);
    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let live = Cell::new(false);
    let request = append(&StreamPath::new("source")?)?;
    let accepted = Validated::append(&request, None)?.execute(
        &mut store,
        MAXIMUM,
        || {
            live.set(true);
            Ok(Guard(&live))
        },
        || {
            assert!(live.get(), "publication began without its authority guard");
            7
        },
        None,
    )?;
    assert!(
        live.get(),
        "accepted decision released its authority guard before settlement"
    );
    store.seal(None)?;
    assert!(live.get());
    drop(accepted);
    assert!(!live.get());
    Ok(())
}

#[test]
fn native_mutation_reports_physical_work_on_its_original_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let request = append(&StreamPath::new("source")?)?;
    let accepted = Validated::append(&request, Some(Arc::clone(&work)))?.execute(
        &mut store,
        MAXIMUM,
        || Ok(()),
        || 7,
        None,
    )?;
    assert!(
        meter.write_operations.load(Ordering::SeqCst) > 0,
        "cold mutation bypassed its original write ledger"
    );
    assert!(meter.write_bytes.load(Ordering::SeqCst) > 0);
    drop(accepted);
    store.seal(Some(Arc::clone(&work)))?;
    assert!(meter.durability_operations.load(Ordering::SeqCst) > 0);
    drop(store);
    drop(work);
    meter.assert_refunded();
    Ok(())
}

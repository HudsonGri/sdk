use super::model_provider::{
    PublicationCut, block_on, fixture_event, pause_before_publish, publish_until_cut, start_fixture,
};
use super::*;
use crate::{
    durable_host::tests::fixture::Fixture,
    executor::{ExecutionEvent, ExecutionJournal},
    task_volume::model_checking::{Fault, set_fault},
};
use acyclic_stream::{IdempotencyKey as StreamKey, ReadRequest, StreamPath};
use futures::StreamExt as _;
use loom::sync::atomic::AtomicU8;

#[inline(never)]
fn setup(
    provider: Arc<PublicationCut>,
) -> (
    Box<Fixture<PublicationCut>>,
    Box<crate::durable_host::TaskJournalOwner<PublicationCut>>,
) {
    let mut coordinator = block_on(Box::pin(open(&provider))).unwrap();
    fixture_event(
        &mut coordinator,
        SchedulerEvent::Declared {
            spec: Box::new(spec(OperationId::from_bytes([1; 16]), 0).unwrap()),
        },
        "declare",
    )
    .unwrap();
    let lease = start_fixture(
        &mut coordinator,
        OperationId::from_bytes([1; 16]),
        &worker("worker"),
        "start",
    )
    .unwrap();
    let (fixture, owner) = Fixture::model(provider.clone(), coordinator, &lease).unwrap();
    (Box::new(fixture), Box::new(owner))
}

fn peer(fixture: &Fixture<PublicationCut>) {
    let mut coordinator = block_on(fixture.model_coordinator().lock());
    fixture_event(
        &mut coordinator,
        SchedulerEvent::Declared {
            spec: Box::new(spec(OperationId::from_bytes([9; 16]), 0).unwrap()),
        },
        "peer",
    )
    .unwrap();
}

fn scenario(peer_before_commit: bool, broken: bool) {
    let phase = Arc::new(AtomicU8::new(0));
    let provider = Arc::new(PublicationCut {
        inner: MemoryStream::default(),
        phase: phase.clone(),
        omit_condition: false,
    });
    let (fixture, owner) = setup(provider.clone());
    let operation = OperationId::from_bytes([2; 16]);
    let event = ExecutionEvent::Started {
        request_digest: [1; 32],
    };
    let key = StreamKey::new(b"entry".to_vec()).unwrap();
    let bytes = acyclic_stream::OwnedBytes::from(b"observation".to_vec());
    if broken {
        set_fault(Fault::StopExecutionFenceRetry);
    }
    let mut work = None;
    let publication =
        Box::pin(owner.append_execution(operation, 0, &key, bytes.clone(), &event, &mut work));
    let publication = if peer_before_commit {
        pause_before_publish(publication, &phase)
    } else {
        publication
    };
    if peer_before_commit {
        peer(&fixture);
    }
    assert!(
        block_on(publication).unwrap(),
        "execution stopped on unrelated coordinator generation"
    );
    if !peer_before_commit {
        peer(&fixture);
    }
    let path = StreamPath::new(format!("harness/v2/execution/{operation}")).unwrap();
    let mut records = block_on(provider.read(ReadRequest {
        path,
        from: 0,
        limit: 2,
    }))
    .unwrap();
    let record = block_on(records.next()).unwrap().unwrap();
    assert_eq!(record.sequence, 0);
    assert_eq!(record.value, bytes);
    assert!(
        block_on(records.next()).is_none(),
        "execution was published twice"
    );
}

#[test]
fn unrelated_coordinator_publication_keeps_the_execution_with_negative_control() {
    // The provider cut fixes the only two relevant linearization orders. Its
    // storage, coordinator authorization and retry are the production paths.
    loom::model(|| scenario(false, false));
    loom::model(|| scenario(true, false));
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true, true)))
        .expect_err("missing execution retry was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("execution stopped on unrelated coordinator generation"),
        "{message}"
    );
}

#[inline(never)]
fn execution(
    fixture: &Fixture<PublicationCut>,
    owner: Box<crate::durable_host::TaskJournalOwner<PublicationCut>>,
) -> Arc<dyn ExecutionJournal> {
    Arc::new(
        fixture
            .execution(*owner, OperationId::from_bytes([2; 16]))
            .unwrap(),
    )
}

fn cancelled_candidate(accepted: bool, broken: bool) {
    let phase = Arc::new(AtomicU8::new(0));
    let provider = Arc::new(PublicationCut {
        inner: MemoryStream::default(),
        phase: phase.clone(),
        omit_condition: false,
    });
    let (fixture, owner) = setup(provider.clone());
    let operation = OperationId::from_bytes([2; 16]);
    let journal = execution(&fixture, owner);
    let event = ExecutionEvent::Started {
        request_digest: [1; 32],
    };
    let writer = journal.clone();
    let candidate = event.clone();
    let future = Box::pin(async move {
        writer
            .append(operation, "entry", &candidate, None)
            .unwrap()
            .await
    });
    let future = if accepted {
        publish_until_cut(future, phase)
    } else {
        pause_before_publish(future, &phase)
    };
    drop(future);
    if broken {
        set_fault(Fault::RetainPendingExecutionCache);
    }
    let resumed = block_on(journal.append(operation, "entry", &event, None).unwrap());
    assert!(
        resumed.is_ok(),
        "cancelled execution candidate survived: {resumed:?}"
    );
    let records = block_on(journal.replay(operation, 0, 2, None).unwrap()).unwrap();
    assert_eq!(records.len(), 1, "cancelled execution candidate survived");
    assert_eq!(records[0].sequence, 1);
}

#[test]
fn cancellation_replays_the_accepted_cut_with_negative_control() {
    loom::model(|| cancelled_candidate(false, false));
    loom::model(|| cancelled_candidate(true, false));
    let error = std::panic::catch_unwind(|| loom::model(|| cancelled_candidate(false, true)))
        .expect_err("cancelled execution cache was trusted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("cancelled execution candidate survived"),
        "{message}"
    );
}

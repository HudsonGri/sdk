use super::model_provider::{PublicationCut, block_on, pause_before_publish};
use super::*;
use crate::{
    durable_host::tests::fixture::Fixture,
    executor::ExecutionEvent,
    task_volume::model_checking::{Fault, set_fault},
};
use acyclic_stream::{IdempotencyKey as StreamKey, ReadRequest, StreamPath};
use futures::StreamExt as _;
use loom::sync::atomic::AtomicU8;
use serde_json::json;

fn scenario(peer_before_commit: bool, broken: bool) {
    let phase = Arc::new(AtomicU8::new(0));
    let provider = Arc::new(PublicationCut {
        inner: MemoryStream::default(),
        phase: phase.clone(),
        omit_condition: false,
    });
    let fixture = block_on(Fixture::new(
        provider.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    ))
    .unwrap();
    let owner = block_on(fixture.running(1)).unwrap();
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
        block_on(fixture.host.admit(fixture.admission(9, json!(0)))).unwrap();
    }
    assert!(
        block_on(publication).unwrap(),
        "execution stopped on unrelated coordinator generation"
    );
    if !peer_before_commit {
        block_on(fixture.host.admit(fixture.admission(9, json!(0)))).unwrap();
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

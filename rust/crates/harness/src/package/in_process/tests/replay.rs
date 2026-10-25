use super::*;

#[tokio::test]
async fn journal_metadata_reads_do_not_materialize_admission_and_checkpoint() {
    use acyclic_test_support::differential::observe_async;
    use journal::EntryRead;
    let (_, _, _, _, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), counter_identity(), json!(0))
        .await
        .unwrap();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    let (initial, reads): (_, Vec<EntryRead>) = observe_async(journal.load()).await;
    assert_eq!(initial.unwrap().tail, 1);
    assert_eq!(reads, [EntryRead(0)], "initial head was decoded twice");
    host.deliver(owner.clone(), event(2, json!(1)))
        .await
        .unwrap();
    let unknown = event(3, json!(1));
    let (retry, reads): (_, Vec<EntryRead>) = observe_async(journal.retry(&unknown)).await;
    assert!(retry.unwrap().is_none());
    assert!(
        reads.is_empty(),
        "retry metadata materialized full journal heads: {reads:?}"
    );
    let (result, reads): (_, Vec<EntryRead>) =
        observe_async(journal.reject_result_identity(&unknown)).await;
    result.unwrap();
    assert!(
        reads.is_empty(),
        "result metadata materialized full journal heads: {reads:?}"
    );
    let proposed = MachineTransition {
        state: json!(1),
        status: MachineStatus::Suspended,
        commands: vec![outbox::command()],
    };
    let (result, reads): (_, Vec<EntryRead>) =
        observe_async(journal.check_commands(&proposed, unknown.operation_id)).await;
    result.unwrap();
    assert!(
        reads.is_empty(),
        "command metadata materialized full journal heads: {reads:?}"
    );
    let (admission, reads): (_, Vec<EntryRead>) =
        observe_async(journal.admission(None).unwrap()).await;
    assert!(admission.unwrap().0.is_some());
    assert_eq!(
        reads,
        [EntryRead(0)],
        "admission read decoded an unrelated checkpoint"
    );
    assert_eq!(
        host.recover(owner).await.unwrap().checkpoint.state,
        json!(1)
    );
}

#[tokio::test]
async fn replay_seeks_to_a_revision_after_interleaved_non_workflow_publications() {
    let (_, _, _, _, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), counter_identity(), json!(0))
        .await
        .unwrap();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    for id in 2..152 {
        host.deliver(owner.clone(), event(id, json!(1)))
            .await
            .unwrap();
        if id == 60 {
            let current = journal.load().await.unwrap();
            let mut metadata = current.last;
            metadata.workflow = None;
            metadata.event = None;
            journal
                .publish(&current.head, current.tail, metadata, None)
                .await
                .unwrap();
        }
    }
    let future = journal.replay(130, 3, None).unwrap();
    assert!(matches!(
        &future,
        crate::store::HistoryContentFuture::Ordinary { .. }
    ));
    let (records, _) = future.await.unwrap();
    assert_eq!(records.len(), 3);
    for (record, expected) in records.iter().zip(130..133) {
        assert_eq!(record.prior.revision, expected);
        assert_eq!(record.prior.state, json!(expected));
    }
    let recovered = host.recover(owner).await.unwrap();
    assert_eq!(
        (recovered.checkpoint.revision, recovered.consumed),
        (150, 150)
    );
    assert_eq!(recovered.checkpoint.state, json!(150));
}

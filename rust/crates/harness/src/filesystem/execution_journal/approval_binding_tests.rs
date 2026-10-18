use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // Only the original provider, private volume and sufficient admitted byte
    // bound may supply the followed publication surface.
    #[test]
    fn task_file_binding_is_exact(
        same_provider in any::<bool>(),
        same_volume in any::<bool>(),
        maximum in prop_oneof![Just(65_535u64), Just(65_536u64), Just(65_537u64), 1u64..=131_072],
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(async {
                let fixture = Fixture::new(
                    Arc::new(MemoryStream::default()),
                    acyclic_fs::Fs::memory(),
                    serde_json::json!({"type":"integer"}),
                    None,
                    &[],
                ).await.unwrap();
                fixture.running(TASK).await.unwrap();
                let journal = fixture.execution(
                    fixture.owner(TASK).await.unwrap(),
                    OperationId::from_bytes([TASK; 16]),
                ).unwrap();
                let files = Arc::new(TaskFiles::open(
                    if same_provider { fixture.stream.clone() }
                    else { Arc::new(MemoryStream::default()) },
                    if same_volume { &fixture.storage } else { &fixture.destination },
                    &fixture.issuer.verifier(&mut None).unwrap(),
                    &journal.scope,
                    maximum,
                ).await.unwrap());
                let valid = same_provider && same_volume && maximum >= 65_536;
                let accepted = journal.with_task_files(files.clone()).is_ok();
                assert_eq!(accepted, valid);
                if !valid {
                    // Negative control: restoring the old unchecked attachment
                    // accepts this same invalid binding and fails the detector.
                    let mut unchecked = fixture.execution(
                        fixture.owner(TASK).await.unwrap(),
                        OperationId::from_bytes([TASK; 16]),
                    ).unwrap();
                    unchecked.task_files = Some(files);
                    assert_ne!(unchecked.task_files.is_some(), valid);
                }
            });
    }
}

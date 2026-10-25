use super::*;
use proptest::prelude::*;

proptest! {
    // Property: every path buffer is funded in all enclosing volume scopes;
    // destroying it refunds that same custody without poisoning any scope.
    #[test]
    fn namespace_copies_fund_nested_volume_custody(
        names in prop::collection::vec("[a-z]{1,24}", 1..8),
        depth in 1..4_usize,
        append in any::<bool>(),
    ) {
        let budget = crate::performance::work_budget(1_000_000, 1_000_000);
        let original = crate::NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let mut volume = original.clone();
        for _ in 0..depth { volume = volume.native_volume_work(budget).unwrap(); }
        let limits = crate::model::VolumeLimits::default();
        let names = names.into_iter().map(|name| LogicalName::new(
            NameEncoding::Utf8, name.into_bytes(), limits.maximum_component_bytes,
        ).unwrap()).collect();
        let path = NamespacePath::new(names, limits).unwrap();
        let baseline = original.live_allocation_bytes();
        let available = volume.remaining().unwrap().peak_allocation_bytes;
        let mut receipt = GenerationReceipt::from_work(&volume).unwrap();
        let copied = clone_namespace(&path, append.then(|| path.components().first().unwrap()), limits, &mut receipt).unwrap();
        let bytes = receipt.custody.bytes();
        prop_assert!(bytes > 0);
        prop_assert_eq!(original.live_allocation_bytes(), baseline + bytes);
        prop_assert_eq!(volume.remaining().unwrap().peak_allocation_bytes, available - bytes);
        drop((copied, receipt));
        prop_assert_eq!(original.live_allocation_bytes(), baseline);
        prop_assert_eq!(volume.remaining().unwrap().peak_allocation_bytes, available);
        // Admission must fail before creating an unfunded path in a child.
        let denied = volume.native_volume_work(crate::WorkBudget { peak_allocation_bytes: 0, ..budget }).unwrap();
        let mut receipt = GenerationReceipt::from_work(&denied).unwrap();
        prop_assert!(clone_namespace(&path, None, limits, &mut receipt).is_err());
        prop_assert_eq!(receipt.custody.bytes(), 0);
        prop_assert!(original.remaining().is_ok());
    }
}


#[tokio::test]
async fn effect_callbacks_bound_their_frame_on_original_native_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let entered = std::cell::Cell::new(0);
    let mut callback = |_| async {
        let bytes = [1_u8; 8192];
        std::future::ready(()).await;
        assert_eq!(std::hint::black_box(&bytes)[0], 1);
        entered.set(entered.get() + 1);
        Ok::<_, MaterializeError>(())
    };
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let budget = if cancelled { WorkBudget::UNBOUNDED } else {
            WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }
        };
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let work = original.native_volume_work(budget)?;
        if cancelled { cancellation.cancel(); }
        let mut control = Some(&mut callback);
        let future = generation_action(&mut control, NativeMaterializationActionPhase::Before, &work);
        let bytes = std::mem::size_of_val(&future);
        eprintln!("EFFECT_CALLBACK_FRAME {bytes}");
        assert!(bytes <= 4096, "effect callback frame is {bytes} bytes");
        assert!(future.await.is_err());
        assert_eq!(entered.get(), 0);
    }
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    let baseline = work.live_allocation_bytes();
    generation_action(&mut Some(&mut callback), NativeMaterializationActionPhase::Before, &work).await?;
    assert_eq!(entered.get(), 1);
    assert!(work.receipt().allocation_operations > 0);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn materialization_children_keep_bounded_frames() {
    type Reader = PinnedReader<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
    type Control = fn(NativeMaterializationActionPhase) -> std::future::Ready<Result<(), MaterializeError>>;
    struct Inputs<'a> {
        reader: &'a Reader,
        root: &'a Arc<(HostRoot, crate::NativeAllocation)>,
        path: &'a NamespacePath,
        host: &'a HeldHostPath,
        file: &'a mut acyclic_native_runtime::NativeFile,
        options: &'a MaterializeOptions,
        cancellation: &'a CancellationToken,
        receipt: &'a mut GenerationReceipt,
        control: &'a mut Option<&'a mut Control>,
    }
    fn future_bytes<F: Future>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let arguments: Option<Inputs<'_>> = None;
    let metadata = future_bytes(|| {
        let input = arguments.unwrap();
        apply_metadata(input.reader, input.root, input.path, input.host,
            None, input.receipt, input.control)
    });
    let arguments: Option<Inputs<'_>> = None;
    let sparse = future_bytes(|| {
        let input = arguments.unwrap();
        materialize_sparse_file(input.reader, input.path, input.file, 0,
            input.options, input.cancellation, input.receipt, input.control)
    });
    assert!(metadata <= 4096, "metadata future is {metadata} bytes");
    assert!(sparse <= 4096, "sparse future is {sparse} bytes");
}

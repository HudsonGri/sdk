use super::*;
#[cfg(any(target_os = "linux", windows))]
use acyclic_test_support::differential;
use acyclic_test_support::{AllocationInfo, measure};
use std::cell::Cell;

/// How a rescan treats a regular file whose prior record and metadata match.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Reuse {
    /// Production: authenticate the content root before staging anything.
    Verified,
    /// Restored previous work: stage, writing every object, then compare.
    PersistFirst,
    /// Unsound control: trust matching metadata without reading a byte.
    MetadataOnly,
}

thread_local! {
    static MODE: Cell<Reuse> = const { Cell::new(Reuse::Verified) };
}

pub(super) fn mode() -> Reuse {
    MODE.with(Cell::get)
}

struct Mode;

impl Mode {
    fn enter(mode: Reuse) -> Self {
        MODE.with(|cell| cell.set(mode));
        Self
    }
}

impl Drop for Mode {
    fn drop(&mut self) {
        MODE.with(|cell| cell.set(Reuse::Verified));
    }
}

const FILES: [(&str, usize); 3] = [("/empty", 0), ("/d/body", 96), ("/d/large", 70_000)];

fn body(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

struct Fixture {
    source: tempfile::TempDir,
    checkout: crate::facade::MemoryCheckout,
    setup: NativeMaterializationWork,
    options: CaptureOptions,
    paths: Vec<NamespacePath>,
}

fn fixture(runtime: &tokio::runtime::Runtime) -> Fixture {
    let source = tempfile::tempdir().unwrap();
    std::fs::create_dir(source.path().join("d")).unwrap();
    for (name, length) in FILES {
        std::fs::write(
            source.path().join(name.trim_start_matches('/')),
            body(length, 7),
        )
        .unwrap();
    }
    let setup =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let mut checkout = runtime
        .block_on(super::capture_policy_tests::native_checkout(&setup))
        .unwrap();
    let limits = checkout.volume_config().limits;
    let paths = FILES
        .map(|(text, _)| {
            NamespacePath::from_portable(
                &crate::path::PortablePath::parse(text, limits).unwrap(),
                limits,
            )
            .unwrap()
        })
        .to_vec();
    let options = CaptureOptions {
        source_root: source.path().to_owned(),
        expected_root_identity: capture_root_identity(source.path()).unwrap(),
        maximum_paths: 8,
        maximum_extent_spans: 8,
    };
    runtime
        .block_on(capture_subtrees_with_native_work(
            &mut checkout,
            &[NamespacePath::new(Vec::new(), limits).unwrap()],
            &options,
            &CapturePolicy::default(),
            setup.cancellation(),
            None,
            &setup,
        ))
        .unwrap();
    Fixture {
        source,
        checkout,
        setup,
        options,
        paths,
    }
}

#[derive(Debug)]
struct Output {
    result: Result<CaptureReceipt, String>,
    bytes: Vec<Vec<u8>>,
    unchanged_root: bool,
    live_before: u64,
    live_after: u64,
}

fn rescan(
    runtime: &tokio::runtime::Runtime,
    fixture: &mut Fixture,
    reuse: Reuse,
    cancelled: bool,
) -> (
    Output,
    serde_json::Value,
    AllocationInfo,
    crate::kernel::GenerationRoot,
) {
    let (work, cancellation) = fixture
        .setup
        .native_volume_work(WorkBudget::UNBOUNDED)
        .unwrap()
        .invocation_view()
        .unwrap();
    if cancelled {
        cancellation.cancel();
    }
    let prior = fixture.checkout.root().clone();
    let baseline = NativeViewBaseline::new(fixture.options.expected_root_identity, true);
    let roots = [NamespacePath::new(Vec::new(), fixture.checkout.volume_config().limits).unwrap()];
    let policy = CapturePolicy::default();
    let live_before = work.live_allocation_bytes();
    let before = serde_json::to_value(work.receipt()).unwrap();
    let mut result = None;
    let heap = {
        let _mode = Mode::enter(reuse);
        let mut future = std::pin::pin!(capture_subtrees_with_native_work(
            &mut fixture.checkout,
            &roots,
            &fixture.options,
            &policy,
            &cancellation,
            Some(&baseline),
            &work,
        ));
        measure(|| result = Some(runtime.block_on(future.as_mut())))
    };
    let mut receipt = serde_json::to_value(work.receipt()).unwrap();
    for (name, value) in receipt.as_object_mut().unwrap() {
        *value = serde_json::json!(value.as_u64().unwrap() - before[name].as_u64().unwrap());
    }
    let live_after = work.live_allocation_bytes();
    let bytes = fixture
        .paths
        .iter()
        .zip(FILES)
        .map(|(path, (_, length))| {
            runtime
                .block_on(fixture.checkout.read_file_range(
                    path,
                    crate::ByteRange {
                        offset: 0,
                        length: length as u64,
                    },
                    &fixture.setup,
                    fixture.setup.cancellation(),
                ))
                .unwrap()
                .bytes
                .as_ref()
                .to_vec()
        })
        .collect();
    let root = fixture.checkout.root().clone();
    let output = Output {
        result: result
            .unwrap()
            .map(|receipt| receipt.value)
            .map_err(|failure| format!("{:?}", failure.error)),
        bytes,
        unchanged_root: root == prior,
        live_before,
        live_after,
    };
    (output, receipt, heap, root)
}

/// Replaces a record's bytes with same-length content and restores its prior
/// metadata: the host's stamps then match although its bytes differ, as after
/// a second write through a shared mapping.
fn tamper(runtime: &tokio::runtime::Runtime, fixture: &mut Fixture, index: usize) {
    let work = &fixture.setup;
    let cancellation = work.cancellation();
    let path = fixture.paths[index].clone();
    let checkout = &mut fixture.checkout;
    runtime.block_on(async {
        let (lookup, _custody) = checkout
            .lookup_batch_no_follow(std::slice::from_ref(&path), work, cancellation)
            .await
            .unwrap();
        let record = lookup.entries[0].record.unwrap();
        let metadata = checkout
            .read_metadata(record.file_id, work, cancellation)
            .await
            .unwrap();
        let replacement = body(FILES[index].1, 99);
        let content = checkout
            .stage_content(
                &mut std::io::Cursor::new(replacement.as_slice()),
                replacement.len() as u64,
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .unwrap()
            .value;
        let changes = vec![
            AuthoredMutation::WriteFromContent {
                path: path.clone(),
                offset: 0,
                content,
            },
            AuthoredMutation::SetMetadata { path, metadata },
        ];
        let slots = work
            .claim(
                (changes.capacity() * std::mem::size_of::<AuthoredMutation>()) as u64,
                1,
                WorkCounters::UNCHARGED,
            )
            .unwrap();
        checkout
            .apply_borrowed_authored_transaction_with_native_work(
                &changes,
                &slots,
                work,
                cancellation,
            )
            .await
            .unwrap();
    });
}

fn candidate(fixture: &Fixture) -> crate::facade::MemoryCheckout {
    let work = &fixture.setup;
    fixture
        .checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
            work.cancellation(),
        )
        .unwrap()
}

fn setup() -> tokio::runtime::Runtime {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

#[test]
fn matching_stamps_never_hide_changed_bytes() {
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = setup();
    for index in [1, 2] {
        let host = body(FILES[index].1, 7);
        let mut fixture = fixture(&runtime);
        tamper(&runtime, &mut fixture, index);
        let tampered = candidate(&fixture);
        let mut outcomes = Vec::new();
        // Each mode rescans its own candidate of the same tampered generation.
        for reuse in [Reuse::Verified, Reuse::PersistFirst, Reuse::MetadataOnly] {
            fixture.checkout = tampered
                .private_candidate(
                    &mut crate::kernel::KernelAccounting::native(
                        &fixture.setup,
                        fixture.setup.cancellation(),
                    ),
                    fixture.setup.cancellation(),
                )
                .unwrap();
            let (output, _, _, root) = rescan(&runtime, &mut fixture, reuse, false);
            let receipt = output.result.clone().unwrap();
            outcomes.push((output.bytes[index] == host, receipt.changed_paths, root));
        }
        let [verified, persisted, metadata_only] = outcomes.try_into().unwrap();
        // Host bytes win and exactly the tampered path changes.
        assert_eq!((verified.0, verified.1), (true, 1), "file {index}");
        assert_eq!(verified, persisted, "file {index}");
        // Negative control: a stamp-trusting skip keeps the stale record.
        assert_eq!(
            (metadata_only.0, metadata_only.1),
            (false, 0),
            "file {index}"
        );
        assert_ne!(metadata_only.2, verified.2, "file {index}");
    }
}

// Literal heap requests and work for one rescan of the unchanged fixture,
// independent of the restored staging control, which must fail them.
#[cfg(target_os = "linux")]
const RESCAN: (u64, [(&str, u64); 6]) = (
    429,
    [
        ("objectBytesWritten", 0),
        ("bytesHashed", 72_231),
        ("bytesEncoded", 384),
        ("bytesCopied", 17_358),
        ("objectProbes", 16),
        ("allocationOperations", 406),
    ],
);
#[cfg(windows)]
const RESCAN: (u64, [(&str, u64); 6]) = (
    417,
    [
        ("objectBytesWritten", 0),
        ("bytesHashed", 72_087),
        ("bytesEncoded", 487),
        ("bytesCopied", 16_551),
        ("objectProbes", 16),
        ("allocationOperations", 394),
    ],
);

#[cfg(any(target_os = "linux", windows))]
fn exact_rescan(heap: AllocationInfo, work: &serde_json::Value) -> Result<(), String> {
    let observed = (
        heap.count_total,
        RESCAN
            .1
            .map(|(name, _)| (name, work[name].as_u64().unwrap())),
    );
    (observed == RESCAN)
        .then_some(())
        .ok_or_else(|| format!("observed {observed:?}, expected {RESCAN:?}"))
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn unchanged_rescan_authenticates_bytes_without_writing_objects() {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = setup();
    for cancelled in [false, true] {
        // Separate host roots, stores, checkouts, cancellations and ledgers.
        let [
            ((before, before_work, before_heap, _), _),
            ((after, after_work, after_heap, _), _),
        ] = differential::pair::<_, _, ()>(
            || fixture(&runtime),
            |mut state, old| {
                let reuse = if old {
                    Reuse::PersistFirst
                } else {
                    Reuse::Verified
                };
                let observed = rescan(&runtime, &mut state, reuse, cancelled);
                drop(state.source);
                observed
            },
        );
        eprintln!(
            "VERIFIED_REUSE cancelled={cancelled} heap={} old_heap={} work={after_work} old_work={before_work}",
            after_heap.count_total, before_heap.count_total
        );
        // Ledgers include independent fixture construction; compare custody per side.
        assert_eq!(
            (&before.result, &before.bytes, before.unchanged_root),
            (&after.result, &after.bytes, after.unchanged_root)
        );
        // The checkout retains its decoded-object cache; verification admits no more.
        let retained = |output: &Output| output.live_after.wrapping_sub(output.live_before);
        eprintln!(
            "VERIFIED_REUSE retained={} old_retained={}",
            retained(&after),
            retained(&before)
        );
        assert!(retained(&after) <= retained(&before));
        assert_eq!(after.result.is_ok(), !cancelled);
        if cancelled {
            assert_eq!(before_work, after_work);
            assert_eq!(before_heap, after_heap);
            continue;
        }
        assert!(after.unchanged_root);
        assert_eq!(
            after.bytes,
            FILES.map(|(_, length)| body(length, 7)).to_vec()
        );
        let receipt = after.result.as_ref().unwrap();
        assert_eq!((receipt.examined_paths, receipt.changed_paths), (5, 0));
        exact_rescan(after_heap, &after_work).unwrap();
        // Negative control: the restored staging work fails the same oracle.
        assert!(exact_rescan(before_heap, &before_work).is_err());
        assert!(before_work["objectBytesWritten"].as_u64().unwrap() > 0);
        // Every source byte is still read in both.
        assert_eq!(
            before_work["sourceBytesRead"],
            after_work["sourceBytesRead"]
        );
    }
}

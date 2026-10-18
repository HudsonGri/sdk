use super::*;
use acyclic_test_support::{AllocationInfo, differential};
use std::cell::Cell;

thread_local! {
    static REOPEN: Cell<bool> = const { Cell::new(false) };
}

pub(in crate::filesystem) fn reopen() -> bool {
    REOPEN.get()
}

fn with_reopen<T>(old: bool, run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            REOPEN.set(self.0);
        }
    }
    let _reset = Reset(REOPEN.replace(old));
    run()
}

fn grant(file: &FileRef) -> Result<ContentGrant> {
    let issuer = AuthorityIssuer::new(
        "pinned",
        [2; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "pinned".into(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([file
            .volume()
            .capability(VolumeOperation::Write, &mut None)?]),
        Some(AgentId::from_bytes([1; 16])),
        None,
        &mut None,
    )?;
    ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        file.volume(),
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )
}

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        7
    }
}

async fn initialize(prefix: usize, bytes: &[u8]) -> Result<(Arc<Host>, FileRef, ContentGrant)> {
    // Authority timestamps are part of the independently initialized read
    // history. A wall clock changes its encoded work between the two runs.
    let stream = Arc::new(acyclic_stream::MemoryStream::new_with_clock(
        acyclic_stream::MemoryLimits::default(),
        Arc::new(Clock),
    ));
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let filesystem = Fs::from_memory_providers(stream, Arc::new(objects), bucket);
    let (host, file) = fixture_with_filesystem(b"seed", filesystem).await?;
    let grant = grant(&file)?;
    let workspace = workspace_ref(
        host.provider.clone(),
        &file.volume().storage_name(&mut None)?,
    )?;
    let mut mutations = vec![WorkspaceMutation::CreateDirectory {
        path: "/history".into(),
    }];
    mutations.extend((0..prefix).map(|index| WorkspaceMutation::PutFile {
        path: format!("/history/{index:04}"),
        bytes: b"history".as_slice().into(),
    }));
    host.apply(
        &workspace,
        None,
        &mutations,
        &IdempotencyKey::new("history")?,
    )
    .await?;
    let target = host
        .put_content(
            file.volume(),
            &grant,
            "retained",
            bytes,
            "text/plain",
            "retained",
            4096,
            &IdempotencyKey::new("retained")?,
            None,
            None,
        )
        .await?;
    host.apply(
        &workspace,
        None,
        &[WorkspaceMutation::PutFile {
            path: "/retained".into(),
            bytes: b"later head".as_slice().into(),
        }],
        &IdempotencyKey::new("later-head")?,
    )
    .await?;
    Ok((host, target, grant))
}

#[derive(Clone, Copy, Debug)]
enum Entry {
    Create,
    Retry,
    Observe,
}

async fn execute(
    host: &Host,
    source: &FileRef,
    grant: &ContentGrant,
    entry: Entry,
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    match entry {
        Entry::Create => {
            host.put_content(
                source.volume(),
                grant,
                "next",
                bytes,
                "text/plain",
                "next",
                4096,
                &IdempotencyKey::new("next")?,
                None,
                io,
            )
            .await
        }
        Entry::Retry => {
            host.put_content(
                source.volume(),
                grant,
                "retained",
                bytes,
                "text/plain",
                "retained",
                4096,
                &IdempotencyKey::new("retained")?,
                None,
                io,
            )
            .await
        }
        Entry::Observe => host
            .content_for_operation(
                source.volume(),
                grant,
                &IdempotencyKey::new("retained")?,
                "retained",
                4096,
                io,
            )
            .await?
            .ok_or_else(|| Error::NotFound("retained publication".into())),
    }
}

fn allocation_bound(before: AllocationInfo, after: AllocationInfo) -> bool {
    after.count_total < before.count_total && after.bytes_total < before.bytes_total
}

#[test]
fn publications_reuse_the_retained_generation_through_every_entry() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Storage(e.to_string()))?;
    for entry in [Entry::Create, Entry::Retry, Entry::Observe] {
        for mode in [None, Some(false), Some(true)] {
            let mut prefix_saving = None;
            for prefix in [16, 32] {
                let [before, after] = differential::pair::<_, _, (&'static str, u64)>(
                    || {
                        runtime
                            .block_on(initialize(prefix, b"published bytes"))
                            .unwrap()
                    },
                    |(host, source, grant), old| {
                        with_reopen(old, || {
                            let mut result = None;
                            let mut receipt = WorkCounters::default();
                            let allocations = acyclic_test_support::measure(|| {
                                result = Some(match mode {
                                    None => runtime
                                        .block_on(execute(
                                            &host,
                                            &source,
                                            &grant,
                                            entry,
                                            b"published bytes",
                                            None,
                                        ))
                                        .and_then(|file| file.copy(&mut None))
                                        .map_err(|e| e.to_string()),
                                    Some(native) => {
                                        accounted(native, 64 * 1024 * 1024, false, |io| {
                                            runtime
                                                .block_on(execute(
                                                    &host,
                                                    &source,
                                                    &grant,
                                                    entry,
                                                    b"published bytes",
                                                    Some(io),
                                                ))
                                                .and_then(|file| file.copy(&mut None))
                                                .map_err(|e| e.to_string())
                                        })
                                        .map(|result| {
                                            receipt = result.1;
                                            result.0
                                        })
                                        .unwrap()
                                    }
                                });
                            });
                            let value = result.unwrap();
                            assert!(value.is_ok(), "{entry:?} {mode:?}: {value:?}");
                            let file = value.unwrap();
                            let workspace = workspace_ref(
                                host.provider.clone(),
                                &file.volume().storage_name(&mut None).unwrap(),
                            )
                            .unwrap();
                            let generation = host.file_generation(&file, None).unwrap();
                            let actual = runtime
                                .block_on(host.read(
                                    &workspace,
                                    Some(&generation),
                                    &format!("/{}", file.path()),
                                    4096,
                                    None,
                                ))
                                .unwrap();
                            assert_eq!(actual.as_ref(), b"published bytes");
                            assert_eq!(
                                runtime
                                    .block_on(host.read(&workspace, None, "/retained", 4096, None))
                                    .unwrap()
                                    .as_ref(),
                                b"later head"
                            );
                            ((file, actual.as_ref().to_vec()), allocations, receipt)
                        })
                    },
                );
                assert_eq!(before.0.0, after.0.0);
                let opens = |trace: &[(&str, u64)]| {
                    trace
                        .iter()
                        .filter(|event| event.0 == "open workspace")
                        .count()
                };
                assert_eq!(opens(&before.1), opens(&after.1) + 1, "{entry:?} {mode:?}");
                let observed = |trace: Vec<(&'static str, u64)>| {
                    trace
                        .into_iter()
                        .filter(|event| {
                            !matches!(
                                event.0,
                                "open workspace"
                                    | "retain"
                                    | "custody-transfer"
                                    | "custody-absorb"
                                    | "custody-take"
                            )
                        })
                        .collect::<Vec<_>>()
                };
                let mut old_trace = observed(before.1);
                let new_trace = observed(after.1);
                // Reopening an authenticated generation decodes its document
                // and index again. Remove only that repeated adjacent pair;
                // preserve the ordering and payload of every remaining event.
                if old_trace != new_trace {
                    let duplicate = old_trace
                        .windows(4)
                        .position(|events| {
                            events[0].0 == "decode document"
                                && events[1].0 == "decode index"
                                && events[..2] == events[2..]
                        })
                        .expect("unexpected observer difference");
                    old_trace.drain(duplicate..duplicate + 2);
                }
                assert_eq!(old_trace, new_trace);
                assert!(
                    allocation_bound(before.0.1, after.0.1),
                    "{entry:?} {mode:?}: {:?} -> {:?}",
                    before.0.1,
                    after.0.1
                );
                assert!(
                    !allocation_bound(before.0.1, before.0.1),
                    "restoring the real reopen must fail the same bound"
                );
                let saving = (
                    before.0.1.count_total - after.0.1.count_total,
                    before.0.1.bytes_total - after.0.1.bytes_total,
                );
                let work = |r: WorkCounters| {
                    [
                        r.authority_records_read,
                        r.backend_read_operations,
                        r.object_probes,
                        r.page_reads,
                        r.items_examined,
                        r.bytes_copied,
                        r.allocation_operations,
                    ]
                };
                let work_saving = std::array::from_fn::<_, 7, _>(|index| {
                    work(before.0.2)[index]
                        .checked_sub(work(after.0.2)[index])
                        .expect("reuse added work")
                });
                eprintln!(
                    "retained {entry:?} mode={mode:?} prefix={prefix} saved={saving:?} work={work_saving:?}"
                );
                if let Some(expected) = prefix_saving {
                    assert_eq!((saving, work_saving), expected);
                }
                prefix_saving = Some((saving, work_saving));
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn retained_reads_preserve_bytes_and_errors(
        bytes in prop::collection::vec(any::<u8>(), 0..4096),
        maximum in 0_u64..8193, tamper in any::<bool>(), cancelled in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for mode in [None, Some(false), Some(true)] {
            let [before, after] = [true, false].map(|old| {
                let (host, mut file) = runtime.block_on(fixture(&bytes)).unwrap();
                if tamper {
                    let mut digest = *file.descriptor().sha256(); digest[0] ^= 1;
                    file = FileRef::new(file.volume().clone(), file.path().into(), file.version().into(),
                        FileDescriptor::new(digest, bytes.len() as u64, "text/plain".into()).unwrap(), file.display_name().into(), &mut None).unwrap();
                }
                let workspace = workspace_ref(host.provider.clone(), &file.volume().storage_name(&mut None).unwrap()).unwrap();
                let opened = runtime.block_on(host.open(&workspace, None)).unwrap();
                let generation_ref = host.file_generation(&file, None).unwrap();
                let generation = runtime.block_on(host.generation(&opened, &generation_ref, None)).unwrap();
                with_reopen(old, || {
                    let read = |io: Option<&PreparationIo>| runtime.block_on(host.read_retained_file(&generation, &file, maximum, io))
                        .map(|bytes| bytes.as_ref().to_vec()).map_err(|e| format!("{e:?}"));
                    match mode {
                        None => read(None),
                        Some(native) => accounted(native, 64 * 1024 * 1024, cancelled, |io| read(Some(io))).unwrap().0,
                    }
                })
            });
            prop_assert_eq!(before, after, "mode {:?}", mode);
        }
    }
}

#[test]
fn retained_native_bytes_keep_original_funding_until_the_last_owner_drops() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for old in [true, false] {
        let (host, file) = runtime.block_on(fixture(b"original funding"))?;
        let workspace = workspace_ref(
            host.provider.clone(),
            &file.volume().storage_name(&mut None)?,
        )?;
        let opened = runtime.block_on(host.open(&workspace, None))?;
        let generation = runtime.block_on(host.generation(
            &opened,
            &host.file_generation(&file, None)?,
            None,
        ))?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 64 * 1024 * 1024),
            &cancellation,
        )
        .map_err(|(error, _)| Error::Invalid(error.to_string()))?;
        let baseline = work.live_allocation_bytes();
        assert_eq!(
            baseline,
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
        let io = PreparationIo::from_native_work(&work)?;
        let bytes = with_reopen(old, || {
            runtime.block_on(host.read_retained_file(&generation, &file, 4096, Some(&io)))
        })?;
        assert_eq!(bytes.as_ref(), b"original funding");
        drop(io);
        drop(generation);
        drop(opened);
        drop(host);
        let funded = |bytes: &acyclic_stream::OwnedBytes| {
            work.live_allocation_bytes()
                .checked_sub(baseline)
                .is_some_and(|live| live >= bytes.retained_capacity().unwrap() as u64 && live != 0)
        };
        assert!(funded(&bytes), "returned bytes lost their original ledger");
        let unfunded: acyclic_stream::OwnedBytes = bytes.as_ref().to_vec().into();
        drop(bytes);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "returned custody leaked"
        );
        assert!(
            !funded(&unfunded),
            "an unfunded copy must fail the same predicate"
        );
    }
    Ok(())
}

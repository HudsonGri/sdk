//! Held-file demand reads: an independent baseline, exact work, reader reuse
//! under roots that are not local, and a source gate against zero-fill.
use super::native::{NativeDemandSource, demand_read_work};
use super::*;
use crate::model::{FilesystemProfile, VolumeLimits};
use acyclic_test_support::differential;
use proptest::prelude::*;
use std::error::Error;

type Work = (&'static str, u64, u64);
/// Bytes, or the failure's variant name.
type Outcome = Result<Vec<u8>, String>;

const MAXIMUM_READ: u64 = 1024 * 1024;

fn path(value: &str) -> NamespacePath {
    let portable =
        crate::path::PortablePath::parse(value, VolumeLimits::default()).expect("test path parses");
    NamespacePath::from_portable(&portable, VolumeLimits::default()).expect("test path is valid")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime starts")
}

/// A source over a directory holding `contents` as `/file`, and that file held open.
fn held(
    runtime: &tokio::runtime::Runtime,
    contents: &[u8],
    remote: bool,
) -> Result<(tempfile::TempDir, NativeDemandSource, Box<dyn DemandFile>), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), contents)?;
    runtime.block_on(async {
        let source = NativeDemandSource::open(
            std::borrow::Cow::Borrowed(directory.path()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
        let source = if remote { source.remote() } else { source };
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source
            .lookup(
                reference,
                &path("/file"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .ok_or("file absent")?;
        let file = source
            .open_file(
                reference,
                &path("/file"),
                node.version,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        Ok::<_, Box<dyn Error>>((directory, source, file))
    })
}

fn outcome(result: Result<acyclic_stream::OwnedBytes, DemandError>) -> Outcome {
    result
        .map(|bytes| bytes.as_ref().to_vec())
        .map_err(|error| {
            format!("{error:?}")
                .split(['(', ' '])
                .next()
                .unwrap_or("")
                .to_owned()
        })
}

/// One read, its outcome, the work it spent, and its trace on this thread.
fn production_read(
    file: &dyn DemandFile,
    offset: u64,
    length: u64,
) -> (Outcome, WorkCounters, Vec<Work>) {
    let cancellation = CancellationToken::new();
    let ((result, spent), trace) = differential::observe::<_, _, Work>(
        || (),
        |()| {
            let mut accounting =
                crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            let result = file.read_range(offset, length, &mut accounting);
            (result.map(|chunk| chunk.bytes), accounting.spent())
        },
    );
    (outcome(result), spent, trace)
}

/// The contract, written separately: requests over 1 MiB or past u64 are
/// invalid; otherwise the bytes from `offset` to the request's end or end of
/// file; a file changed since it was opened is stale.
fn baseline_read(contents: &[u8], current: &[u8], offset: u64, length: u64) -> Outcome {
    if length > MAXIMUM_READ || offset.checked_add(length).is_none() {
        return Err("InvalidRequest".to_owned());
    }
    if contents != current {
        return Err("StaleVersion".to_owned());
    }
    let start = usize::try_from(offset).map_or(contents.len(), |offset| offset.min(contents.len()));
    let end =
        usize::try_from(offset + length).map_or(contents.len(), |end| end.min(contents.len()));
    Ok(contents[start..end.max(start)].to_vec())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    /// Inline and remote held-file reads return exactly the baseline's bytes
    /// or failure, across sizes, ranges past the end, oversized requests and edits.
    #[test]
    fn held_file_reads_match_independent_baseline(
        size in prop_oneof![0_usize..300, 70_000_usize..80_000],
        remote in any::<bool>(),
        offset in prop_oneof![0_u64..90_000, Just(u64::MAX - 1)],
        length in prop_oneof![0_u64..90_000, Just(MAXIMUM_READ), Just(MAXIMUM_READ + 1)],
        edited in any::<bool>(),
    ) {
        let runtime = runtime();
        let contents: Vec<u8> = (0..size).map(|byte| (byte * 31 + 7) as u8).collect();
        let (directory, _source, file) = held(&runtime, &contents, remote).unwrap();
        let current = if edited {
            let edit: Vec<u8> = contents.iter().map(|byte| byte ^ 0x5a).chain([1]).collect();
            std::fs::write(directory.path().join("file"), &edit).unwrap();
            edit
        } else {
            contents.clone()
        };
        let expected = baseline_read(&contents, &current, offset, length);
        let (actual, _, _) = production_read(file.as_ref(), offset, length);
        prop_assert_eq!(actual, expected);
    }
}

/// Inline reads fill uninitialized capacity: no zero-filled bytes, no copy
/// charge for served bytes, one positional read per call that the file can satisfy.
#[test]
fn held_file_reads_do_exact_uninitialized_work() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let contents: Vec<u8> = (0..100_000_u32).map(|byte| (byte * 13) as u8).collect();
    let (_directory, _source, file) = held(&runtime, &contents, false)?;
    // The empty read at end of file spends only the per-read path and stat
    // work; a read that serves bytes adds exactly one read of those bytes.
    let mut empty = None;
    for (offset, length, served) in [
        (100_000_u64, 64_u64, 0_u64),
        (10, 4096, 4096),
        (99_000, 4096, 1000),
    ] {
        for broken in [false, true] {
            let run = || production_read(file.as_ref(), offset, length);
            let (bytes, spent, trace) = if broken {
                demand_read_work::restoring_old_work(run)
            } else {
                run()
            };
            let start = offset as usize;
            assert_eq!(bytes, Ok(contents[start..start + served as usize].to_vec()));
            let mut expected = vec![("storage", 0, served)];
            if served > 0 {
                expected.push(("read", offset, served));
            }
            if broken {
                if served > 0 {
                    assert_ne!(trace, expected, "restored zero-fill was not detected");
                }
                continue;
            }
            assert_eq!(trace, expected);
            let base = *empty.get_or_insert(spent);
            assert_eq!(
                spent.bytes_copied, base.bytes_copied,
                "served bytes were charged as copied"
            );
            assert_eq!(spent.output_bytes, served);
            // The buffer is the only allocation that grows with the request.
            assert!(spent.peak_allocation_bytes <= base.peak_allocation_bytes + served);
            #[cfg(unix)]
            assert_eq!(
                spent,
                WorkCounters {
                    backend_read_operations: base.backend_read_operations + u64::from(served > 0),
                    source_bytes_read: served,
                    output_bytes: served,
                    // The buffer allocates only when it holds bytes.
                    allocation_operations: base.allocation_operations + u64::from(served > 0),
                    peak_allocation_bytes: spent.peak_allocation_bytes,
                    ..base
                }
            );
        }
    }
    Ok(())
}

/// Reads under a root that is not local reuse one reader instead of
/// starting a thread for each read.
#[test]
fn remote_reads_reuse_one_reader() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let contents = b"remote contents".to_vec();
    let (_directory, source, file) = held(&runtime, &contents, true)?;
    for offset in 0..8_u64 {
        let (bytes, _, _) = production_read(file.as_ref(), offset, 4);
        let start = offset as usize;
        assert_eq!(bytes, Ok(contents[start..start + 4].to_vec()));
    }
    assert_eq!(source.remote_readers_started(), 1);
    Ok(())
}

/// Counts forbidden identifiers in every production item of a source file,
/// skipping only nodes compiled solely for tests.
fn production_uses(source: &str, forbidden: &str) -> usize {
    use syn::visit::Visit;
    fn test_only(attributes: &[syn::Attribute]) -> bool {
        attributes.iter().any(|attribute| {
            attribute.path().is_ident("cfg")
                && attribute.meta.require_list().is_ok_and(|list| {
                    let tokens = list.tokens.to_string();
                    tokens == "test" || tokens.starts_with("all (test ,")
                })
        })
    }
    fn expression_attributes(expression: &syn::Expr) -> &[syn::Attribute] {
        match expression {
            syn::Expr::Block(block) => &block.attrs,
            syn::Expr::Call(call) => &call.attrs,
            syn::Expr::MethodCall(call) => &call.attrs,
            syn::Expr::If(branch) => &branch.attrs,
            syn::Expr::Macro(invocation) => &invocation.attrs,
            _ => &[],
        }
    }
    fn item_attributes(item: &syn::Item) -> &[syn::Attribute] {
        match item {
            syn::Item::Fn(item) => &item.attrs,
            syn::Item::Mod(item) => &item.attrs,
            syn::Item::Impl(item) => &item.attrs,
            syn::Item::Use(item) => &item.attrs,
            syn::Item::Const(item) => &item.attrs,
            syn::Item::Static(item) => &item.attrs,
            syn::Item::Struct(item) => &item.attrs,
            syn::Item::Enum(item) => &item.attrs,
            syn::Item::Type(item) => &item.attrs,
            _ => &[],
        }
    }
    struct Production<'a> {
        forbidden: &'a str,
        uses: usize,
    }
    impl<'ast> Visit<'ast> for Production<'_> {
        fn visit_item(&mut self, item: &'ast syn::Item) {
            if !test_only(item_attributes(item)) {
                syn::visit::visit_item(self, item);
            }
        }
        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !test_only(&item.attrs) {
                syn::visit::visit_impl_item_fn(self, item);
            }
        }
        fn visit_stmt(&mut self, statement: &'ast syn::Stmt) {
            let attributes = match statement {
                syn::Stmt::Local(local) => &local.attrs[..],
                syn::Stmt::Macro(invocation) => &invocation.attrs[..],
                syn::Stmt::Expr(expression, _) => expression_attributes(expression),
                syn::Stmt::Item(item) => item_attributes(item),
            };
            if !test_only(attributes) {
                syn::visit::visit_stmt(self, statement);
            }
        }
        fn visit_expr(&mut self, expression: &'ast syn::Expr) {
            if !test_only(expression_attributes(expression)) {
                syn::visit::visit_expr(self, expression);
            }
        }
        fn visit_ident(&mut self, ident: &'ast syn::Ident) {
            if ident == self.forbidden {
                self.uses += 1;
            }
        }
    }
    let file = syn::parse_file(source).expect("production source parses");
    let mut production = Production { forbidden, uses: 0 };
    production.visit_file(&file);
    production.uses
}

const READ_SOURCES: [(&str, &str); 3] = [
    ("demand/native/file.rs", include_str!("native/file.rs")),
    (
        "native_host/yielding.rs",
        include_str!("../native_host/yielding.rs"),
    ),
    (
        "native_host/spare.rs",
        include_str!("../native_host/spare.rs"),
    ),
];

fn read_gate_violations(sources: &[(&str, String)]) -> Vec<String> {
    let mut violations = Vec::new();
    for (name, source) in sources {
        for forbidden in ["resize", "PreparedOwnedBytes", "spawn", "read_at"] {
            let uses = production_uses(source, forbidden);
            if uses != 0 {
                violations.push(format!("{name} uses {forbidden} {uses} times"));
            }
        }
    }
    violations
}

/// Held-file reads never zero-fill, never read into initialized slices, and
/// never start a thread per read: only the spare-capacity reads and the
/// remote reader pool may supply their bytes.
#[test]
fn held_file_reads_have_one_uninitialized_read() {
    let sources: Vec<_> = READ_SOURCES
        .iter()
        .map(|(name, source)| (*name, (*source).to_owned()))
        .collect();
    assert_eq!(read_gate_violations(&sources), Vec::<String>::new());
    // Negative control: the removed zero-fill is caught anywhere in production.
    let anchor = "    fn fill(\n";
    let zeroing =
        "    fn zero(data: &mut Vec<u8>) {\n        data.resize(data.capacity(), 0);\n    }\n";
    let broken: Vec<_> = sources
        .iter()
        .map(|(name, source)| {
            (
                *name,
                source.replacen(anchor, &format!("{zeroing}{anchor}"), 1),
            )
        })
        .collect();
    assert_eq!(
        read_gate_violations(&broken),
        vec!["demand/native/file.rs uses resize 1 times".to_owned()]
    );
}

/// Paired measurement, not a gate: `cargo test --release -p acyclic-fs --lib
/// demand::native_read_tests::measure -- --ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_held_file_reads() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let contents: Vec<u8> = (0..2_000_000_u32).map(|byte| (byte * 7) as u8).collect();
    let (_inline_directory, _inline, inline) = held(&runtime, &contents, false)?;
    let (_remote_directory, _remote, remote) = held(&runtime, &contents, true)?;
    let inline = std::sync::Arc::<dyn DemandFile>::from(inline);
    let time = |rounds: u32, read: &dyn Fn(u32)| {
        for round in 0..rounds / 10 {
            read(round);
        }
        let started = std::time::Instant::now();
        for round in 0..rounds {
            read(round);
        }
        started.elapsed().as_nanos() / u128::from(rounds)
    };
    for length in [4096_u64, 65_536, MAXIMUM_READ] {
        let rounds = if length == MAXIMUM_READ { 400 } else { 4000 };
        let read = |file: &dyn DemandFile, round: u32| {
            let offset = u64::from(round % 16) * 4096;
            let (bytes, _, _) = production_read(file, offset, length);
            assert_eq!(bytes.map(|bytes| bytes.len() as u64), Ok(length));
        };
        let old = time(rounds, &|round| {
            demand_read_work::restoring_old_work(|| read(inline.as_ref(), round));
        });
        let new = time(rounds, &|round| read(inline.as_ref(), round));
        eprintln!("inline {length} B: zero-filled {old} ns, uninitialized {new} ns");
    }
    // The removed remote path started a thread per read; its baseline is that
    // same read on a freshly started thread.
    let spawned = time(2000, &|round| {
        let file = std::sync::Arc::clone(&inline);
        std::thread::spawn(move || {
            let (bytes, _, _) = production_read(file.as_ref(), u64::from(round % 16), 4096);
            assert!(bytes.is_ok());
        })
        .join()
        .unwrap();
    });
    let pooled = time(2000, &|round| {
        let (bytes, _, _) = production_read(remote.as_ref(), u64::from(round % 16), 4096);
        assert!(bytes.is_ok());
    });
    eprintln!("remote 4096 B: thread per read {spawned} ns, reused reader {pooled} ns");
    Ok(())
}

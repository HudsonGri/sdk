# Local Stream group recovery: correctness and paired performance

Baseline: upstream main `66ddfd542be590eca8f5159b1c29cf17ccee7c0c`.
Measured candidate: `3b780f2a258af06eea3ba0441e10d035ffbf5f81`, V2 group seals.

The baseline regression `reordered_unacknowledged_group_is_recoverable` failed
with `Corrupt` before production changes. It now preserves the acknowledged
prefix and repairs the unacknowledged reordered suffix. This replaces #552's
predecessor-fsync serialization with a boundary per shared flush.

## Method

The exact same `rust/crates/stream/examples/local-group-bench.rs` was built against
baseline production code and candidate production code using Rust 1.98.1 release
optimization, `--features local`, and the Windows GNU target. Each executable
uses four Tokio workers, FullFlush, fresh temporary roots on NTFS, one append at
a time per writer, separate writer paths, and no idempotency keys. Every measured
run reopens its root and verifies every writer's final tail.

Host: AMD Ryzen 7 9800X3D, 8 cores / 16 logical processors, Windows x64, fixed NTFS
volume. Compilation and other test runs finished before measurement. This is one
host/device result, not qualification of other devices' power-loss guarantees.

Both executables first warmed up with 64 appends per writer in every workload.
Five measured pairs used 256 appends per writer, alternating baseline/candidate
then candidate/baseline order. The runner and all 40 measured rows are retained:
[runner](local-group-paired.ps1), [raw CSV](2026-10-18-local-group-results.csv).
Flush counts come from the production successful-flush event; they exclude
startup and reopen, which are outside the measured interval. Timing includes
normal append acknowledgement and scheduling.

Executable SHA-256:

- Baseline: `BE92BFD83590598B3EEC47D3E1930E3E583C0981861909924584FDB146E666C2`
- Candidate: `9C8638353F3A8C26AFCF2A52391EF025E51669B3FEA081EDEF821ED6896B25AE`

## Results

Throughput, flush counts and p95 are medians over five runs. Change is the median
of the five **paired** throughput ratios, so it need not equal the ratio of the
two independently selected throughput medians.

| Writers / payload | Baseline ops/s | Candidate ops/s | Paired change | Flushes baseline → candidate | p95 µs baseline → candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 / 256 B | 1,703 | 1,645 | −0.64% | 256 → 256 | 698 → 694 |
| 8 / 256 B | 2,117 | 2,459 | +15.97% | 1,655 → 1,409 | 4,716 → 4,236 |
| 32 / 256 B | 2,036 | 2,407 | +14.18% | 6,651 → 5,697 | 17,554 → 15,469 |
| 8 / 64 KiB | 1,235 | 1,261 | −1.41% | 2,048 → 2,047 | 7,807 → 8,109 |

Every concurrent 256-byte pair improved: +8.0–22.7% for eight writers and
+5.2–27.0% for 32 writers. Capturing the flush group after acquiring the device
gate lets it cover more queued appends. The single-writer paired range was
−9.4–+6.7%; the 64-KiB range was −2.7–+35.4%, including a slow baseline run.
There is no claim that every workload becomes faster. Group commit remains
available, and measured concurrent small-record throughput improves. The seal
costs one additional 78-byte write per flush, with no extra steady-state fsync.
Single-writer journal growth is therefore 19,968 extra bytes for 256 commands.

To reproduce, copy the benchmark example unchanged into a checkout at the
baseline SHA, build both checkouts with:

```sh
rustup run 1.98.1-x86_64-pc-windows-gnu cargo build -p acyclic-stream --features local --release --example local-group-bench --target x86_64-pc-windows-gnu
```

Keep the resulting executables separately, then run:

```powershell
./docs/benchmarks/local-group-paired.ps1 -Baseline ./baseline.exe -Candidate ./candidate.exe -Output ./results.csv
```

## Correctness and concurrency evidence

- Reordered raw frames, surviving same-group seals, every byte cut through two
  commands and their seal, and startup resealing before the next group.
- Validated V1 migration, every replacement-header cut after snapshot install,
  and rejection of a damaged V1 prefix without migration or file modification.
- Capacity admission across a racing seal, deferred native-ticket replacement
  and refunds, late cancellation, and two near-limit commands in one group.
- Real production path with one device flush stalled: 16 other writers reach
  the journal before release; all 17 commands require exactly two seals/flushes.
  Aborting the first caller does not abort its owned durability work. A competing
  explicit flush shares the same device gate.
- Compaction and explicit flush race the stalled group; reopen preserves both
  commands. A physical seal write failure poisons before any subsequent seal.
- The real-path Loom group model exhausts schedules at two preemptions, with no
  time/permutation cap. Removing only the production device gate triggers
  `seal preceded predecessor flush`. Existing replay/flush promotion and its
  negative control also pass.

Validation commands:

Final results: 296 unit tests passed; all four group/replay Loom tests passed;
package formatting, standalone formatting of changed includes, and diff checks passed.

```sh
cargo test -p acyclic-stream --features local,test-support --lib -- --test-threads=4
RUSTFLAGS="--cfg acyclic_loom" cargo test -p acyclic-stream --no-default-features --features local --test local_group_model --test local_replay_model -- --test-threads=1
cargo fmt -p acyclic-stream -- --check
git diff --check
```

The Windows GNU Loom run used Rust's bundled `rust-lld.exe` and
`-C linker-flavor=ld.lld`: the GNU-driver-linked model executable hit a DEP
violation before test enumeration; the LLD-linked executable ran all four tests.
Changed included Rust files were additionally checked with standalone rustfmt,
because Cargo's module traversal does not visit `include!` sources.

Strict full clippy is blocked by existing issues outside this change: auth
documentation/indexing/clone lints and denied unwraps in Stream snapshot jobs and
snapshot-ready tests. A `--no-deps` diagnostic run reported no lint in the new
production seal/recovery code. These existing violations were not suppressed.

## Compatibility and limits

Healthy V1 roots migrate through snapshot installation and epoch restart before
startup exposes state. Ambiguous damaged V1 journals still fail closed; missing
historical group boundaries cannot be reconstructed. V1 snapshots remain
readable, and old binaries reject V2 journals.

The final flush group can be a torn unacknowledged group or subsequent media
damage, so recovery may discard its damaged suffix. A later group's seal proves
earlier corruption and preserves fail-closed behavior. This is not hostile-storage
authentication. See the packaged `LocalStream` rustdoc and
[design](../superpowers/specs/2026-10-18-local-stream-group-recovery.md).

## Review follow-up

Review added positive argument checks to both benchmark tools and made the
Windows GNU target/toolchain explicit in the reproduction command. The timing
samples above remain measurements of the original candidate commit. A positive
runner smoke test still produces all eight baseline/candidate workload rows;
zero/negative pairs or iterations are rejected before output/device IO, and
zero Rust iterations or rounds return an input error rather than panicking.

Admission now reserves a second seal only when an older unsealed group can close
before the accepted append. Exact-fit empty and fully sealed groups are covered
by a regression that failed before this change; the racing-seal regression still
passes. The Loom invariant remains active in release models, and worker panic
payloads are preserved. A poisoned device gate has its own `test-support` crash
simulation error variant; downstream exhaustive matches on `CutSimulationError`
may need to handle `DeviceLock`.

Follow-up validation: 298 unit tests and all four Loom tests in both debug and
release passed. The README is unchanged because its CI guard requires human
maintainer approval even for paragraph deletions; the packaged Rust API format
documentation remains the V2 reference.

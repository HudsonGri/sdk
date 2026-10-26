# Streams partial checkpoint review

`../../scripts/tests/fixtures/proof-policy/streams-checkpoint-v3/receipt.json` captures real owner logs, actual committed source, real git commit/tree identities and signature output. It replaces no historical receipt. The earlier41d8 handover's six failures remain a rejected historical observation; the newer run no longer reproduces them. No Rust compile or provider run was repeated for this admission.

## Source and observed selections

Native snapshot `3c6e21324094455a7ad425db04c00a1fd7c65d17` and signed `391d21d2f2d0d82519d4dcd842909a4c656c610a` have the same actual git tree `9582f891a605b1acb3225c42764273641401a9ec`. Raw native output reports381 library tests8.98s, ten shared memory/local wave1 cases0.04s, persistent journal/snapshot recovery0.65s and eight Loom cases0.01s. The focused proof suites are below1s; the broad library suite is close to the10s budget.

TypeScript snapshot `e8b7315427e3c1624f80737dc5773c80ccc81c95` and current signed draft1522 `3a348ebf1646daf71a35c6f849caf793620f5e54` share actual tree `a06cfb36d78021b8dd497bad4d158ebcaa07675f`. Explicit selection reports105 tests across14 files6.37s in Bun. This tree differs from the native-tested tree. The raw earlier56pass/1fail alias test and invalid broad static/Bun run are also preserved; they are not counted as positive qualification. The later explicit selected run is a different input receipt. No Node, browser-engine, staged native addon or hosted endpoint qualification is inferred.

## Invariants and control classification

| Invariant | Actual source/proof entrypoint | Control and limits |
| --- | --- | --- |
| Atomic descendant heads at one fork cut; source/private isolation | `src/memory/recovery_model.rs::subtree_cut`, `tests/wave1_loom.rs::subtree_fork_races_coordinated_descendant_writes` | Preemption bound2. RootOnlyFork changes the real `memory/commit.rs` fork publication branch under the Loom fault seam; the unchanged expected assertion panics. Memory kernel only; durable/hosted/membership/GC histories remain open. |
| Nested child excludes an existing children subtree without changing its source | `recovery_model.rs::subtree_exclusions`, `wave1_loom.rs::nested_fork_excludes_existing_siblings` | Bound1. IgnoreForkExclusions changes real commit behavior. This fixed witness does not establish every exclusion/prefix/codec edge. |
| Projected headers/values/framing obey page byte bound | `recovery_model.rs::page_tail_bytes` calls the public `conformance::verify_page_tail_bytes` oracle | Bound0. IgnorePageRecordBytes changes real `memory/provider.rs` accounting to path bytes only. No descendant-cardinality, retained-cut or authority bound is inferred. |
| Two projected statuses share one coordinated publication cut | `recovery_model.rs::page_tail_cut`, `wave1_loom.rs::page_statuses_share_one_atomic_cut` | Bound2. The current negative calls children_page twice and splices a returned item in the test model. It is **model postprocessing**, not an actual old/mutated production projection. A production-path independent-read mutation with the unchanged oracle remains required. |

The models use the production MemoryStream state machine with the `query_sync` Loom lock/watch shim. They do not prove Tokio internals or hosted protocol behavior. The scheduler explicitly sets preemption bounds and disables permutation/time truncation. Three control branches mutate real production locations; the fourth has the narrower classification above. Expected-panic test output plus matching source is an observed control, not a substitute for immutable executed-artifact identity.

## Admission and remaining work

Compiled input manifests, executable hashes and the actual generated WASM hash were not retained in these logs. Source/tree equality is independently verified, but exact executed-artifact provenance is incomplete. The current signed head's full native qualification is also absent. Therefore the receipt has zero complete positive proofs and no qualified modes. A source snapshot is not admitted as a new dependency pin.

The evidence checker validates actual selections/runtimes, tree pairs, capture bytes, control classification and missing provenance. Three added policy tests refuse rehashed promotions to a mode/current head/executable proof and refuse relabelling the postprocessed page as a production control. These reporting-policy tests do not qualify Streams semantics. Future domain producers should retain exact compiler-input and executable/WASM manifests once, on changed source; no identical rerun is requested to fill historical metadata.

Complete HST01..12, namespace membership cuts, lifetime dedup/attempt fences, persistent fork closure/GC/restore, inherited wake, real credit/authorization and arbitrary hosted endpoint remain open in the shared inventory. Follow on absence is still NotFound/no effect. Actor shared readiness and A5 retained-cut custody remain owner deliverables. All30 complete cross-mode obligations remain unqualified.

# A7 proof contract and canonical seams (verify, 2026-10-26)

## Decisions

Extend `acyclic-conformance` with a public provider-generic `client` suite. The subject is the production WireSession, canonical Streams session client and ClientViews reducer. Platform adapters supply I/O, credentials and fixture setup only. Do not introduce another session core or extend TaskVolume. The proposed WireSession/PageWindow types in api/client-perf.md are not yet implemented exports.

The existing seam is `stream::session::Client::with_transport` in `rust/crates/stream/src/session/client.rs:261`: its public Transport supplies canonical SessionFrame I/O. `stream::session::serve` in `session/mod.rs:147` runs the actual server, with `serve_with_resources` for product-resource follows. WASM's `wasm_session.rs` wraps this same client. Fold actual received frames through `harness::client::ClientViews`; do not substitute the separate hypothesis kernel Client<D> or its Numbers benchmark. Keep endpoint setup in the conformance runner, using ACYCLIC_CONFORMANCE_ENDPOINT, TOKEN and NAMESPACE, with no memory fallback. These lines are at verify's b733 baseline; owners bind final signed definitions when landing A7.

## Shared cases and controls

| Obligation | Same oracle in memory/local and endpoint modes | Old or mutated production control |
|---|---|---|
| Deterministic reducer and resume | Seeded replay/live overlap, duplicates, reconnect, reset and epoch replacement; compare canonical ClientViews to an independent event-history specification. Applied cursor is monotonic, gapless and deduplicated, including filtered progress. | Drop overlap dedupe, accept a stale generation, advance cursor before application, or reorder conflicting frames. |
| Outbox settlement | Crash before/after send, acceptance, ack and persisted settlement; reopen and resend the identical key/digest. Exactly one durable effect, one application settlement, bounded pending custody. Unknown remains exact-request retry. | Drop unsent durable entry, use a new retry key, settle on transport send, or lose the accepted/unacked entry. |
| PageWindow | Exclusive before/after cursors, alternating loads, overlap, reset with in-flight response, empty pages, eviction and cancellation against ordered entries. For opaque A5 page cursors retain/evict complete pages; use per-entry edges only where provided. | Actual old history reset[b,c], after->[c,d], before->[a,c] fails expected[b,c]; stale cursor after partial eviction; admit stale response; concurrent opposing load mutation. |
| Demand bounds | Total 1k/10k/100k agents with fixed visible/focused demand and independently fixed discovery page/expanded-node budgets; assert interest, retained-byte and outbox limits separately. Nearby/prefetch demand cannot silently acquire full transcript follows. | Follow every discovered agent, hydrate all transcripts, or count queue frames but omit blocked writers/backing segments. |
| Server credit and authority | Actual session server: two independent channels, zero credit, per-channel and aggregate byte exhaustion, held backing payload, release, cancellation and reconnect. Never-requested authority is a typed refusal; retired-generation frames have their own fence behavior. | Bypass delivery admission, return credit before backing payload release, couple channel credit, or admit a frame from unrequested authority. |
| Frame work | At fixed visible demand, count actual frame visits, folds, payload copies and exported array copies as total agents grow. Unchanged JS items retain identity; changed work is proportional to visible/changed content. | Iterate all agents per frame or copy all retained items on every items getter. |
| WASM/TS boundary | Node executes real generated WASM, asserting exact cursor/BigInt values, typed error variants, cancellation and callback/handle lifetimes. Bun is a separate receipt. Native, browser and hosted each need their own applicable execution. | Truncate u64, stringify an AccessError, retain a disposed callback, or fall back to the duplicate TS reducer/window. |
| Composition | HarnessBuilder presets all select the canonical provider Registry/tools::defaults and same WireSession; unsupported composition refuses before effects. Host/plugin/TUI adapters share typed errors and bounded demand. | Construct a private core/registry, silently downgrade an unsupported composition, or catch-all map a typed access failure. |

## Required production hooks

Expose a small read-only conformance observation hook through existing test-support boundaries: current/peak followed interests, retained backing bytes including blocked producers and in-flight/stale responses, outbox bytes/entries, frame visits/copies, server outstanding credit/admissions and physical transport opens. Counters must be charged at actual acquisition/allocation and released at the real ownership boundary, including failed/cancelled admission. Hooks observe production state, not parallel model bookkeeping. Existing production Loom entrypoints in session/client/admissions.rs and pool/discovery_model.rs are the concurrency seam; migrate retired fixtures to current A3/A5 types rather than adding a second concurrency mechanism.

Provider fixture creation may precede measurement, but measure from client construction/open through demand, fold, cancellation and settlement; setup cannot hide client enumeration or residency. Record fixture cut, source pin and seed. Fixed corpora must select nonzero cases and finish under 10 s per suite. Longer seed/capacity exploration belongs to nightly; no fast-tier skip can qualify a mode.

## Receipt interpretation

cef1e415194397155569bee6b40d6b3dd48020c8 Node/Linux and same-artifact Node/macOS prove the existing in-process WASM smoke only. Actual old PageWindow and SessionQueue outputs are historical controls, not a positive A7 implementation receipt. Node, Bun, browser, hosted and native macOS are distinct. No positive A7 qualification is currently admitted. Frozen #1508 remains untouched.

Done for the owner handover: signed public suite entrypoint/provider adapter and production probe definitions, exact-source positive and mutation receipts with the same oracle, and an endpoint runner. Verify will own cross-provider oracle integration and admission; client-perf owns production A7 and its deletion of TS duplicates.

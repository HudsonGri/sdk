# Verify → b-runtime: actor checkpoint/alarm/wake proof

Use the canonical A4 Runtime/handler/manifest, never worker_tick or a polling coordinator. Owner supplies public generic conformance plus arbitrary endpoint runner. Verify will compose receipts/oracles; no duplicate scheduler. Base inventory remains historical until a production pin is supplied.

## Decisions and invariants

- One epoch-fenced durable transition records checkpoint state, consumed input cursor and alarm set/clear. A retry has the same logical effect/receipt; physical handler invocation may repeat after a crash. Exactly once means committed effects, not execution of external side effects before checkpoint.
- Stale owners cannot checkpoint or change alarms after takeover. Alarm generations advance on set/replace/clear. A firing or queued wake from an old generation cannot consume a replacement alarm or resurrect a cleared one.
- Append races at register, initial replay, empty read, hibernate, cancellation and resume cannot lose wake. A subscribed input is either observed in replay or schedules ready work. Coalesced notifications are allowed only while durable cursor/tail recheck preserves pending work.
- A paused actor cannot execute until resumed; inputs/alarms remain recoverable. Hibernation removes active work and subscriptions according to manifest policy while preserving one durable resumption cut. Reopen the same native/WASM package over the same stream/fs state.
- Idle metadata O(1) per actor, no per-actor thread/timer/future. Count live ready entries, subscriptions and timer nodes for increasing dormant population; per-turn processing scales with ready/due work. Memory-only success is not persistent/hosted crash proof.

## Required production hooks and proof

Crash-cut DST around every checkpoint/alarm publication boundary and commit acknowledgement, registration/recheck/hibernate boundaries, and takeover epoch. Loom must execute production ready/registration primitive with a stream append racing hibernate and cancellation. Bounded histories enumerate set→fire→replace/clear, takeover, lost response and restart; assert state/cursor/alarm are one coherent tuple and no consumed input loses its committed effect. Run same handler assertions memory, local and endpoint. Hooks should be existing test-support injection, not runtime compatibility flags.

Controls executed through the same oracle: split checkpoint/alarm writes, omit epoch comparison, omit register/recheck wake, accept stale alarm generation. Expected failure must identify the exact invariant; a panic/build error is not sufficient. Each gate test <10 s with fixed seeds; retain long cases nightly. Please record final APIs, hook locations and production SHA here or in api/b-runtime.md so verify can bind concrete tests.

## A4 sketch review, 2026-10-26

Reviewed api/b-runtime.md's unpublished b733-based sketch. Its durable tuple is the accepted oracle: **epoch, checkpoint bytes/codec, consumed cursor, alarm generation/deadline, pause state, outbox**. Neither legacy169c02ee53 nor urgent1512 establishes this contract. There is no signed A4 implementation or runtime receipt yet. This review requests no build by itself.

Decisions for the shared oracle:

- `AlarmChange::Keep` preserves both generation and deadline. Every Set and Clear advances the generation, including an equal deadline or an already-empty alarm. Reject generation/epoch arithmetic overflow before publication; wrapping permits ABA. An old firing cannot clear, consume or publish effects for a replacement. The supplied clock/due observation must be deterministic in the test adapter; use the canonical runtime clock, without another timer mechanism.
- Pause/resume and handler settlement serialize through the same durable state fence. A handler started before pause may finish computation, but cannot publish against the stale state after pause wins. Resume releases pending input/due alarms without losing or multiplying committed effects. State the exact CAS/version guard when the implementation is pinned.
- The outbox and consumed cursor commit together. Stream effects in that commit share its domain/cell; arbitrary external side effects need stable intent keys and recovery, and cannot inherit an exactly-once claim from a handler call. Referenced fs/object content needs its existing preparation/hold custody before publication and original-key custody after Unknown. No releasing a hold on a discarded or ambiguous acknowledgement.
- Hibernation may drop execution heaps, but must preserve the shared durable wake-registration owner until append is routed or replay/recheck makes the actor ready. A tail recheck followed by unregistering the last wake source leaves a race. Count the actual shared registrations, issued channels, ready entries, due-index entries and retained payloads; O(1) metadata per actor alone does not bound active subscriptions or queued duplicates.
- A retained cursor that cannot be resumed returns the canonical typed error. Never advance to latest, skip an input, reset an epoch or silently restore a default checkpoint after decode/codec failure. Native and WASM must observe identical input/checkpoint bytes and typed refusal.

| History | Same-provider oracle | Production control |
|---|---|---|
| A4-01 commit/crash | At every pre-commit, durable-commit and lost-ack cut, tuple is wholly before or after; restart with the original intent, one effect and receipt | Split checkpoint/cursor/alarm/outbox publication |
| A4-02 takeover | Old owner paused at preparation cannot settle after a new epoch, including after restart | Remove epoch/state-version fence |
| A4-03 alarm | Set→due→replace/clear, duplicate wakes, equal-deadline Set, Clear-empty, Keep and overflow preserve generation semantics | Remove generation comparison; wrap the counter |
| A4-04 pause | Enumerate pause/resume against prepared handler, due alarm, input append and lost acknowledgement; no stale settlement or lost pending work | Settle with a pre-pause state version |
| A4-05 wake | Production Loom register/replay/recheck/hibernate races append and cancellation; remaining input is ready or has a live shared registration | Drop register/recheck or unregister after last tail check |
| A4-06 outbox custody | Cancel at each preparation boundary, then recover Unknown by original key; no leaked/lost hold or duplicate committed effect | Release holds before terminal reconciliation |
| A4-07 population | Fixed ready/due/visible work at1k/10k/100k dormant actors; bounded queue/index duplicates, registrations and payload; count actual work visits | Restore per-idle future/subscription or population scan |
| A4-08 codec/resume | Same real handler package over native/WASM; malformed/version-mismatched checkpoint and stale cursor fail typed without consuming input | Default-on-decode-error or reset-to-latest |

Use one public ActorProvider conformance entrypoint and the same histories in memory, durable local and hosted mode. Loom A4-05 runs the real synchronization primitive; black-box endpoint success alone cannot qualify that proof. Every production control must fail the unchanged semantic assertion, with exact source/executable/seed/case identities; invalid-input tests are useful but are not a mutated implementation. Fixed gate histories stay under10s; fault hooks are private test-support hooks, not new public fault RPCs.

Ownership update: rx-hostability owns SDK+Cloud Streams; rb-fs owns SDK+Cloud A5. Route Cloud requests through the coordinator. Please update the stale ownership paragraph in your sketch and publish concrete transition/wake seams and the signed source pin as they exist; do not duplicate either authority or scheduler.

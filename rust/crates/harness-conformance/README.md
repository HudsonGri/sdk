# Harness scenario substrates

From the SDK root:

```sh
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- --substrate in-process --report out.json
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- --substrate localhost --report out.json
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- --substrate http://127.0.0.1:8080 --filter core/ --report out.json
```

Build on bb2. The runtime budget is measured separately from Cargo compilation.
`--corpus DIR` selects an exported copy of `harness/tests/scenarios`. Defaults
are merged by the existing corpus loader; no case JSON needs changing.
`--substrate local` runs the same portable executor and composer fixtures
directly, providing a baseline for the Actors adapter. The existing
`cargo test -p acyclic-harness --test main` path retains its native driver
selection and scenario inputs.

`--substrate localhost` starts the SDK's single-node server and deploys the same
pinned Actors package over the canonical Actors API. Its Streams journal uses
`LocalStream`; immutable payloads and observations use the canonical fs API.
The server's directory stays owned by every provider clone through shutdown.
It needs no Cloud code or credentials. The stock harness deployment and shipping
GraphCoder client are described in [local-server](../local-server/README.md).

The deployed-parity integration target uses the same discovered corpus and
oracles for both hosts, including complete report equality and a deliberately
changed deployed answer that must fail:

```sh
cargo test --locked -p acyclic-harness-conformance --all-features --test localhost
```

It also compares cancellation, stale-placement and interrupted-recovery event
histories at full account/namespace/actor addresses. The shipping GraphCoder
endpoint test exercises the stock `HarnessCode` package independently of the
scenario fixture package.

The report has a sorted `records` array with one `case`, `status`, `reason` and
`requires` entry per discovered JSON case, plus pass/fail/skip counts. Defaults
files are configuration, not cases. Filtering produces explicit skips. No wall
clock timestamps or random runtime identifiers appear in passing reports. Exit
status is zero only with at least one pass and no failures; a filter matching
nothing cannot qualify a host.
Every known driver's shared schema is validated before capability or filter
exclusions. A malformed native case fails even when its capability is missing.

At the initial corpus revision, the 847 JSON files comprise 60 inherited-default
files and 787 executable cases, matching the existing discovery rule. The
reference qualifies 436 executor/composer cases and explicitly skips 351
native-fixture cases with the capabilities below. Defaults apply to their descendant
cases; they are not executable scenarios.

## External hosts

Implement #1359's `ActorRuntime` and this crate's `substrate::Volume`, then use
`substrate::Actors` with the host's explicit bindings. Install the identical
`HarnessPackage`. Its pinned deterministic handler emits a
`harness.scenario.compute` command; bind that command to `FixtureEffects` with
an explicit `Providers` bundle (or an equivalent executor using
`collect_on`). The effect runs the stock executor with a scripted model or the
public command catalog APIs, using fixture-owned files. The package checkpoints
the resulting immutable fs reference. The runner supplies fixture inputs and
file paths to inspect, never expected answers. Assertions run on the client.
The package digest pins shared parsing, defaults, discovery and observation
code as well as the handler and composer fixture implementation.

The runner uses only public Actors admission/recovery and fs publications. The
runtime's inbox, checkpoint and outbox use Streams as specified in #1359. The
reference host uses `InProcessRuntime` and disk-backed `LocalStream`; memory
Streams cannot authorize durable outbox dispatch. Fixture fs durability lasts
for its provider lifetime. Results reconcile by stable command identity.
Executor journals use that same host Streams provider; executor files use the
supplied fs providers.
Fixture inputs and observations share one immutable-publication adapter over the
canonical filesystem wire service. In-process dispatch calls that same service
directly; deployed dispatch uses its generated client. Each publication has its
own workspace, and each executor still has its own mutable storage. The
reciprocal adapter test reads each connection's writes through the other and
rejects forged descriptors, providers, owners and volume classes through both.
Hosts must not substitute fixture-private memory stores for the providers being
qualified.
This package qualifies operation-level executor hosting; it does not migrate
GraphCoder's native per-step scheduler to Actors or qualify process-loss fs
durability, live model HTTPS, production compute or workload authentication.

An endpoint URL attaches through the same public Actors and filesystem clients
as `--substrate localhost`. It must be a numeric loopback endpoint serving the
SDK services with the pinned conformance package registered. There is no
conformance-specific HTTP protocol or gateway. Deployment selects that compiled
package by digest. A host deploys it once in its namespace; each case receives
a distinct actor ID and private mutable execution storage. The parity history
test separately exercises equal actor IDs in different accounts and namespaces.

## Capabilities

`executor` means turn/summary execution, scripted model recovery/replay,
compaction, context discovery, fs staging/reading and complete journal
observations. Both direct and Actors substrates declare it. Host adapters must
implement all these operations to declare `executor`; an unsupported operation
is a failure, never a silent skip.

`composer` means catalog capture/resolution, completion, pinned mention attachments,
configuration publication and immutable fs reads through the supplied providers.
Both substrates declare it; all 177 composer cases run through the package.

The resolved recipe driver tags cases requiring native fixture introspection as
`local-driver:<driver>`, where `<driver>` is `local`, `goal-local`, `agents`,
`composer_entry`, `notifications`, `task_query`, `subagents`,
`swarm`, `plans` or `plan-query`. They inspect native scheduler state, UI
entrypoints, process fixtures, fixture-owned projections, native builder/run_prompt
fixtures or instrumented provider counters outside the #1359 package adapter,
and are explicitly skipped by this package. Each report reason identifies the
concrete fixture dependency. These capabilities identify native fixture
dependencies; declaring `executor` does not
imply them. The report is the authoritative case and skip count for a corpus
revision. Unknown drivers fail instead of acquiring an automatic capability.

`tests/scenarios.rs` compares every portable case with the original direct
driver, compares direct/Actors reports, exercises SDK endpoint attachment and proves
that dropping a contract completion observation fails qualification.

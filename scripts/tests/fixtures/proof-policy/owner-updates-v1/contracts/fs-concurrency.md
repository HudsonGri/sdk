# Filesystem wave 1 A5 — shared API sketch

Owner: fs-concurrency; implementation and deletions stay in SDK PR #1492. This
extends M1 rather than creating another publication mechanism. Read against
REDESIGN.md and the filesystem audit, 2026-10-26. Cloud review received and read
2026-10-26; numbered resolutions below amend this sketch. They are design
decisions, not implementation or hosted qualification.

## Types and entry points

The names below are proposed contracts, not claims of implemented APIs.

```rust
enum FileCondition { Generation(GenerationId), Digest(Digest), Absent }
// Reuse StagedContent: immutable content root plus logical length.
struct ChangeCursor { /* domain cut, query/auth binding, volume cursors */ }
struct SnapshotFollow<A, O> {
    cut: DomainCut,
    snapshot: Vec<Generation<A, O>>,
    cursor: ChangeCursor,
    // Bounded, resumable follow owned by the same provider.
}
struct SnapshotQuery {
    volumes: Vec<WorkspaceId>, // one account-tree domain
    prefix: CanonicalPath,
    glob: CanonicalGlob,
}
enum PageDirection { Forward, Backward }
struct SnapshotPage {
    entries: OwnedEntries,
    before: OpaquePageCursor,
    after: OpaquePageCursor,
    // Original page/accounting/cut custody; eviction is by complete page.
}

// Public view API; mounts forward into this implementation.
async fn write_if(path, bytes, condition, operation, work, cancellation);
async fn remove_if(path, condition, operation, work, cancellation);
// On Transaction, shared with write_staged; publication uses its existing key.
async fn append_staged(path, segments: &[StagedContent], work, cancellation)
    -> ByteRange;
async fn snapshot_and_follow(query, resume, work, cancellation) -> SnapshotFollow;

struct ForkSpec { source: GenerationForkSource, destination: WorkspaceId /* + selection */ }
async fn prepare_fork_many(forks: &[ForkSpec], operation, work, cancellation)
    -> PreparedForkMany;
// Consumes a preparation into the caller-owned Streams request and its custody.
fn contribute(self: PreparedForkMany, commit: &mut PreparedFilesystemCommit);
// The sole publishing route seals caller + filesystem facts together.
async fn commit(self: PreparedFilesystemCommit, work, cancellation)
    -> FilesystemCommitOutcome;

struct StreamCoreState<P: StreamProvider> { /* shared provider + namespace */ }
pub async fn conformance<P: FilesystemProvider + ?Sized>(provider: &P, namespace, work)
    -> Result<ConformanceReport, ConformanceError>;
```

Generation conditions pin the transaction's exact base and forbid transparent
rebase across a changed head. Digest conditions record an authenticated path
dependency in the existing mutation/footprint machinery; no host-file hashing
or write-then-capture. Absence is explicit, distinct from an empty file. Both
write and remove use the same condition evaluator and publication fences.
Tools receive filesystem capabilities, never raw host file handles.

Append segments extend immutable file extents through the existing authored
transaction and authority publication, returning their logical append range.
Physical chunk/extent boundaries may compact without changing that range. They
are files, not a second task/event database. Snapshot-and-follow captures the exact
generation set and its domain publication, returns that consistent query cut,
then replays strictly after the cursor. Follow registers before reading or
replays from a retained cursor, so an intervening commit cannot be lost. A stale
retention cursor is a typed refusal, never silently replaced by current head.

fork_many prepares immutable roots, source holds and authorization before one
Streams CommitRequest publishes all destination authority records. All or none
become visible; successful request identities replay exactly. The authority's
fork record is the only lineage/origin fact, including lazy origin and selection.
No side lineage CAS or Prepared/Committed lazy-workspace sequence remains.
Streams' subtree-fork primitive composes in this same transaction when needed;
the filesystem must not enumerate/copy inherited conversation descendants.

StreamCoreState stores branch refs, lazy overlay state, journal checkpoints,
contexts and multi-root decisions as typed records over the same StreamProvider.
Expected revisions become stream conditions and related updates share a commit.
Synchronous local-WAL store traits/callers are replaced, not wrapped by blocking
an async runtime. Native materialization effects stay native adapters; their
durable intent/result records use this shared stream journal.

## Invariants and cost

One volume authority actor owns head/epoch and bounded change index. Each client
proposal gets one request/reply, overlapping work is held/rebased inside it,
disjoint work shares one append; successful receipts keep per-operation identity.
No read-before-write head probe. Pipelined batches acknowledge in durability order.
Every new queue, segment list, query page and retained cursor uses existing work
budgets and original native custody. Bounds fail with step + exact recovery.
Footprint checks cost O(changes since base); disjoint rebase never reads content.
Fork metadata is O(number of destinations + path/selection/index work), independent
of inherited payload bytes; cross-authority Streams work is charged per participant,
not advertised as O(1). Query cost includes visited indexes and entries, including
nonmatching glob entries, authorization CPU and returned bytes. Follow charges
delivered changes and retained replay state, with bounded provider credit.

## Deletions and conformance

Delete the standalone memory authority implementation (not memory object storage)
and any IndexedDB authority implementation. Browser already opens
StreamAuthorityStore<BrowserStream>; preserve and qualify this sole route.
Delete core_state's local WAL, ownership/retry loop and duplicate record caches;
delete side lineage store and lazy fork two-phase records after all callers move.
No compatibility or migration shim. Do not rebuild removed TaskVolume/ForkSeed.

The public generic suite is the sole oracle for memory, durable local, browser and
hosted. Runner reads ACYCLIC_CONFORMANCE_ENDPOINT and authentication from env;
never logs credentials, never falls back to memory after endpoint failure.
Cases cover conditional write/remove races, wrong generation/digest/absence,
append order and exact retry, consistent query cut plus gapless resume, atomic
multi-fork crash/retry and source retirement, and typed core-state atomic CAS.
Production-code property/DST and loom models cover concurrent publication,
follow registration and cancellation, with real old-behavior negative controls.
Each suite remains under 10 seconds. Hosted status remains unqualified until the
same suite runs against the endpoint; local evidence is not substituted.

## Decisions

Keep all A5 operations in the existing publication/StreamAuthorityStore path.
Use Streams commit composition, not a parallel filesystem transaction server.
Reject per-directory version-stream fallback: extra streams and server ordering
would break previous_digest. No new wire field is decided in this sketch; any
wire decision must be numbered in fs-perf/cloud-needs.md with full SDK SHA,
field numbers, semantics and conformance case before Cloud consumption.
Implement generation conditions first, then digest/append/query/fork/control-state
deletions; continue M1 and benchmark work alongside these APIs. GC encoding still
requires the user's requested review before implementation.

## Cloud review resolutions (2026-10-26)

1. **One retained domain cut.** All participating agent volumes and companion
   conversation/inbox streams must belong to one explicit account-tree consistency
   domain. `DomainCut` is an owned retained immutable namespace-root version from
   Streams, with account/domain identity and publication identity. Snapshot resolves
   every head from that root. Cross-cell or independent-domain composition returns
   `UnsupportedDomainComposition` before effects; sequential head reads are never a
   substitute. This depends on the canonical Streams namespace-root API; no second
   filesystem root index will be added. The current single-generation sketch above
   must become a generation set bound to this cut for multi-volume queries.

2. **Caller commit composition.** Replace the standalone submitting fork signature
   with preparation/contribution above. `PreparedForkMany` owns canonical fork
   facts, source-generation/segment closure holds, permission observations, source
   and destination conditions, semantic operation identity and request digest.
   `PreparedFilesystemCommit` owns those values together with the caller's normal
   `CommitRequest` mutations (including inbox/ForkAdopted) and original accounting.
   Contribution does no durable publication and adds no service transaction.
   Sealing validates domain and destination uniqueness, then uses one existing
   Streams commit. Outcome is `Committed(receipt)`, `Rejected(preparation, conflict)`
   or `Unknown(recovery)`; the last owns the original key/digest and all holds until
   that exact request is reconciled. Lost-ack/crash tests include a companion
   non-filesystem record and assert all-or-none visibility and identity replay.

3. **Condition semantics.** `Generation` binds volume identity, exact authority
   incarnation/head and base root. Any intervening publication, even unrelated
   data or equal-byte rewrite, forbids transparent rebase. Delete/recreate cannot
   reuse that incarnation. `Digest` deliberately compares logical regular-file
   bytes only, not metadata or file identity: equal-content ABA is allowed at the
   point of admission. An admitted transaction nevertheless retains authenticated
   path/identity dependencies and current permission fences; a later delete/recreate
   cannot evade them. `Absent` is a distinct negative name dependency, never an empty
   file or a zero tail. Qualify stale generation, equal-byte replacement and
   unrelated mutation separately; preserve original failure work/recovery identity.

4. **Holds across cancellation.** A preparation's custody moves into its accepted
   caller operation before any publication may escape. Request cancellation drops
   observation/wait interest, not ownership of an unknown durable decision.
   Reconciliation and generation-fenced release retain the entire immutable closure
   across source retirement/compaction/restore. No per-descendant pin loop is added.
   New body publication and eventual reclamation are separately accounted. This
   does not approve or implement the user-gated GC encoding in gc-design.md.

5. **Query authorization and bounds.** Prefixes are canonical path segments; globs
   are canonical bounded patterns. Cursor identity includes account/domain, cut,
   canonical query, volume identities, position and authorization context. Query
   admission requires covering authority; per-volume/path checks still apply and
   denied names or metadata never leak. Insufficient scope is a typed error, not
   an empty result. Existing work bounds cover pattern complexity/bytes, visited
   pages/entries (including misses), volumes, result bytes and retained cursors.

6. **Cursor ownership and resume.** A cursor is tied to a retained cut/lease with
   bounded page/credit state, not a bare sequence granting authority. Every resume
   checks current authorization and explicit expiry/replacement; revocation ends
   delivery. Follow starts exclusively after the captured publication. Reconnect
   can replay unacknowledged frames with the same cursor, so applications deduplicate
   by cursor. The guarantee is no gap between snapshot and follow, not exactly-once
   socket delivery. Retained cut/history storage is measured and budgeted.
   For A7, choose whole-page ownership: entries and both opaque boundary cursors
   are one owned page. Backfill requests select direction and a boundary from that
   same cut/query. Clients never construct cursors or evict half a page. Canonical
   prefix traversal prunes unrelated subtrees before bounded glob matching; the
   `approvals/*/request.json` case records visited nodes to reject a whole-tree scan.

7. **Count durable phases.** One client request/reply is not one durable phase.
   Metadata-only already-retained roots use the existing Streams path; new bytes
   require Objects publication/retention first. S authorities cost O(S) coordination.
   Batch admission bounds bytes, age and work, preserves each identity, and fences
   stale epochs. Generation-conditioned proposals cannot gain a new base merely
   to enter a batch. Bench receipts include actual paths/pages/bytes/participants,
   stream appends and client round trips, separately from staged body work.

8. **Executable conformance handoff.** The current sketch has no implemented public
   A5 suite/runner or releasable SHA yet; this is an explicit gap. The sole suite
   must exercise the canonical provider operations through thin embedded/hosted
   transport adapters, not require hosted credentials to access private Streams.
   Runner input includes endpoint, bearer credential and isolated namespace from
   env. The receipt identifies full SDK SHA, exact executable hash, provider,
   namespace, nonzero selected case identities, commands, counts and timings;
   credentials are never emitted. A numbered SHA-pinned handoff will name the
   actual public symbol/example once compiled and proved. Cloud alone supplies
   its authenticated fixture/run; no local memory pass is a hosted qualification.

These decisions resolve the sketch's ambiguity, not the review's implementation
requirements. Every numbered item remains unqualified until its production API
and named histories pass. No wire fields are allocated by this document.

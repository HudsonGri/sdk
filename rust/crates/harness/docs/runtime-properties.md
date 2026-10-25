# Runtime contracts

Task, workflow, execution and conversation journals own admission and recovery.
Signed owner scopes and current lease fences govern fresh dispatch and publication.
Historical read custody does not authorize writes. Cancellation retains uncertain
effects and their reservations until reconciliation.

The `native-execution` adapter runs explicitly approved processes through these
journals. Approval covers executable path, arguments, environment, stdin, working
directory, allowances and volume manifest. The host maintains executable integrity
and supplies credentials and publication permits separately.

Recovery verifies retained request and result artifacts and receipt identity.
Failed or uncertain publication cannot become a successful task completion.

Filesystem journals, interactions, readers and publishers retain immutable
authenticated scope evidence. Creating a dependent adapter copies that evidence
through the same admitted constructor rather than verifying its signatures again.
The copy admits each string and vector before allocation; a partial copy retires
its own custody. Every authorization still checks current trust and expiry, and
the existing exact-volume/file checks remain the authorization boundary.
`journal_and_content_adapters_reuse_one_authenticated_owner` covers construction
and revocation; `copied_trust_evidence_admits_each_exact_buffer_before_allocation`
compares admission with the counting allocator at every failure cut. Constructor
custody coverage exercises both owned and native ledgers for all adapter forms.

`WorkspaceGraph::fork` requires the exact ready parent registration before
creating a child intent. The graph issues one creation input containing its
authenticated generation, destination and operation identity. Authored and sparse
creators consume these bindings directly; they do not carry independent copies.
Issued publication guards and original accounting remain inside that creation
phase. Recovery authenticates the original creation before promoting readiness.
Root registration also requires that accounting context and returns owned
metadata; neither first registration nor reopen creates an unlimited ledger.
Rejected in-memory index insertions retire both copied metadata and node custody.
Sparse fork intent and readiness each have one CAS transition. A losing writer
authenticates the durable winner rather than republishing a newer pending record.

Publication ownership is read through the filesystem's indexed parent slot.
`multi_root::context_has_publication` treats a claimed operation without a journal
as owned. Discard must preserve that participant. `parent_publication` validates
the journal against both the requested parent and the claimed operation; it does
not enumerate unrelated publications. These reads do not themselves serialize
discard against a concurrent claim from another host.

Ordinary and native join-side selection use the same metered exact-set validator.
Length and membership alone cannot authorize duplicate conflict regions. The
shared sorter and final ordered walk fund their work on the original invocation.

Source-deletion eligibility belongs to `LazyWorkspace`. Only `NotFound` establishes
absence; admission failures and storage errors propagate. Eligibility follows the
live source epoch and preserves authored parent changes. The merge coordinator
must still check changed ancestors and hold its publication fence; eligibility is
not permission to unlink a physical path after a concurrent source replacement.

`ProcessTreeStorage` owns the exact originally funded process slot on every OS.
Callers cannot extract or replace its tree. Spawn failures retain partial work;
capture and termination use the same invocation meter. Physical cleanup failure
does not acknowledge quiescence. Integrating this owner with an authenticated
child-turn claim and admitted task view remains required before host completion
or recovery may treat it as a claim-specific quiescence receipt.

`OwnershipReleaseBarrier` observes physical owner cleanup and every retained fence,
not the last shared reference decrement. The final field of the shared owner
publishes completion only after those destructors return. Blocking and observing
callers use the same completion condition on every OS. Standalone shutdown and
filesystem reopen tests wait on that boundary rather than polling reference counts.
Async callers register for its shared notification before checking completion;
they do not hold a blocking worker while waiting. Cancelling a wait cannot retain
the physical owner or consume another waiter's completion notification.

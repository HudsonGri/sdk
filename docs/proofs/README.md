# Wave-1 proof inventory

`wave1-inventory.json` records the active redesign obligations separately from the historical whole-stack source audit at `Q:/lanes/verify/inventory.json`. It is a requirement/evidence inventory, not a declaration that the stack is proved correct. A negative control that detects the old implementation is distinct from a passing proof of the new implementation.

The first implemented suite is `acyclic_stream::conformance::wave1`. Its public `verify` and `verify_case` functions are generic over the production `StreamProvider` trait. Memory, local and hosted callers execute the same assertions. A fresh namespace is mandatory: deterministic keys and retained paths make a second invocation in the same namespace a setup failure.

Run memory and local histories on bb2:

```sh
build-slot --remote cargo test -p acyclic-stream --features local --test wave1_keys -- --nocapture --test-threads=1
```

For an isolated HTTP endpoint, provide `ACYCLIC_CONFORMANCE_ENDPOINT`, `ACYCLIC_CONFORMANCE_TOKEN` and `ACYCLIC_CONFORMANCE_NAMESPACE` in the runner's environment, then run:

```sh
build-slot --remote cargo run -p acyclic-stream --features http --example wave1-conformance
```

Optional `ACYCLIC_CONFORMANCE_CASE` selects one exact case name: `append_retry`, `commit_retry`, `absent_zero_tail`, `subtree_fork` or `composed_fork`. An unknown selector fails rather than running zero cases. Each selected history has a nine-second outer deadline. Errors identify case, step, typed provider cause or violated invariant, and recovery command; tokens are never printed. The runner has no memory fallback, public fault-injection endpoint or Cloud service configuration changes. Secrets must be supplied through the build environment's existing credential transport rather than command-line literals.

These fixed histories cover observable conflict-key reuse, stable successful replay/inspection, changed-intent mismatch, record sequence/content and zero-tail absence. Discarding an acknowledgement is a simulated lost result, not a process-crash test. `inspect == None` is asserted only after a definitive conflict; it does not establish that a pending or unknown request has no effect. Lifetime deduplication, real crash/restart, publication concurrency, subtree retention/authorization, WASM boundary and hosted behavior need their own receipts. Do not credit any of those from this suite alone.

The subtree histories also check deep membership, path-segment sibling boundaries, private source/destination writes and pre-commit descendant heads when root/branch appends compose with a fork. They do not establish exclusion, authorization, publication concurrency, resource cost or GC safety.

The new tests fail on b733 production for the intended invariants. Their actual bb2 failures are negative controls; they will remain failing regressions until the owning Streams implementation lands. No verify PR should enter the gate before positive implementations and the remaining wave-1 proofs are integrated. Preserve one PR for the lane; never modify a queued head.

Check the manifest, source references and captured negative/Clippy evidence with `python scripts/check-wave1-inventory.py`. Its pass reports artifact integrity only. `--require-qualified` fails on the open obligations; positive receipt validation and the full public-entrypoint/unsafe/mapper CI policy remain a later milestone. Evidence files are captured real output/source, with provenance and hashes in the manifest. The Clippy JSONL retains exact source-identity, diagnostic and completion lines from the full external log. Four baseline Streams warnings remain; zero new-proof diagnostics is not a strict crate cleanliness pass.

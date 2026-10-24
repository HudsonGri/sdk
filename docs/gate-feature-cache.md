# Gate feature-matrix artifacts

`gate-feature-matrix.sh` runs the reviewed matrix on every invocation. Inside
a proven private buildbox mount, `gate-target-cache.py` seeds the worker targets
from a completed immutable matrix snapshot, then executes the same Node/Cargo
commands. The matrix footer and exit status are newly computed; no verdict is
reused. Mutable targets remain job-owned and are removed by the runtime.

Every worker target is inside the matrix root (`worker-0`,
`worker-0-features-1`, and so on). The existing target store owns the exact
source/argv/environment/toolchain/target key and completed publication. This
adapter does not change those keys, compiler flags or package/feature sets.
Changed source or toolchain requires a new request. Cargo still validates all
fingerprints in a restored target.

The adapter acquires the production hierarchical target lease only while
cloning or publishing. It releases that parent lease before any Cargo worker
starts, so descendant workers can acquire their own leases and siblings can
compile independently. The existing matrix flock covers the complete run;
the job's runtime lease keeps its target and temporary directories alive.
Only successful matrices with unchanged uploaded inputs publish. Failed,
interrupted or source-mutating runs retain their actual nonzero status and
cannot publish a new completed snapshot. Snapshot failures log diagnostics
and execute the checks without a seed. Receipt persistence failures are typed
runner-INFRA. Snapshot diagnostics go to stderr, preserving child stdout.

Canonical PR test/clippy target preparation uses the same receipt plus actual
mount proof and `CARGO_INCREMENTAL=0`. It creates empty targets without a
legacy clean or boot/inflight markers. Outside that proof it retains the
existing boot/interrupted-build protection. `BB_JOB_ACTIVE` alone is never
accepted as proof.

Runner preparation also preserves uploaded source metadata. An already
executable runner is used unchanged; a non-executable uploaded runner is copied
atomically to job temporary storage and made executable there. An unconditional
`chmod +x` changes source ctime even if its mode already includes execute bits,
which correctly makes the source-clock oracle reject subsequent reuse. The
fix preserves the oracle instead of ignoring ctime changes.

Controls run on bb2:

```sh
python3 scripts/gate-target-cache.test.py
python3 scripts/buildbox/cargo_cache.test.py
node --test --test-concurrency=1 scripts/pr-gate-checks.test.mjs scripts/main-gate-checks.test.mjs
```

These include real private-mount proof, flag-only/ordinary-directory negatives,
empty production PR preparation versus the old marker/clean path, actual XFS
reflink publication and clone across worker targets, execution after restore,
nonzero/signal and source-mutation refusal, full-key changes, and hierarchical
parent/child/sibling exclusion. Real gate measurements belong in the PR report;
fixture timings do not establish fleet savings or an eight-second full matrix.

# bb2 tool installation

All lanes and the efficiency loop must install bb2 tools through:

```
sudo /home/var/bin/bb-install.py --source /mnt/bb/<candidate>/scripts/buildbox
# prints /mnt/bb/tmp/bb-install-<qualified-generation>
# after acquiring the idle PR mkdir lock:
sudo python3 /mnt/bb/tmp/bb-install-<qualified-generation>/bb-install.py --publish /mnt/bb/tmp/bb-install-<qualified-generation>
```

For a change confined to selected tools, add `--only bb_target_store.py
bb-install.py` to publication. The complete frozen candidate still qualifies;
only the named tools are replaced, and the manifest records the actual live
hashes of all tools. Qualification records installed predecessors. Publication
refuses a selected destination that changed afterward, preserving another
lane's newer deployment. Unselected tools are preserved, even if they advanced
during qualification. The Cargo source-namespace controls are mandatory in the
installer's qualification suite.

The installer freezes a root-owned candidate, runs the receipt, runtime,
admission and publication controls as `var`, and publishes only after they
pass. Publication writes a temporary file beside the destination, flushes it,
renames it and flushes the directory. Failed qualification changes no tool.
`installed-tools.json` records the qualified hashes. Bin directories and tools
are root-owned and unwritable by the job/lane account; direct writes fail.
The canonical store is `/opt/buildbox/bin`, below protected root parents.
`home-var-bin.mount` pins the legacy `/home/var/bin` entry to that store, so a
lane cannot rename it through its writable HOME parent. The entry is read-only;
publication writes through the protected canonical store. The mount is enabled
at boot. The installer verifies that the entry and store have the same inode.
Do not use sudo to bypass this boundary. The installer is the only privileged
publication path. Existing jobs keep their running interpreter/inode.

Take the coordinator's idle `Q:/lanes/pr-gate/.lock` mkdir lock for the final
publication, releasing it immediately afterward. Qualification can happen
before this lock is acquired. Never interrupt a running gate. Daemon upgrades
must drain existing leases before stopping the idle daemon; replacing its file
does not replace already loaded code.

The gate drain owns a separate SQLite lifetime lease through `gate-queue.py
STATE dispatch DRAIN_SCRIPT`. Queue producers use the short queue transaction
and do not wait for a build. Each prepared run exclusively creates its own pin;
`gate-merge.py record` and `merge` both require that pathname with `--pin`.
Never reconstruct a pass from the mutable shared checkout or another run's pin.
A fence mismatch keeps its candidates queued and invokes the single checked
preparation repair while holding the gate lock. A retained infrastructure
record is diagnostic, not permission to refuse unrelated healthy work.

The current tool list is the SDK-owned runtime set. Another tool must be added
to the installer's explicit list with its required checks before installation;
an unreviewed filename cannot silently become a privileged tool.

Cargo's read-only metadata/tree/locate-project/pkgid/verify-project/read-manifest
commands bypass build tickets centrally in bb-admit. They retain the same
receipt and job HOME boundary. Callers need no tenant-specific copy of this
policy. Wrapper executables (including script interpreters) are resolved at
admission; an unavailable wrapper returns typed admission-INFRA (224).

Admission records every caller resource limit (both soft and hard) before
crossing the daemon boundary. The scope launcher restores that exact set as
root before dropping to `var`, without a PAM session resetting it again. The
HOME namespace repeats the restoration before its final privilege drop.
Public shell entries capture before starting the Node receipt wrapper: libuv
otherwise raises soft nofile to hard nofile before admission observes it. A
nested entry captures its own current limits, preserving deliberate changes.
There is no fixed nofile limit. Terminal runtime receipts record both the
requested and execution limits; mismatches or malformed limits fail as INFRA.

Cargo storage admission precedes every cache bypass, including cold builds.
Cargo's own metadata resolves the effective target and intermediate build
directories from CLI overrides, environment and configuration. Both must be
on the data filesystem and inside the current job or its receipt-proven private
target mount. The launcher and Cargo proxy share the same filesystem check.
The cache consumes this admitted target; it has no second path selector.

Use expanded Cargo commands instead of aliases, whose expansion can hide path
overrides. Installation builds must name a local `--path` producer so Cargo can
discover that producer's configuration; registry/git installation requests must
first supply their local source. `install --list` remains a read-only query.
The installer's mandatory proxy suite exercises root paths, escaping symlinks,
other jobs' paths, config discovery and the former uncached-launch bypass.

The shared cache policy lives only in `admission_policy.py`: 50G under
`/mnt/bb/sccache`. `bb-job.py`, native admission and `sccache-rustc` use the
same `bb-cache.py` authority. A caller disagreement, absent authority or unknown
server policy is CACHE_POLICY infrastructure refusal before compiler work;
there is no uncached fallback. The authority validates the actual Unix peer,
executable, boot/birth identity and launch environment on every admission.
The socket parent becomes root-owned before READY, preventing lazy restarts.

Cache activation is a coordinated host migration, after the signed candidate's
full gate proof. Advance the generated WSL `buildbox.mjs` first: removing its
override is compatible with the existing job-owned launcher. Then publish the
complete policy/authority/client generation through the qualified installer.
It holds admission exclusively, drains connected clients (including queued
tickets) and adopted leases, retires the idle daemon and birth-verified legacy
servers, publishes all consumers, and starts the host service before releasing
admission. Partial cache-generation publication refuses before replacing files.
The compiler wrapper cannot switch while an old job owns its private server.
The same migration is available for later host recovery:

```
sudo -n /usr/bin/python3 -B /home/var/bin/bb-cache.py recover
```

This owner command holds the admission upgrade lock, waits for admitted jobs,
drains legacy servers using pidfds and verified Unix peers, and starts the
root-supervised authority through systemd in the host PID namespace. Private
namespace servers end with their jobs. No cache entries are deleted. The
systemd READY notification follows actual peer/policy validation. Jobs never
stop the host server. A noncanonical legacy endpoint refuses migration rather
than guessing a PID. A private job namespace cannot run the host owner command.

Shared server CPU is explicitly unallocated in each job receipt. Only waited
child CPU is attributed there; cumulative shared-server statistics are never
charged repeatedly to concurrent clients. Under 15% free, admission prints
bounded retained quota observations with their age/source before applying the
unchanged 10% safety floor. Those observations are not current physical usage.

CPU admission uses the existing `bb-admit.py` priority/fair-share authority and
one `CPU_LEASE_SHAPE = (8, 16)` in `admission_policy.py`. No separate scheduler
or budget-test reserve exists. The 128 job CPUs are spread over CPUs 0–191;
the complementary 64 CPUs serve the one host cache. Every admitted command,
including generated Node preparation, is launched inside its root-owned lease
ceiling (`cpuset.cpus` plus `cpu.max`). Only its bounded work subtree is
delegated. Nested commands reuse the kernel-verified lease and tenant. An
environment marker cannot substitute for the protected record and cgroup.

Both services must set `BUILD_SLOT_TENANT=graphcoder` (SDK) or
`BUILD_SLOT_TENANT=cloud` (Cloud) explicitly. Missing labels refuse before heavy
work, with the original argv in an executable recovery command. Generated WSL
and native callers switch together at the coordinator-announced cut-over;
do not install CPU admission before that coordination and full gate proof.
The same exclusive/draining host migration publishes this complete generation.

Dispatch uses a root-peer/birth-verified handshake: the constructor cannot
start the payload before admission durably journals the ticket. A crash before
handoff starts no work; a crash afterward retains the lease through root
custody and pidfd recovery. A slot becomes reusable only after its exact
subtree is empty. Bounded terminal records resolve exit/retirement races.
Receipt sidecars retain lease identity, queue time, execution/end timestamps,
and exclusive subtree CPU/pressure counters. Nested totals overlap their
parent lease and must never be added to it. Shared cache CPU stays unallocated.

The installed sccache 0.18.0 supports its own client-side execution mode:
the wrapper's canonical policy enables `SCCACHE_CLIENT_SIDE=1` and an empty
configuration (`SCCACHE_CONF=/dev/null`). It refuses overrides, error logging
and distributed configuration that can disable this containment. Hashing and
rustc therefore remain in the caller's lease; the host daemon handles storage.
See the [versioned upstream architecture](https://github.com/mozilla/sccache/blob/v0.18.0/docs/Architecture.md).
The actual cold-compiler negative runs server-side mode and observes codegen
outside the job lease, while the admitted mode observes it inside that lease.

The initial shape follows the Cloud observation window
2026-10-25 17:15–19:15Z (292 terminal receipts, 13 active jobs). That evidence
has broad unbounded job scopes and missing/mislabelled tenant custody; it does
not prove whole-host CPU saturation or additive per-merge cost. The initial
8×16 shape is a bounded starting policy, not a measured throughput optimum.
Tune the single shape from joined queue waits, per-merge wall time and CPU
counters after the coordinated cut-over; do not claim realized savings before
those measurements exist.

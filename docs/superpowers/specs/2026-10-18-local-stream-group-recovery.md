# Local Stream recovery boundaries (#245)

Baseline: upstream main `66ddfd542be590eca8f5159b1c29cf17ccee7c0c`.

The V1 journal writes individually checksummed commands before a shared flush.
Before that flush completes, a device may retain a later frame while losing an
earlier frame. V1's intact-frame suffix scan then mistakes an unacknowledged group
for corruption. Serializing every command behind the previous flush fixes that
ambiguity at the cost of group commit; that approach was rejected in #552.

## Design

V2 retains bounded command frames and appends a fixed 78-byte seal before each
flush. Its checksummed payload identifies the epoch, the group's starting byte,
the seal's own byte offset, and cumulative command count. Command count and
timestamps keep their existing meaning. Seals do not consume command capacity.
The maximum command and replay allocation stay unchanged; a group can contain
multiple maximum-sized commands without needing extra flushes.

A device gate serializes seal+flush and snapshot installation/restart. Raw
command appends do not take that gate: writers continue to accumulate the next
group while the preceding flush runs. A nonempty group starts at the end of the
previous seal. No later seal may be written until the previous flush succeeds.
Failure poisons the provider before releasing the gate. Explicit flush, replay
promotion, automatic durability and compaction obey the same ordering.

Recovery validates the complete semantic prefix before repairing the file.
On a damaged frame, a position-bound seal with group start *after* the damage
proves the damaged group preceded a successful flush: fail closed. Raw frames,
or the seal belonging to the damaged group, do not supply that proof. Recovery
discards the damaged suffix, seals any surviving unsealed commands, and flushes
before exposing state. A seal whose own checksum/structure is invalid is damage.
This is a crash-recovery format, not an adversarial storage authentication scheme;
a deliberately fabricated valid later seal conservatively prevents recovery.

Under successful FullFlush semantics, all acknowledged writes survive process or
power loss. Like V1's last-frame repair, V2 cannot distinguish a torn final group
from subsequent media corruption of that same group. The ambiguity now spans
the final group. A subsequent seal makes corruption in earlier groups fatal.
Barrier retains its existing weaker device-policy semantics.

Native preparation reserves two writes and enough bytes/codec work for the raw
frame plus one seal before semantic acceptance. The remaining seal ticket moves
to the journal; superseded tickets refund their unused credit. The seal uses a
stack buffer and performs no fresh admission/allocation. Capacity admission
leaves room for two seals when commands remain unsealed: one racing flush may
close them between admission and append, and the accepted command still needs
its own seal. An empty group needs room for only the new command's seal.

## Compatibility

V2 has a distinct journal magic; V1 snapshots remain unchanged. V1 opens retain
the old fail-closed scan. Only after semantic replay and repair succeed does
startup snapshot the recovered state and restart at V2 in the next epoch.
The existing snapshot-first, parent-sync, header-restart protocol makes every
migration cut recoverable. Never rewrite a live V1 header in place. Old binaries
reject V2, and mixed-version concurrent ownership remains excluded by root lock.

## Verification and performance gate

Reproduce reordered unacknowledged commands on the baseline before changing
production code. Exercise surviving same-group seals, corruption proven by a
later seal, every byte cut, legacy migration, maximum-sized commands, capacity
admission, native late cancellation and exact replay. Force a device stall on
the production path; verify writers continue and one subsequent flush covers
their group, with explicit flush/compaction races and cancellation. Model the
production gate under Loom with a broken-order negative control.

Use the same optimized benchmark executable source and device for baseline and
candidate, alternating runs with 1/8/32 writers at 256 bytes and 8 writers at 64 KiB.
Record throughput, latency p50/p95, flush count and file bytes. Timing is evidence,
not a unit-test assertion. A replacement PR requires passing correctness tests,
real-path concurrency evidence and measured preservation of grouped flushes.

# Disk exhaustion, 2026-10-24 ~00:00Z (coordinator): eliminate the class, top priority alongside bb2 utilisation
Three disks filled within hours:
1. **The PC's C: hit 0 bytes free** (1.9 TB), stalling every lane and agent.
   - Causes: the WSL Ubuntu ext4.vhdx had grown to 884 GB, with /home/var at 596 GB:
     - /home/var/src: 3,503 build-slot staging trees, Cloud's and ours, none pruned;
     - merger-wt: 54 GB;
     - a local sccache: 49 GB, unused since local compiles stopped;
     - old lane targets.
   - Also C:\temp: 65 stale lane scratch dirs, mostly codex-windows-*, wq* and stream-review* targets.
   - The coordinator deleted 200 GB of ours inside WSL, but the vhdx can't shrink without an elevated compaction, so C: is at about 16 GB free.
2. **bb2 root fs:** 95%, then 88%. Cloud found /work/target (42 GB) and target-* on the 96 GB root fs, written by some lane bypassing bb-job (job-owned targets live on /mnt/bb). The coordinator deleted the stale ones; root is now at 45%.
3. **ivar:** repeatedly under 1 GB free. /Users/var/mac-target (47 GB) and per-lane targets.

**Structural fixes (implement; atomic installs; measure):**
- **build-slot remote staging:** staging trees under WSL /home/var/src are content-addressed and reused, with an LRU cap and an hourly reaper that skips trees in use. WSL disk stays under a fixed budget, for example 300 GB, with an alert at 80%. Cloud's trees count too: tell the coordinator and Cloud will be asked. Don't delete Cloud's yourself.
- **Root-fs targets on bb2:** bb-admit/bb-job refuse any job whose CARGO_TARGET_DIR (or any target-* path) is on the root filesystem. CORRECTION: /work/target was the gate's own target (/work/target/.gate-pr-tests), not a rogue lane. Move every gate and lane target onto /mnt/bb; the other /work/target-* dirs came from lanes using direct ssh. Make root-fs targets impossible at admission.
- **ivar:** apply the same per-lane target budget plus an LRU reaper, and a disk floor at admission for macOS jobs.
- **PC C:\temp:** lane scratch goes under one root, for example D:\lane-scratch (D: has 99 GB), with a per-lane quota and a reaper. Put it in lane-rules via the coordinator. No lane writes multi-GB trees to C:.
- Disk-floor checks become preventive, admitting by projected job size, not reactive at the floor.

## 2026-10-24 ~12:45Z: bb2 /mnt/bb at 87–89%, tripping Cloud's 89% merge-queue floor
- **sccache:** the server runs with `SCCACHE_CACHE_SIZE=400G` and had reached 316G, against the agreed **50G cap**. The coordinator trimmed it to 50G by access time. Structural fix: put the 50G cap in the single place that launches the sccache server (admission / bs.ps1), and have one test or receipt check assert it.
- **jobs:** 157k top-level entries, 140k of them older than 6 h. The coordinator pruned entries untouched for more than 24 h, skipping any a live process uses. Structural fix: every job's folders (including the `<id>.targets` trees of 30+ GB) are removed when the job's lease ends, by the job owner, not by an age-based reaper. Folders are never deleted while their lease is live. This is the same lease design as gate-speed r25, so do it together with gate-speed.

# bb2 utilisation: the top priority, now run continuously (coordinator, 2026-10-23T22:30Z)
User: "we don't need more cpus we just need to use them more efficiently". There will be NO instance or CPU increase. Stop proposing spot or instance sizing; the problem is waste.

Your own numbers say where the waste is:
- **Rolling lane validation is 131.7 job-h, against gate 8.3 h and main 3.5 h,** so about 90% of bb2 is lanes re-validating in about 20 worktrees.
- **Every job rebuilds from an empty target.** Only sccache helps, and it can't cache linking, proc-macro or build-script outputs, or test binaries.
- **Over 24h: 8.97 job-h per merge.**
- **The bb2 snapshot:** load about 140 on 192 CPUs, while jobs wait in admission (Cloud saw 13.5 min for a no-compile step). So the CPUs are not the limit; admission and duplicate work are.
- **/mnt/bb/jobs holds 115,321 entries / 57 GB.** Job directories are leaking.

From now on you run continuously (a cycle every 10 min whenever idle, at high effort). Implement in this order, measuring before and after each:
1. **Compile once per (tree hash, crate set, profile, features, target) and share.**
   - A content-addressed build-output store: the first job builds, and every later job with the same key gets a copy-on-write clone of the finished target. Use a reflink, overlayfs lower layer or btrfs/xfs snapshot; check which filesystem /mnt/bb uses and pick the fastest correct one.
   - Concurrent identical requests wait on the single builder (single-flight), never build twice.
   - This must cover lane validation, PR gate and main gate alike, which is where the 131.7 h goes.
   - Hermeticity holds because each job writes only to its own CoW layer. Add a negative control showing that a job's writes never leak into the shared base.
2. **Deduplicate identical runs.** Identical (tree hash, command, env) test runs return the recorded result and log for a fully hermetic, already-completed run, instead of re-executing. Lanes re-run the same thing constantly. Keep it strict: any input difference re-runs.
3. **Enforce affected-crate gating inside build-slot itself,** not by lane discipline. Reject or narrow whole-workspace builds and tests from lanes, without coordinator override, to the crates their diff touches plus reverse dependencies.
4. **Admission by measured cores and memory,** not fixed slot counts.
   - Keep the machine at about 90–95% CPU without memory pressure: dynamic CARGO_BUILD_JOBS per job, with more concurrent jobs while load is below target.
   - Fast-path anything that doesn't compile (cargo metadata/tree/locate-project…; gate-speed is doing the classification; coordinate rather than duplicating).
5. **Clean up the job-directory leak** (/mnt/bb/jobs), and fix its cause structurally: jobs must remove their directory on every exit path, plus a reaper for crashed jobs.
6. **Dependency convergence** (53 duplicate crate names): only after 1–5, assigned via the coordinator.

**Rules:**
- Every tooling install is atomic (write temp + rename).
- Never edit a script that running jobs exec in place.
- Coordinate with the gate-speed lane, which owns gate-checks.sh and the admission classification, by touching different files or going through the coordinator.
- Verification is never reduced.
- Report job-h per merge, CPU utilisation, and admission wait p50/p95 every cycle.

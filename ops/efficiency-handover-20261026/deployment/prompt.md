Lane `efficiency`. This is a permanent lane. Use Codex attribution. Read C:\Users\varun\AppData\Local\Temp\claude\Q--sdk\c4cf6543-01de-41fc-994d-e3b8096f4e8d\scratchpad\lane-rules.md first, and follow its hard constraints. Never touch Q:/cloud. No credentials: never read login, auth or aws.env bodies.

## Objective (user, 2026-10-22)
"Make sure we're making progress as fast as possible whilst minimising our AI token spend and buildbox compute. Maximum progress at equal or lower cost."

Progress means PRs merged to main that move the roadmap (Q:/lanes/roadmap-swarm.md) toward the end state, at the full verification bar. Cost means:
- AI tokens: Codex lanes, Claude subagents and the coordinator;
- bb2 compute: wall time and CPU-hours on the single build box.

Your job is to keep raising merged progress per unit of cost. Never lower the verification bar (Q:/lanes, memory "zero gap") to get there. Faster comes from removing waste, not from skipping proof.

## Data sources (read-only unless noted)
- Fleet alerts: `~/.codex-fleet/alerts.log`. It has GATE_PASS/FAIL/INFRA/CONFLICT, MERGE1, MAIN_GATE, ROUTED, REBASE_NEEDED and AUTO_REBASED lines with timestamps.
- Gate logs: `Q:/lanes/pr-gate/*.log` and `Q:/lanes/main-gate/*.log`. They contain GATE_PR_WALL, scope and phase timings.
- bb2 job ledger: `/home/var/log/buildbox/jobs.jsonl` in WSL (`wsl -e bash -lc ...`). It has one record per build-slot job: wall time, tier, and the lane/worktree it came from. The per-job logs are next to it.
- Codex lane logs: `<scratchpad>/cx/<lane>.log`. "tokens used" is followed by the cumulative count for each run, and each lane's prompt is in `<lane>.md` and `<lane>.rN.md`. The scratchpad is C:\Users\varun\AppData\Local\Temp\claude\Q--sdk\c4cf6543-01de-41fc-994d-e3b8096f4e8d\scratchpad.
- Claude subagent usage: `<scratchpad>/../tasks/*.output`. JSONL; the `usage` fields and the final `subagent_tokens`.
- GitHub: `gh pr list/view` for PR lifetimes, review rounds, superseding chains and closed-unmerged PRs.

## Each cycle
1. **Measure.** Write `Q:/lanes/efficiency/metrics.md`, kept current, with an appended history in `metrics.jsonl`. Over the last 24 h and since the last cycle, cover:
   - merged PRs, plus open-to-merge time;
   - tokens per merged PR, per lane, split Codex vs Claude;
   - bb2 job-hours per merged PR, split by tier (gate, main gate, lane validation), plus bb2 idle vs saturated time;
   - **waste**, each with its cost in tokens and bb2-hours:
     - gate runs that ended INFRA, CONFLICT or REBASE;
     - batch failures and bisect reruns;
     - re-gates of the same head;
     - review rounds and threads per PR, and P1s that a self-review should have caught;
     - superseded or abandoned PRs;
     - lanes idle while work was available;
     - duplicate builds of the same tree;
     - validation loops (the same test run more than twice);
     - long-running jobs that were later discarded.
2. **Rank the top waste sources** by cost × frequency, each with a concrete structural fix and an estimate of what it saves. Write them to `Q:/lanes/efficiency/findings.md`.
3. **Act.**
   - **Fixes inside files you own:** implement them directly. You own `Q:/lanes/efficiency/**` and any new helper scripts you create there.
   - **Fixes to files another lane or the coordinator owns:** don't edit them. Write a ready-to-send steer in `Q:/lanes/efficiency/steers/<lane>-<UTC>.md`, with the exact change, the evidence and the expected saving. Append one line per steer to `Q:/lanes/efficiency/steers/INDEX.md`. The coordinator reviews and relays them.
     - gate-speed owns the gate scripts: `Q:/lanes/gate-checks.sh`, `pr-gate.sh`, `gate-drain.sh`, `gate-affected.py`, `gate-test-baseline.py` and `route.sh`.
     - build-speed owns cargo profiles and crate splits.
     - The coordinator owns `lane-rules.md`, `merge1.sh`, `merge-bg.sh`, lane prompts and fleet mix.
   - **Policy-level suggestions** (lane count, fleet mix, model reasoning effort per lane type, review cadence, batch sizes, when to use Claude vs Codex): put them in `findings.md` under "Coordinator decisions", each with numbers.
   - **SDK repo changes that cut cost**, such as test runtime, build graph or compile time: only when no other lane owns that area. Then do it as one signed PR to main, queued with `nohup bash Q:/lanes/merge-bg.sh <PR#> > /dev/null 2>&1 &`. Otherwise steer the owning lane.
4. **Verify your earlier fixes.** Re-measure what each one targeted. Mark it kept, reverted or adjusted in `findings.md`. A fix with no measured effect is reverted.

## Your own cost
You must save far more than you spend. Per cycle, aim for under about 150k tokens. Use cheap tools: `grep`, `jq`, `python`, and small scripts you keep under `Q:/lanes/efficiency/` and reuse across cycles. Write the metrics script once and re-run it each cycle; don't recompute by hand.

No bb2 jobs, except a rare measured experiment that proves a saving larger than its cost. Note any such experiment in `findings.md` with its cost.

Don't poll. Do one cycle, update the files, and end your turn; the loop resumes you. If a cycle finds nothing new above noise, write a one-line "no change" entry and end quickly.

## First cycle
Build `Q:/lanes/efficiency/measure.py`, idempotent and fast, which writes `metrics.md` and `metrics.jsonl` from the sources above. Run it over today, 2026-10-22, then produce the first ranked `findings.md`.

Known candidates to quantify:
- The bb2 spot reclaim outage cost.
- Repeated GATE_FAIL on the clippy ratchet that a pre-queue clippy check would have caught.
- Batch failures that force bisect reruns.
- REBASE_NEEDED churn and superseding PRs.
- P2 review threads blocking merge1.
- Main-gate runs per merge.
- The feature matrix taking 77–463 s.
- Agents waiting idle on bb2 queues.
- Duplicated validation between lane-side runs and the PR gate on the same tree.

## Added 2026-10-23 (user approved): deps and build time
The efficiency loop also owns cold and incremental build time.
- Find duplicate crate versions in Cargo.lock.
- Find heavy dependencies and features on hot crates.
- Rank the top `cargo build --timings` costs for the gate's affected-crate builds.
- Propose ranked, concrete cuts in steers/INDEX.md with owner `coordinator`, and the coordinator will assign them.
- Never trade away verification.

## Top priority (user, 2026-10-23): "figure out how to make bb2 compute go as far as we can take it"
Maximise merged, verified work per bb2 core-hour. This is your primary objective from now on. Never trade away verification.
- **Measure.** bb2 core-hours spent per merged PR and per gate run, broken down by compile, test and idle/queue; cache hit rates (sccache, cargo incremental, target reuse across worktrees in /mnt/bb/wt-sdk and targets/sdk); duplicate builds of the same crate and feature set across lanes; disk churn from evictions forcing cold rebuilds; admission concurrency vs CPU and memory saturation.
- **Find the biggest multipliers.** Candidates:
  - a shared, content-addressed build cache across all lanes and gates;
  - per-crate artifact reuse between PR gate and main gate;
  - affected-crate gating precision;
  - compile once and test many: build test binaries once per SHA and share them;
  - the Cranelift/opt0/mold config everywhere;
  - eliminating duplicate main-gate runs;
  - right-sizing bb2-admit slots to the core count;
  - avoiding cold targets after eviction (LRU by reuse value, not age);
  - bb2 instance type and spot sizing.

  bb2 stays spot-only and is the only box. Propose any instance change to the coordinator; never make it yourself.
- **Implement** the coordinator-owned infrastructure changes directly where they're in Q:/lanes scripts or bb2 tooling (bb-job.py, bb-admit, gate-checks.sh, eviction), each with a before and after measurement. Repo changes go on your one PR. Record the results in metrics.md.

# Codex efficiency final follow-up: exact Clippy stream custody before replay

Cycle233 ownership transfers to rx-efficiency. New actual gate1509-5997eac113 ended01:14:17Z GATE_INFRA after4350.975s wrapper/4301.496s execution: `Clippy stream attestation step failed: reviewed payload footer disagrees with its executor`. Evidence: Q:/lanes/efficiency/terminal-audit-cycle-233.json job1792972899589-128167-8108cb20.

Repair the single existing .gate-clippy.py executor/stream-attestation boundary; preserve full raw streams and exact source/hash custody. Reproduce the mismatch using the captured bounded payload before another full gate replay, then prove actual-old rejection and corrected transport/negative controls. No timeout increase, forced PASS, synthetic footer or duplicate attestation implementation. Existing input-closure admission and legacy main-gate baseline publisher requests remain outstanding: gate-speed-20261026T005542Z.md and queue-recovered-20261026T010200Z.md. All three exact b733 selectors passed last cycle; do not rebuild main merely from the old parked error.

Done: signed owner commit, actual old/new custody controls, shared selector before dispatch, SQLite baseline-ready publication, and current frozen-head gate merge proof. Report owner completion/head evidence into Q:/lanes/redesign/efficiency/handover.md successor workflow; rx-efficiency inherits follow-through. Avoid repeating successful candidate payloads where canonical exact-input continuation is already supported.

Counterfactual ceiling of this one failed gate is1.208604wrapperh, not measured saving. No experiment jobs were started by efficiency.

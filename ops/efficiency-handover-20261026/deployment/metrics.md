# SDK efficiency metrics

Author: Codex. Endpoint: 2026-10-26T01:17:54.197315Z. Runtime: 53.738 s. No bb2 jobs started.

Re-run: `python -X utf8 Q:/lanes/efficiency/measure.py --date 2026-10-26` (`--offline` reuses GitHub metadata). History: `metrics.jsonl`; structured snapshot: `latest.json`.

Progress denominator is all observed SDK merges, not a certified count of roadmap-advancing PRs; roadmap relevance needs coordinator classification. Verification unchanged.

## last_24h: 2026-10-25T01:17:54.197315Z to 2026-10-26T01:17:54.197315Z

| Measure | Value |
|---|---:|
| Merged PRs | 4 |
| Median / p90 open-to-merge hours | 9.605 / 13.785 |
| Claude gross tokens / merge | 162,184,194 / 40,546,048.500 |
| Codex tokens in window / merge | unknown (undated summaries) |
| bb2 completed SDK job-hours / merge | 217.014 / 54.253 |
| bb2 completed SDK jobs | 1588 |
| Main-gate alerts / merge | 1 / 0.250 |
| Logged feature matrix seconds min / median / max | 660.900 / 1,711.000 / 4,898.000 |

| bb2 tier | Job-hours | Job-hours / merge |
|---|---:|---:|
| lane_validation | 186.697 | 46.674 |
| gate | 26.372 | 6.593 |
| main_gate | 3.945 | 0.986 |

Terminal receipt coverage: `{"terminal_receipts": 1588, "legacy_rows": 0, "with_exec_start": 1567, "with_pr_link": 291, "terminations": {"command": 1534, "infra": 43, "cancelled": 3, "interrupted": 8}, "termination_wrapper_hours": {"cancelled": 0.24809361111111114, "command": 205.89885805555554, "infra": 10.600695277777778, "interrupted": 0.2658886111111111}, "execution_kind": {"unspecified": 1587, "existing-verdict-replay": 1}, "known_execution_hours": 189.59710333333334, "execution_samples": 1567, "admission_overhead_hours": 22.99217277777778, "admission_samples": 1567, "admission_overhead_median_seconds": 46.978, "admission_overhead_p90_seconds": 78.775, "admission_overhead_p95_seconds": 99.233, "admission_overhead_max_seconds": 549.668, "admission_overhead_lane_hours": {"approvals": 4.484011388888888, "fs-concurrency": 2.7268830555555565, "plugins": 2.6758449999999994, "providers-final-20261025b": 2.5860680555555566, "b-runtime": 1.9265699999999994, "win-qual": 1.5970280555555554, "mac-live": 0.8990444444444444, "main-green": 0.8600783333333333, "sessions": 0.6525194444444444, "main-rebase": 0.4246155555555556, "providers-final-20261025a": 0.4193544444444445, "client-perf": 0.40277277777777776, "e2e-gate": 0.40030944444444444, "scale-fds5": 0.35766805555555553, "tool-latency-rebased": 0.3269983333333334, "gate-speed": 0.30443194444444444, "pr-gate": 0.2128330555555555, "scale-fds6": 0.21148833333333333, "b-authority-secrets": 0.17824055555555557, "scale-fds5-diag": 0.17206972222222222, "import-main-proof": 0.14363166666666666, "gate-speed-integration": 0.13374666666666665, "rb-sdk-up": 0.1309738888888889, "gate-speed-cpu-controls": 0.1270133333333333, "b-authority-p1-security": 0.11018611111111112, "redesign-verify": 0.07251777777777778, "pr-gate-baseline": 0.05908250000000001, "gate-speed-rebase": 0.05440027777777777, "gate-speed-review": 0.02547611111111111, "gate-speed-clippy-diagnostics": 0.024093611111111114, "gate-speed-exact-wide": 0.0235275, "main-gate": 0.021949722222222225, "rust": 0.02187027777777778, "gate-speed-windows-full": 0.020457777777777776, "windows-legacy-negative": 0.019803888888888888, "gate-speed-main-clippy-b733": 0.016888333333333335, "gate-speed-cache-channel": 0.016013055555555555, "gate-speed-prep-route": 0.013038888888888888, "main-green-combined-authentication": 0.012836666666666668, "gate-speed-cache-policy": 0.012723055555555555, "live-b733": 0.012165833333333332, "gate-speed-clippy-baseline": 0.01002888888888889, "gate-speed-cache-qualified-generation": 0.0082, "gate-speed-cache-final": 0.007858333333333333, "gate-speed-cache-full-v2": 0.007616944444444444, "gate-speed-cache-qualified": 0.0073525000000000005, "gate-speed-cache-signed-source": 0.007233333333333333, "gate-speed-output-custody": 0.007200277777777778, "gate-speed-cache-canonical": 0.006913611111111111, "gate-speed-cache-host": 0.006901944444444445, "gate-speed-cache-full-v3": 0.006828888888888889, "gate-speed-cache-generation": 0.006544722222222223, "gate-speed-cache-full": 0.006417222222222223, "gate-speed-cache-migration": 0.005973333333333334, "gate-speed-cache-accepted": 0.005875833333333333}, "admission_overhead_top_jobs": [{"lane": "approvals", "seconds": 549.668, "termination": "command", "at": "2026-10-26T00:24:55.467Z"}, {"lane": "providers-final-20261025b", "seconds": 374.11, "termination": "command", "at": "2026-10-26T00:20:29.991Z"}, {"lane": "main-green", "seconds": 356.451, "termination": "command", "at": "2026-10-26T00:16:15.840Z"}, {"lane": "win-qual", "seconds": 212.715, "termination": "command", "at": "2026-10-26T00:02:50.836Z"}, {"lane": "win-qual", "seconds": 198.72, "termination": "command", "at": "2026-10-26T00:58:53.054Z"}, {"lane": "win-qual", "seconds": 195.316, "termination": "command", "at": "2026-10-25T15:38:11.218Z"}, {"lane": "approvals", "seconds": 188.018, "termination": "command", "at": "2026-10-26T00:56:09.864Z"}, {"lane": "approvals", "seconds": 183.377, "termination": "command", "at": "2026-10-26T01:10:05.740Z"}, {"lane": "win-qual", "seconds": 180.974, "termination": "command", "at": "2026-10-25T23:44:07.860Z"}, {"lane": "win-qual", "seconds": 173.394, "termination": "command", "at": "2026-10-25T08:13:35.820Z"}], "execution_termination_hours": {"command": 179.31130666666633, "infra": 9.981266388888887, "cancelled": 0.18638111111111114, "interrupted": 0.11814916666666667}, "admission_phase_coverage": {"receipts": 1588, "backends": {"bb2": 1588}, "cache_states": {"ready": 1564, "unknown": 17, "not-started": 7}, "slot_wait_samples": 1571, "slot_wait_hours": 2.7469166666666668e-05, "slot_wait_p50_seconds": 5.7e-05, "slot_wait_p95_seconds": 7.2e-05, "slot_wait_max_seconds": 0.00324}, "note": "Admission overhead includes sync/probes/queue; not pure queue time. Nested remote receipts are not additional jobs. CPU counters have separate partial coverage."}`

Compute/cache coverage: `{"receipts": 1547, "cpu_samples": 1547, "attributed_core_hours": 944.3650076752774, "core_hours_by_phase": {"command": 688.1244012386107, "pr-gate": 210.6340891947222, "pr-baseline": 6.467349086666667, "immutable-fence-controls": 0.0024771619444444444, "immutable-fence-single-pin": 0.0012707380555555556, "immutable-fence-shared-recovery": 0.0016284355555555555, "durable-drain-wakeup": 0.0030810049999999995, "main-wide": 34.68530639694445, "binary-cutoff-controls": 0.0011994919444444443, "cutoff137-real-capture": 0.0008278980555555556, "exact-base-and-kill137": 0.0032731894444444445, "cutoff137-publication": 0.0007693647222222223, "red-matrix-completion": 0.0009015936111111111, "baseline-generation-handoff": 0.006316121666666667, "baseline-restart-wakeup": 0.005134688055555556, "windows-full-matrix-reuse": 3.728172464166666, "clippy-diagnostic-controls": 0.008609064166666668, "exact-wide-test-baseline-controls": 0.002856336388888889, "clippy-stream-controls": 0.019745093888888888, "exact-main-clippy-requalification": 0.4382744333333334, "p0-exact-base-controls": 0.0014039011111111111, "p0-main-clippy-adapter": 0.0035953175, "p0-main-diagnostic-adapter": 0.003760881111111112, "invocation-output-custody": 0.0033636969444444443, "output-custody-controls": 0.0038574238888888887, "cache-policy-controls": 0.0009016813888888889, "cache-host-controls": 0.002738707777777778, "cache-policy-full-controls": 0.010924845833333334, "cache-policy-final-controls": 0.004537494999999999, "cache-policy-qualified-controls": 0.0043830611111111105, "cache-policy-accepted-controls": 0.004592171666666667, "cache-policy-migration-controls": 0.004372525555555555, "cache-generation-controls": 0.0042800825, "cache-policy-final-source": 0.005149185277777778, "cache-policy-canonical-controls": 0.005086618333333334, "cache-qualified-generation": 0.004879040833333334, "cpu-lease-controls": 0.1157968411111111, "cache-authority-lifetime": 0.005939492500000001, "prep-route-controls": 0.0011933161111111112, "rebase-qualification": 0.011071243888888888, "pr-dispatch-profile": 0.0021077475, "pr-dispatch-isolated": 0.002931002222222222, "review-bootstrap": 0.005066161944444444, "review-installer": 0.0011046672222222223, "baseline-admission-controls": 0.00293648, "startup-readiness-controls": 0.004094261111111111, "input-closure-controls": 0.0013866952777777776, "input-closure-upload-controls": 0.0022175016666666665, "input-closure-runtime-controls": 0.0036996424999999997, "input-closure-byte-custody": 0.0019529894444444446}, "samples_by_phase": {"command": 1448, "pr-gate": 15, "pr-baseline": 5, "immutable-fence-controls": 1, "immutable-fence-single-pin": 1, "immutable-fence-shared-recovery": 1, "durable-drain-wakeup": 1, "main-wide": 2, "binary-cutoff-controls": 1, "cutoff137-real-capture": 1, "exact-base-and-kill137": 2, "cutoff137-publication": 1, "red-matrix-completion": 1, "baseline-generation-handoff": 2, "baseline-restart-wakeup": 1, "windows-full-matrix-reuse": 2, "clippy-diagnostic-controls": 3, "exact-wide-test-baseline-controls": 2, "clippy-stream-controls": 5, "exact-main-clippy-requalification": 2, "p0-exact-base-controls": 1, "p0-main-clippy-adapter": 1, "p0-main-diagnostic-adapter": 1, "invocation-output-custody": 1, "output-custody-controls": 1, "cache-policy-controls": 1, "cache-host-controls": 1, "cache-policy-full-controls": 3, "cache-policy-final-controls": 1, "cache-policy-qualified-controls": 1, "cache-policy-accepted-controls": 1, "cache-policy-migration-controls": 1, "cache-generation-controls": 1, "cache-policy-final-source": 1, "cache-policy-canonical-controls": 1, "cache-qualified-generation": 1, "cpu-lease-controls": 14, "cache-authority-lifetime": 1, "prep-route-controls": 1, "rebase-qualification": 3, "pr-dispatch-profile": 1, "pr-dispatch-isolated": 2, "review-bootstrap": 1, "review-installer": 1, "baseline-admission-controls": 2, "startup-readiness-controls": 2, "input-closure-controls": 1, "input-closure-upload-controls": 1, "input-closure-runtime-controls": 1, "input-closure-byte-custody": 1}, "sccache_samples": 1547, "cache_hits": 123846, "cache_misses": 202668, "cache_hit_rate": 0.37929767176905127, "compiler_executions": 202668, "target_store_requests": 6342, "target_store_states": {"changed-input": 814, "private-target": 5171, "miss": 356, "hit": 1}, "target_store_generations": {"sdk-target-reflink-v4-input-namespace": 6342}, "target_store_clone_seconds": 3.912227991022519, "target_store_publish_seconds": 482.2973306720669, "target_store_wait_seconds": 0.028450760917621665, "target_store_local_wait_samples": 6269, "target_store_local_wait_seconds": 1.5931966773496242, "note": "Partial coverage: waited child tree plus owned sccache CPU. Unawaited descendants and killed jobs can be absent. Phase is job label, not compile/test breakdown. No extrapolation to uninstrumented jobs."}`

Admission overhead p50 / p95: 46.978 / 99.233 s (sync/probes included). Measured slot wait p50 / p95: 0.000 / 0.000 s; 1571 samples.


Shared bb2 inferred idle upper bound: 0.065 h; busy lower bound: 23.935 h; peak observed overlapping completed jobs: 29. Saturation / CPU-hours: unknown.

| Waste signal (overlapping) | Evidence / cost | Tokens |
|---|---|---|
| infra_conflict_rebase | {"events": 9, "gate_wall_hours": 1.235, "timed_logs": 1, "bb2_job_hours": 1.2086041666666667, "matched_jobs": 1, "ledger_matched_logs": 1, "PR_count": 9} | unknown |
| clippy_ratchet_failure | {"events": 4, "gate_wall_hours": 6.848611111111111, "timed_logs": 4, "bb2_job_hours": 6.808345833333333, "matched_jobs": 4, "ledger_matched_logs": 4, "PR_count": 5} | unknown |
| batch_failure | {"events": 5, "gate_wall_hours": 9.063888888888888, "timed_logs": 5, "bb2_job_hours": 9.001495833333333, "matched_jobs": 5, "ledger_matched_logs": 5, "PR_count": 9} | unknown |
| all_gate_failure | {"events": 11, "gate_wall_hours": 19.74416666666667, "timed_logs": 11, "bb2_job_hours": 19.625461944444446, "matched_jobs": 11, "ledger_matched_logs": 11, "PR_count": 11} | unknown |
| baseline_deferred | {"events": 1, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 1} | unknown |
| p2_blocked | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| review_thread_blocked | {"events": 11, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 8} | unknown |
| batch_descendant_rerun_candidates | {"runs": 4, "bb2_job_hours": 6.8688827777777774} | unknown |
| repeated_conflict_candidates | {"repeated_events": 1, "bb2_hours": null} | unknown |
| duplicate_tree_command_candidates | {"groups": 2, "extra_jobs": 2, "extra_wall_hours": 0.037851388888888886, "examples": [{"lane": "gate-speed-integration", "runs": 2, "extra_wall_hours": 0.024903333333333333, "identity_sha256": "f52251b14e69ee09", "all_pass": true}, {"lane": "rb-sdk-up", "runs": 2, "extra_wall_hours": 0.012948055555555555, "identity_sha256": "d5c416199c31e306", "all_pass": true}]} | unknown |
| validation_loop_candidates | {"groups": 0, "jobs_beyond_two": 0, "wall_hours_beyond_two": 0.0} | unknown |
| cross_tier_same_tree_candidates | {"groups": 0, "bb2_hours": null} | unknown |
| regate_same_base_head_candidates | {"groups": 0, "extra_gate_wall_hours": 0.0} | unknown |
| abandoned_or_superseded | {"bb2_hours": null, "PR_count": 16} | unknown |
| discarded_long_jobs | {"failed_jobs_over_600s": 152, "failed_job_wall_hours": 62.39319333333333, "discarded_hours": null} | unknown |
| spot_reclaim | {"bb2_hours": null, "retry_elapsed_lower_bound_hours": 0.0} | unknown |
| review | {"bb2_hours": null, "avoidable_p1": null} | unknown |
| idle_lanes_with_work | {"lane_hours": null, "bb2_hours": null} | unknown |

Per-PR review submissions / threads / P1 / P2 (current snapshot):

| PR | Reviews | Threads | P1 | P2 |
|---|---:|---:|---:|---:|
| #1514 | 1 | 2 | 1 | 1 |
| #1513 | 0 | 0 | 0 | 0 |
| #1512 | 0 | 0 | 0 | 0 |
| #1511 | 0 | 0 | 0 | 0 |
| #1510 | 4 | 2 | 0 | 2 |
| #1509 | 3 | 2 | 1 | 1 |
| #1508 | 0 | 0 | 0 | 0 |
| #1507 | 2 | 4 | 3 | 1 |
| #1506 | 5 | 2 | 1 | 1 |
| #1505 | 2 | 1 | 0 | 1 |
| #1504 | 2 | 1 | 1 | 0 |
| #1503 | 0 | 0 | 0 | 0 |
| #1502 | 0 | 0 | 0 | 0 |
| #1501 | 0 | 0 | 0 | 0 |
| #1500 | 5 | 3 | 1 | 2 |
| #1499 | 2 | 1 | 0 | 1 |
| #1498 | 0 | 0 | 0 | 0 |
| #1497 | 14 | 7 | 6 | 1 |
| #1496 | 0 | 0 | 0 | 0 |
| #1495 | 5 | 2 | 0 | 2 |
| #1494 | 0 | 0 | 0 | 0 |
| #1493 | 0 | 0 | 0 | 0 |
| #1492 | 3 | 1 | 1 | 0 |
| #1491 | 0 | 0 | 0 | 0 |
| #1490 | 5 | 2 | 2 | 0 |
| #1489 | 2 | 1 | 1 | 0 |
| #1488 | 0 | 0 | 0 | 0 |
| #1487 | 3 | 2 | 1 | 1 |
| #1485 | 0 | 0 | 0 | 0 |
| #1484 | 4 | 3 | 2 | 1 |
| #1482 | 4 | 3 | 2 | 1 |
| #1481 | 2 | 1 | 0 | 1 |
| #1475 | 2 | 1 | 0 | 1 |
| #1469 | 6 | 6 | 5 | 1 |
| #1407 | 0 | 0 | 0 | 0 |

Claude tokens by recorded agent (lane mapping unavailable; not inferred PR ownership):

| Agent | Input | Output | Cache create | Cache read | Gross / fleet merge |
|---|---:|---:|---:|---:|---:|
| ac099fa64bdbb8a23 | 1,874 | 623,163 | 13,590,330 | 141,903,270 | 39,029,659.250 |
| ace07418aa8d2bd1f | 110 | 19,429 | 172,680 | 3,747,629 | 984,962.000 |
| a58ac6fb59951302b | 54 | 10,728 | 95,117 | 2,019,810 | 531,427.250 |

Merged PR open-to-merge hours:

#1501 (11.02 h), #1499 (13.79 h), #1489 (8.09 h), #1487 (8.19 h)

## today_utc: 2026-10-26T00:00:00Z to 2026-10-26T01:17:54.197315Z

| Measure | Value |
|---|---:|
| Merged PRs | 0 |
| Median / p90 open-to-merge hours | unknown / unknown |
| Claude gross tokens / merge | 18,443,371 / unknown |
| Codex tokens in window / merge | unknown (undated summaries) |
| bb2 completed SDK job-hours / merge | 15.196 / unknown |
| bb2 completed SDK jobs | 124 |
| Main-gate alerts / merge | 1 / unknown |
| Logged feature matrix seconds min / median / max | 1,468.500 / 1,711.000 / 4,729.000 |

| bb2 tier | Job-hours | Job-hours / merge |
|---|---:|---:|
| lane_validation | 13.978 | unknown |
| gate | 1.218 | unknown |

Terminal receipt coverage: `{"terminal_receipts": 124, "legacy_rows": 0, "with_exec_start": 123, "with_pr_link": 25, "terminations": {"command": 119, "infra": 4, "interrupted": 1}, "termination_wrapper_hours": {"command": 12.905345555555556, "infra": 2.2421327777777775, "interrupted": 0.048219722222222226}, "execution_kind": {"unspecified": 124}, "known_execution_hours": 12.305029444444445, "execution_samples": 123, "admission_overhead_hours": 2.4958324999999997, "admission_samples": 123, "admission_overhead_median_seconds": 53.812, "admission_overhead_p90_seconds": 134.113, "admission_overhead_p95_seconds": 183.377, "admission_overhead_max_seconds": 549.668, "admission_overhead_lane_hours": {"approvals": 0.5742641666666667, "providers-final-20261025b": 0.28738888888888886, "main-green": 0.22550416666666667, "win-qual": 0.20936111111111114, "client-perf": 0.19458638888888888, "plugins": 0.1421194444444444, "rb-sdk-up": 0.1309738888888889, "b-runtime": 0.11221222222222223, "b-authority-p1-security": 0.11018611111111112, "gate-speed": 0.09952888888888889, "fs-concurrency": 0.08404777777777778, "redesign-verify": 0.07251777777777778, "tool-latency-rebased": 0.049439444444444446, "mac-live": 0.04605694444444444, "gate-speed-integration": 0.043560277777777774, "b-authority-secrets": 0.04211444444444444, "rust": 0.02187027777777778, "pr-gate": 0.020375277777777777, "gate-speed-main-clippy-b733": 0.016888333333333335, "main-green-combined-authentication": 0.012836666666666668}, "admission_overhead_top_jobs": [{"lane": "approvals", "seconds": 549.668, "termination": "command", "at": "2026-10-26T00:24:55.467Z"}, {"lane": "providers-final-20261025b", "seconds": 374.11, "termination": "command", "at": "2026-10-26T00:20:29.991Z"}, {"lane": "main-green", "seconds": 356.451, "termination": "command", "at": "2026-10-26T00:16:15.840Z"}, {"lane": "win-qual", "seconds": 212.715, "termination": "command", "at": "2026-10-26T00:02:50.836Z"}, {"lane": "win-qual", "seconds": 198.72, "termination": "command", "at": "2026-10-26T00:58:53.054Z"}, {"lane": "approvals", "seconds": 188.018, "termination": "command", "at": "2026-10-26T00:56:09.864Z"}, {"lane": "approvals", "seconds": 183.377, "termination": "command", "at": "2026-10-26T01:10:05.740Z"}, {"lane": "client-perf", "seconds": 158.865, "termination": "command", "at": "2026-10-26T01:02:26.553Z"}, {"lane": "approvals", "seconds": 153.916, "termination": "command", "at": "2026-10-26T00:58:07.977Z"}, {"lane": "win-qual", "seconds": 149.733, "termination": "command", "at": "2026-10-26T01:15:06.416Z"}], "execution_termination_hours": {"command": 10.08410777777777, "infra": 2.1942866666666667, "interrupted": 0.026635}, "admission_phase_coverage": {"receipts": 124, "backends": {"bb2": 124}, "cache_states": {"ready": 123, "unknown": 1}, "slot_wait_samples": 123, "slot_wait_hours": 1.921111111111111e-06, "slot_wait_p50_seconds": 5.5e-05, "slot_wait_p95_seconds": 6.7e-05, "slot_wait_max_seconds": 7.9e-05}, "note": "Admission overhead includes sync/probes/queue; not pure queue time. Nested remote receipts are not additional jobs. CPU counters have separate partial coverage."}`

Compute/cache coverage: `{"receipts": 121, "cpu_samples": 121, "attributed_core_hours": 56.956020934166666, "core_hours_by_phase": {"command": 46.90001299833333, "exact-main-clippy-requalification": 0.22704518694444448, "baseline-admission-controls": 0.00293648, "startup-readiness-controls": 0.004094261111111111, "input-closure-controls": 0.0013866952777777776, "input-closure-upload-controls": 0.0022175016666666665, "input-closure-runtime-controls": 0.0036996424999999997, "input-closure-byte-custody": 0.0019529894444444446, "pr-gate": 9.81267517888889}, "samples_by_phase": {"command": 111, "exact-main-clippy-requalification": 1, "baseline-admission-controls": 2, "startup-readiness-controls": 2, "input-closure-controls": 1, "input-closure-upload-controls": 1, "input-closure-runtime-controls": 1, "input-closure-byte-custody": 1, "pr-gate": 1}, "sccache_samples": 121, "cache_hits": 9204, "cache_misses": 5135, "cache_hit_rate": 0.641885766092475, "compiler_executions": 5135, "target_store_requests": 419, "target_store_states": {"private-target": 329, "miss": 36, "changed-input": 54}, "target_store_generations": {"sdk-target-reflink-v4-input-namespace": 419}, "target_store_clone_seconds": 0.0009297411306761205, "target_store_publish_seconds": 31.42202735209139, "target_store_wait_seconds": 0.002214492647908628, "target_store_local_wait_samples": 416, "target_store_local_wait_seconds": 0.10662851430242881, "note": "Partial coverage: waited child tree plus owned sccache CPU. Unawaited descendants and killed jobs can be absent. Phase is job label, not compile/test breakdown. No extrapolation to uninstrumented jobs."}`

Admission overhead p50 / p95: 53.812 / 183.377 s (sync/probes included). Measured slot wait p50 / p95: 0.000 / 0.000 s; 123 samples.


Shared bb2 inferred idle upper bound: 0.023 h; busy lower bound: 1.275 h; peak observed overlapping completed jobs: 29. Saturation / CPU-hours: unknown.

| Waste signal (overlapping) | Evidence / cost | Tokens |
|---|---|---|
| infra_conflict_rebase | {"events": 1, "gate_wall_hours": 1.235, "timed_logs": 1, "bb2_job_hours": 1.2086041666666667, "matched_jobs": 1, "ledger_matched_logs": 1, "PR_count": 1} | unknown |
| clippy_ratchet_failure | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| batch_failure | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| all_gate_failure | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| baseline_deferred | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| p2_blocked | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| review_thread_blocked | {"events": 2, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 2} | unknown |
| batch_descendant_rerun_candidates | {"runs": 0, "bb2_job_hours": 0.0} | unknown |
| repeated_conflict_candidates | {"repeated_events": 0, "bb2_hours": null} | unknown |
| duplicate_tree_command_candidates | {"groups": 1, "extra_jobs": 1, "extra_wall_hours": 0.012948055555555555, "examples": [{"lane": "rb-sdk-up", "runs": 2, "extra_wall_hours": 0.012948055555555555, "identity_sha256": "d5c416199c31e306", "all_pass": true}]} | unknown |
| validation_loop_candidates | {"groups": 0, "jobs_beyond_two": 0, "wall_hours_beyond_two": 0.0} | unknown |
| cross_tier_same_tree_candidates | {"groups": 0, "bb2_hours": null} | unknown |
| regate_same_base_head_candidates | {"groups": 0, "extra_gate_wall_hours": 0.0} | unknown |
| abandoned_or_superseded | {"bb2_hours": null, "PR_count": 2} | unknown |
| discarded_long_jobs | {"failed_jobs_over_600s": 12, "failed_job_wall_hours": 4.5133294444444445, "discarded_hours": null} | unknown |
| spot_reclaim | {"bb2_hours": null, "retry_elapsed_lower_bound_hours": 0.0} | unknown |
| review | {"bb2_hours": null, "avoidable_p1": null} | unknown |
| idle_lanes_with_work | {"lane_hours": null, "bb2_hours": null} | unknown |

Per-PR review submissions / threads / P1 / P2 (current snapshot):

| PR | Reviews | Threads | P1 | P2 |
|---|---:|---:|---:|---:|
| #1514 | 1 | 2 | 1 | 1 |
| #1513 | 0 | 0 | 0 | 0 |
| #1512 | 0 | 0 | 0 | 0 |
| #1511 | 0 | 0 | 0 | 0 |
| #1510 | 4 | 2 | 0 | 2 |
| #1509 | 3 | 2 | 1 | 1 |
| #1508 | 0 | 0 | 0 | 0 |
| #1507 | 2 | 4 | 3 | 1 |
| #1504 | 2 | 1 | 1 | 0 |
| #1503 | 0 | 0 | 0 | 0 |
| #1500 | 5 | 3 | 1 | 2 |
| #1498 | 0 | 0 | 0 | 0 |
| #1495 | 5 | 2 | 0 | 2 |
| #1494 | 0 | 0 | 0 | 0 |
| #1492 | 3 | 1 | 1 | 0 |
| #1485 | 0 | 0 | 0 | 0 |

Claude tokens by recorded agent (lane mapping unavailable; not inferred PR ownership):

| Agent | Input | Output | Cache create | Cache read | Gross / fleet merge |
|---|---:|---:|---:|---:|---:|
| ac099fa64bdbb8a23 | 194 | 74,997 | 839,939 | 14,083,555 | unknown |
| ace07418aa8d2bd1f | 64 | 12,313 | 110,216 | 2,409,785 | unknown |
| a58ac6fb59951302b | 20 | 5,952 | 10,787 | 895,549 | unknown |

Merged PR open-to-merge hours:



## since_last_cycle: 2026-10-26T00:56:24.167664Z to 2026-10-26T01:17:54.197315Z

| Measure | Value |
|---|---:|
| Merged PRs | 0 |
| Median / p90 open-to-merge hours | unknown / unknown |
| Claude gross tokens / merge | 1,885,761 / unknown |
| Codex tokens in window / merge | unknown (undated summaries) |
| bb2 completed SDK job-hours / merge | 4.805 / unknown |
| bb2 completed SDK jobs | 41 |
| Main-gate alerts / merge | 0 / unknown |
| Logged feature matrix seconds min / median / max | unknown / unknown / unknown |

| bb2 tier | Job-hours | Job-hours / merge |
|---|---:|---:|
| lane_validation | 3.587 | unknown |
| gate | 1.218 | unknown |

Terminal receipt coverage: `{"terminal_receipts": 41, "legacy_rows": 0, "with_exec_start": 41, "with_pr_link": 9, "terminations": {"command": 40, "infra": 1}, "termination_wrapper_hours": {"command": 3.596064166666667, "infra": 1.2086041666666667}, "execution_kind": {"unspecified": 41}, "known_execution_hours": 3.9686125, "execution_samples": 41, "admission_overhead_hours": 0.7502733333333333, "admission_samples": 41, "admission_overhead_median_seconds": 50.177, "admission_overhead_p90_seconds": 149.733, "admission_overhead_p95_seconds": 158.865, "admission_overhead_max_seconds": 198.72, "admission_overhead_lane_hours": {"approvals": 0.15113305555555553, "client-perf": 0.09793916666666667, "win-qual": 0.0967925, "b-authority-p1-security": 0.06887555555555555, "gate-speed": 0.05556638888888889, "redesign-verify": 0.04820861111111111, "gate-speed-integration": 0.043560277777777774, "plugins": 0.035118333333333335, "providers-final-20261025b": 0.03202083333333333, "rb-sdk-up": 0.03009, "tool-latency-rebased": 0.024728055555555555, "b-runtime": 0.020913055555555553, "pr-gate": 0.020375277777777777, "rust": 0.01364888888888889, "main-green": 0.011303333333333334}, "admission_overhead_top_jobs": [{"lane": "win-qual", "seconds": 198.72, "termination": "command", "at": "2026-10-26T00:58:53.054Z"}, {"lane": "approvals", "seconds": 183.377, "termination": "command", "at": "2026-10-26T01:10:05.740Z"}, {"lane": "client-perf", "seconds": 158.865, "termination": "command", "at": "2026-10-26T01:02:26.553Z"}, {"lane": "approvals", "seconds": 153.916, "termination": "command", "at": "2026-10-26T00:58:07.977Z"}, {"lane": "win-qual", "seconds": 149.733, "termination": "command", "at": "2026-10-26T01:15:06.416Z"}, {"lane": "client-perf", "seconds": 122.368, "termination": "command", "at": "2026-10-26T01:11:48.978Z"}, {"lane": "approvals", "seconds": 110.552, "termination": "command", "at": "2026-10-26T01:13:08.947Z"}, {"lane": "approvals", "seconds": 96.234, "termination": "command", "at": "2026-10-26T01:15:00.392Z"}, {"lane": "plugins", "seconds": 76.249, "termination": "command", "at": "2026-10-26T01:12:04.642Z"}, {"lane": "client-perf", "seconds": 71.348, "termination": "command", "at": "2026-10-26T01:05:28.088Z"}], "execution_termination_hours": {"command": 2.7737524999999996, "infra": 1.19486}, "admission_phase_coverage": {"receipts": 41, "backends": {"bb2": 41}, "cache_states": {"ready": 41}, "slot_wait_samples": 41, "slot_wait_hours": 6.430555555555555e-07, "slot_wait_p50_seconds": 5.5e-05, "slot_wait_p95_seconds": 6.6e-05, "slot_wait_max_seconds": 7.9e-05}, "note": "Admission overhead includes sync/probes/queue; not pure queue time. Nested remote receipts are not additional jobs. CPU counters have separate partial coverage."}`

Compute/cache coverage: `{"receipts": 41, "cpu_samples": 41, "attributed_core_hours": 24.31806895416667, "core_hours_by_phase": {"command": 14.496136946388889, "input-closure-controls": 0.0013866952777777776, "input-closure-upload-controls": 0.0022175016666666665, "input-closure-runtime-controls": 0.0036996424999999997, "input-closure-byte-custody": 0.0019529894444444446, "pr-gate": 9.81267517888889}, "samples_by_phase": {"command": 36, "input-closure-controls": 1, "input-closure-upload-controls": 1, "input-closure-runtime-controls": 1, "input-closure-byte-custody": 1, "pr-gate": 1}, "sccache_samples": 41, "cache_hits": 5305, "cache_misses": 4927, "cache_hit_rate": 0.5184714620797498, "compiler_executions": 4927, "target_store_requests": 241, "target_store_states": {"changed-input": 18, "miss": 17, "private-target": 206}, "target_store_generations": {"sdk-target-reflink-v4-input-namespace": 241}, "target_store_clone_seconds": 0.00041788609814830124, "target_store_publish_seconds": 13.706416552042356, "target_store_wait_seconds": 0.0012103547342121601, "target_store_local_wait_samples": 238, "target_store_local_wait_seconds": 0.06688882931484841, "note": "Partial coverage: waited child tree plus owned sccache CPU. Unawaited descendants and killed jobs can be absent. Phase is job label, not compile/test breakdown. No extrapolation to uninstrumented jobs."}`

Admission overhead p50 / p95: 50.177 / 158.865 s (sync/probes included). Measured slot wait p50 / p95: 0.000 / 0.000 s; 41 samples.


Shared bb2 inferred idle upper bound: 0.023 h; busy lower bound: 0.335 h; peak observed overlapping completed jobs: 12. Saturation / CPU-hours: unknown.

| Waste signal (overlapping) | Evidence / cost | Tokens |
|---|---|---|
| infra_conflict_rebase | {"events": 1, "gate_wall_hours": 1.235, "timed_logs": 1, "bb2_job_hours": 1.2086041666666667, "matched_jobs": 1, "ledger_matched_logs": 1, "PR_count": 1} | unknown |
| clippy_ratchet_failure | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| batch_failure | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| all_gate_failure | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| baseline_deferred | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| p2_blocked | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| review_thread_blocked | {"events": 0, "gate_wall_hours": 0.0, "timed_logs": 0, "bb2_job_hours": 0.0, "matched_jobs": 0, "ledger_matched_logs": 0, "PR_count": 0} | unknown |
| batch_descendant_rerun_candidates | {"runs": 0, "bb2_job_hours": 0.0} | unknown |
| repeated_conflict_candidates | {"repeated_events": 0, "bb2_hours": null} | unknown |
| duplicate_tree_command_candidates | {"groups": 0, "extra_jobs": 0, "extra_wall_hours": 0.0, "examples": []} | unknown |
| validation_loop_candidates | {"groups": 0, "jobs_beyond_two": 0, "wall_hours_beyond_two": 0.0} | unknown |
| cross_tier_same_tree_candidates | {"groups": 0, "bb2_hours": null} | unknown |
| regate_same_base_head_candidates | {"groups": 0, "extra_gate_wall_hours": 0.0} | unknown |
| abandoned_or_superseded | {"bb2_hours": null, "PR_count": 1} | unknown |
| discarded_long_jobs | {"failed_jobs_over_600s": 5, "failed_job_wall_hours": 2.1309944444444446, "discarded_hours": null} | unknown |
| spot_reclaim | {"bb2_hours": null, "retry_elapsed_lower_bound_hours": 0.0} | unknown |
| review | {"bb2_hours": null, "avoidable_p1": null} | unknown |
| idle_lanes_with_work | {"lane_hours": null, "bb2_hours": null} | unknown |

Per-PR review submissions / threads / P1 / P2 (current snapshot):

| PR | Reviews | Threads | P1 | P2 |
|---|---:|---:|---:|---:|
| #1514 | 1 | 2 | 1 | 1 |
| #1513 | 0 | 0 | 0 | 0 |
| #1507 | 2 | 4 | 3 | 1 |
| #1504 | 2 | 1 | 1 | 0 |
| #1500 | 5 | 3 | 1 | 2 |
| #1495 | 5 | 2 | 0 | 2 |
| #1494 | 0 | 0 | 0 | 0 |
| #1492 | 3 | 1 | 1 | 0 |

Claude tokens by recorded agent (lane mapping unavailable; not inferred PR ownership):

| Agent | Input | Output | Cache create | Cache read | Gross / fleet merge |
|---|---:|---:|---:|---:|---:|
| ac099fa64bdbb8a23 | 18 | 4,885 | 418,014 | 1,462,844 | unknown |

Merged PR open-to-merge hours:



## Codex session-counter inventory

Session high-water counters deduplicate resumed footers, including quota refusals. Copies named *.run.*.log are excluded. These counts cannot be assigned to today or one account; previous sum-of-resumes inventories are superseded. Active/incomplete runs may have spent additional tokens. Observation rates reflect reporting time, not exact spend time.

Observation interval 0.357h; newly observed tokens per fleet merge unknown. Unknown until comparable corrected snapshots exist.

| Lane | Completed runs | Session tokens | Newly observed since cycle | Observed tokens/h |
|---|---:|---:|---:|---:|
| windows | 17 | 49,641,187 | 0 | 0.000 |
| providers | 17 | 30,298,149 | 0 | 0.000 |
| b-authority | 37 | 29,585,568 | 0 | 0.000 |
| b-runtime | 26 | 28,295,849 | 0 | 0.000 |
| approvals | 11 | 28,107,874 | 0 | 0.000 |
| b-substrate | 12 | 27,269,568 | 0 | 0.000 |
| tool-latency | 13 | 20,816,437 | 589772 | 1,651,250.512 |
| sessions | 11 | 19,375,857 | 41128 | 115,150.653 |
| plugins | 27 | 18,949,329 | 0 | 0.000 |
| gate-speed | 50 | 16,072,458 | 0 | 0.000 |
| mac-live | 31 | 15,357,400 | 0 | 0.000 |
| fs-concurrency | 10 | 12,805,727 | 404057 | 1,131,283.493 |
| main-green | 25 | 12,624,266 | 0 | 0.000 |
| client-perf | 10 | 12,009,257 | 0 | 0.000 |
| dst | 2 | 8,272,604 | 0 | 0.000 |
| e2e-graphcoder | 14 | 7,666,138 | 0 | 0.000 |
| volume-search | 2 | 7,126,917 | 0 | 0.000 |
| e2e-gate | 22 | 6,811,560 | 75425 | 211,175.793 |
| skills-context | 10 | 6,422,607 | 0 | 0.000 |
| web-outcomes | 2 | 5,168,690 | 0 | 0.000 |
| deletion | 1 | 4,717,468 | 0 | 0.000 |
| ws-green | 1 | 4,481,757 | 0 | 0.000 |
| addressing | 7 | 4,213,513 | 0 | 0.000 |
| objects-flake | 11 | 3,803,057 | 0 | 0.000 |
| test-consolidate | 1 | 3,193,176 | 0 | 0.000 |
| shell-name | 6 | 2,652,653 | 0 | 0.000 |
| lib-speed | 3 | 2,248,367 | 0 | 0.000 |
| pr1037 | 1 | 2,236,014 | 0 | 0.000 |
| tui-stream | 4 | 2,044,450 | 0 | 0.000 |
| pr-triage | 4 | 1,993,381 | 0 | 0.000 |
| fuzz-targets | 2 | 1,901,053 | 0 | 0.000 |
| rebase-split | 3 | 1,760,735 | 0 | 0.000 |
| build-speed | 2 | 1,739,163 | 0 | 0.000 |
| macos-watch | 1 | 1,728,015 | 0 | 0.000 |
| macos-mount | 3 | 1,535,338 | 0 | 0.000 |
| test-budget | 1 | 1,306,196 | 0 | 0.000 |
| devdep-cycle | 2 | 1,215,534 | 0 | 0.000 |
| env-context | 2 | 1,150,630 | 0 | 0.000 |
| substrate-runner | 5 | 1,041,245 | 0 | 0.000 |
| lock-guard | 5 | 1,010,218 | 0 | 0.000 |
| verify | 5 | 928,684 | 226569 | 634,350.524 |
| predictor | 3 | 927,763 | 0 | 0.000 |
| projfs-recovery | 3 | 895,071 | 0 | 0.000 |
| child-errors | 1 | 804,238 | 0 | 0.000 |
| win-shell | 2 | 760,382 | 0 | 0.000 |
| headless-retry | 1 | 616,755 | 0 | 0.000 |
| xplat-live | 2 | 483,656 | 0 | 0.000 |
| final-message | 1 | 416,428 | 0 | 0.000 |
| test-hygiene | 1 | 347,217 | 0 | 0.000 |
| roadmap-audit | 1 | 270,659 | 0 | 0.000 |
| checkout-next | 1 | 217,898 | 0 | 0.000 |
| dev-profile | 0 | 0 | 0 | 0.000 |
| intelligence | 0 | 0 | 0 | 0.000 |
| streams | 0 | 0 | 0 | 0.000 |

## Measured bb2 cache policy (Codex)

Ephemeral-target fixture: before 0.687 s mean; separate-job reuse after 0.307 s mean (2.24x); steady repeat 4.21x. Production total savings pending; no verification skipped.

Deployed: bb-job.py central/nested policy, native build-slot and WSL buildbox.mjs remote payload use CARGO_INCREMENTAL=0; shared sccache remains enabled. Per-job owned CPU/cache counters now retained. Details: compute-results.json, cache-before.log, cache-after-qualified.log and validation.log.

Historical CPU-hours, compile/test CPU split, per-crate duplicates and eviction rebuild cost remain unknown. Fixture child CPU is not full job CPU. Four bounded proof/validation jobs are recorded in findings.md; the routine collector itself starts none.

## bb2 CPU sample

At 2026-10-26T01:19:30.889507+00:00: CPU busy 59.58%, I/O wait 1.31% over 1.00 s; 192 logical CPUs. This point sample is not a 24-hour saturation estimate.

## Cargo target sharing (Codex)

Codex cycle233: v4-input-namespace 6342 requests, {'changed-input': 814, 'private-target': 5171, 'miss': 356, 'hit': 1}, cross-job repeated-key groups2. Gate routing steer OBSERVED_PARTIAL; old-global users0, currently blocked leases0. Fleet ROI unqualified; zero bb2 experiment jobs.

Details and gaps: findings.md; evidence: cow-results.json and cow-*.log. Tests always execute.

## Coverage and uncertainty

- Codex resumed counters are session-cumulative, deduplicated by session high-water. Completed summaries lack trustworthy run timestamps: inventory is not24h/account usage; deltas are newly observed counters, not necessarily tokens spent in the interval. Old sum-of-resumes inventories are superseded, not comparable.
- Claude gross tokens include cache reads/creation, deduplicated by message ID using component-wise maximum; not dollars or directly comparable to Codex summaries. Coordinator usage outside task files unavailable.
- Claude per-agent identity is available, but a reliable agent-to-lane mapping is absent. Branch main is shared by many agents and must not be treated as one lane.
- Per-lane tokens are supplied; PR attribution is unknown without run-to-PR/tree events. Tokens per merge are fleet window ratios, not PR lifecycle costs.
- Gate times use terminal alert time where available, otherwise filesystem mtime (proxy); overwritten same-name logs lose earlier runs.
- Waste categories overlap. Gate logged wall includes orchestration, is not CPU time and is not additive to ledger job-hours.
- Receipt execution/admission timestamps are reported separately; admission includes sync/probes, not just queue. New compute counters have explicit partial coverage; legacy CPU-hours and saturation remain unknown.
- SDK-tagged jobs only; older untagged records excluded rather than guessed. GitHub review pagination capped at 100 entries per PR. Closed-unmerged is not proof of wasted work.
- Claude task mtime filter retains two days around endpoint; old files modified later remain scanned. Historical backfills require retained sources.

## Iteration outcome targets

Author: Codex. Endpoint2026-10-26T01:17:54.197315Z. Queued-to-merged p50/p95 9.300266840694444/12.632916424777777h (4 explicit pairs); jobh/merge 54.253384; longest rolling24h drought 14.263h. First-observed PR pass 1/16 (0.0625); lifetime/head-first pass remains unqualified. Sent-steer to implemented p50/p95 None/Noneh (0 verified outcomes); relays excluded. Sources and coverage: iteration.json.

- First-observed PR outcome in retained alerts is not guaranteed lifetime first-pass or a head-specific pass rate.
- Queue latency covers only explicit first-observed MERGE1 queued events with a successful merge; missing queues excluded.
- Relay acknowledgement is separate from installation/application. Missing outcome is unknown, not zero latency.

## User hard targets effective2026-10-26

Author: Codex. Final cycle233 endpoint 2026-10-26T01:17:54.197315Z. All six thresholds missed.

|Metric|Target|Measured current value|
|---|---|---|
|Queued to merged p50|<45 min|558.02 min (4 explicit pairs)|
|Job wrapper wall p95|<10 min|24.49 min /1588 completed SDK jobs|
|Nonzero jobs over5 min|0|301 rolling24h;6 new this cycle|
|Job-hours per merge|<3|54.253|
|Admission overhead per job|<5 s|p50 46.978s; p95 99.233s|
|Current merge drought with queue|<60 min|189.79 min; queue [1495, 1504, 1508, 1509, 1512]|

Nonzero terminals include expected negative controls;301 is a failure-candidate count, not301 proven discarded jobs. Each of the6 newly observed late failures has its own LATE_FAILURE alert and bounded cause; historical cases remain a diagnosis backlog. Admission per-job distribution is not a guarantee. Attributed core-hours have partial coverage; dividing943 by192*24 does not establish20% actual fleet CPU use. The60s non-LLM watchdog is NOT INSTALLED. The later retirement/handover instruction supersedes starting it in this lane; rx-efficiency inherits LONG_JOB/LATE_FAILURE/HOLD/DROUGHT/IDLE_CAPACITY/RECOMPILE implementation with negative controls, atomic installation, inbox wakeup and live smoke.

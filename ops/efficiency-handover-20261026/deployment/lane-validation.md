# Lane validation — Codex

Endpoint 2026-10-26T01:17:54.197315Z; 186.697140 of 217.013536 SDK job-hours (86.03%). No bb2 jobs started.

| Lane | Jobs | Job-hours | Failed job-hours |
|---|---:|---:|---:|
| approvals | 197 | 30.618 | 11.739 |
| b-runtime | 158 | 27.990 | 16.782 |
| fs-concurrency | 238 | 19.314 | 7.301 |
| mac-live | 67 | 17.010 | 7.415 |
| providers-final-20261025b | 194 | 16.871 | 10.201 |
| plugins | 196 | 16.558 | 9.492 |
| win-qual | 61 | 14.246 | 2.808 |
| sessions | 55 | 8.295 | 3.640 |
| tool-latency-rebased | 33 | 5.743 | 0.524 |
| main-green | 48 | 4.778 | 2.616 |
| scale-fds5 | 30 | 4.008 | 0.000 |
| main-rebase | 23 | 3.204 | 1.573 |
| scale-fds6 | 29 | 3.058 | 0.285 |
| providers-final-20261025a | 30 | 2.803 | 0.778 |
| client-perf | 20 | 2.671 | 1.050 |
| e2e-gate | 33 | 2.304 | 1.316 |
| scale-fds5-diag | 18 | 1.565 | 0.000 |
| import-main-proof | 8 | 1.262 | 0.025 |
| rb-sdk-up | 10 | 0.876 | 0.028 |
| b-authority-secrets | 14 | 0.538 | 0.181 |

Exclusive kinds (command flags take precedence; script-name classes remain descriptive):

- all_features: 46 jobs / 4.377243 jobh.
- opaque_or_focused: 1297 jobs / 156.280872 jobh.
- platform: 20 jobs / 4.301245 jobh.
- qualification: 202 jobs / 21.737780 jobh.

Overlapping tags, do not sum:

- all_features_command: 46 jobs / 4.377243 jobh.
- all_features_log: 307 jobs / 53.176274 jobh.
- all_targets_command: 6 jobs / 0.706566 jobh.
- all_targets_log: 113 jobs / 23.973783 jobh.
- qualification_script_name: 202 jobs / 21.737780 jobh.
- reference_script_name: 139 jobs / 16.296724 jobh.
- windows_script_name: 21 jobs / 4.320471 jobh.
- workspace_log: 1 jobs / 0.020619 jobh.

Exact full-tree/command repetitions: 2 groups; 0.037851 extra jobh; 0.000000 jobh after two. These are investigation ceilings, not measured savings. Cross-tier same-tree groups: 0 (not permission to reuse gate proof).

| Repeated identity | Lanes | Runs | Extra jobh | Same-failure runs |
|---|---|---:|---:|---:|
| f52251b14e69ee09 | gate-speed-windows-full | 2 | 0.0249 | 0 |
| d5c416199c31e306 | rb-sdk-up | 2 | 0.0129 | 0 |

Repeated script metadata (changed inputs remain distinct):

- tool-latency-rebased / `.bb-final.py`: 33 jobs / 5.743157jobh / 32 input identities.
- mac-live / `.bb-local-outcome.sh`: 17 jobs / 4.467161jobh / 17 input identities.
- approvals / `.bb-approvals-final-main-produce.py`: 13 jobs / 4.109827jobh / 13 input identities.
- scale-fds6 / `all_features`: 26 jobs / 2.780965jobh / 25 input identities.
- fs-concurrency / `.bb-fs-witness-stage.sh`: 29 jobs / 2.408615jobh / 29 input identities.
- plugins / `opaque_or_focused`: 28 jobs / 2.404895jobh / 28 input identities.
- approvals / `.bb-approvals-main32-clippy.py`: 14 jobs / 2.385353jobh / 14 input identities.
- main-green / `opaque_or_focused`: 16 jobs / 2.147611jobh / 16 input identities.
- plugins / `.bb-plugins-atomic-integration.sh`: 11 jobs / 2.027807jobh / 11 input identities.
- fs-concurrency / `opaque_or_focused`: 17 jobs / 1.856433jobh / 17 input identities.
- approvals / `.bb-approvals-journal-windows-producer.py`: 3 jobs / 1.412982jobh / 3 input identities.
- approvals / `.bb-approvals-create-batch-qualification.py`: 8 jobs / 1.411106jobh / 8 input identities.

Broad-suite log candidates (at least10 observed test binary labels): {'jobs': 1, 'hours': 0.03554555555555555, 'failed_jobs': 1, 'failed_hours': 0.03554555555555555}. Partial log observations do not establish full workspace scope.

Same failing test observed more than twice (overlapping changed-input investigations):

- b-runtime / `test filesystem::default_tools::tests::aggregate_custody_tests::publisher_custody_covers_its_complete_physical_allocation ... FAILED`: 3 failed jobs / 1.119688jobh; after two 0.522451jobh; 3 input identities.
- plugins / `test local::host_files::tests::every_host_observes_live_files_without_journaling_queries ... FAILED`: 3 failed jobs / 0.602285jobh; after two 0.205889jobh; 3 input identities.
- plugins / `test local::host_files::tests::workspace_projection_uses_shared_results_without_client_state ... FAILED`: 3 failed jobs / 0.602285jobh; after two 0.205889jobh; 3 input identities.
- plugins / `test local::host_owner::tests::claude_sessions_use_shared_ordered_mailboxes_and_resume_after_recreation ... FAILED`: 3 failed jobs / 0.602285jobh; after two 0.205889jobh; 3 input identities.
- plugins / `test local::host_owner::tests::codex_sessions_use_shared_ordered_mailboxes_and_resume_after_recreation ... FAILED`: 3 failed jobs / 0.602285jobh; after two 0.205889jobh; 3 input identities.
- b-runtime / `test command_is_ready_only_after_the_stock_package_is_deployed ... FAILED`: 3 failed jobs / 0.470910jobh; after two 0.183214jobh; 3 input identities.

Coverage: {'logs_read': 1565, 'bytes_read': 19957288, 'truncated_logs': 29}

- Job-hours are summed wrapper wall time, not exclusive box occupancy or CPU-hours.
- Full ledger tree plus command is necessary but insufficient proof custody; mutable external inputs, scope, platforms, toolchain and controls still require qualification.
- Script-name classes and bounded-log flags do not prove executed scope. Opaque scope is reported, never treated as redundant.
- Expected negative controls and distinct required platforms must remain; repeated failures are candidates, not automatic skips.
- Repeated-failed-test groups across changing inputs can overlap; hours after two is exposure, never an avoidable-cost sum.
- Failure after two is observed only when retained bounded diagnostics match; unknown is not success.

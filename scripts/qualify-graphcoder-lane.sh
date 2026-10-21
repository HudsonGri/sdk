#!/usr/bin/env bash
# One canonical build closure per touched crate; build time is outside E2E_WALL.
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
cargo test --locked -p acyclic-harness-core --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py command_request::diff_tests:: task_volume::approval_query::tests:: task_volume::schema::query::tests::folder_ranges_do_not_visit_unrelated_task_history native_settings::model::tests::
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py local::checkout_tools::capture::tests:: tui::render::tests::diff_errors_are_visible_and_replaceable client::input_files::shortcuts:: client::commands::session::consumption::tests:: local::compact::rewind::tests:: local::tests::compact::checkpoints_reject_stale_replay_pin_branches_and_continue_completed_tasks local::tests::headless::explicit_resume_retains_selection_and_clears_only_exit local::tests::tool_hooks::ask_guard_waits_for_exact_file_decision_before_dispatch local::tests::tool_hooks::approval_decisions_cannot_substitute_another_intent client::commands::session::approval::tests::
cargo test --locked -p acyclic-harness --features "$features" --test main --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py file_query::diffs:: client_diff::
cargo test --locked -p acyclic-harness-search --features filesystem,search-http,test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py config:: files:: tools::
cargo test --locked -p graphcoder --features "$binary_features" --bin graphcoder --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py args::tests view::diff::tests::
cargo test --locked -p graphcoder --features "$binary_features" --test tui --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py settlement_ three_prompts_
cargo test --locked -p harness-mock-model --bin harness-mock-model records::tests::
node --test scripts/graphcoder-e2e/*.test.mjs
cargo build --locked -p graphcoder -p harness-mock-model --features "$binary_features"
bun install --cwd scripts/graphcoder-pty --frozen-lockfile
E2E_OUTPUT=e2e-consolidated-results python3 - <<'PY'
import json, os, subprocess, time
start=time.perf_counter()
p=subprocess.run(['node','scripts/graphcoder-e2e/run.mjs',os.environ['CARGO_TARGET_DIR']+'/debug/graphcoder',os.environ['CARGO_TARGET_DIR']+'/debug/harness-mock-model'])
wall=time.perf_counter()-start
with open(os.environ['E2E_OUTPUT']+'/wall.json','w') as f: json.dump({'seconds':wall,'exit':p.returncode},f)
print('E2E_WALL',wall,flush=True)
# Keep strict failures in the matrix and preserve captures for classification.
PY
cargo check --locked -p graphcoder --target x86_64-pc-windows-gnu --features "$binary_features"

#!/usr/bin/env bash
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
cargo test --locked -p acyclic-harness-core --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py command_request::diff_tests::
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py client::input_files::shortcuts:: client::commands::session::consumption::tests:: local::compact::rewind::tests:: local::tests::compact::checkpoints_reject_stale_replay_pin_branches_and_continue_completed_tasks local::tests::headless::explicit_resume_retains_selection_and_clears_only_exit
cargo test --locked -p acyclic-harness --features "$features" --test file-query --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py diffs::
cargo test --locked -p acyclic-harness --features "$features" --test client-diff
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
cargo test --locked -p graphcoder --features "$binary_features" --bin graphcoder args::tests
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness-fuzz --features runtime command_parser --target x86_64-unknown-linux-gnu --dev -- -runs=128 -max_len=4096 -timeout=2

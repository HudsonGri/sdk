#!/usr/bin/env bash
# Follow-up proof after the combined lane qualification; do not rebuild unchanged targets.
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py client::commands::session::archive::settings_tests:: tui::render::tests::compaction_completion_ tui::render::tests::negative_control_error_only_
bun install --cwd scripts/graphcoder-pty --frozen-lockfile
node --test scripts/graphcoder-e2e/*.test.mjs
cargo build --locked -p graphcoder -p harness-mock-model --features "$binary_features"
RUSTFLAGS="--cfg acyclic_harness_loom --cfg acyclic_loom" cargo test --locked -p acyclic-harness --features "$features,model-checking" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py client::commands::session::scroll_models::
E2E_OUTPUT=e2e-followed-cut-results python3 - <<'PY'
import json, os, subprocess, time
start=time.perf_counter()
p=subprocess.run(['node','scripts/graphcoder-e2e/run.mjs',os.environ['CARGO_TARGET_DIR']+'/debug/graphcoder',os.environ['CARGO_TARGET_DIR']+'/debug/harness-mock-model'])
wall=time.perf_counter()-start
with open(os.environ['E2E_OUTPUT']+'/wall.json','w') as f: json.dump({'seconds':wall,'exit':p.returncode},f)
print('E2E_WALL',wall,flush=True)
PY

#!/usr/bin/env bash
set -euo pipefail
mkdir -p .tools
/usr/bin/time -f 'compile wall=%e s' cargo test --locked --no-run \
  -p graphcoder -p acyclic-plugin -p acyclic-harness -p acyclic-fs -p acyclic-test-support -p acyclic-native-runtime \
  --features acyclic-harness/test-support --lib --bin acyclic --test cli --test stack_main \
  --message-format=json > .tools/stack-budget-artifacts.jsonl
python3 - <<'PY'
from pathlib import Path
import subprocess

# The real measurement body forbids unsafe Rust. Restoring the expired-stack
# dereference must fail compilation, even if its bytes happen to remain mapped.
source = Path('rust/crates/test-support/src/stack.rs').read_text()
broken = source.replace('let mut snapshot = vec![0; BYTES];',
    'let _ = unsafe { std::ptr::read_volatile(1usize as *const u8) };\n'
    '    let mut snapshot = vec![0; BYTES];', 1)
assert broken != source
path = Path('.tools/stack-budget-negative.rs')
path.write_text(broken)
result = subprocess.run(['rustc', '--edition=2024', '--crate-type=lib', '--crate-name=stack_budget_negative',
    str(path), '-o', '.tools/stack-budget-negative.rlib'], capture_output=True, text=True)
assert result.returncode != 0 and 'usage of an `unsafe` block' in result.stderr, result.stderr
print('expired-stack dereference negative control: rejected', flush=True)
PY
python3 - <<'PY'
import json, subprocess, time
artifacts = {}
for line in open('.tools/stack-budget-artifacts.jsonl'):
    item = json.loads(line)
    if item.get('reason') == 'compiler-artifact' and item.get('executable') and item['profile']['test']:
        artifacts[item['target']['name']] = item['executable']
checks = [
    ('acyclic_test_support', ['stack::']),
    ('cli', ['main_thread::']),
    ('acyclic', ['control_plane_requests_fit_the_main_thread_budget']),
    ('acyclic_harness', ['tool::erasure_tests']),
    ('acyclic_harness', ['obs::typed::tests']),
    ('acyclic_native_runtime', ['obs::']),
    ('acyclic_fs', ['materializer::preflight::tests']),
    ('acyclic_fs', ['native_mount::stack_budget']),
]
# Each shipping entry is a separate bounded runtime suite.
checks.extend(('stack_main', [f'stack_budget::{case}', '--exact']) for case in [
    'diff', 'commit', 'tools', 'merge-continue', 'merge-abort',
    'spawn', 'send', 'wait', 'archive', 'jsonl', 'tui', 'unboxed',
])
for name, args in checks:
    started = time.monotonic()
    subprocess.run([artifacts[name], *args, '--nocapture'], check=True)
    print(f'{name} {args}: wall={time.monotonic()-started:.3f} s', flush=True)
PY
/usr/bin/time -f 'windows-gnu compile wall=%e s' cargo check --locked \
  -p graphcoder -p acyclic-plugin -p acyclic-harness -p acyclic-fs -p acyclic-test-support -p acyclic-native-runtime -p acyclic-conformance \
  --lib --tests --features acyclic-harness/test-support,acyclic-conformance/local-runner --target x86_64-pc-windows-gnu

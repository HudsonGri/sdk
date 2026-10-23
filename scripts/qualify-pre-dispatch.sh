#!/usr/bin/env bash
# Run on the build host; compile once and bound each independent proof to nine seconds.
set -euo pipefail
cargo test --locked -p acyclic-harness-core --lib --all-features --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py executor::tests::pre_dispatch:: filesystem::task_runtime::tests::model::
cargo test --locked -p acyclic-harness --lib --all-features --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py local::admission::tests::pre_dispatch:: local::tests::pre_dispatch::
cargo build --locked -p graphcoder
binary="${CARGO_TARGET_DIR:-target}/debug/graphcoder"
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch prompt
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl
if [ "${1:-}" != --negative ]; then exit 0; fi
proof=$(mktemp -d)
model=rust/crates/harness-core/src/filesystem/task_commands/model.rs
cp "$model" "$proof/model.rs"
trap 'cp "$proof/model.rs" "$model"; rm -rf "$proof"' EXIT
python3 - <<'PY'
from pathlib import Path
p=Path('rust/crates/harness-core/src/filesystem/task_commands/model.rs')
s=p.read_text()
old='''ExecutionProgress::PreDispatchFailed(failure) => ready(
                Outcome::<TurnOutput>::Failed {
                    message: failure.into_message(),
                },
                io,
            ),'''
assert s.count(old)==1
p.write_text(s.replace(old,'ExecutionProgress::PreDispatchFailed(failure) => Err(Error::Invalid(failure.into_message())),',1))
PY
cargo build --locked -p graphcoder
for mode in prompt jsonl; do
  if node scripts/qualify-final-answer.mjs "$binary" pre-dispatch "$mode" > "$proof/$mode.log" 2>&1; then
    cat "$proof/$mode.log"; echo 'negative control unexpectedly passed' >&2; exit 1
  fi
  cat "$proof/$mode.log"
  grep -Eq 'AssertionError|AbortError' "$proof/$mode.log"
  echo "NEGATIVE_PASS pre-dispatch $mode"
done
cp "$proof/model.rs" "$model"
trap 'rm -rf "$proof"' EXIT
cargo build --locked -p graphcoder
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch prompt
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl

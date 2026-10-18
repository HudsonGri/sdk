#!/bin/bash
set -euo pipefail
features=filesystem-local,native-execution,test-support
mkdir -p target/st-outcomes-pre-dispatch
build_lib() {
  local start=$SECONDS
  if ! cargo test --locked -p acyclic-harness --lib --features "$features" --no-run --message-format=json-render-diagnostics > target/st-outcomes-pre-dispatch/artifacts.json; then
    python3 - <<'PY'
import json
from pathlib import Path
for line in Path('target/st-outcomes-pre-dispatch/artifacts.json').read_text().splitlines():
    try: j=json.loads(line)
    except ValueError: continue
    if j.get('reason')=='compiler-message' and j.get('message',{}).get('level')=='error':
        print(j['message'].get('rendered', j['message']['message']))
PY
    exit 1
  fi
  echo "lib compile: $((SECONDS-start))s"
  test_bin=$(python3 - <<'PY'
import json
from pathlib import Path
for line in Path('target/st-outcomes-pre-dispatch/artifacts.json').read_text().splitlines():
    try: j=json.loads(line)
    except ValueError: continue
    if j.get('reason')=='compiler-artifact' and j.get('target',{}).get('name')=='acyclic_harness' and j.get('executable'):
        print(j['executable'])
PY
)
  test -n "$test_bin"
}
run_test() {
  local start=$SECONDS
  "$test_bin" "$1" --nocapture
  echo "test $1: $((SECONDS-start))s"
}
build_lib
run_test filesystem::task_runtime::tests::
run_test local::admission::tests::pre_dispatch::
run_test executor::tests::
run_test local::admission::poll_sweep::compaction_summary_is_readable_before_any_cancellation
run_test local::tests::compaction::defaults_leave_input_room_when_output_capacity_equals_the_context
run_test local::tests::pre_dispatch::
model=rust/crates/harness/src/filesystem/task_commands/model.rs
cp "$model" target/st-outcomes-pre-dispatch/model.positive.rs
phase=rust/crates/harness/src/executor/phase.rs
cp "$phase" target/st-outcomes-pre-dispatch/phase.positive.rs
trap 'cp target/st-outcomes-pre-dispatch/model.positive.rs "$model"; cp target/st-outcomes-pre-dispatch/phase.positive.rs "$phase"' EXIT
python3 - <<'PY'
from pathlib import Path
import re
p=Path('rust/crates/harness/src/filesystem/task_commands/model.rs')
s=p.read_text()
s,n=re.subn(r'ExecutionProgress::Failed\(failure\) => ready\(\s*Outcome::<TurnOutput>::Failed \{\s*message: failure.into_message\(\),\s*\},\s*io,\s*\)', 'ExecutionProgress::Failed(failure) => Err(Error::Invalid(failure.into_message()))', s)
assert n==1,n
p.write_text(s)
p=Path('rust/crates/harness/src/executor/phase.rs')
s=p.read_text()
assert s.count('self.0.take();')==1
p.write_text(s.replace('self.0.take();', ''))
PY
build_lib
set +e
"$test_bin" local::admission::tests::pre_dispatch:: --nocapture > target/st-outcomes-pre-dispatch/negative.log 2>&1
negative=$?
"$test_bin" executor::tests::pre_dispatch:: --nocapture > target/st-outcomes-pre-dispatch/capability-negative.log 2>&1
capability_negative=$?
"$test_bin" local::tests::pre_dispatch::http_preparation_failure_closes --nocapture > target/st-outcomes-pre-dispatch/http-negative.log 2>&1
http_negative=$?
set -e
cat target/st-outcomes-pre-dispatch/negative.log
test "$negative" = 101
cat target/st-outcomes-pre-dispatch/capability-negative.log
test "$capability_negative" = 101
cat target/st-outcomes-pre-dispatch/http-negative.log
test "$http_negative" = 101
grep -q 'preparation\|OutputZero' target/st-outcomes-pre-dispatch/negative.log
echo 'negative control: former catch-all fails settlement table'
cp target/st-outcomes-pre-dispatch/model.positive.rs "$model"
cp target/st-outcomes-pre-dispatch/phase.positive.rs "$phase"
trap - EXIT
start=$SECONDS
cargo check --locked -p acyclic-harness --lib --tests --features "$features" --target x86_64-pc-windows-gnu
echo "windows-gnu check: $((SECONDS-start))s"

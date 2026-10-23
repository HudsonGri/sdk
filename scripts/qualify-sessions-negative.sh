#!/usr/bin/env bash
# Run on bb2: one combined mutant removes each independent shipping behavior.
set -euo pipefail
proof=$(mktemp -d "$CARGO_TARGET_DIR/sessions-negative.XXXXXX")
export SESSIONS_NEGATIVE_PROOF="$proof"
restore() {
 python3 - <<'PY'
import os
from pathlib import Path
root=Path(os.environ['SESSIONS_NEGATIVE_PROOF'])
for name in (root/'paths').read_text().splitlines():
 Path(name).write_bytes((root/name).read_bytes())
PY
}
trap 'restore; rm -rf "$proof"' EXIT
python3 - <<'PY'
from pathlib import Path
import os, subprocess
root=Path(os.environ['SESSIONS_NEGATIVE_PROOF'])
paths=[
 'rust/crates/harness-providers/src/stream/mod.rs',
 'rust/crates/harness-core/src/filesystem/task_commands/model.rs',
 'rust/crates/harness/src/bin/graphcoder.rs',
 'rust/crates/harness/src/client/local_identity.rs',
 'rust/crates/harness/src/client/input_files.rs',
 'rust/crates/harness/src/local/commands.rs',
 'rust/crates/harness/src/bin/graphcoder/view.rs',
]
(root/'paths').write_text('\n'.join(paths))
for name in paths:
 saved=root/name;saved.parent.mkdir(parents=True,exist_ok=True);saved.write_bytes(Path(name).read_bytes())
def replace(name,old,new):
 p=Path(name);s=p.read_text();assert s.count(old)==1,(name,old,s.count(old));p.write_text(s.replace(old,new))
replace(paths[0],'calls.push(ModelEvent::MessageCompleted { text });','drop(text);')
replace(paths[1],'''ExecutionProgress::PreDispatchFailed(failure) => ready(
                Outcome::<TurnOutput>::Failed {
                    message: failure.into_message(),
                },
                io,
            ),''','ExecutionProgress::PreDispatchFailed(failure) => Err(Error::Invalid(failure.into_message())),')
replace(paths[2],'if let Some(provider) = search.provider()? {','if let Some(provider) = Option::<Arc<dyn acyclic_harness::search::SearchProvider>>::None {')
replace(paths[2],'client::local_identity(operation)','phase(|| client::local_identity(&local, args.prompt.as_deref(), operation)).await?')
replace(paths[2],'phase(|| client::resume_local_identity(&local, operation)).await?','return Err("retained interactive resume is unavailable".into())')
p=Path(paths[3]);s=p.read_text();start=s.index('/// Allocate a fresh submission');end=s.index('/// Keep the selected interactive',start)
old=subprocess.check_output(['git','show','5a6827c20:rust/crates/harness/src/client/local_identity.rs']).decode()
old=old[old.index('/// Reopen an unfinished'):]
s=s[:start]+old+'\n'+s[end:];s=s.replace('{FileKey, FileValue, Query}','{Component, FileKey, FileValue, Query, TaskFile}');p.write_text(s)
replace(paths[4],'return files.cancel_active().await;','return Ok(());')
replace(paths[5],'} => self.rewind_checkpoint(operation, required(target)?).await,','} => Err(Error::Unsupported("rewind is unavailable".into())),')
replace(paths[6],'        if diff_open {','        if false && diff_open {')
PY
cargo build --locked -p graphcoder
binary="$CARGO_TARGET_DIR/debug/graphcoder"
mock="$CARGO_TARGET_DIR/debug/harness-mock-model"
expect_failure() {
 local name=$1; shift
 if "$@" > "$proof/$name.log" 2>&1; then
  cat "$proof/$name.log"; echo "negative control unexpectedly passed: $name" >&2; exit 1
 fi
 cat "$proof/$name.log"
 grep -Eq 'AssertionError|AbortError|"status": "fail"' "$proof/$name.log"
 echo "NEGATIVE_PASS $name"
}
expect_failure final-prompt node scripts/qualify-final-answer.mjs "$binary" completed-only prompt
expect_failure final-jsonl node scripts/qualify-final-answer.mjs "$binary" completed-only jsonl
expect_failure pre-prompt node scripts/qualify-final-answer.mjs "$binary" pre-dispatch prompt
expect_failure pre-jsonl node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl
expect_failure fresh-retry node rust/crates/harness/tests/scenarios/headless-submissions.mjs "$binary" kill-mid-turn
for case in web-search pty-quit-resume pty-checkpoint-rewind pty-cancel-escape pty-diff-modified; do
 expect_failure "$case" env E2E_OUTPUT="$proof/$case" node scripts/graphcoder-e2e/run.mjs "$binary" "$mock" "$case"
done
restore
trap 'rm -rf "$proof"' EXIT
cargo build --locked -p graphcoder
node scripts/qualify-final-answer.mjs "$binary" completed-only prompt
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl
echo SESSIONS_NEGATIVE_CONTROLS_PASSED

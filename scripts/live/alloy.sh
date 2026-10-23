#!/usr/bin/env bash
# Coordinator-only live acceptance: credentials are supplied at run time.
set -euo pipefail
set +x
: "${ANTHROPIC_API_KEY:?supply Anthropic credentials in the environment}"
: "${CODEX_HOME:?supply the runtime Codex login directory}"
: "${ANTHROPIC_MODEL:?supply a catalog Anthropic provider/model identity}"
: "${CODEX_MODEL:?supply a catalog Codex Responses provider/model identity}"
graphcoder=${GRAPHCODER_BINARY:?supply the shipping GraphCoder executable}
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/workspace"
python3 - "$fixture/catalog.json" "$ANTHROPIC_MODEL" "$CODEX_MODEL" <<'PY'
import json,sys
json.dump({"alloys":{"live":{"models":sys.argv[2:],"select":"round_robin"}}},open(sys.argv[1],"w"))
PY
"$graphcoder" --workspace "$fixture/workspace" --catalog "$fixture/catalog.json" --model live -p \
  'Use the write tool to create alloy.txt containing live alloy, then use read to verify it, then report the content.' | tee "$fixture/output.jsonl"
python3 - "$fixture/output.jsonl" "$ANTHROPIC_MODEL" "$CODEX_MODEL" <<'PY'
import json,sys
pending=[json.loads(line) for line in open(sys.argv[1]) if line.strip()]
outcomes=[]
while pending:
    value=pending.pop()
    if isinstance(value,dict):
        if isinstance(value.get('ok'),dict):
            outcomes.append(value['ok'])
        pending.extend(value.values())
    elif isinstance(value,list):
        pending.extend(value)
    elif isinstance(value,str) and value.startswith('{'):
        try: pending.append(json.loads(value))
        except json.JSONDecodeError: pass
assert outcomes, 'missing successful execution result'
usage=outcomes[0].get('metadata',{}).get('members',{})
for member in sys.argv[2:]:
    assert member in usage, 'missing final usage for live member: '+member
    assert usage[member].get('usage',{}).get('output_tokens',0)>0, 'member produced no output: '+member
PY
test "$(cat "$fixture/workspace/alloy.txt")" = 'live alloy'

#!/usr/bin/env bash
set -euo pipefail
source .bb-client-isolate.sh
mkdir -p .bb-client-bounded-before-records
finish() { tar -czf .bb-client-bounded-before-export.tar.gz .bb-client-bounded-before-records .bb-client-bounded-before.test.ts .bb-client-bounded-before.ts .bb-client-bounded-before.sh typescript/packages/harness/src/pagination.ts; }
trap finish EXIT
export NO_COLOR=1
bun .bb-client-bounded-before.ts > .bb-client-bounded-before-records/metrics.log 2>&1
python3 - <<'PY'
from pathlib import Path
import json,subprocess,time
start=time.monotonic()
p=subprocess.run(['bun','test','./.bb-client-bounded-before.test.ts'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
seconds=time.monotonic()-start
Path('.bb-client-bounded-before-records/negative.log').write_text(p.stdout)
Path('.bb-client-bounded-before-records/result.json').write_text(json.dumps(dict(rc=p.returncode,seconds=seconds),indent=2))
print(p.stdout,flush=True)
assert p.returncode==1 and '2 fail' in p.stdout and '0 pass' in p.stdout,(p.returncode,p.stdout)
assert seconds<10,seconds
PY
printf 'CLIENT_PAGINATION_BEFORE_ADMITTED\n'

#!/usr/bin/env bash
# Run through Q:/lanes/bs.ps1 on bb2. No Cloudflare account is used.
set -euo pipefail
trap 'printf "Cloudflare qualification step at line %s failed; recovery: bash scripts/qualify-machines-cloudflare.sh\n" "$LINENO" >&2' ERR
fail() { printf '%s failed; recovery: %s\n' "$1" "$2" >&2; exit 1; }
cargo test --locked -q -p acyclic-machines -p acyclic-machines-daytona -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --lib || fail 'Machines regression tests' 'cargo test --locked -p acyclic-machines -p acyclic-machines-daytona -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --lib'
cargo build --locked -q -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --example cloudflare-conformance || fail 'Cloudflare conformance build' 'cargo build --locked -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --example cloudflare-conformance'
binary="${CARGO_TARGET_DIR:-target}/debug/examples/cloudflare-conformance"
export WRANGLER_SEND_METRICS=false
cd deploy/machines-cloudflare
npm ci --ignore-scripts --no-audit --no-fund || fail 'Worker dependency installation' 'cd deploy/machines-cloudflare && npm ci --ignore-scripts --no-audit --no-fund'
npm run check || fail 'Worker type check' 'cd deploy/machines-cloudflare && npm run check'
npx wrangler dev --local --ip 127.0.0.1 --port 8787 >/tmp/be-machines-workerd.log 2>&1 &
devpid=$!
trap 'kill "$devpid" 2>/dev/null || true' EXIT
ready=false
for attempt in $(seq 1 30); do
  if curl --silent --max-time 1 http://127.0.0.1:8787/ >/dev/null; then ready=true; break; fi
  sleep 1
done
if ! "$ready"; then cat /tmp/be-machines-workerd.log; fail 'Local workerd startup' 'bash scripts/qualify-machines-cloudflare.sh'; fi
warm=http://127.0.0.1:8787/sandboxes/acyclic-00000000-0000-0000-0000-000000000099-00000000-0000-0000-0000-000000000099-0
curl --fail --silent --show-error --max-time 10 -X DELETE "$warm" || fail 'Previous warmup Container cleanup' 'bash scripts/qualify-machines-cloudflare.sh'
curl --fail --silent --show-error --max-time 25 -X POST "$warm" -H 'content-type: application/json' --data '{"image":"workspace","env":{},"internet":false}' || { cat /tmp/be-machines-workerd.log; fail 'Native Container cold start' 'bash scripts/qualify-machines-cloudflare.sh'; }
curl --fail --silent --show-error --max-time 5 "$warm" || fail 'Native Container state read' 'bash scripts/qualify-machines-cloudflare.sh'
curl --fail --silent --show-error --max-time 10 -X DELETE "$warm" || fail 'Warmup Container cleanup' 'bash scripts/qualify-machines-cloudflare.sh'
cd ../..
/usr/bin/time -f 'Canonical Machines suite: %e seconds' timeout 55s "$binary" || { cat /tmp/be-machines-workerd.log; fail 'Cloudflare canonical Machines conformance' 'bash scripts/qualify-machines-cloudflare.sh'; }

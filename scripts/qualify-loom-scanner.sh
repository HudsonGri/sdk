#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
target="$(node -p 'require("node:path").resolve(process.env.CARGO_TARGET_DIR || "target")')"
# Only this standalone scanner is built. Workspace compiler-context qualification
# belongs to qualify-loom-contexts.sh, never the source scanner's startup path.
cargo build --locked --manifest-path rust/verification/loom-locks/Cargo.toml \
  --target-dir "$target/loom-locks"
binary="$(cargo test --locked --manifest-path rust/verification/loom-locks/Cargo.toml \
  --target-dir "$target/loom-locks" --lib --no-run --message-format=json | \
  node -e 'let s="";process.stdin.on("data",c=>s+=c);process.stdin.on("end",()=>{const xs=s.trim().split("\n").map(JSON.parse).filter(x=>x.reason==="compiler-artifact"&&x.executable&&x.target.kind.includes("lib"));if(xs.length!==1)process.exit(1);console.log(xs[0].executable)})')"
export LOOM_SOURCE_SCANNER="$target/loom-locks/debug/loom-lock-scan"
start=$(date +%s%N)
"$binary"
node --test scripts/loom-scan-records.test.mjs scripts/loom-type-contexts.test.mjs \
  scripts/check-loom-models.test.mjs scripts/check-loom-source-registry.test.mjs
elapsed=$(( $(date +%s%N) - start ))
printf 'source scanner gate: %s.%03d s (excluding scoped compilation)\n' \
  "$((elapsed / 1000000000))" "$((elapsed / 1000000 % 1000))"

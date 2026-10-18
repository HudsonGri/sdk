#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
target="$(node -p 'require("node:path").resolve(process.env.CARGO_TARGET_DIR || "target")')"
export CARGO_TARGET_DIR="$target"
sysroot="$(rustc +nightly-2026-07-01 --print sysroot)"
export LD_LIBRARY_PATH="$sysroot/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
RUSTFLAGS="-Lnative=$sysroot/lib" cargo +nightly-2026-07-01 test --locked \
  --manifest-path rust/verification/loom-types/Cargo.toml \
  --target-dir "$target/loom-types" --test spellings
python3 scripts/test-loom-type-contexts.py

#!/usr/bin/env bash
# Run every retained harness mutation target with a separate nightly target tree.
set -euo pipefail
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}/nightly-fuzz"
unset RUSTC RUSTDOC RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
# cargo-fuzz has no --locked option. Resolve under --locked first, then
# prohibit network access and verify that the shared lockfile stays identical.
cargo +nightly-2026-07-01 metadata --locked --no-deps --format-version 1 >/dev/null
lock_digest=$(sha256sum Cargo.lock)
export CARGO_NET_OFFLINE=true
mapfile -t targets < <(python3 - <<'PY'
import tomllib
with open('rust/crates/harness-fuzz/Cargo.toml', 'rb') as stream:
    for target in tomllib.load(stream)['bin']:
        print(target['name'])
PY
)
for target in "${targets[@]}"; do
  echo "FUZZ_START $target $(date -u +%FT%TZ)"
  cargo +nightly-2026-07-01 fuzz run --dev --fuzz-dir rust/crates/harness-fuzz --all-features "$target" --target x86_64-unknown-linux-gnu -- -runs=1000 -max_len=4096 -timeout=2
  test "$(sha256sum Cargo.lock)" = "$lock_digest"
  echo "FUZZ_PASS $target $(date -u +%FT%TZ)"
done

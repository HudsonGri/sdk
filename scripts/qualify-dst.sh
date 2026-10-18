#!/usr/bin/env bash
set -euo pipefail

# Build each focused target once; the artifact runner executes the binaries.
cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py testing::entropy:: testing::executor:: testing::clock:: testing::queue:: testing::durability:: testing::host::
RUSTFLAGS="--cfg acyclic_native_runtime_loom" cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py testing::entropy::models:: testing::executor::models:: testing::queue_model:: testing::durability::models:: testing::host::models::
cargo test --locked -p acyclic-harness --features filesystem-local,test-support --test dst --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py seeded_production_turn_replays_exact_bytes

cargo check --locked -p acyclic-harness --lib --tests --features filesystem-local,test-support --target x86_64-pc-windows-gnu
cargo check --locked -p acyclic-native-runtime --lib --tests --features test-support --target x86_64-pc-windows-gnu
cargo check --locked -p acyclic-stream --lib --tests --features local,test-support --target x86_64-pc-windows-gnu
cargo check --locked -p acyclic-objects --lib --tests --features local,test-support --target x86_64-pc-windows-gnu
cargo check --locked -p acyclic-fs --lib --tests --features local,test-support --target x86_64-pc-windows-gnu

#!/usr/bin/env bash
set -euo pipefail

# Build each focused target once; the artifact runner executes the binaries.
cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py testing::entropy:: testing::executor:: testing::clock:: testing::queue:: testing::durability:: testing::host::
RUSTFLAGS="--cfg acyclic_native_runtime_loom" cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py testing::model:: testing::entropy::models:: testing::executor::models:: testing::queue_model:: testing::durability::models:: testing::host::models::
cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,grpc,test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py filesystem::native_process::capture::tests::
cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,grpc,test-support --test dst --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py --suite seeded_ driver:: recovery:: manifest::

# Keep transitive native features identical across the touched packages' tests.
cargo check --locked -p acyclic-harness -p acyclic-native-runtime -p acyclic-stream -p acyclic-objects -p acyclic-fs --lib --tests \
  --features acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/grpc,acyclic-harness/test-support,acyclic-native-runtime/test-support,acyclic-stream/local,acyclic-stream/test-support,acyclic-objects/local,acyclic-objects/test-support,acyclic-fs/local,acyclic-fs/test-support \
  --target x86_64-pc-windows-gnu

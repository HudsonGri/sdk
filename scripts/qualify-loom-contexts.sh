#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

target="$(node -p 'require("node:path").resolve(process.env.CARGO_TARGET_DIR || "target")')"
export CARGO_TARGET_DIR="$target"
toolchain=nightly-2026-07-01
sysroot="$(rustc "+$toolchain" --print sysroot)"
export LD_LIBRARY_PATH="$sysroot/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# The registry includes the real Apple compilation, not a Linux substitute.
# The build-box operator supplies the licensed SDK; missing host capabilities
# are errors rather than reasons to omit a model.
sdk="${APPLE_SDK_PATH:-$target/darwin-sdk}"
test -f "$sdk/usr/include/TargetConditionals.h" || {
  echo "Apple SDK missing: set APPLE_SDK_PATH to an installed macOS SDK" >&2
  exit 1
}
command -v clang >/dev/null
command -v clang++ >/dev/null
command -v llvm-ar-18 >/dev/null
export CC_aarch64_apple_darwin=clang CXX_aarch64_apple_darwin=clang++
export AR_aarch64_apple_darwin=llvm-ar-18 CC_SHELL_ESCAPED_FLAGS=1
printf -v CFLAGS_aarch64_apple_darwin '%q ' --target=arm64-apple-macosx11.0 -isysroot "$sdk"
export CFLAGS_aarch64_apple_darwin
export CXXFLAGS_aarch64_apple_darwin="$CFLAGS_aarch64_apple_darwin"

RUSTFLAGS="-Lnative=$sysroot/lib" cargo "+$toolchain" test --locked \
  --manifest-path rust/verification/loom-types/Cargo.toml \
  --target-dir "$target/loom-types" --test spellings
python3 scripts/test-loom-type-contexts.py
RUSTFLAGS='--cfg acyclic_loom --cfg acyclic_fs_loom' \
  cargo test --locked -p acyclic-fs-fuzz --test model
cargo test --locked --manifest-path rust/verification/loom-locks/Cargo.toml \
  --target-dir "$target/loom-locks" --lib
export LOOM_SOURCE_SCANNER="$target/loom-locks/debug/loom-lock-scan"
node --test scripts/check-loom-repository.test.mjs
harness_features=terminal,tui,filesystem-local,native-execution,grpc,test-support,search-http,model-checking
model_flags='--cfg acyclic_loom --cfg acyclic_fs_loom --cfg acyclic_harness_loom'
RUSTFLAGS="$model_flags" cargo test --locked -p acyclic-harness \
  --no-default-features --features "$harness_features" --lib task_volume::files::models::
cargo check --locked --manifest-path rust/verification/loom-locks/Cargo.toml \
  --target-dir "$target/loom-locks" --lib --tests --target x86_64-pc-windows-gnu

library="$(x86_64-w64-mingw32-g++ -print-file-name=libstdc++.a)"
test -f "$library"
headers="$(dirname "$library")/include/c++"
windows_cpp_flags=(-fms-extensions -fdeclspec -isystem "$headers"
  -isystem "$headers/x86_64-w64-mingw32" -isystem "$headers/backward")
clang++ --target=x86_64-pc-windows-gnu "${windows_cpp_flags[@]}" \
  -std=c++17 -fsyntax-only -x c++ - <<'CPP'
#include <cstddef>
void probe() {}
__pragma(comment(linker,"/alternatename:foo=bar"))
auto address = __builtin_function_start(probe);
CPP
export CXX_x86_64_pc_windows_gnu=clang++
printf -v CXXFLAGS_x86_64_pc_windows_gnu '%q ' "${windows_cpp_flags[@]}"
export CXXFLAGS_x86_64_pc_windows_gnu
RUSTFLAGS='--cfg acyclic_loom --cfg acyclic_fs_loom' \
  cargo check --locked -p acyclic-fs-fuzz --test model --target x86_64-pc-windows-gnu
RUSTFLAGS="$model_flags" cargo check --locked -p acyclic-harness \
  --no-default-features --features "$harness_features" --lib --tests --target x86_64-pc-windows-gnu

CARGO_TARGET_DIR="$target/loom-source-fuzz" cargo "+$toolchain" fuzz build --dev \
  --fuzz-dir rust/verification/loom-locks/fuzz
for entry in source:863 records:864; do
  name="${entry%:*}" seed="${entry#*:}"
  "$target/loom-source-fuzz/x86_64-unknown-linux-gnu/debug/$name" \
    "rust/verification/loom-locks/fuzz/corpus/$name" -runs=1000 "-seed=$seed"
done

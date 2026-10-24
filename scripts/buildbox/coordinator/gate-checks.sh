#!/bin/bash
# One strict Windows runtime phase, used by PRs, main and lane prechecks.
gate_windows_runtime_clippy() (
  set -u
  exec 2>&1
  phase_started=$SECONDS
  target=${GATE_WINDOWS_RUNTIME_TARGET_DIR:-${CARGO_TARGET_DIR:+$CARGO_TARGET_DIR/.gate-windows-runtime}}
  target=${target:-/mnt/bb/targets/sdk/gate-windows-runtime}
  gate_output=${GATE_WINDOWS_RUNTIME_OUTPUT_DIR:-}
  if [ -z "$gate_output" ]; then
    gate_output=$(mktemp -d "${TMPDIR:-/tmp}/gate-windows-runtime.XXXXXXXX") || exit 2
  fi
  mkdir -p "$gate_output" || exit 2
  echo 'GATE_WINDOWS_RUNTIME_REQUIRED target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all'
  if [[ "$target" == /mnt/bb/targets/*/* ]]; then
    unit=${target#/mnt/bb/targets/}; unit=${unit//\//%}
    mkdir -p /mnt/bb/locks/unit || exit 2
    exec 8>>"/mnt/bb/locks/unit/$unit.lock"
    flock -s 8 || exit 2
  fi
  mkdir -p "$target" || exit 2
  exec 7>"$target/.gate.lock"
  flock -x 7 || exit 2
  trap 'touch "$target"' EXIT
  echo "GATE_WINDOWS_RUNTIME_START target_dir=$target"
  # A caller's general warning cap must not weaken this package's denied lints.
  RUSTFLAGS= CARGO_ENCODED_RUSTFLAGS= CARGO_TARGET_DIR="$target" cargo clippy --locked --target x86_64-pc-windows-gnu \
    -p acyclic-native-runtime --all-targets --all-features --message-format=short 2>&1 | tee "$gate_output/.gate-windows-runtime.out"
  status=("${PIPESTATUS[@]}")
  rc=${status[0]}
  if [ "${status[1]}" != 0 ]; then
    echo "error: could not retain Windows runtime clippy output (tee rc=${status[1]})"
    rc=2
  fi
  echo "GATE_WINDOWS_RUNTIME_CLIPPY rc=$rc seconds=$((SECONDS-phase_started)) target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all"
  exit "$rc"
)

if [ "${1:-}" = --windows-runtime-only ]; then
  [ "$#" = 1 ] || { echo 'error: --windows-runtime-only accepts no other arguments' >&2; exit 2; }
  gate_windows_runtime_clippy
  exit "$?"
fi

exec 2>&1
# The reviewed runner owns workspace, browser and feature coverage. Missing
# code is INFRA, never permission to execute a second embedded implementation.
if [ ! -f scripts/main-gate-checks.sh ]; then
  echo 'preparation-INFRA: missing reviewed scripts/main-gate-checks.sh'
  exit 224
fi
main_output=$(mktemp -d "${TMPDIR:-/tmp}/gate-main-qualification.XXXXXXXX") || exit 224
bash scripts/main-gate-checks.sh "$@" | tee "$main_output/main.out" | sed -u '/^GATE_DONE rc=/d'
main_status=("${PIPESTATUS[@]}")
main_rc=${main_status[0]}
if [ "${main_status[1]}" != 0 ] || [ "${main_status[2]}" != 0 ]; then main_rc=2; fi
mapfile -t completed < <(grep -a '^GATE_DONE rc=' "$main_output/main.out")
if [ "${#completed[@]}" != 1 ] || [ "${completed[0]:-}" != "GATE_DONE rc=$main_rc" ]; then
  echo 'error: reviewed main runner completion does not match its executor result'
  main_rc=2
fi
# The canonical runner must complete its owned phase; never attempt it again.
windows_features_rc=0
if ! grep -aq '^GATE_WINDOWS_FEATURES rc=0$' "$main_output/main.out"; then
  windows_features_rc=2
  echo 'error: reviewed main runner did not complete Windows features successfully'
fi
gate_windows_runtime_clippy; windows_rc=$?
rc=$main_rc
[ "$rc" != 0 ] || rc=$windows_features_rc
[ "$rc" != 0 ] || rc=$windows_rc
echo "GATE_MAIN_QUALIFICATION main_rc=$main_rc windows_features_rc=$windows_features_rc windows_runtime_rc=$windows_rc"
echo "GATE_DONE rc=$rc"
exit "$rc"

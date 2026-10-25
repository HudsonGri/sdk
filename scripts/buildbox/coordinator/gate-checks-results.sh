#!/bin/bash
# Installed as gate-checks-verdict.sh; this is the executable, with no alias wrapper.
# gate-checks-verdict.sh <log> <baseline main-gate log>: what gate-checks.sh found in <log> that main's run did not.
# Prints "clippy: +<n> <file>: <lint>" for every (file, lint) whose count rose, "clippy-error: ..." for new compiler errors,
# and "features: <cargo set>" for each newly failing feature set. Exit 1 if any; 0 if none. A baseline without gate-checks
# lines (older main-gate run) cannot ratchet warning counts. A required Clippy
# completion or failure diagnostics that are missing are typed INFRA (exit4).
log=$1; base=${3:-$2}; feature_base=${4:-$2}; new=0
phase_reason=$(bash "$(dirname "$0")/gate-checks.sh" --required-phases "$log"); phase_status=$?
if [ "$phase_status" != 0 ]; then
  printf '%s\n' "$phase_reason"
  exit "$phase_status"
fi
# Both runners and this public verdict share the sole diagnostic decoder.
clippy=$(python3 "$(dirname "$0")/gate-clippy.py" compare "$log" "$base" 2>&1); clippy_rc=$?
[ -z "$clippy" ] || printf '%s\n' "$clippy"
case "$clippy_rc" in
  0) :;;
  1) new=1;;
  *) exit 4;;
esac
if grep -aq '^GATE_FEATURES ' "$log"; then
  out=$(comm -13 <(grep -a '^GATE_FEATURES FAILED ' "$feature_base" 2>/dev/null | sort -u) <(grep -a '^GATE_FEATURES FAILED ' "$log" | sort -u) | sed 's/^GATE_FEATURES FAILED /features: /')
  [ -n "$out" ] && { printf '%s\n' "$out"; new=1; }
fi
exit $new

# Codex: existing CLI, native drainer avoids per-step MSYS forks.
export LC_ALL=C
S=${FLEET_ROOT:-C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx}
exec python "$S/fleet-steer.py" "$@"

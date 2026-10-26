#!/bin/bash
# Codex: sole controller replacement. Coordinator installs atomically, then restarts.
set -eu
S=${FLEET_ROOT:-C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx}
exec python "$S/fleet-supervisor.py" "$@"

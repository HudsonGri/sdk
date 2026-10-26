#!/bin/bash
# Permanent driver for the `efficiency` Codex lane: starts the lane once, then resumes it for one cycle
# every $INTERVAL seconds whenever its previous turn has ended. Run detached:
#   nohup bash Q:/lanes/efficiency/loop.sh > Q:/lanes/efficiency/loop.out 2>&1 &
# Stop it by deleting Q:/lanes/efficiency/RUN.
INTERVAL=${INTERVAL:-7200}
E=Q:/lanes/efficiency; CX=C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx
touch $E/RUN
running() { powershell -NoProfile -File $CX/lane-running.ps1 efficiency; }
cd Q:/sdk
if ! grep -aq 'session id:' $E/lane.log 2>/dev/null; then
  echo "$(date -u +%FT%TZ) start"
  codex exec -s danger-full-access -c model_reasoning_effort=high -C Q:/sdk -o $CX/efficiency.last.md - < $E/prompt.md >> $E/lane.log 2>&1
fi
while [ -f $E/RUN ]; do
  sleep $INTERVAL
  [ -f $E/RUN ] || break
  running && continue
  sid=$(grep -a -m1 'session id:' $E/lane.log | awk '{print $3}')
  echo "$(date -u +%FT%TZ) cycle $sid"
  printf 'Next cycle (%s). Re-run measure.py, re-rank findings, act, and verify earlier fixes. Read Q:/lanes/efficiency/steers/INDEX.md for which steers the coordinator applied (marked APPLIED or DECLINED).\n' "$(date -u +%FT%TZ)" \
    | codex exec -s danger-full-access -c model_reasoning_effort=high -C Q:/sdk -o $CX/efficiency.last.md resume $sid - >> $E/lane.log 2>&1
done
echo "$(date -u +%FT%TZ) stopped"

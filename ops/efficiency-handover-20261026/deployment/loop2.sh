#!/bin/bash
# Permanent driver for the `efficiency` Codex lane: resumes it for one cycle every $INTERVAL seconds, or as soon as a
# coordinator steer lands in inbox/. The cycle prompt carries every inbox file, which is then moved to inbox/delivered/.
# Run detached:
#   nohup bash Q:/lanes/efficiency/loop2.sh > Q:/lanes/efficiency/loop.out 2>&1 &
# Stop it by deleting Q:/lanes/efficiency/RUN.
INTERVAL=${INTERVAL:-1800}
E=Q:/lanes/efficiency; CX=C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx
touch $E/RUN; mkdir -p $E/inbox/delivered
running() { powershell -NoProfile -File $CX/lane-running.ps1 efficiency; }
cd Q:/sdk
last=0
while [ -f $E/RUN ]; do
  now=$(date +%s)
  if running || { [ $((now - last)) -lt $INTERVAL ] && [ -z "$(ls $E/inbox/*.md 2>/dev/null)" ]; }; then sleep 60; continue; fi
  sid=$(grep -a -m1 'session id:' $E/lane.log | awk '{print $3}')
  p=$E/inbox/.cycle.md
  { printf 'Next cycle (%s). Re-run measure.py, run your anomaly checks (and append ANOMALY lines to alerts.log), re-rank findings, act, and verify earlier fixes. Read Q:/lanes/efficiency/steers/INDEX.md for APPLIED/DECLINED decisions.\n' "$(date -u +%FT%TZ)"
    for f in $E/inbox/*.md; do [ -f "$f" ] || continue; printf '\n## Coordinator steer: %s\n\n' "$(basename $f)"; cat "$f"; done; } > $p
  for f in $E/inbox/*.md; do [ -f "$f" ] && mv "$f" $E/inbox/delivered/; done
  echo "$(date -u +%FT%TZ) cycle $sid"
  # Output goes to a file, never a pipe (a background child holding a pipe hangs the driver).
  codex exec -s danger-full-access -c model_reasoning_effort=high -C Q:/sdk -o $CX/efficiency.last.md resume $sid - < $p >> $E/lane.log 2>&1
  last=$(date +%s)
done
echo "$(date -u +%FT%TZ) stopped"

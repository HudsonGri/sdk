# steer-now.sh <lane> <steer-file>: P0 only. Queues the steer, then ends the lane's running turn so the drainer delivers
# it at once instead of after the turn. The killed turn's in-flight command is lost; the lane resumes with full context.
S=C:/Users/varun/AppData/Local/Temp/claude/Q--sdk/c4cf6543-01de-41fc-994d-e3b8096f4e8d/scratchpad/cx; l=$1
{ printf 'P0 interrupt: your previous turn was ended on purpose so this steer could be delivered now. Re-check the state of any command it was running before you continue.\n\n'; cat "$2"; } > $S/inbox/$l.p0.$$.md
bash $S/steer.sh $l $S/inbox/$l.p0.$$.md &
sleep 2
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { \$_.CommandLine -match 'codex' -and \$_.CommandLine -match 'exec' -and \$_.CommandLine -match '-o \S*$l\.last\.md' -and \$_.Name -notmatch '^(bash|sh|powershell|pwsh)\.exe$' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }"
echo "$(date -u +%FT%T.000Z) steer-now P0_INTERRUPT $l" >> ~/.codex-fleet/alerts.log

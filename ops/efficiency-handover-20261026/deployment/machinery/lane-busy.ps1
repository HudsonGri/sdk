param([Parameter(Mandatory=$true)][string]$lane, [string]$ProcessSnapshot)
# The same process/identity owner serves both probes; no second process matcher.
& "$PSScriptRoot/lane-running.ps1" -lane $lane -Mode Busy -ProcessSnapshot $ProcessSnapshot
exit $LASTEXITCODE

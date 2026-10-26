param([string[]]$stop)
$S = $PSScriptRoot -replace '\','/'
$killed = @()
foreach ($p in Get-CimInstance Win32_Process) {
  if (-not $p.CommandLine) { continue }
  $shell = $p.Name -match '^(bash|sh|powershell|pwsh)\.exe$'
  foreach ($l in $stop) {
    $isCodex = (-not $shell) -and $p.CommandLine -match 'codex' -and $p.CommandLine.Contains("scratchpad/cx/$l.last.md")
    $isDrainer = $shell -and $p.CommandLine.Contains("steer.sh $l ")
    if ($isCodex -or $isDrainer) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue; $killed += "$l($($p.ProcessId))" }
  }
}
"stopped: $($killed -join ' ')"

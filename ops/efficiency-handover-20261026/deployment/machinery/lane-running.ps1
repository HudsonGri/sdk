param([Parameter(Mandatory=$true)][string]$lane,
      [ValidateSet('Running','Busy')][string]$Mode='Running',
      [string]$ProcessSnapshot)
# Codex: one process owner.0 live,1 absent,2 unknown. Never infer DIED from an append.
# Completed Codex roots get120s to exit before the existing completed-root reap.
$ErrorActionPreference='Stop'
try {
    if ($lane -notmatch '^[a-z0-9-]+$') { throw 'invalid lane' }
    $rows=if ($ProcessSnapshot) { @(Get-Content -Raw -LiteralPath $ProcessSnapshot | ConvertFrom-Json) }
          else { @(Get-CimInstance Win32_Process) }
    $base=if ($env:FLEET_ROOT) { $env:FLEET_ROOT } else { $PSScriptRoot }
    $state=Join-Path $base 'lane-state'
    if (-not (Test-Path -LiteralPath $state)) { $null=New-Item -ItemType Directory -Path $state }
    $owners=Join-Path $state "$lane.process-owners.json"
    $last=Join-Path $base "$lane.last.md"
    $written=if (Test-Path -LiteralPath $last) { (Get-Item -LiteralPath $last).LastWriteTimeUtc } else { $null }
    $escaped=[regex]::Escape($lane)
    $roots=@($rows | Where-Object {
        $_.Name -notmatch '^(bash|sh|powershell|pwsh)\.exe$' -and
        $_.CommandLine -match 'codex' -and $_.CommandLine -match '\bexec\b' -and
        $_.CommandLine -match ('-o\s+[^\r\n]*[/\\]' + $escaped + '\.last\.md(?:["\s]|$)')
    })
    function Birth($row) { ([datetime]$row.CreationDate).ToUniversalTime().Ticks }
    $owned=@{}
    if (Test-Path -LiteralPath $owners) {
        $previous=Get-Content -Raw -LiteralPath $owners | ConvertFrom-Json
        foreach ($old in $previous) {
            $current=@($rows | Where-Object { [int]$_.ProcessId -eq [int]$old.pid })
            if ($current.Count -and (Birth $current[0]) -eq [long]$old.birth) {
                $owned[[int]$old.pid]=[long]$old.birth
            }
        }
    }
    foreach ($root in $roots) { $owned[[int]$root.ProcessId]=(Birth $root) }
    $changed=$true
    while ($changed) {
        $changed=$false
        foreach ($row in $rows) {
            $parent=[int]$row.ParentProcessId
            if ($owned.ContainsKey($parent) -and -not $owned.ContainsKey([int]$row.ProcessId) -and
                (Birth $row) -ge $owned[$parent]) {
                $owned[[int]$row.ProcessId]=(Birth $row);$changed=$true
            }
        }
    }
    foreach ($row in $rows) {
        if ($row.CommandLine -match '(bs\.ps1|build-slot|buildbox|cargo|ssh|wsl)' -and
            $row.CommandLine -match ('[/\\](?:wt|lane-scratch)[/\\]' + $escaped + '(?:[/\\"\s]|$)')) {
            $owned[[int]$row.ProcessId]=(Birth $row)
        }
    }
    $retained=@(foreach ($key in $owned.Keys) { @{pid=$key;birth=$owned[$key]} })
    $temp="$owners.$PID.tmp"
    ConvertTo-Json -Compress -InputObject @($retained) | Set-Content -LiteralPath $temp -Encoding UTF8
    Move-Item -LiteralPath $temp -Destination $owners -Force
    if ($Mode -eq 'Busy') {
        $live=@($rows | Where-Object {
            [int]$_.ProcessId -ne $PID -and $owned.ContainsKey([int]$_.ProcessId) -and
            $_.Name -notmatch '^(codex|codex-x86_64-pc-windows-msvc)\.exe$' -and
            $_.CommandLine -notmatch 'lane-(running|busy)\.ps1|lane-supervisor2\.sh|steer\.sh'
        })
        if ($live.Count) { exit 0 } else { exit 1 }
    }
    $live=@($roots | Where-Object {
        -not $written -or ([datetime]$_.CreationDate).ToUniversalTime() -gt $written -or
        ([datetime]::UtcNow-$written).TotalSeconds -le 120
    })
    if ($live.Count) { exit 0 }
    foreach ($root in $roots) {
        if (-not $ProcessSnapshot) { Stop-Process -Id $root.ProcessId -Force -ErrorAction Stop }
    }
    exit 1
} catch { if ($ProcessSnapshot) { Write-Error -ErrorAction Continue ($_.Exception.Message + ' line=' + $_.InvocationInfo.ScriptLineNumber + ' text=' + $_.InvocationInfo.Line) } else { Write-Error -ErrorAction Continue 'lane process ownership query failed' }; exit 2 }

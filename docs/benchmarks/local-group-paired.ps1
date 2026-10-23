param(
    [Parameter(Mandatory = $true)][string]$Baseline,
    [Parameter(Mandatory = $true)][string]$Candidate,
    [Parameter(Mandatory = $true)][string]$Output,
    [ValidateRange(1, 2147483647)][int]$Pairs = 5,
    [ValidateRange(1, 2147483647)][int]$Iterations = 256
)
$ErrorActionPreference = 'Stop'
foreach ($executable in @($Baseline, $Candidate)) {
    & $executable 64 1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Warmup failed: $executable" }
}
'variant,pair,order,round,writers,bytes,operations,fsyncs,ops_per_second,p50_us,p95_us,journal_bytes' | Set-Content -LiteralPath $Output
for ($pair = 0; $pair -lt $Pairs; $pair++) {
    $variants = if ($pair % 2 -eq 0) { @('baseline', 'candidate') } else { @('candidate', 'baseline') }
    $order = 0
    foreach ($variant in $variants) {
        $executable = if ($variant -eq 'baseline') { $Baseline } else { $Candidate }
        $rows = & $executable $Iterations 1
        if ($LASTEXITCODE -ne 0) { throw "Measurement failed: $variant pair $pair" }
        foreach ($row in $rows | Select-Object -Skip 1) {
            "$variant,$pair,$order,$row" | Add-Content -LiteralPath $Output
        }
        Write-Host "Completed pair $pair $variant"
        $order++
    }
}

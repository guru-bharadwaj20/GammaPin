<#
.SYNOPSIS
  Runs one stage of the H1 end-of-day forward test (forward/README.md) and pushes the result.

.DESCRIPTION
  Stages:
    fetch    Download the eve's NSE bhavcopy, participant OI and index close (free, no account).
             Refreshes the RBI 91-day T-bill series when its latest auction is over 14 days old.
    publish  fetch, then `forward_test.py predict-eod --expiry`, check the eve, verify the hash,
             commit forward/{expiry}.json and push. Must finish before 15:30 IST on expiry day;
             the pushed commit is the public timestamp.
    score    Download the expiry day's index close, `forward_test.py score --expiry`,
             commit forward/{expiry}_score.json and push.

  Everything it downloads comes from public NSE and RBI pages. Nothing is paid for.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File scripts/forward_eod.ps1 -Stage publish -Expiry 2026-11-17
  powershell -NoProfile -ExecutionPolicy Bypass -File scripts/forward_eod.ps1 -Stage score   -Expiry 2026-11-17
#>
param(
    [Parameter(Mandatory = $true)][ValidateSet("fetch", "publish", "score")][string]$Stage,
    [Parameter(Mandatory = $true)][string]$Expiry,
    # The eve: the last trading day before expiry. Defaults to the previous weekday.
    [string]$Eve = ""
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$research = Join-Path $repo "research"
$logDir = Join-Path $env:LOCALAPPDATA "GammaPin"
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = Join-Path $logDir "forward_eod.log"

function Log($msg) {
    $line = "{0:yyyy-MM-dd HH:mm:ss} [{1}] {2}" -f (Get-Date), $Stage, $msg
    Write-Host $line
    Add-Content -Path $log -Value $line -Encoding utf8
}

function Run($exe, [string[]]$argv) {
    Log ("> {0} {1}" -f $exe, ($argv -join " "))
    & $exe @argv 2>&1 | ForEach-Object { Log "  $_" }
    if ($LASTEXITCODE -ne 0) { throw "$exe exited $LASTEXITCODE" }
}

$expiryDate = [datetime]::ParseExact($Expiry, "yyyy-MM-dd", $null)
if ($Eve -eq "") {
    $d = $expiryDate.AddDays(-1)
    while ($d.DayOfWeek -eq "Saturday" -or $d.DayOfWeek -eq "Sunday") { $d = $d.AddDays(-1) }
    $Eve = $d.ToString("yyyy-MM-dd")
}

Set-Location $research
Log "repo=$repo expiry=$Expiry eve=$Eve"

function Fetch-Eve {
    Run python @("bhavcopy.py", "--start", $Eve, "--end", $Eve)
    Run python @("participant_oi.py", "--start", $Eve, "--end", $Eve)
    Run python @("settlements.py", "--start", $Eve, "--end", $Eve)

    $stamp = $expiryDate.AddDays(0).ToString("yyyyMMdd")
    $eveStamp = ([datetime]::ParseExact($Eve, "yyyy-MM-dd", $null)).ToString("yyyyMMdd")
    $bhav = Join-Path $research ("cache\bhavcopy\BhavCopy_NSE_FO_0_0_0_{0}_F_0000.csv.zip" -f $eveStamp)
    if (-not (Test-Path $bhav)) { throw "NSE has not published the $Eve bhavcopy yet ($bhav missing). Retry later." }

    # T-bill: refresh when the newest auction is more than 14 days before the eve (rates.py refuses at 21).
    $last = (Import-Csv (Join-Path $research "cache\tbill_91d.csv") | Select-Object -Last 1).date
    $age = ([datetime]::ParseExact($Eve, "yyyy-MM-dd", $null) - [datetime]::ParseExact($last, "yyyy-MM-dd", $null)).Days
    Log "latest T-bill auction $last, $age days before the eve"
    if ($age -gt 14) {
        $pages = Import-Csv (Join-Path $research "cache\tbill_rbi_pages.csv")
        $hi = ($pages | ForEach-Object { [int]$_.prid } | Measure-Object -Maximum).Maximum
        $lo = $hi + 1
        $hi = $lo + 60 * [math]::Ceiling($age / 7.0) + 120
        Run python @("tbill_rbi.py", "--lo", "$lo", "--hi", "$hi")
    }
}

switch ($Stage) {
    "fetch" { Fetch-Eve }

    "publish" {
        $out = Join-Path $repo ("forward\{0}.json" -f $Expiry)
        if (Test-Path $out) { Log "$out already exists; nothing to publish"; exit 0 }
        Fetch-Eve
        Run python @("forward_test.py", "predict-eod", "--expiry", $Expiry)
        $payload = (Get-Content $out -Raw | ConvertFrom-Json).payload
        if ($payload.eve -ne $Eve) {
            Remove-Item $out
            throw "prediction used eve $($payload.eve), not $Eve; removed, not published"
        }
        Run python @("forward_test.py", "verify", $out)
        $sha = (Get-Content $out -Raw | ConvertFrom-Json).sha256
        Set-Location $repo
        Run git @("add", $out)
        Run git @("commit", "-m", ("forward: H1 prediction for {0} (sha256 {1})" -f $Expiry, $sha.Substring(0, 16)))
        Run git @("push", "origin", "main")
        Log "published $Expiry sha256 $sha"
    }

    "score" {
        $pred = Join-Path $repo ("forward\{0}.json" -f $Expiry)
        if (-not (Test-Path $pred)) { throw "no prediction $pred to score" }
        $out = Join-Path $repo ("forward\{0}_score.json" -f $Expiry)
        if (Test-Path $out) { Log "$out already exists; nothing to score"; exit 0 }
        Run python @("settlements.py", "--start", $Expiry, "--end", $Expiry)
        Run python @("forward_test.py", "score", "--expiry", $Expiry)
        $s = Get-Content $out -Raw | ConvertFrom-Json
        Set-Location $repo
        Run git @("add", $out)
        Run git @("commit", "-m", ("forward: score {0} forward test (settlement {1})" -f $Expiry, $s.settlement))
        Run git @("push", "origin", "main")
        Log "scored $Expiry settlement $($s.settlement)"
    }
}

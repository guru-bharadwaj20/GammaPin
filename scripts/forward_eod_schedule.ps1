<#
.SYNOPSIS
  Registers three one-shot Windows scheduled tasks that run scripts/forward_eod.ps1 for one expiry.

.DESCRIPTION
  Times are local (the machine must be on IST, or adjust -PublishAt).
    fetch    eve 20:30        download the eve's NSE files (early warning if NSE is late)
    publish  expiry 07:30     predict-eod, commit, push (deadline 15:30 IST; the open is 09:15)
    score    expiry+1 20:00   score against the official close, commit, push
  Tasks run as the current user, start late if the machine was asleep, and wake it if allowed.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File scripts/forward_eod_schedule.ps1 -Expiry 2026-11-17
#>
param(
    [Parameter(Mandatory = $true)][string]$Expiry,
    [string]$Eve = "",
    [switch]$Remove
)
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$script = Join-Path $repo "scripts\forward_eod.ps1"
$expiryDate = [datetime]::ParseExact($Expiry, "yyyy-MM-dd", $null)
if ($Eve -eq "") {
    $d = $expiryDate.AddDays(-1)
    while ($d.DayOfWeek -eq "Saturday" -or $d.DayOfWeek -eq "Sunday") { $d = $d.AddDays(-1) }
    $Eve = $d.ToString("yyyy-MM-dd")
}
$eveDate = [datetime]::ParseExact($Eve, "yyyy-MM-dd", $null)

$jobs = @(
    @{ Name = "GammaPin forward fetch $Expiry";   At = $eveDate.AddHours(20).AddMinutes(30); Args = "-Stage fetch -Expiry $Expiry -Eve $Eve" },
    @{ Name = "GammaPin forward publish $Expiry"; At = $expiryDate.AddHours(7).AddMinutes(30); Args = "-Stage publish -Expiry $Expiry -Eve $Eve" },
    @{ Name = "GammaPin forward score $Expiry";   At = $expiryDate.AddDays(1).AddHours(20);    Args = "-Stage score -Expiry $Expiry" }
)
if ($Remove) {
    foreach ($j in $jobs) { Unregister-ScheduledTask -TaskName $j.Name -Confirm:$false -ErrorAction SilentlyContinue }
    "removed"; exit 0
}
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun -ExecutionTimeLimit (New-TimeSpan -Hours 2) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 20)
foreach ($j in $jobs) {
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -WorkingDirectory $repo `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$script`" $($j.Args)"
    $trigger = New-ScheduledTaskTrigger -Once -At $j.At
    Register-ScheduledTask -TaskName $j.Name -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
}
Get-ScheduledTask -TaskName "GammaPin forward*$Expiry" | ForEach-Object {
    $i = $_ | Get-ScheduledTaskInfo
    "{0}  next run {1:yyyy-MM-dd HH:mm}" -f $_.TaskName, $i.NextRunTime
}

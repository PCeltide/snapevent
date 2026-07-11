<#
.SYNOPSIS
  Adapt a match-schedule CSV into the canonical kdp capture-schedule JSONL.

.DESCRIPTION
  The canonical JSONL schedule (one event per line) is the stable contract that
  `kdp-cli capture-scheduled` consumes; THIS script is a per-tournament *adapter*
  from a source CSV into that format. Each sport's CSV layout differs, so expect
  to tweak the column mapping below per tournament — keep the OUTPUT canonical,
  let the input side be messy.

  Built for the Women's T20 World Cup 2026 CSV (columns: match_no, stage, date,
  team_1, team_2, venue, group, *_time_ist, *_time_utc, kalshi_market_code,
  notes). It:
    - derives 3-letter team codes from the team names (TEAM_CODES below);
    - uses the CSV's kalshi_market_code as `event_ticker` ONLY when it is a fully
      resolved ticker (no `<TEAMS>` placeholder and not a URL); otherwise leaves
      `event_ticker` null so the supervisor resolves it live by teams + window;
    - emits start_utc from date + start_time_utc;
    - stamps every entry with the given --RemotePrefix.
  Knockout rows with TBA teams are emitted with empty teams (resolve at fire time
  by series+window) and flagged in their label.

.EXAMPLE
  pwsh -File scripts/schedule-from-csv.ps1 `
    -Csv "C:\path\cup_2026_matches.csv" `
    -Series KXCUPMATCH -RemotePrefix remote:kdp/cup-2026 -IdPrefix cup `
    -Out deploy/schedules/cup-2026.jsonl
#>
param(
  [Parameter(Mandatory)] [string] $Csv,
  [Parameter(Mandatory)] [string] $Series,
  [Parameter(Mandatory)] [string] $RemotePrefix,
  [string] $IdPrefix = 'evt',
  [Parameter(Mandatory)] [string] $Out,
  [int] $ArmLeadMin = 60,
  [int] $MaxHours = 8
)

# Team-name -> Kalshi 3-letter code. Confirmed against live cricket tickers;
# extend (and re-verify against live tickers) for other tournaments.
$TEAM_CODES = @{
  'England'      = 'ENG'; 'Sri Lanka'   = 'SRI'; 'Ireland'    = 'IRL'
  'Scotland'     = 'SCO'; 'Australia'   = 'AUS'; 'South Africa'= 'RSA'
  'New Zealand'  = 'NZL'; 'West Indies' = 'WIN'; 'Bangladesh' = 'BAN'
  'Netherlands'  = 'NED'; 'India'       = 'IND'; 'Pakistan'   = 'PAK'
}

function Get-Code([string] $name) {
  $n = $name.Trim()
  if ($TEAM_CODES.ContainsKey($n)) { return $TEAM_CODES[$n] }
  return $null  # TBA / unknown (knockouts) -> no code
}

$rows = Import-Csv -Path $Csv
$lines = New-Object System.Collections.Generic.List[string]

foreach ($r in $rows) {
  # start_utc from date + start_time_utc (the CSV's UTC columns are HH:mm).
  $startUtc = $null
  if ($r.date -and $r.start_time_utc) {
    $startUtc = ('{0}T{1}:00Z' -f $r.date, $r.start_time_utc)
  }
  if (-not $startUtc) { Write-Warning "row $($r.match_no): no date/start_time_utc; skipping"; continue }

  $c1 = Get-Code $r.team_1
  $c2 = Get-Code $r.team_2
  $teams = @()
  if ($c1) { $teams += $c1 }
  if ($c2) { $teams += $c2 }

  # event_ticker: use the CSV code only if it's a real resolved ticker.
  $code = ($r.kalshi_market_code).Trim()
  $eventTicker = $null
  if ($code -and ($code -notmatch '<TEAMS>') -and ($code -notmatch '^https?://')) {
    $eventTicker = $code
  }

  $id = '{0}-m{1}' -f $IdPrefix, ($r.match_no.ToLower())
  $label = '{0} vs {1}' -f $r.team_1, $r.team_2
  if (-not $c1 -or -not $c2) { $label += ' (teams TBD - resolve at fire time)' }

  # Build the entry as an ordered hashtable -> compact JSON line.
  $obj = [ordered]@{
    id            = $id
    label         = $label
    series        = $Series
    event_ticker  = $eventTicker
    teams         = $teams
    start_utc     = $startUtc
    arm_lead_min  = $ArmLeadMin
    max_hours     = $MaxHours
    remote_prefix = $RemotePrefix
  }
  $lines.Add( ($obj | ConvertTo-Json -Compress -Depth 4) )
}

$dir = Split-Path -Parent $Out
if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Force $dir | Out-Null }
# Write BOM-less UTF-8: Windows PowerShell 5.1's `Set-Content -Encoding utf8`
# prepends a BOM, which corrupts the first JSONL line for a strict parser. Use
# .NET with a no-BOM UTF8Encoding so line 1 (a real, confirmed match) is never lost.
$abs = if ([System.IO.Path]::IsPathRooted($Out)) { $Out } else { Join-Path (Get-Location) $Out }
[System.IO.File]::WriteAllLines($abs, $lines, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "wrote $($lines.Count) entries -> $Out"

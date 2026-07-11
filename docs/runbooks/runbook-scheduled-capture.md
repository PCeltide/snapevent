# Runbook — scheduled event capture (`capture-scheduled`)

Capture **pre-scheduled one-off events** (sports matches with known start times)
from a declarative schedule file. The second supervisor over the shared spine
([supervisor.rs](../../crates/kdp-cli/src/supervisor.rs)); the hourly KXBTCD
supervisor is the first. **Capture + store only**, like all of kdp.

A committed example event-set lives at `deploy/schedules/example.jsonl`. Each
new tournament is a new schedule file + one env line, no new code.

## How it works

One long-lived process (`kdp-cli capture-scheduled --schedule FILE`), running as
`kdp-scheduled.service`. It:

1. **Loads** the JSONL schedule (one event/line). Blank/malformed lines are
   skipped (logged, never fatal); a leading UTF-8 BOM is stripped (so a
   BOM-prefixed file never silently drops event #1).
2. **Orders** entries by arm time and drops any whose window is already fully
   past (`start + max_hours < now`) — so a **restart mid-tournament** re-reads the
   file and re-arms only events still ahead / in-window. An entry whose session
   already has an `.archived` marker is skipped.
3. For each entry, **sleeps until its arm time** (`start_utc - arm_lead`,
   default 60 min before play — catches the toss + pre-match movement).
4. **Resolves** the event's markets — **hybrid**:
   - try the entry's predicted `event_ticker`; if its markets are live, capture
     **all** of them (both team sides);
   - else match markets whose `event_ticker`/`ticker` contains **all** the team
     codes, **order-agnostically** (so a `AAABBB` vs `BBBAAA` ticker-order guess
     doesn't matter), within a close-time window around `start_utc`.
   Retries within `--resolve-grace` (default 30 min); none found -> log + skip.
5. **Captures** L2 + trades into `<KDP_DATA_DIR>/<event_ticker>/` via the shared
   `run_capture_unit` (same `capture_session` pipeline as hourly).
6. **Settles**: polls `/markets` (windowed on the markets' real close_time) until
   every target ticker is terminal, then captures `--grace` more seconds; hard
   backstop `--max-hours` (default 8h, for rain delays / overruns).
7. **Archives** in the background: spawns `kdp-archive.sh <event> <remote_prefix>`
   (process -> curate -> verified Drive upload -> opt-in prune). Overlapping
   matches run as concurrent tasks; on SIGINT (`systemctl stop`) in-flight units
   drain + archive before exit.

## The schedule format (canonical JSONL — series-agnostic)

One entry per line:

```json
{"id":"cup-m1","label":"Alpha vs Beta","series":"KXCUPMATCH",
 "event_ticker":"KXCUPMATCH-26JUN121330AAABBB","teams":["AAA","BBB"],
 "start_utc":"2026-06-12T17:30:00Z","arm_lead_min":60,"max_hours":8,
 "remote_prefix":"remote:kdp/cup-2026"}
```

| field | required | meaning |
|---|---|---|
| `id` | yes | stable id for logs |
| `label` | no | human label for logs |
| `series` | yes | Kalshi series the event lives under |
| `event_ticker` | no | predicted ticker; `null` => resolve by `teams` |
| `teams` | no* | team codes for the discovery fallback (order-agnostic) |
| `start_utc` | yes | scheduled start of play (UTC); arm = this − lead |
| `arm_lead_min` | no | minutes before start to arm (else `--arm-lead-min`) |
| `max_hours` | no | hard backstop hours (else `--max-hours`) |
| `remote_prefix` | no | storage namespace (else env `KDP_RCLONE_REMOTE`) |

\* An entry needs **at least one** of `event_ticker` or non-empty `teams` to be
resolvable; one with neither is skipped at load (logged). Unknown JSON fields are
rejected (the line is treated as malformed) so schema drift surfaces loudly.

### Generating a schedule from a CSV

The JSONL is the stable contract; per-tournament CSV converters are throwaway
*adapters*. An example:

```powershell
pwsh -File scripts/schedule-from-csv.ps1 `
  -Csv "<source>.csv" -Series KXCUPMATCH `
  -RemotePrefix remote:kdp/cup-2026 -IdPrefix cup `
  -Out deploy/schedules/cup-2026.jsonl
```

It derives team codes from the team names, uses the CSV's `kalshi_market_code`
as `event_ticker` only when it's a fully-resolved ticker (not a `<TEAMS>`
placeholder or a URL), and writes **BOM-less** UTF-8. Each new sport will need
its column mapping / team-code table tweaked — keep the OUTPUT canonical.

## CLI flags

```
capture-scheduled --schedule FILE        (required)
                  --out DIR              (KDP_DATA_DIR; default /var/lib/kdp/data)
                  --arm-lead-min 60      (per-entry arm_lead_min overrides)
                  --max-hours 8          (per-entry max_hours overrides)
                  --grace 180            (capture this long past settlement)
                  --poll 30              (settlement/resolve poll interval, s)
                  --resolve-grace 1800   (keep retrying resolution this long, s)
                  --buffer 8192  --idle 45
                  --archive-cmd /opt/kdp/bin/kdp-archive.sh   ("" = capture only)
```

Service flags come from `/etc/kdp/kdp.env`: `KDP_SCHEDULE_FILE`,
`KDP_SCHED_ARM_LEAD_MIN`, `KDP_SCHED_MAX_HOURS`, `KDP_DATA_DIR`,
`KDP_SETTLE_GRACE`, `KDP_SETTLE_POLL`.

## Operate (server)

```bash
# 1. (re)generate + place the schedule (install.sh copies deploy/schedules/*.jsonl
#    to /etc/kdp/schedules/ without clobbering an edited copy), then point at it:
#    KDP_SCHEDULE_FILE=/etc/kdp/schedules/cup-2026.jsonl   (in /etc/kdp/kdp.env)
# 2. enable the service:
systemctl enable --now kdp-scheduled
journalctl -u kdp-scheduled -f          # watch it arm/resolve/capture
# storage lands per event-set: remote:kdp/cup-2026/<event_ticker>/{processed,raw}
```

Switch tournaments by changing the one `KDP_SCHEDULE_FILE` line (and dropping the
new file in `/etc/kdp/schedules/`). No code change.

## Caveats / things to watch

- **Knockout-stage teams are unknown** until qualification — such entries have
  empty `teams` + null `event_ticker`, so they are **not resolvable yet** and are
  skipped at load (logged, not silent). **Top up** the schedule file once teams
  are set (fill `teams`, or `event_ticker`), then restart the service.
- **A tournament Final may have no dedicated game market**: some venues run it
  in a shared category market where only the two finalists trade. That is a
  different market shape than a per-match event — do not assume the per-match
  resolver covers it.
- **Team-code table** in the converter is per-tournament; verify codes against
  live tickers for a new tournament before trusting discovery.
- **Two matches never share a close-time window** (they're hours apart), so the
  series + window filter keeps the order-agnostic team match unambiguous.

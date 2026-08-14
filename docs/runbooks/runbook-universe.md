# Runbook — declarative breadth capture (`kdp-universe@<name>`)

How to operate `capture-universe`, the breadth adapter that sweeps a set of
series and arms whatever settlement cohorts are live, instead of a fixed
per-hour schedule. **Capture + store only (ADR-003).**

## 1. What it is

One `kdp-universe@<name>` systemd instance per **universe** — a named group of
series captured under one supervisor and one Drive namespace
(`$KDP_RCLONE_REMOTE/universe-<name>`, joined by
`universe::resolve_remote_prefix`; a bare/unset remote refuses to start once
`--archive-cmd`/`--checkpoint-cmd` are configured, so a misconfigured box can't
silently archive to local disk). The env-file surface, one file per instance
at `/etc/kdp/universes/<name>.env`:

- `KDP_UNIVERSE_SERIES` — comma-separated series tickers to sweep (required).
- `KDP_UNIVERSE_EXTRA` — optional extra flags, word-split onto the command
  line (e.g. `--until 2026-07-14`, `--max-units 12`, `--clash-sub off`).

**Remote roots.** The namespace joins under the box's `KDP_RCLONE_REMOTE`. If
the base `kdp.env` remote is **series-specific** (e.g. `<remote>:<root>/btc-hourly`,
a per-series layout), the universe silently nests inside it
(`.../btc-hourly/universe-<name>/` — ETH data inside a BTC folder). The
instance env file loads *after* `kdp.env`, so override per instance: set
`KDP_RCLONE_REMOTE` to the general root in `/etc/kdp/universes/<name>.env`.
Other units keep the base value. (Found live on the 2026-08-01 rollout.)

Internally it repeats: sweep `/markets` for every configured series, group the
non-terminal markets into settlement cohorts (one per event), decide which
cohorts to arm this pass (the admission gate, §3), arm each new one as a
`CaptureUnit` over the same capture/settle/archive spine `capture-hourly` and
`capture-scheduled` use, sleep `--rediscover-interval` (default 300s), repeat.

**Windowed vs 24/7.** With no `--until`/`--for` the supervisor runs forever —
the re-sweep loop *is* the park, matching `capture-hourly`/`capture-scheduled`.
Pass `--until <RFC3339 | YYYY-MM-DD>` (a bare date is **inclusive** — the bound
is the next UTC midnight) or `--for <duration>` (resolved to an absolute bound
**at launch**, so a restart recomputes time-remaining and can never extend the
window) to run a bounded tournament/event instance instead. The two flags are
mutually exclusive.

## 2. Opt-in flow

1. **Browse the catalog** — `kdp-cli catalog` (category rollup) or `kdp-cli
   catalog --series KXBTCD` (drill into one series: live markets, volume, top
   movers).
2. **Take the suggestion** — the series drill-down ends with a ready-to-paste
   footer: an unbounded `capture-universe --series ... --name ...` command, a
   bounded `--until` variant, and the server opt-in env file:
   ```
   KDP_UNIVERSE_SERIES=<series_ticker>
   KDP_UNIVERSE_EXTRA=--until 2026-12-31
   ```
3. **Write the env file** — `/etc/kdp/universes/<name>.env` (a template lives
   at `deploy/universes/crypto.env.example`; `deploy/install.sh` installs it
   as `crypto.env.example` if none is present yet — copy and edit).
4. **Start it** — `systemctl start kdp-universe@<name>`. `enable --now` is
   for an **unwindowed** (24/7) instance only, to survive reboot. A
   **windowed** (`--until`/`--for`) instance is `start`-only — enabled and
   rebooted past its bound it relaunches as a clean no-op (a bound already
   past at launch counts as "bound reached": one loud warn, exit 0, no
   restart loop) — but that still leaves a stale enabled unit doing a no-op
   start every reboot, so `systemctl disable` it once the bound passes if it
   was ever enabled.

**Exit-code discipline (pinned).** `run_universe` returns `Ok(())` (exit 0)
**only** on an explicit shutdown signal or the `--until`/`--for` window bound
being reached — both legitimate ends. Any other termination path is an `Err`
(nonzero exit). The unit is `Restart=on-failure`, **not** `Restart=always` —
do not change this. A windowed instance is *supposed* to exit 0 and stay
stopped once its bound is reached; under `Restart=always` that legitimate exit
0 would be relaunched into an immediate restart loop, re-arming everything the
window just finished draining. This mirrors the `2026-06-28` scheduled-supervisor
post-mortem: a long-running supervisor under
`Restart=always` must have exactly one reason to exit, or any other return
becomes a restart loop by construction — there it was a 30s drain returning
`Ok(())` mid-match; here it would be the window bound doing the same thing.

## 3. Clash-slot substitution

Kalshi's higher-cadence contract owns a shared expiry: on `KXBTCD`, the hourly
whose expiry falls at 4-5PM ET (the `21:00Z` close in summer) is never listed
at all — the daily owns that slot, the weekly on Fridays. A pure hourly-shaped
capture just sees "no market this hour." `capture-universe` fills that hole:
cadence is classified by lifetime only (`open_time`..`close_time`, no ticker
parsing) — Hourly ~50-70min, Daily ~23-25h, Weekly ~6.5-7.5d, anything longer
is Long (monthly/annual). A Daily or Weekly cohort is deferred until it enters
its **final ~70 minutes** (`now >= close_time - 70min`), then armed — so you'll
see a `KXBTCD-26AUG01` daily session appear alongside the hourly ones once a
day, 4-5PM ET (weekly, Fridays). Long cadence (monthly/annual) is never armed —
skipped with a warn-once log, by design: such a cohort would hold a unit slot
for weeks or months, and its book is far too thin to be worth that.

**Opt-out:** `--clash-sub off` (default `on`). With it off, Daily/Weekly
cohorts are skipped like Long ones — the slot stays a known hole, same as a
plain hourly-only capture. Every skip/no-inferred-start-fallback is logged
**once** per event (a `warn-once` set keyed by event ticker), not every sweep.

Matches with an inferred start (from the event ticker, e.g. cricket
`KXWT20MATCH-...`) use a separate gate: deferred until `now >= start -
--arm-lead-min` (default 30 minutes) rather than the clash-slot window.

### Arm timing: the discovery window and `--arm-lead-min`

**`--status` defaults to `open,unopened`** (it was `open` alone until
2026-08-08). This is what lets a cohort be *discovered before it opens* so the
arm gate can arm it ahead of the first tick. With `open` alone a cohort was
invisible until the instant it opened, the earliest possible arm was the next
sweep boundary, and the first orderbook write landed ~4-5 min into every hourly
market's life — measured on `KXBTC-26AUG0809`: open 12:00:00Z, armed 12:01:45Z,
first write 12:04:11Z. That data is unrecoverable (ADR-003). Pass
**`--status open`** to restore the old behaviour.

Kalshi permits only **one** `status` per request; the sweep issues one request
per status, so a comma list is the right shape. Query values are *not* the
values that come back: query `unopened` returns `initialized`, `open` returns
`active`, `settled` returns `finalized`. Response values are rejected as
queries (HTTP 400).

Three gates decide arming, in this order, and they are **separate on purpose**:

| cohort shape | gate |
|---|---|
| has an inferred start (match tickers) | `now >= inferred_start - --arm-lead-min` |
| Daily/Weekly, clash-sub on | `now >= close_time - 70min` (§3) |
| Hourly / Other (every crypto cohort) | `now >= open_time - --arm-lead-min` |
| no known `open_time` | no gate — arm it (a missed cohort is unrecoverable, a late one is only late) |

Do **not** collapse the first and last rows onto `open_time`. For a match
cohort `open_time` is the *listing* date, not the start:
`KXWT20MATCH-26JUN121330SRIENG` opens 2026-06-01T00:00Z and plays
2026-06-12T17:30Z — gating it on `open_time` would arm it eleven days early and
hold a unit slot through a dead pre-match book. Two regression tests pin this.

**Sweep cost.** The unopened set is one to two orders of magnitude larger than
the open set, because Kalshi lists roughly 1.6 days of lookahead. Measured
against the live API on 2026-08-08:

| series | status | markets | pages @ `limit=1000` |
|---|---|---|---|
| `KXBTC` | open | 318 | 1 |
| `KXBTC` | unopened | 5,908 | 6 |
| `KXETHD` | open | 390 | 1 |
| `KXETHD` | unopened | 9,340 | 10 |

`sweep_series` is bounded at `MAX_PAGES = 30` (raised from 10 on 2026-08-08 —
the box measured `KXETHD` unopened at 10,840 markets / 11 pages that morning,
which the old bound *would* have truncated). Each successful sweep logs
`sweep complete` with `pages` and `markets`; hitting the cap logs a `warn`
naming the series and status. **If you ever see that warn, raise `MAX_PAGES`
or narrow the filter — markets past the cap are invisible every sweep.**

**`--min-volume` is not applied to the `unopened` sweep.** An unopened market
has traded nothing, so its volume is zero by construction; applying the filter
there would silently discard every pre-open cohort and quietly undo pre-open
arming. The `open` sweep still applies it.

**The observed lead oscillates in `[lead − rediscover_interval, lead]`, and
that is not the gate slipping.** A cohort arms on the first sweep at-or-after
`open − lead`, so the actual lead walks later by roughly one sweep-phase per
hour and then wraps. Measured over the 2026-08-09 soak (cohorts open at
:00:00Z, `--arm-lead-min 30`, `--rediscover-interval 300`):

```
21:30:29 -> open-29:31    01:32:27 -> open-27:33
22:30:58 -> open-29:02    02:32:52 -> open-27:08
23:31:27 -> open-28:33    03:33:17 -> open-26:43
00:31:59 -> open-28:01    04:33:41 -> open-26:19
                          05:34:01 -> open-25:59
```

**Steady state is 2 units per series, not 1.** With a 30-minute lead the next
cohort arms while the current one is still capturing, so two crypto series
occupy 4 of `--max-units 8` continuously. Correct behaviour, but it halves the
headroom — size any additional series against 2-per-series, not 1.

**The arm's ticker count does not grow across the open boundary.** Measured
both sides on the live API: `KXBTC` 188 and `KXETHD` 300 at open−47 while
still `initialized`, the same 188/300 at open+37 as `active`, and 188/300 is
what every `arming universe event` line records. So arming pre-open does not
freeze a partial ladder, and the never-re-subscribe design costs nothing here.
Do not compare across cadences when checking this: a clash-sub daily's ladder
is genuinely narrower (KXBTC 80, KXETHD 40), which is not a truncated hourly.

## 4. Daily checkpoints

For a long-window instance (a multi-day tournament under `--until`/`--for`),
losing the box mid-run would otherwise cost the whole in-flight day. The
supervisor's `--checkpoint-cmd` (wired to `kdp-checkpoint.sh` in the systemd
unit) runs just after each UTC day rotation and does an **additive** `rclone
copy` of the session's raw JSONL to `<prefix>/<session>/raw-inflight/`
(excluding `.done`/`.archived`) — a safety net, not the archive of record.
Local day files are **not** pruned by the checkpoint; the settle-time archive
still needs the full raw.

The authoritative path stays `kdp-archive.sh`: after the session settles, it
processes, uploads Parquet + a raw tar.gz, checksum-verifies the tar against
Drive, and **only then** purges `raw-inflight/` (`rclone purge` as the very
last step, after the raw backup is verified — see `deploy/kdp-archive.sh`).
A failed checkpoint alerts (ntfy) but never touches capture; a failed archive
verify leaves `raw-inflight/` in place, so it survives exactly the failure
it exists for. Worst case on a box loss: today's partial day, not the run.

## 5. Migrating a series onto the universe supervisor

**PINNED RULE: never run two supervisors against the same series.** A
`capture-hourly` (or `capture-scheduled`) unit and a `kdp-universe@` instance
must never both be live on one series — each would independently arm and
capture the same market, doubling WS connections and racing the settlement
watcher. This is what shapes the order below.

**Prerequisite: clear any `--max-hours` stopgap first** (see §7). A daily or
weekly cohort under a 2-hour backstop loses most of its life. Such a stopgap is
safe *only* while the universe is hourly-only — which is exactly what adding a
clash-slot series ends.

Steps, in this order:

1. **Deploy binaries** — the built `kdp-cli`/`kdp-process` release, plus the
   units/scripts, via `deploy/install.sh`.
2. **Start the universe on the series that are NOT yet supervised elsewhere** —
   set `KDP_UNIVERSE_SERIES` in `/etc/kdp/universes/<name>.env` to just those,
   leave the contended series out, `systemctl enable --now
   kdp-universe@<name>`. The old supervisor keeps its series unchanged — no
   overlap yet.
3. **Soak a few days of real archive cycles** — confirm `journalctl -u
   kdp-universe@<name>` shows clean arm/settle/archive passes and the digest
   (§6) stays green before touching the old supervisor.
4. **Stop the old supervisor in the clash-slot off-window** (i.e. *not* during
   the ~70 minutes a daily/weekly owns the shared expiry, §3 — for `KXBTCD`
   that is 4-5PM ET):
   ```bash
   systemctl stop --now kdp-hourly
   systemctl disable kdp-hourly
   ```
5. **Add the series AND raise the unit cap** — edit the env file:
   `KDP_UNIVERSE_SERIES=...,<new series>` and `KDP_UNIVERSE_EXTRA="--max-units
   12"`. Size the cap against 2 units per series, not 1 (below).
6. **Restart the universe supervisor** — `systemctl restart
   kdp-universe@<name>` to pick up the new series list, at the cheapest instant
   for the binary that is running (§7, the two-clock rule) — **`:28:00–:29:30`**
   for a 30-min-lead build. It costs one cohort per series, once, because the
   `.done` guard abandons whatever is in flight.

Picking the off-window for step 4 means there is never a moment where both
supervisors could arm the same clash slot.

### Sizing: the peak is 6, the cap is 12, and clash days are not special

Measured 2026-08-09 from real `open_time`/`close_time` per event. The three
series are structurally identical — hourly (1h, 188/300/188 strikes), daily
(25h, 80/40/80), weekly (7d1h, 50 each) — and **the long-dated contract
REPLACES that hour's hourly rather than adding to it.** There is no hourly for
the 20:00–21:00Z slot: Aug 9 runs `-0916` (19:00–20:00Z), then `-0918`
(21:00–22:00Z); `-0917` is the daily. So per series the clash window is flat at
2, never 3:

```
19:50  arm daily/weekly, hourly still live      -> 2
20:00  hourly settles                           -> 1
20:30  arm next hourly, long-dated still live   -> 2
21:00  long-dated settles                       -> 1
```

A weekly expiry takes the slot *instead of* a daily, not beside it — verified
on 2026-08-07, where `KXBTC-26AUG0717`/`KXETHD-26AUG0717` armed with
`tickers=50` (the weekly's width) and no 80-ticker daily existed. **So there is
no special clash day to schedule around**, which supersedes the earlier
"re-check the Aug 14 pinch" note.

**Peak with three series = 3 × 2 = 6.** That clears `--max-units 8`, but 2
slots is not margin worth relying on: all three series are hour-synchronized to
the same open/close instants, so the peak is exactly 3×2 with no smoothing —
every arm and every settle lands together. One unit that fails to release takes
its series to 3 and the total to 7, and the cap-reached path drops a cohort
permanently (ADR-003). Raise the cap.

**The cost objection does not survive measurement** (2026-08-09, on a small
3-core VPS): the universe supervisor was 60 MB RSS / 0.6% CPU for 4 units and
976 tickers, an hourly supervisor 42 MB / 1.4% for 1–2 units and 188 — **~15 MB
per unit**, against ~2.5 GB available and load 0.04. Twelve units is ~180 MB.
The "memory is the constraint, don't raise `--max-units`" rule of thumb was
never measured and is wrong at this scale; fd headroom is `LimitNOFILE=65536`
against ~7k fds at 12 units. Note also that migrating a series (§5) **moves**
load rather than adding it — those tickers were already being captured.

## 6. Digest

`kdp-digest.timer` (`OnCalendar=*-*-* 06:30:00 UTC`, `Persistent=true`) runs
`kdp-digest.service` (oneshot, `/opt/kdp/bin/kdp-digest.sh`) once a night. It
sweeps every `manifest.json` modified in the last `KDP_DIGEST_WINDOW_HOURS`
(default 24) under `KDP_PROC_DIR` and pushes **one** ntfy summary — session/
ticker counts, min uptime, gaps, incomplete tickers, verify mismatches,
underflows, disk free. Uptime is `1 - hole_us/span_us` per manifest, the same
clamped-hole rule as `coverage()` (§9 of the data guide); manifests without
the fields (pre-`v0.3.0`) are excluded from the uptime metric, never counted
as 0 or 1.

Knobs (both in `/etc/kdp/kdp.env`, universe-independent — one digest covers
every session/universe on the box):

- `KDP_DIGEST_UPTIME_FLOOR` (default `0.999`) — a ticker's uptime below this
  counts toward `low_uptime` and flips the push to urgent.
- `KDP_DIGEST_WINDOW_HOURS` (default `24`) — the manifest-mtime lookback.

Alert semantics:

- **All green** (no incomplete/mismatches/underflows/low-uptime tickers) —
  ntfy `Priority: low`, silent receipt ("still alive").
- **Any problem** — `Priority: urgent`.
- **Zero manifests found in the window** — `Priority: urgent` even though
  nothing looks "wrong": a dead capture/archive/timer must never read as
  silence-is-fine.

**Log monitoring caveat:** the supervisors write `tracing` output to stdout,
so journald files **every** line at info priority — `journalctl -p warning`
returns nothing even while ERROR lines sit in the log. Filter by text
(`journalctl -u kdp-universe@<name> | grep -Ei 'warn|error'`), never by
journald priority. (Bit the 2026-08-01 rollout: two ERRORs were invisible
to `-p warning`.)

## 7. Upgrade, restart, and reboot hazards

Capture is 24/7 and L2 is live-only (ADR-003), so there is **no safe window** to
schedule maintenance around. Every unplanned restart costs whatever cohorts are
in flight, permanently. Three items, in descending order of how well they are
handled.

### Picking the restart instant (the two-clock rule)

A restart during a cohort's life drains it, writes `.done`, and `plan_arms`
then skips that event **forever** on the `.done` guard. Since pre-open arming
landed, a cohort exists from `open − arm_lead`, so the cost is no longer just
the current cohort's tail — restart after the next hour's cohort has armed and
you lose that **whole** hour, on every series.

**The safe instant is: after the current cohort closes, and before the running
binary's next sweep arms the following one.** Both halves matter, and the
second one is set by the binary that is running *right now*, not the one you
are installing:

| binary in the process you are restarting | cheapest instant |
|---|---|
| pre-`2b27762` (no arm lead — arms at the first sweep after open) | **:00:00 – :01:50** |
| `2b27762` or later, `--arm-lead-min 30` | **:28:00 – :29:30** |

**Read the two rows as mirror images, not as one window that got wider.** With
no arm lead the window opens at the boundary, because nothing is armed yet and
the only thing in flight is the outgoing cohort's grace tail, which was about
to stop anyway. With a 30-minute lead there is **always** at least one cohort
in flight, so there is no free instant — only a least-bad one, and it sits at
the *end* of the interval, immediately before the next arm. At :00:30 the
cohort that opened at :00 has been capturing since :29:30 the previous hour and
has a full hour ahead of it: restarting there abandons the whole thing. At
:29:00 that same cohort has ~31 minutes left and the next one is not yet armed.
So the interval `:00–:29` contains both the most expensive instant in the hour
and the cheapest, ~2× apart — quoting the interval instead of the instant is
the mistake. Do not restart at :59 either: the next cohort armed at :30 and has
30 minutes of pre-open book to lose.

The narrow one is real, not theoretical. Restarting the universe to *install*
pre-open arming meant the running process was the old binary, whose arm times
were `14:01:48 15:01:50 16:01:51 17:01:52 18:01:53 19:01:55` — every hour at
~:01:50. A restart at :05, comfortably inside the new binary's window, would
have drained a cohort armed three minutes earlier and lost ~57 minutes of both
series. Same failure, reached from the other side.

Also check what else is in flight before choosing: at 2026-08-08T21:00:11Z the
`26AUG0816` hourlies had closed at 20:00Z and the `26AUG0817` clash-sub daily
closed at exactly 21:00:00Z, so :00:11 cost nothing — while restarting at
20:55Z would have cut that daily's last five minutes, i.e. its expiry. Arm the
restart with a transient timer rather than typing it:

```bash
# ALWAYS an absolute instant -- never a wildcard hour. Pick today's date and
# the cheapest instant for the binary that is running (table above).
sudo systemd-run --on-calendar='2026-08-13 20:28:30' --timer-property=AccuracySec=1s --unit=kdp-restart-once systemctl restart kdp-universe@crypto
```

**A wildcard hour in that calendar spec is a live landmine, not a typo.** This
doc previously carried `--on-calendar='*-*-* *:00:10'`, which restarts the
supervisor **every hour, forever**. The two forms diverge on one systemd rule:
an elapsed transient timer is unloaded only if it **cannot elapse anymore**.

| form | after it fires |
|---|---|
| `'2026-08-13 20:28:30'` (dated) | cannot elapse again → **garbage-collected**; the unit is gone, not merely inert. No cleanup step exists, and none is needed. |
| `'*-*-* *:28:30'` (wildcard hour) | can always elapse again → **stays loaded and keeps firing, hourly, forever** |

So: give it a date, and do not write a cleanup step for the dated form —
verified 2026-08-14, `list-timers` and `systemctl status` both report no such
unit after a dated one-shot completed. The check below is still worth running
before arming a new one (`--unit=` is reused), precisely because a **wildcard**
leftover is the thing that would survive to show up in it:

```bash
systemctl list-timers --all | grep kdp-restart-once
```

Absence of a timer is weak evidence on its own (the unit may simply have been
reaped). Corroborate from the other side — count the restarts that actually
happened, which a wildcard could not hide:

```bash
journalctl -u kdp-universe@crypto --no-pager -o cat | grep -c 'starting universe supervisor'
```

Three since 2026-08-08 — the three restarts that were actually issued. An
hourly wildcard running over that same span would have put the count near 140,
so it is a decisive check, not a reassuring one.

Drain itself is not a concern: at the 2026-08-08 11:46Z restart all 8 units
drained in 32 ms, every one `end=Some(Shutdown)`, `gaps=0`.

### Building on the box: cap the build, it shares RAM with capture

On a small VPS with the supervisors resident, an uncapped `-j3` release build
of the arrow crates can put the OOM killer in reach of a capture process —
which costs live L2. Cap it; the worst case is a build you retry at `-j1`:

```bash
systemd-run --scope -p MemoryMax=1500M -p MemorySwapMax=1200M cargo build --release --workspace -j 2
```

`install.sh` **restarts nothing** — it prints "restart it to pick up the new
binary" and leaves every MainPID untouched. Its `apt-get` step is also the
needrestart blacklist's live exercise; confirm no kdp unit was touched.

### ✅ The `--max-hours 2` stopgap (removed 2026-08-09)

`/etc/kdp/universes/crypto.env` carried `KDP_UNIVERSE_EXTRA="--max-hours 2"`
from 2026-08-08 11:46:45Z. It was an emergency brake: settlement detection had
never worked, so every unit held its slot to the 8-hour backstop and the 8
slots filled with finished cohorts (8/8 in flight, 2,627 cap warns). Dropping
the backstop to 2h matched the hourly cadence and freed the slots (steady 4 in
flight, 0 cap warns).

Removed after the event-scoped settlement poll was installed; `KDP_UNIVERSE_EXTRA`
is now empty and the backstop is back to the 8h default. **Kept here for the
order, which is the transferable part: install first, *then* remove the
stopgap, *then* restart.** Removing it before the new binary is in place
restores the slot exhaustion; leaving it in after truncates any non-hourly
cohort by hours.

### needrestart blacklist (applied 2026-08-08, verified)

An `unattended-upgrades` run that touches a linked library (libssl, and it
will) makes needrestart `systemctl restart` the kdp units mid-cohort. The unit
drains cleanly, writes `.done`, and archives — and `plan_arms` then skips that
event **forever** on the `.done` guard, so the rest of that cohort's life is
permanently lost. It cost KXBTCD units on 2026-08-06.

Blacklist the units and restart deliberately at deploy time instead.
`/etc/needrestart/conf.d/99-kdp.conf`, one line:

```perl
$nrconf{override_rc}{qr(^kdp-)} = 0;
```

Assign a single **key**, never the whole hash. `needrestart.conf:83` assigns
`$nrconf{override_rc} = {...}` wholesale with 55 distro entries (dbus, display
managers, networking); a conf.d file that reassigns the hash wipes all of them.
Appending one key preserves them — verified 55 → 56. The file must end in
`.conf`, and conf.d is read in sort order, so `99-` sorts last. Reverse by
deleting the file.

**`needrestart -p` alone does not prove the override works** — it only lists
services already needing a restart, and no kdp unit will. Verify by `do`ing
both config files in order and evaluating the matching rule directly.

### Reboots (OPEN — not mitigated)

The needrestart blacklist above does nothing for a reboot: a reboot stops
everything and loses every in-flight cohort, permanently (ADR-003). A pending
kernel upgrade is the usual way this arrives — needrestart flags it CRIT and it
sits there until someone acts. **A drain-then-reboot procedure does not exist
yet.** Until it is written, do not reboot without draining first, and pick the
instant by the two-clock rule above.

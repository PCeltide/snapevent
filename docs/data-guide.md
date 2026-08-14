# The kdp Data Guide

> **A from-scratch guide to the processed Kalshi data**: what it is, what every
> file and column means, how the order book works, and how to look at it quickly.
> No prior context needed. If you'll be *using* this data, read this once and
> you'll be fluent.

This guide describes the output of `kdp-process` — the compact columnar files you
get after running the processor on a raw capture. For *how the pipeline is built*,
see `phase-3-processing.md`; this doc is for the **person using the data**.

---

## Contents

1. [The big picture](#1-the-big-picture)
2. [The market model (read this first)](#2-the-market-model-read-this-first)
3. [Units & timestamps](#3-units--timestamps)
4. [The files at a glance](#4-the-files-at-a-glance)
5. [`book_events` — the lossless vault](#5-book_events--the-lossless-vault)
6. [`book_top` — the daily driver](#6-book_top--the-daily-driver)
7. [`trades` — the tape](#7-trades--the-tape)
8. [`gaps` & `raw` — the smoke detectors](#8-gaps--raw--the-smoke-detectors)
9. [`manifest.json` & drop-safety](#9-manifestjson--drop-safety)
10. [How to look at the data](#10-how-to-look-at-the-data)
11. [Recipes — things you'll actually compute](#11-recipes--things-youll-actually-compute)
12. [The lifecycle](#12-the-lifecycle)
13. [Reference card](#13-reference-card)
14. [Glossary](#14-glossary)

---

## 1. The big picture

Kalshi runs **prediction markets** — people bet real money on yes/no questions
("Will San Diego beat Washington?"). Each market has a live **order book** (who's
willing to buy/sell at what price) and a **trade tape** (what actually changed
hands).

`kdp` does two things:

1. **Capture** records the live feed verbatim to append-only text (JSONL) — paranoid
   and verbose, because L2 order-book history exists *only* live and is gone
   forever if missed.
2. **Process** (`kdp-process`) turns that raw text into compact, analysis-ready
   **columnar tables** (Parquet by default), and certifies when the raw is safe to
   delete.

You live in the **processed** side. The running example throughout this guide is a
real 5-minute capture of an MLB game market, `KXMLBGAME-26MAY291845SDWSH` (San
Diego @ Washington), which produced ~12× smaller files than the raw.

A processed capture is one folder **per ticker**:

```
data_mlb_processed/
└─ KXMLBGAME-26MAY291845SDWSH-SD/      ← one ticker = one market ("does SD win?")
   ├─ book_events.parquet     the lossless order-book log (replaces the raw)
   ├─ book_top.parquet        derived top-of-book time series   ← you'll use this most
   ├─ trades.parquet          the executed-trade tape
   ├─ gaps.parquet            detected holes (usually empty)
   ├─ raw.parquet             undecodable messages (only if any)
   ├─ verify.parquet          REST cross-check verdicts (only if sweeps ran)
   └─ manifest.json           provenance + the "safe to drop raw?" flag
```

---

## 2. The market model (read this first)

Everything else builds on this. Kalshi markets are **binary**, and the order book
has a twist worth internalizing.

**Tickers come in pairs — one per outcome.** The game above produced two markets:
`…SDWSH-SD` ("San Diego wins") and `…SDWSH-WSH` ("Washington wins"). Each is its
own market with its own book.

**Two books per ticker: `yes` and `no`.**
- A **YES** contract pays **$1 if the thing happens**, $0 otherwise.
- A **NO** contract pays **$1 if it doesn't**, $0 otherwise.
- The **`yes` book** = everyone bidding to *buy YES*. The **`no` book** = everyone
  bidding to *buy NO*.

**Price ≈ probability.** Because a YES pays $1, its price is the market's implied
probability. A best YES bid of **$0.45** means the market thinks ~**45%**.

**The $1-complement (the key trick).** There is no separate "sell YES" list. A YES
and a NO contract together always pay exactly $1, so **buying a NO at $0.54 is the
same as selling a YES at $0.46** ($1 − $0.54). Therefore:

```
best YES bid  = highest price on the YES book
best YES ask  = $1 − (highest price on the NO book)
```

In our example: best YES bid `$0.45`, best NO bid `$0.54` → **best YES ask
`$0.46`**, spread `$0.01`. **You never compute this by hand** — `book_top` already
has `yes_ask` filled in — but recognizing it explains where "ask" comes from.

---

## 3. Units & timestamps

Three conventions are used in *every* table. Learn them once.

**Money is exact integers (never floats).**
| Stored column | Meaning | Example |
|---|---|---|
| `price_micro` | price × 1,000,000 (micro-dollars) | `450000` = **$0.45** |
| `*_centi` (sizes) | contracts × 100 (centi-contracts) | `8003183` = **80,031.83 contracts** |

Storing money as integers means `$0.45` is *exactly* `450000`, never `0.44999…`.
For convenience, the derived tables **also** include ready-made dollar/contract
floats (`yes_bid`, `mid`, `count`, …) so you don't divide by hand. The integers
are the source of truth; the floats are for quick reading and plots.

**Timestamps are int64 microseconds since 1970 (UTC).** Columns ending `_us`
(`recv_ts_us`, `event_ts_us`) are big integers like `1780102617904962`. Convert
when you read:
```python
pd.to_datetime(df["recv_ts_us"], unit="us", utc=True)
```
```sql
to_timestamp(recv_ts_us / 1e6)   -- DuckDB
```

**Which clock to trust: `recv_ts_us`.** There are two timestamps. `recv_ts_us` is
when *our machine* received the message; `event_ts_us` is the exchange's own
stamp, which was measured running **~1.2 s fast**. For ordering and timing, **use
`recv_ts_us`.**

---

## 4. The files at a glance

| File | One row = | Answers | How often |
|---|---|---|---|
| **`book_top`** | one order-book event | *price / spread / depth over time* | 🟢 daily driver |
| **`trades`** | one executed trade | *what traded, when, how big* | 🟢 often |
| **`book_events`** | one price-level change | *rebuild the full book at any instant* | 🟡 when you need depth/fidelity |
| **`gaps`** | one detected hole | *did we miss data?* | ⚪ trust-check (usually empty) |
| **`raw`** | one undecodable message | *what couldn't be parsed?* | ⚪ rare (often absent) |
| **`verify`** | one REST cross-check | *did the replayed book match the venue?* | ⚪ trust-check (only if sweeps ran) |
| `manifest.json` | — | *provenance + safe to drop raw?* | the receipt |

Rule of thumb: **`book_top` for price, `trades` for fills, `gaps` to confirm it's
clean, `book_events` only when you need the full depth.**

---

## 5. `book_events` — the lossless vault

This is the table that *replaces the raw orderbook file*. It is a complete,
replayable log of the book, and it's just two kinds of rows.

**Columns:** `event_idx, recv_ts_us, event_ts_us, seq, sid, is_snapshot, side,
price_micro, qty_centi`.

**Row type 1 — snapshot (`is_snapshot = true`).** A full photo of the book at one
instant. Because a table cell can't hold a list of levels, one snapshot is
*flattened to one row per price level*, all sharing the same `event_idx`. Here
`qty_centi` is the **absolute** amount resting:

```jsonc
{event_idx:0, seq:1, is_snapshot:true,  side:"yes", price_micro:10000, qty_centi:2410100}
//  ↑ same event_idx for every row of one snapshot      $0.01            24,101.00 contracts
{event_idx:0, seq:1, is_snapshot:true,  side:"yes", price_micro:20000, qty_centi:1675510}
```
(The opening snapshot in our example was one message → **78 rows**: 27 yes + 51 no levels.)

**Row type 2 — delta (`is_snapshot = false`).** After the photo, only *changes*
arrive — one per row, each its own `event_idx`. Here `qty_centi` is a **signed
change**:

```jsonc
{event_idx:1, seq:3, is_snapshot:false, side:"yes", price_micro:420000, qty_centi: 200000}  // +2,000.00 added
{event_idx:3, seq:5, is_snapshot:false, side:"yes", price_micro:410000, qty_centi:-202800}  // -2,028.00 removed
```
**Positive = liquidity added, negative = removed.** When a level's running total
hits 0, that price disappears.

**Replay (why this is lossless).** Snapshot + every delta = the exact book at any
instant. The rule:
```
start empty
for each row in order:
    is_snapshot row → set level = qty_centi        (lay down the photo)
    delta row       → level += qty_centi; drop the level if it reaches 0
```
Play it forward and at any moment you have the complete ladder — full depth, every
price. Nothing was summarized away. That's why the raw orderbook JSONL can be
deleted: these rows hold 100% of the same information, much smaller.

> **Two gotchas, both normal:**
> - **`seq` skips numbers** (1, 3, 4, 5…). The sequence counter is *shared* across
>   the trade feed and the other ticker, so any single file looks "gappy."
>   **Never use `seq` jumps to detect data loss** — the `gaps` table is the
>   authoritative signal.
> - **`event_idx`** groups one message's rows *and* is the join key to `book_top`
>   (each `book_top` row has the same `event_idx`).

---

## 6. `book_top` — the daily driver

The processor already did the replay above and, at **every order-book event**,
wrote down the top of book + aggregates. One row per order-book message. This is
the table you'll open 90% of the time.

**Columns** (integers are truth; the last three are convenience dollars):

| Column | Meaning |
|---|---|
| `event_idx` | message ordinal; joins to `book_events` |
| `recv_ts_us`, `event_ts_us` | timestamps (µs); use `recv_ts_us` |
| `seq` | wire sequence number |
| `event` | `"snapshot"` or `"delta"` (what caused this row) |
| `yes_bid_micro`, `yes_bid_sz_centi` | best YES bid price + size |
| `yes_ask_micro`, `yes_ask_sz_centi` | best YES ask (= $1 − best NO bid) + size |
| `mid_micro`, `spread_micro` | mid price, ask − bid |
| `yes_total_centi`, `no_total_centi` | total resting size on each book (depth) |
| `yes_levels`, `no_levels` | number of price levels on each book |
| `imbalance` | `(yes_total − no_total) / (yes_total + no_total)`, in [−1, 1] |
| `yes_bid`, `yes_ask`, `mid` | the same prices, in **dollars** (convenience) |

**A real opening row, decoded:**
```jsonc
{event:"snapshot",
 yes_bid_micro:450000, yes_ask_micro:460000, mid_micro:455000, spread_micro:10000,
 yes_bid_sz_centi:8003183, yes_ask_sz_centi:8427534,
 yes_total_centi:40573590, no_total_centi:53135934, yes_levels:27, no_levels:51,
 imbalance:-0.134, yes_bid:0.45, yes_ask:0.46, mid:0.455}
```
Reads as: *bid **$0.45** (80,031.83 size) / ask **$0.46** (84,275.34) / mid
**$0.455** / spread **1¢**; 405,735.90 contracts resting on YES vs 531,359.34 on
NO (`imbalance −0.134`, i.e. slightly more size queued on NO).*

`imbalance` is a quick order-book pressure gauge: negative = heavier NO side,
positive = heavier YES side.

---

## 7. `trades` — the tape

What actually changed hands. One row per executed trade.

**Columns:** `recv_ts_us, event_ts_us, seq, price_micro, count_centi, yes_price,
count, taker_side, taker_book_side, trade_id`.

| Column | Meaning |
|---|---|
| `price_micro` / `yes_price` | trade price (micro-dollars / dollars) |
| `count_centi` / `count` | size (centi-contracts / contracts) |
| `taker_side` | which outcome the aggressor took (`"yes"`/`"no"`) |
| `taker_book_side` | which book side was hit (`"bid"`/`"ask"`), when reported |
| `trade_id` | Kalshi's unique trade id (dedup key) |

**A real first fill:**
```jsonc
{price_micro:460000, yes_price:0.46, count_centi:57605, count:576.05,
 taker_side:"yes", taker_book_side:"bid", trade_id:"9090fb7d-…"}
```
*An aggressive buyer took **576.05 YES contracts at $0.46*** — i.e. they lifted
exactly the $0.46 ask we saw in `book_top`. (The book and the tape agree, a good
sanity check.)

> Trades come from two sources: the **live WebSocket** feed (during capture) and
> the **REST backfill** (the historical tape, after the match). The REST tape is
> the authoritative full history for a finalized market; live trades may not carry
> every optional field. `trade_id` lets you dedup across the two.

---

## 8. `gaps` & `raw` — the smoke detectors

Usually silent. When they're not, look before trusting the rest.

**`gaps`** — one row per detected hole in the stream (a sequence jump, a
reconnect, a deliberate resubscribe, or a `verify_mismatch` — see below).
Columns: `recv_ts_us, seq, reason, channel, last_seq, observed_seq, detail`.
**This — not `seq` continuity — is the real "did we lose data" signal.** An
empty `gaps` table means the capture is whole.

**`raw`** — one row per message the parser *couldn't* decode at capture time,
preserved verbatim so nothing is ever silently dropped. Columns: `recv_ts_us, seq,
channel, raw_type, ticker, error, payload_json` (`payload_json` is the original
message). **This file only exists if there were any.** If present, review it before
deleting the raw.

**`verify`** — one row per REST cross-check. During capture the tool
periodically fetches the venue's *own* REST order book (`--verify-interval`,
default every 15 min; `0` disables) and stores it inline as an observation;
at processing time each observation is diffed against the book replayed from
the captured stream. This catches what sequence tracking structurally cannot:
a venue-side emission bug (a delta never sent under a continuous `seq`) or a
decode/replay bug on our side. Columns: `recv_ts_us, outcome, match_lag_us,
yes_levels_json, no_levels_json, detail` — the REST observation itself is
preserved losslessly as integer `[[price_micro, qty_centi], …]` pairs.
Outcomes:

| `outcome` | Meaning |
|---|---|
| `matched` | the replayed book equals the venue's book at some instant within a ±5 s tolerance window (`match_lag_us` = signed offset to the matching instant — absorbs deltas in flight between the REST fetch and the WS stream) |
| `mismatch` | no instant in the window matched — the books genuinely diverged; a `gaps` row with `reason: "verify_mismatch"` is also written (the book is suspect from that instant until the next real snapshot re-anchors it) and the processor logs a warning |
| `skipped_gap` | the book was already suspect from an unresolved gap, so no verdict was rendered (a mismatch here would be a lie) |
| `truncated` | the capture ended inside the check's tolerance window; verdict withheld |

**This file only exists if verify sweeps ran.** Old captures (before the sweep
existed) simply have no `verify` table and `null` verification columns in
`coverage()` — absence of a check is never presented as a passing one.

---

## 9. `manifest.json` & drop-safety

Every ticker gets a `manifest.json` — the receipt. It records provenance (source
files, time span), per-table row counts, and the all-important verdict:

```jsonc
{ "counts": { "book_events": 8305, "book_top": 8228, "trades": 400, "gaps": 0, "raw": 0, "verify": 4 },
  "read_errors": 0,
  "complete": true,
  "verify_checks": 4, "verify_mismatches": 0, "verify_skipped": 0,
  "underflows": 0,
  "notes": ["all source records decoded into the structured tables; the raw capture files are safe to drop once these outputs are verified"] }
```

**`tool_version` identifies the BUILD, not the commit.** It is
`env!("CARGO_PKG_VERSION")`, resolved when the binary is compiled — so every
build made between one release and the next stamps the *older* version string.
That is fine while a version's behaviour is uniform, and misleading the moment
it is not: deploy a fix that changes what a field *means* before the version
bump lands, and both the pre-fix and post-fix outputs carry the same
`tool_version`, with no way to tell them apart from inside the dataset. Two
consequences worth knowing:

- If a value looks wrong for a manifest's stated version, check the
  `CHANGELOG` entry *after* that version too — the producing build may predate
  the bump.
- **Re-processing rewrites `tool_version` with the re-running binary's own
  value.** So repairing old outputs with a fixed-but-not-yet-bumped build
  stamps them with the same ambiguous string as the corrupt ones. Bump and
  rebuild first, then re-process.

**`complete: true` is the green light to delete the raw JSONL.** It means:

```
complete  =  (read_errors == 0)  AND  (raw count == 0)
```

i.e. every line decoded cleanly *and* nothing was left only as a raw fallback — so
the structured tables are the whole story. If either is non-zero, `complete` is
`false`: the data is still preserved (unreadable lines → `read_errors.jsonl`, raw
fallbacks → `raw.parquet`), but you should review before deleting, because the
structured tables alone aren't the complete picture.

**The verification fields are a separate trust axis.** `verify_checks` /
`verify_mismatches` / `verify_skipped` summarize the `verify` table (REST
cross-checks, §8), and `underflows` counts deltas that drove a price level
*strictly below zero* during replay (a consistency signal — a legitimate full
cancel lands exactly on zero and does not count). **None of these affect
`complete`**: `complete` certifies that the structured tables faithfully
represent *what was captured* (capture→table); the verification fields speak
to whether what was captured matches *what the venue had* (capture→venue). A
capture can be `complete: true` and still carry a mismatch — both facts are
reported, neither is hidden in the other. Manifests written before these
fields existed simply lack them (readers surface `null`, never a fake `0`).

**`span_us` / `hole_us` — capture span and uptime.** `span_us` is the
`book_events` `recv_ts_us` extent (a directory with no book events falls back
to the `trades` `event_ts_us` extent; neither present -> `null`); `hole_us` is
the unioned, span-clamped hole time from the `gaps` table -- the same rule
`kdp-data`'s `coverage()` uses, so the manifest stamp and the Python accounting
can never disagree about the same directory. Uptime = `1 - hole_us / span_us`.
A few things worth knowing before you use these fields:

- `span_us` is deliberately **not** `last_recv_ts_us - first_recv_ts_us`.
  Those two neighbor fields fold in *every* record -- gap markers, verify rows,
  anything with a `recv_ts` -- while `span_us` is scoped to book events (or the
  trades fallback) only, so the two can legitimately disagree.
- Trade-channel gaps count toward `hole_us`, for parity with `kdp-data`
  `coverage()` -- this is asymmetric with the verify sweep, which gates only on
  orderbook-channel gaps (a trade-tape hole says nothing about book fidelity).
- Three distinct "no value" states, don't conflate them: `hole_us: null` means
  no book events exist (unknown uptime, never a fake `0`); `span_us: 0` is a
  real, legitimate value (a single-instant book) -- guard the divide; the keys
  **absent entirely** mean a pre-`v0.3.0` manifest (absent is not the same as
  `0`). The jq uptime recipe below already carries both guards:
  ```sh
  jq 'select(.span_us > 0 and .hole_us != null) | 1 - .hole_us / .span_us' manifest.json
  ```

> **The golden rule:** delete a raw capture **only** when its `manifest.complete`
> is `true` and you've verified the output. The lossless `book_events` table is
> what makes that safe.

---

## 10. How to look at the data

Four ways, from "right now, zero installs" to "full analysis." Pick by need.

### a) Quick peek — built into the tool, no dependencies
The processor can print any table's schema + first rows as JSON. Great for a
30-second sanity check on any machine:
```sh
kdp-process --head data_mlb_processed/<TICKER>/book_top.parquet --rows 20
# works on Parquet and Feather; --rows defaults to 5
```

### b) Visually in VS Code — Data Wrangler / a Parquet viewer
Parquet is a binary columnar format, so you can't just open it in a text editor —
use a grid viewer:
- **Data Wrangler** (Microsoft extension): right-click the `.parquet` →
  *Open in Data Wrangler* for a spreadsheet-style grid with sortable columns,
  filters, and quick stats.
- Or a lightweight **"Parquet Viewer"** extension for a fast read-only grid.

One thing that confuses everyone the first time: the **timestamp columns show as
giant integers** (`1780102617904962`) because they're stored as int64 microseconds
(§3). That's expected — the column is `recv_ts_us` in *microseconds since 1970*.
In Data Wrangler you can add a derived column converting it to a datetime, or just
remember the unit. Everything else (prices, sizes, the `yes_bid`/`mid` dollar
columns) reads naturally.

### c) DuckDB — SQL straight on Parquet (single binary, no Python)
The best ad-hoc tool. Point SQL at the files directly:
```sql
-- price & spread over time
SELECT to_timestamp(recv_ts_us/1e6) AS t, yes_bid, yes_ask, mid, spread_micro
FROM 'data_mlb_processed/…-SD/book_top.parquet'
ORDER BY t;

-- the ten biggest trades
SELECT to_timestamp(recv_ts_us/1e6) AS t, yes_price, count, taker_side
FROM 'data_mlb_processed/…-SD/trades.parquet'
ORDER BY count DESC LIMIT 10;
```

### d) pandas / polars — notebooks & plots
Two read-time rituals and you're free (§3): convert `*_us` to datetime, and use
the float dollar columns (or divide the integers yourself).
```python
import pandas as pd
bt = pd.read_parquet("data_mlb_processed/…-SD/book_top.parquet")
bt["t"] = pd.to_datetime(bt["recv_ts_us"], unit="us", utc=True)
bt.plot(x="t", y="mid")                      # price over time

tr = pd.read_parquet("data_mlb_processed/…-SD/trades.parquet")
tr.nlargest(10, "count")[["yes_price", "count", "taker_side"]]
```
```python
import polars as pl
bt = pl.read_parquet("…/book_top.parquet").with_columns(
    pl.from_epoch("recv_ts_us", time_unit="us").alias("t"))
```

---

## 11. Recipes — things you'll actually compute

| You want… | Use | How |
|---|---|---|
| Price over time | `book_top` | plot `mid` (or `yes_bid`/`yes_ask`) vs `recv_ts_us` |
| Spread / tightness | `book_top` | `spread_micro` over time |
| Liquidity / depth | `book_top` | `yes_total_centi`, `no_total_centi`, `*_levels` |
| Order-book pressure | `book_top` | `imbalance` (−1 NO-heavy … +1 YES-heavy) |
| Biggest / latest trades | `trades` | sort by `count` desc, or by `recv_ts_us` |
| Buy vs sell pressure | `trades` | group by `taker_side` |
| Full depth at an instant | `book_events` | replay up to a timestamp (§5), inspect the ladder |
| "Book when this trade hit" | `trades` + `book_top` | as-of join on `recv_ts_us` |
| Snapshot ↔ summary | `book_events` + `book_top` | join on `event_idx` |

---

## 12. The lifecycle

```
1. capture    kdp-cli capture  --tickers <...> --out data/     # live, during the event
2. backfill   kdp-cli backfill --series  <...> --out data/     # the trade tape, after
3. process    kdp-process --in data/                           # -> data_processed/
4. verify     open each manifest.json  →  "complete": true
5. drop raw   delete data/   (the Parquet is now the source of truth)
```
Step 4 is the safety latch. Everything in this guide exists to make that one line
trustworthy.

**The 4-5PM ET clash slot.** On contracts like `KXBTCD`, Kalshi's
higher-cadence contract owns a shared expiry: the hourly whose close falls at
4-5PM ET is never listed at all (the daily owns it; the weekly on Fridays;
monthly/annual are skipped rather than substituted). `capture-universe`
substitutes the owning contract's expiry-hour session in that hour's place, so
a `KXBTCD-26AUG01` daily session shows up once a day alongside the hourly
ones. An hourly-only capture (`capture-hourly`, no clash-sub) has no
substitute to arm, so that slot correctly shows up as a known hole — not a bug
(see `docs/runbooks/runbook-universe.md` §3).

**Loading it back (Rust): `kdp-load`.** For replay consumers (backtests), the
`crates/kdp-load` library turns a processed ticker directory into a single
deterministic, time-ordered, TYPED event stream — snapshots grouped, integer
units, gaps in-band, explicit WS/REST trade provenance, and `between(t0, t1)`
with a synthetic opening book at `t0`. It owns the ordering rule this guide
implies (WS rows by `recv_ts_us`; REST-backfilled trades by `event_ts_us` —
never the backfill's fetch time), so consumers don't re-derive it. Try it
against the committed fixture, no data needed:
`cargo run -p kdp-load --example replay_tour` (open → verdict → stream counts
→ full-depth ladder at an instant). One caveat worth knowing: REST `verify`
observations are not replay events, so on a settled market the stream's last
timestamp can trail the manifest's `last_recv_ts_us` by up to one verify
interval — take "capture end" from the manifest, never from
`events().last()`. Full contract: the crate rustdoc
(`cargo doc -p kdp-load --open`). The same replay is callable **from
Python** via the `kdp_load` bindings (`crates/kdp-load-py`, built with
`scripts/check-load-py.ps1`) — see `python/README.md` "Full-depth replay".

**Loading it back (Python): `kdp-data`.** For tabular analysis (Polars), the
`python/` package indexes any local tree of processed dirs
(`DatasetIndex.build(root)` — completeness verdict per entry, event date from
the ticker, day tarballs listed lazily with `extract_day_tars` on demand) and
loads per-table frames (`load_trades` / `load_book_top`; lists concat with a
`source_path` column). It mirrors the honesty gates (incomplete dirs raise
unless `allow_incomplete=True`; newer schema versions are refused) but
deliberately does NO ordering/dedup/replay — a mixed WS+REST directory's
trades frame contains both copies of a print; use `kdp-load` for the
canonical stream. See `python/README.md`.

**Trusting it (Python): `coverage` / `holes`.** Before building on a dataset,
account for its holes: `coverage(entries)` returns one row per directory —
capture span, unioned hole time (each `gaps` row runs to the first
re-anchoring snapshot after it), `uptime`, unresolved-gap count, the
manifest's `complete`/`reasons`, and the verification stats
(`verify_checks`/`verify_mismatches`/`underflows` — `null` for directories
processed before those existed) as columns (coverage reads incomplete dirs by
design; it is the reporting side of the honesty gate). `holes(entry)` gives
the per-gap windows — a `verify_mismatch` row opens a hole like any other gap
and closes at the next real snapshot. This is the dataset-level answer to
"where are the holes and what fraction of the window is trustworthy."

---

## 13. Reference card

**Market:** two books per ticker (`yes`/`no`); price = probability; **best YES ask
= $1 − best NO bid**.

**Units:** `price_micro ÷ 1e6 = $`; `*_centi ÷ 100 = contracts`; convenience float
columns are pre-divided. Timestamps are **int64 µs** → `to_datetime(col, unit="us",
utc=True)`. **Trust `recv_ts_us`** (exchange `event_ts_us` runs ~1.2 s fast).
`seq` skips numbers in one file — normal; `gaps` is the real loss signal.

**Files:** `book_top` (price over time) · `trades` (fills) · `book_events`
(lossless log → full depth via replay) · `gaps`/`raw`/`verify` (smoke
detectors) · `manifest.json` (`complete: true` ⇒ safe to drop raw;
`verify_mismatches`/`underflows` = capture-to-venue trust, separate axis).

**`book_events` replay:** empty → `is_snapshot` row *sets* a level (absolute) →
delta row `+= qty_centi` (signed; drop at 0). `event_idx` groups a message and
joins to `book_top`.

**`span_us`/`hole_us` (uptime):** `manifest.json` fields — `span_us` = the
`book_events` extent (trades-fallback for trade-only dirs), `hole_us` = unioned
span-clamped hole time (same rule as `coverage()`); uptime = `1 - hole_us /
span_us`. `hole_us: null` = no book events; `span_us: 0` is a real value
(guard the divide); keys **absent** = pre-`v0.3.0` manifest.

**Clash-slot substitution:** `capture-universe` arms the owning daily/weekly
contract in place of an hourly whose expiry the higher-cadence contract owns
(`--clash-sub on`, default); Long cadence (monthly/annual) is always skipped.

**Peek anywhere:** `kdp-process --head <file>.parquet --rows 20`.

---

## 14. Glossary

- **L2 / order book** — the full ladder of resting bids on each side, with sizes
  (vs L1 = best bid/ask only).
- **Snapshot** — a full photo of the book at one instant.
- **Delta** — an incremental change to one price level (signed size change).
- **YES / NO book** — the two sides of a binary market (buyers of YES vs NO).
- **bid / ask** — best price to buy (bid) vs sell (ask); ask is derived via the
  $1-complement.
- **mid** — midpoint of bid and ask.
- **spread** — ask − bid (market tightness).
- **imbalance** — relative resting size, YES vs NO.
- **taker** — the aggressor who crosses the spread to trade now.
- **micro-dollars / centi-contracts** — the integer units (× 1e6 / × 100) that
  keep money exact.
- **`event_idx`** — per-message ordinal; groups `book_events` rows and joins to
  `book_top`.
- **`seq` / `sid`** — the wire sequence number and subscription id.
- **gap** — a recorded hole in the stream (sequence jump / reconnect /
  resubscribe / verify mismatch).
- **raw fallback** — a message the parser couldn't decode, preserved verbatim.
- **verify check** — a REST order-book observation taken during capture and
  diffed offline against the replayed book (±5 s tolerance window); the
  independent cross-check that seq tracking can't provide.
- **underflow** — a delta that drove a price level strictly below zero during
  replay; counted in the manifest as a consistency signal.
- **drop-safe / `complete`** — the manifest's certification that the raw JSONL can
  be deleted.

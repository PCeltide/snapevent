"""Dataset-level coverage accounting: capture spans, gap holes, uptime.

Coverage deliberately does NOT apply the R3 incomplete gate: it is the tool
that REPORTS trustworthiness, so it reads incomplete directories too and
surfaces ``complete``/``reasons`` as columns instead. R8 still applies (an
unsupported ``schema_version`` is refused by ``read_entry``). A hole opens at
each ``gaps`` row and closes at the first re-anchoring snapshot at-or-after
it — the same re-anchor rule the Rust ``kdp-load`` replayer uses. No dedup,
no ordering, no replay here (kdp-load is the canonical event stream).
"""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Sequence
from pathlib import Path

import polars as pl

from kdp_data.errors import KdpDataError, MissingTable
from kdp_data.index import Entry
from kdp_data.loaders import _as_entry, _Target

_HOLES_SCHEMA = {
    "recv_ts_us": pl.Int64,
    "reason": pl.String,
    "channel": pl.String,
    "detail": pl.String,
    "hole_end_us": pl.Int64,
    "hole_us": pl.Int64,
    "resolved": pl.Boolean,
}


def _read_table(entry: Entry, table: str, columns: list[str]) -> pl.DataFrame | None:
    """The table's needed columns; None if the file is absent, typed on failure."""
    file = entry.path / f"{table}.parquet"
    if not file.exists():
        return None
    try:
        return pl.read_parquet(file, columns=columns)
    except Exception as exc:
        raise KdpDataError(f"{file}: unreadable table: {exc}") from exc


def _gaps(entry: Entry, require: bool) -> pl.DataFrame | None:
    gaps = _read_table(entry, "gaps", ["recv_ts_us", "reason", "channel", "detail"])
    if gaps is None and require:
        # An L2 capture without its gap table cannot be accounted for — typed,
        # never "assume no gaps" (mirrors kdp-load's reader rule).
        raise MissingTable(entry.path, "gaps")
    return gaps


def _gap_windows(
    gaps: pl.DataFrame, snap_ts: list[int], span_end: int | None
) -> list[tuple[int, int | None, bool]]:
    """Per gap: (start, end, resolved). End = first snapshot at-or-after the
    gap, else the span end (unresolved). End is None when the window is
    unmeasurable: no span at all, or the gap opened past the last book row
    (a capture that died inside the hole) — never a negative window."""
    out: list[tuple[int, int | None, bool]] = []
    for start in gaps.get_column("recv_ts_us").to_list():
        i = bisect_left(snap_ts, start)
        if i < len(snap_ts):
            out.append((start, snap_ts[i], True))
        else:
            end = None if span_end is None or span_end < start else span_end
            out.append((start, end, False))
    return out


def _union_us(windows: list[tuple[int, int]]) -> int:
    """Total covered microseconds of possibly-overlapping [start, end) windows."""
    total = 0
    cur: tuple[int, int] | None = None
    for start, end in sorted(windows):
        if cur is None or start > cur[1]:
            if cur is not None:
                total += cur[1] - cur[0]
            cur = (start, end)
        else:
            cur = (cur[0], max(cur[1], end))
    if cur is not None:
        total += cur[1] - cur[0]
    return total


def _book_and_windows(
    entry: Entry,
) -> tuple[bool, int | None, int | None, pl.DataFrame | None, list[tuple[int, int | None, bool]]]:
    """(has_book, span_start, span_end, gaps, windows) for one entry.

    Span = the book_events extent; a trades-only directory (no/empty
    book_events) falls back to the trade tape's ``event_ts_us`` extent with
    ``has_book = False`` — uptime is an L2-capture concept and stays null there.
    """
    book = _read_table(entry, "book_events", ["recv_ts_us", "is_snapshot"])
    has_book = book is not None and book.height > 0
    if has_book:
        ts = book.get_column("recv_ts_us")
        span_start, span_end = ts.min(), ts.max()
        snap_ts = sorted(book.filter(pl.col("is_snapshot")).get_column("recv_ts_us").unique())
    else:
        trades = _read_table(entry, "trades", ["event_ts_us"])
        if trades is not None and trades.height > 0:
            ts = trades.get_column("event_ts_us")
            span_start, span_end = ts.min(), ts.max()
        else:
            span_start = span_end = None
        snap_ts = []
    gaps = _gaps(entry, require=has_book)
    windows = _gap_windows(gaps, snap_ts, span_end) if gaps is not None else []
    return has_book, span_start, span_end, gaps, windows


def holes(target: _Target) -> pl.DataFrame:
    """One row per gap record: where the hole opened, where (if) it closed."""
    entry = _as_entry(target)
    _, _, _, gaps, windows = _book_and_windows(entry)
    if gaps is None or gaps.height == 0:
        return pl.DataFrame(schema=_HOLES_SCHEMA)
    return gaps.with_columns(
        pl.Series("hole_end_us", [end for _, end, _ in windows], dtype=pl.Int64),
        pl.Series(
            "hole_us",
            [None if end is None else end - start for start, end, _ in windows],
            dtype=pl.Int64,
        ),
        pl.Series("resolved", [r for _, _, r in windows], dtype=pl.Boolean),
    )


_COVERAGE_SCHEMA = {
    "ticker": pl.String,
    "source_path": pl.String,
    "date": pl.Date,
    "complete": pl.Boolean,
    "reasons": pl.List(pl.String),
    "has_book": pl.Boolean,
    "span_start_us": pl.Int64,
    "span_end_us": pl.Int64,
    "span_us": pl.Int64,
    "n_gaps": pl.Int64,
    "n_unresolved_gaps": pl.Int64,
    "hole_us": pl.Int64,
    "uptime": pl.Float64,
    "n_book_events": pl.Int64,
    "n_trades": pl.Int64,
}


def _entry_row(entry: Entry) -> dict:
    has_book, span_start, span_end, gaps, windows = _book_and_windows(entry)
    span_us = None if span_start is None else span_end - span_start
    hole_us = None
    uptime = None
    if has_book:
        # Uptime denominates the observed span, so each hole contributes only
        # its overlap with [span_start, span_end] (a pre-snapshot reconnect
        # gap's hole lies wholly before the span and counts 0; the unresolved
        # count is the signal for out-of-span gaps).
        clamped = [
            (max(s, span_start), min(e, span_end))
            for s, e, _ in windows
            if e is not None and min(e, span_end) > max(s, span_start)
        ]
        hole_us = _union_us(clamped)
        if span_us:
            uptime = 1.0 - hole_us / span_us
        elif not windows:
            # A single-instant book (span 0) with no gaps is trivially whole.
            uptime = 1.0
    return {
        "ticker": entry.ticker,
        "source_path": str(entry.path),
        "date": entry.date,
        "complete": entry.complete,
        "reasons": list(entry.reasons),
        "has_book": has_book,
        "span_start_us": span_start,
        "span_end_us": span_end,
        "span_us": span_us,
        "n_gaps": None if gaps is None else gaps.height,
        "n_unresolved_gaps": (None if gaps is None else sum(1 for _, _, r in windows if not r)),
        "hole_us": hole_us,
        "uptime": uptime,
        "n_book_events": entry.tables.get("book_events", 0),
        "n_trades": entry.tables.get("trades", 0),
    }


def coverage(target: _Target | Sequence[_Target]) -> pl.DataFrame:
    """One accounting row per entry — no R3 gate (see the module docstring)."""
    if isinstance(target, (Entry, Path, str)):
        targets: list[_Target] = [target]
    else:
        targets = list(target)
        if not targets:
            raise KdpDataError("empty target list for coverage: the index filter matched nothing")
    rows = [_entry_row(_as_entry(t)) for t in targets]
    return pl.DataFrame(rows, schema=_COVERAGE_SCHEMA).with_columns(
        pl.from_epoch("span_start_us", time_unit="us").alias("span_start"),
        pl.from_epoch("span_end_us", time_unit="us").alias("span_end"),
    )

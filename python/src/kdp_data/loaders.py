"""Per-table Polars loaders with the manifest-honesty gate (R3/R8).

These are TABULAR reads: each returns one table's Parquet as-is (integer unit
columns intact; the writer's convenience float columns pass through). They
deliberately implement NO ordering, NO trade dedup, and NO book replay — a
trades frame from a directory holding both WS and REST copies of a print
contains BOTH rows. The canonical deterministic, deduped event stream is the
Rust ``kdp-load`` crate (pyo3 bindings planned); use it for backtest replay.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import polars as pl

from kdp_data.errors import IncompleteData, KdpDataError, MissingTable
from kdp_data.index import Entry, read_entry

_Target = Entry | Path | str


def _as_entry(target: _Target) -> Entry:
    return target if isinstance(target, Entry) else read_entry(Path(target))


def _load_one(table: str, target: _Target, allow_incomplete: bool) -> pl.DataFrame:
    entry = _as_entry(target)
    if not entry.complete and not allow_incomplete:
        raise IncompleteData(entry.path, entry.reasons)
    file = entry.path / f"{table}.parquet"
    if not file.exists():
        raise MissingTable(entry.path, table)
    return pl.read_parquet(file)


def _load(table: str, target: _Target | Sequence[_Target], allow_incomplete: bool) -> pl.DataFrame:
    if isinstance(target, (Entry, Path, str)):
        return _load_one(table, target, allow_incomplete)
    target = list(target)
    if not target:
        raise KdpDataError(
            f"empty target list for {table!r}: the index filter matched nothing"
        )
    frames = []
    for t in target:
        entry = _as_entry(t)
        frames.append(
            _load_one(table, entry, allow_incomplete).with_columns(
                pl.lit(str(entry.path)).alias("source_path")
            )
        )
    # diagonal: schemas may differ benignly across writer generations (e.g.
    # optional convenience columns); union with nulls, never drop a column.
    return pl.concat(frames, how="diagonal")


def load_trades(
    target: _Target | Sequence[_Target], *, allow_incomplete: bool = False
) -> pl.DataFrame:
    """The ``trades`` table (tape order as written; see module docstring)."""
    return _load("trades", target, allow_incomplete)


def load_book_top(
    target: _Target | Sequence[_Target], *, allow_incomplete: bool = False
) -> pl.DataFrame:
    """The ``book_top`` table (one row per top-of-book change)."""
    return _load("book_top", target, allow_incomplete)

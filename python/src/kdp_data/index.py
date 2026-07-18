"""Walk-on-demand dataset index over a kdp-processed tree.

A directory containing ``manifest.json`` becomes an :class:`Entry`; a
``YYYY-MM-DD[.archive|.live].tar.gz`` day tarball (the all-2026 backfill
shape — the backfill script suffixes its tier) becomes a lazy
:class:`TarEntry` — listed, never opened, until
:func:`kdp_data.tars.extract_day_tars`. No cache file: manifests are tiny and
a full walk is sub-second, so the index is rebuilt per call (revisit only if a
real tree measures slow). A malformed manifest raises — never skip-and-warn.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from kdp_data.errors import KdpDataError, UnsupportedSchema

SUPPORTED_SCHEMA_VERSION = 1

_MONTHS = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()
_EVENT_DATE_RE = re.compile(r"-(\d{2})(" + "|".join(_MONTHS) + r")(\d{2})")
# Plain YYYY-MM-DD.tar.gz plus the tiered names stream-backfill.sh actually
# emits (<day>.archive.tar.gz / <day>.live.tar.gz; seam days carry both).
_DAY_TAR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:\.(?:archive|live))?\.tar\.gz$")


@dataclass(frozen=True)
class Entry:
    """One processed per-ticker directory, as its manifest describes it."""

    ticker: str
    path: Path
    tables: dict[str, int]
    complete: bool
    reasons: tuple[str, ...]
    date: date | None
    # Verification stats (new in the 2026-07-11 capture-hardening pass); None
    # when the manifest predates them -- never a fabricated 0. Defaulted so
    # pre-existing Entry constructions keep working.
    verify_checks: int | None = None
    verify_mismatches: int | None = None
    underflows: int | None = None


@dataclass(frozen=True)
class TarEntry:
    """One day tarball, unopened (extract with kdp_data.tars)."""

    date: date
    path: Path


def _event_date(ticker: str) -> date | None:
    """The Kalshi event-date segment (-YYMONDD...), else None — never guessed."""
    m = _EVENT_DATE_RE.search(ticker)
    if m is None:
        return None
    yy, mon, dd = m.groups()
    try:
        return date(2000 + int(yy), _MONTHS.index(mon) + 1, int(dd))
    except ValueError:
        # The regex matched but it isn't a real date (e.g. -26FEB30): a false
        # positive, not an event date. None — and never abort the index build.
        return None


def _optional_int(raw: dict, key: str) -> int | None:
    """``raw[key]`` as int, or None when the key is absent (old manifest) --
    never a fabricated 0. A present-but-malformed value raises like any other
    manifest field (caught by the caller's shared except clause)."""
    return None if key not in raw else int(raw[key])


def read_entry(dir_path: Path) -> Entry:
    """Read + validate one directory's ``manifest.json`` into an Entry.

    Raises ``KdpDataError`` on a malformed/missing manifest or a non-parquet
    format, ``UnsupportedSchema`` when schema_version exceeds
    ``SUPPORTED_SCHEMA_VERSION`` (R8).
    """
    dir_path = Path(dir_path)
    manifest_path = dir_path / "manifest.json"
    try:
        raw = json.loads(manifest_path.read_text())
    except (OSError, ValueError) as exc:
        raise KdpDataError(f"{manifest_path}: unreadable manifest: {exc}") from exc
    try:
        version = int(raw["schema_version"])
        ticker = str(raw["ticker"])
        fmt = str(raw["format"])
        complete = bool(raw["complete"])
        read_errors = int(raw["read_errors"])
        counts = {k: int(v) for k, v in raw["counts"].items()}
        verify_checks = _optional_int(raw, "verify_checks")
        verify_mismatches = _optional_int(raw, "verify_mismatches")
        underflows = _optional_int(raw, "underflows")
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        # AttributeError covers shape errors like a non-dict "counts".
        raise KdpDataError(f"{manifest_path}: malformed manifest field: {exc}") from exc
    if version > SUPPORTED_SCHEMA_VERSION:
        raise UnsupportedSchema(dir_path, version, SUPPORTED_SCHEMA_VERSION)
    if fmt != "parquet":
        raise KdpDataError(f"{manifest_path}: unsupported format {fmt!r} (parquet only)")
    reasons: list[str] = []
    if not complete:
        if read_errors:
            reasons.append(f"{read_errors} read error(s)")
        if counts.get("raw", 0):
            reasons.append(f"{counts['raw']} raw fallback record(s)")
        if not reasons:
            reasons.append("manifest marked incomplete")
    return Entry(
        ticker=ticker,
        path=dir_path,
        tables=counts,
        complete=complete,
        reasons=tuple(reasons),
        date=_event_date(ticker),
        verify_checks=verify_checks,
        verify_mismatches=verify_mismatches,
        underflows=underflows,
    )


def _in_range(d: date | None, date_range: tuple[date, date] | None) -> bool:
    if date_range is None:
        return True
    if d is None:
        return False
    lo, hi = date_range
    return lo <= d <= hi


class DatasetIndex:
    """Everything loadable under one local root, queried as plain lists."""

    def __init__(self, entries: list[Entry], tars: list[TarEntry]) -> None:
        self._entries = sorted(entries, key=lambda e: (e.ticker, str(e.path)))
        self._tars = sorted(tars, key=lambda t: (t.date, str(t.path)))

    @classmethod
    def build(cls, root: Path | str) -> "DatasetIndex":
        root = Path(root)
        entries: list[Entry] = []
        tars: list[TarEntry] = []
        for dirpath, _dirnames, filenames in os.walk(root):
            d = Path(dirpath)
            if "manifest.json" in filenames:
                entries.append(read_entry(d))
            for name in filenames:
                m = _DAY_TAR_RE.match(name)
                if m:
                    tars.append(TarEntry(date=date.fromisoformat(m.group(1)), path=d / name))
        return cls(entries, tars)

    def tickers(self) -> list[str]:
        return sorted({e.ticker for e in self._entries})

    def entries(
        self,
        ticker: str | None = None,
        complete: bool | None = None,
        date_range: tuple[date, date] | None = None,
    ) -> list[Entry]:
        return [
            e
            for e in self._entries
            if (ticker is None or e.ticker == ticker)
            and (complete is None or e.complete == complete)
            and _in_range(e.date, date_range)
        ]

    def tar_entries(self, date_range: tuple[date, date] | None = None) -> list[TarEntry]:
        return [t for t in self._tars if _in_range(t.date, date_range)]

    def to_frame(self) -> pl.DataFrame:
        """The index itself as one Polars row per entry."""
        return pl.DataFrame(
            {
                "ticker": [e.ticker for e in self._entries],
                "path": [str(e.path) for e in self._entries],
                "complete": [e.complete for e in self._entries],
                "reasons": [list(e.reasons) for e in self._entries],
                "date": [e.date for e in self._entries],
                "trades": [e.tables.get("trades", 0) for e in self._entries],
                "book_top": [e.tables.get("book_top", 0) for e in self._entries],
                "book_events": [e.tables.get("book_events", 0) for e in self._entries],
                "gaps": [e.tables.get("gaps", 0) for e in self._entries],
                "verify_checks": [e.verify_checks for e in self._entries],
                "verify_mismatches": [e.verify_mismatches for e in self._entries],
                "underflows": [e.underflows for e in self._entries],
            },
            # When every manifest predates the verify fields, these three
            # columns are all None; Polars would otherwise infer Null (a
            # dataset-dependent schema -- Int64 once any manifest carries a
            # real value, Null when none do). Pin the dtype so a consumer's
            # schema never depends on which manifests happen to be present.
            schema_overrides={
                "verify_checks": pl.Int64,
                "verify_mismatches": pl.Int64,
                "underflows": pl.Int64,
            },
        )

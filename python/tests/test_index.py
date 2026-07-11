import json
from datetime import date

import polars as pl
import pytest
from conftest import FIXTURES, MIXED, PURE_WS

from kdp_data import DatasetIndex, KdpDataError, UnsupportedSchema
from kdp_data.index import Entry, read_entry


def test_build_indexes_both_fixture_dirs():
    idx = DatasetIndex.build(FIXTURES)
    assert idx.tickers() == ["KDPSYNTH-R6MIX", "KXBTCD-26JUL0306-T61699.99"]
    (mixed,) = idx.entries(ticker="KDPSYNTH-R6MIX")
    assert mixed.complete is True
    assert mixed.tables["trades"] == 4
    assert mixed.tables["book_top"] == 0
    assert mixed.path == MIXED


def test_event_date_parsed_from_ticker():
    idx = DatasetIndex.build(FIXTURES)
    (ws,) = idx.entries(ticker="KXBTCD-26JUL0306-T61699.99")
    assert ws.date == date(2026, 7, 3)
    (mixed,) = idx.entries(ticker="KDPSYNTH-R6MIX")
    assert mixed.date is None  # no -YYMONDD segment: None, never guessed


def test_entries_filter_by_completeness_and_date(tmp_path):
    # Synthesize one incomplete dir next to nothing else.
    d = tmp_path / "KXTEST-26AUG0100-T1"
    d.mkdir()
    (d / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "ticker": "KXTEST-26AUG0100-T1", "format": "parquet",
        "complete": False, "read_errors": 2,
        "counts": {"book_events": 1, "book_top": 0, "trades": 0, "gaps": 0, "raw": 3},
    }))
    idx = DatasetIndex.build(tmp_path)
    (e,) = idx.entries()
    assert e.complete is False
    assert any("read error" in r for r in e.reasons)
    assert any("raw fallback" in r for r in e.reasons)
    assert idx.entries(complete=True) == []
    assert idx.entries(date_range=(date(2026, 8, 1), date(2026, 8, 31))) == [e]
    assert idx.entries(date_range=(date(2026, 1, 1), date(2026, 1, 2))) == []


def test_newer_schema_is_refused(tmp_path):
    d = tmp_path / "KXFUT"
    d.mkdir()
    (d / "manifest.json").write_text(json.dumps({
        "schema_version": 99, "ticker": "KXFUT", "format": "parquet",
        "complete": True, "read_errors": 0,
        "counts": {"book_events": 0, "book_top": 0, "trades": 0, "gaps": 0, "raw": 0},
    }))
    with pytest.raises(UnsupportedSchema):
        DatasetIndex.build(tmp_path)


def test_malformed_manifest_is_typed_never_skipped(tmp_path):
    d = tmp_path / "BAD"
    d.mkdir()
    (d / "manifest.json").write_text("{not json")
    with pytest.raises(KdpDataError):
        DatasetIndex.build(tmp_path)


def test_invalid_date_segment_is_none_not_a_crash(tmp_path):
    # -26FEB30 regex-matches but Feb 30 is not a date: a false-positive match
    # must yield date=None, never abort the whole index build (review fix).
    d = tmp_path / "KXBAD-26FEB3000-T1"
    d.mkdir()
    (d / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "ticker": "KXBAD-26FEB3000-T1", "format": "parquet",
        "complete": True, "read_errors": 0,
        "counts": {"book_events": 0, "book_top": 0, "trades": 0, "gaps": 0, "raw": 0},
    }))
    (e,) = DatasetIndex.build(tmp_path).entries()
    assert e.date is None


def test_non_dict_counts_is_typed(tmp_path):
    # counts as a list used to escape as AttributeError (review fix).
    d = tmp_path / "KXSHAPE"
    d.mkdir()
    (d / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "ticker": "KXSHAPE", "format": "parquet",
        "complete": True, "read_errors": 0, "counts": [1, 2, 3],
    }))
    with pytest.raises(KdpDataError):
        DatasetIndex.build(tmp_path)


def test_day_tarballs_index_lazily(tmp_path):
    (tmp_path / "2026-01-05.tar.gz").write_bytes(b"not opened at index time")
    (tmp_path / "notes.tar.gz").write_bytes(b"non-date tars ignored")
    idx = DatasetIndex.build(tmp_path)
    (t,) = idx.tar_entries()
    assert t.date == date(2026, 1, 5)
    assert idx.tar_entries(date_range=(date(2026, 1, 6), date(2026, 1, 7))) == []


def test_to_frame_has_one_row_per_entry():
    frame = DatasetIndex.build(FIXTURES).to_frame()
    assert isinstance(frame, pl.DataFrame)
    assert frame.height == 2
    assert set(["ticker", "path", "complete", "date"]).issubset(frame.columns)


def test_read_entry_single_dir():
    e = read_entry(PURE_WS)
    assert isinstance(e, Entry)
    assert e.complete is True
    assert e.tables["trades"] == 18

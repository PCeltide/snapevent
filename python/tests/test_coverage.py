"""Coverage accounting tests, grounded in the two committed Rust fixtures."""

import json

import polars as pl
import pytest
from conftest import MIXED, PURE_WS

from kdp_data import KdpDataError, MissingTable, coverage, holes


def _synthetic_book_dir(tmp_path, *, book, gaps, trades=None, complete=True):
    """A minimal manifest dir: book = [(recv_ts_us, is_snapshot)], gaps =
    [recv_ts_us] or None to omit the file, trades = [event_ts_us]."""
    d = tmp_path / "KDPSYNTH-COV"
    d.mkdir()
    counts = {
        "book_events": len(book or []),
        "book_top": 0,
        "trades": len(trades or []),
        "gaps": len(gaps or []),
        "raw": 0,
    }
    manifest = {
        "schema_version": 1,
        "ticker": "KDPSYNTH-COV",
        "format": "parquet",
        "complete": complete,
        "read_errors": 0,
        "counts": counts,
    }
    (d / "manifest.json").write_text(json.dumps(manifest))
    if book is not None:
        pl.DataFrame(
            {"recv_ts_us": [t for t, _ in book], "is_snapshot": [s for _, s in book]},
            schema={"recv_ts_us": pl.Int64, "is_snapshot": pl.Boolean},
        ).write_parquet(d / "book_events.parquet")
    if gaps is not None:
        pl.DataFrame(
            {
                "recv_ts_us": gaps,
                "reason": ["seq_jump"] * len(gaps),
                "channel": ["orderbook_delta"] * len(gaps),
                "detail": [""] * len(gaps),
            },
            schema={
                "recv_ts_us": pl.Int64,
                "reason": pl.String,
                "channel": pl.String,
                "detail": pl.String,
            },
        ).write_parquet(d / "gaps.parquet")
    if trades is not None:
        pl.DataFrame({"event_ts_us": trades}, schema={"event_ts_us": pl.Int64}).write_parquet(
            d / "trades.parquet"
        )
    return d


def test_mixed_fixture_has_one_resolved_hole():
    df = holes(MIXED)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["recv_ts_us"] == 1_300_000
    assert row["hole_end_us"] == 1_500_000  # the re-anchoring snapshot
    assert row["hole_us"] == 200_000
    assert row["resolved"] is True
    assert row["reason"] == "seq_jump"


def test_pure_ws_fixture_has_no_holes_but_the_schema():
    df = holes(PURE_WS)
    assert df.height == 0
    assert df.schema["hole_end_us"] == pl.Int64
    assert df.schema["resolved"] == pl.Boolean


def test_trailing_gap_is_unresolved_to_span_end(tmp_path):
    d = _synthetic_book_dir(
        tmp_path,
        book=[(1_000_000, True), (3_000_000, False)],
        gaps=[2_000_000],
    )
    row = holes(d).row(0, named=True)
    assert row["hole_end_us"] == 3_000_000
    assert row["hole_us"] == 1_000_000
    assert row["resolved"] is False


def test_missing_gaps_table_beside_book_events_is_typed(tmp_path):
    d = _synthetic_book_dir(tmp_path, book=[(1_000_000, True)], gaps=None)
    with pytest.raises(MissingTable):
        holes(d)


def test_corrupt_parquet_is_a_typed_failure(tmp_path):
    d = _synthetic_book_dir(tmp_path, book=[(1_000_000, True)], gaps=[])
    (d / "book_events.parquet").write_bytes(b"not parquet")
    with pytest.raises(KdpDataError):
        holes(d)


def test_pure_ws_coverage_is_full_uptime():
    row = coverage(PURE_WS).row(0, named=True)
    assert row["complete"] is True and row["has_book"] is True
    assert row["n_gaps"] == 0 and row["n_unresolved_gaps"] == 0
    assert row["hole_us"] == 0
    assert row["uptime"] == 1.0
    assert row["span_us"] == row["span_end_us"] - row["span_start_us"] > 0
    assert row["n_trades"] == 18 and row["n_book_events"] > 0


def test_mixed_coverage_accounts_the_hole_without_an_incomplete_gate():
    # MIXED is read with no allow_incomplete ceremony: coverage REPORTS trust.
    row = coverage(MIXED).row(0, named=True)
    assert row["span_start_us"] == 1_000_000
    assert row["n_gaps"] == 1 and row["n_unresolved_gaps"] == 0
    assert row["hole_us"] == 200_000
    assert row["uptime"] == pytest.approx(1 - 200_000 / row["span_us"])


def test_overlapping_holes_are_unioned(tmp_path):
    d = _synthetic_book_dir(
        tmp_path,
        book=[(1_000_000, True), (1_500_000, True)],
        gaps=[1_100_000, 1_200_000],
    )
    row = coverage(d).row(0, named=True)
    assert row["hole_us"] == 400_000  # union, not 700_000


def test_trades_only_dir_has_null_gap_metrics(tmp_path):
    d = _synthetic_book_dir(tmp_path, book=None, gaps=None, trades=[5_000_000, 9_000_000])
    row = coverage(d).row(0, named=True)
    assert row["has_book"] is False
    assert row["span_start_us"] == 5_000_000 and row["span_end_us"] == 9_000_000
    assert row["n_gaps"] is None and row["uptime"] is None


def test_gap_after_the_last_book_row_is_unmeasurable_not_negative(tmp_path):
    # A reconnect gap can be stamped after the last book row (capture died in
    # the hole). Its window is unmeasurable -- null, never a negative hole_us,
    # and uptime must not exceed 1.0 (external review finding, 2026-07-11).
    d = _synthetic_book_dir(
        tmp_path,
        book=[(1_000_000, True), (2_000_000, False)],
        gaps=[3_000_000],
    )
    hrow = holes(d).row(0, named=True)
    assert hrow["hole_end_us"] is None and hrow["hole_us"] is None
    assert hrow["resolved"] is False
    crow = coverage(d).row(0, named=True)
    assert crow["hole_us"] == 0
    assert crow["uptime"] == 1.0
    assert crow["n_unresolved_gaps"] == 1  # the signal that the tail is suspect


def test_gap_before_the_span_only_counts_its_in_span_overlap(tmp_path):
    # A pre-snapshot reconnect gap resolves at the first snapshot = span start:
    # its hole lies entirely before the span, so it contributes 0 in-span hole
    # time (uptime stays in [0, 1]), while holes() keeps the raw geometry.
    d = _synthetic_book_dir(
        tmp_path,
        book=[(2_000_000, True), (2_100_000, False)],
        gaps=[1_000_000],
    )
    hrow = holes(d).row(0, named=True)
    assert hrow["hole_end_us"] == 2_000_000 and hrow["hole_us"] == 1_000_000
    assert hrow["resolved"] is True
    crow = coverage(d).row(0, named=True)
    assert crow["hole_us"] == 0
    assert crow["uptime"] == 1.0


def test_list_target_stacks_and_empty_list_is_typed():
    df = coverage([PURE_WS, MIXED])
    assert df.height == 2
    assert df.get_column("ticker").n_unique() == 2
    with pytest.raises(KdpDataError):
        coverage([])

import json

import polars as pl
import pytest
from conftest import MIXED, PURE_WS

from kdp_data import (
    DatasetIndex,
    IncompleteData,
    KdpDataError,
    MissingTable,
    load_book_top,
    load_trades,
)


def test_load_trades_single_dir():
    df = load_trades(PURE_WS)
    assert df.height == 18
    assert {"recv_ts_us", "price_micro", "count_centi", "trade_id"}.issubset(df.columns)


def test_load_book_top_single_dir():
    df = load_book_top(PURE_WS)
    assert df.height > 0
    assert {"recv_ts_us", "mid_micro", "yes_bid_micro"}.issubset(df.columns)


def test_missing_table_is_typed():
    # KDPSYNTH-R6MIX deliberately has no book_top.parquet (counts.book_top=0).
    with pytest.raises(MissingTable):
        load_book_top(MIXED)


def test_python_loader_does_not_dedup():
    # 4 rows incl. the WS+REST copy of trade t1: the TABULAR loader returns the
    # file as-is; dedup semantics live in kdp-load only (docstring contract).
    df = load_trades(MIXED)
    assert df.height == 4
    assert df.filter(pl.col("trade_id") == "t1").height == 2


def test_incomplete_requires_explicit_acknowledgment(tmp_path):
    d = tmp_path / "KXPART"
    d.mkdir()
    (d / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "ticker": "KXPART", "format": "parquet",
        "complete": False, "read_errors": 1,
        "counts": {"book_events": 0, "book_top": 0, "trades": 1, "gaps": 0, "raw": 0},
    }))
    pl.DataFrame({"recv_ts_us": [1], "price_micro": [450000]}).write_parquet(
        d / "trades.parquet"
    )
    with pytest.raises(IncompleteData) as exc:
        load_trades(d)
    assert "read error" in str(exc.value)
    df = load_trades(d, allow_incomplete=True)
    assert df.height == 1


def test_empty_list_target_is_typed():
    # A filter that matches nothing (typo'd ticker, nothing complete) must be
    # a KdpDataError, not polars' bare "cannot concat empty list" (review fix).
    with pytest.raises(KdpDataError):
        load_trades([])


def test_list_target_concats_with_source_path():
    idx = DatasetIndex.build(PURE_WS.parent)
    df = load_trades(idx.entries())
    assert "source_path" in df.columns
    assert df.height == 18 + 4
    assert df["source_path"].n_unique() == 2

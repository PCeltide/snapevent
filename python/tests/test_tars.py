import json
import tarfile

import polars as pl
import pytest

from kdp_data import DatasetIndex, extract_day_tars, load_trades


def _make_day_tar(tmp_path, day: str):
    """A day tarball shaped like the all-2026 backfill: per-ticker dirs inside."""
    src = tmp_path / "stage" / "KXTEST-26AUG0100-T1"
    src.mkdir(parents=True)
    (src / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "ticker": "KXTEST-26AUG0100-T1", "format": "parquet",
        "complete": True, "read_errors": 0,
        "counts": {"book_events": 0, "book_top": 0, "trades": 1, "gaps": 0, "raw": 0},
    }))
    pl.DataFrame({"recv_ts_us": [1], "price_micro": [450000]}).write_parquet(
        src / "trades.parquet"
    )
    tar_path = tmp_path / f"{day}.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tf:
        tf.add(src, arcname=src.name)
    return tar_path


def test_extract_then_index_round_trip(tmp_path):
    _make_day_tar(tmp_path, "2026-08-01")
    idx = DatasetIndex.build(tmp_path)
    dest = tmp_path / "extracted"
    dirs = extract_day_tars(idx.tar_entries(), dest)
    assert dirs == [dest / "2026-08-01"]
    reindexed = DatasetIndex.build(dest)
    (entry,) = reindexed.entries()
    assert load_trades(entry).height == 1


def test_failed_extraction_leaves_no_day_dir_to_skip(tmp_path):
    # A corrupt tar must fail LOUDLY and leave no day dir behind -- otherwise
    # the next run's skip-unless-overwrite check would silently treat the
    # half-written dir as done (review fix: extract to staging, then rename).
    (tmp_path / "2026-09-01.tar.gz").write_bytes(b"this is not a tarball")
    idx = DatasetIndex.build(tmp_path)
    dest = tmp_path / "extracted"
    with pytest.raises(Exception):
        extract_day_tars(idx.tar_entries(), dest)
    assert not (dest / "2026-09-01").exists(), "failed day must stay retryable"


def test_already_extracted_days_skip_unless_overwrite(tmp_path):
    _make_day_tar(tmp_path, "2026-08-01")
    idx = DatasetIndex.build(tmp_path)
    dest = tmp_path / "extracted"
    first = extract_day_tars(idx.tar_entries(), dest)
    marker = dest / "2026-08-01" / "marker.txt"
    marker.write_text("left by a later run")
    second = extract_day_tars(idx.tar_entries(), dest)
    assert second == first
    assert marker.exists(), "skip means SKIP: no re-extract without overwrite"
    third = extract_day_tars(idx.tar_entries(), dest, overwrite=True)
    assert third == first
    assert not marker.exists(), "overwrite replaces the day dir"

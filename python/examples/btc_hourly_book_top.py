"""BTC hourly book_top study — the kdp-data acceptance demo.

Point --root at a locally-synced btc-hourly tree (per-hour event folders of
processed per-ticker dirs). Builds the index, loads every COMPLETE dir's
book_top into one frame, and prints a per-source mid summary.

    uv run python examples/btc_hourly_book_top.py --root D:/data/btc-hourly
"""

import argparse

import polars as pl

from kdp_data import DatasetIndex, load_book_top


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", required=True, help="local root of processed capture dirs")
    args = ap.parse_args()

    idx = DatasetIndex.build(args.root)
    complete = idx.entries(complete=True)
    print(
        f"index: {len(idx.entries())} dirs ({len(complete)} complete), "
        f"{len(idx.tar_entries())} day tars"
    )
    if not complete:
        print("nothing complete to load; sync some processed hours first")
        return

    df = load_book_top(complete)
    summary = (
        df.filter(pl.col("mid_micro").is_not_null())
        .group_by("source_path")
        .agg(
            rows=pl.len(),
            mid_lo=pl.col("mid_micro").min() / 1_000_000,
            mid_hi=pl.col("mid_micro").max() / 1_000_000,
        )
        .sort("source_path")
    )
    print(summary)


if __name__ == "__main__":
    main()

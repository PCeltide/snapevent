# kdp-data

Dataset index + Polars tabular loaders for kdp-processed Kalshi market data
(the Parquet per-ticker directories `kdp-process` writes — see
`docs/data-guide.md` at the repo root).

Tabular reads only: deterministic ordering, trade dedup, and book replay live
in the Rust `kdp-load` crate (pyo3 bindings planned). A trades frame from a
directory holding both WS and REST copies of a print contains both rows.

## Install

    uv add "kdp-data @ git+https://github.com/PCeltide/snapevent#subdirectory=python"
    # or, inside this repo:
    cd python && uv sync

## Use

    from kdp_data import DatasetIndex, load_trades, load_book_top

    idx = DatasetIndex.build("D:/data/kdp")        # walk-on-demand, no cache
    idx.to_frame()                                  # the index as a Polars frame
    df = load_trades(idx.entries(ticker="..."))     # list -> concat + source_path

    # Incomplete captures refuse to load unless acknowledged (R3):
    load_trades(entry, allow_incomplete=True)

    # Day tarballs (all-2026 backfill) index lazily; extract on demand:
    from kdp_data import extract_day_tars
    dirs = extract_day_tars(idx.tar_entries(), "D:/data/extracted")

## Coverage — is the data trustworthy?

    from kdp_data import coverage, holes

    cov = coverage(idx.entries())          # one row per dir: span, hole_us, uptime
    holes(entry)                           # one row per gap: where the hole opened/closed

Coverage deliberately does NOT apply the R3 gate — it is the tool that
*reports* trustworthiness, so it reads incomplete dirs too and surfaces
`complete`/`reasons` as columns. A hole runs from a `gaps` row to the first
re-anchoring snapshot after it (unresolved holes run to the span end);
overlapping holes are unioned and clamped to the observed span, so `uptime`
stays in `[0, 1]` — a gap outside the span signals via `n_unresolved_gaps`;
trades-only dirs report null gap metrics (uptime is an L2-capture concept).
Rollups are one `group_by` away:

    cov.group_by("date").agg(pl.mean("uptime"), pl.sum("hole_us"))

Example: `uv run python examples/btc_hourly_book_top.py --root <tree>`.

Dev: `uv run pytest` / `uv run ruff check .` (or `powershell -File
../scripts/check-py.ps1` from python/, `scripts/check-py.ps1` from the repo
root).

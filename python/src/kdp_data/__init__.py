"""kdp-data: dataset index + Polars tabular loaders over kdp-processed trees.

Tabular reads only — deterministic ordering, trade dedup, and book replay live
in the Rust ``kdp-load`` crate (pyo3 bindings later), not here.
"""

from kdp_data.coverage import coverage, holes
from kdp_data.errors import IncompleteData, KdpDataError, MissingTable, UnsupportedSchema
from kdp_data.index import SUPPORTED_SCHEMA_VERSION, DatasetIndex, Entry, TarEntry
from kdp_data.loaders import load_book_top, load_trades
from kdp_data.tars import extract_day_tars

__all__ = [
    "SUPPORTED_SCHEMA_VERSION",
    "DatasetIndex",
    "Entry",
    "IncompleteData",
    "KdpDataError",
    "MissingTable",
    "TarEntry",
    "UnsupportedSchema",
    "coverage",
    "extract_day_tars",
    "holes",
    "load_book_top",
    "load_trades",
]

"""Typed failures. Nothing in kdp-data warns-and-continues: every anomaly is
one of these exceptions, and the only opt-out is the explicit
``allow_incomplete=True`` on the loaders (mirroring kdp-load's R3 gate)."""

from __future__ import annotations

from pathlib import Path


class KdpDataError(Exception):
    """Base for all kdp-data failures (malformed manifest, bad tree, ...)."""


class IncompleteData(KdpDataError):
    """The directory's manifest says the capture is not complete (R3).

    Pass ``allow_incomplete=True`` to load anyway — an explicit
    acknowledgment, never a default.
    """

    def __init__(self, path: Path, reasons: tuple[str, ...]) -> None:
        self.path = path
        self.reasons = reasons
        super().__init__(f"{path}: incomplete capture: {'; '.join(reasons)}")


class UnsupportedSchema(KdpDataError):
    """The manifest's schema_version is newer than this package reads (R8)."""

    def __init__(self, path: Path, found: int, supported: int) -> None:
        self.path = path
        self.found = found
        self.supported = supported
        super().__init__(f"{path}: schema_version {found} > supported {supported}")


class MissingTable(KdpDataError):
    """The requested table's Parquet file is absent from the directory."""

    def __init__(self, path: Path, table: str) -> None:
        self.path = path
        self.table = table
        super().__init__(f"{path}: table {table!r} not present")

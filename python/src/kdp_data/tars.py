"""Extract-on-demand for day tarballs (the all-2026 backfill shape).

The index lists ``YYYY-MM-DD[.archive|.live].tar.gz`` files lazily; this
helper extracts a selection into ``dest/<tar name>/`` — the full stem, so the
two tiers of a cutoff-seam day land in separate dirs, never merged (tarfile's
``filter="data"`` guards against
path traversal; note it needs Python 3.11.4+, a hair above the declared
``>=3.11`` floor), skipping already-extracted days unless ``overwrite=True``.
Re-index the returned dirs with ``DatasetIndex.build(dest)``.
"""

from __future__ import annotations

import shutil
import tarfile
from collections.abc import Sequence
from pathlib import Path

from kdp_data.index import TarEntry


def extract_day_tars(
    tar_entries: Sequence[TarEntry], dest: Path | str, *, overwrite: bool = False
) -> list[Path]:
    dest = Path(dest)
    out: list[Path] = []
    for entry in tar_entries:
        day = entry.path.name.removesuffix(".tar.gz")
        day_dir = dest / day
        if day_dir.exists():
            if not overwrite:
                out.append(day_dir)
                continue
            shutil.rmtree(day_dir)
        # Extract into a staging dir and rename only on success: day_dir must
        # never exist half-written, or the skip check above would silently
        # treat a failed (retryable) extraction as done.
        staging = dest / f".extracting-{day}"
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        try:
            with tarfile.open(entry.path, "r:gz") as tf:
                tf.extractall(staging, filter="data")
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        staging.rename(day_dir)
        out.append(day_dir)
    return out

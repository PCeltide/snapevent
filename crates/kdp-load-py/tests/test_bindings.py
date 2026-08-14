"""Smoke tests for the kdp_load bindings, against the committed kdp-load
fixture. Run via scripts/check-load-py.ps1 (needs maturin develop first)."""

import json
import shutil
from pathlib import Path

import pytest
from kdp_load import IncompleteData, Loader

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "kdp-load"
    / "tests"
    / "fixture"
    / "KXBTCD-26JUL0306-T61699.99"
)


def test_events_match_fixture_counts():
    ld = Loader(str(FIXTURE))
    assert ld.complete and ld.reasons == []
    kinds: dict[str, int] = {}
    last_ts = None
    for ev in ld.events():
        kinds[ev["kind"]] = kinds.get(ev["kind"], 0) + 1
        if last_ts is not None:
            assert ev["ts_us"] >= last_ts  # monotonic effective ts
        last_ts = ev["ts_us"]
    assert kinds == {"snapshot": 1, "delta": 2824, "trade": 18}


def test_between_opens_with_synthetic_snapshot():
    ld = Loader(str(FIXTURE))
    evs = list(ld.events())
    t0 = (evs[0]["ts_us"] + evs[-1]["ts_us"]) // 2
    first = next(iter(ld.between(t0, evs[-1]["ts_us"] + 1)))
    assert first["kind"] == "snapshot" and first["synthetic"]
    assert first["ts_us"] == t0


def test_book_at_returns_positive_ladder():
    ld = Loader(str(FIXTURE))
    evs = list(ld.events())
    b = ld.book_at((evs[0]["ts_us"] + evs[-1]["ts_us"]) // 2)
    assert b["suspect_gaps"] == []
    assert b["yes"] and b["no"]
    assert all(q > 0 for _, q in b["yes"] + b["no"])


def test_incomplete_gate_raises_then_acknowledges(tmp_path):
    bad = tmp_path / FIXTURE.name
    shutil.copytree(FIXTURE, bad)
    manifest = json.loads((bad / "manifest.json").read_text())
    manifest["complete"] = False
    manifest["read_errors"] = 1
    (bad / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(IncompleteData):
        next(iter(Loader(str(bad)).events()))
    evs = list(Loader(str(bad), allow_incomplete=True).events())
    assert len(evs) == 2843

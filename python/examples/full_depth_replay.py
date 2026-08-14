"""Full-depth replay from Python via the kdp_load bindings.

Build the bindings first (from the repo root):

    powershell -File scripts/check-load-py.ps1     # maturin develop + smoke

Then run against the committed kdp-load fixture (no data of your own needed):

    cd python && uv run python examples/full_depth_replay.py [TICKER_DIR]
"""

import sys
from collections import Counter
from pathlib import Path

from kdp_load import Loader

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "crates/kdp-load/tests/fixture/KXBTCD-26JUL0306-T61699.99"
)


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else str(FIXTURE)
    ld = Loader(target)  # raises IncompleteData unless allow_incomplete=True
    print(f"opened {target}\ncomplete={ld.complete} reasons={ld.reasons}")

    # The merged, deterministic, time-ordered stream (integer units, ADR-004).
    kinds = Counter()
    t_first = t_last = None
    for ev in ld.events():
        kinds[ev["kind"]] += 1
        t_first = t_first if t_first is not None else ev["ts_us"]
        t_last = ev["ts_us"]
    print(f"events: {dict(kinds)} over {(t_last - t_first) / 1e6:.1f} s")

    # The full ladder at any instant — one call.
    book = ld.book_at((t_first + t_last) // 2)
    print(f"mid-capture ladder: {len(book['yes'])} yes / {len(book['no'])} no levels")
    if book["suspect_gaps"]:
        print("  WARNING: capture hole since last snapshot -> ladder is suspect")
    for price_micro, qty_centi in sorted(book["yes"], reverse=True)[:3]:
        print(f"  yes ${price_micro / 1e6:.2f} x {qty_centi / 100:,.0f}")
    for price_micro, qty_centi in sorted(book["no"], reverse=True)[:3]:
        print(f"  no  ${price_micro / 1e6:.2f} x {qty_centi / 100:,.0f}")


if __name__ == "__main__":
    main()

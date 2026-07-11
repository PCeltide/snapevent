from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "crates" / "kdp-load" / "tests" / "fixture"
PURE_WS = FIXTURES / "KXBTCD-26JUL0306-T61699.99"
MIXED = FIXTURES / "KDPSYNTH-R6MIX"

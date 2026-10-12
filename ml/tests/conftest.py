import sys
from pathlib import Path

# Make `aqiml` (ml/) and `helpers` (ml/tests/) importable when running `pytest ml/tests` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

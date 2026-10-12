"""Imported first by every notebook: puts ml/ on sys.path so `import aqiml` works
whether the file runs as a script or as a notebook opened from this folder."""

import sys
from pathlib import Path

ML_DIR = Path(__file__).resolve().parents[1]
if str(ML_DIR) not in sys.path:
    sys.path.insert(0, str(ML_DIR))

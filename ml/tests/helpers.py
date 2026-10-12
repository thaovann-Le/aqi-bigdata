"""Synthetic data in the format of role C's export (docs/data_contract.md)."""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from aqiml.data import VALUE_COLUMNS

ML_DIR = Path(__file__).resolve().parents[1]

EXPORT_VALUE_COLUMNS = ["pm25", "pm10", "n_stations", "aqi_model", "pm25_model", "pm10_model", "temperature",
                        "humidity", "wind_speed", "wind_direction", "precipitation", "pressure"]


def hourly_frame(hours: int = 24 * 10, start: str = "2024-12-25 00:00") -> pd.DataFrame:
    """City-hour frame as returned by aqiml.data.to_hourly; ``aqi`` equals the row number."""
    index = pd.date_range(start, periods=hours, freq="h", name="ts")
    rng = np.random.default_rng(0)
    frame = pd.DataFrame(rng.uniform(1, 100, (hours, len(VALUE_COLUMNS))), index=index, columns=VALUE_COLUMNS)
    return frame.assign(aqi=np.arange(hours, dtype=float), wind_direction=rng.uniform(0, 360, hours))


def export_rows(city: str, hours: list[str], aqi: list[float]) -> pd.DataFrame:
    """Rows as written by export_ml_dataset.py (ts_utc in UTC, *_model column names)."""
    ts = pd.to_datetime(hours).tz_localize("Asia/Ho_Chi_Minh").tz_convert("UTC")
    values = {c: 1.0 for c in EXPORT_VALUE_COLUMNS}
    return pd.DataFrame({"city": city, "ts_local": hours, "ts_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "aqi": aqi, **values})


def load_script(name: str, rel: str):
    """Import a script under ml/ (serving/, training/) that is not part of the aqiml package."""
    spec = importlib.util.spec_from_file_location(name, ML_DIR / rel)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

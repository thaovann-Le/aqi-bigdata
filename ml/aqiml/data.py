"""Read role C's processed city-hour data (docs/data_contract.md, section 3).

Two sources, one output format (one row per hour, naive UTC+7 index):
  - ``load_dataset``: the flat export of ``processing/batch/export_ml_dataset.py`` (training, notebooks)
  - ``aqiml.storage.read_city_hourly``: Cassandra ``daily_agg`` + ``weather_hourly`` (hourly serving)

Columns after loading:
  aqi, pm25, pm10, n_stations      measured at stations (mean over physical stations that hour)
  cams_aqi, cams_pm25, cams_pm10   CAMS model values from Open-Meteo (renamed from *_model)
  temperature, humidity, wind_speed (m/s), wind_direction, precipitation, pressure
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import LOCAL_TZ, ML_DATASET, SPLITS

MEASURED_COLUMNS = ["aqi", "pm25", "pm10", "n_stations"]
CAMS_COLUMNS = ["cams_aqi", "cams_pm25", "cams_pm10"]
WEATHER_COLUMNS = ["temperature", "humidity", "wind_speed", "wind_direction", "precipitation", "pressure"]
VALUE_COLUMNS = MEASURED_COLUMNS + CAMS_COLUMNS + WEATHER_COLUMNS

_RENAME = {"aqi_model": "cams_aqi", "pm25_model": "cams_pm25", "pm10_model": "cams_pm10"}


def _read_export(path: Path, cities: list[str] | None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run role C's pipeline first (processing/README.md: "
            "docker compose -f docker-compose.demo.yml up -d) or set AQI_ML_DATASET."
        )
    csv_parts = sorted(path.glob("part-*.csv")) if path.is_dir() else []
    if csv_parts:
        df = pd.concat(pd.read_csv(p) for p in csv_parts)
        return df if cities is None else df[df["city"].isin(cities)]
    filters = None if cities is None else [("city", "in", list(cities))]
    return pd.read_parquet(path, filters=filters)


def load_dataset(cities: list[str] | None = None, path: Path = ML_DATASET) -> pd.DataFrame:
    """Long frame (``city``, ``ts`` + value columns). ``cities=None`` loads all 63 points."""
    df = _read_export(Path(path), cities).rename(columns=_RENAME)
    ts = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)
    df = df.assign(ts=ts)[["city", "ts", *VALUE_COLUMNS]]
    df[VALUE_COLUMNS] = df[VALUE_COLUMNS].apply(pd.to_numeric, errors="coerce")
    return df.sort_values(["city", "ts"]).reset_index(drop=True)


def to_hourly(long: pd.DataFrame, city: str) -> pd.DataFrame:
    """One city on a gap-free hourly index (missing hours become NaN rows), so that
    ``shift(k)`` means exactly k hours."""
    one = long[long["city"] == city]
    if one.empty:
        raise ValueError(f"No rows for city {city!r}")
    one = one.drop_duplicates("ts", keep="last").set_index("ts")[VALUE_COLUMNS]
    index = pd.date_range(one.index.min(), one.index.max(), freq="h", name="ts")
    return one.reindex(index)


def load_city_hourly(city: str, path: Path = ML_DATASET) -> pd.DataFrame:
    return to_hourly(load_dataset([city], path), city)


def current_network_era(long: pd.DataFrame) -> pd.DataFrame:
    """Rows from the start of the station network that real-time data also uses
    (Hanoi 2024-01, HCMC 2024-11-19; see SPLITS). Before that the measured series comes
    from a different monitor and its level is not comparable."""
    starts = long["city"].map({city: pd.Timestamp(d.train_start) for city, d in SPLITS.items()})
    return long[starts.notna() & (long["ts"] >= starts)]

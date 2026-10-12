"""Feature engineering for the +24h city AQI forecast.

Each row is a forecast issued at hour ``t`` (the index). Features use data at ``t`` or
earlier, except the ``*_fut`` weather columns of the ``weather_forecast`` set: weather at
``t + horizon``. In training these come from the historical weather (a perfect forecast);
when serving they come from the Open-Meteo forecast API, so live accuracy is somewhat lower.
The target is the measured city AQI at ``t + horizon``.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

TARGET = "aqi"
HORIZON_HOURS = 24
LAGS = (0, 1, 3, 6, 24)  # hours before the issue time t
ROLLING = (6, 24)

WEATHER_FEATURES = ["temperature", "humidity", "precipitation", "wind_speed", "pressure"]
NON_FEATURE_COLUMNS = ("target", "persistence", "target_ts")


@dataclass(frozen=True)
class FeatureSet:
    name: str
    use_future_weather: bool


FEATURE_SETS: dict[str, FeatureSet] = {
    # AQI history + calendar + current weather: everything comes from Cassandra.
    "base": FeatureSet("base", use_future_weather=False),
    # + weather at the target hour (next-day forecast). Deployed model; falls back to
    # "base" when the forecast API is unavailable.
    "weather_forecast": FeatureSet("weather_forecast", use_future_weather=True),
}
DEFAULT_FEATURE_SET = "weather_forecast"
FALLBACK_FEATURE_SET = "base"


def _wind_components(speed: pd.Series, direction_deg: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Meteorological convention: direction is where the wind blows *from*."""
    rad = np.deg2rad(direction_deg)
    return -speed * np.sin(rad), -speed * np.cos(rad)


def _require_hourly_index(hourly: pd.DataFrame) -> None:
    if not isinstance(hourly.index, pd.DatetimeIndex):
        raise TypeError("hourly frame must be indexed by timestamp")
    steps = hourly.index.to_series().diff().dropna()
    if not (steps == pd.Timedelta(hours=1)).all():
        raise ValueError("hourly frame must have a gap-free hourly index (see aqiml.data.to_hourly)")


def make_features(
    hourly: pd.DataFrame,
    feature_set: FeatureSet = FEATURE_SETS[DEFAULT_FEATURE_SET],
    horizon: int = HORIZON_HOURS,
) -> pd.DataFrame:
    """Feature matrix + ``target`` (AQI at t+horizon) + ``persistence`` (AQI at t).

    Rows with a missing target are kept so the same function serves inference.
    """
    _require_hourly_index(hourly)
    y = hourly[TARGET]
    target_ts = hourly.index + pd.Timedelta(hours=horizon)

    cols: dict[str, pd.Series | np.ndarray] = {f"{TARGET}_lag{k}": y.shift(k) for k in LAGS}
    for w in ROLLING:
        cols[f"{TARGET}_mean_{w}h"] = y.rolling(w, min_periods=w // 2).mean()
    cols[f"{TARGET}_max_24h"] = y.rolling(24, min_periods=12).max()
    cols[f"{TARGET}_delta_24h"] = y - y.shift(24)
    cols["n_stations"] = hourly["n_stations"]

    cols["hour"] = target_ts.hour
    cols["dayofweek"] = target_ts.dayofweek
    cols["month"] = target_ts.month
    cols["dayofyear"] = target_ts.dayofyear

    wind_u, wind_v = _wind_components(hourly["wind_speed"], hourly["wind_direction"])
    weather = hourly[WEATHER_FEATURES].assign(wind_u=wind_u, wind_v=wind_v)
    precip_24h = hourly["precipitation"].rolling(24, min_periods=12).sum()
    cols.update({c: weather[c] for c in weather.columns})
    cols["precipitation_sum_24h"] = precip_24h
    cols["wind_speed_mean_24h"] = hourly["wind_speed"].rolling(24, min_periods=12).mean()
    cols["pressure_delta_24h"] = hourly["pressure"] - hourly["pressure"].shift(24)

    if feature_set.use_future_weather:
        cols.update({f"{c}_fut": weather[c].shift(-horizon) for c in weather.columns})
        cols["precipitation_sum_24h_fut"] = precip_24h.shift(-horizon)  # rain over the next 24h

    features = pd.DataFrame(cols, index=hourly.index)
    return features.assign(target=y.shift(-horizon), persistence=y, target_ts=target_ts)


def feature_columns(frame: pd.DataFrame) -> list[str]:
    return [c for c in frame.columns if c not in NON_FEATURE_COLUMNS]

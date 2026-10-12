"""I/O boundaries with fakes: Cassandra rows -> city-hour frame, and the Open-Meteo forecast API."""

import pandas as pd
import pytest
import requests
from helpers import load_script

from aqiml.config import CITY_COORDS
from aqiml.data import VALUE_COLUMNS, WEATHER_COLUMNS
from aqiml.storage import read_city_hourly

predict = load_script("predict", "serving/predict.py")


class FakeSession:
    """Mimics the cassandra-driver calls used by read_city_hourly. Timestamps are naive UTC, as the driver returns."""

    def __init__(self, measured=(), model=(), weather=()):
        self.rows = {"measured": list(measured), "model": list(model), "weather": list(weather)}

    def prepare(self, query):
        return query

    def execute(self, query, params):
        if "daily_agg" in query:
            return self.rows[params[1]]  # params = (city, data_basis, start, end)
        return self.rows["weather"]


def test_read_city_hourly_joins_sources_on_local_hour_and_fills_gaps():
    # 01:00 UTC is 08:00 in Vietnam.
    session = FakeSession(
        measured=[(pd.Timestamp("2025-01-10 01:00"), 120.0, 45.0, 60.0, 4)],
        model=[(pd.Timestamp("2025-01-10 01:00"), 140.0, 50.0, 70.0, 1)],
        weather=[(pd.Timestamp("2025-01-10 01:00"), 18.0, 80.0, 2.5, 90.0, 0.0, 1015.0),
                 (pd.Timestamp("2025-01-10 02:00"), 19.0, 78.0, 2.0, 100.0, 0.3, 1014.0)],
    )

    frame = read_city_hourly(session, "hanoi", pd.Timestamp("2025-01-10 07:00"), pd.Timestamp("2025-01-10 10:00"))

    assert list(frame.columns) == VALUE_COLUMNS
    assert list(frame.index) == list(pd.date_range("2025-01-10 07:00", periods=3, freq="h"))  # end is exclusive
    eight = frame.loc["2025-01-10 08:00"]
    assert (eight["aqi"], eight["n_stations"], eight["cams_aqi"], eight["temperature"]) == (120.0, 4.0, 140.0, 18.0)
    assert frame.loc["2025-01-10 09:00", "precipitation"] == 0.3
    assert pd.isna(frame.loc["2025-01-10 07:00", "aqi"]) and pd.isna(frame.loc["2025-01-10 09:00", "aqi"])


def test_read_city_hourly_with_no_rows_returns_all_nan_frame():
    frame = read_city_hourly(FakeSession(), "hcm", pd.Timestamp("2025-01-10 07:00"), pd.Timestamp("2025-01-10 10:00"))
    assert len(frame) == 3 and frame.isna().all().all() and list(frame.columns) == VALUE_COLUMNS


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


PAYLOAD = {"hourly": {
    "time": ["2025-01-10T00:00", "2025-01-10T01:00"],
    "temperature_2m": [20.0, 21.0], "relative_humidity_2m": [70, 71], "wind_speed_10m": [2.0, 2.5],
    "wind_direction_10m": [90, 100], "precipitation": [0.0, 0.2], "surface_pressure": [1010.0, 1011.0],
}}


def test_fetch_weather_forecast_requests_ms_units_and_maps_columns(monkeypatch):
    seen = {}

    def fake_get(url, params, timeout):
        seen.update(url=url, params=params)
        return FakeResponse(PAYLOAD)

    monkeypatch.setattr(predict.requests, "get", fake_get)

    weather = predict.fetch_weather_forecast("hanoi")

    assert seen["params"]["wind_speed_unit"] == "ms" and seen["params"]["timezone"] == "Asia/Bangkok"
    assert (seen["params"]["latitude"], seen["params"]["longitude"]) == CITY_COORDS["hanoi"]
    assert list(weather.columns) == WEATHER_COLUMNS
    assert weather.index[0] == pd.Timestamp("2025-01-10 00:00") and weather.index.tz is None
    assert weather.loc["2025-01-10 01:00", "precipitation"] == 0.2


def test_fetch_weather_forecast_retries_then_raises(monkeypatch):
    calls = []

    def failing_get(url, params, timeout):
        calls.append(1)
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(predict.requests, "get", failing_get)
    monkeypatch.setattr(predict.time, "sleep", lambda s: None)

    with pytest.raises(RuntimeError, match="unavailable"):
        predict.fetch_weather_forecast("hcm")
    assert len(calls) == predict.HTTP_RETRIES

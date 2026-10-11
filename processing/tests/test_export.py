from datetime import datetime, timedelta, timezone

import pytest

from common import sources
from common.pipeline import build_measurements

from export_ml_dataset import build_dataset  # processing/batch được thêm vào sys.path trong conftest.py

VN = timezone(timedelta(hours=7))
CITIES = ["hanoi", "hcm"]


@pytest.fixture(scope="module")
def ds(spark, stations, sample_history):
    weather = sources.history_weather(spark, sample_history, CITIES)
    aq = sources.history_openaq(spark, sample_history, stations, CITIES)
    model = sources.history_model_aq(spark, sample_history, CITIES)
    meas = build_measurements(aq.unionByName(model.select(*aq.columns)), weather)
    return {r["city"] + " " + r["ts_local"]: r.asDict() for r in build_dataset(spark, meas, weather, stations).collect()}


def test_one_row_per_city_hour(ds):
    assert all(k.count(" ") == 2 for k in ds)


def test_measured_model_and_weather_side_by_side(ds):
    r = ds["hanoi 2024-01-01 10:00"]
    assert r["n_stations"] == 2 and r["pm25"] == pytest.approx((40.0 + 15.5) / 2)
    assert r["aqi_model"] == 77 and r["pm25_model"] == 25.0
    assert r["wind_speed"] == pytest.approx(10.0) and r["ts_utc"] == "2024-01-01T03:00:00Z"


def test_model_only_hours_have_null_measured(ds):
    # TP.HCM 2024: không có trạm đo trong dữ liệu mẫu (giống thực tế 01-11/2024) -> chỉ có số mô hình
    r = ds["hcm 2024-01-01 10:00"]
    assert r["aqi"] is None and r["n_stations"] is None and r["aqi_model"] == 77

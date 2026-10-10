"""Kiểm tra batch lịch sử trên dữ liệu mẫu đúng định dạng của B (xem tools/make_sample_history.py)."""
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

import make_sample_history as gen
from common import config, sources
from common.aqi import aqi_py
from common.pipeline import build_alerts, build_measurements

VN = timezone(timedelta(hours=7))
CITIES = ["hanoi", "hcm"]


@pytest.fixture(scope="module")
def built(spark, stations, sample_history):
    weather = sources.history_weather(spark, sample_history, CITIES)
    aq = sources.history_openaq(spark, sample_history, stations, CITIES)
    model = sources.history_model_aq(spark, sample_history, CITIES)
    meas = build_measurements(aq.unionByName(model.select(*aq.columns)), weather).persist()
    meas.count()
    return {"weather": weather.persist(), "meas": meas, "stations": stations}


def vn(ts):
    """Spark trả datetime 'naive' theo múi giờ hệ thống; đổi về giờ Việt Nam (naive) để so sánh."""
    return ts.astimezone(VN).replace(tzinfo=None)


def rows(df, **where):
    cond = None
    for k, v in where.items():
        c = F.col(k) == v
        cond = c if cond is None else cond & c
    return [r.asDict() for r in (df.where(cond) if cond is not None else df).collect()]


def by_hour(df, station_id):
    return {vn(x["ts"]): x for x in rows(df, station_id=station_id)}


def test_columns_match_cassandra_table(built):
    assert tuple(built["meas"].columns) == config.MEASUREMENT_COLS


def test_unknown_station_and_other_cities_dropped(built):
    ids = {r["station_id"] for r in built["meas"].select("station_id").distinct().collect()}
    assert ids == {"openaq:7441", "openaq:2161292", "openaq:4946811", "openaq:7440", "openaq:3276359",
                   "model:hanoi", "model:hcm"}
    cities = {r["city"] for r in built["weather"].select("city").distinct().collect()}
    assert cities == {"hanoi", "hcm"}                                  # haiphong bị loại


def test_missing_and_outlier_become_null_not_averaged(built):
    got = by_hour(built["meas"], "openaq:7441")
    assert got[datetime(2023, 1, 2, 3)]["pm25"] is None                # -999 -> thiếu
    assert got[datetime(2023, 1, 2, 4)]["pm25"] is None                # 985 > 500 -> loại
    assert got[datetime(2023, 1, 2, 5)]["pm25"] == pytest.approx(gen.pm25_hanoi(datetime(2023, 1, 2, 5)))


def test_openaq_aqi_computed_from_pm(built):
    got = by_hour(built["meas"], "openaq:7441")
    x = got[datetime(2023, 1, 2, 10)]
    assert x["pm25"] == pytest.approx(40.0) and x["pm10"] == pytest.approx(60.0)
    assert x["aqi"] == aqi_py(40.0, 60.0)
    assert x["aqi_standard"] == "US_AQI" and x["source"] == "openaq" and x["is_model"] is False
    # Giờ pm25 bị loại nhưng pm10 hợp lệ -> AQI vẫn tính được từ pm10
    y = got[datetime(2023, 1, 2, 3)]
    assert y["pm25"] is None and y["aqi"] == aqi_py(None, 53.0)


def test_five_minute_station_collapsed_to_hourly(built):
    got = rows(built["meas"], station_id="openaq:4946811")
    assert len(got) == 2 * 3 * 24            # 2 cửa sổ (tháng 1, tháng 7) x 3 ngày x 24 giờ
    assert len(got) == len({x["ts"] for x in got})
    assert all(x["pm25"] == pytest.approx(15.5) for x in got)   # 12 mẫu 10.0 + (0..11) -> trung bình 15.5


def test_non_mass_units_ignored(built):
    got = rows(built["meas"], station_id="openaq:3276359")      # CMT8 có um003 (particles/cm³)
    assert len(got) == len({x["ts"] for x in got}) and all(x["pm25"] == 40.0 for x in got)


def test_weather_join_units_and_timezone(built):
    x = by_hour(built["meas"], "openaq:7441")[datetime(2023, 1, 2, 10)]
    assert x["wind_speed"] == pytest.approx(10.0)                      # 36 km/h = 10 m/s
    assert x["temperature"] == pytest.approx(20.0 + 10 / 2)
    assert x["pressure"] == pytest.approx(1010.0)
    # Open-Meteo "2023-01-02T10:00" là 10:00 giờ VN = 03:00 UTC (kiểm tra bằng instant tuyệt đối)
    assert x["ts"].astimezone(timezone.utc).hour == 3
    first = [r for r in rows(built["weather"], city="hanoi") if vn(r["ts"]) == datetime(2023, 1, 1, 0)][0]
    assert first["humidity"] is None                                   # độ ẩm 150 -> loại


def test_model_rows_flagged_and_separate(built):
    m = rows(built["meas"], station_id="model:hanoi")
    assert m and all(x["is_model"] is True and x["source"] == "openmeteo_aq" for x in m)
    assert all(x["aqi"] == 77 and x["pm25"] == 25.0 for x in m)
    assert not [x for x in rows(built["meas"], is_model=False) if x["station_id"].startswith("model")]


def test_alerts_only_measured_above_threshold(built):
    a150 = [r.asDict() for r in build_alerts(built["meas"], threshold=150, stations=built["stations"]).collect()]
    assert len(a150) == 1                                              # chỉ giờ SPIKE (80 µg/m³ -> AQI 163)
    r = a150[0]
    assert r["station_id"] == "openaq:2161292" and vn(r["ts"]) == datetime(2025, 1, 2, 8)
    assert r["aqi"] == aqi_py(80.0) and r["level"] == "unhealthy" and "Lưu Quang Vũ" in r["message"]
    low = [x.asDict() for x in build_alerts(built["meas"], threshold=60).collect()]
    assert low and all(x["aqi"] > 60 and not x["station_id"].startswith("model") for x in low)


def test_idempotent_no_duplicate_keys(built):
    m = built["meas"]
    assert m.count() == m.select("city", "station_id", "ts").distinct().count()

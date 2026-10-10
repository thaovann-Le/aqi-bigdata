from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from common import sources
from common.aggregate import COLS, aggregate_city, city_hourly
from common.aqi import aqi_py
from common.pipeline import build_measurements

VN = timezone(timedelta(hours=7))
CITIES = ["hanoi", "hcm"]


def vn(ts):
    return ts.astimezone(VN).replace(tzinfo=None)


@pytest.fixture(scope="module")
def agg(spark, stations, sample_history):
    weather = sources.history_weather(spark, sample_history, CITIES)
    aq = sources.history_openaq(spark, sample_history, stations, CITIES)
    model = sources.history_model_aq(spark, sample_history, CITIES)
    meas = build_measurements(aq.unionByName(model.select(*aq.columns)), weather)
    out = aggregate_city(spark, meas, stations).persist()
    out.count()
    return out


def pick(df, when, **kw):
    cond = None
    for k, v in kw.items():
        c = F.col(k) == v
        cond = c if cond is None else cond & c
    found = [r.asDict() for r in df.where(cond).collect() if vn(r["period_start"]) == when]
    assert len(found) == 1, (kw, when, found)
    return found[0]


def test_columns_and_period_types(agg):
    assert agg.columns == COLS
    assert {r[0] for r in agg.select("period_type").distinct().collect()} == {"hour", "day", "month", "season"}
    assert {r[0] for r in agg.select("data_basis").distinct().collect()} == {"measured", "model"}


def test_hour_single_station(agg):
    r = pick(agg, datetime(2023, 1, 2, 10), city="hanoi", period_type="hour", data_basis="measured")
    assert r["avg_aqi"] == aqi_py(40.0, 60.0) and r["record_count"] == 1
    assert r["avg_pm25"] == pytest.approx(40.0) and r["label"] is None


def test_hour_averages_physical_stations(agg):
    # 2024-01-01 10:00 Hà Nội có hai trạm: 2161292 (pm25 = 40) và trạm 5 phút 4946811 (pm25 = 15.5)
    r = pick(agg, datetime(2024, 1, 1, 10), city="hanoi", period_type="hour", data_basis="measured")
    assert r["record_count"] == 2
    assert r["avg_pm25"] == pytest.approx((40.0 + 15.5) / 2)
    assert r["avg_aqi"] == pytest.approx((aqi_py(40.0) + aqi_py(15.5)) / 2)
    assert r["max_aqi"] == aqi_py(40.0) and r["min_aqi"] == aqi_py(15.5)


def test_measured_and_model_never_mixed(agg):
    m = pick(agg, datetime(2024, 1, 1, 10), city="hanoi", period_type="hour", data_basis="model")
    assert m["avg_aqi"] == 77 and m["record_count"] == 1


def test_day_and_month_rollup(agg):
    d = pick(agg, datetime(2023, 1, 2), city="hanoi", period_type="day", data_basis="measured")
    assert d["label"] == "2023-01-02" and d["record_count"] == 24     # giờ 03:00 vẫn có AQI nhờ pm10
    mo = pick(agg, datetime(2023, 1, 1), city="hanoi", period_type="month", data_basis="measured")
    assert mo["label"] == "2023-01" and mo["record_count"] == 72


def test_season_assignment_wraps_new_year(agg):
    def seasons(city):
        return {(x["label"], vn(x["period_start"]))
                for x in agg.where((F.col("city") == city) & (F.col("period_type") == "season")
                                   & (F.col("data_basis") == "measured")).collect()}
    hanoi = seasons("hanoi")
    assert ("winter", datetime(2022, 11, 1)) in hanoi          # tháng 1/2023 thuộc mùa đông bắt đầu 11/2022
    assert ("summer", datetime(2023, 5, 1)) in hanoi           # tháng 7
    hcm = seasons("hcm")
    assert ("dry", datetime(2022, 11, 1)) in hcm and ("rainy", datetime(2023, 5, 1)) in hcm


def test_same_physical_station_counted_once(spark, stations):
    """AQICN 13026 và OpenAQ 2161292 là cùng một trạm (cách nhau < 300 m) -> một lần trong trung bình."""
    t = datetime(2026, 1, 1, 10)
    rows = [("hanoi", "aqicn:13026", t, 50, 10.0, False), ("hanoi", "openaq:2161292", t, 100, 30.0, False),
            ("hanoi", "openaq:7441", t, 20, 5.0, False)]
    df = spark.createDataFrame(rows, "city string, station_id string, ts timestamp, aqi int, pm25 double, is_model boolean")
    h = city_hourly(df.withColumn("pm10", F.lit(None).cast("double")), stations).collect()[0]
    # 2 trạm vật lý: trạm 2161292 = (50+100)/2 = 75 và trạm 7441 = 20 -> trung bình 47.5
    assert h["record_count"] == 2 and h["avg_aqi"] == pytest.approx((75 + 20) / 2)

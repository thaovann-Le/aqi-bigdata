import pytest
from pyspark.sql import functions as F

from common import aqi, config

CASES = [None, -1.0, 0.0, 5.0, 9.0, 9.05, 9.1, 12.0, 12.1, 35.4, 35.45, 35.5, 55.4, 55.5, 150.4, 150.5,
         125.4, 125.5, 225.4, 225.5, 325.4, 400.0, 600.0]


@pytest.mark.parametrize("version", ["2024", "2012"])
def test_spark_matches_python_reference(spark, monkeypatch, version):
    monkeypatch.setattr(config, "PM25_BREAKPOINTS", version)
    rows = [(a, b) for a in CASES for b in (None, 10.0, 54.0, 55.0, 200.0, 500.0, 700.0)]
    df = spark.createDataFrame(rows, "pm25 double, pm10 double")
    got = {(r["pm25"], r["pm10"]): r["aqi"]
           for r in df.withColumn("aqi", aqi.us_aqi(F.col("pm25"), F.col("pm10"))).collect()}
    for (a, b), v in got.items():
        assert v == aqi.aqi_py(a, b), (version, a, b, v, aqi.aqi_py(a, b))


def test_known_values_2024(monkeypatch):
    monkeypatch.setattr(config, "PM25_BREAKPOINTS", "2024")
    assert aqi.aqi_py(pm25=9.0) == 50
    assert aqi.aqi_py(pm25=9.1) == 51
    assert aqi.aqi_py(pm25=35.4) == 100
    assert aqi.aqi_py(pm25=35.5) == 101
    assert aqi.aqi_py(pm25=55.5) == 151
    assert aqi.aqi_py(pm25=225.5) == 301
    assert aqi.aqi_py(pm25=1000) == 500
    assert aqi.aqi_py() is None and aqi.aqi_py(pm25=-5) is None


def test_known_values_2012(monkeypatch):
    monkeypatch.setattr(config, "PM25_BREAKPOINTS", "2012")
    assert aqi.aqi_py(pm25=12.0) == 50
    assert aqi.aqi_py(pm25=55.5) == 151


def test_pm10_and_max_of_subindices():
    assert aqi.aqi_py(pm10=54) == 50
    assert aqi.aqi_py(pm10=55) == 51
    assert aqi.aqi_py(pm25=10.0, pm10=200) == max(aqi.aqi_py(pm25=10.0), aqi.aqi_py(pm10=200))


def test_alert_levels(spark):
    df = spark.createDataFrame([(40,), (60,), (120,), (151,), (201,), (301,)], "aqi int")
    got = [r["l"] for r in df.select(aqi.alert_level(F.col("aqi")).alias("l")).collect()]
    assert got == ["good", "moderate", "unhealthy_for_sensitive", "unhealthy", "very_unhealthy", "hazardous"]

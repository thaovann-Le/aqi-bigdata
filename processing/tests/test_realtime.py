"""Kiểm tra xử lý real-time trên dữ liệu THẬT trong ingestion/results/raw-mau.zip (1.116 message)."""
import json
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from common import config, sources
from common.aqi import aqi_py
from common.pipeline import build_measurements, process_raw

VN = timezone(timedelta(hours=7))


def vn(ts):
    return ts.astimezone(VN).replace(tzinfo=None)


@pytest.fixture(scope="module")
def raw(spark, raw_sample):
    return spark.read.text([f"{raw_sample}/aqi_raw/date=*/*.jsonl", f"{raw_sample}/weather_raw/date=*/*.jsonl"])


@pytest.fixture(scope="module")
def env(raw):
    return sources.parse_envelope(raw).persist()


def test_all_real_messages_parse(raw, env):
    assert raw.count() == 1116 and env.count() == 1116


def test_corrupt_lines_are_dropped(spark, raw):
    bad = spark.createDataFrame([("not json",), ('{"foo": 1}',), ("",)], "value string")
    assert sources.parse_envelope(raw.limit(5).unionByName(bad)).count() == 5


def test_aqicn_uses_aqi_only_and_vn_hour(env):
    r = [x.asDict() for x in sources.realtime_aqicn(env).where("station_id = 'aqicn:1583'").orderBy("ts").collect()][0]
    assert vn(r["ts"]) == datetime(2026, 10, 6, 18, 0)          # time.iso = 18:00+07:00
    assert r["aqi"] == 12
    assert r["pm25"] is None and r["pm10"] is None             # iaqi.* là AQI con, không phải µg/m³


def test_openaq_hourly_pivot_and_stale_values_dropped(env):
    df = sources.realtime_openaq(env)
    # Số đo cũ nhiều tháng bị loại: no2 của 2161292 (2026-07-13) và pm10 của CMT8 (2025-01-14)
    assert df.where(F.col("ts") < F.lit("2026-10-01").cast("timestamp")).count() == 0
    cmt8 = {vn(x["ts"]): x.asDict() for x in df.where("station_id = 'openaq:3276359'").collect()}
    x = cmt8[datetime(2026, 10, 6, 23, 0)]
    assert x["pm25"] == pytest.approx(37.8) and x["pm10"] is None   # um003/pm1/nhiệt độ/độ ẩm không lọt vào
    assert x["is_model"] is False and x["source"] == "openaq"


def test_openaq_split_across_messages_is_merged(spark):
    """Hai lần gọi API khác nhau mang số đo của các giờ khác nhau: không được mất chất nào."""
    def msg(fetched, results):
        return json.dumps({"source": "openaq", "city": "hanoi", "station_id": 1, "fetched_at": fetched,
                           "payload": {"location": {"id": 1}, "results": results}})

    def r(p, v, t="2026-10-06T02:00:00Z"):
        return {"datetime": {"utc": t}, "value": v, "parameter": p, "units": "µg/m³"}

    lines = [msg("2026-10-06T02:50:00+00:00", [r("pm25", 10.0), r("pm10", 20.0)]),
             msg("2026-10-06T03:10:00+00:00", [r("pm25", 12.0, "2026-10-06T03:00:00Z"), r("pm10", 20.0)])]
    e = sources.parse_envelope(spark.createDataFrame([(x,) for x in lines], "value string"))
    got = {vn(x["ts"]): x.asDict() for x in sources.realtime_openaq(e).collect()}
    assert got[datetime(2026, 10, 6, 9)]["pm25"] == 10.0 and got[datetime(2026, 10, 6, 9)]["pm10"] == 20.0
    assert got[datetime(2026, 10, 6, 10)]["pm25"] == 12.0


def test_model_rows_flagged(env):
    x = [r.asDict() for r in sources.realtime_model_aq(env).where("city = 'hcm'").orderBy("ts").collect()][0]
    assert x["station_id"] == "model:hcm" and x["is_model"] is True and x["source"] == "openmeteo_aq"
    assert vn(x["ts"]) == datetime(2026, 10, 6, 23, 0)
    assert x["pm25"] == pytest.approx(45.6) and x["aqi"] == 116


def test_weather_hourly_units_and_hour_ending(env):
    w = [r.asDict() for r in sources.realtime_weather(env).collect()]
    keyed = {(r["city"], vn(r["ts"])): r for r in w}
    assert len(keyed) == len(w)                                 # mỗi (city, giờ) một dòng
    x = keyed[("hanoi", datetime(2026, 10, 7, 0, 0))]           # current.time 23:30, 23:45, 00:00 -> giờ 00:00
    assert ("hanoi", datetime(2026, 10, 6, 23, 0)) not in keyed  # chưa có số đọc nào trong (22:00, 23:00]
    assert 0 <= x["wind_speed"] < 5                             # đã đổi km/h -> m/s
    assert 15 < x["temperature"] < 30 and 900 < x["pressure"] < 1100


def _weather_msg(time, t, rain, fetched, interval=900):
    return json.dumps({"source": "openmeteo", "city": "hanoi", "station_id": None, "fetched_at": fetched,
                       "payload": {"current": {"time": time, "interval": interval, "temperature_2m": t,
                                               "relative_humidity_2m": 80, "precipitation": rain,
                                               "wind_speed_10m": 3.6, "wind_direction_10m": 90,
                                               "surface_pressure": 1010}}})


def test_weather_hour_matches_openmeteo_hourly_convention(spark):
    """Giờ T: biến tức thời = số đọc lúc T (mới nhất trong (T-1h, T]); mưa = tổng các số đọc 15 phút trong (T-1h, T]."""
    lines = [_weather_msg("2026-10-07T08:15", 20.0, 0.1, "2026-10-07T01:20:00+00:00"),
             _weather_msg("2026-10-07T08:30", 21.0, 0.2, "2026-10-07T01:35:00+00:00"),
             _weather_msg("2026-10-07T08:45", 22.0, 0.3, "2026-10-07T01:50:00+00:00"),
             _weather_msg("2026-10-07T09:00", 23.0, 0.4, "2026-10-07T02:05:00+00:00"),
             _weather_msg("2026-10-07T09:00", 23.0, 0.4, "2026-10-07T02:10:00+00:00"),   # trùng: cùng số đọc
             _weather_msg("2026-10-07T09:15", 24.0, 0.5, "2026-10-07T02:20:00+00:00"),
             _weather_msg("2026-10-07T09:45", 26.0, 0.3, "2026-10-07T02:50:00+00:00")]   # thiếu 09:30, 10:00
    e = sources.parse_envelope(spark.createDataFrame([(x,) for x in lines], "value string"))
    got = {vn(r["ts"]): r.asDict() for r in sources.realtime_weather(e).collect()}
    assert set(got) == {datetime(2026, 10, 7, 9), datetime(2026, 10, 7, 10)}
    assert got[datetime(2026, 10, 7, 9)]["temperature"] == 23.0
    assert got[datetime(2026, 10, 7, 9)]["precipitation"] == pytest.approx(1.0)
    assert got[datetime(2026, 10, 7, 10)]["temperature"] == 26.0
    assert got[datetime(2026, 10, 7, 10)]["precipitation"] == pytest.approx((0.5 + 0.3) * 4 / 2)   # quy đổi 2/4 số đọc
    assert got[datetime(2026, 10, 7, 9)]["wind_speed"] == pytest.approx(1.0)


def _oaq_msg(fetched, results, station=4946813):
    return json.dumps({"source": "openaq", "city": "hanoi", "station_id": station, "fetched_at": fetched,
                       "payload": {"results": [{"datetime": {"utc": t}, "value": v, "parameter": p, "units": "µg/m³"}
                                               for t, p, v in results]}})


def test_five_minute_samples_belong_to_the_hour_they_end_in(spark):
    """Mẫu 07:55 thuộc giờ 08:00 (khoảng 07:00–08:00]; mẫu 08:05, 08:15 thuộc giờ 09:00 - như trung bình giờ của OpenAQ."""
    lines = [_oaq_msg("2026-10-07T01:16:00+00:00", [("2026-10-07T00:55:00Z", "pm25", 61.2)]),
             _oaq_msg("2026-10-07T01:26:00+00:00", [("2026-10-07T01:05:00Z", "pm25", 80.0),
                                                     ("2026-10-07T01:15:00Z", "pm25", 70.0)])]
    e = sources.parse_envelope(spark.createDataFrame([(x,) for x in lines], "value string"))
    got = {vn(r["ts"]): r["pm25"] for r in sources.realtime_openaq(e).collect()}
    assert got == {datetime(2026, 10, 7, 8): pytest.approx(61.2), datetime(2026, 10, 7, 9): pytest.approx(75.0)}


def test_future_timestamps_dropped(spark):
    """Thời điểm đo sau lúc gọi API hơn MAX_FUTURE_MINUTES là sai đồng hồ: không được lọt qua bộ lọc 48 giờ."""
    future = _oaq_msg("2026-10-07T01:00:00+00:00", [("2026-10-07T05:00:00Z", "pm25", 30.0),
                                                     ("2026-10-07T01:00:00Z", "pm10", 40.0)])
    aqicn = json.dumps({"source": "aqicn", "city": "hanoi", "station_id": 1583,
                        "fetched_at": "2026-10-07T01:00:00+00:00",
                        "payload": {"aqi": "90", "time": {"iso": "2026-10-07T11:00:00+07:00"}}})
    e = sources.parse_envelope(spark.createDataFrame([(future,), (aqicn,)], "value string"))
    assert [vn(r["ts"]) for r in sources.realtime_openaq(e).collect()] == [datetime(2026, 10, 7, 8)]
    assert sources.realtime_aqicn(e).count() == 0


def test_model_aqi_left_null_when_source_has_none(spark):
    """CAMS không có us_aqi -> giữ null (không thay bằng AQI tự tính theo giờ); OpenAQ thì tự tính."""
    model = json.dumps({"source": "openmeteo_aq", "city": "hanoi", "station_id": None,
                        "fetched_at": "2026-10-07T01:10:00+00:00",
                        "payload": {"current": {"time": "2026-10-07T08:00", "pm2_5": 80.0, "pm10": 90.0,
                                                "us_aqi": None}}})
    oaq = _oaq_msg("2026-10-07T01:10:00+00:00", [("2026-10-07T01:00:00Z", "pm25", 80.0)], station=2161292)
    e = sources.parse_envelope(spark.createDataFrame([(model,), (oaq,)], "value string"))
    m = {r["station_id"]: r.asDict()
         for r in build_measurements(sources.realtime_aq(e), sources.realtime_weather(e)).collect()}
    assert m["model:hanoi"]["aqi"] is None and m["model:hanoi"]["pm25"] == 80.0
    assert m["openaq:2161292"]["aqi"] == aqi_py(80.0)


def test_measurements_have_no_duplicate_keys_and_have_weather(env):
    aq, w = sources.realtime_aq(env), sources.realtime_weather(env)
    m = build_measurements(aq, w).persist()
    assert tuple(m.columns) == config.MEASUREMENT_COLS
    assert m.count() == m.select("city", "station_id", "ts").distinct().count()
    first_weather = w.agg(F.min("ts")).first()[0]
    covered = m.where(F.col("ts") >= F.lit(first_weather))
    assert covered.count() > 0 and covered.where("temperature is null").count() == 0
    cmt8 = {vn(x["ts"]): x.asDict() for x in m.where("station_id = 'openaq:3276359'").collect()}
    x = cmt8[datetime(2026, 10, 6, 23, 0)]
    assert x["aqi"] == aqi_py(37.8, None) == 107 and x["aqi_standard"] == "US_AQI"


def test_process_raw_end_to_end(spark, raw, stations, sink):
    m = process_raw(spark, raw, sink, None, stations, threshold=150)
    assert m["messages_in"] == 1116 and m["messages_parsed"] == 1116
    assert m["measurement_rows"] == len(sink.rows("measurements")) > 0
    assert len(sink.rows("weather_hourly")) == m["weather_rows"] > 0
    alerts = sink.rows("alerts")
    assert alerts and all(a["aqi"] > 150 and not a["station_id"].startswith("model") for a in alerts)
    assert m["messages_per_min"] and m["seconds"] > 0
    # Chạy lại cùng dữ liệu cho cùng kết quả (idempotent theo khóa chính)
    sink2 = type(sink)()
    process_raw(spark, raw, sink2, None, stations, threshold=150)

    def key(r):
        return (r["city"], r["station_id"], r["ts"])
    assert sorted(map(key, sink.rows("measurements"))) == sorted(map(key, sink2.rows("measurements")))

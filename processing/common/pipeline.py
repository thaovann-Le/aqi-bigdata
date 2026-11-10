"""Lõi xử lý dùng chung: message thô (envelope JSON) -> measurements, weather_hourly, alerts.

Streaming (foreachBatch), batch đọc lại bucket `raw` và test đều gọi `process_raw`, nên logic làm sạch
chỉ có một bản.
"""
import json
import logging
import os
import time
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import config, sources
from common.aqi import alert_level
from common.cleaning import attach_weather, dedupe_latest, finalize_measurements, merge_stored

log = logging.getLogger("processing")
Lookup = Callable[[SparkSession, DataFrame], DataFrame]


def build_alerts(measurements: DataFrame, threshold: int = None, stations: DataFrame = None) -> DataFrame:
    """Cảnh báo = số đo TẠI TRẠM (không phải số mô hình) có AQI > ngưỡng."""
    thr = config.ALERT_THRESHOLD if threshold is None else threshold
    a = measurements.where((~F.col("is_model")) & (F.col("aqi") > thr))
    a = a.withColumn("level", alert_level(F.col("aqi")))
    if stations is not None:
        a = a.join(F.broadcast(stations.select("station_id", F.col("name").alias("_name"))), "station_id", "left")
    else:
        a = a.withColumn("_name", F.lit(None).cast("string"))
    msg = F.concat(F.lit("AQI "), F.col("aqi").cast("string"), F.lit(" ("), F.col("level"),
                   F.lit(") tại "), F.coalesce(F.col("_name"), F.col("station_id")))
    return a.select("city", "ts", "station_id", "aqi", "level", msg.alias("message"))


def build_measurements(aq: DataFrame, weather: DataFrame) -> DataFrame:
    """Ghép số đo + thời tiết rồi chuẩn hóa về đúng cột bảng `measurements`."""
    return finalize_measurements(attach_weather(aq, weather))


def process_raw(
    spark: SparkSession,
    raw: DataFrame,
    write: Callable[[DataFrame, str], None],
    weather_lookup: Optional[Lookup] = None,
    stations: DataFrame = None,
    value_col: str = "value",
    threshold: int = None,
    obs_lookup: Optional[Lookup] = None,
    readings_lookup: Optional[Lookup] = None,
    meas_lookup: Optional[Lookup] = None,
    delete: Optional[Callable[[DataFrame, str], None]] = None,
) -> dict:
    """Xử lý một lô message thô. Trả về số liệu (số dòng vào/ra, thời gian) để đưa vào báo cáo hiệu năng.

    Streaming chia dữ liệu một giờ ra nhiều micro-batch, nên số liệu giờ không được tính chỉ từ phần nằm trong lô.
    Các lookup đọc trạng thái đã lưu ở các lô trước (None = tính trong lô, như batch_raw đọc một lần):
      readings_lookup(spark, readings) -> số đọc thời tiết 15 phút đã lưu (weather_readings)
      obs_lookup(spark, obs)           -> số đo OpenAQ đã lưu (openaq_observations): trạm đo 5 phút, cảm biến lệch giờ
      weather_lookup(spark, aq)        -> thời tiết giờ đã lưu (weather_hourly) cho giờ của số đo không có trong lô
      meas_lookup(spark, keys)         -> measurements đã lưu (city, station_id, ts, aqi) tại các (city, ts) của lô:
                                          ghi lại thời tiết cho dòng cũ khi thời tiết giờ đó đổi, biết AQI cũ để
      delete(df, "alerts")                gỡ cảnh báo của (trạm, giờ) mà AQI tính lại không còn vượt ngưỡng
    Với đủ lookup, kết quả sau mọi lô bằng kết quả chạy batch một lần trên cùng dữ liệu (test_streaming_* kiểm tra).
    """
    t0 = time.time()
    thr = config.ALERT_THRESHOLD if threshold is None else threshold
    cached = []

    def keep(df: DataFrame) -> DataFrame:
        cached.append(df.persist())
        return cached[-1]

    try:
        raw = keep(raw)
        n_in = raw.count()
        env = keep(sources.parse_envelope(raw, value_col))
        n_ok = env.count()

        # ---- thời tiết: số đọc 15 phút -> giờ
        readings = keep(sources.realtime_weather_readings(env))
        n_readings = readings.count()
        all_readings = readings
        if readings_lookup is not None and n_readings:
            all_readings = merge_stored(readings, readings_lookup(spark, readings), ["city", "ts_reading"], ["city"],
                                        "ts_reading")
        weather = keep(sources.weather_from_readings(all_readings))
        n_weather = weather.count()
        if n_readings:
            write(readings, "weather_readings")
        if n_weather:
            write(weather.select("city", "ts", *config.WEATHER_COLS), "weather_hourly")

        # ---- số đo
        obs = keep(sources.realtime_openaq_obs(env))
        n_obs = obs.count()
        if n_obs:
            write(obs, "openaq_observations")
        stored_obs = obs_lookup(spark, obs) if obs_lookup is not None and n_obs else None
        aq = keep(sources.realtime_aq(env, stored_obs=stored_obs))
        n_aq = aq.count()

        all_weather = weather
        if weather_lookup is not None and n_aq:
            # Thời tiết của giờ này có thể đã đến ở lô trước và nằm sẵn trong Cassandra.
            cols = ["city", "ts", *config.WEATHER_COLS]
            stored = weather_lookup(spark, aq)
            # Cùng (city, giờ) thì ưu tiên bản của lô hiện tại (prio = 1) hơn bản đã lưu (prio = 0).
            both = (weather.select(*cols).withColumn("prio", F.lit(1))
                    .unionByName(stored.select(*cols).withColumn("prio", F.lit(0))))
            all_weather = dedupe_latest(both, ["city", "ts"], order_col="prio").drop("prio")

        meas = keep(build_measurements(aq, all_weather))
        n_meas = meas.count()
        # Đọc trạng thái measurements TRƯỚC khi ghi lô này (để biết AQI cũ và dòng nào đã có)
        stored_meas = None
        if meas_lookup is not None and (n_weather or n_meas):
            keys = weather.select("city", "ts").unionByName(meas.select("city", "ts")).distinct()
            stored_meas = keep(meas_lookup(spark, keys).select("city", "station_id", "ts", "aqi"))
            stored_meas.count()
        n_alerts = n_cleared = n_refresh = 0
        if n_meas:
            write(meas, "measurements")
            alerts = keep(build_alerts(meas, thr, stations))
            n_alerts = alerts.count()
            if n_alerts:
                write(alerts, "alerts")
            if delete is not None and stored_meas is not None:
                # (trạm, giờ) từng vượt ngưỡng (AQI đã lưu) nhưng AQI tính lại với nhiều số đo hơn thì không -> gỡ cảnh
                # báo cũ. Chỉ xóa đúng các dòng này (không xóa đại trà) để không sinh tombstone thừa trong Cassandra.
                k = ["city", "station_id", "ts"]
                cleared = (meas.where(~F.col("is_model") & ~F.coalesce(F.col("aqi") > thr, F.lit(False))).select(*k)
                               .join(stored_meas.where(F.col("aqi") > thr).select(*k), k))
                cleared = keep(cleared)
                n_cleared = cleared.count()
                if n_cleared:
                    delete(cleared, "alerts")
        if stored_meas is not None and n_weather:
            # Thời tiết của các giờ này vừa đổi (thêm số đọc): ghi lại cột thời tiết của số đo đã ghi ở lô trước.
            refresh = (stored_meas.select("city", "station_id", "ts").join(weather.select("city", "ts"), ["city", "ts"])
                       .join(meas.select("city", "station_id", "ts"), ["city", "station_id", "ts"], "left_anti")
                       .join(weather, ["city", "ts"])
                       .select("city", "station_id", "ts", *config.WEATHER_COLS))
            refresh = keep(refresh)
            n_refresh = refresh.count()
            if n_refresh:
                write(refresh, "measurements")

        elapsed = time.time() - t0
        m = {"messages_in": n_in, "messages_parsed": n_ok, "weather_readings": n_readings, "weather_rows": n_weather,
             "openaq_obs": n_obs, "aq_rows": n_aq, "measurement_rows": n_meas, "alert_rows": n_alerts,
             "alerts_cleared": n_cleared, "weather_refreshed": n_refresh, "seconds": round(elapsed, 3),
             "messages_per_min": round(n_in / elapsed * 60, 1) if elapsed > 0 else None}
        log.info("process_raw %s", m)
        return m
    finally:
        for df in cached:                 # chỉ bỏ cache của chính lô này (không xóa cache của nơi gọi, ví dụ bảng trạm)
            df.unpersist()


def append_metrics(name: str, metrics: dict, path: str = None) -> None:
    """In một dòng JSON và (nếu ghi được) nối vào file METRICS_FILE; dùng cho mục Hiệu năng của báo cáo."""
    line = json.dumps({"job": name, "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), **metrics}, ensure_ascii=False)
    print("METRICS " + line, flush=True)
    path = path or os.getenv("METRICS_FILE", "/checkpoints/metrics.jsonl")
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def make_foreach_batch(write, stations=None, weather_lookup=None, metrics_path=None, threshold=None,
                       obs_lookup=None, readings_lookup=None, meas_lookup=None, delete=None):
    """Tạo hàm cho writeStream.foreachBatch(...). Micro-batch phải có cột `value` (chuỗi JSON)."""
    def handler(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():          # DataFrame.isEmpty chạy trên JVM, không cần Python ở executor
            return
        m = process_raw(batch_df.sparkSession, batch_df, write, weather_lookup, stations, threshold=threshold,
                        obs_lookup=obs_lookup, readings_lookup=readings_lookup, meas_lookup=meas_lookup,
                        delete=delete)
        append_metrics(f"stream_batch_{batch_id}", m, metrics_path)
    return handler

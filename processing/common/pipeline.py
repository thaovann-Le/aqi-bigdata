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
from common.cleaning import attach_weather, dedupe_latest, finalize_measurements

log = logging.getLogger("processing")


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
    weather_lookup: Optional[Callable[[SparkSession, DataFrame], DataFrame]] = None,
    stations: DataFrame = None,
    value_col: str = "value",
    threshold: int = None,
) -> dict:
    """Xử lý một lô message thô. Trả về số liệu (số dòng vào/ra, thời gian) để đưa vào báo cáo hiệu năng."""
    t0 = time.time()
    raw = raw.persist()
    try:
        n_in = raw.count()
        env = sources.parse_envelope(raw, value_col).persist()
        n_ok = env.count()

        weather = sources.realtime_weather(env).persist()
        n_weather = weather.count()
        if n_weather:
            write(weather.select("city", "ts", *config.WEATHER_COLS), "weather_hourly")

        aq = sources.realtime_aq(env).persist()
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

        meas = build_measurements(aq, all_weather).persist()
        n_meas = meas.count()
        n_alerts = 0
        if n_meas:
            write(meas, "measurements")
            alerts = build_alerts(meas, threshold, stations).persist()
            n_alerts = alerts.count()
            if n_alerts:
                write(alerts, "alerts")
        elapsed = time.time() - t0
        m = {"messages_in": n_in, "messages_parsed": n_ok, "weather_rows": n_weather, "aq_rows": n_aq,
             "measurement_rows": n_meas, "alert_rows": n_alerts, "seconds": round(elapsed, 3),
             "messages_per_min": round(n_in / elapsed * 60, 1) if elapsed > 0 else None}
        log.info("process_raw %s", m)
        return m
    finally:
        spark.catalog.clearCache()


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


def make_foreach_batch(write, stations=None, weather_lookup=None, metrics_path=None, threshold=None):
    """Tạo hàm cho writeStream.foreachBatch(...). Micro-batch phải có cột `value` (chuỗi JSON)."""
    def handler(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():          # DataFrame.isEmpty chạy trên JVM, không cần Python ở executor
            return
        m = process_raw(batch_df.sparkSession, batch_df, write, weather_lookup, stations, threshold=threshold)
        append_metrics(f"stream_batch_{batch_id}", m, metrics_path)
    return handler

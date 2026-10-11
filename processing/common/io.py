"""Khởi tạo Spark, nạp bảng tham chiếu trạm, và các "sink" (nơi ghi kết quả).

Sink là một hàm `write(df, table)`; job nào cũng nhận sink làm tham số nên test được mà không cần Cassandra:
  cassandra          ghi vào Cassandra (mặc định khi chạy trên cụm)
  parquet:<thư mục>  ghi Parquet vào <thư mục>/<bảng> (để test hoặc chạy không cần Docker)
  console            in vài dòng đầu (kiểm tra nhanh)
"""
import csv
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import config

log = logging.getLogger("processing")


def build_spark(app_name: str, master: str = None) -> SparkSession:
    """SparkSession với múi giờ Việt Nam (bắt buộc: mọi phép cắt giờ/ngày/mùa dựa vào đây)."""
    b = (SparkSession.builder.appName(app_name)
         .config("spark.sql.session.timeZone", config.TZ)
         .config("spark.ui.showConsoleProgress", "false"))          # giữ log gọn, dòng METRICS dễ đọc
    if os.getenv("SHUFFLE_PARTITIONS"):
        b = b.config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS"))
    elif master and master.startswith("local"):
        b = b.config("spark.sql.shuffle.partitions", "8")           # dữ liệu nhỏ: 200 partition mặc định quá nhiều
    if master:
        b = b.master(master)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_stations(spark: SparkSession, path=config.STATIONS_CSV) -> DataFrame:
    """Đọc stations.csv ở driver rồi tạo DataFrame nhỏ (không phụ thuộc hệ file chung giữa các executor)."""
    with open(path, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    schema = T.StructType([T.StructField(c, T.StringType()) for c in
                           ("station_id", "source", "native_id", "city", "name", "physical_id")])
    return spark.createDataFrame([tuple(r[c.name] for c in schema) for r in rows], schema)


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------
def cassandra_writer(df: DataFrame, table: str) -> None:
    (df.write.format("org.apache.spark.sql.cassandra")
       .options(table=table, keyspace=config.KEYSPACE).mode("append").save())


def parquet_writer(root: str):
    def write(df: DataFrame, table: str) -> None:
        df.write.mode("append").parquet(f"{root.rstrip('/')}/{table}")
    return write


def console_writer(df: DataFrame, table: str) -> None:
    print(f"--- {table} ---")
    df.show(10, truncate=False)


def make_sink(spec: str):
    if spec == "cassandra":
        return cassandra_writer
    if spec == "console":
        return console_writer
    if spec.startswith("parquet:"):
        return parquet_writer(spec.split(":", 1)[1])
    raise ValueError(f"Sink không hợp lệ: {spec!r} (dùng cassandra | console | parquet:<thư mục>)")


def read_cassandra(spark: SparkSession, table: str) -> DataFrame:
    return (spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=table, keyspace=config.KEYSPACE).load())


def read_sink_table(spark: SparkSession, sink_spec: str, table: str) -> DataFrame:
    """Đọc lại bảng từ chính sink đã ghi (job tổng hợp dùng để đọc `measurements`)."""
    if sink_spec == "cassandra":
        return read_cassandra(spark, table)
    if sink_spec.startswith("parquet:"):
        return spark.read.parquet(f"{sink_spec.split(':', 1)[1].rstrip('/')}/{table}")
    raise ValueError(f"Không đọc lại được từ sink {sink_spec!r}")


def weather_lookup_from_cassandra(spark: SparkSession, aq: DataFrame) -> DataFrame:
    """Lấy thời tiết đã lưu trong Cassandra cho các (city, giờ) có trong `aq`.

    Dùng cho streaming khi thời tiết của một giờ đã đến ở micro-batch trước. Điều kiện city + khoảng ts
    được đẩy xuống Cassandra nên chỉ đọc phần nhỏ của bảng.
    """
    w = read_cassandra(spark, "weather_hourly")
    bounds = aq.agg(F.min("ts").alias("lo"), F.max("ts").alias("hi")).first()
    if bounds is None or bounds["lo"] is None:
        return w.limit(0)
    cities = [r["city"] for r in aq.select("city").distinct().collect()]
    return w.where(F.col("city").isin(*cities) & (F.col("ts") >= F.lit(bounds["lo"])) & (F.col("ts") <= F.lit(bounds["hi"])))

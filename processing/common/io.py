"""Khởi tạo Spark, nạp bảng tham chiếu trạm, và các "sink" (nơi ghi kết quả).

Sink là một hàm `write(df, table)`; job nào cũng nhận sink làm tham số nên test được mà không cần Cassandra:
  cassandra          ghi vào Cassandra (mặc định khi chạy trên cụm)
  parquet:<thư mục>  ghi Parquet vào <thư mục>/<bảng> (để test hoặc chạy không cần Docker)
  console            in vài dòng đầu (kiểm tra nhanh)
Streaming còn cần đọc lại trạng thái đã ghi ở các lô trước: make_lookups(sink, stations).
"""
import csv
import logging
import os
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import config
from common.cleaning import hour_ending
from common.sources import path_exists

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


def cassandra_deleter(df: DataFrame, table: str) -> None:
    """Xóa các dòng theo khóa chính (df chỉ chứa cột khóa) qua phiên CQL của Spark Cassandra Connector.

    Chỉ dùng cho vài dòng mỗi lô (gỡ cảnh báo). Lỗi được ghi log chứ không làm dừng streaming: tệ nhất là cảnh báo
    cũ còn lại, như trước khi có bước này.
    """
    rows = df.collect()
    if not rows:
        return
    session = None
    try:
        sc = df.sparkSession.sparkContext
        connector = sc._jvm.com.datastax.spark.connector.cql.CassandraConnector.apply(sc._jsc.sc().getConf())
        session = connector.openSession()
        for r in rows:
            conds = []
            for k, v in r.asDict().items():
                lit = f"{int(v.timestamp() * 1000)}" if hasattr(v, "timestamp") else "'" + str(v).replace("'", "''") + "'"
                conds.append(f"{k} = {lit}")
            session.execute(f"DELETE FROM {config.KEYSPACE}.{table} WHERE {' AND '.join(conds)}")
    except Exception as e:                                  # noqa: BLE001
        log.warning("Không xóa được %d dòng của %s: %s", len(rows), table, e)
    finally:
        if session is not None:
            session.close()


# ---------------------------------------------------------------------------
# Đọc lại trạng thái cho streaming (lookup)
# ---------------------------------------------------------------------------
def _hour_bounds(df: DataFrame, col: str):
    """(đầu, cuối) của khoảng thời gian chứa trọn các giờ (T-1h, T] mà cột `col` của df chạm tới."""
    b = df.agg(F.min(hour_ending(F.col(col))).alias("lo"), F.max(hour_ending(F.col(col))).alias("hi")).first()
    if b is None or b["lo"] is None:
        return None
    return b["lo"] - timedelta(hours=1), b["hi"]


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


def _obs_filter(t: DataFrame, obs: DataFrame) -> DataFrame:
    bounds = _hour_bounds(obs, "ts_obs")
    if bounds is None:
        return t.limit(0)
    keys = obs.select("city", "station_id").distinct().collect()
    return t.where(F.col("city").isin(*{k["city"] for k in keys}) & F.col("station_id").isin(*{k["station_id"] for k in keys})
                   & (F.col("ts_obs") > F.lit(bounds[0])) & (F.col("ts_obs") <= F.lit(bounds[1])))


def _readings_filter(t: DataFrame, readings: DataFrame) -> DataFrame:
    bounds = _hour_bounds(readings, "ts_reading")
    if bounds is None:
        return t.limit(0)
    cities = [r["city"] for r in readings.select("city").distinct().collect()]
    return t.where(F.col("city").isin(*cities) & (F.col("ts_reading") > F.lit(bounds[0]))
                   & (F.col("ts_reading") <= F.lit(bounds[1])))


def obs_lookup_from_cassandra(spark: SparkSession, obs: DataFrame) -> DataFrame:
    """Số đo OpenAQ đã lưu (openaq_observations) của các trạm trong lô, trong các giờ lô chạm tới."""
    return _obs_filter(read_cassandra(spark, "openaq_observations"), obs)


def readings_lookup_from_cassandra(spark: SparkSession, readings: DataFrame) -> DataFrame:
    """Số đọc thời tiết 15 phút đã lưu (weather_readings) của các giờ lô chạm tới."""
    return _readings_filter(read_cassandra(spark, "weather_readings"), readings)


def meas_lookup_from_cassandra(stations: DataFrame):
    """measurements đã lưu (city, station_id, ts, aqi) tại các (city, ts) cho trước.

    Bảng có khóa phân vùng (city, station_id) nên lấy danh sách trạm của thành phố từ bảng tham chiếu (cộng trạm ảo
    model:<city>) để truy vấn đúng phân vùng thay vì quét cả bảng.
    """
    by_city = {}
    for r in stations.select("city", "station_id").collect():
        by_city.setdefault(r["city"], set()).add(r["station_id"])
    for city in list(by_city):
        by_city[city].add(f"model:{city}")

    def lookup(spark: SparkSession, keys: DataFrame) -> DataFrame:
        k = keys.distinct().collect()
        cities = sorted({r["city"] for r in k})
        ids = sorted({s for c in cities for s in by_city.get(c, ())})
        ts = sorted({r["ts"] for r in k})
        t = read_cassandra(spark, "measurements")
        if not ids or not ts:
            return t.select("city", "station_id", "ts", "aqi").limit(0)
        return (t.where(F.col("city").isin(*cities) & F.col("station_id").isin(*ids) & F.col("ts").isin(*ts))
                 .select("city", "station_id", "ts", "aqi"))
    return lookup


def _parquet_lookup(root: str, table: str, filt):
    path = f"{root.rstrip('/')}/{table}"

    def lookup(spark: SparkSession, df: DataFrame) -> DataFrame:
        if not path_exists(spark, f"{path}/*.parquet"):
            return df.limit(0)
        return filt(spark.read.parquet(path), df)
    return lookup


def make_lookups(sink_spec: str, stations: DataFrame) -> dict:
    """Các hàm đọc lại trạng thái cho streaming, theo sink (truyền thẳng vào make_foreach_batch(**...)).

    cassandra: đủ cả 5 (kết quả streaming = batch). parquet: chỉ đọc lại số đo/số đọc/thời tiết (Parquet chỉ ghi
    nối, không sửa được dòng cũ nên không ghi lại thời tiết và không gỡ cảnh báo). console: không có.
    """
    if sink_spec == "cassandra":
        return {"weather_lookup": weather_lookup_from_cassandra, "obs_lookup": obs_lookup_from_cassandra,
                "readings_lookup": readings_lookup_from_cassandra, "meas_lookup": meas_lookup_from_cassandra(stations),
                "delete": cassandra_deleter}
    if sink_spec.startswith("parquet:"):
        root = sink_spec.split(":", 1)[1]

        def weather_filter(t, aq):
            cities = [r["city"] for r in aq.select("city").distinct().collect()]
            return t.where(F.col("city").isin(*cities))
        return {"weather_lookup": _parquet_lookup(root, "weather_hourly", weather_filter),
                "obs_lookup": _parquet_lookup(root, "openaq_observations", _obs_filter),
                "readings_lookup": _parquet_lookup(root, "weather_readings", _readings_filter)}
    return {}

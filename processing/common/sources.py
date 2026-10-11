"""Đọc và chuẩn hóa từng nguồn dữ liệu về dạng bảng chung.

Hai họ hàm:
  history_*   đọc dữ liệu lịch sử trong MinIO (bucket `history`, do B nạp)
  realtime_*  đọc message JSON trong Kafka / bucket `raw` (cùng lớp vỏ `envelope` của producer)

Đầu ra của mọi hàm có chung quy ước:
  ts           = đầu giờ (timestamp tuyệt đối; giờ Việt Nam khi hiển thị)
  station_id   = '<nguồn>:<mã>' (openaq:2161292, aqicn:1583, model:hanoi)
  đơn vị       = µg/m³ cho chất ô nhiễm; °C, %, m/s, độ, mm, hPa cho thời tiết
Yêu cầu: SparkSession đã đặt spark.sql.session.timeZone=Asia/Ho_Chi_Minh (xem common/io.py),
vì giờ Open-Meteo là giờ địa phương không kèm múi giờ.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import config
from common.cleaning import dedupe_latest, is_mass_conc, null_out_of_range

KMH_TO_MS = 1 / 3.6

# ---------------------------------------------------------------------------
# Open-Meteo lịch sử: JSON dạng cột -> dạng dòng
# ---------------------------------------------------------------------------
WEATHER_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation",
                "wind_speed_10m", "wind_direction_10m", "surface_pressure"]
AQ_VARS = ["pm2_5", "pm10", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "carbon_monoxide", "us_aqi"]


def _openmeteo_schema(variables):
    hourly = T.StructType([T.StructField("time", T.ArrayType(T.StringType()))]
                          + [T.StructField(v, T.ArrayType(T.DoubleType())) for v in variables])
    return T.StructType([T.StructField("point_id", T.StringType()), T.StructField("hourly", hourly)])


def _explode_hourly(df: DataFrame, variables) -> DataFrame:
    """hourly.{time, var1, var2...} (các mảng cùng độ dài) -> mỗi giờ một dòng."""
    zipped = F.arrays_zip(*[F.col(f"hourly.{c}").alias(c) for c in ["time"] + variables])
    exploded = df.select(F.col("point_id").alias("city"), F.explode(zipped).alias("h"))
    return exploded.select("city", *[F.col(f"h.{c}").alias(c) for c in ["time"] + variables])


def history_weather(spark: SparkSession, base: str, cities, years: str = "*") -> DataFrame:
    """`years` là glob Hadoop, ví dụ "*" hoặc "{2024,2025}".

    Thời tiết theo giờ -> (city, ts, temperature, humidity, wind_speed m/s, wind_direction, precipitation, pressure).
    """
    path = f"{base}/openmeteo/weather/point=*/year={years}.json"
    raw = spark.read.schema(_openmeteo_schema(WEATHER_VARS)).option("multiLine", True).json(path)
    df = _explode_hourly(raw.where(F.col("point_id").isin(*cities)), WEATHER_VARS)
    df = df.select(
        "city",
        F.date_trunc("hour", F.to_timestamp("time")).alias("ts"),
        F.col("temperature_2m").alias("temperature"),
        F.col("relative_humidity_2m").alias("humidity"),
        (F.col("wind_speed_10m") * KMH_TO_MS).alias("wind_speed"),
        F.col("wind_direction_10m").alias("wind_direction"),
        "precipitation",
        F.col("surface_pressure").alias("pressure"),
    )
    return null_out_of_range(df, config.WEATHER_RANGES).dropDuplicates(["city", "ts"])


def history_model_aq(spark: SparkSession, base: str, cities, years: str = "*") -> DataFrame:
    """Số mô hình CAMS của Open-Meteo (không phải số đo) -> định dạng measurements, is_model = true."""
    path = f"{base}/openmeteo/air_quality/point=*/year={years}.json"
    raw = spark.read.schema(_openmeteo_schema(AQ_VARS)).option("multiLine", True).json(path)
    df = _explode_hourly(raw.where(F.col("point_id").isin(*cities)), AQ_VARS)
    df = df.withColumn("ts", F.date_trunc("hour", F.to_timestamp("time"))).dropDuplicates(["city", "ts"])
    return _model_rows(df, pm25="pm2_5", pm10="pm10", no2="nitrogen_dioxide", so2="sulphur_dioxide",
                       o3="ozone", co="carbon_monoxide", aqi="us_aqi")


def _model_rows(df: DataFrame, pm25, pm10, no2, so2, o3, co, aqi) -> DataFrame:
    return df.select(
        "city",
        F.concat(F.lit("model:"), F.col("city")).alias("station_id"),
        "ts",
        F.col(pm25).alias("pm25"), F.col(pm10).alias("pm10"), F.col(o3).alias("o3"),
        F.col(no2).alias("no2"), F.col(so2).alias("so2"), F.col(co).alias("co"),
        F.col(aqi).cast("int").alias("aqi"),
        F.lit("openmeteo_aq").alias("source"),
        F.lit(True).alias("is_model"),
    )


# ---------------------------------------------------------------------------
# OpenAQ lịch sử: CSV dạng dài (mỗi dòng một số đo) -> trung bình theo giờ, dạng rộng
# ---------------------------------------------------------------------------
OPENAQ_SCHEMA = T.StructType([
    T.StructField("location_id", T.LongType()), T.StructField("sensors_id", T.LongType()),
    T.StructField("location", T.StringType()), T.StructField("datetime", T.StringType()),
    T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
    T.StructField("parameter", T.StringType()), T.StructField("units", T.StringType()),
    T.StructField("value", T.DoubleType()),
])


def hourly_wide(obs: DataFrame) -> DataFrame:
    """obs: (city, station_id, ts_obs, parameter, value) đã lọc đơn vị -> trung bình theo giờ, mỗi chất một cột.

    Giá trị ngoài khoảng hợp lệ (gồm -999 = thiếu) bị loại TRƯỚC khi lấy trung bình, nên một số đo rác
    không kéo lệch cả giờ. Trạm đo 5 phút một lần được gộp về giờ ở bước này.
    """
    aggs = []
    for p, (lo, hi) in config.POLLUTANT_RANGES.items():
        aggs.append(F.avg(F.when((F.col("parameter") == p) & F.col("value").between(lo, hi), F.col("value"))).alias(p))
    return (obs.withColumn("ts", F.date_trunc("hour", F.col("ts_obs")))
               .groupBy("city", "station_id", "ts").agg(*aggs)
               .withColumn("aqi", F.lit(None).cast("int"))
               .withColumn("source", F.lit("openaq"))
               .withColumn("is_model", F.lit(False)))


def history_openaq(spark: SparkSession, base: str, stations: DataFrame, cities, years: str = "*") -> DataFrame:
    path = f"{base}/openaq/locationid=*/year={years}/*.csv.gz"
    raw = spark.read.option("header", True).schema(OPENAQ_SCHEMA).csv(path)
    st = (stations.where((F.col("source") == "openaq") & F.col("city").isin(*cities))
                  .select("station_id", "city"))
    obs = (raw.where(F.col("parameter").isin(*config.POLLUTANTS) & is_mass_conc(F.col("units")))
              .withColumn("station_id", F.concat(F.lit("openaq:"), F.col("location_id").cast("string")))
              .join(F.broadcast(st), "station_id")                  # inner join: bỏ trạm ngoài hai thành phố
              .select("city", "station_id", F.to_timestamp("datetime").alias("ts_obs"), "parameter", "value"))
    return hourly_wide(obs)


# ---------------------------------------------------------------------------
# Real-time: message trong Kafka và file JSONL trong bucket raw (cùng lớp vỏ)
# ---------------------------------------------------------------------------
_RESULT = T.StructType([
    T.StructField("datetime", T.StructType([T.StructField("utc", T.StringType())])),
    T.StructField("value", T.DoubleType()),
    T.StructField("parameter", T.StringType()),
    T.StructField("units", T.StringType()),
])
_CURRENT = T.StructType(
    [T.StructField("time", T.StringType())]
    + [T.StructField(v, T.DoubleType()) for v in AQ_VARS + WEATHER_VARS]
)
ENVELOPE_SCHEMA = T.StructType([
    T.StructField("source", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("station_id", T.LongType()),
    T.StructField("fetched_at", T.StringType()),
    T.StructField("payload", T.StructType([
        T.StructField("aqi", T.StringType()),            # AQICN trả "-" khi thiếu, nên đọc dạng chuỗi rồi ép int
        T.StructField("time", T.StructType([T.StructField("iso", T.StringType())])),
        T.StructField("results", T.ArrayType(_RESULT)),
        T.StructField("current", _CURRENT),
    ])),
])


def parse_envelope(df: DataFrame, value_col="value") -> DataFrame:
    """Cột `value` (chuỗi JSON hoặc nhị phân từ Kafka) -> các cột source, city, station_id, fetched_at, payload."""
    parsed = df.select(F.from_json(F.col(value_col).cast("string"), ENVELOPE_SCHEMA).alias("e"))
    return (parsed.where(F.col("e").isNotNull() & F.col("e.source").isNotNull())      # message hỏng bị bỏ
                  .select("e.source", "e.city", "e.station_id",
                          F.to_timestamp("e.fetched_at").alias("fetched_at"), "e.payload"))


def realtime_aqicn(env: DataFrame) -> DataFrame:
    """AQICN: `aqi` là US AQI tổng hợp; iaqi.* là chỉ số AQI con (không phải µg/m³) nên KHÔNG đưa vào cột pm25/pm10."""
    df = (env.where(F.col("source") == "aqicn")
             .select("city", F.concat(F.lit("aqicn:"), F.col("station_id").cast("string")).alias("station_id"),
                     F.date_trunc("hour", F.to_timestamp("payload.time.iso")).alias("ts"),
                     F.col("payload.aqi").cast("int").alias("aqi"), "fetched_at")
             .where(F.col("ts").isNotNull() & F.col("aqi").isNotNull()))
    df = dedupe_latest(df, ["city", "station_id", "ts"]).drop("fetched_at")
    for p in config.POLLUTANTS:
        df = df.withColumn(p, F.lit(None).cast("double"))
    return df.withColumn("source", F.lit("aqicn")).withColumn("is_model", F.lit(False))


def realtime_openaq(env: DataFrame, max_age_hours: int = None) -> DataFrame:
    max_age = config.MAX_AGE_HOURS if max_age_hours is None else max_age_hours
    obs = (env.where(F.col("source") == "openaq")
              .select("city", F.concat(F.lit("openaq:"), F.col("station_id").cast("string")).alias("station_id"),
                      "fetched_at", F.explode("payload.results").alias("r"))
              .select("city", "station_id", "fetched_at",
                      F.to_timestamp("r.datetime.utc").alias("ts_obs"),
                      F.col("r.parameter").alias("parameter"), F.col("r.value").alias("value"),
                      F.col("r.units").alias("units")))
    obs = obs.where(F.col("parameter").isin(*config.POLLUTANTS) & is_mass_conc(F.col("units")))
    # Bỏ số đo cũ lẫn trong payload "latest" (có số đo cách đây nhiều tháng).
    obs = obs.where(F.col("ts_obs") >= F.col("fetched_at") - F.expr(f"INTERVAL {int(max_age)} HOURS"))
    # Khử trùng ở mức số đo (không phải mức dòng đã pivot) để không mất chất nào khi
    # hai lần gọi API cùng giờ mang các chất khác nhau.
    obs = dedupe_latest(obs, ["city", "station_id", "parameter", "ts_obs"]).drop("fetched_at", "units")
    return hourly_wide(obs)


def realtime_model_aq(env: DataFrame) -> DataFrame:
    df = (env.where(F.col("source") == "openmeteo_aq")
             .select("city", F.date_trunc("hour", F.to_timestamp("payload.current.time")).alias("ts"),
                     F.col("payload.current.*"), "fetched_at")
             .where(F.col("ts").isNotNull()))
    df = dedupe_latest(df, ["city", "ts"]).drop("fetched_at")
    return _model_rows(df, pm25="pm2_5", pm10="pm10", no2="nitrogen_dioxide", so2="sulphur_dioxide",
                       o3="ozone", co="carbon_monoxide", aqi="us_aqi")


def realtime_aq(env: DataFrame, max_age_hours: int = None) -> DataFrame:
    """Hợp nhất AQICN + OpenAQ + mô hình; cột: city, station_id, ts, pm25..co, aqi, source, is_model."""
    cols = ["city", "station_id", "ts", *config.POLLUTANTS, "aqi", "source", "is_model"]
    parts = [realtime_aqicn(env), realtime_openaq(env, max_age_hours), realtime_model_aq(env)]
    out = parts[0].select(*cols)
    for p in parts[1:]:
        out = out.unionByName(p.select(*cols))
    return out


def realtime_weather(env: DataFrame) -> DataFrame:
    """Thời tiết hiện tại của Open-Meteo -> một dòng mỗi (city, giờ); giữ lần đo mới nhất trong giờ."""
    c = "payload.current"
    df = (env.where(F.col("source") == "openmeteo")
             .select("city", F.date_trunc("hour", F.to_timestamp(f"{c}.time")).alias("ts"),
                     F.col(f"{c}.temperature_2m").alias("temperature"),
                     F.col(f"{c}.relative_humidity_2m").alias("humidity"),
                     (F.col(f"{c}.wind_speed_10m") * KMH_TO_MS).alias("wind_speed"),
                     F.col(f"{c}.wind_direction_10m").alias("wind_direction"),
                     F.col(f"{c}.precipitation").alias("precipitation"),
                     F.col(f"{c}.surface_pressure").alias("pressure"), "fetched_at")
             .where(F.col("ts").isNotNull()))
    df = dedupe_latest(df, ["city", "ts"]).drop("fetched_at")
    return null_out_of_range(df, config.WEATHER_RANGES)

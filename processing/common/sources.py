"""Đọc và chuẩn hóa từng nguồn dữ liệu về dạng bảng chung.

Hai họ hàm:
  history_*   đọc dữ liệu lịch sử trong MinIO (bucket `history`, do B nạp), kể cả thư mục backfill/ (tải bù)
  realtime_*  đọc message JSON trong Kafka / bucket `raw` (cùng lớp vỏ `envelope` của producer)

Đầu ra của mọi hàm có chung quy ước:
  ts           = mốc giờ T: số liệu của khoảng (T-1h, T] hoặc tức thời lúc T (xem cleaning.hour_ending)
  station_id   = '<nguồn>:<mã>' (openaq:2161292, aqicn:1583, model:hanoi)
  đơn vị       = µg/m³ cho chất ô nhiễm; °C, %, m/s, độ, mm, hPa cho thời tiết
Yêu cầu: SparkSession đã đặt spark.sql.session.timeZone=Asia/Ho_Chi_Minh (xem common/io.py),
vì giờ Open-Meteo là giờ địa phương không kèm múi giờ.
"""
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import config
from common.cleaning import dedupe_latest, hour_ending, is_mass_conc, merge_stored, null_out_of_range

KMH_TO_MS = 1 / 3.6

# Tải bù (ingestion/backfill.py của B, backfill.zip): cùng cột/cùng dạng với lịch sử nhưng KHÔNG có thư mục year=.
BACKFILL = {
    "openaq": "backfill/openaq/locationid=*/*.csv.gz",
    "weather": "backfill/openmeteo/weather/point=*/*.json",
    "air_quality": "backfill/openmeteo/air_quality/point=*/*.json",
}


def path_exists(spark: SparkSession, pattern: str) -> bool:
    """Glob Hadoop (s3a://, file://, đường dẫn cục bộ) có khớp file nào không."""
    try:
        jvm = spark.sparkContext._jvm
        path = jvm.org.apache.hadoop.fs.Path(pattern)
        found = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).globStatus(path)
        return found is not None and len(found) > 0
    except Exception:          # hệ file không liệt kê được (ví dụ Windows thiếu winutils) -> coi như không có
        return False


def _read_union(spark: SparkSession, paths, read, cols) -> DataFrame:
    """Đọc từng đường dẫn riêng rồi hợp lại: lịch sử (…/year=…) và backfill khác cấu trúc thư mục, đọc chung một lần
    Spark sẽ báo xung đột cột phân vùng."""
    return reduce(DataFrame.unionByName, [read(p).select(*cols) for p in paths])


def _paths(spark: SparkSession, base: str, main: str, kind: str, backfill: bool):
    extra = f"{base}/{BACKFILL[kind]}"
    return [main] + ([extra] if backfill and path_exists(spark, extra) else [])


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


def _read_openmeteo(spark: SparkSession, paths, variables) -> DataFrame:
    schema = _openmeteo_schema(variables)
    return _read_union(spark, paths, lambda p: spark.read.schema(schema).option("multiLine", True).json(p),
                       ["point_id", "hourly"])


def _explode_hourly(df: DataFrame, variables) -> DataFrame:
    """hourly.{time, var1, var2...} (các mảng cùng độ dài) -> mỗi giờ một dòng."""
    zipped = F.arrays_zip(*[F.col(f"hourly.{c}").alias(c) for c in ["time"] + variables])
    exploded = df.select(F.col("point_id").alias("city"), F.explode(zipped).alias("h"))
    return exploded.select("city", *[F.col(f"h.{c}").alias(c) for c in ["time"] + variables])


def _only_cities(col, cities):
    """Điều kiện lọc theo thành phố; cities = None nghĩa là lấy mọi điểm có trong dữ liệu (63 tỉnh/thành)."""
    return F.lit(True) if cities is None else col.isin(*cities)


def history_weather(spark: SparkSession, base: str, cities, years: str = "*", backfill: bool = False) -> DataFrame:
    """`years` là glob Hadoop, ví dụ "*" hoặc "{2024,2025}". `cities` = None: mọi điểm. `backfill`: đọc thêm backfill/.

    Thời tiết theo giờ -> (city, ts, temperature, humidity, wind_speed m/s, wind_direction, precipitation, pressure).
    Open-Meteo hourly: biến tức thời lúc ts, lượng mưa là tổng của giờ trước ts (cùng quy ước với realtime_weather).
    """
    paths = _paths(spark, base, f"{base}/openmeteo/weather/point=*/year={years}.json", "weather", backfill)
    raw = _read_openmeteo(spark, paths, WEATHER_VARS)
    df = _explode_hourly(raw.where(_only_cities(F.col("point_id"), cities)), WEATHER_VARS)
    df = df.select(
        "city",
        hour_ending(F.to_timestamp("time")).alias("ts"),
        F.col("temperature_2m").alias("temperature"),
        F.col("relative_humidity_2m").alias("humidity"),
        (F.col("wind_speed_10m") * KMH_TO_MS).alias("wind_speed"),
        F.col("wind_direction_10m").alias("wind_direction"),
        "precipitation",
        F.col("surface_pressure").alias("pressure"),
    )
    return null_out_of_range(df, config.WEATHER_RANGES).dropDuplicates(["city", "ts"])


def history_model_aq(spark: SparkSession, base: str, cities, years: str = "*", backfill: bool = False) -> DataFrame:
    """Số mô hình CAMS của Open-Meteo (không phải số đo) -> định dạng measurements, is_model = true."""
    paths = _paths(spark, base, f"{base}/openmeteo/air_quality/point=*/year={years}.json", "air_quality", backfill)
    raw = _read_openmeteo(spark, paths, AQ_VARS)
    df = _explode_hourly(raw.where(_only_cities(F.col("point_id"), cities)), AQ_VARS)
    df = df.withColumn("ts", hour_ending(F.to_timestamp("time"))).dropDuplicates(["city", "ts"])
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

    Giờ T gồm các số đo trong (T-1h, T] (hour_ending): trạm đo theo giờ giữ nguyên mốc, trạm đo 5 phút gộp 12 mẫu
    của giờ kết thúc lúc T - đúng như trung bình giờ do chính OpenAQ tính. Giá trị ngoài khoảng hợp lệ (gồm -999 =
    thiếu) bị loại TRƯỚC khi lấy trung bình, nên một số đo rác không kéo lệch cả giờ.
    """
    aggs = []
    for p, (lo, hi) in config.POLLUTANT_RANGES.items():
        aggs.append(F.avg(F.when((F.col("parameter") == p) & F.col("value").between(lo, hi), F.col("value"))).alias(p))
    return (obs.withColumn("ts", hour_ending(F.col("ts_obs")))
               .groupBy("city", "station_id", "ts").agg(*aggs)
               .withColumn("aqi", F.lit(None).cast("int"))
               .withColumn("source", F.lit("openaq"))
               .withColumn("is_model", F.lit(False)))


def history_openaq(spark: SparkSession, base: str, stations: DataFrame, cities, years: str = "*",
                   backfill: bool = False) -> DataFrame:
    paths = _paths(spark, base, f"{base}/openaq/locationid=*/year={years}/*.csv.gz", "openaq", backfill)
    raw = _read_union(spark, paths, lambda p: spark.read.option("header", True).schema(OPENAQ_SCHEMA).csv(p),
                      OPENAQ_SCHEMA.fieldNames())
    st = (stations.where((F.col("source") == "openaq") & _only_cities(F.col("city"), cities))
                  .select("station_id", "city"))
    obs = (raw.where(F.col("parameter").isin(*config.POLLUTANTS) & is_mass_conc(F.col("units")))
              .withColumn("station_id", F.concat(F.lit("openaq:"), F.col("location_id").cast("string")))
              .join(F.broadcast(st), "station_id")                  # inner join: bỏ trạm ngoài hai thành phố
              .select("city", "station_id", F.to_timestamp("datetime").alias("ts_obs"), "parameter", "value"))
    # backfill trùng giờ với lịch sử/real-time: cùng (trạm, chất, thời điểm) chỉ lấy một giá trị
    return hourly_wide(obs.dropDuplicates(["station_id", "parameter", "ts_obs"]))


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
    [T.StructField("time", T.StringType()), T.StructField("interval", T.IntegerType())]
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


def _not_future(ts_col, fetched_col="fetched_at"):
    """Thời điểm đo không được sau lúc gọi API quá MAX_FUTURE_MINUTES (sai đồng hồ)."""
    return ts_col <= F.col(fetched_col) + F.expr(f"INTERVAL {int(config.MAX_FUTURE_MINUTES)} MINUTES")


def parse_envelope(df: DataFrame, value_col="value") -> DataFrame:
    """Cột `value` (chuỗi JSON hoặc nhị phân từ Kafka) -> các cột source, city, station_id, fetched_at, payload."""
    parsed = df.select(F.from_json(F.col(value_col).cast("string"), ENVELOPE_SCHEMA).alias("e"))
    return (parsed.where(F.col("e").isNotNull() & F.col("e.source").isNotNull())      # message hỏng bị bỏ
                  .select("e.source", "e.city", "e.station_id",
                          F.to_timestamp("e.fetched_at").alias("fetched_at"), "e.payload"))


def realtime_aqicn(env: DataFrame) -> DataFrame:
    """AQICN: `aqi` là US AQI tổng hợp; iaqi.* là chỉ số AQI con (không phải µg/m³) nên KHÔNG đưa vào cột pm25/pm10.

    Số đo AQICN thường đến trễ nhiều giờ (dữ liệu thật: trung vị ~16 giờ) -> luôn ghi theo time.iso (lúc đo).
    """
    measured = F.to_timestamp("payload.time.iso")
    df = (env.where(F.col("source") == "aqicn")
             .select("city", F.concat(F.lit("aqicn:"), F.col("station_id").cast("string")).alias("station_id"),
                     hour_ending(measured).alias("ts"), measured.alias("_measured"),
                     F.col("payload.aqi").cast("int").alias("aqi"), "fetched_at")
             .where(F.col("ts").isNotNull() & F.col("aqi").isNotNull() & _not_future(F.col("_measured")))
             .drop("_measured"))
    df = dedupe_latest(df, ["city", "station_id", "ts"]).drop("fetched_at")
    for p in config.POLLUTANTS:
        df = df.withColumn(p, F.lit(None).cast("double"))
    return df.withColumn("source", F.lit("aqicn")).withColumn("is_model", F.lit(False))


def realtime_openaq_obs(env: DataFrame, max_age_hours: int = None) -> DataFrame:
    """Từng số đo OpenAQ hợp lệ của lô (cột config.OBSERVATION_COLS) - cũng là dòng lưu vào bảng openaq_observations.

    Lọc: chỉ chất ô nhiễm có đơn vị khối lượng; bỏ số đo cũ hơn MAX_AGE_HOURS (payload "latest" lẫn số đo của cảm
    biến đã chết từ nhiều tháng trước) và số đo ở tương lai. Khử trùng ở mức số đo (không phải mức dòng đã pivot)
    để không mất chất nào khi hai lần gọi API cùng giờ mang các chất khác nhau.
    """
    max_age = config.MAX_AGE_HOURS if max_age_hours is None else max_age_hours
    obs = (env.where(F.col("source") == "openaq")
              .select("city", F.concat(F.lit("openaq:"), F.col("station_id").cast("string")).alias("station_id"),
                      "fetched_at", F.explode("payload.results").alias("r"))
              .select("city", "station_id", "fetched_at",
                      F.to_timestamp("r.datetime.utc").alias("ts_obs"),
                      F.col("r.parameter").alias("parameter"), F.col("r.value").alias("value"),
                      F.col("r.units").alias("units")))
    obs = obs.where(F.col("parameter").isin(*config.POLLUTANTS) & is_mass_conc(F.col("units")))
    obs = obs.where((F.col("ts_obs") >= F.col("fetched_at") - F.expr(f"INTERVAL {int(max_age)} HOURS"))
                    & _not_future(F.col("ts_obs")))
    return dedupe_latest(obs, ["city", "station_id", "parameter", "ts_obs"]).select(*config.OBSERVATION_COLS)


def realtime_openaq(env: DataFrame, max_age_hours: int = None, stored_obs: DataFrame = None) -> DataFrame:
    """Số đo OpenAQ -> (trạm, giờ) dạng rộng. `stored_obs`: số đo đã lưu ở các lô trước (streaming) - gộp vào để
    trung bình giờ và AQI luôn tính trên đủ số đo của giờ, như khi chạy batch một lần."""
    obs = realtime_openaq_obs(env, max_age_hours)
    if stored_obs is not None:
        obs = merge_stored(obs, stored_obs, ["city", "station_id", "parameter", "ts_obs"], ["city", "station_id"],
                           "ts_obs")
    return hourly_wide(obs.drop("fetched_at"))


def realtime_model_aq(env: DataFrame) -> DataFrame:
    df = (env.where(F.col("source") == "openmeteo_aq")
             .select("city", hour_ending(F.to_timestamp("payload.current.time")).alias("ts"),
                     F.col("payload.current.*"), "fetched_at")
             .where(F.col("ts").isNotNull()))
    df = dedupe_latest(df, ["city", "ts"]).drop("fetched_at")
    return _model_rows(df, pm25="pm2_5", pm10="pm10", no2="nitrogen_dioxide", so2="sulphur_dioxide",
                       o3="ozone", co="carbon_monoxide", aqi="us_aqi")


def realtime_aq(env: DataFrame, max_age_hours: int = None, stored_obs: DataFrame = None) -> DataFrame:
    """Hợp nhất AQICN + OpenAQ + mô hình; cột: city, station_id, ts, pm25..co, aqi, source, is_model."""
    cols = ["city", "station_id", "ts", *config.POLLUTANTS, "aqi", "source", "is_model"]
    parts = [realtime_aqicn(env), realtime_openaq(env, max_age_hours, stored_obs), realtime_model_aq(env)]
    out = parts[0].select(*cols)
    for p in parts[1:]:
        out = out.unionByName(p.select(*cols))
    return out


def realtime_weather_readings(env: DataFrame) -> DataFrame:
    """Số đọc `current` của Open-Meteo (15 phút một lần) -> cột config.READING_COLS; lưu vào bảng weather_readings.

    Biến tức thời là giá trị lúc current.time; precipitation là tổng của `interval` giây trước đó (900 = 15 phút).
    """
    c = "payload.current"
    df = (env.where(F.col("source") == "openmeteo")
             .select("city", F.to_timestamp(f"{c}.time").alias("ts_reading"),
                     F.coalesce(F.col(f"{c}.interval"), F.lit(900)).alias("interval_sec"),
                     F.col(f"{c}.temperature_2m").alias("temperature"),
                     F.col(f"{c}.relative_humidity_2m").alias("humidity"),
                     (F.col(f"{c}.wind_speed_10m") * KMH_TO_MS).alias("wind_speed"),
                     F.col(f"{c}.wind_direction_10m").alias("wind_direction"),
                     F.col(f"{c}.precipitation").alias("precipitation"),
                     F.col(f"{c}.surface_pressure").alias("pressure"), "fetched_at")
             .where(F.col("ts_reading").isNotNull()))
    df = null_out_of_range(df, config.WEATHER_RANGES)
    return dedupe_latest(df, ["city", "ts_reading"]).select(*config.READING_COLS)


def weather_from_readings(readings: DataFrame) -> DataFrame:
    """Số đọc 15 phút -> thời tiết theo giờ, cùng quy ước với Open-Meteo hourly (dữ liệu lịch sử, dữ liệu huấn luyện):

      biến tức thời lúc T = số đọc mới nhất trong (T-1h, T] (thường chính là số đọc lúc T);
      precipitation của T  = tổng mưa trong (T-1h, T]; thiếu số đọc thì quy đổi theo số đọc có
                             (tổng × số đọc mong đợi / số đọc có) thay vì để tổng 15 phút giả làm tổng giờ.
    """
    r = readings.withColumn("ts", hour_ending(F.col("ts_reading")))
    inst = [F.max_by(F.col(c), F.col("ts_reading")).alias(c) for c in config.WEATHER_COLS if c != "precipitation"]
    rain = F.sum("precipitation") * (F.lit(3600.0) / F.max("interval_sec")) / F.count("precipitation")
    out = r.groupBy("city", "ts").agg(*inst, rain.alias("precipitation"))
    return null_out_of_range(out.select("city", "ts", *config.WEATHER_COLS), config.WEATHER_RANGES)


def realtime_weather(env: DataFrame, stored_readings: DataFrame = None) -> DataFrame:
    """Thời tiết theo giờ của lô. `stored_readings`: số đọc đã lưu ở các lô trước (streaming) - gộp vào để mỗi giờ
    tính trên đủ 4 số đọc như batch."""
    rd = realtime_weather_readings(env)
    if stored_readings is not None:
        rd = merge_stored(rd, stored_readings, ["city", "ts_reading"], ["city"], "ts_reading")
    return weather_from_readings(rd)

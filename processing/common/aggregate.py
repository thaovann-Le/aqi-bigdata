"""Tổng hợp theo giờ / ngày / tháng / mùa cho từng thành phố -> bảng `daily_agg`.

Quy tắc (ghi vào docs/processing.md):
  1. Chỉ dùng dòng có AQI. Số đo trạm (data_basis='measured') và số mô hình ('model') tách thành hai chuỗi.
  2. Giờ-thành phố: mỗi TRẠM VẬT LÝ tính một lần (AQICN và OpenAQ cùng một trạm không bị đếm đôi),
     rồi lấy trung bình giữa các trạm.
  3. Ngày/tháng/mùa: trung bình các giờ-thành phố có dữ liệu; max/min là max/min của các giá trị giờ-thành phố đó.
  4. Ngày/tháng cắt theo giờ Việt Nam (cần spark.sql.session.timeZone=Asia/Ho_Chi_Minh).
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import config

COLS = ["city", "period_type", "data_basis", "period_start", "label",
        "avg_aqi", "max_aqi", "min_aqi", "avg_pm25", "avg_pm10", "record_count"]


def _season_map(spark: SparkSession) -> DataFrame:
    rows = []
    for city, seasons in config.SEASONS.items():
        for name, start, end in seasons:
            months = list(range(start, end + 1)) if start <= end else list(range(start, 13)) + list(range(1, end + 1))
            rows += [(city, m, name, start) for m in months]
    schema = T.StructType([T.StructField("city", T.StringType()), T.StructField("month", T.IntegerType()),
                           T.StructField("season", T.StringType()), T.StructField("start_month", T.IntegerType())])
    return spark.createDataFrame(rows, schema)


def city_hourly(meas: DataFrame, stations: DataFrame) -> DataFrame:
    """(city, data_basis, ts) -> avg/max/min AQI, avg PM2.5/PM10, record_count = số trạm vật lý."""
    phys = stations.select("station_id", "physical_id")
    m = (meas.where(F.col("aqi").isNotNull())
             .withColumn("data_basis", F.when(F.col("is_model"), "model").otherwise("measured"))
             .join(F.broadcast(phys), "station_id", "left")
             .withColumn("physical_id", F.coalesce("physical_id", "station_id")))
    per_station = (m.groupBy("city", "data_basis", "physical_id", "ts")
                    .agg(F.avg("aqi").alias("aqi"), F.avg("pm25").alias("pm25"), F.avg("pm10").alias("pm10")))
    return (per_station.groupBy("city", "data_basis", "ts")
            .agg(F.avg("aqi").alias("avg_aqi"), F.max("aqi").alias("max_aqi"), F.min("aqi").alias("min_aqi"),
                 F.avg("pm25").alias("avg_pm25"), F.avg("pm10").alias("avg_pm10"),
                 F.count("*").cast("int").alias("record_count")))


def _roll_up(hourly: DataFrame, period_start, label, period_type: str) -> DataFrame:
    h = hourly.withColumn("period_start", period_start).withColumn("label", label)
    return (h.groupBy("city", "data_basis", "period_start", "label")
             .agg(F.avg("avg_aqi").alias("avg_aqi"), F.max("avg_aqi").alias("max_aqi"), F.min("avg_aqi").alias("min_aqi"),
                  F.avg("avg_pm25").alias("avg_pm25"), F.avg("avg_pm10").alias("avg_pm10"),
                  F.count("*").cast("int").alias("record_count"))
             .withColumn("period_type", F.lit(period_type)))


def aggregate_city(spark: SparkSession, meas: DataFrame, stations: DataFrame) -> DataFrame:
    """Trả về DataFrame đúng cột bảng daily_agg gồm cả 4 period_type."""
    # Đọc lại từ Parquet có thể có dòng trùng khóa (append nhiều lần); Cassandra thì đã gộp sẵn.
    meas = meas.dropDuplicates(["city", "station_id", "ts"])
    hourly = city_hourly(meas, stations).persist()
    hour_rows = (hourly.withColumnRenamed("ts", "period_start").withColumn("label", F.lit(None).cast("string"))
                       .withColumn("period_type", F.lit("hour")))
    day = _roll_up(hourly, F.date_trunc("day", "ts"), F.date_format("ts", "yyyy-MM-dd"), "day")
    month = _roll_up(hourly, F.date_trunc("month", "ts"), F.date_format("ts", "yyyy-MM"), "month")

    seasons = _season_map(spark)
    hs = (hourly.withColumn("month", F.month("ts")).withColumn("year", F.year("ts"))
                .join(F.broadcast(seasons), ["city", "month"], "inner"))
    # Mùa vòng qua năm mới (ví dụ đông Hà Nội 11->1): tháng 1 thuộc mùa bắt đầu từ tháng 11 năm trước.
    start_year = F.when(F.col("month") < F.col("start_month"), F.col("year") - 1).otherwise(F.col("year"))
    start = F.to_timestamp(F.format_string("%04d-%02d-01 00:00:00", start_year, F.col("start_month")))
    season = _roll_up(hs, start, F.col("season"), "season")

    return (hour_rows.select(*COLS)
            .unionByName(day.select(*COLS)).unionByName(month.select(*COLS)).unionByName(season.select(*COLS)))

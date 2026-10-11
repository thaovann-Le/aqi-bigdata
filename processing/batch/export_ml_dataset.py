"""Xuất bộ dữ liệu phẳng cho phần D (phân tích & ML): mỗi dòng là một (thành phố, giờ).

Đọc `measurements` + `weather_hourly` (từ Cassandra hoặc Parquet), ghi CSV (một file) và Parquet.

Cột đầu ra (giờ theo Asia/Ho_Chi_Minh):
  city, ts_local ('YYYY-MM-DD HH:00', giờ VN), ts_utc (ISO 8601),
  aqi, pm25, pm10, n_stations      <- số ĐO tại trạm (trung bình các trạm vật lý trong giờ); null nếu không có trạm
  aqi_model, pm25_model, pm10_model <- số MÔ HÌNH CAMS của Open-Meteo (dùng để lấp chỗ trống thì phải đánh dấu)
  temperature, humidity, wind_speed (m/s), wind_direction, precipitation, pressure

  spark-submit export_ml_dataset.py --out /opt/processing/_runtime/ml_dataset
  spark-submit export_ml_dataset.py --sink parquet:/tmp/out --out /tmp/ml_dataset
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import config, io  # noqa: E402
from common.aggregate import city_hourly  # noqa: E402
from common.pipeline import append_metrics  # noqa: E402

COLS = ["city", "ts_local", "ts_utc", "aqi", "pm25", "pm10", "n_stations", "aqi_model", "pm25_model", "pm10_model",
        *config.WEATHER_COLS]


def build_dataset(spark: SparkSession, meas: DataFrame, weather: DataFrame, stations: DataFrame) -> DataFrame:
    meas = meas.dropDuplicates(["city", "station_id", "ts"])
    weather = weather.dropDuplicates(["city", "ts"]).select("city", "ts", *config.WEATHER_COLS)
    h = city_hourly(meas, stations)
    measured = h.where("data_basis = 'measured'").select(
        "city", "ts", F.col("avg_aqi").alias("aqi"), F.col("avg_pm25").alias("pm25"),
        F.col("avg_pm10").alias("pm10"), F.col("record_count").alias("n_stations"))
    model = h.where("data_basis = 'model'").select(
        "city", "ts", F.col("avg_aqi").alias("aqi_model"), F.col("avg_pm25").alias("pm25_model"),
        F.col("avg_pm10").alias("pm10_model"))
    df = (measured.join(model, ["city", "ts"], "full").join(weather, ["city", "ts"], "full")
          .withColumn("ts_local", F.date_format("ts", "yyyy-MM-dd HH:mm"))                  # theo session TZ = UTC+7
          .withColumn("ts_utc", F.date_format(F.to_utc_timestamp("ts", config.TZ), "yyyy-MM-dd'T'HH:mm:ss'Z'")))
    return df.select(*COLS).orderBy("city", "ts_local")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sink", default="cassandra", help="nơi đọc measurements/weather_hourly: cassandra | parquet:<thư mục>")
    ap.add_argument("--out", default="/opt/processing/_runtime/ml_dataset")
    ap.add_argument("--master", default=None)
    a = ap.parse_args()

    spark = io.build_spark("export_ml_dataset", a.master)
    t0 = time.time()
    stations = io.load_stations(spark)
    meas = io.read_sink_table(spark, a.sink, "measurements")
    weather = io.read_sink_table(spark, a.sink, "weather_hourly")
    ds = build_dataset(spark, meas, weather, stations).persist()
    n = ds.count()
    ds.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{a.out}/city_hourly_csv")
    ds.write.mode("overwrite").parquet(f"{a.out}/city_hourly_parquet")
    append_metrics("export_ml_dataset", {"rows": n, "out": a.out, "seconds_total": round(time.time() - t0, 1)})
    spark.stop()


if __name__ == "__main__":
    main()

"""Spark Structured Streaming: Kafka (aqi_raw, weather_raw) -> làm sạch -> Cassandra.

  - bỏ message hỏng, bản ghi trùng, giá trị ngoài khoảng; đổi đơn vị; giờ về UTC+7 (đầu giờ)
  - ghi weather_hourly, ghép thời tiết vào số đo theo (city, giờ), ghi measurements
  - sinh alerts khi AQI tại trạm > ngưỡng

Logic nằm trong common/pipeline.py (dùng chung với batch). Script này chỉ lo nguồn Kafka và trigger.

  spark-submit stream_measurements.py                     # chạy liên tục, mỗi 30 giây một lô
  spark-submit stream_measurements.py --once              # xử lý hết dữ liệu đang có trong Kafka rồi dừng
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common import io  # noqa: E402
from common.pipeline import make_foreach_batch  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topics", default="aqi_raw,weather_raw")
    ap.add_argument("--starting", default="earliest", choices=["earliest", "latest"],
                    help="chỉ có tác dụng ở lần chạy đầu; các lần sau tiếp tục từ checkpoint")
    ap.add_argument("--checkpoint", default="/checkpoints/measurements")
    ap.add_argument("--trigger", default="30 seconds")
    ap.add_argument("--once", action="store_true", help="xử lý hết dữ liệu hiện có rồi dừng (availableNow)")
    ap.add_argument("--max-offsets", type=int, default=5000, help="số message tối đa mỗi lô")
    ap.add_argument("--sink", default="cassandra")
    ap.add_argument("--master", default=None)
    a = ap.parse_args()

    spark = io.build_spark("stream_measurements", a.master)
    stations = io.load_stations(spark)
    lookup = io.weather_lookup_from_cassandra if a.sink == "cassandra" else None
    handler = make_foreach_batch(io.make_sink(a.sink), stations, lookup)

    kafka = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", a.bootstrap)
             .option("subscribe", a.topics)
             .option("startingOffsets", a.starting)
             .option("maxOffsetsPerTrigger", a.max_offsets)
             .option("failOnDataLoss", "false")
             .load())
    writer = (kafka.select(F.col("value").cast("string").alias("value"))
              .writeStream.foreachBatch(handler)
              .option("checkpointLocation", a.checkpoint))
    writer = writer.trigger(availableNow=True) if a.once else writer.trigger(processingTime=a.trigger)
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()

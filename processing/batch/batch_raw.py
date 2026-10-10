"""Batch: đọc lại dữ liệu real-time đã lưu trong MinIO (bucket `raw`, do consumer của B ghi) và xử lý
bằng đúng logic của streaming. Dùng khi:
  - cần nạp bù dữ liệu real-time (ví dụ raw-full.zip gửi sau 11/10),
  - muốn chạy thử mà không cần Kafka.
Chạy lại được: ghi theo khóa chính nên không nhân đôi dữ liệu.

  spark-submit batch_raw.py                       # s3a://raw/{aqi_raw,weather_raw}/date=*/*.jsonl
  spark-submit batch_raw.py --date 2026-10-07     # chỉ một ngày
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import io  # noqa: E402
from common.pipeline import append_metrics, process_raw  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="s3a://raw")
    ap.add_argument("--date", default="*", help="YYYY-MM-DD hoặc * (mặc định: mọi ngày)")
    ap.add_argument("--sink", default="cassandra")
    ap.add_argument("--master", default=None)
    a = ap.parse_args()

    spark = io.build_spark("batch_raw", a.master)
    stations = io.load_stations(spark)
    paths = [f"{a.base}/aqi_raw/date={a.date}/*.jsonl", f"{a.base}/weather_raw/date={a.date}/*.jsonl"]
    raw = spark.read.text(paths)          # mỗi dòng một message, cột `value`
    # Không cần weather_lookup: cả hai topic nằm trong cùng một lần đọc nên ghép trực tiếp được.
    m = process_raw(spark, raw, io.make_sink(a.sink), None, stations)
    append_metrics("batch_raw", m)
    spark.stop()


if __name__ == "__main__":
    main()

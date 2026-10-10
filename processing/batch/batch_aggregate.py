"""Batch: đọc `measurements` -> tính tổng hợp giờ/ngày/tháng/mùa theo thành phố -> `daily_agg`.

Đọc lại từ chính sink nên bao gồm cả dữ liệu lịch sử lẫn dữ liệu real-time đã ghi. Chạy lại được.
Nên chạy định kỳ (ví dụ mỗi giờ) để daily_agg cập nhật theo dữ liệu streaming.

  spark-submit batch_aggregate.py                          # đọc/ghi Cassandra
  spark-submit batch_aggregate.py --sink parquet:/tmp/out  # đọc/ghi Parquet (không cần Cassandra)
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import io  # noqa: E402
from common.aggregate import aggregate_city  # noqa: E402
from common.pipeline import append_metrics  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sink", default="cassandra")
    ap.add_argument("--master", default=None)
    a = ap.parse_args()

    spark = io.build_spark("batch_aggregate", a.master)
    stations = io.load_stations(spark)
    t0 = time.time()
    meas = io.read_sink_table(spark, a.sink, "measurements")
    agg = aggregate_city(spark, meas, stations).persist()
    counts = {r["period_type"]: r["count"] for r in agg.groupBy("period_type").count().collect()}
    io.make_sink(a.sink)(agg, "daily_agg")
    elapsed = time.time() - t0
    append_metrics("batch_aggregate", {"rows_by_period": counts, "seconds_total": round(elapsed, 1)})
    spark.stop()


if __name__ == "__main__":
    main()

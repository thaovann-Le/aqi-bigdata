"""Batch: dữ liệu lịch sử trong MinIO (bucket `history`) -> measurements, weather_hourly, alerts.

Chạy lại được (idempotent): ghi theo khóa chính nên chạy lại chỉ ghi đè cùng giá trị.
Chạy xong thì chạy tiếp batch_aggregate.py để sinh daily_agg.

  spark-submit batch_history.py                                   # hanoi + hcm, mọi năm, ghi Cassandra
  spark-submit batch_history.py --years 2024,2025 --sink parquet:/tmp/out
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # .../processing

from common import config, io, sources  # noqa: E402
from common.pipeline import append_metrics, build_alerts, build_measurements  # noqa: E402


def years_glob(years: str) -> str:
    """'' -> '*'; '2025' -> '2025'; '2024,2025' -> '{2024,2025}' (glob Hadoop)."""
    parts = [y.strip() for y in years.split(",") if y.strip()]
    if not parts:
        return "*"
    return parts[0] if len(parts) == 1 else "{" + ",".join(parts) + "}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="s3a://history", help="thư mục gốc dữ liệu lịch sử (s3a://history hoặc đường dẫn cục bộ)")
    ap.add_argument("--cities", default=",".join(config.CITIES))
    ap.add_argument("--years", default="", help="ví dụ 2024,2025 (mặc định: tất cả)")
    ap.add_argument("--sink", default="cassandra", help="cassandra | console | parquet:<thư mục>")
    ap.add_argument("--no-model", action="store_true", help="bỏ số mô hình Open-Meteo (chỉ số đo trạm)")
    ap.add_argument("--master", default=None)
    a = ap.parse_args()

    cities = [c.strip() for c in a.cities.split(",")]
    spark = io.build_spark("batch_history", a.master)
    write = io.make_sink(a.sink)
    stations = io.load_stations(spark)
    yrs = years_glob(a.years)
    t0 = time.time()

    weather = sources.history_weather(spark, a.base, cities, yrs).persist()
    aq = sources.history_openaq(spark, a.base, stations, cities, yrs)
    if not a.no_model:
        aq = aq.unionByName(sources.history_model_aq(spark, a.base, cities, yrs).select(*aq.columns))
    meas = build_measurements(aq, weather).persist()

    n_weather, n_meas = weather.count(), meas.count()
    n_model = meas.where("is_model").count()
    t_read = time.time() - t0
    write(weather.select("city", "ts", *config.WEATHER_COLS), "weather_hourly")
    write(meas, "measurements")
    alerts = build_alerts(meas, stations=stations).persist()
    n_alerts = alerts.count()
    if n_alerts:
        write(alerts, "alerts")
    elapsed = time.time() - t0

    append_metrics("batch_history", {
        "cities": cities, "years": a.years or "all", "weather_rows": n_weather, "measurement_rows": n_meas,
        "of_which_model": n_model, "alert_rows": n_alerts, "seconds_read_and_transform": round(t_read, 1),
        "seconds_total": round(elapsed, 1), "rows_per_min": round(n_meas / elapsed * 60) if elapsed else None,
    })
    spark.stop()


if __name__ == "__main__":
    main()

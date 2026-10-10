"""Cách phần D (phân tích & ML) lấy dữ liệu đã xử lý của C và ghi dự báo. Chỉ cần pandas (+ cassandra-driver).

    pip install pandas cassandra-driver

Hai cách lấy dữ liệu, cùng một định dạng DataFrame (mỗi dòng một thành phố-giờ, index theo giờ Việt Nam):

1. Từ file CSV do job export_ml_dataset xuất ra (không cần Cassandra, hợp với notebook/huấn luyện):
       df = load_city_hourly_csv("processing/_runtime/ml_dataset/city_hourly_csv")
2. Từ Cassandra (cho script dự báo chạy hằng giờ, cần dữ liệu mới nhất):
       df = load_city_hourly_cassandra("hanoi", start, end)

Cột: city, aqi, pm25, pm10, n_stations (số ĐO tại trạm), aqi_model, pm25_model, pm10_model (số MÔ HÌNH CAMS),
     temperature, humidity, wind_speed (m/s), wind_direction, precipitation, pressure.
Biến mục tiêu nên là `aqi`/`pm25` (số đo). Nếu lấp chỗ thiếu bằng số mô hình thì thêm cột đánh dấu.

Ghi dự báo:  write_forecasts(session, [{"city": "hanoi", "forecast_for": ..., "predicted_aqi": 87.0, ...}])

Chạy thử:  python processing/examples/ml_data_access.py <thư mục city_hourly_csv>
"""
import glob
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

VN = "Asia/Ho_Chi_Minh"
MEASURED = ["aqi", "pm25", "pm10", "n_stations"]
MODEL = ["aqi_model", "pm25_model", "pm10_model"]
WEATHER = ["temperature", "humidity", "wind_speed", "wind_direction", "precipitation", "pressure"]
COLUMNS = ["city"] + MEASURED + MODEL + WEATHER


# ---------------------------------------------------------------------------
# 1. Từ CSV xuất bởi batch/export_ml_dataset.py
# ---------------------------------------------------------------------------
def load_city_hourly_csv(folder_or_file: str) -> pd.DataFrame:
    path = folder_or_file
    if not os.path.exists(path):
        raise FileNotFoundError(f"Không có {path}; chạy batch/export_ml_dataset.py trước (xem processing/README.md)")
    if os.path.isdir(path):
        parts = glob.glob(os.path.join(path, "part-*.csv"))
        if not parts:
            raise FileNotFoundError(f"Không thấy part-*.csv trong {path}; đã chạy export_ml_dataset chưa?")
        path = parts[0]
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert(VN)
    return df.drop(columns=["ts_local", "ts_utc"]).set_index("ts").sort_index()[COLUMNS]


# ---------------------------------------------------------------------------
# 2. Từ Cassandra: daily_agg (period_type='hour') + weather_hourly
# ---------------------------------------------------------------------------
def connect(hosts=None, keyspace="aqi"):
    """hosts mặc định: biến môi trường CASSANDRA_HOSTS, hoặc localhost (cổng 9042 đã mở ra máy host)."""
    from cassandra.cluster import Cluster
    hosts = hosts or os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    return Cluster(hosts).connect(keyspace)


def _rows(session, cql, params):
    return [r._asdict() for r in session.execute(cql, params)]


def load_city_hourly_cassandra(city: str, start: datetime, end: datetime, session=None) -> pd.DataFrame:
    """Chuỗi theo giờ của một thành phố trong [start, end). start/end nên có múi giờ (naive = UTC)."""
    session = session or connect()
    q_agg = ("SELECT period_start, avg_aqi, avg_pm25, avg_pm10, record_count FROM daily_agg "
             "WHERE city=%s AND period_type='hour' AND data_basis=%s AND period_start >= %s AND period_start < %s")
    measured = pd.DataFrame(_rows(session, q_agg, (city, "measured", start, end)),
                            columns=["period_start", "avg_aqi", "avg_pm25", "avg_pm10", "record_count"])
    measured.columns = ["ts", "aqi", "pm25", "pm10", "n_stations"]
    model = pd.DataFrame(_rows(session, q_agg, (city, "model", start, end)),
                         columns=["period_start", "avg_aqi", "avg_pm25", "avg_pm10", "record_count"])
    model = model.drop(columns="record_count")
    model.columns = ["ts", "aqi_model", "pm25_model", "pm10_model"]
    q_w = ("SELECT ts, temperature, humidity, wind_speed, wind_direction, precipitation, pressure "
           "FROM weather_hourly WHERE city=%s AND ts >= %s AND ts < %s")
    weather = pd.DataFrame(_rows(session, q_w, (city, start, end)), columns=["ts"] + WEATHER)

    df = measured.merge(model, on="ts", how="outer").merge(weather, on="ts", how="outer")
    df["city"] = city
    num = MEASURED + MODEL + WEATHER
    df[num] = df[num].apply(pd.to_numeric, errors="coerce")          # cột toàn null -> float NaN thay vì object
    # cassandra-driver trả datetime naive theo UTC
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert(VN)
    return df.set_index("ts").sort_index()[COLUMNS]


def write_forecasts(session, rows, model_name="xgboost_v1"):
    """rows: các dict có city, forecast_for (datetime có múi giờ), predicted_aqi, [predicted_pm25], [horizon_hours]."""
    stmt = session.prepare(
        "INSERT INTO forecasts (city, forecast_for, generated_at, predicted_aqi, predicted_pm25, model_name, horizon_hours) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)")
    now = datetime.now(timezone.utc)
    for r in rows:
        session.execute(stmt, (r["city"], r["forecast_for"], r.get("generated_at", now), float(r["predicted_aqi"]),
                               None if r.get("predicted_pm25") is None else float(r["predicted_pm25"]),
                               r.get("model_name", model_name), int(r.get("horizon_hours", 24))))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = load_city_hourly_csv(sys.argv[1])
    else:
        end = datetime.now(timezone.utc)
        d = load_city_hourly_cassandra("hanoi", end - timedelta(days=7), end)
    print(d.head(10).to_string())
    print(f"\n{len(d)} dòng; số giờ có AQI đo theo thành phố:\n{d.groupby('city')['aqi'].count()}")

"""Sinh dữ liệu lịch sử GIẢ nhưng ĐÚNG ĐỊNH DẠNG của B (OpenAQ CSV.gz + Open-Meteo JSON dạng cột).

Dùng để (1) chạy test cục bộ, (2) chạy thử cả pipeline khi chưa tải được history-*.zip từ Drive.
Giá trị mang tính xác định (hàm của thời điểm) nên test tính được kết quả kỳ vọng. Có cài sẵn các "bẫy"
như dữ liệu thật: -999 (thiếu), 985 (rác), trạm đo 5 phút, đơn vị không phải µg/m³, trạm ngoài hai thành phố,
giờ Open-Meteo không kèm múi giờ.

  python processing/tools/make_sample_history.py <thư mục ra>
"""
import csv
import gzip
import io
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

VN = timezone(timedelta(hours=7))
YEARS = (2023, 2024, 2025)
# Mỗi năm chỉ sinh vài ngày (đủ để thử mùa đông và mùa hè): (tháng, ngày đầu, ngày cuối)
WINDOWS = [(1, 1, 3), (7, 1, 3)]
OUTLIER = datetime(2023, 1, 2, 4, tzinfo=VN)       # trạm 7441: 985 µg/m³ -> phải bị loại
MISSING = datetime(2023, 1, 2, 3, tzinfo=VN)       # trạm 7441: -999 -> thiếu
SPIKE = datetime(2025, 1, 2, 8, tzinfo=VN)         # trạm 2161292: 80 µg/m³ -> AQI > 150 -> cảnh báo


def hours(year):
    for month, d0, d1 in WINDOWS:
        t = datetime(year, month, d0, tzinfo=VN)
        end = datetime(year, month, d1, 23, tzinfo=VN)
        while t <= end:
            yield t
            t += timedelta(hours=1)


def pm25_hanoi(t):          # 30..53
    return 30.0 + t.hour


def pm10_hanoi(t):
    return 50.0 + t.hour


def pm25_hcm(t):
    return 40.0


def weather_value(city, t):
    base = 20.0 if city == "hanoi" else 30.0
    return {"temperature_2m": base + t.hour / 2, "relative_humidity_2m": 80.0, "precipitation": 0.0,
            "wind_speed_10m": 36.0, "wind_direction_10m": 90.0, "surface_pressure": 1010.0}


def _csv_gz(rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"])
    w.writerows(rows)
    return gzip.compress(buf.getvalue().encode("utf-8"))


def _put(root, rel, data):
    p = Path(root) / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data if isinstance(data, bytes) else data.encode("utf-8"))


def openaq_rows(loc_id, name, year, hourly_fn, extra_param=None, five_min=False, lat=21.0, lon=105.8):
    rows = []
    for t in hours(year):
        for minute in (range(0, 60, 5) if five_min else (0,)):
            tt = t + timedelta(minutes=minute)
            ts = tt.isoformat()
            v25 = hourly_fn(t) + (minute / 5 if five_min else 0)
            if loc_id == 7441 and t == MISSING:
                v25 = -999.0
            if loc_id == 7441 and t == OUTLIER:
                v25 = 985.0
            if loc_id == 2161292 and t == SPIKE:
                v25 = 80.0
            rows.append([loc_id, loc_id * 10 + 1, name, ts, lat, lon, "pm25", "µg/m³", v25])
            if loc_id == 7441:
                rows.append([loc_id, loc_id * 10 + 2, name, ts, lat, lon, "pm10", "µg/m³", pm10_hanoi(t)])
            if extra_param:
                p, u, v = extra_param
                rows.append([loc_id, loc_id * 10 + 3, name, ts, lat, lon, p, u, v])
    return rows


def main(out):
    out = Path(out)
    for year in YEARS:
        # Hà Nội: 7441 (2023), 2161292 (2024-2025)
        if year == 2023:
            _put(out, f"openaq/locationid=7441/year={year}/location-7441-{year}.csv.gz",
                 _csv_gz(openaq_rows(7441, "Hanoi-7441", year, pm25_hanoi)))
        else:
            _put(out, f"openaq/locationid=2161292/year={year}/location-2161292-{year}.csv.gz",
                 _csv_gz(openaq_rows(2161292, "Lưu Quang Vũ", year, pm25_hanoi)))
        # Trạm đo 5 phút một lần (phải được gộp về giờ), chỉ năm 2024
        if year == 2024:
            _put(out, f"openaq/locationid=4946811/year={year}/location-4946811-{year}.csv.gz",
                 _csv_gz(openaq_rows(4946811, "556 Nguyễn Văn Cừ", year, lambda t: 10.0, five_min=True)))
        # TP.HCM: 7440 (2023), CMT8 (2025) có thêm cảm biến đếm hạt (phải bị bỏ)
        if year == 2023:
            _put(out, f"openaq/locationid=7440/year={year}/location-7440-{year}.csv.gz",
                 _csv_gz(openaq_rows(7440, "US Diplomatic Post: HCMC", year, pm25_hcm, lat=10.78, lon=106.7)))
        if year == 2025:
            _put(out, f"openaq/locationid=3276359/year={year}/location-3276359-{year}.csv.gz",
                 _csv_gz(openaq_rows(3276359, "CMT8", year, pm25_hcm, extra_param=("um003", "particles/cm³", 4927.0),
                                     lat=10.785, lon=106.67)))
        # Trạm ở tỉnh khác (không thuộc stations.csv) -> phải bị bỏ
        if year == 2024:
            _put(out, f"openaq/locationid=999999/year={year}/location-999999-{year}.csv.gz",
                 _csv_gz(openaq_rows(999999, "Tỉnh khác", year, lambda t: 99.0)))

        for city in ("hanoi", "hcm", "haiphong"):
            ts = [t.strftime("%Y-%m-%dT%H:%M") for t in hours(year)]      # giờ địa phương, KHÔNG kèm múi giờ
            hourly = {"time": ts}
            for var in ("temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m",
                        "wind_direction_10m", "surface_pressure"):
                hourly[var] = [weather_value(city, t)[var] for t in hours(year)]
            if city == "hanoi" and year == 2023:
                hourly["relative_humidity_2m"][0] = 150.0                   # độ ẩm vô lý -> null
            _put(out, f"openmeteo/weather/point={city}/year={year}.json",
                 json.dumps({"point_id": city, "hourly": hourly, "hourly_units": {"time": "iso8601"}}))
            if year >= 2024:
                n = len(ts)
                aq = {"time": ts, "pm2_5": [25.0] * n, "pm10": [40.0] * n, "nitrogen_dioxide": [10.0] * n,
                      "sulphur_dioxide": [5.0] * n, "ozone": [30.0] * n, "carbon_monoxide": [300.0] * n,
                      "us_aqi": [77] * n}
                _put(out, f"openmeteo/air_quality/point={city}/year={year}.json",
                     json.dumps({"point_id": city, "hourly": aq}))
    print(f"Đã sinh dữ liệu mẫu vào {out}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "sample_history")

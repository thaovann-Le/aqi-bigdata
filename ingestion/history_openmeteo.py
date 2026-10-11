"""Lịch sử Open-Meteo theo giờ (thời tiết + chất lượng không khí mô hình CAMS) cho các điểm trong points_vn.json.

  python history_openmeteo.py hanoi hcm   # chỉ hai thành phố (chạy trước, xong trong vài phút)
  python history_openmeteo.py             # tất cả các điểm

Chạy lại được: file nào đã có trong MinIO thì bỏ qua. Khi hết hạn mức trong ngày,
script dừng lại; hôm sau chạy lại đúng lệnh cũ để tải tiếp.
"""
import json
import os
import sys
import time

import requests

from common import ensure_bucket, load_points, minio_client, object_exists, setup_logging

log = setup_logging("history_openmeteo")
SLEEP = float(os.getenv("OPENMETEO_SLEEP_SEC", "20"))
END = "2025-12-31"
# tên: (url, ngày bắt đầu có dữ liệu, các biến theo giờ)
DATASETS = {
    "weather": (
        "https://archive-api.open-meteo.com/v1/archive",
        "2021-01-01",
        "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,surface_pressure",
    ),
    "air_quality": (
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        "2022-08-04",
        "pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,ozone,carbon_monoxide,us_aqi",
    ),
}


class DailyLimit(Exception):
    pass


def fetch(url, params):
    errors = 0
    while True:
        try:
            r = requests.get(url, params=params, timeout=120)
        except requests.RequestException as e:
            errors += 1
            if errors >= 3:
                log.error("Bỏ qua sau 3 lần lỗi mạng: %s", e)
                return None
            time.sleep(30)
            continue
        if r.status_code == 429:
            try:
                reason = str(r.json().get("reason") or "")
            except (ValueError, AttributeError):
                reason = r.text
            if "daily" in reason.lower():
                raise DailyLimit(reason)
            wait = 3700 if "hourly" in reason.lower() else 70
            log.warning("Bị giới hạn (%s). Chờ %ds rồi gọi lại", reason, wait)
            time.sleep(wait)
            continue
        if not r.ok:
            log.error("HTTP %s: %s", r.status_code, r.text[:200])
            return None
        try:
            return r.json()
        except ValueError:
            log.error("Phản hồi không phải JSON: %s", r.text[:200])
            return None


def main(selected):
    points = [p for p in load_points() if not selected or p["id"] in selected]
    s3 = minio_client()
    ensure_bucket(s3, "history")
    written = 0
    for p in points:
        for name, (url, first_day, hourly) in DATASETS.items():
            for year in range(int(first_day[:4]), int(END[:4]) + 1):
                key = f"openmeteo/{name}/point={p['id']}/year={year}.json"
                if object_exists(s3, "history", key):
                    continue
                body = fetch(url, {
                    "latitude": p["lat"],
                    "longitude": p["lon"],
                    "start_date": max(first_day, f"{year}-01-01"),
                    "end_date": min(END, f"{year}-12-31"),
                    "hourly": hourly,
                    "timezone": "Asia/Bangkok",
                })
                if body and "hourly" in body:
                    body["point_id"], body["point_name"] = p["id"], p["name"]
                    data = json.dumps(body, ensure_ascii=False).encode()
                    s3.put_object(Bucket="history", Key=key, Body=data)
                    written += 1
                    log.info("%s: %d giờ, %.0f KB", key, len(body["hourly"]["time"]), len(data) / 1024)
                elif body is not None:
                    log.error("%s: phản hồi không có dữ liệu: %s", key, str(body)[:200])
                time.sleep(SLEEP)
    log.info("Xong %d điểm, ghi mới %d file", len(points), written)


if __name__ == "__main__":
    try:
        main(set(sys.argv[1:]))
    except DailyLimit as e:
        log.warning("Hết hạn mức trong ngày (%s). Ngày mai chạy lại đúng lệnh này để tải tiếp.", e)
    except Exception:
        log.exception("Script dừng vì lỗi; chạy lại đúng lệnh này để tải tiếp")
        raise

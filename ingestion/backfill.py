"""Tải bù số liệu theo giờ cho giai đoạn thu thập real-time, để lấp các khoảng máy bị ngủ hoặc tắt.

  python backfill.py                          # từ 2026-10-06 đến hôm nay
  python backfill.py 2026-10-06 2026-10-13    # tự chọn khoảng ngày

Ghi vào MinIO bucket history, thư mục backfill/ (tách riêng khỏi dữ liệu real-time trong bucket raw):
  backfill/openaq/locationid=<mã>/location-<mã>-backfill.csv.gz   # cùng cột với kho lưu trữ OpenAQ
  backfill/openmeteo/<weather|air_quality>/point=<thành phố>/backfill.json
AQICN không tải bù được vì API chỉ trả số đo mới nhất. Chạy lại thì ghi đè file cũ.
"""
import csv
import gzip
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from common import CITIES, HERE, ensure_bucket, get_json, minio_client, setup_logging

log = setup_logging("backfill")
VN = ZoneInfo("Asia/Ho_Chi_Minh")
BUCKET = "history"
OPENAQ_COLUMNS = ["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"]
OPENMETEO = {
    "weather": (
        "https://api.open-meteo.com/v1/forecast",
        "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,surface_pressure",
    ),
    "air_quality": (
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        "pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,ozone,carbon_monoxide,us_aqi",
    ),
}


def backfill_openaq(s3, start, end):
    """Số đo trung bình theo giờ của các trạm OpenAQ mà producer đang theo dõi."""
    key = os.getenv("OPENAQ_KEY")
    if not key:
        log.error("Thiếu OPENAQ_KEY trong .env, bỏ qua phần OpenAQ")
        return
    locations = [
        x for x in json.loads((HERE / "openaq_locations.json").read_text(encoding="utf-8"))
        if x["city"] in CITIES and (x.get("last") or "") >= "2026-10-01"
    ]
    # API nhận giờ UTC
    params = {
        "datetime_from": start.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "datetime_to": end.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": 1000,
    }
    for loc in locations:
        if not loc.get("sensors"):
            log.error("Trạm %s chưa có bảng sensor; chạy lại `python history_openaq.py find`", loc["id"])
            continue
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(OPENAQ_COLUMNS)
        rows = 0
        for sensor in loc["sensors"]:
            page = 1
            while True:
                body = get_json(
                    f"https://api.openaq.org/v3/sensors/{sensor['id']}/hours",
                    {**params, "page": page},
                    headers={"X-API-Key": key},
                )
                time.sleep(1.1)  # giới hạn 60 lượt gọi mỗi phút
                results = (body or {}).get("results") or []
                for r in results:
                    # Thời điểm của một số đo theo giờ là lúc kết thúc giờ đó
                    measured = ((r.get("period") or {}).get("datetimeTo") or {}).get("local")
                    if measured is None or r.get("value") is None:
                        continue
                    writer.writerow([
                        loc["id"], sensor["id"], loc["name"], measured, loc["lat"], loc["lon"],
                        sensor["parameter"], sensor["units"], r["value"],
                    ])
                    rows += 1
                if len(results) < 1000:
                    break
                page += 1
            if body is None:
                log.warning("Trạm %s, sensor %s (%s): không lấy được", loc["id"], sensor["id"], sensor["parameter"])
        if not rows:
            log.warning("Trạm %s (%s): không có số đo nào, không ghi file", loc["id"], loc["name"])
            continue
        object_key = f"backfill/openaq/locationid={loc['id']}/location-{loc['id']}-backfill.csv.gz"
        s3.put_object(Bucket=BUCKET, Key=object_key, Body=gzip.compress(out.getvalue().encode()))
        log.info("%s (%s, %s): %d số đo theo giờ -> %s", loc["id"], loc["city"], loc["name"], rows, object_key)


def backfill_openmeteo(s3, start, end):
    """Thời tiết và chất lượng không khí mô hình theo giờ cho hai thành phố."""
    now = datetime.now(VN).strftime("%Y-%m-%dT%H:%M")
    for city, info in CITIES.items():
        for name, (url, hourly) in OPENMETEO.items():
            body = get_json(url, {
                "latitude": info["lat"],
                "longitude": info["lon"],
                "hourly": hourly,
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "timezone": "Asia/Bangkok",
            })
            if not body or "hourly" not in body:
                log.error("%s %s: không lấy được: %s", city, name, str(body)[:200])
                continue
            # Bỏ các giờ chưa tới: với ngày hôm nay API trả cả số dự báo
            keep = sum(1 for t in body["hourly"]["time"] if t <= now)
            body["hourly"] = {k: v[:keep] for k, v in body["hourly"].items()}
            body["point_id"], body["backfill"] = city, True
            object_key = f"backfill/openmeteo/{name}/point={city}/backfill.json"
            s3.put_object(Bucket=BUCKET, Key=object_key, Body=json.dumps(body, ensure_ascii=False).encode())
            log.info("%s %s: %d giờ -> %s", city, name, keep, object_key)
            time.sleep(1)


if __name__ == "__main__":
    args = sys.argv[1:]
    start = datetime.strptime(args[0] if args else "2026-10-06", "%Y-%m-%d").replace(tzinfo=VN)
    end_day = datetime.strptime(args[1], "%Y-%m-%d") if len(args) > 1 else datetime.now(VN).replace(tzinfo=None)
    end = end_day.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=VN)
    log.info("Tải bù từ %s đến %s", start.strftime("%Y-%m-%d %H:%M"), end.strftime("%Y-%m-%d %H:%M"))

    s3 = minio_client()
    ensure_bucket(s3, BUCKET)
    backfill_openmeteo(s3, start, end)
    backfill_openaq(s3, start, end)
    log.info("Xong. Xuất bằng: python sync_minio.py export history")

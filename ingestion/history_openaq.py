"""Lịch sử OpenAQ cho toàn bộ trạm ở Việt Nam.

  python history_openaq.py find                 # tìm trạm, ghi openaq_locations.json (cần OPENAQ_KEY)
  python history_openaq.py download hanoi hcm   # chỉ trạm quanh Hà Nội và TP.HCM
  python history_openaq.py download             # tất cả trạm trong openaq_locations.json
  python history_openaq.py download 1234 5678   # theo id trạm

Nguồn là mỗi trạm mỗi ngày một file; script gộp thành một file cho mỗi trạm mỗi năm.
Chạy lại được: trạm-năm nào đã có trong MinIO thì bỏ qua.
"""
import gzip
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from common import (
    CITIES, HERE, distance_km, ensure_bucket, get_json, minio_client, object_exists, scan, setup_logging,
)

log = setup_logging("history_openaq")
SRC_BUCKET = "openaq-data-archive"
YEARS = [2021, 2022, 2023, 2024, 2025, 2026]
CITY_RADIUS_KM = 40
LOCATIONS_FILE = HERE / "openaq_locations.json"


def city_of(lat, lon):
    for city, info in CITIES.items():
        if distance_km(lat, lon, info["lat"], info["lon"]) <= CITY_RADIUS_KM:
            return city
    return "other"


def find():
    headers = {"X-API-Key": os.environ["OPENAQ_KEY"]}
    locations, page = [], 1
    while True:
        body = get_json("https://api.openaq.org/v3/locations", {"iso": "VN", "limit": 1000, "page": page}, headers=headers)
        results = (body or {}).get("results", [])
        for loc in results:
            lat, lon = loc["coordinates"]["latitude"], loc["coordinates"]["longitude"]
            locations.append({
                "id": loc["id"],
                "name": loc["name"],
                "city": city_of(lat, lon),
                "lat": lat,
                "lon": lon,
                "first": (loc.get("datetimeFirst") or {}).get("utc"),
                "last": (loc.get("datetimeLast") or {}).get("utc"),
                "parameters": sorted({s["parameter"]["name"] for s in loc.get("sensors", [])}),
                # producer cần bảng này để biết mỗi sensor đo thông số gì
                "sensors": [
                    {"id": s["id"], "parameter": s["parameter"]["name"], "units": s["parameter"].get("units")}
                    for s in loc.get("sensors", [])
                ],
            })
        if len(results) < 1000:
            break
        page += 1

    locations.sort(key=lambda x: (x["city"] == "other", x["city"], x["id"]))
    LOCATIONS_FILE.write_text(json.dumps(locations, ensure_ascii=False, indent=1), encoding="utf-8")
    for x in locations:
        print(f"{x['city']}\t{x['id']}\t{x['name']}\t{x['first']} -> {x['last']}\t{','.join(x['parameters'])}")
    log.info("Đã ghi %d trạm vào %s", len(locations), LOCATIONS_FILE.name)


def download(selectors):
    locations = json.loads(LOCATIONS_FILE.read_text(encoding="utf-8"))
    if selectors:
        locations = [x for x in locations if str(x["id"]) in selectors or x["city"] in selectors]
    src = boto3.client(
        "s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED, max_pool_connections=32)
    )
    dst = minio_client()
    ensure_bucket(dst, "history")

    def fetch(key):
        return gzip.decompress(src.get_object(Bucket=SRC_BUCKET, Key=key)["Body"].read())

    total_files = total_bytes = 0
    with ThreadPoolExecutor(32) as pool:
        for loc in locations:
            for year in YEARS:
                dst_key = f"openaq/locationid={loc['id']}/year={year}/location-{loc['id']}-{year}.csv.gz"
                if object_exists(dst, "history", dst_key):
                    continue
                keys = sorted(o["Key"] for o in scan(src, SRC_BUCKET, f"records/csv.gz/locationid={loc['id']}/year={year}/"))
                if not keys:
                    continue
                try:
                    parts = list(pool.map(fetch, keys))
                except Exception:
                    log.exception("Lỗi tải trạm %s năm %s, bỏ qua (chạy lại để tải bù)", loc["id"], year)
                    continue
                # Giữ dòng tiêu đề của file đầu, bỏ ở các file sau
                merged = parts[0] + b"".join(p.split(b"\n", 1)[1] if b"\n" in p else b"" for p in parts[1:])
                data = gzip.compress(merged)
                dst.put_object(Bucket="history", Key=dst_key, Body=data)
                total_files += 1
                total_bytes += len(data)
                log.info("%s (%s) %d: gộp %d file ngày, %d dòng, %.1f KB",
                         loc["id"], loc["name"], year, len(keys), merged.count(b"\n") - 1, len(data) / 1024)
    log.info("Xong: ghi mới %d file, %.2f MB", total_files, total_bytes / 1e6)


if __name__ == "__main__":
    if sys.argv[1:2] == ["find"]:
        find()
    elif sys.argv[1:2] == ["download"]:
        download(sys.argv[2:])
    else:
        print(__doc__)

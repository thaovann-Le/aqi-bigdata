import json
import logging
import math
import os
import time
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Phạm vi real-time và phân tích: tâm thành phố.
CITIES = {
    "hanoi": {"lat": 21.0245, "lon": 105.8412},
    "hcm": {"lat": 10.8230, "lon": 106.6297},
}


def distance_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


def load_points():
    """Các điểm lấy dữ liệu lịch sử Open-Meteo (63 thành phố tỉnh lỵ, Hà Nội và TP.HCM đứng đầu)."""
    return json.loads((HERE / "points_vn.json").read_text(encoding="utf-8"))


def setup_logging(name):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(HERE / f"{name}.log")],
    )
    # Thư viện kafka ghi rất nhiều dòng INFO, làm khó đọc log của mình
    logging.getLogger("kafka").setLevel(logging.WARNING)
    return logging.getLogger(name)


def get_json(url, params=None, headers=None, retries=3):
    """GET có retry, trả về None nếu thất bại sau `retries` lần."""
    log = logging.getLogger("http")
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            wait = 5 * 2**attempt
            log.warning("Lỗi gọi %s (lần %d/%d): %s. Thử lại sau %ds", url, attempt + 1, retries, e, wait)
            time.sleep(wait)
    log.error("Bỏ qua %s sau %d lần thử", url, retries)
    return None


def minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
        config=Config(max_pool_connections=32),
    )


def ensure_bucket(s3, bucket):
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)


def object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def scan(s3, bucket, prefix=""):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])

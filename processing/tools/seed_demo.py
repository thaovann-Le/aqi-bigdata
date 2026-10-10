"""Nạp dữ liệu cho lần chạy thử end-to-end (service `seed` trong docker-compose.demo.yml).

1. Tạo bucket MinIO `raw`, `history`.
2. Lịch sử -> bucket `history`:
     - nếu HISTORY_DIR (mặc định /data/history) có dữ liệu thật (giải nén history-hn-hcm.zip vào data/history/)
       thì nạp thư mục đó;
     - nếu không, sinh dữ liệu GIẢ đúng định dạng (tools/make_sample_history.py) để pipeline vẫn chạy được.
3. Real-time: giải nén RAW_ZIPS (mặc định ingestion/results/raw-mau.zip, dữ liệu thật của B), nạp vào bucket `raw`
   và phát lại vào Kafka (aqi_raw, weather_raw) cho job streaming. Bỏ qua bước phát lại nếu topic đã có dữ liệu
   (đặt FORCE_REPLAY=1 để phát lại).

Chạy lại được: file đã có cùng kích thước trong MinIO thì bỏ qua.
"""
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

MINIO = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
HISTORY_DIR = Path(os.getenv("HISTORY_DIR", "/data/history"))
RAW_DIR = Path(os.getenv("RAW_DIR", "/data/raw"))
RAW_ZIPS = [p for p in os.getenv("RAW_ZIPS", str(HERE.parents[1] / "ingestion/results/raw-mau.zip")).split(",") if p]
REPLAY_DELAY = float(os.getenv("REPLAY_DELAY_SEC", "0"))
TOPICS = ("aqi_raw", "weather_raw")


def log(msg):
    print(f"[seed] {msg}", flush=True)


def s3_client():
    return boto3.client("s3", endpoint_url=MINIO, region_name="us-east-1",
                        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"))


def wait_minio(s3, timeout=180):
    t0 = time.time()
    while True:
        try:
            s3.list_buckets()
            return
        except (BotoCoreError, ClientError) as e:
            if time.time() - t0 > timeout:
                raise
            log(f"chờ MinIO ({e.__class__.__name__})...")
            time.sleep(3)


def ensure_bucket(s3, name):
    if name not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=name)


def upload_dir(s3, folder: Path, bucket: str) -> int:
    existing = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        existing.update({o["Key"]: o["Size"] for o in page.get("Contents", [])})
    n = 0
    for p in sorted(folder.rglob("*")):
        if not p.is_file():
            continue
        key = p.relative_to(folder).as_posix()
        if existing.get(key) == p.stat().st_size:
            continue
        s3.upload_file(str(p), bucket, key)
        n += 1
    return n


def has_files(folder: Path) -> bool:
    return folder.is_dir() and any(p.is_file() for p in folder.rglob("*"))


def kafka_has_data(consumer_cls, tp_cls) -> bool:
    c = consumer_cls(bootstrap_servers=KAFKA)
    try:
        tps = [tp_cls(t, p) for t in TOPICS for p in (c.partitions_for_topic(t) or [])]
        return bool(tps) and sum(c.end_offsets(tps).values()) > 0
    finally:
        c.close()


def replay(raw_root: Path) -> int:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    for attempt in range(60):
        try:
            if kafka_has_data(KafkaConsumer, TopicPartition) and os.getenv("FORCE_REPLAY") != "1":
                log("Kafka đã có dữ liệu, bỏ qua phát lại (FORCE_REPLAY=1 để phát lại)")
                return 0
            break
        except Exception as e:  # Kafka chưa sẵn sàng
            log(f"chờ Kafka ({e.__class__.__name__})...")
            time.sleep(3)
    producer = KafkaProducer(bootstrap_servers=KAFKA, acks="all")
    lines = []
    for topic in TOPICS:
        for p in sorted((raw_root / topic).rglob("*.jsonl")):
            lines += [(topic, ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    # Gửi theo đúng thứ tự thời gian thu thập, như luồng thật.
    import json
    lines.sort(key=lambda x: json.loads(x[1]).get("fetched_at") or "")
    for topic, ln in lines:
        msg = json.loads(ln)
        key = str(msg.get("station_id") or msg.get("city")).encode()
        producer.send(topic, key=key, value=ln.encode())
        if REPLAY_DELAY:
            time.sleep(REPLAY_DELAY)
    producer.flush()
    return len(lines)


def main():
    s3 = s3_client()
    wait_minio(s3)
    for b in ("raw", "history"):
        ensure_bucket(s3, b)

    if has_files(HISTORY_DIR):
        log(f"nạp dữ liệu lịch sử THẬT từ {HISTORY_DIR}")
        log(f"history: tải lên {upload_dir(s3, HISTORY_DIR, 'history')} file mới")
    else:
        import make_sample_history
        log(f"KHÔNG thấy dữ liệu trong {HISTORY_DIR} -> dùng dữ liệu lịch sử GIẢ (chỉ để thử pipeline). "
            "Giải nén history-hn-hcm.zip vào data/history/ để dùng dữ liệu thật.")
        with tempfile.TemporaryDirectory() as d:
            make_sample_history.main(d)
            log(f"history: tải lên {upload_dir(s3, Path(d), 'history')} file mới")

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        for z in RAW_ZIPS:
            if Path(z).exists():
                with zipfile.ZipFile(z) as f:
                    f.extractall(root)
                log(f"giải nén {z}")
        raw_root = root / "raw"
        if has_files(RAW_DIR):                       # dữ liệu real-time thêm (ví dụ raw-full giải nén vào data/raw/)
            raw_root = RAW_DIR
            log(f"dùng dữ liệu real-time trong {RAW_DIR}")
        if not has_files(raw_root):
            log("không có dữ liệu real-time để nạp")
            return
        log(f"raw: tải lên {upload_dir(s3, raw_root, 'raw')} file mới")
        log(f"Kafka: đã phát lại {replay(raw_root)} message vào {KAFKA}")
    log("xong")


if __name__ == "__main__":
    main()

"""Thống kê 3V từ hai bucket raw và history: số file, dung lượng, số bản ghi, tốc độ real-time.

  python stats_3v.py | tee stats_3v.txt
"""
import gzip
import json
from collections import Counter

from common import minio_client, scan

s3 = minio_client()


def read(bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def records(bucket, key, body):
    if key.endswith(".jsonl"):
        return body.count(b"\n") + 1
    if key.endswith(".csv.gz"):
        return gzip.decompress(body).count(b"\n") - 1
    if key.endswith(".json"):
        return len(json.loads(body)["hourly"]["time"])
    return 0


files, size, rows, formats = Counter(), Counter(), Counter(), Counter()
per_day = Counter()
for bucket in ("raw", "history"):
    for obj in scan(s3, bucket):
        key = obj["Key"]
        parts = key.split("/")
        source = f"{bucket}/" + ("/".join(parts[:2]) if parts[0] == "openmeteo" else parts[0])
        n = records(bucket, key, read(bucket, key))
        files[source] += 1
        size[source] += obj["Size"]
        rows[source] += n
        formats[key.split(".", 1)[-1]] += 1
        if bucket == "raw":
            per_day[(parts[1], parts[0])] += n

print("== Volume: theo nguồn ==")
print("nguồn\tsố file\tMB\tsố bản ghi")
for source in sorted(files):
    print(f"{source}\t{files[source]}\t{size[source] / 1e6:.2f}\t{rows[source]}")
print(f"TỔNG\t{sum(files.values())}\t{sum(size.values()) / 1e6:.2f}\t{sum(rows.values())}")

print("\n== Variety: số file theo định dạng ==")
for fmt, n in formats.most_common():
    print(f"{fmt}\t{n}")

print("\n== Velocity: bản ghi real-time theo ngày ==")
print("ngày\ttopic\tsố bản ghi\tbản ghi/giờ (chia 24)")
for (date, topic), n in sorted(per_day.items()):
    print(f"{date}\t{topic}\t{n}\t{n / 24:.1f}")

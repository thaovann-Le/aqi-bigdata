"""Kiểm tra dữ liệu trong MinIO có đủ không: so với số message producer đã gửi và số file lịch sử phải có.

  python check_data.py
"""
import gzip
import json
import re
from collections import Counter, defaultdict

from common import HERE, load_points, minio_client, scan

s3 = minio_client()
buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}


def read(bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


print("== 1. Real-time (bucket raw) ==")
if "raw" not in buckets:
    print("Chưa có bucket raw: consumer chưa ghi lần nào")
else:
    per_day, stored, broken = Counter(), 0, 0
    for obj in scan(s3, "raw"):
        date = obj["Key"].split("/")[1]
        for line in read("raw", obj["Key"]).splitlines():
            try:
                per_day[(date, json.loads(line)["source"])] += 1
                stored += 1
            except (ValueError, KeyError):
                broken += 1
    for (date, source), n in sorted(per_day.items()):
        print(f"{date}\t{source}\t{n} bản ghi")
    log_file = HERE / "producer.log"
    sent = sum(int(n) for n in re.findall(r"Đã gửi (\d+) message", log_file.read_text(encoding="utf-8"))) if log_file.exists() else 0
    print(f"Producer đã gửi {sent}, MinIO đang có {stored}, chênh {sent - stored}; dòng hỏng: {broken}")
    print("Chênh trong khoảng 1-2 chu kỳ là bình thường (consumer gom 5 phút mới ghi). Chênh lớn hơn: xem consumer_minio.log")

print("\n== 2. Lịch sử Open-Meteo (bucket history) ==")
EXPECTED = {"weather": range(2021, 2026), "air_quality": range(2022, 2026)}
have = defaultdict(set)
small = []
if "history" in buckets:
    for obj in scan(s3, "history", "openmeteo/"):
        _, dataset, point, year = obj["Key"].split("/")
        have[point.split("=")[1]].add((dataset, int(year[5:9])))
        if obj["Size"] < 100_000 and not (dataset == "air_quality" and "2022" in year):
            small.append(obj["Key"])
need = {(d, y) for d, years in EXPECTED.items() for y in years}
done, partial, todo = [], [], []
for p in load_points():
    got = have.get(p["id"], set())
    (done if got >= need else partial if got else todo).append(p["id"])
print(f"Đủ {len(need)} file: {len(done)} điểm {done[:6]}{' ...' if len(done) > 6 else ''}")
print(f"Đang dở: {len(partial)} điểm {partial}")
print(f"Chưa tải: {len(todo)} điểm")
if small:
    print(f"File nhỏ bất thường (dưới 100 KB), nên xóa rồi tải lại: {small}")

print("\n== 3. Lịch sử OpenAQ (bucket history) ==")
names = {}
locations_file = HERE / "openaq_locations.json"
if locations_file.exists():
    names = {x["id"]: (x["city"], x["name"]) for x in json.loads(locations_file.read_text(encoding="utf-8"))}
stations = defaultdict(lambda: {"years": [], "rows": 0, "bytes": 0})
if "history" in buckets:
    for obj in scan(s3, "history", "openaq/"):
        _, loc, year, _ = obj["Key"].split("/")
        s = stations[int(loc.split("=")[1])]
        s["years"].append(year.split("=")[1])
        s["rows"] += gzip.decompress(read("history", obj["Key"])).count(b"\n") - 1
        s["bytes"] += obj["Size"]
print("thành phố\tmã\ttên\tcác năm có dữ liệu\tsố dòng\tKB")
for loc_id, s in sorted(stations.items(), key=lambda kv: (names.get(kv[0], ("?", ""))[0], kv[0])):
    city, name = names.get(loc_id, ("?", "?"))
    print(f"{city}\t{loc_id}\t{name}\t{','.join(sorted(s['years']))}\t{s['rows']}\t{s['bytes'] / 1024:.0f}")
print(f"Tổng: {len(stations)} trạm có dữ liệu trên {len(names)} trạm trong openaq_locations.json, "
      f"{sum(s['rows'] for s in stations.values())} dòng")
print("Trạm không xuất hiện ở đây là trạm không có dữ liệu trong 2021-2026 trên kho OpenAQ.")

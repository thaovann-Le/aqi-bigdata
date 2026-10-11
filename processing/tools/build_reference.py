"""Sinh processing/reference/stations.csv từ ingestion/stations.json và ingestion/openaq_locations.json.

Chạy lại khi B cập nhật danh sách trạm:

    python processing/tools/build_reference.py

Cột `physical_id` gom các trạm trùng vật lý (cùng một trạm xuất hiện ở cả AQICN và OpenAQ,
cách nhau <= 300 m) để khi tính trung bình thành phố không bị đếm hai lần.
"""
import csv
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "processing" / "reference" / "stations.csv"
CITIES = {"hanoi", "hcm"}
DUP_KM = 0.3


def dist_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


def main():
    aqicn = json.loads((ROOT / "ingestion" / "stations.json").read_text(encoding="utf-8"))
    openaq = json.loads((ROOT / "ingestion" / "openaq_locations.json").read_text(encoding="utf-8"))
    rows = []
    for s in openaq:
        if s["city"] in CITIES:
            rows.append({"station_id": f"openaq:{s['id']}", "source": "openaq", "native_id": s["id"],
                         "city": s["city"], "name": s["name"], "lat": s["lat"], "lon": s["lon"]})
    for s in aqicn:
        if s["city"] in CITIES:
            rows.append({"station_id": f"aqicn:{s['uid']}", "source": "aqicn", "native_id": s["uid"],
                         "city": s["city"], "name": s["name"], "lat": s["lat"], "lon": s["lon"]})
    for city in sorted(CITIES):  # trạm "mô hình" của Open-Meteo, một trạm ảo mỗi thành phố
        rows.append({"station_id": f"model:{city}", "source": "openmeteo_aq", "native_id": city,
                     "city": city, "name": f"Open-Meteo CAMS model ({city})", "lat": "", "lon": ""})
    oaq = [r for r in rows if r["source"] == "openaq"]
    for r in rows:
        r["physical_id"] = r["station_id"]
        if r["source"] == "aqicn":
            for o in oaq:
                if o["city"] == r["city"] and dist_km(r["lat"], r["lon"], o["lat"], o["lon"]) <= DUP_KM:
                    r["physical_id"] = o["station_id"]
                    break
    OUT.parent.mkdir(parents=True, exist_ok=True)
    cols = ["station_id", "source", "native_id", "city", "name", "lat", "lon", "physical_id"]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    dups = [(r["station_id"], r["physical_id"]) for r in rows if r["physical_id"] != r["station_id"]]
    print(f"Đã ghi {len(rows)} trạm vào {OUT}; trùng vật lý: {dups}")


if __name__ == "__main__":
    main()

"""Tìm các trạm AQICN quanh Hà Nội và TP.HCM, ghi ra stations.json.

Tìm bằng hai cách (theo khung tọa độ và theo từ khóa), rồi gọi thử từng trạm:
chỉ trạm nào trả về AQI đo trong MAX_AGE_HOURS giờ gần đây mới được ghi vào stations.json.
"""
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from common import CITIES, HERE, distance_km, get_json, setup_logging

log = setup_logging("list_stations")
TOKEN = os.environ["AQICN_TOKEN"]
API = "https://api.waqi.info"
RADIUS_KM = 50
MIN_STATIONS = 3
MAX_AGE_HOURS = 48
VN = ZoneInfo("Asia/Ho_Chi_Minh")
KEYWORDS = {
    "hanoi": ["hanoi", "ha noi", "hà nội"],
    "hcm": ["ho chi minh", "hồ chí minh", "saigon", "thu duc", "binh duong", "bien hoa"],
}


def discover(city, info):
    """Trả về {uid: {name, lat, lon}} của mọi trạm AQICN trong bán kính RADIUS_KM."""
    found = {}
    box = f"{info['lat'] - 0.5},{info['lon'] - 0.5},{info['lat'] + 0.5},{info['lon'] + 0.5}"
    body = get_json(f"{API}/map/bounds/", {"latlng": box, "token": TOKEN}) or {}
    if body.get("status") != "ok":
        log.error("%s: tìm theo khung tọa độ lỗi: %s", city, body)
    for s in body.get("data") or []:
        if body.get("status") == "ok":
            found[s["uid"]] = {"name": s["station"]["name"], "lat": s["lat"], "lon": s["lon"]}

    for keyword in KEYWORDS[city]:
        body = get_json(f"{API}/search/", {"keyword": keyword, "token": TOKEN}) or {}
        if body.get("status") != "ok":
            continue
        for s in body.get("data") or []:
            geo = s["station"].get("geo") or []
            if len(geo) == 2 and s["uid"] not in found:
                found[s["uid"]] = {"name": s["station"]["name"], "lat": geo[0], "lon": geo[1]}

    return {
        uid: s for uid, s in found.items()
        if distance_km(s["lat"], s["lon"], info["lat"], info["lon"]) <= RADIUS_KM
    }


def working_feed(uid):
    """Gọi thử trạm; trả về (mã feed, aqi, thời điểm đo) nếu trạm đang có số liệu."""
    # Trạm cộng đồng có uid âm, một số chỉ gọi được bằng mã dạng A<số>
    refs = [f"@{uid}"] + ([f"A{-uid}"] if uid < 0 else [])
    for ref in refs:
        body = get_json(f"{API}/feed/{ref}/", {"token": TOKEN}) or {}
        data = body.get("data")
        if body.get("status") == "ok" and isinstance(data, dict) and isinstance(data.get("aqi"), (int, float)):
            return ref, data["aqi"], (data.get("time") or {}).get("s")
    return None, None, None


def is_fresh(measured):
    """AQICN vẫn trả số cũ của trạm đã ngừng; chỉ nhận số đo gần đây (giờ địa phương của trạm)."""
    try:
        at = datetime.strptime(measured, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    return datetime.now(VN).replace(tzinfo=None) - at <= timedelta(hours=MAX_AGE_HOURS)


stations = []
for city, info in CITIES.items():
    candidates = discover(city, info)
    active = 0
    for uid, s in sorted(candidates.items()):
        feed, aqi, measured = working_feed(uid)
        if not feed:
            log.info("%s: BỎ   %s  %s (không có số liệu)", city, uid, s["name"])
            continue
        if not is_fresh(measured):
            log.info("%s: BỎ   %s  %s (số liệu cũ, lần đo cuối %s)", city, uid, s["name"], measured)
            continue
        stations.append({"city": city, "uid": uid, "feed": feed, "name": s["name"], "lat": s["lat"], "lon": s["lon"]})
        active += 1
        log.info("%s: GIỮ  %s  AQI %s lúc %s  %s", city, uid, aqi, measured, s["name"])
    log.info("%s: tìm thấy %d trạm, %d trạm có số liệu trong %d giờ gần đây", city, len(candidates), active, MAX_AGE_HOURS)
    if active < MIN_STATIONS:
        log.warning("%s chỉ có %d trạm đang hoạt động (muốn ít nhất %d)", city, active, MIN_STATIONS)

(HERE / "stations.json").write_text(json.dumps(stations, ensure_ascii=False, indent=2), encoding="utf-8")
log.info("Đã ghi %d trạm vào stations.json", len(stations))

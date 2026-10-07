"""Real-time: gọi AQICN và Open-Meteo theo chu kỳ, đẩy JSON vào Kafka."""
import json
import os
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from common import CITIES, HERE, KAFKA_BOOTSTRAP, get_json, setup_logging

log = setup_logging("producer")
TOKEN = os.environ["AQICN_TOKEN"]
INTERVAL = int(os.getenv("POLL_INTERVAL_SEC", "600"))
WEATHER_VARS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,surface_pressure"
AQ_VARS = "pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,ozone,carbon_monoxide,us_aqi"
OPENAQ_KEY = os.getenv("OPENAQ_KEY")
OPENAQ_ACTIVE_DAYS = 3

STATIONS = json.loads((HERE / "stations.json").read_text(encoding="utf-8"))


def load_openaq_stations():
    """Trạm OpenAQ ở hai thành phố còn gửi số liệu gần đây (theo lần chạy `history_openaq.py find` gần nhất)."""
    path = HERE / "openaq_locations.json"
    if not OPENAQ_KEY or not path.exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=OPENAQ_ACTIVE_DAYS)
    return [
        x for x in json.loads(path.read_text(encoding="utf-8"))
        if x["city"] in CITIES and x.get("last") and datetime.fromisoformat(x["last"].replace("Z", "+00:00")) >= cutoff
    ]


OPENAQ_STATIONS = load_openaq_stations()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda k: str(k).encode(),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
    acks="all",
    retries=5,
)


def envelope(source, city, station_id, payload):
    return {
        "source": source,
        "city": city,
        "station_id": station_id,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "payload": payload,
    }


def run_once():
    sent = 0
    for s in STATIONS:
        body = get_json(f"https://api.waqi.info/feed/{s.get('feed', '@%s' % s['uid'])}/", {"token": TOKEN})
        # AQICN trả HTTP 200 cả khi lỗi, phải xem trường status
        if not body or body.get("status") != "ok":
            log.warning("AQICN lỗi ở trạm %s (%s): %s", s["uid"], s["name"], body)
            continue
        producer.send("aqi_raw", key=s["uid"], value=envelope("aqicn", s["city"], s["uid"], body["data"]))
        sent += 1

    for city, info in CITIES.items():
        body = get_json(
            "https://api.open-meteo.com/v1/forecast",
            {"latitude": info["lat"], "longitude": info["lon"], "current": WEATHER_VARS, "timezone": "Asia/Bangkok"},
        )
        if not body:
            continue
        producer.send("weather_raw", key=city, value=envelope("openmeteo", city, None, body))
        sent += 1

    # Số liệu mô hình CAMS: bảo đảm thành phố nào cũng có AQI kể cả khi không có trạm AQICN
    for city, info in CITIES.items():
        body = get_json(
            "https://air-quality-api.open-meteo.com/v1/air-quality",
            {"latitude": info["lat"], "longitude": info["lon"], "current": AQ_VARS, "timezone": "Asia/Bangkok"},
        )
        if not body:
            continue
        producer.send("aqi_raw", key=f"model-{city}", value=envelope("openmeteo_aq", city, None, body))
        sent += 1

    # Số đo tại trạm từ OpenAQ (nguồn trạm thật duy nhất ở TP.HCM). Để cuối vì hay chậm hơn các nguồn khác
    for loc in OPENAQ_STATIONS:
        body = get_json(f"https://api.openaq.org/v3/locations/{loc['id']}/latest", headers={"X-API-Key": OPENAQ_KEY})
        results = (body or {}).get("results")
        if not results:
            log.warning("OpenAQ không có số liệu ở trạm %s (%s)", loc["id"], loc["name"])
            continue
        sensors = {s["id"]: s for s in loc.get("sensors", [])}
        for r in results:
            sensor = sensors.get(r.get("sensorsId"), {})
            r["parameter"], r["units"] = sensor.get("parameter"), sensor.get("units")
        payload = {"location": {k: loc[k] for k in ("id", "name", "lat", "lon")}, "results": results}
        producer.send("aqi_raw", key=f"openaq-{loc['id']}", value=envelope("openaq", loc["city"], loc["id"], payload))
        sent += 1

    producer.flush()
    log.info("Đã gửi %d message", sent)


if __name__ == "__main__":
    log.info("Bắt đầu: %d trạm AQICN, %d trạm OpenAQ %s, chu kỳ %ds, Kafka %s",
             len(STATIONS), len(OPENAQ_STATIONS), [x["id"] for x in OPENAQ_STATIONS], INTERVAL, KAFKA_BOOTSTRAP)
    while True:
        started = time.time()
        try:
            run_once()
        except Exception:
            log.exception("Chu kỳ thất bại, chờ chu kỳ sau")
        time.sleep(max(0, INTERVAL - (time.time() - started)))

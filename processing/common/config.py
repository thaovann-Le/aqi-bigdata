"""Cấu hình dùng chung cho các job Spark của phần C.

Mọi giá trị chỉnh được bằng biến môi trường để không phải sửa code khi demo.
"""
import os
from pathlib import Path

TZ = "Asia/Ho_Chi_Minh"                       # UTC+7, múi giờ của mọi phép cắt giờ/ngày/mùa
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "aqi")
AQI_STANDARD = "US_AQI"                        # phải khớp cột aqi_standard trong schema
CITIES = ("hanoi", "hcm")

# Ngưỡng cảnh báo: AQI > ALERT_THRESHOLD. Hạ xuống (ví dụ 100) khi demo mà trời đang sạch.
ALERT_THRESHOLD = int(os.getenv("ALERT_THRESHOLD", "150"))

# Bảng điểm gãy PM2.5 của US AQI: "2024" (EPA cập nhật 05/2024) hoặc "2012" (bản cũ).
PM25_BREAKPOINTS = os.getenv("PM25_BREAKPOINTS", "2024")

# Số đo OpenAQ real-time cũ hơn ngần này giờ (so với lúc producer gọi API) bị coi là cảm biến đã chết và bỏ.
# Payload OpenAQ "latest" lẫn cả số đo từ nhiều tháng trước (no2 tháng 7, pm10 tháng 1/2025 trong dữ liệu mẫu),
# nhưng cũng có trạm báo trễ cả chục giờ (trạm 2161292 trễ ~14 giờ ở đầu dữ liệu mẫu) -> số đo đó vẫn hợp lệ,
# được ghi theo đúng thời điểm đo. 48 giờ tách được hai trường hợp (dữ liệu real-time 06–13/10/2026: 37% số đo
# chất ô nhiễm cũ hơn 48 giờ, 22% trễ 2–48 giờ).
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "48"))
# Số đo có thời điểm đo SAU lúc gọi API quá ngần này phút là sai đồng hồ -> bỏ (chưa gặp trong dữ liệu thật).
MAX_FUTURE_MINUTES = int(os.getenv("MAX_FUTURE_MINUTES", "60"))

# Nguồn không công bố AQI -> tự tính US AQI từ nồng độ. AQICN và Open-Meteo (CAMS) đã có AQI của nguồn: nếu nguồn
# để trống thì giữ null, không thay bằng AQI tự tính theo giờ (khác phương pháp 24 giờ/NowCast của nguồn).
AQI_FROM_CONCENTRATION = ("openaq",)

REFERENCE_DIR = Path(__file__).resolve().parents[1] / "reference"
STATIONS_CSV = REFERENCE_DIR / "stations.csv"

# Khoảng giá trị hợp lệ [min, max]; ngoài khoảng (kể cả -999 của OpenAQ) -> null. Đây là kiểm tra "có thể xảy ra
# về vật lý", không phải ngưỡng chất lượng không khí. Đối chiếu với dữ liệu thật (OpenAQ 2021–2026, 2,1 triệu dòng):
#   pm25 > 500: 2.223 dòng, 2.175 dòng là giá trị 985 lặp lại của trạm 7440, còn lại tới 4.988 (rác); số thật cao nhất
#        ở Hà Nội vài trăm µg/m³. Bảng AQI 2024 chặn ở 325,4 µg/m³ (AQI 500) nên 500 không làm mất AQI nào.
#   pm10 > 1000: 10 dòng (1.141–5.070); o3 < 0: 1.834 dòng (lệch không của cảm biến), > 800: 4 dòng;
#   no2 > 2000 (tới 12.671), so2 > 2000 (tới 4.155), co > 50.000 (tới 77.685): mỗi loại 4–8 dòng.
POLLUTANT_RANGES = {
    "pm25": (0.0, 500.0),
    "pm10": (0.0, 1000.0),
    "o3": (0.0, 800.0),
    "no2": (0.0, 2000.0),
    "so2": (0.0, 2000.0),
    "co": (0.0, 50000.0),
}
# Thời tiết (Open-Meteo, 63 điểm 2021–2025): chỉ áp suất từng vượt khoảng cũ (850–1100). Đó là áp suất BỀ MẶT
# (surface_pressure) nên phụ thuộc độ cao: Đà Lạt (~1.500 m) thật quanh 840–855 hPa, 7.655/43.824 giờ bị loại nhầm.
# Cận dưới 500 hPa (tương đương ~5.500 m) vẫn chặn được giá trị vô lý mà không phụ thuộc độ cao của điểm.
WEATHER_RANGES = {
    "temperature": (-10.0, 50.0),       # kỷ lục Việt Nam: khoảng -6 °C đến 44 °C
    "humidity": (0.0, 100.0),
    "wind_speed": (0.0, 60.0),          # m/s
    "wind_direction": (0.0, 360.0),
    "precipitation": (0.0, 300.0),      # mm/giờ
    "pressure": (500.0, 1100.0),        # hPa, áp suất bề mặt
}
AQI_RANGE = (0, 500)

POLLUTANTS = tuple(POLLUTANT_RANGES)
WEATHER_COLS = tuple(WEATHER_RANGES)
MEASUREMENT_COLS = (
    ("city", "station_id", "ts") + POLLUTANTS
    + ("aqi", "aqi_standard") + WEATHER_COLS + ("source", "is_model")
)
WEATHER_TABLE_COLS = ("city", "ts") + WEATHER_COLS
# Bảng trạng thái cho streaming (xem infra/cassandra/schema.cql): số đo OpenAQ từng thời điểm, số đọc thời tiết 15 phút.
OBSERVATION_COLS = ("city", "station_id", "ts_obs", "parameter", "value", "fetched_at")
READING_COLS = ("city", "ts_reading", "interval_sec") + WEATHER_COLS + ("fetched_at",)

# Mùa theo thành phố: (tên mùa, tháng bắt đầu, tháng kết thúc), vòng qua năm mới được.
SEASONS = {
    "hanoi": [("spring", 2, 4), ("summer", 5, 7), ("autumn", 8, 10), ("winter", 11, 1)],
    "hcm": [("dry", 11, 4), ("rainy", 5, 10)],
}

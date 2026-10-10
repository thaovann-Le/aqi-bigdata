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
# được ghi theo đúng thời điểm đo. 48 giờ tách được hai trường hợp.
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "48"))

REFERENCE_DIR = Path(__file__).resolve().parents[1] / "reference"
STATIONS_CSV = REFERENCE_DIR / "stations.csv"

# Khoảng giá trị hợp lệ [min, max]; ngoài khoảng (kể cả -999 của OpenAQ) -> null.
# Ngưỡng PM2.5 <= 500 theo gợi ý của B (README ingestion, mục 2.3).
POLLUTANT_RANGES = {
    "pm25": (0.0, 500.0),
    "pm10": (0.0, 1000.0),
    "o3": (0.0, 800.0),
    "no2": (0.0, 2000.0),
    "so2": (0.0, 2000.0),
    "co": (0.0, 50000.0),
}
WEATHER_RANGES = {
    "temperature": (-10.0, 50.0),
    "humidity": (0.0, 100.0),
    "wind_speed": (0.0, 60.0),          # m/s
    "wind_direction": (0.0, 360.0),
    "precipitation": (0.0, 300.0),
    "pressure": (850.0, 1100.0),
}
AQI_RANGE = (0, 500)

POLLUTANTS = tuple(POLLUTANT_RANGES)
WEATHER_COLS = tuple(WEATHER_RANGES)
MEASUREMENT_COLS = (
    ("city", "station_id", "ts") + POLLUTANTS
    + ("aqi", "aqi_standard") + WEATHER_COLS + ("source", "is_model")
)
WEATHER_TABLE_COLS = ("city", "ts") + WEATHER_COLS

# Mùa theo thành phố: (tên mùa, tháng bắt đầu, tháng kết thúc), vòng qua năm mới được.
SEASONS = {
    "hanoi": [("spring", 2, 4), ("summer", 5, 7), ("autumn", 8, 10), ("winter", 11, 1)],
    "hcm": [("dry", 11, 4), ("rainy", 5, 10)],
}

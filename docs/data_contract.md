# Hợp đồng dữ liệu: đầu ra của phần xử lý (C)

Tài liệu cho **D** (phân tích, ML) và **A** (Grafana): có những bảng gì, mỗi cột nghĩa là gì, đơn vị nào,
và nên đọc thế nào. Schema gốc: [`infra/cassandra/schema.cql`](../infra/cassandra/schema.cql).
Cách chạy pipeline để có dữ liệu: [`processing/README.md`](../processing/README.md).

## 1. Quy ước chung

| Quy ước | Giá trị |
|---|---|
| Thành phố (`city`) | `hanoi`, `hcm` có số đo trạm + số mô hình + thời tiết. 61 tỉnh/thành còn lại (mã điểm trong `ingestion/points_vn.json`, ví dụ `haiphong`, `danang`) **chỉ có số mô hình CAMS (`is_model = true`) và thời tiết**, dùng để tăng dung lượng và so sánh; phân tích chính, dashboard và mô hình chỉ dùng `hanoi`, `hcm`. |
| Thời gian | Cột `timestamp` là thời điểm tuyệt đối (Cassandra lưu UTC). Mọi giờ/ngày/tháng/mùa được cắt theo **giờ Việt Nam (UTC+7)**. `cqlsh` hiển thị theo UTC, hãy cộng 7 giờ khi đọc bằng mắt. |
| Mốc giờ | `ts` = **đầu giờ** của thời điểm đo (10:37 → 10:00) |
| Thang AQI | **US AQI** (EPA, bảng điểm gãy PM2.5 bản 05/2024). Cột `aqi_standard` luôn là `US_AQI`. |
| Đơn vị | chất ô nhiễm µg/m³; nhiệt độ °C; độ ẩm %; **gió m/s** (đã đổi từ km/h); hướng gió độ; mưa mm; áp suất hPa |
| Mã trạm (`station_id`) | `<nguồn>:<mã>`: `openaq:2161292`, `aqicn:1583`, `model:hanoi`. Tên và tọa độ trạm: [`processing/reference/stations.csv`](../processing/reference/stations.csv) |
| **Số đo và số mô hình** | `is_model = false`: đo tại trạm (OpenAQ, AQICN). `is_model = true`: số mô hình CAMS của Open-Meteo (trạm ảo `model:<city>`), liền mạch nhưng **không phải số đo**. Hai loại này không bao giờ bị trộn trong các phép tổng hợp. |

## 2. Các bảng

### `measurements` — số liệu theo giờ của từng trạm

Khóa: `((city, station_id), ts)`. Một dòng mỗi trạm-giờ.

| Cột | Ghi chú |
|---|---|
| `pm25`, `pm10`, `o3`, `no2`, `so2`, `co` | µg/m³; trung bình các số đo trong giờ; giá trị âm/vô lý đã bị loại (null) |
| `aqi` | AQICN và Open-Meteo: AQI do nguồn công bố. OpenAQ: C tự tính từ PM2.5/PM10 **theo giờ** (lấy chỉ số con lớn hơn) |
| `temperature` … `pressure` | thời tiết Open-Meteo **mức thành phố**, ghép theo (city, giờ) |
| `source` | `openaq`, `aqicn`, `openmeteo_aq` |
| `is_model` | xem mục 1 |

Lưu ý: AQICN chỉ cho chỉ số AQI (các số `iaqi` của AQICN là chỉ số con, không phải µg/m³), nên dòng AQICN có
`pm25 = null`. Một số trạm có ở cả AQICN và OpenAQ (ví dụ 13026 ≈ 2161292); bảng này giữ cả hai,
còn các phép tổng hợp chỉ tính một lần (cột `physical_id` trong `stations.csv`).

### `weather_hourly` — thời tiết theo giờ của từng thành phố

Khóa: `((city), ts)`. Cột như phần thời tiết của `measurements`.

### `daily_agg` — tổng hợp theo thành phố

Khóa: `((city, period_type, data_basis), period_start)`.

| Cột | Ghi chú |
|---|---|
| `period_type` | `hour`, `day`, `month`, `season` |
| `data_basis` | `measured` (số đo trạm) hoặc `model` (số mô hình) |
| `period_start` | đầu kỳ theo giờ Việt Nam |
| `label` | `2025-01-02` (ngày), `2025-01` (tháng), tên mùa; null với `hour` |
| `avg_aqi`, `max_aqi`, `min_aqi`, `avg_pm25`, `avg_pm10` | `hour`: trung bình/max/min **giữa các trạm** trong giờ. `day`/`month`/`season`: trung bình của các giá trị **giờ-thành phố**, max/min cũng là max/min của các giá trị giờ đó |
| `record_count` | `hour`: số trạm vật lý; các kỳ khác: số giờ có dữ liệu (dùng để biết kỳ nào thiếu dữ liệu) |

Mùa: Hà Nội `spring` (2–4), `summer` (5–7), `autumn` (8–10), `winter` (11–1); TP.HCM `dry` (11–4), `rainy` (5–10).
Mùa vắt qua năm mới tính từ tháng bắt đầu: tháng 1/2025 thuộc `winter` có `period_start` = 2024-11-01.

Bảng này được tính lại định kỳ (`batch_aggregate.py`, mặc định 15 phút một lần trong compose demo).

### `alerts` — cảnh báo

Khóa: `((city), ts, station_id)`. Một dòng khi AQI **đo tại trạm** của một giờ > 150 (đổi bằng `ALERT_THRESHOLD`).
`level`: `unhealthy` (151–200), `very_unhealthy` (201–300), `hazardous` (> 300). `message` có tên trạm.

### `forecasts` — dự báo (D ghi)

Khóa: `((city), forecast_for, generated_at)`. Xem mục 3.3.

## 3. Dành cho D

### 3.1. Huấn luyện / notebook: dùng file CSV đã xuất (khuyên dùng)

Job `export_ml_dataset.py` xuất một bảng phẳng **mỗi dòng một thành phố-giờ**:

- Đường dẫn khi chạy bằng compose demo: `processing/_runtime/ml_dataset/city_hourly_csv/part-*.csv` (một file),
  hoặc `city_hourly_parquet/`.
- Cột: `city, ts_local, ts_utc, aqi, pm25, pm10, n_stations, aqi_model, pm25_model, pm10_model,
  temperature, humidity, wind_speed, wind_direction, precipitation, pressure`.
  - `aqi`, `pm25`, `pm10`: trung bình **số đo** các trạm trong giờ; null nếu giờ đó không có trạm nào.
  - `aqi_model`, `pm25_model`, `pm10_model`: số **mô hình** (có từ 08/2022).
  - Giờ không có cả số đo lẫn số mô hình nhưng có thời tiết vẫn có dòng (cột ô nhiễm null).

```python
import sys; sys.path.insert(0, "processing/examples")
from ml_data_access import load_city_hourly_csv

df = load_city_hourly_csv("processing/_runtime/ml_dataset/city_hourly_csv")   # index: giờ Việt Nam
hn = df[df.city == "hanoi"].asfreq("h")          # đủ mọi giờ, giờ thiếu thành NaN -> lag tính đúng
hn["aqi_lag1"] = hn["aqi"].shift(1)
hn["aqi_lag24"] = hn["aqi"].shift(24)
hn["target_aqi_24h"] = hn["aqi"].shift(-24)
```

Gợi ý: biến mục tiêu nên là `aqi`/`pm25` (số đo). Nếu lấp chỗ thiếu bằng `aqi_model` thì thêm cột đánh dấu.
Lưu ý thiếu dữ liệu: TP.HCM không có trạm đo trong 01–11/2024; Hà Nội 2022 chỉ khoảng 35% số giờ
(chi tiết trong `ingestion/README.md` mục 2 và 4).

### 3.2. Script dự báo hằng giờ: đọc thẳng Cassandra

Không nên quét `measurements` (khóa phân vùng là từng trạm). Dùng `daily_agg` với `period_type='hour'`
(mỗi thành phố một phân vùng, đọc theo khoảng thời gian rất nhanh) và `weather_hourly`:

```sql
SELECT period_start, avg_aqi, avg_pm25, record_count FROM aqi.daily_agg
 WHERE city='hanoi' AND period_type='hour' AND data_basis='measured'
   AND period_start >= '2026-10-08 00:00:00+0700';
SELECT * FROM aqi.weather_hourly WHERE city='hanoi' AND ts >= '2026-10-08 00:00:00+0700';
```

Hàm có sẵn trả về cùng định dạng DataFrame như mục 3.1:

```python
from datetime import datetime, timedelta, timezone
from ml_data_access import connect, load_city_hourly_cassandra, write_forecasts

session = connect(["localhost"])                         # cổng 9042 mở ra máy host
end = datetime.now(timezone.utc)
df = load_city_hourly_cassandra("hanoi", end - timedelta(hours=48), end, session)
```

`daily_agg` giờ chỉ mới bằng lần chạy `batch_aggregate` gần nhất (15 phút một lần trong compose demo).
Nếu dự báo lúc hh:00, hãy chạy lúc hh:20 hoặc đặt `AGG_INTERVAL_SEC` nhỏ hơn.

### 3.3. Ghi dự báo

```python
write_forecasts(session, [{"city": "hanoi", "forecast_for": t_plus_24h, "predicted_aqi": 87.0,
                           "predicted_pm25": 30.1, "horizon_hours": 24}], model_name="xgboost_v1")
```

Mỗi lần chạy là một `generated_at` mới, nên giữ được lịch sử các lần dự báo cho cùng một giờ
(dashboard "dự báo so với thực tế" lấy bản mới nhất). Mô hình cơ sở ghi `model_name='baseline_persistence'`.

## 4. Dành cho A (Grafana, plugin `hadesarchitect-cassandra-datasource`)

Plugin yêu cầu truy vấn theo khóa phân vùng. Gợi ý cho từng panel:

| Panel | Truy vấn |
|---|---|
| AQI theo thời gian, so sánh 2 thành phố | `daily_agg`, `period_type='hour'`, `data_basis='measured'`, mỗi `city` một truy vấn |
| Xu hướng theo ngày/tháng/mùa | `daily_agg` với `period_type` tương ứng |
| Bảng cảnh báo | `SELECT ts, station_id, aqi, level, message FROM aqi.alerts WHERE city='hanoi' AND ts > $__timeFrom` |
| Bản đồ trạm | tọa độ trong `processing/reference/stations.csv`; AQI mới nhất của trạm: `SELECT ts, aqi FROM aqi.measurements WHERE city=? AND station_id=? LIMIT 1` |
| Dự báo so với thực tế | `forecasts` (D) chồng lên `daily_agg` giờ |

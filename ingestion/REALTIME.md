# Dữ liệu real-time: cách lấy và mô tả

Dành cho người xử lý luồng real-time (Spark Streaming). Chi tiết đầy đủ hơn nằm trong [README.md](README.md).

## Lấy dữ liệu

Kafka trên máy thu thập không truy cập được từ máy khác, nên dữ liệu được giao dưới dạng file và phát lại vào Kafka của bạn.

```bash
cd ingestion
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                       # sửa KAFKA_BOOTSTRAP=localhost:9094 nếu dùng compose của nhóm
unzip results/raw-full.zip -d export/      # tạo export/raw/
python replay.py export/raw                # gửi hết một lượt
python replay.py export/raw 0.05           # hoặc: nghỉ 0,05 giây giữa hai message
```

Message vào đúng topic `aqi_raw` và `weather_raw`. Muốn thử nhanh thì dùng `results/raw-mau.zip` (14 giờ đầu).

## Dữ liệu gồm gì

- **Thời gian:** 23:35 ngày 06/10/2026 đến 06:55 ngày 13/10/2026, chu kỳ 10 phút, 14 message mỗi chu kỳ.
- **Quy mô:** 11.555 message, 14 MB (nén 2,8 MB).
- **File:** `raw/<topic>/date=<ngày>/*.jsonl`, mỗi dòng một message. `date` là ngày ghi file theo giờ Việt Nam.

| Topic | `source` | Số message | Là gì | Khóa Kafka |
|---|---|---|---|---|
| `aqi_raw` | `aqicn` | 3.305 | Số đo tại 4 trạm Hà Nội | Mã trạm |
| `aqi_raw` | `openaq` | 4.944 | Số đo tại 5 trạm Hà Nội và 1 trạm TP.HCM | `openaq-<mã trạm>` |
| `aqi_raw` | `openmeteo_aq` | 1.654 | AQI mô hình CAMS, mỗi thành phố một điểm | `model-<thành phố>` |
| `weather_raw` | `openmeteo` | 1.652 | Thời tiết, mỗi thành phố một điểm | Tên thành phố |

## Cấu trúc message

Mọi message có cùng lớp vỏ; phần khác nhau nằm trong `payload`.

```json
{
  "source": "openaq",
  "city": "hcm",
  "station_id": 3276359,
  "fetched_at": "2026-10-09T17:03:14+00:00",
  "payload": {}
}
```

| Trường | Ý nghĩa |
|---|---|
| `source` | `aqicn`, `openaq`, `openmeteo_aq` hoặc `openmeteo` |
| `city` | `hanoi` hoặc `hcm` |
| `station_id` | Mã trạm; `null` với hai nguồn Open-Meteo |
| `fetched_at` | Lúc gọi API (UTC), **không phải lúc đo** |

**`payload` theo từng nguồn:**

| `source` | Trường cần dùng | Thời điểm đo |
|---|---|---|
| `aqicn` | `aqi` (thang US AQI); `iaqi.pm25.v`, `iaqi.pm10.v`, `iaqi.o3.v`, `iaqi.so2.v`, `iaqi.t.v`, `iaqi.h.v`; `city.geo` (tọa độ) | `time.iso`, có kèm múi giờ |
| `openaq` | `location` (`id`, `name`, `lat`, `lon`); `results[]`, mỗi phần tử là một thông số: `parameter`, `units`, `value` | `results[].datetime.utc` và `.local` |
| `openmeteo_aq` | `current.pm2_5`, `current.pm10`, `current.nitrogen_dioxide`, `current.sulphur_dioxide`, `current.ozone`, `current.carbon_monoxide` (µg/m³), `current.us_aqi` | `current.time`, giờ Việt Nam, không kèm múi giờ |
| `openmeteo` | `current.temperature_2m` (°C), `relative_humidity_2m` (%), `precipitation` (mm), `wind_speed_10m` (km/h), `wind_direction_10m` (°), `surface_pressure` (hPa) | `current.time`, giờ Việt Nam, không kèm múi giờ |

Ví dụ một phần tử `results` của `openaq`:

```json
{"datetime": {"utc": "2026-10-09T02:00:00Z", "local": "2026-10-09T09:00:00+07:00"},
 "value": 0.51, "sensorsId": 7771973, "locationsId": 2161292, "parameter": "so2", "units": "µg/m³"}
```

## Phải xử lý khi đọc

1. **`aqicn.aqi` không phải lúc nào cũng là số.** Khoảng 17% message có `"aqi": "-"` (trạm chưa có chỉ số tổng). Đọc trường này dạng chuỗi rồi chuyển đổi, bỏ giá trị không phải số.
2. **Bỏ trùng theo thời điểm đo.** Các nguồn cập nhật mỗi giờ nhưng được hỏi 10 phút một lần, nên cùng một số đo lặp lại nhiều lần. Khóa bỏ trùng: `source`, `station_id`, thông số, thời điểm đo.
3. **Số đo có thể cũ.** Một message `openaq` có thể mang số đo từ nhiều giờ trước nếu cảm biến chưa cập nhật. Dùng thời điểm đo, không dùng `fetched_at`.
4. **Khóa trạm là cặp (`source`, `station_id`).** Mã của AQICN và OpenAQ có thể trùng số. Một số trạm Hà Nội có mặt ở cả hai nguồn.
5. **Đơn vị khác nhau.** `aqicn` cho chỉ số AQI; `openaq` cho nồng độ (µg/m³), cần quy đổi nếu lưu AQI.
6. **`openmeteo_aq` là số mô hình**, không phải số đo. Nên có một cột đánh dấu.
7. **TP.HCM chỉ có một trạm đo thật** (CMT8, mã 3276359, qua `openaq`).
8. **Có gián đoạn** khi máy thu thập ngủ hoặc tắt, tổng khoảng 10,8 giờ; danh sách ở mục 7 của README. Sau mỗi lần máy thức dậy có thể có một lô message bị ghi trùng.

Số liệu theo giờ để lấp các khoảng gián đoạn nằm trong `results/backfill.zip`; đó là dữ liệu tải lại sau, không phải message real-time.

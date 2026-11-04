# Chương: Dữ liệu & 3V (vai B)

## 1. Nguồn dữ liệu

| Nguồn | Loại dữ liệu | Phạm vi | Tần suất | Định dạng gốc |
|---|---|---|---|---|
| AQICN | AQI (US AQI) + chỉ số con PM2.5/PM10 theo trạm | Hà Nội (4 trạm) | Real-time, 10 phút/lần | JSON |
| OpenAQ | Nồng độ chất ô nhiễm (µg/m³) theo trạm | Hà Nội (38 trạm có số liệu), TP.HCM (13 trạm) | Lịch sử 2021–10/2026 + real-time 10 phút/lần | CSV (lịch sử), JSON (real-time) |
| Open-Meteo (thời tiết) | Nhiệt độ, độ ẩm, gió, mưa, áp suất | 63 tỉnh/thành (điểm trong `points_vn.json`) | Lịch sử 2021–2025 + real-time | JSON dạng cột |
| Open-Meteo (chất lượng không khí, mô hình CAMS) | Nồng độ + US AQI mô hình hóa | 63 tỉnh/thành | Lịch sử từ 08/2022 + real-time | JSON dạng cột |
| NASA FIRMS | Điểm cháy/đốt rơm rạ | — | — | **Không dùng** — để ở mục "nếu còn thời gian" trong `docs/PLAN.md`, không thực hiện do giới hạn 1 tuần |

Chỉ Hà Nội và TP.HCM có số liệu đo tại trạm (AQICN + OpenAQ); 61 tỉnh/thành còn lại chỉ có số liệu mô hình CAMS
của Open-Meteo, dùng để tăng dung lượng/so sánh tương đối (mục 3), không dùng cho phân tích chính hay huấn luyện
mô hình (xem `docs/data_integration_concepts.md`, `docs/analysis_ml.md`).

Nguồn gốc file lịch sử và mẫu real-time: `ingestion/README.md` mục 1 (chi tiết dung lượng, đường dẫn tải).

## 2. Mô tả trường dữ liệu

Hai tầng dữ liệu, trường khác nhau vì khác định dạng gốc:

**Nguồn thô (B thu thập)** — xem `ingestion/README.md` mục 3 cho bảng cột đầy đủ:
- OpenAQ (CSV): `location_id`, `sensors_id`, `location`, `datetime` (kèm múi giờ), `lat`, `lon`, `parameter`
  (`pm25`, `pm10`, `no2`, `so2`, `o3`, `co`...), `units`, `value` (`-999.0` = thiếu).
- Open-Meteo (JSON dạng cột): `hourly.time` + một mảng cùng độ dài cho mỗi biến — thời tiết:
  `temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `wind_direction_10m`,
  `surface_pressure`; chất lượng không khí: `pm2_5`, `pm10`, `nitrogen_dioxide`, `sulphur_dioxide`, `ozone`,
  `carbon_monoxide`, `us_aqi`.
- Real-time (Kafka, JSON): mỗi message có lớp vỏ chung `{source, city, station_id, fetched_at, payload}`,
  `payload` giữ nguyên cấu trúc gốc của từng nguồn (xem `ingestion/README.md` mục 7).

**Dữ liệu đã chuẩn hóa (C xử lý, dùng cho dashboard/ML)** — bảng `measurements`: `pm25`, `pm10`, `o3`, `no2`,
`so2`, `co` (µg/m³), `aqi` (US AQI), `temperature`…`pressure`, `source`, `is_model`. Mô tả đầy đủ, kèm bảng
`daily_agg`/`alerts`/`forecasts`: `docs/data_contract.md`.

## 3. Chứng minh 3V

Số liệu Volume/Variety đo trên bộ dữ liệu B thu thập (`ingestion/results/stats_3v.txt`, cập nhật khi nạp
`backfill.zip` ngày 13/10/2026):

### Volume

| Nguồn | Số file | Dung lượng | Số bản ghi |
|---|---:|---:|---:|
| `history/openaq` | 81 | 13,17 MB | 2.116.508 |
| `history/openmeteo/air_quality` | 252 | 112,95 MB | 1.883.952 |
| `history/openmeteo/weather` | 315 | 145,96 MB | 2.760.912 |
| `raw/aqi_raw` (real-time) | 78 | 1,21 MB | 954 |
| `raw/weather_raw` (real-time) | 78 | 0,12 MB | 162 |
| **Tổng** | **804** | **273,4 MB** | **6.762.488** |

Sau khi C xử lý (lọc, ghép thời tiết, tổng hợp), số liệu trong Cassandra (đo trực tiếp bằng
`processing/_runtime/metrics.jsonl` sau khi chạy `batch_history` + `batch_aggregate` trên toàn bộ dữ liệu):

| Bảng | Số dòng |
|---|---:|
| `measurements` | 2.102.033 (trong đó 1.883.865 là số mô hình CAMS) |
| `weather_hourly` | 2.761.266 |
| `alerts` | 37.109 |
| `daily_agg` (giờ) | 1.950.663 |
| `daily_agg` (ngày) | 81.479 |
| `daily_agg` (tháng) | 2.700 |
| `daily_agg` (mùa) | 57 |
| Bảng thành phố-giờ xuất cho ML (`export_ml_dataset.py`, 63 tỉnh/thành) | 2.774.442 |

### Velocity

Tốc độ dữ liệu **đến** (producer real-time của B, 10 phút/lần — xem `ingestion/README.md` mục 7):

| Ngày | Topic | Số bản ghi | Bản ghi/giờ |
|---|---|---:|---:|
| 06/10/2026 | `aqi_raw` | 42 | 1,8 |
| 06/10/2026 | `weather_raw` | 10 | 0,4 |
| 07/10/2026 | `aqi_raw` | 912 | 38,0 |
| 07/10/2026 | `weather_raw` | 152 | 6,3 |

(Ngày 06/10 thấp vì producer mới chạy từ 23:35.) Producer chạy liên tục **từ 23:35 06/10 đến ít nhất 13/10/2026**
(dữ liệu mở rộng qua `backfill.zip`, ~7 ngày) — đáp ứng yêu cầu "thu thập real-time liên tục ít nhất 5 ngày"
trong `docs/PLAN.md`.

Tốc độ **xử lý** (đo trên máy chạy demo, `processing/_runtime/metrics.jsonl`):

| Job | Số dòng | Thời gian | Tốc độ |
|---|---:|---:|---|
| `batch_history` (toàn bộ lịch sử, 63 tỉnh/thành) | 2.102.033 (measurements) + 2.761.266 (weather) | 480 giây | ~262.700 dòng/phút |
| Streaming (1 micro-batch, dữ liệu real-time mẫu) | 1.116 message → 114 dòng measurements | 94 giây | ~714 message/phút |

### Variety

- **3 nguồn khác nhau** (AQICN, OpenAQ, Open-Meteo), **2 loại số liệu** (đo tại trạm vs mô hình CAMS), mỗi nguồn
  một schema gốc khác nhau trước khi C chuẩn hóa về chung `measurements`/`weather_hourly`.
- **3 định dạng file**: JSON (567 file — Open-Meteo lịch sử + message real-time), JSONL (156 file — real-time
  lưu trong MinIO theo ngày), CSV nén gzip (81 file — OpenAQ lịch sử).
- **Khác thang đo AQI**: AQICN và Open-Meteo cho sẵn US AQI; OpenAQ chỉ cho nồng độ (µg/m³), C phải tự tính AQI
  theo công thức EPA (`docs/processing.md` mục 5).
- **Khác tần suất gốc**: phần lớn trạm đo 1 giờ/lần, riêng 3 trạm OpenAQ (4946811/4946812/4946813) đo 5 phút/lần
  — phải gộp về giờ trước khi ghép với thời tiết.
- **Khác múi giờ biểu diễn**: OpenAQ lịch sử có kèm múi giờ (`+07:00`); Open-Meteo lịch sử là giờ Việt Nam
  nhưng **không** kèm múi giờ; real-time `fetched_at` là UTC. Chi tiết: `ingestion/README.md` mục 5.

## 4. Thách thức khi thu thập

| Thách thức | Biểu hiện thực tế | Cách xử lý |
|---|---|---|
| Giới hạn số lượt gọi API | AQICN/OpenAQ đều giới hạn request; producer gọi theo chu kỳ cố định (10 phút), không gọi dồn dập | Lưu dữ liệu thô vào MinIO/Kafka trước khi xử lý, để không phải gọi lại API khi cần tính toán lại |
| Giá trị thiếu | OpenAQ dùng `-999.0` để biểu diễn "không có số đo" | Lọc `value < 0` thành null **trước** mọi phép tính trung bình (nếu không sẽ kéo lệch kết quả) |
| Giá trị bất thường | Trạm 7440 (TP.HCM) lặp lại giá trị 985 nhiều năm; trạm 2161306 (Hà Nội) có giá trị PM2.5 > 4.000 µg/m³ | Đặt ngưỡng hợp lệ theo từng thông số (PM2.5 0–500, PM10 0–1000...) trong `processing/common/config.py` |
| Tập trạm thay đổi theo thời gian | TP.HCM không có trạm đo hợp lệ 01/2024–11/2024 (trạm Lãnh sự quán Mỹ ngừng); Hà Nội đổi từ 1 trạm (Đại sứ quán Mỹ) sang mạng nhiều trạm thành phố từ 2024 | Gắn cờ `is_model`/`data_basis` phân biệt số đo và số mô hình; giới hạn giai đoạn huấn luyện ML vào mạng trạm hiện tại (xem `ml/README.md`) thay vì trộn lẫn các giai đoạn |
| Real-time gián đoạn | Máy chạy producer gập/sập 2 lần trong tuần đầu (07/10, tổng khoảng 2 giờ) | Chấp nhận khoảng trống (không nội suy ở tầng thu thập); dữ liệu lịch sử bù vào phần lớn khối lượng cho phân tích |
| Thang đo AQI khác nhau giữa nguồn | AQICN/Open-Meteo có sẵn AQI, OpenAQ chỉ có nồng độ | Chọn thống nhất **US AQI** cho toàn hệ thống (lý do: đây là thang cả AQICN và Open-Meteo đều dùng, chỉ cần tự tính thêm cho OpenAQ) |
| Trạm trùng giữa hai nguồn | Cùng một trạm vật lý xuất hiện ở cả AQICN và OpenAQ (ví dụ khu vực Minh Khai – Bắc Từ Liêm) | Gán `physical_id` chung theo khoảng cách địa lý ≤ 300 m (`processing/tools/build_reference.py`), tránh đếm hai lần khi tổng hợp theo thành phố |

# Thu thập dữ liệu (phần của B)

Hướng dẫn cho A, C và D: có những dữ liệu gì, lấy về thế nào, và phải lưu ý gì khi dùng. Mã thu thập nằm cùng thư mục này.

## 1. Có những dữ liệu gì

| Dữ liệu | Phạm vi | Kích thước | Lấy ở đâu |
|---|---|---|---|
| Lịch sử Hà Nội + TP.HCM | OpenAQ 2021–10/2026, Open-Meteo 2021–2025 | `history-hn-hcm.zip`, 14 MB | `<DÁN LINK>` |
| Lịch sử đầy đủ | Như trên, thêm Open-Meteo của 61 tỉnh/thành khác | `history-full.zip`, 61 MB (giải nén 261 MB) | `<DÁN LINK>` |
| Mẫu real-time | Khoảng 14 giờ đầu, 1.116 message | `results/raw-mau.zip`, 264 KB | Có sẵn trong repo |
| Real-time đầy đủ | Từ 23:35 ngày 06/10 đến 06:55 ngày 13/10/2026; 819 chu kỳ, 11.555 message | `results/raw-full.zip`, 2,8 MB (giải nén 14 MB) | Có sẵn trong repo |
| Dữ liệu tải bù | Số liệu theo giờ 06–13/10 để lấp các khoảng gián đoạn của real-time (mục 7) | `results/backfill.zip` | Có sẵn trong repo |
| Số liệu 3V | Dung lượng, số bản ghi, tốc độ | `results/stats_3v.txt` | Có sẵn trong repo |
| Danh sách trạm | AQICN, OpenAQ, 63 điểm Open-Meteo | `stations.json`, `openaq_locations.json`, `points_vn.json` | Có sẵn trong repo |

`history-full.zip` đã bao gồm toàn bộ `history-hn-hcm.zip`. Ai chỉ làm việc với hai thành phố thì tải file nhỏ là đủ.

Tổng hiện có: 273 MB, khoảng 6,76 triệu bản ghi (2,1 triệu số đo OpenAQ, 4,6 triệu giờ Open-Meteo).

## 2. Phải biết trước khi dùng

1. **Giá trị âm là dữ liệu thiếu.** Trong OpenAQ, `-999.0` nghĩa là không có số đo. Lọc `value < 0` trước mọi phép tính.
2. **TP.HCM không có số đo tại trạm trong 01/2024–11/2024.** Trạm Lãnh sự quán Mỹ chỉ còn `-999` từ 2024; trạm thay thế (CMT8) bắt đầu từ 19/11/2024.
3. **Có giá trị bất thường.** Trạm 7440 có giá trị 985 lặp lại nhiều năm; trạm 2161306 có giá trị trên 4.000 µg/m³. Nên đặt ngưỡng loại bỏ, ví dụ PM2.5 trên 500.
4. **Tần suất đo khác nhau.** Đa số một lần mỗi giờ; ba trạm 4946811, 4946812, 4946813 đo 5 phút một lần. Gộp về theo giờ trước khi ghép.
5. **Số đo và số mô hình không được trộn lẫn.** OpenAQ và AQICN là số đo tại trạm; Open-Meteo Air Quality là số từ mô hình CAMS, liền mạch nhưng không phải số đo.
6. **Múi giờ khác nhau giữa các nguồn** (mục 5).
7. **Real-time có gián đoạn** (mục 7).

## 3. Cấu trúc và định dạng file

```text
history/
├── openaq/locationid=<mã trạm>/year=<năm>/location-<mã>-<năm>.csv.gz   # mỗi trạm mỗi năm một file
└── openmeteo/
    ├── weather/point=<tên điểm>/year=<năm>.json          # 2021–2025
    └── air_quality/point=<tên điểm>/year=<năm>.json      # 2022–2025, bắt đầu từ 04/08/2022
raw/
├── aqi_raw/date=<ngày>/*.jsonl          # mỗi dòng một message
└── weather_raw/date=<ngày>/*.jsonl
```

Tên điểm Open-Meteo (`hanoi`, `hcm`, `haiphong`...) và tọa độ nằm trong `points_vn.json`.

**OpenAQ (CSV nén).** Mỗi dòng là một số đo của một thông số tại một thời điểm. OpenAQ chỉ có trạm quanh Hà Nội (38 trạm có dữ liệu) và TP.HCM (13 trạm), cộng một trạm ở Đội Bình.

| Cột | Ví dụ | Ghi chú |
|---|---|---|
| `location_id` | `7441` | Mã trạm |
| `sensors_id` | `21632` | Mỗi thông số một cảm biến |
| `location` | `Hanoi-7441` | Tên trạm, có thể đổi theo thời gian |
| `datetime` | `2025-01-01T01:00:00+07:00` | Có kèm múi giờ |
| `lat`, `lon` | `21.021939`, `105.818806` | |
| `parameter` | `pm25` | Còn có `pm10`, `pm1`, `no2`, `so2`, `o3`, `co`, `temperature`, `relativehumidity`, `um003` tùy trạm |
| `units` | `µg/m³` | |
| `value` | `35.0` | `-999.0` là thiếu |

**Open-Meteo (JSON dạng cột).** `hourly.time` là danh sách thời điểm, mỗi biến là một danh sách cùng độ dài. Đơn vị nằm trong `hourly_units`, tên điểm trong `point_id`.

| File | Biến trong `hourly` |
|---|---|
| weather | `temperature_2m` (°C), `relative_humidity_2m` (%), `precipitation` (mm), `wind_speed_10m` (km/h), `wind_direction_10m` (°), `surface_pressure` (hPa) |
| air_quality | `pm2_5`, `pm10`, `nitrogen_dioxide`, `sulphur_dioxide`, `ozone`, `carbon_monoxide` (đều µg/m³), `us_aqi` |

## 4. Nên dùng trạm nào cho PM2.5

Số giờ có PM2.5 hợp lệ (đã bỏ giá trị âm). Một năm đủ là 8.760 giờ.

**Hà Nội**

| Trạm | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (đến 06/10) |
|---|---|---|---|---|---|---|
| 7441 — Đại sứ quán Mỹ | 4.966 | 3.080 | 5.645 | 7.218 | 2.265 (ngừng 04/2025) | — |
| 2161292 — Số 46 Lưu Quang Vũ | — | — | — | 7.043 | 7.360 | 2.638 |
| 2161306 — Minh Khai, Bắc Từ Liêm | — | — | — | 4.378 | 1.791 | 1.418 |

**TP.HCM**

| Trạm | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (đến 06/10) |
|---|---|---|---|---|---|---|
| 7440 — Lãnh sự quán Mỹ | 7.748 | 4.191 | 4.388 | 0 | 0 | — |
| 3276359 — CMT8 | — | — | — | 1.006 (từ 19/11) | 7.453 | 6.574 |

- **Hà Nội:** 7441 cho 2021–2023, rồi 2161292 từ 2024. Hai trạm trùng nhau từ 2024 đến 04/2025, dùng để đối chiếu. Năm 2022 chỉ có 35% số giờ.
- **TP.HCM:** 7440 cho 2021–2023, rồi CMT8 từ 11/2024. Khoảng 01–11/2024 chỉ có số mô hình của Open-Meteo; nếu dùng thì phải đánh dấu.
- Trạm CMT8 có vẻ là cảm biến giá rẻ (đo cả PM1 và đếm hạt), khác loại với trạm của cơ quan ngoại giao Mỹ, nên hai giai đoạn của TP.HCM không hoàn toàn so sánh được.
- Các trạm còn lại ở Hà Nội chỉ có dữ liệu trong 2024–2025, nhiều trạm chỉ vài ngày đến vài tháng. Các trạm còn lại ở TP.HCM chỉ có vài giờ đến vài tuần.

## 5. Múi giờ

| Nguồn | Thời gian | Cách xử lý |
|---|---|---|
| OpenAQ lịch sử | Có kèm múi giờ, ví dụ `+07:00` | Đọc trực tiếp |
| Open-Meteo lịch sử | Giờ Việt Nam, **không** kèm múi giờ (`2023-01-01T00:00`) | Gán `Asia/Ho_Chi_Minh` khi đọc |
| Real-time: `fetched_at` | UTC | Là lúc gọi API, không phải lúc đo |
| Real-time: thời điểm đo | Nằm trong `payload` của từng nguồn | Xem mục 7 |

## 6. Cách lấy dữ liệu về dùng

### Cách 1: đọc thẳng file (D)

Tải file zip, giải nén, đọc bằng pandas. Đoạn mã dưới chưa được chạy thử; chỉnh đường dẫn theo nơi giải nén.

```python
import glob
import json

import pandas as pd

# PM2.5 đo tại một trạm, gộp về theo giờ
files = glob.glob("history/openaq/locationid=7441/year=*/*.csv.gz")
df = pd.concat(pd.read_csv(f) for f in files)
df = df[(df["parameter"] == "pm25") & (df["value"] >= 0)]
df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
pm25 = df.set_index("datetime")["value"].resample("1h").mean()

# Thời tiết một năm
with open("history/openmeteo/weather/point=hanoi/year=2023.json", encoding="utf-8") as f:
    body = json.load(f)
weather = pd.DataFrame(body["hourly"])
weather["time"] = pd.to_datetime(weather["time"]).dt.tz_localize("Asia/Ho_Chi_Minh")
weather = weather.set_index("time")
```

### Cách 2: nạp vào MinIO (C và A)

1. Bật hạ tầng của nhóm (`docker compose up -d` ở thư mục gốc repo).
2. Cài thư viện và tạo file cấu hình:

```bash
cd ingestion
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

3. Sửa `ingestion/.env` cho khớp với `docker-compose.yml` của nhóm:

```bash
KAFKA_BOOTSTRAP=localhost:9094
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
```

Hai dòng `AQICN_TOKEN` và `OPENAQ_KEY` chỉ cần khi tự chạy thu thập.

4. Giải nén file zip vào `ingestion/export/` rồi nạp:

```bash
python sync_minio.py import export/history history
python sync_minio.py import export/raw raw
```

Lệnh bỏ qua file đã có, nên khi nhận bản dữ liệu mới chỉ cần chạy lại. Kiểm tra bằng `python check_data.py` hoặc mở http://localhost:9001. Không muốn dùng script thì kéo thả thư mục `history` và `raw` vào đúng bucket trên giao diện web của MinIO.

### Cách 3: phát lại real-time vào Kafka (C)

Kafka trên máy B không truy cập được từ máy khác. Để thử Spark Streaming, phát lại các message đã lưu vào Kafka của mình:

```bash
unzip results/raw-mau.zip -d export/
python replay.py export/raw          # gửi hết một lượt
python replay.py export/raw 0.05     # nghỉ 0,05 giây giữa hai message để giống luồng thật
```

## 7. Dữ liệu real-time

Producer chạy từ khoảng 23:35 ngày 06/10/2026, chu kỳ 10 phút, 14 message mỗi chu kỳ (khoảng 2.000 mỗi ngày). Mọi message có cùng lớp vỏ:

```json
{
  "source": "aqicn",
  "city": "hanoi",
  "station_id": 1583,
  "fetched_at": "2026-10-06T16:40:06+00:00",
  "payload": { "...": "JSON gốc của nguồn" }
}
```

| Topic | `source` | Message mỗi chu kỳ | `station_id` | Nội dung `payload` |
|---|---|---|---|---|
| `aqi_raw` | `aqicn` | 4 (trạm Hà Nội) | Mã trạm AQICN | `aqi` (thang US AQI), `iaqi.pm25.v`, `iaqi.pm10.v`, `time.iso` (lúc đo) |
| `aqi_raw` | `openaq` | 6 (5 trạm Hà Nội, 1 trạm TP.HCM) | Mã trạm OpenAQ | `location` (mã, tên, tọa độ) và `results`: các số đo mới nhất, mỗi phần tử có `parameter`, `units`, `value`, thời điểm đo |
| `aqi_raw` | `openmeteo_aq` | 2 (mỗi thành phố một) | `null` | `current`: `pm2_5`, `pm10`, `us_aqi`... (số mô hình) |
| `weather_raw` | `openmeteo` | 2 (mỗi thành phố một) | `null` | `current`: nhiệt độ, độ ẩm, mưa, gió, áp suất |

- **Khóa trạm là cặp (`source`, `station_id`)**, vì mã của AQICN và OpenAQ có thể trùng số.
- **Bản ghi trùng là bình thường.** Các nguồn cập nhật theo giờ trong khi producer gọi 10 phút một lần. Bỏ trùng theo trạm và thời điểm đo.
- **Một số trạm Hà Nội có ở cả AQICN và OpenAQ** (ít nhất là Minh Khai - Bắc Từ Liêm). Đừng đếm thành hai trạm.
- **TP.HCM chỉ có một trạm đo thật** (CMT8, qua OpenAQ). AQICN không còn trạm nào hoạt động ở đó.
- Cấu trúc `payload` của `openaq` được mô tả theo mã của producer; hãy mở file mẫu xem cấu trúc thật trước khi viết schema.
- OpenAQ cho nồng độ (µg/m³), không cho chỉ số AQI; cần quy đổi nếu lưu AQI.

**Gián đoạn đã biết** (máy thu thập bị ngủ hoặc tắt):

| Khoảng gián đoạn | Nguyên nhân |
|---|---|
| 07/10, 07:50–08:57 | Gập máy |
| 07/10, 17:27–19:24 | Máy sập, khởi động lại |
| 08/10, 08:14–08:53 | Máy ngủ |
| 08/10, 17:33–21:20 | Máy ngủ |
| 09/10, 08:10–08:47 | Gập máy |
| 09/10, 17:33–18:23 | Máy ngủ |
| 12/10, 08:16–08:55 | Máy ngủ |
| 12/10, 09:45–10:02 | Máy ngủ |
| 12/10, 17:32–18:28 | Máy ngủ |
| 13/10, 00:17–06:55 | Máy ngủ qua đêm |

Số chu kỳ mỗi ngày (đủ là 144): 07/10: 128, 08/10: 121, 09/10: 138, 10/10: 144, 11/10: 144, 12/10: 136. Tính đến 00:17 ngày 13/10, tổng thời gian gián đoạn là khoảng 10,8 giờ trên 144,7 giờ.

Có lô message bị ghi trùng sau các lần máy ngủ dậy (ví dụ 14 message lúc 08:01 ngày 07/10); bỏ trùng như hướng dẫn ở trên.

**Dữ liệu tải bù.** `results/backfill.zip` chứa số liệu theo giờ của cả giai đoạn 06–13/10, lấy lại sau từ các nguồn, để có chuỗi liền mạch qua các khoảng gián đoạn:

```text
history/backfill/
├── openaq/locationid=<mã>/location-<mã>-backfill.csv.gz        # 6 trạm đang theo dõi, trung bình theo giờ, cùng cột với OpenAQ lịch sử
└── openmeteo/<weather|air_quality>/point=<hanoi|hcm>/backfill.json   # cùng dạng với Open-Meteo lịch sử
```

- Đây là dữ liệu tải lại, không phải message real-time; không tính vào số liệu Velocity.
- AQICN không tải bù được, vì API chỉ trả số đo mới nhất.
- Thời điểm trong file OpenAQ tải bù là lúc kết thúc của mỗi giờ. Trước khi ghép với dữ liệu real-time, hãy đối chiếu vài giá trị với message `openaq` cùng giờ để chắc không lệch một giờ.

## 8. Lưu ý theo từng người

**A (hạ tầng, dashboard)**
- Tọa độ trạm real-time lấy từ `stations.json` (AQICN) và `openaq_locations.json` (OpenAQ).
- Panel cần lọc theo thành phố, vì dữ liệu lịch sử có 63 điểm.
- Biểu đồ theo thời gian sẽ có chỗ đứt ở các khoảng gián đoạn.
- Hôm demo, producer và consumer chạy trên máy demo (mục 9); cần `.env`, `stations.json`, `openaq_locations.json`.

**C (Spark)**
- Job batch cần: lọc giá trị âm và bất thường, gộp trạm đo 5 phút về theo giờ, chuyển Open-Meteo từ dạng cột sang dạng dòng và gán múi giờ.
- Job nên chạy lại được, vì sẽ có thêm dữ liệu real-time và có thể có dữ liệu tải bù.
- Schema nên có trường phân biệt số đo với số mô hình.

**D (phân tích, ML)**
- Thời tiết đủ 2021–2025; PM2.5 mô hình chỉ từ 08/2022.
- Biến mục tiêu nên là số đo tại trạm. Nếu dùng số mô hình để lấp chỗ thiếu thì thêm cột đánh dấu.
- Dữ liệu 2026 hiện chỉ có ở OpenAQ; cần thời tiết 2026 thì báo B.

## 9. Các script và cách tự chạy thu thập

| File | Việc |
|---|---|
| `list_stations.py` | Tìm trạm AQICN còn hoạt động → `stations.json` |
| `producer.py` | Real-time: AQICN, OpenAQ, Open-Meteo → Kafka |
| `consumer_minio.py` | Real-time: Kafka → MinIO bucket `raw` |
| `history_openaq.py` | Lịch sử OpenAQ → MinIO bucket `history` |
| `history_openmeteo.py` | Lịch sử Open-Meteo cho 63 điểm → MinIO bucket `history` |
| `check_data.py` | Kiểm tra dữ liệu trong MinIO có đủ không |
| `stats_3v.py` | Thống kê dung lượng, số bản ghi, tốc độ |
| `sync_minio.py` | Chép dữ liệu giữa MinIO và thư mục trên máy |
| `replay.py` | Phát lại message đã lưu vào Kafka |
| `common.py` | Cấu hình và hàm dùng chung |
| `docker-compose.dev.yml` | Kafka + MinIO tối giản mà B dùng để thu thập trên máy riêng |

Chạy thu thập trên một máy khác, ví dụ máy demo:

```bash
python list_stations.py            # cần AQICN_TOKEN
python history_openaq.py find      # cần OPENAQ_KEY
python producer.py                 # terminal 1
python consumer_minio.py           # terminal 2
```

Không chạy producer trên hai máy cùng ghi vào một Kafka, vì dữ liệu sẽ bị nhân đôi.

Các script đọc `ingestion/.env` với tên biến riêng (`AQICN_TOKEN`, `OPENAQ_KEY`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`), khác với `.env.example` ở thư mục gốc (`AQICN_API_TOKEN`, `OPENAQ_API_KEY`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`). Hai file `.env` độc lập với nhau.

Thiếu gì hoặc thấy số liệu lạ thì báo B.

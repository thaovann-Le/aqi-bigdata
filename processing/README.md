# processing/ — Xử lý Spark (vai C)

Làm sạch, chuẩn hóa, ghép thời tiết, tổng hợp và sinh cảnh báo cho dữ liệu AQI Hà Nội và TP.HCM.
Streaming (Kafka) và batch (MinIO) dùng **chung một bộ hàm làm sạch** trong `common/`, nên hai luồng
cho cùng kết quả.

- Bảng, cột, đơn vị và cách đọc dữ liệu đầu ra (cho D và A): [`docs/data_contract.md`](../docs/data_contract.md)
- Chương báo cáo (thiết kế, quyết định, giới hạn): [`docs/processing.md`](../docs/processing.md)

## 1. Chạy thử toàn bộ pipeline bằng một lệnh

Yêu cầu: Docker + Docker Compose, cấp cho Docker **ít nhất 8 GB RAM**, có Internet ở lần chạy đầu
(tải image, jar Spark từ Maven Central, gói Python, plugin Grafana).

```bash
# ở thư mục gốc repo
docker compose down                     # tắt bộ docker-compose.yml chính nếu đang chạy (trùng cổng)

# (khuyên dùng) dữ liệu lịch sử THẬT của B: tải history-hn-hcm.zip (link trong ingestion/README.md)
mkdir -p data && unzip history-hn-hcm.zip -d data/      # phải ra data/history/openaq/... và data/history/openmeteo/...

docker compose -f docker-compose.demo.yml up -d
docker compose -f docker-compose.demo.yml logs -f seed job-history job-stream
```

Không có `data/history/` thì `seed` tự sinh dữ liệu lịch sử **giả** đúng định dạng (vài ngày mỗi năm)
để pipeline vẫn chạy được, và in cảnh báo trong log. Dữ liệu real-time luôn là dữ liệu **thật** của B
(`ingestion/results/raw-mau.zip`, 1.116 message). Có `raw-full.zip` thì giải nén vào `data/raw/`
(phải ra `data/raw/aqi_raw/...`, `data/raw/weather_raw/...`) rồi chạy lại `seed` với `FORCE_REPLAY=1`.

Thứ tự các service:

| Service | Việc | Khi nào xong |
|---|---|---|
| `kafka-init`, `cassandra-init` | tạo topic, schema | thoát với exit 0 |
| `seed` | nạp lịch sử vào MinIO `history`, real-time vào MinIO `raw` và phát lại vào Kafka | thoát với exit 0 |
| `job-history` | `batch_history` → `batch_aggregate` → `export_ml_dataset` | thoát với exit 0 (vài phút với dữ liệu thật) |
| `job-stream` | đọc Kafka liên tục, 30 giây một lô | chạy mãi |
| `job-aggregate` | mỗi 15 phút chạy lại tổng hợp + xuất dữ liệu cho D (để có cả số real-time) | chạy mãi |
| `grafana` | http://localhost:3000 (admin/admin) | — |

### Kiểm tra kết quả

```bash
docker compose -f docker-compose.demo.yml ps -a          # *-init, seed, job-history: Exited (0)
docker compose -f docker-compose.demo.yml exec cassandra cqlsh -e "
  SELECT count(*) FROM aqi.measurements;
  SELECT * FROM aqi.alerts LIMIT 5;
  SELECT period_start, label, avg_aqi, record_count FROM aqi.daily_agg
    WHERE city='hanoi' AND period_type='month' AND data_basis='measured' LIMIT 12;"
cat processing/_runtime/metrics.jsonl                    # số liệu hiệu năng cho báo cáo (mỗi job/lô một dòng)
ls processing/_runtime/ml_dataset/                       # bộ dữ liệu cho D (CSV + Parquet)
```

Giao diện: Spark http://localhost:8080, MinIO http://localhost:9001 (minioadmin / minioadmin123).

### Tùy chỉnh (đặt biến môi trường trước lệnh `up`, hoặc trong file `.env`)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `HISTORY_ARGS` | (trống) | tham số thêm cho batch_history, ví dụ `--years 2024,2025` |
| `ALERT_THRESHOLD` | `150` | ngưỡng cảnh báo AQI; hạ xuống (ví dụ `100`) khi demo mà trời sạch |
| `AGG_INTERVAL_SEC` | `900` | chu kỳ chạy lại tổng hợp |
| `STREAM_TRIGGER` | `30 seconds` | chu kỳ micro-batch streaming |
| `SPARK_MASTER` | `spark://spark-master:7077` | đặt `local[2]` nếu máy yếu (job chạy ngay trong container của nó) |
| `FORCE_REPLAY` | `0` | `1`: seed phát lại dữ liệu vào Kafka kể cả khi topic đã có dữ liệu |

Dừng: `docker compose -f docker-compose.demo.yml down` (giữ dữ liệu) hoặc thêm `-v` (xóa sạch, kể cả checkpoint streaming).

## 2. Chạy từng job bằng tay

`bin/submit.sh` gói sẵn mọi cấu hình (jar Kafka/Cassandra/S3A, múi giờ, MinIO). Thư mục `processing/`
được mount vào `/opt/processing` trong mọi container Spark.

```bash
C="docker compose -f docker-compose.demo.yml exec spark-master /opt/processing/bin/submit.sh"
$C batch/batch_history.py --years 2024,2025      # lịch sử -> measurements, weather_hourly, alerts
$C batch/batch_raw.py --date 2026-10-07          # nạp lại real-time từ MinIO `raw` (không cần Kafka)
$C batch/batch_aggregate.py                      # measurements -> daily_agg
$C batch/export_ml_dataset.py --out /opt/processing/_runtime/ml_dataset
$C streaming/stream_measurements.py --once --checkpoint /checkpoints/manual   # xử lý hết Kafka rồi dừng
```

Với `docker-compose.yml` chính của A (image Bitnami): bỏ `-f docker-compose.demo.yml`, thêm
`-e SPARK_HOME=/opt/bitnami/spark -e SPARK_DRIVER_HOST=spark-master -e SPARK_PKG_VERSION=<phiên bản Spark của image>`
vào lệnh `exec`, và với streaming thêm `--checkpoint /tmp/ck` (compose chính không có volume `/checkpoints`).
Lưu ý: tag `bitnami/*` có thể không còn tải được (đã chuyển sang `bitnamilegacy` từ 08/2025) — đó là lý do
có `docker-compose.demo.yml`.

Mọi job **chạy lại được** (idempotent): ghi theo khóa chính của Cassandra nên chạy lại chỉ ghi đè cùng giá trị.

## 3. Chạy test (không cần Docker)

```bash
cd processing
python -m venv .venv && source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt                      # pyspark 3.5, pytest, pandas; cần Java 11/17
python -m pytest tests -q                                # 38 test, khoảng 3-6 phút
```

Test chạy Spark cục bộ trên: dữ liệu real-time **thật** của B (`raw-mau.zip`), và dữ liệu lịch sử giả đúng
định dạng có cài sẵn các lỗi của dữ liệu thật (-999, giá trị 985, trạm đo 5 phút, đơn vị đếm hạt,
trạm ngoài thành phố, giờ không múi giờ). Chạy job không cần Docker: thêm `--master local[2] --sink parquet:/tmp/out`
và `--base <thư mục>` (xem `tests/test_streaming_and_scripts.py`).

## 4. Cấu trúc

```
processing/
├── bin/submit.sh                  spark-submit với đầy đủ cấu hình
├── common/
│   ├── config.py                  ngưỡng, khoảng hợp lệ, mùa, thang AQI (đổi bằng biến môi trường)
│   ├── aqi.py                     US AQI từ PM2.5/PM10 (biểu thức Spark + bản Python để đối chiếu)
│   ├── cleaning.py                lọc giá trị, khử trùng, chuẩn hóa cột, ghép thời tiết
│   ├── sources.py                 đọc từng nguồn: OpenAQ/Open-Meteo lịch sử, message real-time
│   ├── pipeline.py                lõi dùng chung streaming/batch: message -> measurements, weather, alerts
│   ├── aggregate.py               tổng hợp giờ/ngày/tháng/mùa
│   └── io.py                      SparkSession, sink (Cassandra/Parquet/console), bảng trạm
├── batch/
│   ├── batch_history.py           MinIO history -> measurements, weather_hourly, alerts
│   ├── batch_raw.py               MinIO raw -> (cùng logic streaming)
│   ├── batch_aggregate.py         measurements -> daily_agg
│   └── export_ml_dataset.py       -> CSV/Parquet theo thành phố-giờ cho D
├── streaming/stream_measurements.py   Kafka -> measurements, weather_hourly, alerts
├── reference/stations.csv         bảng trạm (sinh bởi tools/build_reference.py từ dữ liệu của B)
├── tools/                         build_reference.py, make_sample_history.py, seed_demo.py
├── examples/ml_data_access.py     mẫu cho D: đọc CSV/Cassandra bằng pandas, ghi forecasts
└── tests/                         pytest
```

Khi B cập nhật `ingestion/stations.json` hoặc `ingestion/openaq_locations.json`: chạy
`python processing/tools/build_reference.py` rồi commit `reference/stations.csv`.

## 5. Lỗi thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| `pull access denied` / `not found` khi `up` | image đã bị gỡ khỏi Docker Hub; đổi tag trong `docker-compose.demo.yml` (xem chú thích đầu file) |
| `job-*` lỗi `Unresolved dependencies` | không tải được jar từ Maven Central: kiểm tra Internet/proxy rồi chạy lại; jar được cache trong volume `ivy_cache` |
| `job-*` treo ở `Initial job has not accepted any resources` | worker hết core/RAM: giảm job chạy song song, hoặc tăng `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY`, hoặc đặt `SPARK_MASTER=local[2]` |
| Cassandra lỗi `Undefined column name pressure` / sai khóa chính | volume còn schema cũ: `docker compose ... exec cassandra cqlsh -e "DROP KEYSPACE aqi"` rồi `docker compose ... restart cassandra-init` |
| `measurements` có số nhưng `temperature` null ở vài giờ đầu | bình thường: thời tiết real-time bắt đầu muộn hơn số đo AQICN (dữ liệu trạm báo trễ) |
| Không có cảnh báo nào | AQI chưa vượt 150; hạ `ALERT_THRESHOLD` khi demo |
| Đổi code mà streaming không nhận | dừng `job-stream` và khởi động lại; đổi logic lớn thì xóa checkpoint (`down -v` hoặc đổi `--checkpoint`) |

# Chương: Kiến trúc hệ thống (vai A)

## 1. Sơ đồ kiến trúc tổng thể

```
AQICN / Open-Meteo API ──► Kafka (aqi_raw, weather_raw) ──► Spark Structured Streaming ──┐
                                      │                                                   ├──► Cassandra (measurements, daily_agg, alerts, forecasts) ──► Grafana
OpenAQ lịch sử / Open-Meteo Historical ──► MinIO (raw/, history/) ──► Spark batch ────────┘
                                                                            │
                                                                            └──► ml/ (XGBoost) ──► forecasts
```

Hai luồng dữ liệu song song, dùng chung một lõi xử lý:

- **Luồng real-time:** producer của B gọi API mỗi 10 phút → Kafka (`aqi_raw`, `weather_raw`) → Spark Structured
  Streaming đọc theo micro-batch 30 giây → ghi `measurements`/`weather_hourly`/`alerts`.
- **Luồng lịch sử:** dữ liệu OpenAQ/Open-Meteo B tải sẵn (nhiều năm, dạng file) → nạp vào MinIO bucket `history` →
  Spark batch đọc một lần, xử lý bằng **cùng các hàm** với luồng streaming (`processing/common/pipeline.py`) →
  ghi vào cùng các bảng Cassandra.

Cả hai luồng hội tụ ở Cassandra, nên Grafana và `ml/` không cần biết một con số đến từ real-time hay từ dữ liệu
nạp bù lịch sử. `batch_aggregate.py` chạy định kỳ (15 phút) tính lại `daily_agg` từ toàn bộ `measurements`,
và `ml/serving/predict.py` đọc `daily_agg` để dự báo, ghi lại `forecasts` — khép kín vòng dữ liệu → xử lý →
dự báo → hiển thị.

## 2. Lý do chọn công nghệ

> Quyết định dùng Kafka/MinIO/Cassandra làm ba lớp lưu trữ riêng (thô → đệm → đã xử lý) gắn liền với lựa
> chọn kiến trúc tích hợp dữ liệu **Warehousing** (thay vì Virtual Integration) — đối chiếu chi tiết với lý
> thuyết môn học: [`docs/data_integration_concepts.md`](data_integration_concepts.md) mục 2.

- **Kafka chế độ KRaft (không Zookeeper):** giảm một thành phần hạ tầng phải quản lý trên máy một node, trong khi
  vẫn giữ được đặc tính hàng đợi bền (producer ghi liên tục, consumer có thể dừng/khởi động lại mà không mất dữ
  liệu nhờ offset). Ghi thẳng vào Cassandra từ producer sẽ làm mất khả năng phát lại khi Spark lỗi hoặc cần đổi
  logic xử lý — đây là lý do giữ Kafka làm lớp đệm thay vì bỏ qua.
- **MinIO cho dữ liệu thô, Cassandra cho dữ liệu đã xử lý** (thay vì HDFS, hoặc Elasticsearch cho cả hai):
  dữ liệu thô (JSON/CSV theo lô, theo ngày) hợp với mô hình object storage hơn là một hệ file phân tán cần
  NameNode riêng; MinIO tương thích API S3 nên Spark đọc bằng cùng connector `s3a` dùng cho AWS thật, không
  cần học thêm API riêng. Cassandra được chọn cho dữ liệu đã xử lý vì mô hình truy vấn của dashboard và `ml/`
  đều là "theo khóa đã biết trước" (thành phố, trạm, khoảng thời gian) — đúng sở trường của Cassandra
  (wide-column, đọc theo partition key rất nhanh) — trong khi Elasticsearch phù hợp hơn cho tìm kiếm tự do,
  không phải nhu cầu ở đây.
- **Spark Structured Streaming thay vì chỉ xử lý theo lô:** dashboard cần số liệu gần thời gian thực (producer
  gọi API 10 phút một lần), nên cần một tiến trình luôn chạy đọc Kafka liên tục. Chọn Structured Streaming
  (thay vì Spark Streaming kiểu cũ dựa trên DStream) vì API DataFrame giống hệt batch, nên **một bộ hàm xử lý
  dùng chung được cho cả hai luồng** (`processing/common/`) — đã kiểm chứng bằng test
  `test_streaming_microbatches_match_batch` cho kết quả giống nhau trên dữ liệu thật.
- **Grafana cho dashboard:** có sẵn plugin đọc trực tiếp Cassandra
  (`hadesarchitect-cassandra-datasource`), không cần dựng thêm một lớp backend/API riêng chỉ để phục vụ biểu đồ.

## 3. Triển khai

Dùng `docker-compose.demo.yml` (không phải `docker-compose.yml` ban đầu — xem mục 4) để dựng toàn bộ hạ tầng
bằng một lệnh. Các service chính và phụ thuộc khởi động:

| Service | Image | Vai trò | Chờ gì trước khi chạy |
|---|---|---|---|
| `kafka` | `apache/kafka:3.9.0` | hàng đợi real-time (KRaft) | — |
| `kafka-init` | cùng image | tạo topic `aqi_raw`, `weather_raw` | `kafka` healthy |
| `minio` | `coollabsio/minio` | data lake (bucket `raw`, `history`) | — |
| `cassandra` | `cassandra:4.1` | lưu dữ liệu đã xử lý | — |
| `cassandra-init` | cùng image | áp dụng `infra/cassandra/schema.cql` | `cassandra` healthy |
| `spark-master` / `spark-worker` | `apache/spark:3.5.6` | cụm Spark standalone (1 worker, mặc định 2 core/2 GB) | — |
| `seed` | `python:3.12-slim` | nạp dữ liệu lịch sử + mẫu real-time vào MinIO/Kafka | `kafka`, `minio` |
| `job-history` | Spark image | `batch_history` → `batch_aggregate` → `export_ml_dataset` | `cassandra-init`, `seed` xong |
| `job-stream` | Spark image | đọc Kafka liên tục, ghi `measurements` | `job-history` xong |
| `job-aggregate` | Spark image | chạy lại `batch_aggregate` mỗi 15 phút | `job-history` xong |
| `grafana` | `grafana/grafana:10.4.2` | dashboard | `cassandra` healthy |

Các container `*-init` dùng `healthcheck` + `depends_on: condition: service_healthy/service_completed_successfully`
để đảm bảo thứ tự: tạo schema/topic trước khi job nào đọc/ghi. Cổng mở ra ngoài: Kafka `9094`, Spark UI `8080`/
`8081`, MinIO `9000`/`9001`, Cassandra `9042`, Grafana `3000`.

**Bài học triển khai thực tế:** cụm Spark chỉ có 2 core cho mọi job (`job-history`, `job-stream`, `job-aggregate`
chạy đồng thời). Khi nạp dữ liệu lịch sử thật (2,7 triệu dòng), ba job tranh tài nguyên khiến `job-history` bị
treo ("Initial job has not accepted any resources") và ghi Cassandra timeout. Cách xử lý: tạm dừng `job-stream`/
`job-aggregate` trong lúc nạp lịch sử lần đầu, bật lại sau — không cần tăng cấu hình nếu chỉ chạy demo một lần.

## 4. Dashboard Grafana

Dashboard "AQI Hà Nội & TP.HCM - Tổng quan" (`infra/grafana/dashboards/aqi-overview.json`) tự nạp khi Grafana
khởi động (provisioning), gồm 7 panel: bản đồ trạm, AQI hiện tại 2 thành phố, bảng 4 trạm AQICN Hà Nội, xu hướng
AQI theo giờ so sánh 2 thành phố, bảng cảnh báo, và dự báo so với thực tế.

**Lỗi plugin phát hiện khi chạy với dữ liệu thật:** `hadesarchitect-cassandra-datasource` trả sai thứ tự/trộn
dữ liệu cũ-mới khi chạy CQL 2 cột (`time` + 1 giá trị) với `ORDER BY ... DESC LIMIT N`, N lớn hơn khoảng 15 (đã
xác minh bằng cách so sánh với `cqlsh` chạy cùng câu lệnh — luôn đúng). Cách khắc phục áp dụng cho các panel
timeseries: thêm transform `merge`, đặt tên cột riêng cho từng chuỗi, và **giữ LIMIT nhỏ** (`ORDER BY DESC LIMIT
≤ 15`) hoặc dùng `ORDER BY ASC` với `WHERE` giới hạn khoảng thời gian (ASC không bị lỗi này ở bất kỳ số dòng
nào, đã kiểm chứng tới hàng trăm dòng). Chi tiết: `processing/README.md` mục "Lỗi thường gặp".

Panel dùng CQL cố định (`ORDER BY ... LIMIT N`), không dùng macro thời gian của plugin (`$__timeFrom()` test cho
kết quả không ổn định) — nghĩa là thanh chọn khung thời gian của Grafana không lọc lại các panel này; muốn đổi
khoảng xem phải sửa trực tiếp CQL trong từng target.

## 5. Giới hạn và hướng mở rộng

- **Single-node cho mọi thành phần:** 1 broker Kafka, 1 node Cassandra, 1 Spark worker — không có khả năng chịu
  lỗi hay mở rộng ngang. Phù hợp quy mô demo 1 tuần, không phù hợp vận hành thật.
- **Image gốc (`docker-compose.yml`) không còn tải được:** Bitnami chuyển phần lớn image miễn phí sang
  `bitnamilegacy` (08/2025), và image MinIO chính thức bị gỡ khỏi Docker Hub trong lúc làm project. Giải pháp
  tạm: `docker-compose.demo.yml` dùng image thay thế còn tải được (`apache/kafka`, `apache/spark`,
  `coollabsio/minio`, `cassandra:4.1`, `grafana/grafana`) — đây là compose file dùng để demo và chạy thật, không
  phải `docker-compose.yml` ban đầu.
- **Hướng mở rộng nếu có thêm thời gian:** nhiều broker Kafka, Spark cluster nhiều worker (cần tăng
  `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY` hoặc thêm worker để job lịch sử/real-time không tranh nhau core),
  Cassandra nhiều node (replication factor > 1, hiện đang chạy 1 node nên mọi dữ liệu chỉ có 1 bản sao).

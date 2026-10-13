# AQI Big Data — Giám sát và dự báo chất lượng không khí Hà Nội & TP.HCM

Hệ thống thu thập, xử lý và dự báo chất lượng không khí theo thời gian thực cho
Hà Nội và TP.HCM. Xem kế hoạch chi tiết (phân công, timeline, outcome) tại
[`docs/PLAN.md`](docs/PLAN.md).

## Kiến trúc

```
AQICN / Open-Meteo API ──► Kafka (aqi_raw, weather_raw) ──► Spark Structured Streaming ──┐
                                      │                                                   ├──► Cassandra (measurements, daily_agg, alerts, forecasts) ──► Grafana
OpenAQ lịch sử / Open-Meteo Historical ──► MinIO (raw/, history/) ──► Spark batch ────────┘
                                                                            │
                                                                            └──► ml/ (XGBoost) ──► forecasts
```

- **Kafka** (KRaft, không cần Zookeeper): hàng đợi dữ liệu thô real-time.
- **MinIO**: data lake cho dữ liệu thô (bucket `raw`) và dữ liệu lịch sử (bucket `history`).
- **Spark**: xử lý streaming (từ Kafka) và batch (từ MinIO), ghi vào Cassandra.
- **Cassandra**: lưu dữ liệu đã làm sạch/tổng hợp/cảnh báo/dự báo.
- **Grafana**: dashboard, đọc trực tiếp từ Cassandra qua plugin `hadesarchitect-cassandra-datasource`.

## Cấu trúc thư mục

```
infra/        docker-compose.yml, script khởi tạo Kafka/MinIO/Cassandra, provisioning Grafana
ingestion/    producer (gọi API -> Kafka), consumer (Kafka -> MinIO), historical (tải dữ liệu quá khứ)
processing/   job Spark streaming và batch
ml/           notebook khám phá dữ liệu, huấn luyện XGBoost, script dự báo định kỳ
docs/         báo cáo theo chương + kế hoạch gốc
```

## Chạy hệ thống

Yêu cầu: Docker + Docker Compose. Toàn bộ hạ tầng chạy bằng một file duy nhất.

```bash
cp .env.example .env     # chỉnh sửa nếu cần (API key, mật khẩu)
docker compose up -d
```

Các container `*-init` (`kafka-init`, `minio-init`, `cassandra-init`) chạy một lần để
tạo topic Kafka, bucket MinIO và schema Cassandra, sau đó tự thoát (exit code 0 là bình thường).

Kiểm tra trạng thái:

```bash
docker compose ps
```

### Các cổng dịch vụ

| Dịch vụ | URL | Ghi chú |
|---|---|---|
| Kafka (bên ngoài host) | `localhost:9094` | listener `EXTERNAL` |
| Spark master UI | http://localhost:8080 | |
| Spark worker UI | http://localhost:8081 | |
| MinIO API | http://localhost:9000 | |
| MinIO Console | http://localhost:9001 | đăng nhập bằng `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` trong `.env` |
| Cassandra | `localhost:9042` | |
| Grafana | http://localhost:3000 | đăng nhập bằng `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` |

### Dừng / dọn dẹp

```bash
docker compose down            # dừng, giữ lại volume (dữ liệu)
docker compose down -v         # dừng và xóa toàn bộ dữ liệu (Kafka/MinIO/Cassandra/Grafana)
```

### Chạy thử toàn bộ pipeline (hạ tầng + dữ liệu + job Spark)

```bash
docker compose -f docker-compose.demo.yml up -d
```

Một lệnh dựng hạ tầng, nạp dữ liệu mẫu (lịch sử vào MinIO, real-time vào Kafka) và chạy các job xử lý của C.
Dùng image còn tải được sau khi Bitnami và MinIO gỡ image khỏi Docker Hub. Chi tiết, cách kiểm tra kết quả và
xử lý lỗi: [`processing/README.md`](processing/README.md).

## Schema Cassandra

Schema nằm ở [`infra/cassandra/schema.cql`](infra/cassandra/schema.cql): bảng `measurements`,
`weather_hourly`, `daily_agg`, `alerts`, `forecasts`. Ý nghĩa từng cột, đơn vị và cách đọc (cho D và A):
[`docs/data_contract.md`](docs/data_contract.md). Sau khi chỉnh schema, áp dụng lại bằng:

```bash
docker compose restart cassandra-init
docker compose logs -f cassandra-init
```

`CREATE TABLE IF NOT EXISTS` không sửa bảng đã có: khi đổi khóa chính hoặc thêm cột, xóa keyspace cũ trước
(`docker compose exec cassandra cqlsh -e "DROP KEYSPACE aqi"`), hoặc `docker compose down -v`.

## Dashboard Grafana

Dashboard "AQI Hà Nội & TP.HCM - Tổng quan" tự nạp khi Grafana khởi động (provisioning), gồm 7 panel:
bản đồ vị trí trạm, AQI hiện tại mỗi thành phố (số lớn), bảng giá trị mới nhất 4 trạm AQICN Hà Nội, xu hướng
AQI theo giờ so sánh 2 thành phố, bảng cảnh báo, và dự báo so với thực tế. File gốc:
[`infra/grafana/dashboards/aqi-overview.json`](infra/grafana/dashboards/aqi-overview.json).

Các panel dùng CQL cố định dạng `ORDER BY ts DESC LIMIT N` (không dùng macro thời gian của plugin Cassandra
vì chưa xác minh được cú pháp macro trên phiên bản plugin cài qua `GF_INSTALL_PLUGINS`) — nghĩa là thanh chọn
khung thời gian của Grafana **không** lọc lại các panel này; muốn xem khoảng dài/ngắn hơn thì sửa `LIMIT`
trong từng target của panel (Edit panel → chọn target → sửa CQL).

Nếu panel nào hiện "No data" hoặc lỗi query sau khi Grafana mở lên lần đầu: mở panel đó → Edit → kiểm tra ô
CQL có hiển thị đúng không (field `query`/`target` trong JSON — tùy phiên bản plugin đọc field nào); nếu
trống, dán lại câu CQL trong mô tả panel (icon (i) ở góc) vào ô query. Panel "Dự báo so với thực tế" sẽ trống
cho tới khi `ml/serving` (vai D) chạy và ghi vào bảng `forecasts`.

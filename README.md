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

## Schema Cassandra

Schema khởi tạo (đề xuất ban đầu, cả nhóm thống nhất lại ở Ngày 1) nằm ở
[`infra/cassandra/schema.cql`](infra/cassandra/schema.cql): bảng `measurements`,
`daily_agg`, `alerts`, `forecasts`. Sau khi chỉnh schema, áp dụng lại bằng:

```bash
docker compose restart cassandra-init
docker compose logs -f cassandra-init
```

## Quy ước làm việc

- Mỗi vai (A/B/C/D) làm việc chủ yếu trong thư mục tương ứng (`infra/`, `ingestion/`,
  `processing/`, `ml/`) — xem phân công chi tiết trong `docs/PLAN.md`.
- Commit nhỏ, thường xuyên; họp nhanh mỗi tối để đồng bộ.
- Báo cáo từng chương nằm trong `docs/` (`architecture.md`, `data_3v.md`,
  `processing.md`, `analysis_ml.md`), ghép lại thành báo cáo cuối cùng ở Ngày 6.

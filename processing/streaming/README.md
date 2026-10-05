# processing/streaming (vai C)

Spark Structured Streaming job: đọc Kafka (`aqi_raw`, `weather_raw`) → làm sạch → ghi Cassandra.

## Việc cần làm

- [ ] Đọc 2 topic Kafka (bootstrap server `kafka:9092` trong mạng `aqi-net`).
- [ ] Loại bỏ bản ghi trùng/lỗi (thiếu trường bắt buộc, giá trị âm/vô lý).
- [ ] Chuẩn hóa đơn vị: µg/m³ ↔ chỉ số AQI (chọn một thang: US AQI hoặc VN AQI — ghi rõ
      lựa chọn này trong `docs/processing.md`, phải khớp với bảng `aqi_standard` trong schema).
- [ ] Chuyển múi giờ về UTC+7.
- [ ] Ghép (join) dữ liệu AQI với thời tiết theo giờ + thành phố.
- [ ] Ghi kết quả vào bảng Cassandra `measurements` (schema: `infra/cassandra/schema.cql`).

## Chạy job trên cụm Spark

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/processing/streaming/<ten_file>.py
```

(Thư mục `processing/` của repo được mount vào `/opt/processing` trong cả spark-master và spark-worker.)

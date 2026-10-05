# processing/batch (vai C)

Spark batch job: đọc dữ liệu lịch sử từ MinIO (`history/`) → cùng logic làm sạch của
streaming job → ghi vào cùng các bảng Cassandra. Ngoài ra tính tổng hợp và cảnh báo.

## Việc cần làm

- [ ] Đọc dữ liệu lịch sử OpenAQ + Open-Meteo từ MinIO bucket `history` (S3A: `s3a://history/...`,
      endpoint `http://minio:9000`).
- [ ] Áp dụng cùng logic làm sạch/chuẩn hóa như `processing/streaming`, ghi vào `measurements`.
- [ ] Tính tổng hợp theo giờ/ngày/tháng/mùa, so sánh Hà Nội vs TP.HCM, ghi vào `daily_agg`.
- [ ] Sinh cảnh báo khi AQI vượt ngưỡng (ví dụ > 150), ghi vào `alerts`.
- [ ] Đo thời gian xử lý và số bản ghi/phút — số liệu này đưa vào `docs/processing.md`.

## Chạy job

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/processing/batch/<ten_file>.py
```

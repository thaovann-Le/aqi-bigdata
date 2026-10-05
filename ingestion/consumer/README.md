# ingestion/consumer (vai B)

Consumer đọc từ Kafka (`aqi_raw`, `weather_raw`) và ghi dữ liệu thô vào MinIO bucket `raw`,
chia thư mục theo ngày, ví dụ:

```
raw/aqi/2026/10/05/<station_id>_<timestamp>.json
raw/weather/2026/10/05/<city>_<timestamp>.json
```

## Việc cần làm

- [ ] Kết nối MinIO qua S3 API (`http://minio:9000` trong mạng `aqi-net`, hoặc
      `http://localhost:9000` từ host), dùng `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` trong `.env`.
- [ ] Đọc liên tục từ Kafka, ghi từng message (hoặc batch nhỏ) thành object JSON trong `raw/`.
- [ ] Xử lý lỗi ghi/mất kết nối MinIO mà không làm rớt message (commit offset Kafka sau khi ghi thành công).

## Gợi ý thư viện

`boto3` hoặc `minio` (Python SDK chính thức của MinIO), `confluent-kafka`/`kafka-python`.

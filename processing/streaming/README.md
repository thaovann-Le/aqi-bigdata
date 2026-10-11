# processing/streaming (vai C)

`stream_measurements.py`: Spark Structured Streaming đọc Kafka (`aqi_raw`, `weather_raw`) → làm sạch → Cassandra
(`measurements`, `weather_hourly`, `alerts`). Mỗi micro-batch gọi `common/pipeline.process_raw`, đúng hàm mà
`batch/batch_raw.py` và test dùng.

```bash
/opt/processing/bin/submit.sh streaming/stream_measurements.py              # liên tục, 30 giây một lô
/opt/processing/bin/submit.sh streaming/stream_measurements.py --once       # xử lý hết dữ liệu hiện có rồi dừng
```

Chi tiết: [`processing/README.md`](../README.md).

- [x] Đọc 2 topic Kafka (`kafka:9092`), lưu offset ở checkpoint `/checkpoints/measurements`.
- [x] Bỏ message hỏng, bản ghi trùng, giá trị âm/vô lý, số đo OpenAQ cũ lẫn trong payload.
- [x] Chuẩn hóa đơn vị (µg/m³, m/s) và thang AQI (US AQI).
- [x] Giờ theo UTC+7, làm tròn xuống đầu giờ.
- [x] Ghép thời tiết theo (thành phố, giờ), kể cả khi thời tiết đến ở lô trước (đọc lại từ `weather_hourly`).
- [x] Ghi `measurements`, `weather_hourly`, `alerts`.

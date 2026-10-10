# processing/batch (vai C)

| Script | Đọc | Ghi |
|---|---|---|
| `batch_history.py` | MinIO `history/` (OpenAQ CSV.gz, Open-Meteo JSON) | `measurements`, `weather_hourly`, `alerts` |
| `batch_raw.py` | MinIO `raw/` (JSONL do consumer của B ghi) | như streaming |
| `batch_aggregate.py` | `measurements` | `daily_agg` (giờ/ngày/tháng/mùa) |
| `export_ml_dataset.py` | `measurements`, `weather_hourly` | CSV + Parquet theo thành phố-giờ cho D |

Cách chạy, tham số và kiểm tra kết quả: [`processing/README.md`](../README.md).

- [x] Đọc dữ liệu lịch sử OpenAQ + Open-Meteo từ MinIO bucket `history` (S3A).
- [x] Cùng logic làm sạch/chuẩn hóa như streaming (`common/`), ghi `measurements`.
- [x] Tổng hợp giờ/ngày/tháng/mùa, tách số đo và số mô hình, ghi `daily_agg`.
- [x] Cảnh báo AQI > ngưỡng (mặc định 150, US AQI), ghi `alerts`.
- [x] Đo thời gian và số bản ghi/phút: `processing/_runtime/metrics.jsonl`.

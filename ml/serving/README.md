# ml/serving (vai D)

> **Dữ liệu đầu vào (từ C):** đọc [`docs/data_contract.md`](../../docs/data_contract.md) mục 3. Bộ dữ liệu phẳng theo thành phố-giờ: `processing/_runtime/ml_dataset/` (sinh bởi `docker-compose.demo.yml`), hàm đọc mẫu: [`processing/examples/ml_data_access.py`](../../processing/examples/ml_data_access.py).

Script chạy định kỳ mỗi giờ: đọc dữ liệu mới nhất từ Cassandra (`measurements`), tính
đặc trưng, chạy mô hình đã huấn luyện (`ml/training`), ghi kết quả vào bảng `forecasts`.

## Việc cần làm

- [ ] Load model đã huấn luyện từ `ml/training`.
- [ ] Đọc N giờ dữ liệu gần nhất từ Cassandra cho mỗi thành phố.
- [ ] Tính đặc trưng giống lúc huấn luyện, dự báo AQI +24h.
- [ ] Ghi vào bảng `forecasts` (`city`, `forecast_for`, `generated_at`, `predicted_aqi`,
      `model_name`, `horizon_hours`).
- [ ] Lên lịch chạy mỗi giờ (cron, `schedule` trong Python, hoặc Airflow nếu còn thời gian —
      không bắt buộc, một cron job đơn giản là đủ cho bản 1 tuần).

# ml/notebooks (vai D)

> **Dữ liệu đầu vào (từ C):** đọc [`docs/data_contract.md`](../../docs/data_contract.md) mục 3. Bộ dữ liệu phẳng theo thành phố-giờ: `processing/_runtime/ml_dataset/` (sinh bởi `docker-compose.demo.yml`), hàm đọc mẫu: [`processing/examples/ml_data_access.py`](../../processing/examples/ml_data_access.py).

Notebook khám phá dữ liệu (EDA) và phân tích tương quan.

## Việc cần làm (Ngày 1–2)

- [ ] Đọc mẫu dữ liệu lịch sử đầu tiên do B tải (từ MinIO `history/` hoặc file mẫu cục bộ).
- [ ] Thống kê tỷ lệ dữ liệu thiếu, phân bố AQI/PM2.5, xu hướng theo giờ/ngày/mùa.
- [ ] Phân tích tương quan giữa PM2.5/AQI và các yếu tố thời tiết (gió, mưa, độ ẩm, nhiệt độ).
- [ ] Xuất biểu đồ dùng cho chương "Phân tích & Mô hình" trong `docs/analysis_ml.md`.

Đặt notebook `.ipynb` trong thư mục này (ví dụ `eda_hanoi.ipynb`, `eda_hcmc.ipynb`,
`correlation_weather.ipynb`).

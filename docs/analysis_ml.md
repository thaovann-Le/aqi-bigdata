# Chương: Phân tích & Mô hình (vai D)

> Khung sườn — điền nội dung trước Ngày 6.

## 1. Khám phá dữ liệu (EDA)

- Tỷ lệ dữ liệu thiếu theo trạm/thời gian.
- Phân bố AQI/PM2.5, xu hướng theo giờ/ngày/mùa, so sánh Hà Nội vs TP.HCM.

## 2. Tương quan với thời tiết

- Tương quan PM2.5/AQI với gió, mưa, độ ẩm, nhiệt độ (biểu đồ + hệ số tương quan).

## 3. Mô hình dự báo

- Đặc trưng đầu vào: giờ, ngày trong tuần, tháng, thời tiết, AQI lag 1/3/6/24h.
- Mô hình: XGBoost dự báo AQI +24h, mỗi thành phố.
- Baseline: "ngày mai giống hôm nay" (persistence).
- Kết quả: bảng so sánh MAE, RMSE giữa XGBoost và baseline, cho mỗi thành phố.

## 4. Ý nghĩa thực tế

- Mô hình có đủ tốt để cảnh báo sớm không?
- Hạn chế hiện tại (dữ liệu real-time ngắn, chưa có LSTM so sánh, v.v.) và hướng cải thiện.

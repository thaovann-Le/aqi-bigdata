# Chương: Xử lý dữ liệu (vai C)

> Khung sườn — điền nội dung trước Ngày 6.

## 1. Luồng streaming

- Sơ đồ: Kafka → Spark Structured Streaming → Cassandra (`measurements`).
- Logic làm sạch: loại bản ghi trùng/lỗi, chuẩn hóa đơn vị (nêu rõ thang AQI đã chọn:
  US AQI hay VN AQI), chuyển múi giờ UTC+7, ghép AQI với thời tiết.

## 2. Luồng batch

- Sơ đồ: MinIO (`history/`) → Spark batch → Cassandra (cùng bảng `measurements`,
  cộng `daily_agg`, `alerts`).
- Khác biệt so với streaming (nếu có): xử lý theo lô lớn, tổng hợp theo giờ/ngày/mùa.

## 3. Xử lý dữ liệu thiếu và không đồng nhất

- Cách xử lý trạm thiếu dữ liệu tại một thời điểm.
- Cách hòa giải khác biệt định dạng/đơn vị giữa AQICN, OpenAQ, Open-Meteo.

## 4. Cảnh báo

- Ngưỡng cảnh báo đã chọn (ví dụ AQI > 150) và căn cứ (thang VN_AQI hay US AQI).
- Logic sinh cảnh báo, ghi vào bảng `alerts`.

## 5. Hiệu năng

- Thời gian xử lý batch cho toàn bộ dữ liệu lịch sử.
- Số bản ghi/phút xử lý được (streaming).

# Chương: Dữ liệu & 3V (vai B)

> Khung sườn — điền nội dung trước Ngày 6.

## 1. Nguồn dữ liệu

| Nguồn | Loại dữ liệu | Tần suất | Định dạng gốc |
|---|---|---|---|
| AQICN | AQI, chất ô nhiễm theo trạm | Real-time (10–15 phút) | JSON |
| OpenAQ | AQI/chất ô nhiễm lịch sử | Lịch sử 2–3 năm | CSV/Parquet (S3 archive) |
| Open-Meteo | Thời tiết hiện tại + lịch sử | Real-time + lịch sử | JSON |
| NASA FIRMS (nếu còn thời gian) | Điểm cháy/đốt rơm rạ | Theo ngày | CSV |

## 2. Mô tả trường dữ liệu

(Liệt kê các trường chính: pm25, pm10, o3, no2, so2, co, aqi, temperature, humidity,
wind_speed, wind_direction, precipitation, timestamp, station_id, city...)

## 3. Chứng minh 3V

- **Volume:** tổng dung lượng (MB/GB), tổng số bản ghi, khoảng thời gian phủ.
- **Velocity:** tốc độ dữ liệu đến (bản ghi/phút từ producer real-time), độ trễ Kafka → Cassandra.
- **Variety:** số nguồn khác nhau, định dạng khác nhau (JSON, CSV, Parquet), cấu trúc schema khác nhau
  giữa các nguồn trước khi chuẩn hóa.

## 4. Thách thức khi thu thập

- Giới hạn số lượt gọi API (rate limit).
- Dữ liệu thiếu/trạm offline.
- Khác biệt đơn vị và thang đo AQI giữa các nguồn.

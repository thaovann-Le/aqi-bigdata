-- Schema của pipeline AQI (chủ sở hữu: C - Xử lý Spark).
-- Áp dụng bằng: docker compose restart cassandra-init  (CREATE ... IF NOT EXISTS, an toàn khi chạy lại).
-- LƯU Ý: CREATE IF NOT EXISTS KHÔNG sửa bảng đã tồn tại. Khi đổi khóa chính/cột, phải xóa bảng cũ:
--   docker compose exec cassandra cqlsh -e "DROP KEYSPACE aqi"   rồi chạy lại cassandra-init.
-- Quy ước thời gian: mọi cột `timestamp` là thời điểm tuyệt đối (Cassandra lưu UTC);
-- giờ/ngày/tháng/mùa luôn được cắt theo múi giờ Asia/Ho_Chi_Minh (UTC+7). Thang AQI: US AQI.

CREATE KEYSPACE IF NOT EXISTS aqi
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE aqi;

-- Số đo theo giờ của từng trạm (streaming + batch cùng ghi vào đây).
-- station_id có tiền tố nguồn: 'aqicn:1583' | 'openaq:2161292' | 'model:hanoi'
-- (mã AQICN và OpenAQ có thể trùng số nên phải có tiền tố).
-- is_model = true: số liệu mô hình CAMS của Open-Meteo, KHÔNG phải số đo tại trạm.
CREATE TABLE IF NOT EXISTS measurements (
  city            text,       -- 'hanoi' | 'hcm'
  station_id      text,
  ts              timestamp,  -- đầu giờ (làm tròn xuống giờ) của thời điểm đo
  pm25            double,     -- µg/m³ (null với AQICN: nguồn này chỉ cho chỉ số AQI con)
  pm10            double,     -- µg/m³
  o3              double,     -- µg/m³
  no2             double,     -- µg/m³
  so2             double,     -- µg/m³
  co              double,     -- µg/m³
  aqi             int,        -- US AQI (xem docs/processing.md về cách tính theo từng nguồn)
  aqi_standard    text,       -- luôn là 'US_AQI'
  temperature     double,     -- °C   (Open-Meteo, mức thành phố)
  humidity        double,     -- %
  wind_speed      double,     -- m/s  (đã đổi từ km/h)
  wind_direction  double,     -- độ
  precipitation   double,     -- mm
  pressure        double,     -- hPa
  source          text,       -- 'aqicn' | 'openaq' | 'openmeteo_aq'
  is_model        boolean,
  PRIMARY KEY ((city, station_id), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Thời tiết theo giờ của từng thành phố (Open-Meteo). measurements lấy cột thời tiết từ bảng này.
CREATE TABLE IF NOT EXISTS weather_hourly (
  city            text,
  ts              timestamp,
  temperature     double,
  humidity        double,
  wind_speed      double,     -- m/s
  wind_direction  double,
  precipitation   double,
  pressure        double,
  PRIMARY KEY ((city), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Tổng hợp theo thành phố. data_basis tách số đo trạm ('measured') khỏi số mô hình ('model'),
-- hai chuỗi này không bao giờ được trộn lẫn.
--   period_type: 'hour' | 'day' | 'month' | 'season'
--   hour  : trung bình các trạm trong giờ (mỗi trạm vật lý tính một lần)
--   day/month/season: trung bình các giờ-thành phố có dữ liệu trong kỳ
CREATE TABLE IF NOT EXISTS daily_agg (
  city          text,
  period_type   text,
  data_basis    text,       -- 'measured' | 'model'
  period_start  timestamp,  -- đầu kỳ theo giờ Việt Nam
  label         text,       -- tên mùa ('winter', 'dry'...) hoặc 'YYYY-MM' / 'YYYY-MM-DD'; null với 'hour'
  avg_aqi       double,
  max_aqi       double,
  min_aqi       double,
  avg_pm25      double,
  avg_pm10      double,
  record_count  int,        -- hour: số trạm; day/month/season: số giờ có dữ liệu
  PRIMARY KEY ((city, period_type, data_basis), period_start)
) WITH CLUSTERING ORDER BY (period_start DESC);

-- Cảnh báo khi AQI của một trạm đo vượt ngưỡng (mặc định > 150). Không sinh cảnh báo từ số mô hình.
CREATE TABLE IF NOT EXISTS alerts (
  city        text,
  ts          timestamp,
  station_id  text,
  aqi         int,
  level       text,   -- 'unhealthy' (151-200) | 'very_unhealthy' (201-300) | 'hazardous' (>300)
  message     text,
  PRIMARY KEY ((city), ts, station_id)
) WITH CLUSTERING ORDER BY (ts DESC, station_id ASC);

-- Dự báo của mô hình (D ghi vào hằng giờ).
CREATE TABLE IF NOT EXISTS forecasts (
  city            text,
  forecast_for    timestamp,
  generated_at    timestamp,
  predicted_aqi   double,
  predicted_pm25  double,
  model_name      text,   -- ví dụ 'xgboost_v1', 'baseline_persistence'
  horizon_hours   int,
  PRIMARY KEY ((city), forecast_for, generated_at)
) WITH CLUSTERING ORDER BY (forecast_for DESC, generated_at DESC);

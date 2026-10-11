# Chương: Xử lý dữ liệu (vai C)

> Bản nháp đã có thiết kế và các quyết định. Mục 6 (hiệu năng) cần điền số liệu thật sau khi chạy
> toàn bộ dữ liệu lịch sử: lấy từ `processing/_runtime/metrics.jsonl`.

## 1. Tổng quan

```
Kafka (aqi_raw, weather_raw) ──► Spark Structured Streaming ──┐
                                   (foreachBatch, 30 giây)      │   common/pipeline.process_raw
MinIO raw/ (JSONL) ───────────► Spark batch (batch_raw) ───────┤──► measurements, weather_hourly, alerts
MinIO history/ (CSV.gz, JSON) ► Spark batch (batch_history) ───┘                │
                                                                                 ▼
                                                      batch_aggregate ──► daily_agg (giờ/ngày/tháng/mùa)
                                                      export_ml_dataset ──► CSV/Parquet cho phần ML
```

Nguyên tắc thiết kế:

1. **Một bộ logic cho cả hai luồng.** Mọi phép làm sạch nằm trong `processing/common/` dưới dạng hàm
   DataFrame → DataFrame. Streaming gọi đúng hàm mà batch và test gọi, nên kết quả hai luồng giống nhau
   (test `test_streaming_microbatches_match_batch` chứng minh điều này trên dữ liệu thật).
2. **Ghi lại được (idempotent).** Mỗi dòng có khóa chính tự nhiên (thành phố, trạm, giờ). Cassandra ghi theo
   kiểu upsert nên chạy lại một job, phát lại Kafka hay nạp bù dữ liệu không làm nhân đôi bản ghi.
3. **Không dùng Python UDF.** Mọi phép tính (kể cả công thức AQI) là biểu thức Spark SQL chạy trên JVM,
   nhanh hơn và executor không cần cài Python.

## 2. Luồng streaming

`processing/streaming/stream_measurements.py`, mỗi micro-batch:

1. **Phân tích JSON** theo schema cố định của lớp vỏ do producer tạo (`source, city, station_id, fetched_at, payload`).
   Message hỏng (không phải JSON, thiếu `source`) bị bỏ.
2. **Tách theo nguồn**, mỗi nguồn một cách đọc payload:
   - AQICN: lấy `aqi` (US AQI tổng hợp) và thời điểm đo `time.iso`. Các số trong `iaqi` là **chỉ số AQI con**,
     không phải nồng độ, nên không đưa vào cột µg/m³.
   - OpenAQ: tách mảng `results`, chỉ giữ chất ô nhiễm có đơn vị khối lượng (bỏ đếm hạt, nhiệt độ, độ ẩm của
     cảm biến), **bỏ số đo cũ hơn 48 giờ** so với lúc gọi API (payload `latest` lẫn số đo từ nhiều tháng trước),
     rồi lấy trung bình theo giờ và xoay thành mỗi chất một cột.
   - Open-Meteo (thời tiết và chất lượng không khí mô hình): đọc `current`, gán giờ Việt Nam.
3. **Khử trùng.** Producer gọi API 10 phút một lần trong khi nguồn cập nhật theo giờ, nên cùng một số đo
   đến nhiều lần. Giữ bản mới nhất theo `fetched_at` cho mỗi (trạm, giờ) — với OpenAQ khử trùng ở mức từng
   số đo để không mất chất nào.
4. **Ghép thời tiết** theo (thành phố, giờ). Thời tiết của một giờ có thể đã đến ở lô trước, nên lô hiện tại
   đọc thêm `weather_hourly` trong Cassandra cho khoảng giờ đang xử lý (bản của lô hiện tại được ưu tiên).
5. **Ghi** `weather_hourly`, `measurements`, `alerts`.

Offset Kafka được lưu trong checkpoint, nên job dừng rồi chạy lại sẽ tiếp tục từ chỗ cũ.

## 3. Luồng batch

- `batch_history.py`: OpenAQ (CSV.gz dạng dài, mỗi dòng một số đo) được lọc, gộp về giờ và xoay thành dạng
  rộng; Open-Meteo (JSON dạng cột: mỗi biến là một mảng) được chuyển thành dạng dòng bằng `arrays_zip` + `explode`.
  Sau đó dùng chung bước ghép thời tiết, chuẩn hóa và cảnh báo với streaming.
- `batch_raw.py`: đọc lại JSONL trong bucket `raw` và gọi đúng hàm của streaming; dùng để nạp bù dữ liệu
  real-time hoặc chạy thử khi không có Kafka.
- `batch_aggregate.py`: tính `daily_agg` từ toàn bộ `measurements` (cả lịch sử lẫn real-time), chạy định kỳ.

Khác biệt chính so với streaming: dữ liệu lịch sử là bảng lớn đọc một lần, có thể lọc theo năm (`--years`);
không cần đọc lại thời tiết từ Cassandra vì cả hai nguồn nằm trong cùng một lần đọc.

## 4. Xử lý dữ liệu thiếu và không đồng nhất

| Vấn đề | Cách xử lý |
|---|---|
| `-999` của OpenAQ là "không có số đo" | Mọi giá trị ngoài khoảng hợp lệ thành null **trước** khi lấy trung bình, nên không kéo lệch giờ đó |
| Giá trị bất thường (985 lặp lại, PM2.5 > 4.000) | Khoảng hợp lệ trong `common/config.py`: PM2.5 0–500, PM10 0–1000, độ ẩm 0–100, áp suất 850–1100… |
| Tần suất đo khác nhau (đa số 1 giờ, 3 trạm 5 phút) | Gộp tất cả về trung bình theo giờ |
| Đơn vị khác nhau | Chất ô nhiễm giữ µg/m³ (chỉ nhận đơn vị khối lượng, kể cả hai cách viết `µ`/`μ`); gió đổi km/h → m/s |
| Thang AQI | Chọn **US AQI** vì AQICN và Open-Meteo đều dùng; OpenAQ chỉ có nồng độ nên tự tính (mục 5) |
| Múi giờ | OpenAQ có múi giờ; Open-Meteo là giờ địa phương không kèm múi giờ; producer dùng UTC. Phiên Spark đặt `Asia/Ho_Chi_Minh` nên mọi phép cắt giờ/ngày/tháng là theo giờ Việt Nam, còn lưu trữ là thời điểm tuyệt đối |
| Mã trạm trùng giữa AQICN và OpenAQ | Mã trạm có tiền tố nguồn (`aqicn:`, `openaq:`) |
| Một trạm có ở cả hai nguồn | Bảng trạm (`reference/stations.csv`) gán cùng `physical_id` cho trạm cách nhau ≤ 300 m; tổng hợp chỉ tính một lần |
| Số đo và số mô hình CAMS | Cờ `is_model`; tổng hợp tách hai chuỗi bằng `data_basis`; cảnh báo chỉ dùng số đo |
| Trạm thiếu dữ liệu tại một thời điểm | Không nội suy ở tầng xử lý: trung bình thành phố chỉ tính các trạm có số; `record_count` cho biết số trạm/số giờ có dữ liệu để người dùng biết kỳ nào thiếu. Lấp chỗ trống (nếu cần) do phần ML quyết định và đánh dấu |
| TP.HCM không có trạm đo 01–11/2024 | Chuỗi `measured` có khoảng trống; chuỗi `model` vẫn liên tục |

## 5. Cách tính AQI

US AQI từ nồng độ theo giờ, công thức tuyến tính theo khoảng của EPA; PM2.5 dùng bảng điểm gãy cập nhật
05/2024 (đổi về bản 2012 bằng `PM25_BREAKPOINTS=2012`). AQI = max(chỉ số con PM2.5, chỉ số con PM10).

Giới hạn cần nêu: EPA và các nguồn tính AQI từ trung bình 24 giờ hoặc NowCast, còn ở đây dùng trung bình
1 giờ, nên AQI tự tính dao động mạnh hơn số AQICN/Open-Meteo công bố. Không tính chỉ số con từ O3/NO2/SO2/CO
vì cần đổi đơn vị sang ppb/ppm và thời gian lấy trung bình khác nhau.

## 6. Cảnh báo

Ngưỡng: AQI > 150 (mức "Unhealthy" của US AQI), đổi bằng `ALERT_THRESHOLD`. Mỗi trạm-giờ vượt ngưỡng sinh
một dòng trong `alerts` với mức `unhealthy` (151–200), `very_unhealthy` (201–300) hoặc `hazardous` (> 300).
Chỉ dùng số đo tại trạm, không dùng số mô hình.

## 7. Hiệu năng

Mỗi job ghi một dòng JSON vào `processing/_runtime/metrics.jsonl` (số bản ghi, thời gian, bản ghi/phút;
streaming ghi theo từng micro-batch).

| Job | Dữ liệu | Số bản ghi ra | Thời gian | Bản ghi/phút |
|---|---|---|---|---|
| batch_history | (điền) | | | |
| batch_aggregate | | | | |
| streaming (trung bình mỗi lô) | | | | |

Môi trường đo: (điền cấu hình máy, số core/RAM cấp cho Spark worker).

## 8. Kiểm thử

`processing/tests/` (pytest, Spark cục bộ) kiểm tra trên dữ liệu real-time thật (`raw-mau.zip`) và dữ liệu
lịch sử giả đúng định dạng có cài các lỗi của dữ liệu thật: công thức AQI (Spark khớp bản Python), loại
-999/giá trị bất thường, gộp trạm 5 phút, bỏ đơn vị lạ, bỏ trạm ngoài thành phố, múi giờ, đổi đơn vị gió,
khử trùng, ghép thời tiết qua nhiều micro-batch, tổng hợp theo mùa vắt qua năm mới, chạy lại không nhân đôi.

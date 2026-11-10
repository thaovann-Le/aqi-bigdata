# Chương: Xử lý dữ liệu (vai C)

> Phần này là nơi áp dụng trực tiếp nhất kiến thức môn **Tích hợp Dữ liệu Lớn**: hợp nhất 3 nguồn không
> đồng nhất (Heterogeneity), ánh xạ về schema chung theo GAV, và gộp trạm trùng bằng Record Linkage/Entity
> Resolution. Đối chiếu chi tiết với slide môn học, có trích dẫn file/dòng code: xem
> [`docs/data_integration_concepts.md`](data_integration_concepts.md).

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
   DataFrame → DataFrame. Streaming gọi đúng hàm mà batch và test gọi; dữ liệu của một giờ đến qua nhiều
   micro-batch nên mỗi lô đọc lại phần đã đến ở lô trước (mục 2) — kết quả sau mọi lô bằng **đúng** kết quả chạy
   batch một lần, mọi cột (test `test_streaming_microbatches_match_batch` kiểm tra trên dữ liệu thật).
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
     không phải nồng độ, nên không đưa vào cột µg/m³. Số đo AQICN thường đến trễ nhiều giờ (trung vị ~16 giờ trên
     dữ liệu real-time đầy đủ) nên luôn ghi theo giờ đo, không theo giờ gọi API.
   - OpenAQ: tách mảng `results`, chỉ giữ chất ô nhiễm có đơn vị khối lượng (bỏ đếm hạt, nhiệt độ, độ ẩm của
     cảm biến), **bỏ số đo cũ hơn 48 giờ** so với lúc gọi API (payload `latest` lẫn số đo của cảm biến đã chết từ
     nhiều tháng trước) và số đo ở tương lai (> 60 phút sau lúc gọi, sai đồng hồ), rồi lấy trung bình theo giờ
     (mục 4: giờ kết thúc khoảng đo) và xoay thành mỗi chất một cột.
   - Open-Meteo thời tiết: mỗi số đọc `current` là 15 phút (biến tức thời lúc `current.time`, mưa là tổng 15 phút
     trước đó). Giờ T lấy số đọc mới nhất trong (T−1h, T] cho biến tức thời và **cộng mưa** của các số đọc trong
     giờ — đúng quy ước Open-Meteo hourly của dữ liệu lịch sử (trước đây lấy số đọc cuối giờ gán cho đầu giờ và
     dùng mưa 15 phút như mưa cả giờ, thấp hơn khoảng 4 lần so với dữ liệu huấn luyện).
   - Open-Meteo chất lượng không khí (mô hình CAMS): đọc `current` (theo giờ), `is_model = true`.
3. **Khử trùng.** Producer gọi API 10 phút một lần trong khi nguồn cập nhật theo giờ, nên cùng một số đo
   đến nhiều lần (thêm 114 dòng trùng y hệt do consumer ghi lại lô). Giữ bản mới nhất theo `fetched_at` — với
   OpenAQ khử trùng ở mức từng số đo để không mất chất nào.
4. **Đọc lại trạng thái của các lô trước** (`io.make_lookups`), vì một giờ dữ liệu đến qua nhiều lô:
   - `weather_readings`: số đọc thời tiết 15 phút đã đến → tính lại trọn giờ (mưa, số đọc lúc T);
   - `openaq_observations`: số đo OpenAQ đã đến → trung bình giờ của trạm đo 5 phút và AQI của trạm có cảm biến
     lệch giờ tính trên mọi số đo của giờ (trước đây mỗi lô tự tính từ phần của mình rồi ghi đè: trên 6 ngày dữ
     liệu thật, 276/580 dòng OpenAQ lệch so với batch, 20 dòng đổi trạng thái cảnh báo, có giờ AQI 174 bị đè
     thành 79);
   - `weather_hourly`: thời tiết của giờ mà số đo cần nhưng không có trong lô;
   - `measurements`: dòng đã ghi ở các giờ vừa đổi thời tiết → ghi lại cột thời tiết của chúng; AQI cũ → nếu AQI
     tính lại không còn vượt ngưỡng thì gỡ cảnh báo cũ (chỉ xóa đúng các dòng đó).
   Hai bảng trạng thái đầu chỉ dùng nội bộ và tự hết hạn sau 14 ngày (TTL).
5. **Ghi** `weather_readings`, `weather_hourly`, `openaq_observations`, `measurements`, `alerts`.

Offset Kafka được lưu trong checkpoint, nên job dừng rồi chạy lại sẽ tiếp tục từ chỗ cũ.

## 3. Luồng batch

- `batch_history.py`: OpenAQ (CSV.gz dạng dài, mỗi dòng một số đo) được lọc, gộp về giờ và xoay thành dạng
  rộng; Open-Meteo (JSON dạng cột: mỗi biến là một mảng) được chuyển thành dạng dòng bằng `arrays_zip` + `explode`.
  Sau đó dùng chung bước ghép thời tiết, chuẩn hóa và cảnh báo với streaming. Khi không lọc năm, job đọc thêm
  thư mục `backfill/` (tải bù 06–13/10/2026 của B: cùng cột nhưng không có thư mục `year=` nên trước đây không được
  đọc); mỗi đường dẫn đọc riêng rồi hợp lại vì khác cấu trúc thư mục. `--no-backfill` để bỏ.
- `batch_raw.py`: đọc lại JSONL trong bucket `raw` và gọi đúng hàm của streaming; dùng để nạp bù dữ liệu
  real-time hoặc chạy thử khi không có Kafka.
- `batch_aggregate.py`: tính `daily_agg` từ toàn bộ `measurements` (cả lịch sử lẫn real-time), chạy định kỳ.

Khác biệt chính so với streaming: dữ liệu lịch sử là bảng lớn đọc một lần, có thể lọc theo năm (`--years`);
không cần đọc lại thời tiết từ Cassandra vì cả hai nguồn nằm trong cùng một lần đọc.

## 4. Xử lý dữ liệu thiếu và không đồng nhất

| Vấn đề | Cách xử lý |
|---|---|
| `-999` của OpenAQ là "không có số đo" | Mọi giá trị ngoài khoảng hợp lệ thành null **trước** khi lấy trung bình, nên không kéo lệch giờ đó |
| Giá trị bất thường (985 lặp lại, PM2.5 > 4.000, o3 âm) | Khoảng hợp lệ trong `common/config.py` (kiểm tra "có thể xảy ra về vật lý", có ghi chú số liệu đối chiếu): PM2.5 0–500, PM10 0–1000, O3 0–800, NO2/SO2 0–2000, CO 0–50.000 µg/m³; nhiệt độ −10–50 °C, độ ẩm 0–100 %, gió 0–60 m/s, mưa 0–300 mm/giờ, áp suất bề mặt 500–1100 hPa |
| Áp suất bề mặt phụ thuộc độ cao | Khoảng cũ 850–1100 loại nhầm áp suất thật của Đà Lạt (~1.500 m, 840–855 hPa): 7.655/43.824 giờ. Hạ cận dưới xuống 500 hPa |
| Tần suất đo khác nhau (đa số 1 giờ, 3 trạm 5 phút) | Gộp tất cả về trung bình theo giờ |
| Mốc giờ của số liệu theo giờ | **Giờ kết thúc khoảng đo**: giờ T gồm số đo trong (T−1h, T]; số đo đúng đầu giờ giữ nguyên mốc, mẫu lẻ phút thuộc giờ kế tiếp (08:05 → 09:00). Đây là quy ước của OpenAQ (trung bình giờ OpenAQ tính cho trạm 5 phút trong `backfill.zip` khớp trung bình các mẫu trong (T−1h, T], sai số trung vị 0,5–1,3 µg/m³; khớp [T, T+1h) thì lệch 5–15) và của Open-Meteo hourly. Trước đây mẫu lẻ phút bị cắt về đầu giờ nên trạm 5 phút lệch 1 giờ so với trạm đo theo giờ |
| Số đo ở tương lai | Bỏ số đo có thời điểm đo sau lúc gọi API hơn `MAX_FUTURE_MINUTES` (60 phút); bộ lọc 48 giờ trước đây chỉ chặn số đo cũ |
| CAMS không có `us_aqi` | Giữ null (1.953 giờ lịch sử), không thay bằng AQI tự tính theo giờ — khác phương pháp 24 giờ của CAMS; chỉ OpenAQ (`AQI_FROM_CONCENTRATION`) được tự tính |
| Đơn vị khác nhau | Chất ô nhiễm giữ µg/m³ (chỉ nhận đơn vị khối lượng, kể cả hai cách viết `µ`/`μ`); gió đổi km/h → m/s |
| Thang AQI | Chọn **US AQI** vì AQICN và Open-Meteo đều dùng; OpenAQ chỉ có nồng độ nên tự tính (mục 5) |
| Múi giờ | OpenAQ có múi giờ; Open-Meteo là giờ địa phương không kèm múi giờ; producer dùng UTC. Phiên Spark đặt `Asia/Ho_Chi_Minh` nên mọi phép cắt giờ/ngày/tháng là theo giờ Việt Nam, còn lưu trữ là thời điểm tuyệt đối |
| Dữ liệu tải bù (`backfill.zip`) | Đọc cùng lịch sử (mục 3); trùng khóa với dữ liệu real-time nên upsert ghi đè bằng số theo giờ đầy đủ hơn |
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
Chỉ dùng số đo tại trạm, không dùng số mô hình. Streaming: nếu một (trạm, giờ) đã có cảnh báo từ phần số đo đến
trước nhưng AQI tính lại với đủ số đo không còn vượt ngưỡng, cảnh báo đó được gỡ (`io.cassandra_deleter`; lỗi
khi xóa chỉ ghi log, không dừng streaming).

## 7. Hiệu năng

Mỗi job ghi một dòng JSON vào `processing/_runtime/metrics.jsonl` (số bản ghi, thời gian, bản ghi/phút;
streaming ghi theo từng micro-batch). Số liệu dưới đây đo khi chạy `batch_history` trên **toàn bộ dữ liệu
lịch sử thật** (63 tỉnh/thành, 2021–2026, qua `docker-compose.demo.yml`):

| Job | Dữ liệu | Số bản ghi ra | Thời gian | Bản ghi/phút |
|---|---|---:|---:|---:|
| batch_history | Toàn bộ lịch sử (63 tỉnh/thành) | 2.102.033 measurements + 2.761.266 weather_hourly | 480 giây | ~262.700 |
| batch_aggregate | Toàn bộ `measurements` → `daily_agg` (giờ/ngày/tháng/mùa) | 1.950.663 + 81.479 + 2.700 + 57 = 2.034.899 | 230 giây | ~530.800 |
| streaming (1 micro-batch, dữ liệu real-time mẫu) | 1.116 message Kafka | 114 dòng measurements, 33 alerts | 94 giây | ~714 message/phút |

Môi trường đo: Spark standalone 1 worker, `SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=2g` (cấu hình mặc định
của `docker-compose.demo.yml`). Ở cấu hình này, chạy đồng thời `job-history` + `job-stream` + `job-aggregate`
làm batch_history bị nghẽn tài nguyên (worker chỉ có 2 core cho cả 3 job) — số liệu trên đo khi tạm dừng
`job-stream`/`job-aggregate` trong lúc batch_history chạy (xem `docs/architecture.md` mục 3 "Bài học triển khai
thực tế").

## 8. Kiểm thử

`processing/tests/` (pytest, Spark cục bộ) kiểm tra trên dữ liệu real-time thật (`raw-mau.zip`) và dữ liệu
lịch sử giả đúng định dạng có cài các lỗi của dữ liệu thật: công thức AQI (Spark khớp bản Python), loại
-999/giá trị bất thường, gộp trạm 5 phút theo giờ kết thúc, bỏ đơn vị lạ, bỏ trạm ngoài thành phố, múi giờ,
đổi đơn vị gió, thời tiết theo giờ đúng quy ước Open-Meteo (mưa cộng dồn 15 phút), số đo ở tương lai, AQI mô
hình để trống, đọc backfill, áp suất vùng cao, khử trùng, tổng hợp theo mùa vắt qua năm mới, chạy lại không nhân
đôi; streaming chia 4 lô cho kết quả bằng đúng batch (mọi cột) và gỡ cảnh báo khi đủ số đo.

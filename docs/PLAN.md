# Kế hoạch 1 tuần: Giám sát và dự báo chất lượng không khí Hà Nội & TP.HCM

Nhóm 4 người, 7 ngày. Vì thời gian ngắn, phạm vi được thu gọn so với bản mô tả đầy đủ: ưu tiên một hệ thống chạy được từ đầu đến cuối, các phần nâng cao để ở mục "nếu còn thời gian".

## 1. Phạm vi đã thu gọn

| Thành phần | Bản đầy đủ | Bản 1 tuần |
|---|---|---|
| Nguồn dữ liệu | AQICN, OpenAQ, Open-Meteo, NASA FIRMS | AQICN + OpenAQ (lịch sử) + Open-Meteo. FIRMS để sau |
| Lưu trữ | HDFS/MinIO + Cassandra hoặc Elasticsearch | MinIO (dữ liệu thô) + Cassandra (dữ liệu đã xử lý). Bỏ HDFS |
| Xử lý | Spark batch + streaming | Spark Structured Streaming đọc từ Kafka, Spark batch cho dữ liệu lịch sử |
| ML | XGBoost, LSTM | Chỉ XGBoost dự báo PM2.5/AQI 24 giờ tới. LSTM để sau |
| Dashboard | Grafana | Grafana (giữ nguyên) |
| Triển khai | — | Toàn bộ bằng một file `docker-compose.yml` |

**Lưu ý về Volume:** trong 1 tuần, dữ liệu real-time thu được chỉ vài chục MB. Phần Volume phải dựa vào dữ liệu lịch sử OpenAQ (2–3 năm, nhiều trạm) và Open-Meteo. Nên mở rộng thêm các tỉnh/thành khác hoặc vài nước lân cận nếu cần tăng dung lượng.

## 2. Phân công

Mỗi người phụ trách một khâu kỹ thuật, một chương báo cáo và một phần trình bày, để khối lượng gần bằng nhau. Việc tổng hợp cũng được chia: B ghép báo cáo, A ghép slide.

| | A: Hạ tầng & Dashboard | B: Thu thập dữ liệu | C: Xử lý Spark | D: Phân tích & ML |
|---|---|---|---|---|
| Kỹ thuật | Docker Compose, Kafka topic, Grafana | Producer real-time, tải dữ liệu lịch sử, lưu MinIO | Làm sạch, kết hợp dữ liệu, tổng hợp, cảnh báo | Khám phá dữ liệu, tương quan, mô hình dự báo |
| Báo cáo | Chương Kiến trúc hệ thống | Chương Dữ liệu & 3V. **Ghép báo cáo** | Chương Xử lý dữ liệu | Chương Phân tích & Mô hình |
| Slide/demo | Phần kiến trúc + demo trực tiếp. **Ghép slide**, quay video dự phòng | Phần dữ liệu | Phần xử lý, cảnh báo | Phần kết quả phân tích, dự báo |

### A: Hạ tầng & Dashboard
1. Viết `docker-compose.yml` gồm: Kafka (chế độ KRaft, không cần Zookeeper), Spark master + 1 worker, MinIO, Cassandra, Grafana. Chạy ổn trên máy của cả nhóm.
2. Tạo Kafka topic `aqi_raw`, `weather_raw`; tạo bucket MinIO `raw`, `history`.
3. Tạo repo GitHub chung, cấu trúc thư mục (`infra/`, `ingestion/`, `processing/`, `ml/`, `docs/`), hướng dẫn cả nhóm cách chạy.
4. Cài plugin Cassandra cho Grafana, kết nối dữ liệu.
5. Dựng dashboard: bản đồ AQI theo trạm, biểu đồ AQI theo thời gian, so sánh 2 thành phố, bảng cảnh báo, dự báo so với thực tế (lấy bảng từ C và D).
6. Chạy lại toàn bộ hệ thống từ đầu, viết README.
7. Báo cáo: chương Kiến trúc hệ thống (sơ đồ, lý do chọn công nghệ). Ghép slide, quay video demo dự phòng.

### B: Thu thập dữ liệu
1. Đăng ký API key AQICN, OpenAQ; chốt danh sách trạm ở Hà Nội và TP.HCM.
2. Viết producer Python gọi AQICN và Open-Meteo mỗi 10–15 phút, đẩy JSON vào Kafka. Có retry khi lỗi, ghi log.
3. Viết consumer ghi dữ liệu thô từ Kafka vào MinIO, chia thư mục theo ngày.
4. **Bật producer chạy liên tục từ ngày 2** trên một máy luôn mở (hoặc VPS miễn phí).
5. Tải dữ liệu lịch sử 2–3 năm: OpenAQ (kho lưu trữ trên AWS) và Open-Meteo Historical, lưu vào MinIO `history`.
6. Thống kê chứng minh 3V: tổng dung lượng, số bản ghi, tốc độ dữ liệu đến, các định dạng.
7. Nếu còn thời gian: thêm nguồn NASA FIRMS.
8. Báo cáo: chương Dữ liệu & 3V (nguồn, mô tả trường, thách thức). Ghép báo cáo hoàn chỉnh.

### C: Xử lý Spark
1. Ngày 1, thống nhất schema với cả nhóm và tạo các bảng Cassandra: `measurements` (dữ liệu sạch theo giờ), `daily_agg` (tổng hợp), `alerts` (cảnh báo), `forecasts` (dự báo, D ghi vào).
2. Spark Structured Streaming đọc Kafka: bỏ bản ghi trùng/lỗi, chuẩn hóa đơn vị (µg/m³ ↔ AQI), chuyển múi giờ về UTC+7, ghép AQI với thời tiết theo giờ và thành phố, ghi `measurements`.
3. Spark batch xử lý dữ liệu lịch sử từ MinIO theo cùng logic, ghi cùng bảng.
4. Tính tổng hợp theo giờ/ngày/mùa và so sánh 2 thành phố, ghi `daily_agg`.
5. Sinh cảnh báo khi AQI vượt ngưỡng (ví dụ > 150, theo thang của VN_AQI hoặc US AQI, chọn một và ghi rõ), ghi `alerts`.
6. Đo thời gian xử lý, số bản ghi/phút để đưa vào báo cáo.
7. Báo cáo: chương Xử lý dữ liệu (luồng streaming và batch, cách xử lý dữ liệu thiếu và không đồng nhất).

### D: Phân tích & ML
1. Ngày 1–2, dùng mẫu dữ liệu lịch sử đầu tiên do B tải để khám phá dữ liệu trên notebook: tỷ lệ thiếu, phân bố, xu hướng theo giờ/ngày/mùa.
2. Phân tích tương quan giữa PM2.5/AQI và thời tiết (gió, mưa, độ ẩm, nhiệt độ), vẽ biểu đồ cho báo cáo.
3. Tạo đặc trưng: giờ, ngày trong tuần, tháng, thời tiết, AQI của 1/3/6/24 giờ trước.
4. Huấn luyện XGBoost dự báo AQI 24 giờ tới cho mỗi thành phố; so sánh với mô hình cơ sở "ngày mai giống hôm nay"; đánh giá bằng MAE, RMSE.
5. Viết script chạy định kỳ mỗi giờ: đọc dữ liệu mới từ Cassandra, dự báo, ghi bảng `forecasts`.
6. Nếu còn thời gian: thử LSTM để so sánh.
7. Báo cáo: chương Phân tích & Mô hình (kết quả khám phá, tương quan, đánh giá mô hình, ý nghĩa thực tế).

## 3. Timeline theo ngày

| Ngày | A | B | C | D |
|---|---|---|---|---|
| **1** | Docker Compose chạy đủ các dịch vụ; tạo repo | API key, chốt trạm, gọi thử API, lưu JSON mẫu | Chốt schema với nhóm, tạo bảng Cassandra | Bắt đầu khám phá mẫu dữ liệu lịch sử |
| **2** | Tạo Kafka topic, bucket MinIO; hỗ trợ B kết nối | Producer + consumer chạy liên tục; bắt đầu tải dữ liệu lịch sử | Spark Streaming đọc được Kafka | Khám phá dữ liệu, phân tích tương quan |
| **3** | Kết nối Grafana với Cassandra, panel đầu tiên | Hoàn tất dữ liệu lịch sử; theo dõi producer | Làm sạch, ghép dữ liệu, ghi `measurements` (streaming + batch) | Tạo đặc trưng, huấn luyện XGBoost bản đầu |
| **4** | Dashboard: bản đồ, biểu đồ theo thời gian | Thống kê 3V; viết chương Dữ liệu | Tổng hợp `daily_agg`, cảnh báo `alerts` | Tinh chỉnh, đánh giá mô hình; script ghi `forecasts` |
| **5** | Dashboard: cảnh báo, so sánh dự báo/thực tế | (Tùy chọn) FIRMS; dựng khung báo cáo chung | Đo hiệu năng; viết chương Xử lý | Viết chương Phân tích & Mô hình |
| **6** | Chạy lại toàn bộ từ đầu, README. **Đóng băng code** | Ghép báo cáo hoàn chỉnh | Sửa lỗi, rà soát báo cáo | Sửa lỗi, rà soát báo cáo |
| **7** | Ghép slide, quay video demo | Nộp báo cáo | Tập trình bày phần của mình | Tập trình bày phần của mình |

**Điểm phối hợp:** họp nhanh 15 phút mỗi tối. Sau ngày 3, nếu một khâu xong sớm, người đó hỗ trợ khâu đang chậm nhất.

## 4. Outcome cần đạt

**Bắt buộc:**
1. Hệ thống chạy bằng Docker Compose: API → Kafka → Spark → MinIO + Cassandra → Grafana.
2. Producer thu thập dữ liệu real-time liên tục ít nhất 5 ngày.
3. Dữ liệu lịch sử 2–3 năm đã xử lý và lưu trữ, kèm số liệu dung lượng và số bản ghi để chứng minh 3V.
4. Phân tích thống kê: AQI theo giờ/ngày/mùa, so sánh Hà Nội và TP.HCM, tương quan với thời tiết.
5. Cảnh báo khi AQI vượt ngưỡng.
6. Mô hình XGBoost dự báo AQI 24 giờ tới, có chỉ số đánh giá.
7. Dashboard Grafana với ít nhất 4 biểu đồ: bản đồ trạm, xu hướng theo thời gian, cảnh báo, dự báo so với thực tế.
8. Báo cáo, slide, README và video demo dự phòng; mỗi thành viên trình bày phần mình phụ trách.

**Nếu còn thời gian:**
- NASA FIRMS và phân tích ảnh hưởng của đốt rơm rạ.
- So sánh XGBoost với LSTM.
- Mở rộng thêm tỉnh/thành để tăng Volume.

## 5. Rủi ro và cách xử lý

| Rủi ro | Cách xử lý |
|---|---|
| Dựng Docker/Kafka/Spark mất nhiều thời gian | A làm ngay ngày 1, dùng image có sẵn (bitnami, apache). Nếu máy yếu, giảm số node, chạy Spark ở chế độ local |
| API giới hạn số lượt gọi hoặc lỗi | Lưu dữ liệu thô vào MinIO trước khi xử lý; có retry trong producer |
| Dữ liệu real-time quá ít | Dựa vào dữ liệu lịch sử cho phân tích và huấn luyện mô hình |
| Một khâu chậm làm cả nhóm chờ | Thống nhất schema từ ngày 1; mỗi người làm việc với file dữ liệu mẫu trong lúc chờ |

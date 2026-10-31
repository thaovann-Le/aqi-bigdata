# Kiến thức tích hợp dữ liệu áp dụng trong project

> Đối chiếu nội dung slide môn **Big Data Integration & Processing** (Vu Tuyet Trinh, SoICT - HUST,
> thư mục `Slide/`) với thiết kế và code thật của project. Mục đích: chỉ rõ project đã áp dụng
> những khái niệm nào của môn, ở đâu trong code, để đưa vào báo cáo/slide thay vì chỉ liệt kê
> công nghệ (Kafka/Spark/Cassandra) mà không gắn với lý thuyết tích hợp dữ liệu đã học.

Project tích hợp 3 nguồn không đồng nhất (AQICN, OpenAQ, Open-Meteo) thành một schema chung
trong Cassandra, nên đúng là một bài toán **data integration** theo nghĩa slide 1 định nghĩa:
"tie together different sources, controlled by many people, under a common schema".

## 1. Heterogeneity (Slide `1. Introduction.pdf`, mục "Levels of heterogeneity")

Slide chia 6 mức không đồng nhất. Project gặp và xử lý hầu hết các mức này:

| Mức (theo slide) | Biểu hiện trong project | Xử lý ở đâu |
|---|---|---|
| System/platform | AQICN/OpenAQ qua REST API JSON, Open-Meteo JSON, dữ liệu lịch sử CSV.gz, lưu trữ qua Kafka (streaming) + MinIO (file) + Cassandra (mediated) | `ingestion/producer.py`, `processing/common/io.py` |
| Syntactic/format | JSON lồng nhau (AQICN `iaqi`, OpenAQ `results[]`), CSV.gz dạng dài, JSON dạng cột (Open-Meteo lịch sử: mỗi biến một mảng) | `processing/common/sources.py`, xử lý bằng `arrays_zip`+`explode` (xem `docs/processing.md` mục 3) |
| Structural/schema | Mỗi nguồn có "bảng" khác nhau: AQICN cho sẵn `aqi`; OpenAQ cho nồng độ từng chất theo `sensorsId`; Open-Meteo cho `current.{biến}` | `processing/common/sources.py` — mỗi nguồn một hàm trích xuất riêng, gộp về cùng tập cột |
| Semantic/conceptual | `iaqi` của AQICN là **chỉ số con**, không phải nồng độ µg/m³ — nếu nhầm là cùng loại dữ liệu với OpenAQ sẽ sai hoàn toàn | Phân biệt rõ trong `docs/processing.md` mục 2 (gạch đầu dòng AQICN) |
| Instance/identity | Cùng một trạm vật lý xuất hiện ở cả AQICN và OpenAQ với 2 mã khác nhau | → xem mục 4 (Record Linkage) bên dưới |
| Quality/governance | Số đo tại trạm (`is_model=false`) vs số mô hình CAMS (`is_model=true`) — không được trộn lẫn khi tính trung bình hay cảnh báo | Cờ `is_model`, cột `data_basis` trong `daily_agg` (`infra/cassandra/schema.cql`) |

## 2. Kiến trúc: Warehousing thay vì Virtual Integration (Slide `1. Introduction.pdf`, "Virtual, Warehousing and in Between")

Slide nêu 2 kiến trúc tích hợp chính:
- **Virtual**: dữ liệu ở lại nguồn, wrapper dịch truy vấn sang từng nguồn tại thời điểm hỏi (mediator +
  query reformulation + query optimizer, xem slide "Virtual Data Integration Architecture").
- **Warehousing**: ETL định kỳ, nạp toàn bộ dữ liệu đã làm sạch vào một kho vật lý, tách rời nguồn và
  kho (slide "Data Warehouses – Offline Replication").

Project chọn **warehousing** một cách có chủ đích: Spark batch/streaming đọc từ Kafka/MinIO, làm sạch,
rồi ghi (ETL) vào Cassandra — không có bước nào gọi lại API nguồn tại thời điểm người dùng truy vấn
dashboard. Đây khớp với các *Pros* của warehousing trong slide: "Can run very heavy-duty computations,
including data mining and cleaning" (biện minh cho việc dùng Spark để làm sạch/tổng hợp) và "Queries
over the warehouse don't disrupt the data sources" (Grafana truy vấn Cassandra, không gọi lại AQICN/
OpenAQ mỗi lần xem dashboard — quan trọng vì các API này có giới hạn lượt gọi).

*Lưu ý trung thực:* project **không** triển khai phần "Query Processing" ở dạng virtual integration thật
sự (không có mediator reformulate truy vấn SQL thành truy vấn con cho từng wrapper tại runtime, không có
query optimizer chọn kế hoạch vật lý) — vì kiến trúc warehousing không cần bước đó. Phần "sinh mediation
query" (slide `2_mediationquery.pdf`) không áp dụng trực tiếp vì project không tích hợp dữ liệu bán cấu
trúc XML.

## 3. Schema Alignment — GAV (Slide `2_SchemaAlignment.pdf`)

Slide định nghĩa 3 cách biểu diễn ánh xạ: **GAV** (mediated schema = view trên nguồn), **LAV** (nguồn =
view trên mediated schema), **GLAV** (kết hợp cả hai).

Project dùng **GAV**: mediated schema là các bảng Cassandra (`measurements`, `weather_hourly`), và với
mỗi nguồn có một hàm biến đổi (trong `processing/common/sources.py` + `pipeline.py`) đóng vai trò chính
là một *GAV view definition* — tương đương biểu thức `Gi(X) ⊇ Q(S)` trong slide. Viết lại theo ký hiệu
của slide (rút gọn, bỏ phép làm sạch chi tiết):

```
measurements(city, station_id, ts, aqi, pm25, …) ⊇
    π city,station_id,ts,aqi (AQICN.feed)                         -- nguồn 1: AQICN

measurements(city, station_id, ts, pm25, pm10, …, aqi) ⊇
    AQI_from_PM(π city,station_id,ts,parameter,value (OpenAQ.results))  -- nguồn 2: OpenAQ,
                                                                          -- kèm transformation (mục 5)

measurements(city, "model:"+city, ts, pm25, …, is_model=true) ⊇
    π city,ts,current.* (OpenMeteo.air_quality)                   -- nguồn 3: Open-Meteo CAMS
```

Đây chính là mẫu "Mappings with Transformations" trong slide (`S(city,temp-32*5/9,month) ⊆
Weather(...)`) — không chỉ project/select đơn thuần mà còn có hàm biến đổi (đổi đơn vị gió km/h→m/s,
tính AQI từ nồng độ — xem mục 5 bên dưới).

**Vì sao GAV mà không phải LAV?** Theo slide ("GAV Summary"): GAV cho reformulation dễ (ở đây là
"ghi dữ liệu" chứ không phải reformulate truy vấn, nhưng logic tương tự: biết chính xác cách build mỗi
hàng `measurements` từ nguồn nào), đổi lại là nhược điểm đúng như slide nêu — "GAV forces everything
into the mediated schema's perspective": nếu sau này cần thêm một nguồn có cấu trúc rất khác (ví dụ một
API trả cả AQI và director phim trong cùng bản ghi — ví dụ cực đoan của slide "Tricky Example for GAV"),
sẽ phải viết lại mapping thủ công thay vì suy luận tự động như LAV.

## 4. Record Linkage / Entity Resolution (Slide `3_RecordLinkage_ER (1).pdf`)

Đây là phần project áp dụng **rõ nhất và trực tiếp nhất**. Vấn đề: một trạm vật lý (ví dụ một điểm đo ở
Hoàn Kiếm) có thể được cả AQICN và OpenAQ báo cáo, với 2 mã riêng — nếu tính trung bình thành phố mà
không gộp, trạm đó bị đếm hai lần, làm méo số liệu (đúng vấn đề "Instance and identity heterogeneity"
ở slide Introduction, và đúng định nghĩa "Entity Resolution" ở slide này: *"Recognizing those records
which represent identical persons, objects, or events"* — Fellegi & Sunter 1969).

File `processing/tools/build_reference.py` triển khai đúng 3 bước đầu của **Entity Resolution Pipeline**
(slide, mục "Entity Resolution Pipeline": Prepare Data → Blocking/Sorting → Matching → Clustering):

| Bước trong slide | Trong `build_reference.py` | Dòng |
|---|---|---|
| Prepare Data | Chuẩn hóa mỗi trạm về cùng schema `(station_id, source, city, name, lat, lon)` từ 2 nguồn khác định dạng (`stations.json`, `openaq_locations.json`) | 28–41 |
| Blocking | Chỉ so khớp các trạm **cùng thành phố** (`o["city"] == r["city"]`) — đúng kiểu "Basic: equivalent values on selected attributes" trong slide, tránh so khớp toàn bộ O(n²) giữa Hà Nội và TP.HCM | 46 |
| Matching | Dùng **distance measure** (Haversine, km) thay cho string similarity — đúng ghi chú của slide: *"Distance measure/cost measure have also been used — same concept, but smaller values → higher similarities"*. Ngưỡng `DUP_KM = 0.3` km đóng vai trò threshold `t` trong `s(x,y) ≥ t` (ở đây là `d(x,y) ≤ t`) | 18, 47 |
| Clustering | Gán chung `physical_id` cho các bản ghi khớp (merge/survivorship đơn giản: giữ `station_id` của OpenAQ làm đại diện) | 44–49 |

`infra/cassandra/schema.cql` ghi chú hệ quả: *"Một trạm có ở cả hai nguồn → bảng trạm gán cùng
`physical_id`... tổng hợp chỉ tính một lần"* — đây chính là bước dùng kết quả Entity Resolution khi tính
`daily_agg`.

**Giới hạn cần nêu trong báo cáo (trung thực, không phóng đại):** project chỉ dùng **một** tín hiệu
matching (khoảng cách địa lý), không kết hợp thêm **similarity đo tên trạm** (Jaccard/edit distance/
Soundex như slide trình bày chi tiết) vì tên trạm giữa 2 nguồn thường không đủ gần nhau để so sánh text
hữu ích (ví dụ AQICN đặt theo địa chỉ, OpenAQ đặt theo mã khu vực). Đây là lựa chọn thiết kế có lý do,
không phải thiếu sót chưa biết — nhưng nếu muốn mở rộng, có thể thêm bước Matching thứ 2 dùng
`similarity(name_aqicn, name_openaq)` làm tín hiệu phụ trợ trước khi merge, đúng tinh thần "generalized
Jaccard"/"soft TF-IDF" của slide.

## 5. Mappings with Transformations — tính AQI (Slide `2_SchemaAlignment.pdf`, "Data-Level Heterogeneity")

Slide minh họa mapping có hàm biến đổi dữ liệu (°C↔°F, giá có/không thuế). Project có ví dụ tương đương
quan trọng hơn: **US AQI** không có sẵn ở nguồn OpenAQ (chỉ có nồng độ PM2.5/PM10 µg/m³), nên phải áp
dụng công thức điểm gãy (breakpoint) của EPA để tính — một hàm biến đổi phi tuyến tính, phức tạp hơn
nhiều so với ví dụ đổi đơn vị đơn giản trong slide. Thực hiện tại `processing/common/aqi.py`, mô tả ở
`docs/processing.md` mục 5. Đây là bằng chứng cụ thể cho "Data value and representation heterogeneity"
(slide Introduction, bảng `TuitionFee`/`Rating` — ở đây tương ứng cột AQI của mỗi nguồn cần quy về cùng
một thang trước khi so sánh/tổng hợp).

## 6. Sources Selection (Slide `5_SourcesSelection.pdf`)

Slide phân biệt **source profiling** (hiểu nguồn chứa gì) và **source selection** (chọn tập nguồn nào
dùng, dựa trên coverage/accuracy/freshness/overlap). Project có 2 quyết định chọn nguồn cụ thể, tuy làm
thủ công (không chạy thuật toán GRASP/tối ưu benefit-cost như slide trình bày cho trường hợp tổng quát),
nhưng dựa trên đúng các tiêu chí slide liệt kê:

1. **Chọn nguồn theo thành phố dựa trên coverage thật tế**: Hà Nội dùng AQICN (có trạm cố định, tin
   cậy); TP.HCM dùng OpenAQ vì — theo phân tích của D ở `docs/analysis_ml.md` mục 1.2 — trạm AQICN duy
   nhất từng có ở TP.HCM (Lãnh sự quán Mỹ) đã ngừng báo số liệu hợp lệ từ 2024. Đây là quyết định
   *source selection* dựa trên **freshness** (nguồn còn cập nhật hay không) đúng khái niệm slide mục
   "Dynamic sources: freshness".
2. **Lọc trạm OpenAQ theo độ tươi dữ liệu**: `ingestion/producer.py` (`OPENAQ_ACTIVE_DAYS = 3`, hàm
   `load_openaq_stations`) chỉ coi một trạm OpenAQ là nguồn hợp lệ cho real-time nếu nó có số liệu trong
   3 ngày gần nhất — tránh gọi API tốn hạn mức cho các trạm đã ngừng hoạt động. Đây là một bước **source
   profiling động** (đánh giá freshness liên tục) trước khi đưa vào tập nguồn được chọn (*source
   selection*), đúng khái niệm "Time-aware selection" của slide (tuy không dùng mô hình toán hình thức,
   chỉ dùng ngưỡng đơn giản).
3. Open-Meteo (CAMS) được chọn làm **nguồn dự phòng/bổ sung** (không thay thế) cho cả 2 thành phố — vì
   nó *luôn* có dữ liệu (coverage=100%, nhưng accuracy thấp hơn — tương quan với số đo thật chỉ 0,49 ở Hà
   Nội theo `docs/analysis_ml.md` mục 1.4). Đây là đánh đổi **benefit vs cost/accuracy** kiểu slide mục
   "Source selection: benefit and cost" — chấp nhận một nguồn có coverage cao nhưng accuracy thấp hơn để
   đảm bảo mọi thành phố luôn có ít nhất một giá trị AQI, miễn là tách riêng khỏi số đo thật (`is_model`)
   để không làm giảm chất lượng dữ liệu chính.

## Tóm tắt nhanh cho slide thuyết trình

| Khái niệm môn học | 1 câu áp dụng trong project |
|---|---|
| Heterogeneity (6 mức) | 3 nguồn khác hệ thống/định dạng/schema/ngữ nghĩa/danh tính/chất lượng, xử lý ở `processing/common/` |
| Virtual vs Warehousing | Chọn Warehousing (ETL Spark → Cassandra) để không phụ thuộc hạn mức API khi truy vấn dashboard |
| Schema Alignment (GAV) | Mỗi nguồn là một view (có transformation) ánh xạ vào bảng `measurements` chung |
| Record Linkage / ER | `build_reference.py`: blocking theo thành phố, matching bằng ngưỡng khoảng cách 300m, gán `physical_id` chung |
| Data value transformation | Tính US AQI từ nồng độ bằng công thức breakpoint EPA (nguồn không có sẵn AQI) |
| Sources Selection | Chọn AQICN/OpenAQ theo thành phố dựa trên freshness thật; lọc trạm OpenAQ còn hoạt động (3 ngày); Open-Meteo làm nguồn dự phòng coverage cao/accuracy thấp |

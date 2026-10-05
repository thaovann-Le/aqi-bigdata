# ingestion/historical (vai B)

Script tải dữ liệu lịch sử 2–3 năm, lưu vào MinIO bucket `history`.

## Việc cần làm

- [ ] OpenAQ: tải từ kho lưu trữ công khai trên AWS S3 (xem tài liệu OpenAQ
      "OpenAQ S3 archive") cho các trạm ở Hà Nội và TP.HCM.
- [ ] Open-Meteo Historical API: tải dữ liệu thời tiết lịch sử tương ứng cùng khoảng thời gian/tọa độ.
- [ ] Lưu vào `history/openaq/...` và `history/open-meteo/...` trong MinIO, giữ định dạng gốc
      (CSV/JSON/Parquet tùy nguồn) kèm metadata (nguồn, khoảng thời gian, trạm).
- [ ] Sau khi tải xong, viết script/notebook thống kê: tổng dung lượng, số bản ghi,
      khoảng thời gian phủ, định dạng — dùng cho chương "Dữ liệu & 3V".

## Ghi chú

Đây là phần chính chứng minh **Volume** của đồ án (dữ liệu real-time trong 1 tuần chỉ
vài chục MB). Nếu cần thêm dung lượng, mở rộng thêm tỉnh/thành hoặc quốc gia lân cận.

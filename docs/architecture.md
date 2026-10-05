# Chương: Kiến trúc hệ thống (vai A)

> Khung sườn — điền nội dung trước Ngày 6.

## 1. Sơ đồ kiến trúc tổng thể

(Chèn sơ đồ: nguồn dữ liệu → Kafka → Spark → MinIO/Cassandra → Grafana.
Xem sơ đồ text trong `README.md` làm điểm xuất phát.)

## 2. Lý do chọn công nghệ

- Vì sao Kafka (KRaft) thay vì ghi thẳng vào lưu trữ?
- Vì sao MinIO cho dữ liệu thô, Cassandra cho dữ liệu đã xử lý (thay vì HDFS/Elasticsearch)?
- Vì sao Spark Structured Streaming thay vì xử lý theo lô đơn thuần?
- Vì sao Grafana cho dashboard?

## 3. Triển khai

- Docker Compose: các service, phụ thuộc khởi động (healthcheck, init container).
- Cấu hình mạng, volume, các cổng dịch vụ.

## 4. Giới hạn và hướng mở rộng

- Single-node cho mọi thành phần (không có khả năng chịu lỗi/mở rộng ngang).
- Hướng mở rộng nếu có thêm thời gian: Zookeeper/multi-broker Kafka, Spark cluster nhiều worker,
  Cassandra multi-node.

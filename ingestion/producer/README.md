# ingestion/producer (vai B)

Producer Python gọi AQICN + Open-Meteo mỗi 10–15 phút, đẩy JSON vào Kafka.

## Việc cần làm (Ngày 1–2)

- [ ] Đăng ký API key AQICN (`AQICN_API_TOKEN`) và OpenAQ (`OPENAQ_API_KEY`), đặt vào `.env` ở repo gốc.
- [ ] Chốt danh sách trạm ở Hà Nội và TP.HCM (station id / tọa độ).
- [ ] Script gọi API theo lịch (10–15 phút/lần), publish vào topic Kafka:
  - `aqi_raw` — dữ liệu AQI/ô nhiễm từ AQICN
  - `weather_raw` — dữ liệu thời tiết từ Open-Meteo
- [ ] Retry khi lỗi mạng/API, ghi log rõ ràng (thời điểm, trạm, lỗi).
- [ ] Kết nối Kafka qua `localhost:9094` (từ host) hoặc `kafka:9092` (từ container khác trong `aqi-net`).

## Gợi ý thư viện

`requests` hoặc `httpx`, `confluent-kafka` hoặc `kafka-python`, `python-dotenv`, `tenacity` (retry), `schedule` hoặc vòng lặp `time.sleep`.

## Ghi chú

Producer cần chạy liên tục từ Ngày 2 trên một máy luôn mở (hoặc VPS) để đạt yêu cầu
"real-time liên tục ít nhất 5 ngày" trong `docs/PLAN.md`.

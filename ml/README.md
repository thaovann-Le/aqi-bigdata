# ml/ — Phân tích & dự báo (vai D)

Khám phá dữ liệu, tương quan với thời tiết, so sánh 63 tỉnh/thành, mô hình XGBoost dự báo
AQI 24 giờ tới cho Hà Nội và TP.HCM, script dự báo hằng giờ ghi bảng `forecasts`.

**Đầu vào là dữ liệu đã xử lý của C** ([docs/data_contract.md](../docs/data_contract.md)), không đọc dữ liệu thô:

| Việc | Nguồn |
|---|---|
| Notebook, huấn luyện | File thành phố-giờ do `processing/batch/export_ml_dataset.py` xuất ra |
| Dự báo hằng giờ | Cassandra `daily_agg` (`period_type='hour'`) + `weather_hourly` |
| Thời tiết lúc dự báo (72 giờ trước + ngày mai) | Open-Meteo Forecast API, giá trị theo giờ (cùng định nghĩa với dữ liệu train). `weather_hourly` real-time của C là ảnh chụp "current" 15 phút (mưa = 15 phút gần nhất), nên chỉ dùng để lấp giờ API thiếu |

## Chạy

```bash
# 1. Hạ tầng + pipeline của C (sinh processing/_runtime/ml_dataset/), xem processing/README.md
docker compose -f docker-compose.demo.yml up -d

# 2. Môi trường Python của D
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r ml/requirements.txt

# 3. Test -> 4 notebook -> huấn luyện (khoảng 1 phút)
bash ml/run_all.sh

# 4. Đổ dự báo của giai đoạn test vào Cassandra cho panel "dự báo vs thực tế" của A
#    (model_name có hậu tố _backtest: dùng thời tiết thật của ngày sau nên tốt hơn dự báo live)
python ml/serving/backfill_forecasts.py

# 5. Dự báo hằng giờ (chạy mãi; hoặc bỏ --every-minutes và đặt cron mỗi giờ, phút 20)
python ml/serving/predict.py --every-minutes 60
```

Đổi đường dẫn bằng biến môi trường:
- `AQI_ML_DATASET`: file C xuất ra.
- `AQI_OUTPUT_DIR`: thư mục kết quả.
- `CASSANDRA_HOST`, `CASSANDRA_PORT`, `CASSANDRA_KEYSPACE`, `CASSANDRA_DC`: kết nối Cassandra.

Kết quả nằm trong `ml/outputs/`:
- `figures/` (hình) và `tables/` (CSV) được đưa vào git vì báo cáo dùng.
- `models/` không đưa vào git, vì train lại chưa tới 1 phút.

## Cấu trúc

```
aqiml/                 package dùng chung
  config.py            đường dẫn, mùa (giống C), giai đoạn train/validation/test, ngưỡng cảnh báo
  data.py              đọc file C xuất ra -> chuỗi theo giờ của từng thành phố
  features.py          đặc trưng cho dự báo +24h
  modeling.py          chia tập, XGBoost, chỉ số đánh giá, lưu/đọc mô hình
  storage.py           đọc daily_agg/weather_hourly, ghi forecasts (Cassandra)
  aqi.py, plotting.py  hạng AQI, kiểu hình dùng chung
notebooks/             định dạng "# %%": chạy như script hoặc mở như notebook (VS Code / jupytext)
  01_data_quality.py   quy mô, độ phủ, số trạm, số đo vs số mô hình
  02_eda_trends.py     chu kỳ giờ/tuần/tháng/mùa, hạng AQI, Hà Nội vs TP.HCM
  03_weather_correlation.py   Spearman, trong cùng tháng, theo nhóm, hoa ô nhiễm, độ trễ
  04_spatial_overview.py      63 tỉnh/thành: xếp hạng, ba miền, tương quan theo khoảng cách
training/train_xgboost.py     huấn luyện + đánh giá + hình/bảng
serving/predict.py            dự báo hằng giờ -> forecasts
serving/backfill_forecasts.py ghi dự báo giai đoạn test vào forecasts
tests/                        pytest
```

## Các quyết định cần ghi trong báo cáo

- **Chỉ dùng giai đoạn mạng trạm hiện tại:** Hà Nội từ 01/2024, TP.HCM từ 19/11/2024.
  - Chuỗi AQI đo của thành phố là trung bình các trạm có số liệu trong giờ, mà tập trạm đã đổi. Trước 2024 chỉ có trạm ĐSQ/Lãnh sự quán Mỹ; sau đó là mạng trạm thành phố (Hà Nội) và một cảm biến giá rẻ (TP.HCM, CMT8).
  - Mức của chuỗi đổi theo: chênh lệch "AQI đo − AQI mô hình" của Hà Nội đổi từ khoảng 0 sang −18 đến −33 (notebook 01).
  - Dữ liệu real-time đến từ cùng loại trạm với giai đoạn sau.
- **TP.HCM chỉ có một cảm biến giá rẻ.** Loại này thường đo cao khi độ ẩm cao, nên phải thận trọng khi so sánh mức AQI với Hà Nội.
- **Mùa theo quy ước của C:**
  - Hà Nội: Xuân 2–4, Hạ 5–7, Thu 8–10, Đông 11–1.
  - TP.HCM: mùa khô 11–4, mùa mưa 5–10.
- **61 tỉnh còn lại chỉ có số mô hình CAMS.** 63 tỉnh nhưng chỉ 53 chuỗi khác nhau, do lưới CAMS thô.

## Mô hình

- **Mục tiêu:** AQI đo của thành phố tại t+24h. Mô hình học **mức thay đổi** so với AQI hiện tại; dự báo = AQI hiện tại + thay đổi dự báo. Cách này tốt hơn dự báo trực tiếp mức AQI, vì mô hình không kéo dự báo về mức trung bình.
- **Baseline persistence:** "24 giờ tới giống bây giờ".
- **Bộ đặc trưng** (`features.py`):
  - `base`: AQI 1/3/6/24 giờ trước, trung bình trượt, số trạm, lịch, thời tiết hiện tại và trung bình/tổng 24 giờ.
  - `weather_forecast` (**bản triển khai**): `base` + thời tiết tại t+24.
    - Khi train, dùng thời tiết lịch sử (coi như dự báo hoàn hảo), nên độ chính xác thực tế sẽ thấp hơn một chút.
    - Khi chạy thật, lấy từ Open-Meteo Forecast API. API lỗi thì tự quay về `base`, và bỏ cột mưa (số mưa real-time trong Cassandra là ảnh chụp 15 phút, khác nghĩa với dữ liệu train).
  - Bộ dùng thêm CAMS đã thử nhưng không cải thiện, nên bỏ.
- **Chia tập theo thời gian của mục tiêu** (`SPLITS` trong `config.py`):

| | Train | Validation (early stopping) | Test |
|---|---|---|---|
| Hà Nội | 01/2024 – 09/2025, trừ validation | 01–03/2025 | 10–12/2025 |
| TP.HCM | 19/11/2024 – 09/2025, trừ validation | 01–03/2025 | 10–12/2025 |

  Bỏ 48 giờ ngay sau cửa sổ validation khỏi tập train, vì đặc trưng của các giờ đó còn chứa AQI trong validation. Validation chỉ dùng cho early stopping; số liệu trung thực là của tập test.

  Validation đặt vào mùa ô nhiễm. Đặt vào mùa hè thì early stopping chọn mô hình hợp với AQI thấp, và mô hình thua persistence ở mùa đông. Dữ liệu 2026 chưa dùng vì thời tiết và CAMS lịch sử mới có đến hết 2025.
- **Kết quả:** `outputs/tables/forecast_metrics.csv` (MAE, RMSE, R², bias, precision/recall cảnh báo AQI > 150), cùng `forecast_metrics_by_season.csv` và `feature_importance.csv`.

## Dự báo hằng giờ

- Lần lượt cho từng thành phố, script đọc 72 giờ gần nhất từ Cassandra (bỏ giờ hiện tại vì còn thiếu dữ liệu), dự báo cho giờ cuối cùng có AQI đo, và ghi `forecasts`.
- Bỏ qua thành phố nào không có AQI đo trong 3 giờ gần nhất.
- C tính lại `daily_agg` 15 phút một lần, nên nếu dùng cron thì đặt vào khoảng phút 20 mỗi giờ.
- Chạy thử offline, không cần Cassandra:
  ```bash
  python ml/serving/predict.py --source dataset --as-of "2025-12-10 08:00" --dry-run
  ```

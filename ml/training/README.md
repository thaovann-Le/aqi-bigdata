# ml/training (vai D)

Huấn luyện mô hình XGBoost dự báo AQI 24 giờ tới, cho mỗi thành phố riêng.

## Việc cần làm

- [ ] Tạo đặc trưng (feature engineering): giờ, ngày trong tuần, tháng, thời tiết,
      AQI của 1/3/6/24 giờ trước (lag features).
- [ ] Huấn luyện XGBoost dự báo AQI +24h cho Hà Nội và TP.HCM (2 mô hình riêng, hoặc 1 mô hình
      có đặc trưng `city`).
- [ ] So sánh với mô hình cơ sở (baseline) "ngày mai giống hôm nay" (persistence model).
- [ ] Đánh giá bằng MAE, RMSE trên tập test theo thời gian (time-based split, không shuffle ngẫu nhiên).
- [ ] Lưu model đã huấn luyện (ví dụ `.json` của XGBoost) để `ml/serving` dùng lại.

## Gợi ý thư viện

`xgboost`, `pandas`, `scikit-learn` (metrics, time series split), `cassandra-driver` (đọc dữ liệu
từ bảng `measurements`).

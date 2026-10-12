# %% [markdown]
# # 01 - Chất lượng dữ liệu đầu vào (dữ liệu đã xử lý của C)
#
# Nguồn: bảng thành phố-giờ do `processing/batch/export_ml_dataset.py` xuất ra
# (cùng nội dung với Cassandra `daily_agg` giờ + `weather_hourly`, xem `docs/data_contract.md`).
#
# Câu hỏi: dữ liệu phủ được bao nhiêu, thiếu ở đâu, và chuỗi "AQI đo" của thành phố có
# so sánh được giữa các năm không. Kết quả quyết định giai đoạn dùng cho phân tích và mô hình.
#
# Chạy như script: `python ml/notebooks/01_data_quality.py` (mỗi `# %%` là một cell).

# %%
import _setup  # noqa: F401  (adds ml/ to sys.path)
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from aqiml import plotting
from aqiml.config import CITIES, TABLE_DIR, ensure_output_dirs
from aqiml.data import current_network_era, load_dataset

ensure_output_dirs()
everything = load_dataset()  # 63 tỉnh/thành
df = everything[everything["city"].isin(CITIES)]

# %% [markdown]
# ## 1. Quy mô bộ dữ liệu
#
# Hà Nội và TP.HCM có số đo trạm + số mô hình + thời tiết; 61 tỉnh còn lại chỉ có số mô hình
# CAMS và thời tiết (dùng để tăng dung lượng và so sánh toàn quốc, notebook 04).

# %%
size = everything.groupby("city").agg(
    hours=("ts", "size"),
    measured_hours=("aqi", lambda s: int(s.notna().sum())),
    model_hours=("cams_aqi", lambda s: int(s.notna().sum())),
    weather_hours=("temperature", lambda s: int(s.notna().sum())),
)
overview = pd.DataFrame(
    {
        "Nhóm": ["Hà Nội + TP.HCM", "61 tỉnh còn lại", "Tổng"],
        "Số thành phố": [2, len(size) - 2, len(size)],
        "Số dòng thành phố-giờ": [size.loc[list(CITIES), "hours"].sum(), size.drop(list(CITIES))["hours"].sum(),
                                   size["hours"].sum()],
        "Giờ có số đo trạm": [size.loc[list(CITIES), "measured_hours"].sum(),
                              size.drop(list(CITIES))["measured_hours"].sum(), size["measured_hours"].sum()],
    }
)
overview.to_csv(TABLE_DIR / "01_dataset_size.csv", index=False)
print(overview.to_string(index=False))

# %% [markdown]
# ## 2. Độ phủ theo năm và số trạm
#
# `n_stations` là số trạm vật lý có số đo trong giờ đó. Chuỗi AQI đo của thành phố là
# trung bình các trạm này, nên khi tập trạm thay đổi thì mức của chuỗi cũng thay đổi.

# %%
yearly = df.assign(year=df["ts"].dt.year).groupby(["city", "year"]).agg(
    pct_measured=("aqi", lambda s: 100 * s.notna().mean()),
    mean_stations=("n_stations", "mean"),
    aqi_measured=("aqi", "mean"),
    aqi_model=("cams_aqi", "mean"),
    pct_weather=("temperature", lambda s: 100 * s.notna().mean()),
)
yearly = yearly.assign(measured_minus_model=yearly["aqi_measured"] - yearly["aqi_model"]).round(1)
yearly.to_csv(TABLE_DIR / "01_coverage_by_year.csv")
print(yearly.to_string())

# %%
fig, axes = plt.subplots(2, 1, figsize=(13, 6.5), sharex=True)
for ax, city in zip(axes, CITIES):
    g = df[df["city"] == city].set_index("ts")
    monthly = g[["aqi", "cams_aqi", "n_stations"]].resample("MS").mean()
    coverage = g["aqi"].notna().resample("MS").mean()
    ax.plot(monthly.index, monthly["aqi"], color=plotting.CITY_COLORS[city], lw=2, label="AQI đo (TB tháng)")
    ax.plot(monthly.index, monthly["cams_aqi"], color=plotting.MODEL_COLORS["cams"], lw=1.2, ls="--",
            label="AQI mô hình CAMS")
    ax.set(ylabel="US AQI", title=plotting.CITY_LABELS[city])
    twin = ax.twinx()
    twin.bar(monthly.index, monthly["n_stations"], width=25, color="#999999", alpha=0.25, label="Số trạm TB")
    twin.set_ylabel("Số trạm")
    twin.grid(False)
    for month, cov in coverage.items():
        if cov < 0.5:
            ax.axvspan(month, month + pd.offsets.MonthEnd(1), color="#F5B7B1", alpha=0.25, lw=0)
    lines = ax.get_legend_handles_labels()
    bars = twin.get_legend_handles_labels()
    ax.legend(lines[0] + bars[0], lines[1] + bars[1], loc="upper left", ncol=3)
fig.suptitle("AQI đo, AQI mô hình và số trạm theo tháng (vùng hồng: < 50% số giờ có số đo)", y=1.0)
plotting.save(fig, "01_monthly_measured_model_stations")

# %% [markdown]
# **Đọc biểu đồ:** CAMS dùng cùng một mô hình cho cả giai đoạn nên là thước đo ổn định.
# Khoảng cách giữa AQI đo và AQI mô hình đổi hẳn khi tập trạm đổi:
# - Hà Nội: 2021-2023 chỉ có trạm ĐSQ Mỹ; từ 2024 là mạng trạm thành phố (nhiều trạm, đo thấp hơn).
# - TP.HCM: 2021-2023 là trạm Lãnh sự quán Mỹ; từ 19/11/2024 chỉ còn cảm biến giá rẻ CMT8.
#
# Dữ liệu real-time đến từ cùng loại trạm với giai đoạn sau, nên phân tích chính và mô hình
# chỉ dùng **giai đoạn mạng trạm hiện tại** (`SPLITS` trong `ml/aqiml/config.py`).

# %% [markdown]
# ## 3. Độ phủ theo giờ trong giai đoạn mạng trạm hiện tại

# %%
era = current_network_era(df)
era_cov = era.groupby("city").agg(
    start=("ts", "min"),
    end=("ts", "max"),
    hours=("ts", "size"),
    pct_measured=("aqi", lambda s: round(100 * s.notna().mean(), 1)),
    pct_weather=("temperature", lambda s: round(100 * s.notna().mean(), 1)),
    median_stations=("n_stations", "median"),
)
era_cov.to_csv(TABLE_DIR / "01_current_era_coverage.csv")
print(era_cov.to_string())

# Gaps: lengths of consecutive missing-hour runs in the measured series.
gap_rows = []
for city, g in era.groupby("city"):
    missing = g.set_index("ts")["aqi"].isna()
    runs = missing.ne(missing.shift()).cumsum()[missing]
    lengths = runs.value_counts()
    gap_rows.append({"city": city, "gaps": len(lengths), "longest_gap_h": int(lengths.max()) if len(lengths) else 0,
                     "gaps_over_24h": int((lengths > 24).sum())})
gaps = pd.DataFrame(gap_rows)
gaps.to_csv(TABLE_DIR / "01_gaps.csv", index=False)
print(gaps.to_string(index=False))

# %% [markdown]
# ## 4. Số đo trạm so với số mô hình (giai đoạn hiện tại)
#
# Mức độ khớp giữa CAMS và số đo cho biết có nên dùng CAMS làm đặc trưng hay để lấp chỗ trống.

# %%
fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
stats = []
for ax, city in zip(axes, CITIES):
    g = era[era["city"] == city].dropna(subset=["aqi", "cams_aqi"])
    ax.scatter(g["cams_aqi"], g["aqi"], s=3, alpha=0.2, color=plotting.CITY_COLORS[city])
    lim = float(np.nanpercentile(g[["cams_aqi", "aqi"]], 99.5))
    ax.plot([0, lim], [0, lim], color="black", lw=1)
    ax.set(xlim=(0, lim), ylim=(0, lim), xlabel="AQI mô hình CAMS", ylabel="AQI đo",
           title=f"{plotting.CITY_LABELS[city]}: đo vs mô hình (theo giờ)")
    daily = g.set_index("ts")[["aqi", "cams_aqi"]].resample("D").mean().dropna()
    stats.append({
        "city": city,
        "hours": len(g),
        "pearson_hourly": g["aqi"].corr(g["cams_aqi"]),
        "pearson_daily": daily["aqi"].corr(daily["cams_aqi"]),
        "mean_measured": g["aqi"].mean(),
        "mean_model": g["cams_aqi"].mean(),
        "mae_model_vs_measured": (g["cams_aqi"] - g["aqi"]).abs().mean(),
    })
plotting.save(fig, "01_measured_vs_model")
stats = pd.DataFrame(stats).round(3)
stats.to_csv(TABLE_DIR / "01_measured_vs_model.csv", index=False)
print(stats.to_string(index=False))

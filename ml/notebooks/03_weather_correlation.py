# %% [markdown]
# # 03 - Tương quan AQI với thời tiết
#
# Dữ liệu: AQI đo của thành phố + thời tiết Open-Meteo do C ghép theo (thành phố, giờ),
# giai đoạn mạng trạm hiện tại có thời tiết (đến 31/12/2025). Gió đơn vị m/s.
# Dùng hệ số **Spearman** (đơn điệu, không giả định tuyến tính, ít nhạy với ngoại lai);
# tính cả theo giờ và theo ngày.

# %%
import _setup  # noqa: F401
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from aqiml import plotting
from aqiml.config import CITIES, TABLE_DIR, ensure_output_dirs
from aqiml.data import current_network_era, load_dataset

ensure_output_dirs()
df = current_network_era(load_dataset(list(CITIES))).dropna(subset=["aqi", "temperature"])

WEATHER = {
    "temperature": "Nhiệt độ (°C)",
    "humidity": "Độ ẩm (%)",
    "precipitation": "Lượng mưa (mm/h)",
    "wind_speed": "Tốc độ gió (m/s)",
    "pressure": "Áp suất (hPa)",
}


def daily_frame(g: pd.DataFrame) -> pd.DataFrame:
    d = g.set_index("ts")
    agg = d[["aqi", "temperature", "humidity", "wind_speed", "pressure"]].resample("D").mean()
    agg["precipitation"] = d["precipitation"].resample("D").sum(min_count=12)
    return agg.dropna()


print(df.groupby("city")["ts"].agg(["min", "max", "size"]).to_string())

# %% [markdown]
# ## 1. Hệ số tương quan Spearman (theo giờ và theo ngày)

# %%
rows = []
for city, g in df.groupby("city"):
    d = daily_frame(g)
    for col in WEATHER:
        rows.append({
            "city": city,
            "variable": col,
            "spearman_hourly": g["aqi"].corr(g[col], method="spearman"),
            "spearman_daily": d["aqi"].corr(d[col], method="spearman"),
        })
corr = pd.DataFrame(rows).round(3)
corr.to_csv(TABLE_DIR / "03_spearman.csv", index=False)
print(corr.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 4))
width, x = 0.38, np.arange(len(WEATHER))
for i, city in enumerate(CITIES):
    vals = corr[corr["city"] == city].set_index("variable").loc[list(WEATHER), "spearman_daily"]
    ax.bar(x + (i - 0.5) * width, vals, width, color=plotting.CITY_COLORS[city], label=plotting.CITY_LABELS[city])
ax.axhline(0, color="black", lw=0.8)
ax.set_xticks(x, [WEATHER[c].split(" (")[0] for c in WEATHER])
ax.set(ylabel="Spearman ρ (theo ngày)", title="Tương quan AQI với các yếu tố thời tiết")
ax.legend()
plotting.save(fig, "03_spearman_bar")

# %% [markdown]
# ## 2. Tương quan *trong cùng tháng*
#
# Mùa ô nhiễm cũng là mùa lạnh/khô, nên một phần tương quan với nhiệt độ, độ ẩm, áp suất chỉ
# phản ánh mùa. Tính lại trong từng tháng rồi lấy trung bình để tách hiệu ứng thời tiết khỏi mùa.

# %%
within = []
for city, g in df.groupby("city"):
    d = daily_frame(g)
    d = d.assign(month=d.index.month)
    for col in WEATHER:
        per_month = [m["aqi"].corr(m[col], method="spearman") for _, m in d.groupby("month") if len(m) >= 15]
        within.append({"city": city, "variable": col, "spearman_within_month": np.nanmean(per_month)})
within = pd.DataFrame(within).round(3)
within.to_csv(TABLE_DIR / "03_spearman_within_month.csv", index=False)
print(within.pivot(index="variable", columns="city", values="spearman_within_month").to_string())

# %% [markdown]
# ## 3. AQI theo nhóm giá trị thời tiết; ngày mưa và ngày không mưa

# %%
fig, axes = plt.subplots(1, 4, figsize=(16, 3.8))
bins = {
    "wind_speed": [0, 1, 2, 3, 4, 5, 7, 10, 20],
    "humidity": [20, 50, 60, 70, 80, 90, 101],
    "temperature": [5, 12, 16, 20, 24, 28, 32, 40],
}
for ax, (col, edges) in zip(axes, bins.items()):
    for city in CITIES:
        g = df[df["city"] == city]
        grouped = g.groupby(pd.cut(g[col], edges), observed=True)["aqi"]
        med, q1, q3 = grouped.median(), grouped.quantile(0.25), grouped.quantile(0.75)
        centers = [iv.mid for iv in med.index]
        ax.plot(centers, med.to_numpy(), marker="o", color=plotting.CITY_COLORS[city], label=plotting.CITY_LABELS[city])
        ax.fill_between(centers, q1.to_numpy(), q3.to_numpy(), color=plotting.CITY_COLORS[city], alpha=0.12)
    ax.set(xlabel=WEATHER[col], ylabel="US AQI", title=f"AQI theo {WEATHER[col].split(' (')[0].lower()}")

rain_rows = []
for i, city in enumerate(CITIES):
    d = daily_frame(df[df["city"] == city])
    rainy = d["precipitation"] >= 1
    rain_rows.append({"city": city, "aqi_dry_days": d.loc[~rainy, "aqi"].median(),
                      "aqi_rain_days": d.loc[rainy, "aqi"].median(), "rain_days": int(rainy.sum()),
                      "dry_days": int((~rainy).sum())})
    box = axes[3].boxplot([d.loc[~rainy, "aqi"], d.loc[rainy, "aqi"]], positions=[i * 3, i * 3 + 1],
                          showfliers=False, patch_artist=True, widths=0.7)
    for patch in box["boxes"]:
        patch.set(facecolor=plotting.CITY_COLORS[city], alpha=0.35)
axes[3].set_xticks([0, 1, 3, 4], ["HN khô", "HN mưa", "HCM khô", "HCM mưa"])
axes[3].set(ylabel="AQI TB ngày", title="Ngày mưa (>= 1 mm) vs không mưa")
axes[0].legend()
plotting.save(fig, "03_binned_weather")
rain = pd.DataFrame(rain_rows).round(1)
rain.to_csv(TABLE_DIR / "03_rain_effect.csv", index=False)
print(rain.to_string(index=False))

# %% [markdown]
# ## 4. Hoa ô nhiễm (pollution rose): AQI theo hướng gió
#
# Hướng gió theo quy ước khí tượng (hướng gió *thổi đến từ*). Bỏ giờ lặng gió (< 0,5 m/s).

# %%
SECTORS = 16
fig, axes = plt.subplots(1, 2, figsize=(12, 5), subplot_kw={"projection": "polar"})
rose_rows = []
for ax, city in zip(axes, CITIES):
    g = df[(df["city"] == city) & (df["wind_speed"] > 0.5)]
    sector = ((g["wind_direction"] + 360 / SECTORS / 2) % 360 // (360 / SECTORS)).astype(int)
    stats = g.groupby(sector)["aqi"].agg(["mean", "size"]).reindex(range(SECTORS))
    theta = np.deg2rad(np.arange(SECTORS) * 360 / SECTORS)
    norm = plt.Normalize(stats["mean"].min(), stats["mean"].max())
    ax.bar(theta, stats["size"] / stats["size"].sum(), width=2 * np.pi / SECTORS * 0.9,
           color=plt.cm.YlOrRd(norm(stats["mean"])), edgecolor="grey", linewidth=0.3)
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_xticks(np.deg2rad(np.arange(0, 360, 45)), ["B", "ĐB", "Đ", "ĐN", "N", "TN", "T", "TB"])
    ax.set_yticklabels([])
    fig.colorbar(plt.cm.ScalarMappable(norm=norm, cmap="YlOrRd"), ax=ax, shrink=0.6, pad=0.1, label="AQI TB")
    ax.set_title(f"{plotting.CITY_LABELS[city]}\n(độ dài = tần suất gió, màu = AQI TB)", fontsize=10)
    rose_rows.append(stats.assign(city=city, direction_deg=np.arange(SECTORS) * 360 / SECTORS))
plotting.save(fig, "03_pollution_rose")
pd.concat(rose_rows).round(1).to_csv(TABLE_DIR / "03_pollution_rose.csv")

# %% [markdown]
# ## 5. Tương quan có độ trễ
#
# Độ trễ k = thời tiết của k ngày trước so với AQI hôm nay. Tương quan còn cao ở k = 1-2 cho
# thấy thời tiết tác động kéo dài (tích tụ hoặc rửa trôi bụi qua nhiều ngày), nên mô hình dùng
# cả giá trị trung bình/tổng 24h của thời tiết. Lưu ý: với độ ẩm và áp suất, tương quan tăng
# theo độ trễ phần lớn do cả hai cùng đi theo mùa.

# %%
lag_rows = []
for city, g in df.groupby("city"):
    d = daily_frame(g)
    for col in ["wind_speed", "precipitation", "humidity", "pressure"]:
        for lag in range(0, 4):
            lag_rows.append({"city": city, "variable": col, "lag_days": lag,
                             "spearman": d["aqi"].corr(d[col].shift(lag), method="spearman")})
lags = pd.DataFrame(lag_rows).round(3)
lags.to_csv(TABLE_DIR / "03_lagged_correlation.csv", index=False)

fig, axes = plt.subplots(1, 2, figsize=(12, 3.8), sharey=True)
for ax, city in zip(axes, CITIES):
    for col, g in lags[lags["city"] == city].groupby("variable"):
        ax.plot(g["lag_days"], g["spearman"], marker="o", label=WEATHER[col].split(" (")[0])
    ax.axhline(0, color="black", lw=0.8)
    ax.set(xlabel="Độ trễ (ngày)", ylabel="Spearman ρ", xticks=range(4),
           title=plotting.CITY_LABELS[city])
axes[0].legend()
plotting.save(fig, "03_lagged_correlation")
print(lags.pivot_table(index=["city", "variable"], columns="lag_days", values="spearman").to_string())

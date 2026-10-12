# %% [markdown]
# # 02 - Xu hướng AQI theo giờ, ngày, tháng, mùa; so sánh Hà Nội và TP.HCM
#
# Dữ liệu: AQI đo của thành phố (dữ liệu đã xử lý của C), **giai đoạn mạng trạm hiện tại**
# (Hà Nội từ 01/2024, TP.HCM từ 19/11/2024 - lý do: notebook 01). Mùa theo quy ước của C:
# Hà Nội Xuân 2-4, Hạ 5-7, Thu 8-10, Đông 11-1; TP.HCM mùa khô 11-4, mùa mưa 5-10.

# %%
import _setup  # noqa: F401
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from aqiml import plotting
from aqiml.aqi import AQI_CATEGORY_COLORS, AQI_CATEGORY_LABELS, aqi_category
from aqiml.config import ALERT_AQI, CITIES, SEASON_ORDER, TABLE_DIR, ensure_output_dirs, season_of
from aqiml.data import current_network_era, load_dataset

ensure_output_dirs()
df = current_network_era(load_dataset(list(CITIES))).dropna(subset=["aqi"])
df = df.assign(
    hour=df["ts"].dt.hour,
    dayofweek=df["ts"].dt.dayofweek,
    month=df["ts"].dt.month,
    season=[season_of(c, m) for c, m in zip(df["city"], df["ts"].dt.month)],
)
daily = df.set_index("ts").groupby("city")["aqi"].resample("D").mean().dropna().reset_index()
WEEKDAYS = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]


def profile(group_col: str) -> pd.DataFrame:
    return df.groupby(["city", group_col])["aqi"].describe(percentiles=[0.25, 0.5, 0.75])


# %% [markdown]
# ## 1. Thống kê tổng quan

# %%
summary = df.groupby("city").agg(
    start=("ts", "min"),
    end=("ts", "max"),
    hours=("aqi", "size"),
    aqi_mean=("aqi", "mean"),
    aqi_median=("aqi", "median"),
    aqi_p95=("aqi", lambda s: s.quantile(0.95)),
    pct_hours_over_alert=("aqi", lambda s: 100 * (s > ALERT_AQI).mean()),
)
summary = summary.join(
    daily.groupby("city").agg(days=("aqi", "size"), pct_days_over_alert=("aqi", lambda s: 100 * (s > ALERT_AQI).mean()))
)
summary = summary.apply(lambda col: col.round(1) if col.dtype.kind == "f" else col)
summary.to_csv(TABLE_DIR / "02_summary_by_city.csv")
print(summary.T.to_string())

# %% [markdown]
# ## 2. Chu kỳ trong ngày và trong tuần

# %%
fig, axes = plt.subplots(1, 2, figsize=(13, 4))
for city in CITIES:
    p = profile("hour").loc[city]
    axes[0].plot(p.index, p["50%"], color=plotting.CITY_COLORS[city], label=plotting.CITY_LABELS[city], lw=2)
    axes[0].fill_between(p.index, p["25%"], p["75%"], color=plotting.CITY_COLORS[city], alpha=0.15)
    w = profile("dayofweek").loc[city]
    axes[1].plot([WEEKDAYS[i] for i in w.index], w["50%"], marker="o", color=plotting.CITY_COLORS[city],
                 label=plotting.CITY_LABELS[city])
axes[0].set(xlabel="Giờ trong ngày", ylabel="US AQI", title="Chu kỳ ngày (trung vị, vùng IQR)", xticks=range(0, 24, 3))
axes[1].set(xlabel="Ngày trong tuần", ylabel="AQI trung vị", title="Chu kỳ tuần")
axes[0].legend()
plotting.save(fig, "02_diurnal_weekly")
profile("hour")["50%"].unstack(0).round(1).to_csv(TABLE_DIR / "02_diurnal_median.csv")

# %% [markdown]
# ## 3. Chu kỳ năm và theo mùa

# %%
fig, axes = plt.subplots(1, 2, figsize=(13, 4))
for city in CITIES:
    m = profile("month").loc[city]
    axes[0].plot(m.index, m["mean"], marker="o", color=plotting.CITY_COLORS[city], label=plotting.CITY_LABELS[city])
    axes[0].fill_between(m.index, m["25%"], m["75%"], color=plotting.CITY_COLORS[city], alpha=0.12)
axes[0].axhline(ALERT_AQI, ls="--", color="#8E44AD", lw=1)
axes[0].set(xlabel="Tháng", ylabel="US AQI", title="Chu kỳ năm (trung bình, vùng IQR)", xticks=range(1, 13))
axes[0].legend()

positions, data, colors, labels = [], [], [], []
for i, city in enumerate(CITIES):
    for j, season in enumerate(SEASON_ORDER[city]):
        positions.append(i * 5 + j)
        data.append(df[(df["city"] == city) & (df["season"] == season)]["aqi"].to_numpy())
        colors.append(plotting.CITY_COLORS[city])
        labels.append(f"{plotting.CITY_LABELS[city]}\n{season}")
box = axes[1].boxplot(data, positions=positions, showfliers=False, patch_artist=True, widths=0.7)
for patch, color in zip(box["boxes"], colors):
    patch.set(facecolor=color, alpha=0.35)
axes[1].set_xticks(positions, labels, fontsize=8)
axes[1].set(ylabel="US AQI", title="Phân bố theo mùa")
plotting.save(fig, "02_monthly_seasonal")

seasonal = df.groupby(["city", "season"])["aqi"].agg(["mean", "median", "count"]).round(1)
seasonal.to_csv(TABLE_DIR / "02_seasonal.csv")
print(seasonal.to_string())

# %% [markdown]
# ## 4. Bản đồ nhiệt giờ x tháng

# %%
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
for ax, city in zip(axes, CITIES):
    grid = df[df["city"] == city].pivot_table(index="hour", columns="month", values="aqi", aggfunc="mean")
    im = ax.imshow(grid.to_numpy(), aspect="auto", cmap="YlOrRd", origin="lower")
    ax.set_xticks(range(len(grid.columns)), grid.columns)
    ax.set_yticks(range(0, 24, 3), range(0, 24, 3))
    ax.set(xlabel="Tháng", ylabel="Giờ", title=f"{plotting.CITY_LABELS[city]} - AQI TB theo giờ x tháng")
    ax.grid(False)
    fig.colorbar(im, ax=ax, label="US AQI")
plotting.save(fig, "02_hour_month_heatmap")

# %% [markdown]
# ## 5. Phân bố hạng AQI và số ngày vượt ngưỡng cảnh báo

# %%
cat_share = (
    pd.crosstab(df["city"], aqi_category(df["aqi"]), normalize="index")
    .reindex(columns=AQI_CATEGORY_LABELS, fill_value=0)
    .mul(100)
)
cat_share.round(1).to_csv(TABLE_DIR / "02_aqi_category_share.csv")
print(cat_share.round(1).to_string())

fig, axes = plt.subplots(1, 2, figsize=(13, 3.8), gridspec_kw={"width_ratios": [1, 1.4]})
left = np.zeros(len(cat_share))
names = [plotting.CITY_LABELS[c] for c in cat_share.index]
for label, color in zip(AQI_CATEGORY_LABELS, AQI_CATEGORY_COLORS):
    axes[0].barh(names, cat_share[label], left=left, color=color, label=label, edgecolor="white")
    left += cat_share[label].to_numpy()
axes[0].set(xlabel="% số giờ", title="Phân bố hạng AQI (theo giờ)")
axes[0].legend(fontsize=7, ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.2))

over = daily.assign(month=daily["ts"].dt.month, over=daily["aqi"] > ALERT_AQI)
over_by_month = over.groupby(["city", "month"])["over"].mean().mul(100).unstack(0)
for city in CITIES:
    axes[1].plot(over_by_month.index, over_by_month[city], marker="o", color=plotting.CITY_COLORS[city],
                 label=plotting.CITY_LABELS[city])
axes[1].set(xlabel="Tháng", ylabel="% số ngày", xticks=range(1, 13),
            title=f"Tỷ lệ ngày có AQI trung bình ngày > {ALERT_AQI}")
axes[1].legend()
plotting.save(fig, "02_aqi_categories")
over_by_month.round(1).to_csv(TABLE_DIR / "02_pct_days_over_alert_by_month.csv")

# %% [markdown]
# ## 6. Hà Nội và TP.HCM trong cùng các giờ
#
# So sánh trên đúng những giờ cả hai thành phố cùng có số đo (từ 19/11/2024), tránh lệch
# do mỗi thành phố thiếu dữ liệu ở những thời điểm khác nhau.
#
# **Thận trọng khi kết luận:** TP.HCM giai đoạn này chỉ có một cảm biến giá rẻ (CMT8). Cảm biến
# loại này thường đo cao hơn máy chuẩn khi độ ẩm cao. Trong 2021-2023, trạm Lãnh sự quán Mỹ cho
# AQI TP.HCM thấp hơn Hà Nội rõ rệt; khoảng cách nhỏ ở đây một phần do khác loại thiết bị
# (notebook 01, mục 2: AQI đo của TP.HCM cao hơn AQI mô hình khi chuyển sang CMT8).

# %%
both = df.pivot_table(index="ts", columns="city", values="aqi").dropna()
both_daily = both.resample("D").mean().dropna()
paired = pd.DataFrame(
    {
        "hours": [len(both)],
        "hanoi_mean": [both["hanoi"].mean()],
        "hcm_mean": [both["hcm"].mean()],
        "pct_hours_hanoi_worse": [100 * (both["hanoi"] > both["hcm"]).mean()],
        "corr_daily": [both_daily["hanoi"].corr(both_daily["hcm"])],
    }
).round(2)
paired.to_csv(TABLE_DIR / "02_hanoi_vs_hcm_paired.csv", index=False)
print(paired.to_string(index=False))

fig, ax = plt.subplots(figsize=(13, 3.8))
for city in CITIES:
    ax.plot(both_daily.index, both_daily[city], color=plotting.CITY_COLORS[city], lw=1.2,
            label=plotting.CITY_LABELS[city])
ax.axhline(ALERT_AQI, ls="--", color="#8E44AD", lw=1, label=f"Ngưỡng {ALERT_AQI}")
ax.set(ylabel="AQI TB ngày", title="Hà Nội và TP.HCM trên cùng các ngày")
ax.legend(ncol=3)
plotting.save(fig, "02_hanoi_vs_hcm_daily")

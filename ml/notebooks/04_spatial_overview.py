# %% [markdown]
# # 04 - Toàn cảnh 63 tỉnh/thành (số mô hình CAMS)
#
# Số đo trạm chỉ có ở Hà Nội và TP.HCM. Với cả 63 tỉnh/thành, dữ liệu đã xử lý của C có
# AQI/PM2.5 **mô hình hóa** (CAMS qua Open-Meteo, `is_model = true`), 08/2022-2025.
# Dùng để đặt hai thành phố vào bối cảnh cả nước và so sánh mùa ô nhiễm giữa ba miền.
# Đây là giá trị mô hình, không phải đo đạc (độ lệch so với số đo: notebook 01, mục 4).

# %%
import json

import _setup  # noqa: F401
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from aqiml import plotting
from aqiml.config import REPO_ROOT, TABLE_DIR, ensure_output_dirs
from aqiml.data import load_dataset

ensure_output_dirs()
coords = pd.DataFrame(json.loads((REPO_ROOT / "ingestion" / "points_vn.json").read_text(encoding="utf-8")))
coords = coords.rename(columns={"id": "city"}).set_index("city")

aq = load_dataset()[["city", "ts", "cams_aqi", "cams_pm25"]].dropna(subset=["cams_pm25"])
aq = aq[(aq["ts"] >= "2023-01-01") & (aq["ts"] < "2026-01-01")]  # 3 full years
print(f"{aq['city'].nunique()} tỉnh/thành, {len(aq):,} giờ có số mô hình (2023-2025)")

# North: lat >= 19 (to Thanh Hoa), Central + Highlands: 11.5-19, South: < 11.5.
REGIONS = [(19.0, "Miền Bắc"), (11.5, "Miền Trung - Tây Nguyên"), (-90, "Miền Nam")]
REGION_COLORS = {"Miền Bắc": "#C0392B", "Miền Trung - Tây Nguyên": "#D68910", "Miền Nam": "#2471A3"}


def region_of(lat: float) -> str:
    return next(name for bound, name in REGIONS if lat >= bound)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1, lon1, lat2, lon2 = map(np.deg2rad, (lat1, lon1, lat2, lon2))
    h = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return float(6371 * 2 * np.arcsin(np.sqrt(h)))


# %% [markdown]
# ## 1. AQI/PM2.5 trung bình 2023-2025 theo tỉnh
#
# CAMS có lưới thô (~0,4°): các tỉnh liền kề có thể rơi vào cùng một ô lưới và có chuỗi giống
# hệt nhau. Các tỉnh trùng chuỗi được gán cùng `cell`.

# %%
fingerprint = aq.groupby("city")["cams_pm25"].apply(lambda s: hash(tuple(s.round(2))))
points = aq.groupby("city").agg(
    aqi_mean=("cams_aqi", "mean"),
    pm25_mean=("cams_pm25", "mean"),
    pm25_p95=("cams_pm25", lambda s: s.quantile(0.95)),
    pct_hours_aqi_over_150=("cams_aqi", lambda s: 100 * (s > 150).mean()),
)
points = points.join(coords[["name", "lat", "lon"]])
points = points.assign(
    region=points["lat"].map(region_of),
    cell=fingerprint.map({fp: i for i, fp in enumerate(fingerprint.unique())}),
).sort_values("pm25_mean", ascending=False)
points.round(2).to_csv(TABLE_DIR / "04_provinces_mean.csv")

shared = points.groupby("cell").filter(lambda g: len(g) > 1).groupby("cell")["name"].agg(", ".join)
print(f"{len(points)} tỉnh/thành nhưng {points['cell'].nunique()} chuỗi CAMS khác nhau. Nhóm trùng ô lưới:")
print(shared.to_string())
print(points[["name", "region", "aqi_mean", "pm25_mean", "pct_hours_aqi_over_150"]].round(1).head(10).to_string())
print(points.groupby("region")["pm25_mean"].describe().round(1).to_string())

# %%
fig, axes = plt.subplots(1, 2, figsize=(14, 9), gridspec_kw={"width_ratios": [1, 1.3]})
sc = axes[0].scatter(points["lon"], points["lat"], c=points["pm25_mean"], cmap="YlOrRd",
                     s=70, edgecolor="black", linewidth=0.4)
for city in ["hanoi", "hcm", "danang", "haiphong", "cantho"]:
    axes[0].annotate(points.at[city, "name"], (points.at[city, "lon"], points.at[city, "lat"]), fontsize=8,
                     xytext=(5, 3), textcoords="offset points")
axes[0].set(xlabel="Kinh độ", ylabel="Vĩ độ", title="PM2.5 TB 2023-2025 (CAMS)", aspect="equal")
fig.colorbar(sc, ax=axes[0], shrink=0.6, label="µg/m³")

ranked = points.iloc[::-1]
axes[1].barh(ranked["name"], ranked["pm25_mean"], color=[REGION_COLORS[r] for r in ranked["region"]])
axes[1].tick_params(axis="y", labelsize=6)
axes[1].set(xlabel="PM2.5 TB (µg/m³)", title="Xếp hạng 63 tỉnh/thành")
handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in REGION_COLORS.values()]
axes[1].legend(handles, REGION_COLORS.keys(), loc="lower right")
plotting.save(fig, "04_spatial_map_ranking")

# %% [markdown]
# ## 2. Hà Nội và TP.HCM so với cả nước

# %%
rank = points.reset_index().reset_index(names="rank").set_index("city")
context = pd.DataFrame(
    {
        "rank_pm25_of_63": [int(rank.at[c, "rank"]) + 1 for c in ("hanoi", "hcm")],
        "pm25_mean": [points.at[c, "pm25_mean"] for c in ("hanoi", "hcm")],
        "national_median": points["pm25_mean"].median(),
    },
    index=["hanoi", "hcm"],
).round(1)
context.to_csv(TABLE_DIR / "04_hanoi_hcm_vs_country.csv")
print(context.to_string())

# %% [markdown]
# ## 3. Chu kỳ năm theo miền
#
# Miền Bắc ô nhiễm nặng vào mùa đông; miền Nam vào mùa khô (12-4); miền Trung lệch pha do
# mùa mưa vào thu-đông.

# %%
aq = aq.assign(region=aq["city"].map(points["region"]), month=aq["ts"].dt.month)
monthly = aq.groupby(["region", "month"])["cams_pm25"].mean().unstack(0)
monthly.round(1).to_csv(TABLE_DIR / "04_region_monthly.csv")

fig, ax = plt.subplots(figsize=(8, 4))
for region, color in REGION_COLORS.items():
    ax.plot(monthly.index, monthly[region], marker="o", color=color, label=region)
ax.set(xlabel="Tháng", ylabel="PM2.5 TB (µg/m³)", xticks=range(1, 13), title="Chu kỳ năm PM2.5 theo miền (CAMS)")
ax.legend()
plotting.save(fig, "04_region_monthly")

# %% [markdown]
# ## 4. Ô nhiễm có tính vùng: tương quan giảm theo khoảng cách
#
# Bỏ các cặp tỉnh nằm cùng ô lưới CAMS (tương quan = 1 do trùng chuỗi).

# %%
daily = aq.set_index("ts").groupby("city")["cams_pm25"].resample("D").mean().unstack(0)
corr = daily.corr()
names = list(corr.index)
pairs = pd.DataFrame([
    {
        "distance_km": haversine_km(points.at[a, "lat"], points.at[a, "lon"], points.at[b, "lat"], points.at[b, "lon"]),
        "corr": corr.at[a, b],
    }
    for i, a in enumerate(names)
    for b in names[i + 1:]
    if points.at[a, "cell"] != points.at[b, "cell"]
])
binned = pairs.groupby(pd.cut(pairs["distance_km"], range(0, 1801, 100)), observed=True)["corr"].median()
binned.round(3).to_csv(TABLE_DIR / "04_correlation_vs_distance.csv")

fig, ax = plt.subplots(figsize=(7, 4))
ax.scatter(pairs["distance_km"], pairs["corr"], s=4, alpha=0.3, color="#555555")
ax.plot([iv.mid for iv in binned.index], binned.to_numpy(), color="#C0392B", lw=2, label="Trung vị")
ax.set(xlabel="Khoảng cách giữa hai tỉnh (km)", ylabel="Tương quan PM2.5 ngày",
       title="Tương quan PM2.5 giữa các tỉnh theo khoảng cách")
ax.legend()
plotting.save(fig, "04_correlation_vs_distance")
print(binned.round(2).head(8).to_string())

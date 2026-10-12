"""US AQI categories (EPA). AQI values themselves are computed by role C."""

from __future__ import annotations

import numpy as np
import pandas as pd

AQI_CATEGORY_BINS = [-np.inf, 50, 100, 150, 200, 300, np.inf]
AQI_CATEGORY_LABELS = [
    "Tốt",
    "Trung bình",
    "Kém (nhóm nhạy cảm)",
    "Xấu",
    "Rất xấu",
    "Nguy hại",
]
AQI_CATEGORY_COLORS = ["#00E400", "#FFFF00", "#FF7E00", "#FF0000", "#8F3F97", "#7E0023"]  # EPA official


def aqi_category(aqi: np.ndarray | pd.Series) -> pd.Categorical:
    return pd.cut(np.asarray(aqi, dtype=float), bins=AQI_CATEGORY_BINS, labels=AQI_CATEGORY_LABELS)

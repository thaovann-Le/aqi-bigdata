"""Paths, cities, seasons and shared constants for role D (analysis + ML).

Input data is the processed output of role C (docs/data_contract.md), never the raw files:
    AQI_ML_DATASET  C's flat city-hour export (default: processing/_runtime/ml_dataset/city_hourly_parquet)
    AQI_OUTPUT_DIR  where caches, figures, tables and models are written (default: ml/outputs)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

ML_DATASET = Path(
    os.environ.get("AQI_ML_DATASET", REPO_ROOT / "processing" / "_runtime" / "ml_dataset" / "city_hourly_parquet")
)
OUTPUT_DIR = Path(os.environ.get("AQI_OUTPUT_DIR", REPO_ROOT / "ml" / "outputs"))
FIGURE_DIR = OUTPUT_DIR / "figures"
TABLE_DIR = OUTPUT_DIR / "tables"
MODEL_DIR = OUTPUT_DIR / "models"

# All timestamps are naive local time (UTC+7, no DST in Vietnam).
LOCAL_TZ = "Asia/Ho_Chi_Minh"

# Analysis, dashboard and model use these two cities; the other 61 provinces only have
# CAMS model values + weather and are used for volume and the national comparison.
CITY_NAMES = {"hanoi": "Hà Nội", "hcm": "TP.HCM"}
CITIES = tuple(CITY_NAMES)

# Same seasons as role C (processing/common/config.py) so report, dashboard and model agree.
SEASON_MONTHS: dict[str, dict[str, tuple[int, ...]]] = {
    "hanoi": {"Xuân": (2, 3, 4), "Hạ": (5, 6, 7), "Thu": (8, 9, 10), "Đông": (11, 12, 1)},
    "hcm": {"Mùa khô": (11, 12, 1, 2, 3, 4), "Mùa mưa": (5, 6, 7, 8, 9, 10)},
}
SEASON_ORDER = {city: list(seasons) for city, seasons in SEASON_MONTHS.items()}


def season_of(city: str, month: int) -> str:
    return next(name for name, months in SEASON_MONTHS[city].items() if month in months)


@dataclass(frozen=True)
class SplitDates:
    """Split on the target time. Test is the last period; validation (early stopping) is a
    window *inside* the training era, chosen in the polluted season so that early stopping
    does not tune the model on clean summer hours. Train = [train_start, test_start) minus valid."""

    train_start: str
    valid_start: str
    valid_end: str
    test_start: str
    test_end: str


# The city-hour "measured" series is the mean of whatever stations report that hour, and the
# station mix changed: Hanoi went from 1 US-embassy monitor (2021-2023) to the city network
# (2024+, ~4-12 stations); HCMC only has the CMT8 low-cost sensor from 2024-11-19. Real-time
# data comes from the same kind of stations as 2024+, so the model is trained on that era only.
# Historical weather/CAMS end on 2025-12-31, hence the test period ends there.
# Validation = 01-03/2025: Hanoi winter / HCMC dry season, the hours where forecasts matter.
SPLITS: dict[str, SplitDates] = {
    "hanoi": SplitDates("2024-01-01", "2025-01-01", "2025-04-01", "2025-10-01", "2026-01-01"),
    "hcm": SplitDates("2024-11-19", "2025-01-01", "2025-04-01", "2025-10-01", "2026-01-01"),
}

# Open-Meteo grid points used by role B for the historical weather (ingestion/points_vn.json);
# serving requests the next-day weather forecast at the same coordinates.
CITY_COORDS = {"hanoi": (21.0245, 105.8412), "hcm": (10.823, 106.6296)}

# Alert threshold shared with role C: US AQI > 150 ("Unhealthy").
ALERT_AQI = 150


def ensure_output_dirs() -> None:
    for path in (FIGURE_DIR, TABLE_DIR, MODEL_DIR):
        path.mkdir(parents=True, exist_ok=True)

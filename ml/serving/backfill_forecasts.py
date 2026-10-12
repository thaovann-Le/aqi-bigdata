"""Write the test-period predictions from training into Cassandra ``forecasts``.

Real-time data only covers a few days, so the Grafana "forecast vs actual" panel would be
almost empty. This backfills the test period (10-12/2025) with the predictions made by
ml/training/train_xgboost.py; the matching actual values are already in ``daily_agg``.

Usage (from the repo root, after training):
    python ml/serving/backfill_forecasts.py --dry-run
    python ml/serving/backfill_forecasts.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # make `aqiml` importable

import pandas as pd  # noqa: E402

from aqiml.config import TABLE_DIR  # noqa: E402
from aqiml.features import DEFAULT_FEATURE_SET, FEATURE_SETS, HORIZON_HOURS  # noqa: E402
from aqiml.storage import ForecastRow, connect, write_forecasts  # noqa: E402

logger = logging.getLogger("backfill_forecasts")


def load_rows(feature_set: str) -> list[ForecastRow]:
    path = TABLE_DIR / "test_predictions.parquet"
    if not path.exists():
        raise SystemExit(f"{path} not found - run ml/training/train_xgboost.py first")
    preds = pd.read_parquet(path)
    preds = preds[preds["feature_set"] == feature_set]
    # Test-period predictions used the *observed* next-day weather (a perfect forecast), so they
    # are labelled apart from live forecasts, which use the real weather forecast.
    model_name = f"xgboost_{feature_set}_h{HORIZON_HOURS}_backtest"
    return [
        ForecastRow(city=r.city, forecast_for=r.target_ts, generated_at=r.issued_at,
                    predicted_aqi=round(float(r.xgboost), 1), model_name=model_name, horizon_hours=HORIZON_HOURS)
        for r in preds.itertuples()
    ]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--feature-set", default=DEFAULT_FEATURE_SET, choices=list(FEATURE_SETS))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    rows = load_rows(args.feature_set)
    summary = pd.DataFrame(rows).groupby("city")["forecast_for"].agg(["size", "min", "max"])
    logger.info("Forecast rows to write:\n%s", summary.to_string())
    if not args.dry_run:
        logger.info("Wrote %d rows", write_forecasts(connect(), rows))


if __name__ == "__main__":
    main()

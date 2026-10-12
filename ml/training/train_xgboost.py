"""Train and evaluate the +24h city AQI XGBoost forecaster on role C's processed data.

For every city x feature set (base / weather_forecast) it:
  1. builds features (ml/aqiml/features.py) and splits chronologically (SPLITS in config.py),
  2. trains XGBoost with early stopping on the validation period,
  3. scores the test period against the persistence baseline ("tomorrow = today"),
  4. saves model + metadata, test predictions, metrics tables and figures.

Usage (from the repo root):
    python ml/training/train_xgboost.py
    python ml/training/train_xgboost.py --cities hanoi --feature-sets weather_forecast
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # make `aqiml` importable

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from aqiml import plotting  # noqa: E402
from aqiml.config import ALERT_AQI, CITIES, MODEL_DIR, SPLITS, TABLE_DIR, ensure_output_dirs, season_of  # noqa: E402
from aqiml.data import load_city_hourly  # noqa: E402
from aqiml.features import FEATURE_SETS, HORIZON_HOURS, feature_columns, make_features  # noqa: E402
from aqiml.modeling import (  # noqa: E402
    ModelMeta,
    evaluate,
    predict_aqi,
    save_model,
    time_split,
    train_xgboost,
    usable_rows,
)

logger = logging.getLogger("train_xgboost")
KEYS = ["city", "feature_set"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cities", nargs="+", default=list(CITIES), choices=list(CITIES))
    parser.add_argument("--feature-sets", nargs="+", default=list(FEATURE_SETS), choices=list(FEATURE_SETS))
    parser.add_argument("--horizon", type=int, default=HORIZON_HOURS)
    return parser.parse_args()


def upsert_table(new: pd.DataFrame, path: Path, keys: list[str]) -> pd.DataFrame:
    """Replace rows for the (city, feature_set) pairs just trained and keep the others,
    so training one city does not erase the other city's results."""
    if path.exists():
        old = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        old = old.merge(new[keys].drop_duplicates(), on=keys, how="left", indicator=True)
        new = pd.concat([old[old["_merge"] == "left_only"].drop(columns="_merge"), new], ignore_index=True)
    if path.suffix == ".parquet":
        new.to_parquet(path, index=False)
    else:
        new.to_csv(path, index=False)
    return new


def season_metrics(city: str, test: pd.DataFrame, y_pred: np.ndarray) -> pd.DataFrame:
    seasons = test["target_ts"].dt.month.map(lambda m: season_of(city, m)).to_numpy()
    y, persistence = test["target"].to_numpy(), test["persistence"].to_numpy()
    rows = [
        {
            "city": city,
            "season": season,
            "n": int(mask.sum()),
            "mae_xgboost": float(np.mean(np.abs(y_pred[mask] - y[mask]))),
            "mae_persistence": float(np.mean(np.abs(persistence[mask] - y[mask]))),
        }
        for season in pd.unique(seasons)
        for mask in [seasons == season]
    ]
    return pd.DataFrame(rows)


def plot_test_period(city: str, feature_set: str, preds: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    for col, label in [("actual", "Thực tế"), ("persistence", "Persistence (t)"), ("xgboost", "XGBoost")]:
        ax.plot(preds["target_ts"], preds[col], label=label, color=plotting.MODEL_COLORS[col],
                lw=1.6 if col == "xgboost" else 1.0, alpha=0.6 if col == "persistence" else 1.0)
    ax.axhline(ALERT_AQI, ls="--", color="#8E44AD", lw=1, label=f"Ngưỡng cảnh báo AQI {ALERT_AQI}")
    ax.set(title=f"{plotting.CITY_LABELS[city]} - dự báo AQI +24h ({feature_set}), giai đoạn test", ylabel="US AQI")
    ax.legend(ncol=4, loc="upper left")
    plotting.save(fig, f"forecast_{city}_{feature_set}_timeseries")

    fig, ax = plt.subplots(figsize=(5, 5))
    lim = float(np.nanpercentile(preds[["actual", "xgboost"]].to_numpy(), 99.5))
    ax.scatter(preds["actual"], preds["xgboost"], s=4, alpha=0.3, color=plotting.CITY_COLORS[city])
    ax.plot([0, lim], [0, lim], color="black", lw=1)
    ax.set(xlim=(0, lim), ylim=(0, lim), xlabel="AQI thực tế", ylabel="AQI dự báo",
           title=f"{plotting.CITY_LABELS[city]} - dự báo vs thực tế")
    plotting.save(fig, f"forecast_{city}_{feature_set}_scatter")


def plot_importance(city: str, feature_set: str, model, cols: list[str]) -> pd.DataFrame:
    gain = model.get_booster().get_score(importance_type="gain")
    imp = pd.DataFrame({"feature": cols, "gain": [gain.get(c, 0.0) for c in cols]})
    imp = imp.assign(gain_share=imp["gain"] / imp["gain"].sum()).sort_values("gain_share", ascending=False)

    top = imp.head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.barh(top["feature"], top["gain_share"], color=plotting.CITY_COLORS[city])
    ax.set(xlabel="Tỷ trọng gain", title=f"{plotting.CITY_LABELS[city]} - độ quan trọng đặc trưng ({feature_set})")
    plotting.save(fig, f"importance_{city}_{feature_set}")
    return imp.assign(city=city, feature_set=feature_set)


def run_one(city: str, feature_set: str, horizon: int) -> tuple[list[dict], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    features = usable_rows(make_features(load_city_hourly(city), FEATURE_SETS[feature_set], horizon))
    split = time_split(features, SPLITS[city])
    cols = feature_columns(features)
    logger.info("%s/%s: train=%d valid=%d test=%d features=%d", city, feature_set,
                len(split.train), len(split.valid), len(split.test), len(cols))
    if min(len(split.train), len(split.valid), len(split.test)) == 0:
        raise ValueError(f"Empty split for {city}: check SPLITS in config.py against the data coverage")

    model = train_xgboost(split, cols)
    y_pred = predict_aqi(model, split.test, cols)
    y_true = split.test["target"].to_numpy()
    persistence = split.test["persistence"].to_numpy()
    key = {"city": city, "feature_set": feature_set}
    model_metrics = evaluate(y_true, y_pred)
    baseline_metrics = evaluate(y_true, persistence)
    rows = [{**key, "model": "xgboost", **model_metrics}, {**key, "model": "persistence", **baseline_metrics}]

    meta = ModelMeta(
        city=city,
        feature_set=feature_set,
        horizon_hours=horizon,
        features=cols,
        best_iteration=int(model.best_iteration),
        split=vars(SPLITS[city]),
        valid_metrics=evaluate(split.valid["target"].to_numpy(), predict_aqi(model, split.valid, cols)),
        test_metrics=model_metrics,
        baseline_test_metrics=baseline_metrics,
        trained_at=datetime.now().isoformat(timespec="seconds"),
    )
    save_model(model, meta, MODEL_DIR)

    preds = pd.DataFrame({
        "issued_at": split.test.index,
        "target_ts": split.test["target_ts"].to_numpy(),
        "actual": y_true,
        "persistence": persistence,
        "xgboost": y_pred,
    }).assign(**key)
    plot_test_period(city, feature_set, preds)
    importance = plot_importance(city, feature_set, model, cols)
    seasons = season_metrics(city, split.test, y_pred).assign(feature_set=feature_set)
    return rows, preds, seasons, importance


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    ensure_output_dirs()

    metric_rows, preds, seasons, importances = [], [], [], []
    for city in args.cities:
        for fs in args.feature_sets:
            rows, p, s, imp = run_one(city, fs, args.horizon)
            metric_rows += rows
            preds.append(p)
            seasons.append(s)
            importances.append(imp)

    metrics = upsert_table(pd.DataFrame(metric_rows), TABLE_DIR / "forecast_metrics.csv", KEYS)
    upsert_table(pd.concat(seasons), TABLE_DIR / "forecast_metrics_by_season.csv", KEYS)
    upsert_table(pd.concat(importances), TABLE_DIR / "feature_importance.csv", KEYS)
    upsert_table(pd.concat(preds), TABLE_DIR / "test_predictions.parquet", KEYS)

    cols = ["city", "feature_set", "model", "n", "mae", "rmse", "r2", "bias", "alert_events", "alert_precision",
            "alert_recall"]
    logger.info("Test metrics:\n%s", metrics[cols].round(3).to_string(index=False))


if __name__ == "__main__":
    main()

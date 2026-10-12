"""Runs the training script's run_one/main on a synthetic city, writing only into tmp_path."""

import numpy as np
import pandas as pd
import pytest
from helpers import hourly_frame, load_script

from aqiml import modeling, plotting
from aqiml.config import SplitDates

train = load_script("train_xgboost", "training/train_xgboost.py")

DATES = SplitDates("2024-12-01", "2024-12-20", "2024-12-27", "2025-01-05", "2025-01-20")
SMALL_XGB = {"n_estimators": 30, "learning_rate": 0.1, "max_depth": 3, "early_stopping_rounds": 5,
             "eval_metric": "mae", "tree_method": "hist", "n_jobs": 1, "random_state": 0}


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Point every output of the training script at tmp_path and feed it synthetic data."""
    frame = hourly_frame(24 * 50, start="2024-12-01 00:00")
    frame = frame.assign(aqi=100 + 40 * np.sin(np.arange(len(frame)) / 12))
    monkeypatch.setattr(train, "load_city_hourly", lambda city: frame)
    monkeypatch.setattr(train, "SPLITS", {"hanoi": DATES})
    monkeypatch.setattr(train, "MODEL_DIR", tmp_path / "models")
    monkeypatch.setattr(train, "TABLE_DIR", tmp_path / "tables")
    monkeypatch.setattr(plotting, "FIGURE_DIR", tmp_path / "figures")
    monkeypatch.setattr(modeling, "XGB_PARAMS", SMALL_XGB)
    for sub in ("models", "tables", "figures"):
        (tmp_path / sub).mkdir()
    return tmp_path


def test_run_one_scores_model_against_persistence_and_saves_artifacts(sandbox):
    rows, preds, seasons, importance = train.run_one("hanoi", "weather_forecast", 24)

    assert {r["model"] for r in rows} == {"xgboost", "persistence"}
    by_model = {r["model"]: r for r in rows}
    assert by_model["xgboost"]["n"] == by_model["persistence"]["n"] == len(preds)  # same hours for both
    assert by_model["xgboost"]["mae"] < by_model["persistence"]["mae"]  # a smooth signal is easy to forecast
    assert preds["target_ts"].min() >= pd.Timestamp(DATES.test_start)
    assert (sandbox / "models" / "xgb_aqi_hanoi_weather_forecast_h24.json").exists()
    assert (sandbox / "figures" / "forecast_hanoi_weather_forecast_timeseries.png").exists()
    assert importance["gain_share"].sum() == pytest.approx(1.0)
    assert set(seasons["season"]) <= {"Xuân", "Hạ", "Thu", "Đông"}


def test_main_writes_metric_tables_and_keeps_other_results(sandbox, monkeypatch):
    monkeypatch.setattr("sys.argv", ["train_xgboost.py", "--cities", "hanoi", "--feature-sets", "base"])
    train.main()
    monkeypatch.setattr("sys.argv", ["train_xgboost.py", "--cities", "hanoi", "--feature-sets", "weather_forecast"])
    train.main()

    metrics = pd.read_csv(sandbox / "tables" / "forecast_metrics.csv")
    assert set(metrics["feature_set"]) == {"base", "weather_forecast"}  # second run did not erase the first
    assert len(metrics) == 4
    preds = pd.read_parquet(sandbox / "tables" / "test_predictions.parquet")
    assert set(preds["feature_set"]) == {"base", "weather_forecast"}


def test_run_one_rejects_empty_split(sandbox, monkeypatch):
    monkeypatch.setattr(train, "SPLITS", {"hanoi": SplitDates("2030-01-01", "2030-02-01", "2030-03-01", "2030-04-01", "2030-05-01")})
    with pytest.raises(ValueError, match="Empty split"):
        train.run_one("hanoi", "base", 24)

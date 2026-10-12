"""End-to-end checks on synthetic data: train -> save -> load -> forecast -> backfill rows."""

import numpy as np
import pandas as pd
import pytest
from helpers import hourly_frame, load_script

from aqiml.aqi import AQI_CATEGORY_LABELS, aqi_category
from aqiml.config import SEASON_MONTHS, SplitDates, season_of
from aqiml.data import WEATHER_COLUMNS
from aqiml.features import FEATURE_SETS, HORIZON_HOURS, feature_columns, make_features
from aqiml.modeling import (
    ModelMeta,
    evaluate,
    load_model,
    predict_aqi,
    regression_metrics,
    save_model,
    time_split,
    train_xgboost,
    usable_rows,
)

predict = load_script("predict", "serving/predict.py")
train = load_script("train_xgboost", "training/train_xgboost.py")
backfill = load_script("backfill_forecasts", "serving/backfill_forecasts.py")

DATES = SplitDates("2024-12-01", "2024-12-20", "2024-12-27", "2025-01-05", "2025-01-20")
SMALL_XGB = {"n_estimators": 30, "learning_rate": 0.1, "max_depth": 3, "early_stopping_rounds": 5,
             "eval_metric": "mae", "tree_method": "hist", "n_jobs": 1, "random_state": 0}


def synthetic_city(hours: int = 24 * 50) -> pd.DataFrame:
    frame = hourly_frame(hours, start="2024-12-01 00:00")
    return frame.assign(aqi=100 + 40 * np.sin(np.arange(hours) / 12))


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    model_dir = tmp_path_factory.mktemp("models")
    feats = usable_rows(make_features(synthetic_city(), FEATURE_SETS["weather_forecast"]))
    split = time_split(feats, DATES)
    cols = feature_columns(feats)
    model = train_xgboost(split, cols, SMALL_XGB)
    meta = ModelMeta(city="hanoi", feature_set="weather_forecast", horizon_hours=HORIZON_HOURS, features=cols,
                     best_iteration=int(model.best_iteration), split=vars(DATES), valid_metrics={},
                     test_metrics={}, baseline_test_metrics={}, trained_at="test")
    save_model(model, meta, model_dir)
    return {"model": model, "split": split, "cols": cols, "dir": model_dir}


def test_saved_model_reloads_with_identical_predictions(trained):
    reloaded, meta = load_model(trained["dir"], "hanoi", "weather_forecast", HORIZON_HOURS)
    expected = predict_aqi(trained["model"], trained["split"].test, trained["cols"])
    np.testing.assert_allclose(predict_aqi(reloaded, trained["split"].test, meta.features), expected, rtol=1e-6)
    assert meta.model_name == "xgboost_weather_forecast_h24"
    assert len(trained["split"].test) > 0 and len(trained["split"].valid) > 0


def test_load_model_missing_file_points_to_training_script(tmp_path):
    with pytest.raises(FileNotFoundError, match="train_xgboost.py"):
        load_model(tmp_path, "hanoi", "base", HORIZON_HOURS)


def test_forecast_city_end_to_end_with_trained_model(trained, monkeypatch):
    monkeypatch.setattr(predict, "MODEL_DIR", trained["dir"])
    full = synthetic_city()
    history = full.iloc[100:172]
    api_weather = full[WEATHER_COLUMNS].iloc[100: 172 + HORIZON_HOURS]
    now = history.index[-1] + pd.Timedelta(hours=1)

    row = predict.forecast_city("hanoi", history, now, "weather_forecast", api_weather)

    assert row.model_name == "xgboost_weather_forecast_h24"
    assert 0 <= row.predicted_aqi <= 500
    assert row.generated_at == history.index[-1]
    assert row.forecast_for == history.index[-1] + pd.Timedelta(hours=HORIZON_HOURS)


def test_forecast_city_skips_stale_city(trained, monkeypatch):
    monkeypatch.setattr(predict, "MODEL_DIR", trained["dir"])
    history = synthetic_city().iloc[100:172]
    stale_now = history.index[-1] + pd.Timedelta(hours=10)
    assert predict.forecast_city("hanoi", history, stale_now, "weather_forecast", None) is None


def test_backfill_rows_are_labelled_as_backtest(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "TABLE_DIR", tmp_path)
    pd.DataFrame({
        "city": ["hanoi"], "feature_set": ["weather_forecast"],
        "issued_at": [pd.Timestamp("2025-12-10 08:00")], "target_ts": [pd.Timestamp("2025-12-11 08:00")],
        "actual": [200.0], "persistence": [150.0], "xgboost": [123.456],
    }).to_parquet(tmp_path / "test_predictions.parquet")

    (row,) = backfill.load_rows("weather_forecast")

    assert row.model_name == "xgboost_weather_forecast_h24_backtest"
    assert row.predicted_aqi == 123.5 and row.forecast_for == pd.Timestamp("2025-12-11 08:00")


def test_backfill_requires_trained_predictions(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "TABLE_DIR", tmp_path)
    with pytest.raises(SystemExit):
        backfill.load_rows("weather_forecast")


def test_season_metrics_compare_model_with_persistence_per_season():
    test = pd.DataFrame({
        "target_ts": pd.to_datetime(["2025-11-05 00:00", "2025-11-06 00:00", "2025-10-05 00:00"]),
        "target": [100.0, 100.0, 100.0],
        "persistence": [110.0, 130.0, 100.0],
    })
    out = train.season_metrics("hanoi", test, np.array([100.0, 120.0, 90.0])).set_index("season")
    assert out.loc["Đông", "n"] == 2 and out.loc["Đông", "mae_xgboost"] == 10.0
    assert out.loc["Đông", "mae_persistence"] == 20.0
    assert out.loc["Thu", "mae_xgboost"] == 10.0 and out.loc["Thu", "mae_persistence"] == 0.0


def test_evaluate_reports_regression_and_alert_metrics():
    m = evaluate(np.array([100.0, 200.0]), np.array([110.0, 190.0]))
    assert m["mae"] == 10.0 and m["rmse"] == 10.0 and m["bias"] == 0.0
    assert m["r2"] == pytest.approx(0.96) and m["alert_events"] == 1
    assert regression_metrics(np.array([5.0, 5.0]), np.array([5.0, 5.0]))["r2"] != regression_metrics(
        np.array([5.0, 6.0]), np.array([5.0, 6.0]))["r2"]


def test_season_of_matches_role_c_definition():
    assert [season_of("hanoi", m) for m in (1, 2, 5, 8, 11, 12)] == ["Đông", "Xuân", "Hạ", "Thu", "Đông", "Đông"]
    assert season_of("hcm", 11) == "Mùa khô" and season_of("hcm", 5) == "Mùa mưa"
    for months in SEASON_MONTHS.values():
        assert sorted(m for season in months.values() for m in season) == list(range(1, 13))  # every month once


def test_aqi_category_boundaries():
    cats = aqi_category(np.array([0, 50, 51, 100, 150, 151, 300, 301]))
    assert list(cats) == [AQI_CATEGORY_LABELS[i] for i in (0, 0, 1, 1, 2, 3, 4, 5)]

from datetime import UTC

import numpy as np
import pandas as pd
from helpers import hourly_frame, load_script

from aqiml.data import WEATHER_COLUMNS
from aqiml.features import make_features
from aqiml.storage import from_driver, to_aware

predict = load_script("predict", "serving/predict.py")
train = load_script("train_xgboost", "training/train_xgboost.py")


def test_local_time_survives_cassandra_round_trip():
    local = pd.Timestamp("2025-01-10 08:00")
    returned_by_driver = to_aware(local).astimezone(UTC).replace(tzinfo=None)
    assert returned_by_driver == pd.Timestamp("2025-01-10 01:00")
    assert from_driver(pd.Series([returned_by_driver])).iloc[0] == local


def test_api_weather_replaces_stored_snapshots_and_extends_to_target_hour():
    history = hourly_frame(48)
    api = pd.DataFrame(-5.0, columns=WEATHER_COLUMNS, index=pd.date_range(history.index[2], periods=94, freq="h"))
    out = predict.with_api_weather(history, api)
    assert len(out) == 48 + 24
    assert (out["temperature"].iloc[2:] == -5.0).all()  # API hourly values win, past and future
    assert (out["temperature"].iloc[:2] == history["temperature"].iloc[:2]).all()  # stored fills API gaps
    assert out["aqi"].iloc[48:].isna().all() and (out["aqi"].iloc[:48] == history["aqi"]).all()


def test_latest_issue_time_rejects_stale_data():
    history = hourly_frame(48)
    last = history.index[-1]
    assert predict.latest_issue_time(history, last + pd.Timedelta(hours=1)) == last
    assert predict.latest_issue_time(history, last + pd.Timedelta(hours=5)) is None


def test_forecast_falls_back_to_base_model_without_weather_forecast(monkeypatch):
    used = {}

    class Meta:
        features = ["aqi_lag0"]
        model_name = "xgboost_base_h24"

    class Model:
        def predict(self, x):
            return np.zeros(len(x))

    def fake_load_model(model_dir, city, feature_set, horizon):
        used["feature_set"] = feature_set
        return Model(), Meta()

    def fake_make_features(hourly, feature_set, horizon):
        used["precipitation"] = hourly["precipitation"]
        return make_features(hourly, feature_set, horizon)

    monkeypatch.setattr(predict, "load_model", fake_load_model)
    monkeypatch.setattr(predict, "make_features", fake_make_features)
    history = hourly_frame(48)
    row = predict.forecast_city("hanoi", history, history.index[-1], "weather_forecast", None)
    assert used["feature_set"] == "base"
    assert used["precipitation"].isna().all()  # snapshot precipitation is not fed to the model
    assert row.predicted_aqi == history["aqi"].iloc[-1]  # zero change -> persistence
    assert row.forecast_for == history.index[-1] + pd.Timedelta(hours=24)


def test_upsert_table_keeps_other_cities(tmp_path):
    path = tmp_path / "metrics.csv"
    keys = ["city", "feature_set"]
    first = pd.DataFrame({"city": ["hanoi", "hcm"], "feature_set": ["base"] * 2, "mae": [1.0, 2.0]})
    train.upsert_table(first, path, keys)
    out = train.upsert_table(pd.DataFrame({"city": ["hanoi"], "feature_set": ["base"], "mae": [9.0]}), path, keys)
    assert sorted(zip(out["city"], out["mae"])) == [("hanoi", 9.0), ("hcm", 2.0)]

import numpy as np
import pandas as pd
from helpers import hourly_frame

from aqiml.config import SplitDates
from aqiml.features import make_features
from aqiml.modeling import alert_metrics, predict_aqi, time_split, usable_rows


def test_time_split_keeps_validation_inside_training_era_and_test_last():
    feats = usable_rows(make_features(hourly_frame(24 * 30, start="2024-12-01")))
    split = time_split(feats, SplitDates("2024-12-01", "2024-12-10", "2024-12-15", "2024-12-25", "2024-12-29"))
    t_train = split.train["target_ts"]
    assert t_train.min() >= pd.Timestamp("2024-12-01") and t_train.max() < pd.Timestamp("2024-12-25")
    assert not ((t_train >= pd.Timestamp("2024-12-10")) & (t_train < pd.Timestamp("2024-12-15"))).any()
    assert split.valid["target_ts"].between(pd.Timestamp("2024-12-10"), pd.Timestamp("2024-12-14 23:00")).all()
    assert split.test["target_ts"].between(pd.Timestamp("2024-12-25"), pd.Timestamp("2024-12-28 23:00")).all()
    # purge: no training target in the 48h after validation (its inputs would include validation AQI)
    assert not ((t_train >= pd.Timestamp("2024-12-15")) & (t_train < pd.Timestamp("2024-12-17"))).any()
    assert (t_train >= pd.Timestamp("2024-12-17")).any()


class ConstantChange:
    def __init__(self, change: float):
        self.change = change

    def predict(self, x):
        return np.full(len(x), self.change)


def test_predict_aqi_adds_predicted_change_to_current_value_and_clips():
    frame = pd.DataFrame({"persistence": [100.0, 480.0, 5.0], "f": [0, 0, 0]})
    assert predict_aqi(ConstantChange(30.0), frame, ["f"]).tolist() == [130.0, 500.0, 35.0]
    assert predict_aqi(ConstantChange(-30.0), frame, ["f"]).tolist() == [70.0, 450.0, 0.0]


def test_alert_metrics_use_strictly_greater_than_threshold():
    y_true = np.array([150.0, 160.0, 170.0, 20.0])
    y_pred = np.array([155.0, 151.0, 120.0, 10.0])
    m = alert_metrics(y_true, y_pred, threshold=150)
    assert m["alert_events"] == 2
    assert m["alert_precision"] == 0.5 and m["alert_recall"] == 0.5

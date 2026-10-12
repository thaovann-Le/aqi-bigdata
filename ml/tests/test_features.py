import numpy as np
import pandas as pd
import pytest
from helpers import hourly_frame

from aqiml.features import FEATURE_SETS, feature_columns, make_features


def test_target_is_future_aqi_and_lags_are_past_values():
    feats = make_features(hourly_frame(), FEATURE_SETS["base"], horizon=24)
    row = feats.iloc[30]  # aqi equals the row number
    assert row["target"] == 54 and row["persistence"] == 30
    assert row["aqi_lag0"] == 30 and row["aqi_lag1"] == 29 and row["aqi_lag24"] == 6
    assert row["target_ts"] == feats.index[30] + pd.Timedelta(hours=24)


@pytest.mark.parametrize("feature_set", ["base", "weather_forecast"])
def test_no_feature_uses_future_aqi(feature_set):
    hourly = hourly_frame()
    changed = hourly.copy()
    changed.iloc[31:, changed.columns.get_loc("aqi")] = -1.0
    before, after = (make_features(h, FEATURE_SETS[feature_set]) for h in (hourly, changed))
    cols = feature_columns(before)
    pd.testing.assert_series_equal(before.iloc[30][cols], after.iloc[30][cols])


def test_only_fut_columns_look_ahead_and_only_in_weather_forecast_set():
    hourly = hourly_frame()
    assert not any(c.endswith("_fut") for c in feature_columns(make_features(hourly, FEATURE_SETS["base"])))

    fc = make_features(hourly, FEATURE_SETS["weather_forecast"])
    assert fc.iloc[0]["temperature_fut"] == hourly.iloc[24]["temperature"]
    later_weather = hourly.copy()
    later_weather.iloc[31:, later_weather.columns.get_loc("temperature")] = -99.0
    changed = make_features(later_weather, FEATURE_SETS["weather_forecast"])
    non_fut = [c for c in feature_columns(fc) if not c.endswith("_fut")]
    pd.testing.assert_series_equal(fc.iloc[30][non_fut], changed.iloc[30][non_fut])


def test_wind_components_follow_meteorological_convention():
    hourly = hourly_frame().assign(wind_speed=2.0, wind_direction=0.0)  # wind from the north
    row = make_features(hourly, FEATURE_SETS["base"]).iloc[0]
    assert np.isclose(row["wind_u"], 0.0) and np.isclose(row["wind_v"], -2.0)


def test_gap_in_index_is_rejected():
    hourly = hourly_frame()
    with pytest.raises(ValueError):
        make_features(hourly.drop(index=hourly.index[5]))

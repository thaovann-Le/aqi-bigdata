import pandas as pd
import pytest
from helpers import export_rows

from aqiml.data import current_network_era, load_dataset, to_hourly


@pytest.fixture
def export_dir(tmp_path):
    rows = pd.concat([
        export_rows("hanoi", ["2024-01-01 00:00", "2024-01-01 03:00"], [50.0, 80.0]),
        export_rows("hcm", ["2024-01-01 00:00"], [40.0]),
        export_rows("danang", ["2024-01-01 00:00"], [float("nan")]),
    ])
    path = tmp_path / "city_hourly_parquet"
    path.mkdir()
    rows.to_parquet(path / "part-0.parquet", index=False)
    return path


def test_load_dataset_converts_utc_to_local_time_and_renames_model_columns(export_dir):
    df = load_dataset(["hanoi"], export_dir)
    assert df["ts"].tolist() == [pd.Timestamp("2024-01-01 00:00"), pd.Timestamp("2024-01-01 03:00")]
    assert {"cams_aqi", "cams_pm25", "cams_pm10"} <= set(df.columns)
    assert "aqi_model" not in df.columns


def test_load_dataset_reads_c_csv_export(tmp_path):
    path = tmp_path / "city_hourly_csv"
    path.mkdir()
    export_rows("hcm", ["2025-03-01 07:00"], [99.0]).to_csv(path / "part-00000-x.csv", index=False)
    df = load_dataset(None, path)
    assert df.loc[0, "ts"] == pd.Timestamp("2025-03-01 07:00") and df.loc[0, "aqi"] == 99.0


def test_load_dataset_without_filter_returns_all_points(export_dir):
    assert set(load_dataset(None, export_dir)["city"]) == {"hanoi", "hcm", "danang"}


def test_missing_export_gives_actionable_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="docker-compose.demo.yml"):
        load_dataset(None, tmp_path / "nope")


def test_to_hourly_fills_missing_hours(export_dir):
    hourly = to_hourly(load_dataset(None, export_dir), "hanoi")
    assert len(hourly) == 4 and hourly["aqi"].isna().sum() == 2


def test_current_network_era_uses_city_specific_start():
    long = pd.DataFrame({
        "city": ["hanoi", "hanoi", "hcm", "hcm", "danang"],
        "ts": pd.to_datetime(["2023-12-31", "2024-01-02", "2024-11-01", "2024-11-20", "2025-01-01"]),
    })
    era = current_network_era(long)
    kept = list(zip(era["city"], era["ts"].dt.strftime("%Y-%m-%d")))
    assert kept == [("hanoi", "2024-01-02"), ("hcm", "2024-11-20")]

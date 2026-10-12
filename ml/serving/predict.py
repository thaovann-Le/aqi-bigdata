"""Hourly +24h city AQI forecast -> Cassandra ``forecasts`` (read by role A's Grafana panel).

Inputs per city:
  - last 72h of city-hour AQI + weather: Cassandra ``daily_agg`` / ``weather_hourly`` written by
    role C (--source cassandra), or C's export file for an offline replay (--source dataset);
  - weather from the Open-Meteo forecast API, hourly values for the past 72h and the next 24h.
    Role C's ``weather_hourly`` holds real-time *snapshots* of Open-Meteo "current" (precipitation
    = last 15 minutes), while training used hourly values (precipitation = hourly sum), so the API
    is preferred for every hour and Cassandra weather only fills hours the API lacks. If the API
    fails, the ``base`` model runs on Cassandra weather without precipitation (see
    ``drop_snapshot_precipitation``).

Usage (from the repo root):
    python ml/serving/predict.py                       # once; schedule hourly (C refreshes daily_agg every 15 min)
    python ml/serving/predict.py --every-minutes 60    # keep running
    python ml/serving/predict.py --source dataset --as-of "2025-12-10 08:00" --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # make `aqiml` importable

import pandas as pd  # noqa: E402
import requests  # noqa: E402

from aqiml.config import CITIES, CITY_COORDS, LOCAL_TZ, MODEL_DIR  # noqa: E402
from aqiml.data import WEATHER_COLUMNS, load_city_hourly  # noqa: E402
from aqiml.features import (  # noqa: E402
    DEFAULT_FEATURE_SET,
    FALLBACK_FEATURE_SET,
    FEATURE_SETS,
    HORIZON_HOURS,
    make_features,
)
from aqiml.modeling import load_model, predict_aqi  # noqa: E402
from aqiml.storage import ForecastRow, connect, read_city_hourly, write_forecasts  # noqa: E402

logger = logging.getLogger("predict")

HISTORY_HOURS = 72  # covers the 24h lags and rolling windows with margin
MAX_STALENESS_HOURS = 3  # skip a city if its latest measured AQI is older than this
HTTP_RETRIES = 3
HTTP_TIMEOUT_S = 20
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
# Open-Meteo variable -> our column (same variables role B downloads for the history).
OPENMETEO_WEATHER = {
    "temperature_2m": "temperature",
    "relative_humidity_2m": "humidity",
    "wind_speed_10m": "wind_speed",
    "wind_direction_10m": "wind_direction",
    "precipitation": "precipitation",
    "surface_pressure": "pressure",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cities", nargs="+", default=list(CITIES), choices=list(CITIES))
    parser.add_argument("--feature-set", default=DEFAULT_FEATURE_SET, choices=list(FEATURE_SETS))
    parser.add_argument("--source", default="cassandra", choices=["cassandra", "dataset"])
    parser.add_argument("--as-of", help="issue time for --source dataset, e.g. '2025-12-10 08:00'")
    parser.add_argument("--dry-run", action="store_true", help="print forecasts instead of writing them")
    parser.add_argument("--every-minutes", type=int, help="loop forever with this period")
    return parser.parse_args()


def fetch_weather_forecast(city: str) -> pd.DataFrame:
    """Hourly weather for the last 4 days .. tomorrow, same units as role C (wind in m/s)."""
    lat, lon = CITY_COORDS[city]
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(OPENMETEO_WEATHER),
        "wind_speed_unit": "ms",
        "timezone": "Asia/Bangkok",  # UTC+7, same as Vietnam
        "past_days": 4,  # covers the 72h history window
        "forecast_days": 2,
    }
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            response = requests.get(FORECAST_URL, params=params, timeout=HTTP_TIMEOUT_S)
            response.raise_for_status()
            hourly = pd.DataFrame(response.json()["hourly"])
            return hourly.assign(ts=pd.to_datetime(hourly["time"])).set_index("ts").rename(
                columns=OPENMETEO_WEATHER)[WEATHER_COLUMNS]
        except (requests.RequestException, KeyError, ValueError) as exc:
            logger.warning("Weather forecast for %s failed (%d/%d): %s", city, attempt, HTTP_RETRIES, exc)
            if attempt < HTTP_RETRIES:
                time.sleep(2**attempt)
    raise RuntimeError(f"Open-Meteo forecast unavailable for {city}")


def with_api_weather(history: pd.DataFrame, api_weather: pd.DataFrame) -> pd.DataFrame:
    """Extend the history to t+24; weather of every hour comes from the hourly API values,
    falling back to the stored weather where the API has no value. AQI after t stays NaN."""
    index = pd.date_range(history.index.min(), history.index.max() + pd.Timedelta(hours=HORIZON_HOURS),
                          freq="h", name="ts")
    extended = history.reindex(index)
    extended[WEATHER_COLUMNS] = api_weather.reindex(index)[WEATHER_COLUMNS].combine_first(extended[WEATHER_COLUMNS])
    return extended


def drop_snapshot_precipitation(history: pd.DataFrame) -> pd.DataFrame:
    """Cassandra's real-time precipitation is a 15-minute snapshot, ~4x below the hourly sum the
    model was trained on; a missing value (handled natively by XGBoost) is less wrong."""
    return history.assign(precipitation=float("nan"))


def latest_issue_time(history: pd.DataFrame, now: pd.Timestamp) -> pd.Timestamp | None:
    observed = history["aqi"].dropna()
    if observed.empty or now - observed.index.max() > pd.Timedelta(hours=MAX_STALENESS_HOURS):
        return None
    return observed.index.max()


def forecast_city(city: str, history: pd.DataFrame, now: pd.Timestamp, feature_set: str,
                  weather_forecast: pd.DataFrame | None) -> ForecastRow | None:
    issued_at = latest_issue_time(history, now)
    if issued_at is None:
        logger.error("%s: no measured AQI within %dh of %s, skipping", city, MAX_STALENESS_HOURS, now)
        return None
    history = history.loc[:issued_at]
    if weather_forecast is not None:
        history = with_api_weather(history, weather_forecast)
    else:
        history = drop_snapshot_precipitation(history)
        if FEATURE_SETS[feature_set].use_future_weather:
            logger.warning("%s: no weather forecast, falling back to the %s model", city, FALLBACK_FEATURE_SET)
            feature_set = FALLBACK_FEATURE_SET

    model, meta = load_model(MODEL_DIR, city, feature_set, HORIZON_HOURS)
    features = make_features(history, FEATURE_SETS[feature_set], HORIZON_HOURS).loc[[issued_at]]
    aqi = float(predict_aqi(model, features, meta.features)[0])
    return ForecastRow(city=city, forecast_for=issued_at + pd.Timedelta(hours=HORIZON_HOURS),
                       generated_at=issued_at, predicted_aqi=round(aqi, 1), model_name=meta.model_name,
                       horizon_hours=HORIZON_HOURS)


def inputs_for(args: argparse.Namespace, session, city: str) -> tuple[pd.DataFrame, pd.Timestamp, pd.DataFrame | None]:
    if args.source == "dataset":
        now = pd.Timestamp(args.as_of)
        full = load_city_hourly(city)
        # Replay: weather of the next 24h plays the role of the forecast.
        return full.loc[now - pd.Timedelta(hours=HISTORY_HOURS): now], now, full[WEATHER_COLUMNS]
    now = pd.Timestamp.now(tz=LOCAL_TZ).tz_localize(None).floor("h")
    # End is exclusive: the current, still incomplete hour is left out (training saw full hours only).
    history = read_city_hourly(session, city, now - pd.Timedelta(hours=HISTORY_HOURS), now)
    try:
        weather = fetch_weather_forecast(city)
    except RuntimeError as exc:
        logger.warning("%s", exc)
        weather = None
    return history, now, weather


def run_once(args: argparse.Namespace) -> None:
    session = None if args.source == "dataset" and args.dry_run else connect()
    rows = []
    for city in args.cities:
        try:
            history, now, weather = inputs_for(args, session, city)
            row = forecast_city(city, history, now, args.feature_set, weather)
        except Exception:  # one city failing must not block the other
            logger.exception("%s: forecast failed", city)
            continue
        if row is not None:
            rows.append(row)
            logger.info("%s: AQI %.0f forecast for %s (%s)", city, row.predicted_aqi, row.forecast_for, row.model_name)

    if args.dry_run:
        print(pd.DataFrame(rows).to_string(index=False) if rows else "No forecasts produced.")
    elif rows:
        logger.info("Wrote %d forecasts", write_forecasts(session, rows))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    if args.source == "dataset" and not args.as_of:
        raise SystemExit("--source dataset requires --as-of")
    while True:
        run_once(args)
        if not args.every_minutes:
            break
        time.sleep(args.every_minutes * 60)


if __name__ == "__main__":
    main()

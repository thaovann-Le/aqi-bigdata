"""Cassandra access for hourly serving (schema: infra/cassandra/schema.cql, docs/data_contract.md).

Reads the same city-hour series as C's export, from the tables C recommends for fast range
reads: ``daily_agg`` (period_type='hour', data_basis measured/model) + ``weather_hourly``.
Writes ``forecasts`` (read by role A's Grafana panel "dự báo so với thực tế").

Timestamps are absolute instants: writes use timezone-aware datetimes; the driver returns
naive UTC, converted here to naive UTC+7 like the rest of the ML code.

Connection: CASSANDRA_HOST (default localhost), CASSANDRA_PORT (9042), CASSANDRA_KEYSPACE (aqi),
CASSANDRA_DC (dc1).
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from .config import LOCAL_TZ
from .data import VALUE_COLUMNS, WEATHER_COLUMNS


@dataclass(frozen=True)
class ForecastRow:
    city: str
    forecast_for: pd.Timestamp  # naive local time
    generated_at: pd.Timestamp  # naive local time
    predicted_aqi: float
    model_name: str
    horizon_hours: int


KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "aqi")
PROTOCOL_VERSION = 5  # Cassandra 4.1 (infra/cassandra); avoids the driver's downgrade negotiation
WRITE_CONCURRENCY = 50


def connect():
    from cassandra.cluster import Cluster  # lazy: only serving needs the driver
    from cassandra.policies import DCAwareRoundRobinPolicy

    cluster = Cluster(
        [os.environ.get("CASSANDRA_HOST", "localhost")],
        port=int(os.environ.get("CASSANDRA_PORT", "9042")),
        protocol_version=PROTOCOL_VERSION,
        # Data center name set in docker-compose (CASSANDRA_DC: dc1).
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=os.environ.get("CASSANDRA_DC", "dc1")),
    )
    return cluster.connect()  # tables are always keyspace-qualified below


def to_aware(ts: pd.Timestamp) -> datetime:
    return pd.Timestamp(ts).tz_localize(LOCAL_TZ).to_pydatetime()


def from_driver(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values).dt.tz_localize("UTC").dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)


_AGG_QUERY = (
    f"SELECT period_start, avg_aqi, avg_pm25, avg_pm10, record_count FROM {KEYSPACE}.daily_agg "
    "WHERE city = ? AND period_type = 'hour' AND data_basis = ? AND period_start >= ? AND period_start < ?"
)
_WEATHER_QUERY = (
    f"SELECT ts, {', '.join(WEATHER_COLUMNS)} FROM {KEYSPACE}.weather_hourly WHERE city = ? AND ts >= ? AND ts < ?"
)


def _frame(rows, columns: list[str]) -> pd.DataFrame:
    df = pd.DataFrame([tuple(r) for r in rows], columns=columns)
    return df.assign(ts=from_driver(df["ts"])).set_index("ts") if len(df) else df.set_index("ts")


def read_city_hourly(session, city: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Hours in [start, end) (naive local) on a gap-free index, same columns as aqiml.data."""
    params = (to_aware(start), to_aware(end))
    agg = session.prepare(_AGG_QUERY)
    measured = _frame(session.execute(agg, (city, "measured", *params)),
                      ["ts", "aqi", "pm25", "pm10", "n_stations"])
    model = _frame(session.execute(agg, (city, "model", *params)),
                   ["ts", "cams_aqi", "cams_pm25", "cams_pm10", "_count"]).drop(columns="_count")
    weather = _frame(session.execute(session.prepare(_WEATHER_QUERY), (city, *params)), ["ts", *WEATHER_COLUMNS])

    index = pd.date_range(start, end, freq="h", inclusive="left", name="ts")
    frame = measured.join(model, how="outer").join(weather, how="outer")
    return frame.reindex(index)[VALUE_COLUMNS].apply(pd.to_numeric, errors="coerce")


def write_forecasts(session, rows: Iterable[ForecastRow]) -> int:
    from cassandra.concurrent import execute_concurrent_with_args

    insert = session.prepare(
        f"INSERT INTO {KEYSPACE}.forecasts (city, forecast_for, generated_at, predicted_aqi, model_name, "
        "horizon_hours) VALUES (?, ?, ?, ?, ?, ?)"
    )
    params = [(r.city, to_aware(r.forecast_for), to_aware(r.generated_at), float(r.predicted_aqi),
               r.model_name, int(r.horizon_hours)) for r in rows]
    results = execute_concurrent_with_args(session, insert, params, concurrency=WRITE_CONCURRENCY,
                                           raise_on_first_error=True)
    return sum(1 for ok, _ in results if ok)

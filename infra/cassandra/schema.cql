-- Starting schema proposal for the AQI pipeline (owner: C - Xu ly Spark).
-- To be confirmed/adjusted by the team on Day 1 per the plan, then re-applied
-- with: docker compose exec cassandra cqlsh -f /scripts/schema.cql
-- (or just restart the cassandra-init container).

CREATE KEYSPACE IF NOT EXISTS aqi
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE aqi;

-- Cleaned, hourly station-level readings (streaming + batch write here).
CREATE TABLE IF NOT EXISTS measurements (
  city            text,
  station_id      text,
  ts              timestamp,
  pm25            double,
  pm10            double,
  o3              double,
  no2             double,
  so2             double,
  co              double,
  aqi             int,
  aqi_standard    text,   -- 'US_AQI' or 'VN_AQI' - pick one, document in report
  temperature     double,
  humidity        double,
  wind_speed      double,
  wind_direction  double,
  precipitation   double,
  source          text,   -- 'aqicn' | 'openaq' | 'open-meteo'
  PRIMARY KEY ((city, station_id), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Hourly/daily/monthly/seasonal aggregates, per city.
CREATE TABLE IF NOT EXISTS daily_agg (
  city          text,
  period_type   text,      -- 'hour' | 'day' | 'month' | 'season'
  period_start  timestamp,
  avg_aqi       double,
  max_aqi       double,
  min_aqi       double,
  avg_pm25      double,
  record_count  int,
  PRIMARY KEY ((city, period_type), period_start)
) WITH CLUSTERING ORDER BY (period_start DESC);

-- Threshold-breach alerts.
CREATE TABLE IF NOT EXISTS alerts (
  city        text,
  ts          timestamp,
  station_id  text,
  aqi         int,
  level       text,   -- e.g. 'unhealthy', 'very_unhealthy', 'hazardous'
  message     text,
  PRIMARY KEY ((city), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Model forecasts (D writes here hourly).
CREATE TABLE IF NOT EXISTS forecasts (
  city            text,
  forecast_for    timestamp,
  generated_at    timestamp,
  predicted_aqi   double,
  predicted_pm25  double,
  model_name      text,   -- e.g. 'xgboost_v1', 'baseline_persistence'
  horizon_hours   int,
  PRIMARY KEY ((city), forecast_for, generated_at)
) WITH CLUSTERING ORDER BY (forecast_for DESC, generated_at DESC);

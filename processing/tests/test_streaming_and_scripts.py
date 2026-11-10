"""Mô phỏng streaming bằng file-stream (cùng hàm foreachBatch như bản Kafka) và chạy thử script đúng như người dùng chạy."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from common import config
from common.pipeline import make_foreach_batch, process_raw

ROOT = Path(__file__).resolve().parents[1]
WEATHER_SCHEMA = "city string, ts timestamp, " + ", ".join(f"{c} double" for c in config.WEATHER_COLS)


MEAS_KEY = ("city", "station_id", "ts")


def collector_lookups(sink):
    """Các lookup của process_raw đọc lại từ sink giả (vai trò của io.make_lookups với Cassandra)."""
    def table_df(spark, table, like):
        rows = [tuple(r.get(c) for c in like.columns) for r in sink.rows(table)]
        return spark.createDataFrame(rows, like.schema)

    def weather(sp, aq):
        stored = [tuple(r.get(c) for c in ("city", "ts", *config.WEATHER_COLS)) for r in sink.rows("weather_hourly")]
        return sp.createDataFrame(stored, WEATHER_SCHEMA)

    def meas(sp, keys):
        want = {(r["city"], r["ts"]) for r in keys.collect()}
        rows = [(k[0], k[1], k[2], v.get("aqi")) for k, v in sink.merged("measurements", MEAS_KEY).items()
                if (k[0], k[2]) in want]
        return sp.createDataFrame(rows, "city string, station_id string, ts timestamp, aqi int")

    return {"weather_lookup": weather, "obs_lookup": lambda sp, df: table_df(sp, "openaq_observations", df),
            "readings_lookup": lambda sp, df: table_df(sp, "weather_readings", df), "meas_lookup": meas,
            "delete": sink.delete}


def same_rows(a, b):
    """So hai bảng đã gộp upsert: cùng khóa, cùng giá trị (số thực so gần đúng)."""
    if set(a) != set(b):
        return False
    for k in a:
        for c in set(a[k]) | set(b[k]):
            x, y = a[k].get(c), b[k].get(c)
            if isinstance(x, float) or isinstance(y, float):
                if x is None or y is None or abs(x - y) > 1e-9:
                    return False
            elif x != y:
                return False
    return True


def test_streaming_microbatches_match_batch(spark, raw_sample, stations, sink, tmp_path):
    lines = []
    for p in sorted(Path(raw_sample).rglob("*.jsonl")):
        lines += [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    lines.sort(key=lambda ln: json.loads(ln)["fetched_at"])          # đúng thứ tự thời gian như Kafka
    src = tmp_path / "in"
    src.mkdir()
    n_chunks = 4
    size = (len(lines) + n_chunks - 1) // n_chunks
    for i in range(n_chunks):
        (src / f"part-{i}.jsonl").write_text("\n".join(lines[i * size:(i + 1) * size]), encoding="utf-8")

    metrics = tmp_path / "m.jsonl"
    handler = make_foreach_batch(sink, stations, metrics_path=str(metrics), threshold=150, **collector_lookups(sink))
    q = (spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(str(src))
         .writeStream.foreachBatch(handler).option("checkpointLocation", str(tmp_path / "ck"))
         .trigger(availableNow=True).start())
    q.awaitTermination()

    batch_sink = type(sink)()
    process_raw(spark, spark.createDataFrame([(ln,) for ln in lines], "value string"), batch_sink, None, stations,
                threshold=150)

    # Dữ liệu một giờ (trạm đo 5 phút, số đọc thời tiết 15 phút) nằm vắt qua các lô: nhờ đọc lại trạng thái đã lưu,
    # kết quả sau 4 lô phải bằng ĐÚNG kết quả chạy một lần - mọi cột, không chỉ tập khóa.
    s, b = sink.merged("measurements", MEAS_KEY), batch_sink.merged("measurements", MEAS_KEY)
    assert len(b) > 0 and same_rows(s, b)
    assert same_rows(sink.merged("weather_hourly", ("city", "ts")), batch_sink.merged("weather_hourly", ("city", "ts")))
    assert set(sink.merged("alerts", ("city", "ts", "station_id"))) == set(batch_sink.merged("alerts",
                                                                                              ("city", "ts", "station_id")))
    assert len(metrics.read_text().splitlines()) == n_chunks


def test_streaming_clears_alert_when_more_samples_arrive(spark, stations, sink):
    """Trạm 5 phút: mẫu đầu của giờ cao (cảnh báo), các mẫu sau kéo trung bình giờ xuống dưới ngưỡng -> gỡ cảnh báo."""
    def msg(fetched, samples):
        return json.dumps({"source": "openaq", "city": "hanoi", "station_id": 4946813, "fetched_at": fetched,
                           "payload": {"results": [{"datetime": {"utc": t}, "value": v, "parameter": "pm25",
                                                    "units": "µg/m³"} for t, v in samples]}})
    b0 = [msg("2026-10-07T01:10:00+00:00", [("2026-10-07T01:05:00Z", 80.0)])]
    b1 = [msg("2026-10-07T01:40:00+00:00", [("2026-10-07T01:25:00Z", 30.0), ("2026-10-07T01:35:00Z", 30.0)])]
    handler = make_foreach_batch(sink, stations, threshold=150, **collector_lookups(sink))
    handler(spark.createDataFrame([(x,) for x in b0], "value string"), 0)
    assert len(sink.merged("alerts", ("city", "ts", "station_id"))) == 1        # AQI(80) = 163 > 150
    handler(spark.createDataFrame([(x,) for x in b1], "value string"), 1)
    row = list(sink.merged("measurements", MEAS_KEY).values())[0]
    assert row["pm25"] == pytest.approx(140 / 3)                                   # trung bình cả 3 mẫu của giờ
    assert sink.merged("alerts", ("city", "ts", "station_id")) == {}


def _run(script, *args, env=None):
    e = {**os.environ, **(env or {})}
    r = subprocess.run([sys.executable, str(ROOT / script), *args], capture_output=True, text=True, env=e, timeout=900)
    assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-3000:]
    return r.stdout


def test_scripts_history_then_aggregate_parquet(sample_history, tmp_path, spark):
    out = f"parquet:{tmp_path / 'out'}"
    metrics = tmp_path / "metrics.jsonl"
    env = {"METRICS_FILE": str(metrics)}
    _run("batch/batch_history.py", "--base", sample_history, "--sink", out, "--master", "local[2]", env=env)
    # chạy lại một phần: không được lỗi
    _run("batch/batch_history.py", "--base", sample_history, "--sink", out, "--master", "local[2]",
         "--years", "2024", "--no-model", env=env)
    _run("batch/batch_aggregate.py", "--sink", out, "--master", "local[2]", env=env)
    ml = tmp_path / "ml"
    _run("batch/export_ml_dataset.py", "--sink", out, "--out", str(ml), "--master", "local[2]", env=env)
    csvs = list((ml / "city_hourly_csv").glob("part-*.csv"))
    assert len(csvs) == 1
    header = csvs[0].read_text(encoding="utf-8").splitlines()[0]
    assert header.startswith("city,ts_local,ts_utc,aqi,pm25,pm10,n_stations,aqi_model")

    base = tmp_path / "out"
    assert {p.name for p in base.iterdir()} == {"weather_hourly", "measurements", "alerts", "daily_agg"}
    m = spark.read.parquet(str(base / "measurements"))
    assert m.select("city", "station_id", "ts").distinct().count() > 0
    agg = spark.read.parquet(str(base / "daily_agg"))
    # dù measurements có dòng trùng (append 2 lần), daily_agg vẫn đúng một dòng mỗi khóa
    assert agg.count() == agg.select("city", "period_type", "data_basis", "period_start").distinct().count()
    lines = [json.loads(ln) for ln in metrics.read_text().splitlines()]
    assert [ln["job"] for ln in lines] == ["batch_history", "batch_history", "batch_aggregate", "export_ml_dataset"]
    assert lines[0]["measurement_rows"] > 0 and lines[0]["alert_rows"] == 1 and lines[0]["rows_per_min"] > 0


def test_batch_raw_script(raw_sample, tmp_path, spark):
    out = f"parquet:{tmp_path / 'out'}"
    _run("batch/batch_raw.py", "--base", str(raw_sample), "--sink", out, "--master", "local[2]",
         env={"METRICS_FILE": str(tmp_path / "metrics.jsonl")})
    assert spark.read.parquet(str(tmp_path / "out" / "measurements")).count() > 0

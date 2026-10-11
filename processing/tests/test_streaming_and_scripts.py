"""Mô phỏng streaming bằng file-stream (cùng hàm foreachBatch như bản Kafka) và chạy thử script đúng như người dùng chạy."""
import json
import os
import subprocess
import sys
from pathlib import Path

from common import config
from common.pipeline import make_foreach_batch, process_raw

ROOT = Path(__file__).resolve().parents[1]
WEATHER_SCHEMA = "city string, ts timestamp, " + ", ".join(f"{c} double" for c in config.WEATHER_COLS)


def _merge_upserts(rows):
    """Giả lập Cassandra + ignoreNulls: ghi sau ghi đè ghi trước theo khóa chính, null không xóa giá trị cũ."""
    d = {}
    for r in rows:
        k = (r["city"], r["station_id"], r["ts"])
        d[k] = {**d.get(k, {}), **{c: v for c, v in r.items() if v is not None}}
    return d


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

    def lookup(sp, aq):   # giả lập weather_lookup_from_cassandra bằng các dòng weather_hourly đã "ghi"
        stored = [tuple(r[c] for c in ("city", "ts", *config.WEATHER_COLS)) for r in sink.rows("weather_hourly")]
        return sp.createDataFrame(stored, WEATHER_SCHEMA)

    metrics = tmp_path / "m.jsonl"
    handler = make_foreach_batch(sink, stations, lookup, metrics_path=str(metrics), threshold=150)
    q = (spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(str(src))
         .writeStream.foreachBatch(handler).option("checkpointLocation", str(tmp_path / "ck"))
         .trigger(availableNow=True).start())
    q.awaitTermination()

    batch_sink = type(sink)()
    process_raw(spark, spark.createDataFrame([(ln,) for ln in lines], "value string"), batch_sink, None, stations,
                threshold=150)

    s, b = _merge_upserts(sink.rows("measurements")), _merge_upserts(batch_sink.rows("measurements"))
    assert set(s) == set(b) and len(b) > 0
    with_weather = sum(1 for v in s.values() if v.get("temperature") is not None)
    expected = sum(1 for v in b.values() if v.get("temperature") is not None)
    assert with_weather >= 0.9 * expected, (with_weather, expected)
    assert len(metrics.read_text().splitlines()) == n_chunks


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

#!/usr/bin/env bash
# Runs the whole role-D pipeline on role C's processed data: tests -> EDA notebooks -> training.
# Usage (from the repo root): bash ml/run_all.sh
# Input: AQI_ML_DATASET (default processing/_runtime/ml_dataset/city_hourly_parquet, produced by
# `docker compose -f docker-compose.demo.yml up -d`). Outputs: ml/outputs/.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python}"
export PYTHONIOENCODING=utf-8 MPLBACKEND=Agg

cd "$ROOT"
# Allow a relative interpreter path (e.g. PYTHON=.venv/bin/python): notebooks run from another folder.
if [[ "$PYTHON" == */* ]]; then PYTHON="$(cd "$(dirname "$PYTHON")" && pwd)/$(basename "$PYTHON")"; fi
mkdir -p ml/outputs/tables

"$PYTHON" -m pytest ml/tests -q

# Notebooks import `_setup` from their own folder, so run them from there.
for nb in 01_data_quality 02_eda_trends 03_weather_correlation 04_spatial_overview; do
  echo "== $nb"
  (cd ml/notebooks && "$PYTHON" "$nb.py" > "../outputs/tables/$nb.log")
done

"$PYTHON" ml/training/train_xgboost.py
echo "Done. Figures: ml/outputs/figures, tables: ml/outputs/tables, models: ml/outputs/models"

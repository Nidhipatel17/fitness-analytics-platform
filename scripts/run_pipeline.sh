#!/usr/bin/env bash
# End-to-end pipeline entrypoint: extract -> load_raw -> dbt build -> quality
# checks. Mirrors orchestration/dags/fitness_pipeline_dag.py so the exact
# same steps can be run locally without Airflow.
#
# `dbt build` rather than `dbt run` + `dbt test` separately: dbt run alone
# skips seeds and snapshots (they need `dbt seed`/`dbt snapshot`), which is
# an easy trap -- dim_exercise_type and dim_user both fail on a fresh
# project if you run `dbt run` without those first. `dbt build` runs seeds,
# snapshots, models, and tests together in correct dependency order, so
# that ordering mistake isn't something you can make.
#
# Usage: scripts/run_pipeline.sh [source] [landing-zone] [project] [dataset]

set -euo pipefail

SOURCE="${1:-synthetic}"
LANDING_ZONE="${2:-landing}"
PROJECT="${3:-fitness-analytics-dev}"
DATASET="${4:-raw}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== [1/4] extract (${SOURCE}) ==="
python3 "${REPO_ROOT}/ingestion/extract.py" --source "${SOURCE}" --landing-zone "${LANDING_ZONE}" --entity all

echo "=== [2/4] load_raw ==="
python3 "${REPO_ROOT}/ingestion/load_raw.py" --landing-zone "${LANDING_ZONE}" --source "${SOURCE}" --entity all --project "${PROJECT}" --dataset "${DATASET}"

echo "=== [3/4] dbt build (seed + snapshot + run + test) ==="
(cd "${REPO_ROOT}/dbt" && dbt build)

echo "=== [4/4] quality checks ==="
echo "TODO(week 3): python3 quality/freshness_check.py && python3 quality/volume_anomaly_check.py"

echo "=== pipeline run complete ==="

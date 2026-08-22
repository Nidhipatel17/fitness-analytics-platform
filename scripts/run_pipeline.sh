#!/usr/bin/env bash
# End-to-end pipeline entrypoint: extract -> load_raw -> dbt run -> dbt test
# -> quality checks. Mirrors orchestration/dags/fitness_pipeline_dag.py so
# the exact same steps can be run locally without Airflow.
#
# Usage: scripts/run_pipeline.sh [source] [landing-zone] [project] [dataset]

set -euo pipefail

SOURCE="${1:-synthetic}"
LANDING_ZONE="${2:-landing}"
PROJECT="${3:-fitness-analytics-dev}"
DATASET="${4:-raw}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== [1/5] extract (${SOURCE}) ==="
python3 "${REPO_ROOT}/ingestion/extract.py" --source "${SOURCE}" --landing-zone "${LANDING_ZONE}" --entity all

echo "=== [2/5] load_raw ==="
python3 "${REPO_ROOT}/ingestion/load_raw.py" --landing-zone "${LANDING_ZONE}" --source "${SOURCE}" --entity all --project "${PROJECT}" --dataset "${DATASET}"

echo "=== [3/5] dbt run ==="
echo "TODO(week 2): wire up once dbt/models/{staging,marts} are built -- (cd dbt && dbt run)"

echo "=== [4/5] dbt test ==="
echo "TODO(week 2): (cd dbt && dbt test)"

echo "=== [5/5] quality checks ==="
echo "TODO(week 3): python3 quality/freshness_check.py && python3 quality/volume_anomaly_check.py"

echo "=== pipeline run complete (ingestion steps only -- see TODOs above) ==="

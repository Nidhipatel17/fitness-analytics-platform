# fitness-analytics-platform

Fitness data pipeline: ingestion → BigQuery → dbt → orchestration → dashboard.

## Stack

| Tool | Used for |
|---|---|
| Python | ingestion clients (`ingestion/`), extract/load scripts, quality checks |
| BigQuery | raw tables + dbt-modeled warehouse |
| dbt | dimensional model — staging, marts, snapshots, tests (`dbt/`) |
| Airflow (via Docker Compose) | scheduling/orchestration (`orchestration/`) |
| Docker | containerized ingestion service + local Airflow |
| GitHub Actions | CI — lint, tests, real `dbt build` on every PR |
| Looker Studio | dashboard |

## Run it

```bash
python3 -m pytest tests/ -v

python3 ingestion/extract.py --source synthetic --landing-zone landing --entity all
python3 ingestion/load_raw.py --landing-zone landing --source synthetic --entity all

cd dbt && dbt build
```

Or end-to-end: `scripts/run_pipeline.sh <source> <landing-zone> <project> <dataset>`.

Local Airflow: `docker compose up`.

Needs a GCP project + `~/.dbt/profiles.yml` (copy from `dbt/profiles.yml.example`).

## Dashboard

https://datastudio.google.com/reporting/b658258e-3083-45d8-8fa5-131c148b00a7

- Overview — trend line, friends leaderboard, personal records
- Personal Insights — KPIs, session mix, heart rate trend
- Pipeline Health — ingestion volume, per-user data freshness

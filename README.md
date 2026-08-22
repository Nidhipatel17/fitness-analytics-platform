# fitness-analytics-platform

A fitness data platform built as an interview-prep project: synthetic + real
(Strava) ingestion → BigQuery raw → dbt dimensional model → orchestration →
quality checks → dashboard, following the roadmap in this README's Roadmap
section. Eventually pairs with an actual fitness app on top of the same
warehouse.

## Status: Week 1 (ingestion) done

What exists and runs today, under [ingestion/](ingestion/):

- [`ingestion/clients/base_client.py`](ingestion/clients/base_client.py) — the interface every source implements: `fetch_users`/`fetch_workouts` (each takes `since`/`until` for incremental or bounded-replay pulls), `normalize_user`/`normalize_workout`, and a shared per-entity watermark (`get_since`/`set_since`).
- [`ingestion/clients/synthetic_client.py`](ingestion/clients/synthetic_client.py) — a fake backend that persists its own history across calls and can be told to misbehave on demand: malformed records, business-rule anomalies (>24h duration, negative distance), duplicate re-deliveries, and late-arriving data (`start_time` days in the past, `synced_at` now). See its module docstring for why this is built before the real API client.
- [`ingestion/clients/strava_client.py`](ingestion/clients/strava_client.py) — real Strava API v3 client (OAuth2 refresh-token flow, `/athlete/activities` with `after`/`before`, per-activity `/streams` for heart-rate/GPS) implementing the same interface. Needs `STRAVA_CLIENT_ID`/`STRAVA_CLIENT_SECRET`/`STRAVA_REFRESH_TOKEN` to actually run.
- [`ingestion/extract.py`](ingestion/extract.py) — pulls "new since last sync" from a client, normalizes, and writes JSONL to a local landing zone partitioned by `synced_at` date (`landing/<entity>/source=<source>/dt=<date>/`). Malformed records are caught and routed to `landing/_rejects/...` instead of crashing the run. Supports a bounded `--since --until` backfill/replay mode that never touches the watermark.
- [`ingestion/load_raw.py`](ingestion/load_raw.py) — loads landing-zone JSONL into BigQuery raw tables (`raw_workouts`, `raw_users`) via a staging table + `MERGE` on natural key (`workout_id`, or `user_id`+`source`), so re-loading the same file updates in place instead of duplicating rows. Supports `--dry-run` (prints the MERGE SQL, touches no GCP) since this environment has no live GCP project.
- [`scripts/backfill.sh`](scripts/backfill.sh) — replays extract+load_raw one UTC day at a time for an arbitrary past range. Idempotent: backfill output filenames are deterministic per window and get overwritten (not appended) on replay; verified by running the same window twice and diffing output (byte-identical, 0 new files).
- [`scripts/run_pipeline.sh`](scripts/run_pipeline.sh) — end-to-end entrypoint; extract + load_raw run for real today, dbt/quality steps are marked TODO until Week 2/3 land.
- [`tests/test_clients.py`](tests/test_clients.py) — 10 tests covering user/workout generation, watermark advancement, malformed-vs-anomalous record handling, duplicate delivery, late arrival, and bounded-replay idempotency. `python3 -m pytest tests/` to run.

### Try it

```bash
python3 -m pytest tests/ -v

python3 ingestion/extract.py --source synthetic --landing-zone landing --entity all
python3 ingestion/load_raw.py --landing-zone landing --source synthetic --entity all --dry-run

scripts/backfill.sh synthetic 2026-08-01 2026-08-07 landing fitness-analytics-dev raw --dry-run
```

Drop `--dry-run` on `load_raw.py`/`backfill.sh` once `GOOGLE_APPLICATION_CREDENTIALS` and a real BigQuery project are set up.

### Raw data shape

Every workout carries a nested, repeated `samples` array (heart rate / GPS /
elevation per timestamp) — decided at design time because it's what
justifies BigQuery's nested/repeated columns later in the dbt layer, rather
than bolting nesting on after the fact. See the docstring in
[`base_client.py`](ingestion/clients/base_client.py) for the full canonical schema.

## Roadmap

- **Week 1 — Ingestion** ✅ done (see above).
- **Week 2 — Dimensional model + dbt**: star schema (`fact_workout` grain = one row per workout, `fact_daily_metrics` grain = one row per user per day), `dim_user` as SCD Type 2, staging dedupe on natural key + `synced_at`, dbt tests including a custom "no workout > 24h" check (the synthetic client already produces that anomaly on demand — see `anomaly_rate` in `synthetic_client.py`).
- **Week 3 — Orchestration + quality**: Airflow DAG mirroring `run_pipeline.sh`, freshness check (fail if a user hasn't synced in >48h), volume anomaly check (flag if daily row count < 50% of trailing 7-day average), and a written batch-vs-streaming tradeoff doc.
- **Week 4 — Containerize, CI, dashboard**: Dockerfile + docker-compose (ingestion + Postgres + Airflow), CI running `dbt test` + lint on every PR, Looker Studio dashboard (trends, leaderboard, PRs), and this README's final architecture write-up.
- **Stretch**: point-in-time-correct churn features + BigQuery ML model; simulated live heart-rate → Pub/Sub → Dataflow → BigQuery streaming pipeline.
- **Beyond the roadmap**: a fitness app on top of the same warehouse/marts.

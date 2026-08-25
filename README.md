# fitness-analytics-platform

A fitness data platform: synthetic + real (Strava) ingestion → BigQuery raw
→ dbt dimensional model → orchestration → quality checks → dashboard.
Eventually pairs with an actual fitness app on top of the same warehouse.

## What's built

Under [ingestion/](ingestion/):

- [`ingestion/clients/base_client.py`](ingestion/clients/base_client.py) — the interface every source implements: `fetch_users`/`fetch_workouts` (each takes `since`/`until` for incremental or bounded-replay pulls), `normalize_user`/`normalize_workout`, and a shared per-entity watermark (`get_since`/`set_since`).
- [`ingestion/clients/synthetic_client.py`](ingestion/clients/synthetic_client.py) — a fake backend that persists its own history across calls and can be told to misbehave on demand: malformed records, business-rule anomalies (>24h duration, negative distance), duplicate re-deliveries, and late-arriving data (`start_time` days in the past, `synced_at` now). See its module docstring for why this is built before the real API client.
- [`ingestion/clients/strava_client.py`](ingestion/clients/strava_client.py) — real Strava API v3 client (OAuth2 refresh-token flow, `/athlete/activities` with `after`/`before`, per-activity `/streams` for heart-rate/GPS) implementing the same interface. Needs `STRAVA_CLIENT_ID`/`STRAVA_CLIENT_SECRET`/`STRAVA_REFRESH_TOKEN` to actually run.
- [`ingestion/extract.py`](ingestion/extract.py) — pulls "new since last sync" from a client, normalizes, and writes JSONL to a local landing zone partitioned by `synced_at` date (`landing/<entity>/source=<source>/dt=<date>/`). Malformed records are caught and routed to `landing/_rejects/...` instead of crashing the run. Supports a bounded `--since --until` backfill/replay mode that never touches the watermark.
- [`ingestion/load_raw.py`](ingestion/load_raw.py) — loads landing-zone JSONL into BigQuery raw tables (`raw_workouts`, `raw_users`) via a staging table + `MERGE` on natural key (`workout_id`, or `user_id`+`source`), so re-loading the same file updates in place instead of duplicating rows. Supports `--dry-run` (prints the MERGE SQL, touches no GCP) since this environment has no live GCP project.
- [`scripts/backfill.sh`](scripts/backfill.sh) — replays extract+load_raw one UTC day at a time for an arbitrary past range. Idempotent: backfill output filenames are deterministic per window and get overwritten (not appended) on replay; verified by running the same window twice and diffing output (byte-identical, 0 new files).
- [`scripts/run_pipeline.sh`](scripts/run_pipeline.sh) — end-to-end entrypoint: extract → load_raw → `dbt build` → quality checks (quality checks still TODO). `dbt build` rather than `dbt run` + `dbt test` separately, deliberately: `dbt run` alone skips seeds/snapshots, which is a real trap — see below.
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

Under [dbt/](dbt/) — the dimensional model on top of the raw tables:

- [`models/staging/stg_workouts.sql`](dbt/models/staging/stg_workouts.sql) / [`stg_users.sql`](dbt/models/staging/stg_users.sql) — dedupe on natural key via `QUALIFY ROW_NUMBER() ... = 1` (the same pattern used to fix `load_raw.py`'s MERGE bug, kept consistent rather than reinventing it), cast types. `samples` stays nested here rather than getting UNNESTed — that happens once, downstream, in `fact_workout`.
- [`snapshots/users_snapshot.sql`](dbt/snapshots/users_snapshot.sql) + [`models/marts/dim_user.sql`](dbt/models/marts/dim_user.sql) — SCD Type 2 via a native `dbt snapshot` (timestamp strategy on `updated_at`), with `dim_user.sql` as a thin wrapper renaming `dbt_valid_from`/`dbt_valid_to` and coalescing the open-ended NULL into a `9999-12-31` sentinel.
- [`models/marts/dim_date.sql`](dbt/models/marts/dim_date.sql) — generated via `GENERATE_DATE_ARRAY`, not seeded (a date spine is derivable, so a static file would just be dead weight).
- [`models/marts/dim_exercise_type.sql`](dbt/models/marts/dim_exercise_type.sql) — the opposite call: seeded from [`seeds/exercise_types.csv`](dbt/seeds/exercise_types.csv), because this genuinely is static reference data.
- [`models/marts/fact_workout.sql`](dbt/models/marts/fact_workout.sql) — grain: one row per workout. Joins `dim_user` as an **as-of join** (`start_time >= valid_from AND start_time < valid_to`), not a plain `user_id` join — a workout from 6 months ago has to attach to the user's weight/goal *as of then*, not today's. `samples` gets UNNESTed here for heart-rate aggregates, once, still one row out per workout in.
- [`models/marts/fact_daily_metrics.sql`](dbt/models/marts/fact_daily_metrics.sql) — grain: one row per user per **calendar day**, dense (zero-workout days included, not just active ones) — built from `dim_user × dim_date`. This is the grain choice that actually matters here: a 7-day rolling average computed with `ROWS BETWEEN 6 PRECEDING` silently means something different than "7 calendar days" the moment there's a sparse gap day. Dense rows plus `RANGE BETWEEN 6 PRECEDING` on `UNIX_DATE(date_day)` make the two agree by construction. Also computes week-over-week % change and days-since-previous-session.
- [`macros/test_duration_under_hours.sql`](dbt/macros/test_duration_under_hours.sql) — custom generic dbt test for the ">24h workout" business rule (schema-valid, business-wrong — exactly what `synthetic_client.py`'s `anomaly_rate` injects on demand), applied to `fact_workout.duration_minutes` in [`models/marts/schema.yml`](dbt/models/marts/schema.yml).

**Verified against a real BigQuery project, not just `dbt parse`** — `dbt build` (seed + snapshot + run + test) executed clean: 1 seed, 1 snapshot, 5 table models, 2 view models, 41 tests. Two things only a real run could catch, not static parsing:
  - `dbt run` alone doesn't touch seeds or snapshots — `dim_exercise_type` and `dim_user` both fail on a fresh project without `dbt seed`/`dbt snapshot` first. `dbt build` runs everything in correct dependency order, so `run_pipeline.sh` uses that, not `dbt run`.
  - The `duration_under_hours` test **actually caught a live anomaly**: `synthetic_client.py`'s `anomaly_rate` injected a 28.4-hour "strength" workout (`syn-00000014`) into that run's batch, and the test failed the build on it — exactly the intended behavior.
  - Also directly confirmed SCD2 works: forced a user's weight to drift, re-ran the snapshot, and queried `dim_user` — the old row closed out (`valid_to` = the exact timestamp of the change, `is_current = false`), a new row opened with the sentinel end-date and `is_current = true`.

Copy [`profiles.yml.example`](dbt/profiles.yml.example) to `~/.dbt/profiles.yml`, fill in your own GCP project, then `python3 ingestion/extract.py ... && python3 ingestion/load_raw.py ... && (cd dbt && dbt build)` — or just run `scripts/run_pipeline.sh <source> <landing-zone> <project> <dataset>`.

Under [orchestration/](orchestration/), [quality/](quality/), and [docker-compose.yml](docker-compose.yml) — running the whole thing on a schedule:

- [`orchestration/dags/fitness_pipeline_dag.py`](orchestration/dags/fitness_pipeline_dag.py) — `extract_synthetic` and `extract_strava` as independent, parallel TaskGroups feeding into one `dbt_build`, then `freshness_check`/`volume_anomaly_check` run in parallel after it. Strava's TaskGroup opens with a `ShortCircuitOperator` that skips (not fails) the branch when its credentials aren't set. `catchup=False` deliberately — `extract.py` already tracks "since last sync" itself via its own watermark; Airflow's own catchup/backfill on top of that would be a second, conflicting mechanism (`scripts/backfill.sh` owns historical replay).
- [`quality/freshness_check.py`](quality/freshness_check.py) — fails if any user's workout data hasn't synced in >48h. This is finer-grained than the `freshness:` block already in [`dbt/models/staging/schema.yml`](dbt/models/staging/schema.yml): that one is table-level ("did *any* row arrive recently"), this is per-user, so one active user can't mask 24 silent ones.
- [`quality/volume_anomaly_check.py`](quality/volume_anomaly_check.py) — fails if today's row count is <50% of the trailing 7-day average. Catches a different failure than freshness does: a source still delivering *something* per user but at a fraction of normal volume. Skips (doesn't fail) when there isn't enough trailing history yet, so a brand-new project doesn't false-alarm on day 2.
- [`docker-compose.yml`](docker-compose.yml) — local Airflow: Postgres + webserver + scheduler, `LocalExecutor` (simpler than the Celery/Redis/worker/flower stack Airflow's own docs default to — this project's task volume doesn't need distributed workers). Whole repo bind-mounted to `/opt/airflow/project` so tasks running *inside* the container write to the same `landing/`/`.state/` the host sees.

**All of this verified for real — inside an actual Docker container, against real BigQuery, not just parsed.** Found and fixed three real bugs only a live run could surface:
  - `airflow-init` crashed with `AirflowConfigException: user has no username`. Copied the `AIRFLOW_UID` pattern straight from Airflow's official (Linux-focused) docs without questioning it — mapping the container to the host UID is unnecessary on macOS (Docker Desktop's VM already handles that permission mapping) and actively breaks things, since an arbitrary host UID has no matching entry in the container's own user database.
  - Unpausing the DAG fired an automatic scheduled run *at the same moment* a manual trigger queued a second one — two overlapping runs raced on the same local landing-zone state and the same BigQuery staging table name, and one failed. Fixed with `max_active_runs=1`, the standard Airflow answer for "these tasks aren't safe under concurrent execution," rather than trying to make every script itself concurrency-safe.
  - `ignore_downstream_trigger_rules` on `ShortCircuitOperator` defaults to `True` — my first fix (just setting `dbt_build`'s trigger rule to `none_failed_min_one_success`) silently failed on its own, because the short-circuit was aggressively skipping its *entire* downstream subgraph regardless of trigger rules. Verified the actual mechanism with a throwaway test DAG before trusting the fix, since this one didn't match my first assumption.
  - Also watched `dbt_build` correctly fail on a real, live `duration_under_hours` violation mid-DAG-run (not a bug — the quality gate working exactly as intended), which is what caught that `retries=2` (inherited from `default_args`, meant for extract/load's transient network blips) makes no sense for a data-quality test failure — the anomalous row is still there on attempt 2. `dbt_build` now overrides `retries=0`.

## Batch vs. streaming

This pipeline is batch, deliberately, not by default. The questions it actually answers — weekly mileage trends, a leaderboard, "have I been keeping up with my goal" — tolerate hours of latency; nobody needs their 7-day rolling average to update mid-workout. Batch is also just operationally cheaper here: one scheduled DAG run, `MERGE`-based idempotency, easy to reason about and replay (`scripts/backfill.sh`) when something goes wrong.

The one thing in this domain that *doesn't* tolerate batch latency is in-workout feedback — a live heart-rate zone alert, real-time pace coaching while you're mid-run. That's a genuinely different latency requirement, not a bigger version of the same one, which is exactly why it's scoped as a separate stretch component (`streaming/`: simulated heart-rate samples → Pub/Sub → Dataflow windowed aggregation → BigQuery) rather than a reason to convert the whole pipeline to streaming. Converting everything would mean paying streaming's operational cost (more moving parts, harder to replay/backfill, trickier exactly-once semantics) for questions that never needed sub-minute freshness in the first place.

## What's not built yet

- Containerize, CI, dashboard: Dockerfile for the ingestion service specifically (docker-compose above already covers local Airflow), CI running `dbt test` + lint on every PR, Looker Studio dashboard (trends, leaderboard, PRs).
- Stretch: point-in-time-correct churn features + BigQuery ML model; the `streaming/` pipeline described above.
- Beyond this repo: a fitness app on top of the same warehouse/marts.

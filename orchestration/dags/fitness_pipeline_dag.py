"""extract -> load_raw -> dbt build -> quality checks, orchestrated.

Mirrors scripts/run_pipeline.sh exactly -- every BashOperator below shells
out to the same commands you'd run by hand, so a failing task can be
reproduced locally with a single copy-paste, not re-derived from Airflow
internals.

Structure: synthetic and strava are independent TaskGroups running in
parallel, both feeding into one dbt_build, then freshness_check and
volume_anomaly_check run in parallel after it. Strava has no real
credentials in dev, so its TaskGroup starts with a ShortCircuitOperator
that skips the whole branch (not fails it) when STRAVA_CLIENT_ID/SECRET/
REFRESH_TOKEN aren't set -- a missing integration isn't a pipeline failure.

dbt_build needs two things together to actually run when Strava is
skipped, not one -- verified this empirically (my first instinct, just
setting dbt_build's trigger_rule, silently failed on its own):
  1. ignore_downstream_trigger_rules=False on the ShortCircuitOperator --
     by default it aggressively skips its *entire* downstream subgraph
     regardless of trigger rules, which would skip dbt_build outright.
  2. trigger_rule="none_failed_min_one_success" on dbt_build itself, so a
     single skipped parent (strava.load_raw) doesn't block it via the
     default all_success rule, as long as synthetic actually succeeded.

catchup=False, deliberately: extract.py already tracks "since last sync"
itself via its own watermark (ingestion/clients/base_client.py). Turning on
Airflow's own catchup/backfill semantics on top of that would be a second,
conflicting incremental mechanism. Historical replay is scripts/backfill.sh's
job, not this DAG's.

max_active_runs=1, found necessary the hard way: unpausing this DAG fires
an automatic run for the most recent interval *at the same moment* a manual
trigger can queue a second one, and the two will race -- they share the
same local landing-zone/watermark state on disk, and load_raw.py's staging
tables aren't named per-run, so concurrent runs can stomp each other's
staging table mid-MERGE. Rather than making every script safe under
concurrent execution, the standard Airflow answer is to just not allow
overlapping runs of a pipeline whose tasks weren't designed for it.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

REPO_ROOT = "/opt/airflow/project"
LANDING_ZONE = f"{REPO_ROOT}/landing"
DBT_DIR = f"{REPO_ROOT}/dbt"

default_args = {
    "owner": "fitness-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _strava_is_configured() -> bool:
    return bool(
        os.environ.get("STRAVA_CLIENT_ID")
        and os.environ.get("STRAVA_CLIENT_SECRET")
        and os.environ.get("STRAVA_REFRESH_TOKEN")
    )


with DAG(
    dag_id="fitness_pipeline",
    description="extract -> load_raw -> dbt build -> quality checks",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    params={"project": "fitness-analytics-506523", "dataset": "raw"},
    tags=["fitness-analytics"],
) as dag:

    with TaskGroup(group_id="extract_synthetic") as synthetic_group:
        synthetic_extract = BashOperator(
            task_id="extract",
            bash_command=(
                f"python3 {REPO_ROOT}/ingestion/extract.py "
                f"--source synthetic --landing-zone {LANDING_ZONE} --entity all"
            ),
        )
        synthetic_load_raw = BashOperator(
            task_id="load_raw",
            bash_command=(
                f"python3 {REPO_ROOT}/ingestion/load_raw.py "
                f"--landing-zone {LANDING_ZONE} --source synthetic --entity all "
                "--project {{ params.project }} --dataset {{ params.dataset }}"
            ),
        )
        synthetic_extract >> synthetic_load_raw

    with TaskGroup(group_id="extract_strava") as strava_group:
        strava_configured = ShortCircuitOperator(
            task_id="skip_if_not_configured",
            python_callable=_strava_is_configured,
            ignore_downstream_trigger_rules=False,
        )
        strava_extract = BashOperator(
            task_id="extract",
            bash_command=(
                f"python3 {REPO_ROOT}/ingestion/extract.py "
                f"--source strava --landing-zone {LANDING_ZONE} --entity all"
            ),
        )
        strava_load_raw = BashOperator(
            task_id="load_raw",
            bash_command=(
                f"python3 {REPO_ROOT}/ingestion/load_raw.py "
                f"--landing-zone {LANDING_ZONE} --source strava --entity all "
                "--project {{ params.project }} --dataset {{ params.dataset }}"
            ),
        )
        strava_configured >> strava_extract >> strava_load_raw

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build",
        trigger_rule="none_failed_min_one_success",
        # retries=0, unlike everything else here: watched this fail live on
        # a real duration_under_hours violation, and retrying doesn't help
        # -- the anomalous row is still there on attempt 2. default_args'
        # retries=2 is for transient failures (a flaky network call in
        # extract/load_raw); a failed data-quality test isn't one.
        retries=0,
    )

    freshness_check = BashOperator(
        task_id="freshness_check",
        bash_command=(
            f"python3 {REPO_ROOT}/quality/freshness_check.py "
            "--project {{ params.project }} --dataset {{ params.dataset }}"
        ),
    )

    volume_anomaly_check = BashOperator(
        task_id="volume_anomaly_check",
        bash_command=(
            f"python3 {REPO_ROOT}/quality/volume_anomaly_check.py "
            "--project {{ params.project }} --dataset {{ params.dataset }}"
        ),
    )

    done = EmptyOperator(task_id="done")

    [synthetic_group, strava_group] >> dbt_build >> [freshness_check, volume_anomaly_check] >> done

"""Load landing-zone JSONL into the BigQuery raw dataset, partitioned by
synced_at (workouts) / updated_at (users) date.

Idempotent by construction: every load goes through a staging table and a
MERGE on the entity's natural key (workout_id, or user_id+source), not a
blind INSERT/append. Re-running load_raw.py against the same landing-zone
file -- because a run was retried, or because backfill.sh replayed a date
range -- updates existing rows in place instead of duplicating them. That's
what makes the whole extract -> load_raw round-trip safe to re-run.

--dry-run prints the MERGE SQL and the files it would load without touching
BigQuery at all, so this is testable without live GCP credentials.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("load_raw")

SAMPLE_STRUCT_FIELDS = [
    {"name": "ts", "type": "TIMESTAMP"},
    {"name": "heart_rate_bpm", "type": "INT64"},
    {"name": "lat", "type": "FLOAT64"},
    {"name": "lon", "type": "FLOAT64"},
    {"name": "elevation_m", "type": "FLOAT64"},
]

WORKOUTS_SCHEMA = [
    {"name": "workout_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED"},
    {"name": "workout_type", "type": "STRING"},
    {"name": "start_time", "type": "TIMESTAMP"},
    {"name": "end_time", "type": "TIMESTAMP"},
    {"name": "device_id", "type": "STRING"},
    {"name": "synced_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "distance_meters", "type": "FLOAT64"},
    {"name": "calories", "type": "FLOAT64"},
    {"name": "samples", "type": "RECORD", "mode": "REPEATED", "fields": SAMPLE_STRUCT_FIELDS},
]

USERS_SCHEMA = [
    {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED"},
    {"name": "display_name", "type": "STRING"},
    {"name": "email", "type": "STRING"},
    {"name": "age", "type": "INT64"},
    {"name": "weight_kg", "type": "FLOAT64"},
    {"name": "height_cm", "type": "FLOAT64"},
    {"name": "fitness_goal", "type": "STRING"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

ENTITY_CONFIG = {
    "workouts": {
        "table": "raw_workouts",
        "schema": WORKOUTS_SCHEMA,
        "natural_key": ["workout_id"],
        "watermark_field": "synced_at",
        "partition_field": "synced_at",
    },
    "users": {
        "table": "raw_users",
        "schema": USERS_SCHEMA,
        "natural_key": ["user_id", "source"],
        "watermark_field": "updated_at",
        "partition_field": "updated_at",
    },
}


def _bq_schema(fields: list[dict]):
    from google.cloud import bigquery

    def build(f):
        if f.get("type") == "RECORD":
            return bigquery.SchemaField(
                f["name"], "RECORD", mode=f.get("mode", "NULLABLE"),
                fields=[build(sub) for sub in f["fields"]],
            )
        return bigquery.SchemaField(f["name"], f["type"], mode=f.get("mode", "NULLABLE"))

    return [build(f) for f in fields]


def find_landing_files(landing_zone: Path, entity: str, source: str, dt: str | None) -> list[Path]:
    base = landing_zone / entity / f"source={source}"
    if not base.exists():
        return []
    pattern = f"dt={dt}/*.jsonl" if dt else "dt=*/*.jsonl"
    return sorted(base.glob(pattern))


def build_merge_sql(project: str, dataset: str, entity: str, staging_table: str) -> str:
    cfg = ENTITY_CONFIG[entity]
    target = f"`{project}.{dataset}.{cfg['table']}`"
    key_cond = " AND ".join(f"T.{k} = S.{k}" for k in cfg["natural_key"])
    all_cols = [f["name"] for f in cfg["schema"]]
    update_set = ", ".join(f"{c} = S.{c}" for c in all_cols)
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"S.{c}" for c in all_cols)
    wf = cfg["watermark_field"]
    return f"""
MERGE {target} T
USING `{project}.{dataset}.{staging_table}` S
ON {key_cond}
WHEN MATCHED AND S.{wf} > T.{wf} THEN
  UPDATE SET {update_set}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols}) VALUES ({insert_vals})
""".strip()


def load_entity(
    project: str,
    dataset: str,
    entity: str,
    files: list[Path],
    dry_run: bool = False,
) -> None:
    cfg = ENTITY_CONFIG[entity]
    if not files:
        logger.info("%s: no landing files to load", entity)
        return

    staging_table = f"_stg_{cfg['table']}_{entity}_load"
    sql = build_merge_sql(project, dataset, entity, staging_table)

    if dry_run:
        logger.info("[dry-run] would load %d file(s) into staging: %s", len(files), [str(f) for f in files])
        logger.info("[dry-run] MERGE SQL:\n%s", sql)
        return

    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset}"
    client.create_dataset(dataset_ref, exists_ok=True)

    target_ref = f"{dataset_ref}.{cfg['table']}"
    try:
        client.get_table(target_ref)
    except Exception:
        table = bigquery.Table(target_ref, schema=_bq_schema(cfg["schema"]))
        table.time_partitioning = bigquery.TimePartitioning(field=cfg["partition_field"])
        client.create_table(table)
        logger.info("created target table %s", target_ref)

    staging_ref = f"{dataset_ref}.{staging_table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=_bq_schema(cfg["schema"]),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(_concat_files(files), "rb") as f:
        job = client.load_table_from_file(f, staging_ref, job_config=job_config)
    job.result()
    logger.info("staged %d row(s) from %d file(s) into %s", job.output_rows, len(files), staging_ref)

    query_job = client.query(sql)
    query_job.result()
    logger.info("merged staging -> %s (%s)", target_ref, query_job.num_dml_affected_rows)

    client.delete_table(staging_ref, not_found_ok=True)


def _concat_files(files: list[Path]) -> Path:
    """BigQuery's load API wants one file handle; landing files are already
    JSONL, so just concatenate them into a scratch file for the load job."""
    import tempfile

    scratch = Path(tempfile.mkstemp(suffix=".jsonl")[1])
    with scratch.open("w") as out:
        for f in files:
            out.write(f.read_text())
    return scratch


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--landing-zone", type=Path, default=Path("landing"))
    p.add_argument("--source", default="synthetic")
    p.add_argument("--entity", choices=["users", "workouts", "all"], default="all")
    p.add_argument("--dt", default=None, help="specific dt=YYYY-MM-DD partition; default loads all available")
    p.add_argument("--project", default="fitness-analytics-dev")
    p.add_argument("--dataset", default="raw")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    entities = ("users", "workouts") if args.entity == "all" else (args.entity,)
    for entity in entities:
        files = find_landing_files(args.landing_zone, entity, args.source, args.dt)
        load_entity(args.project, args.dataset, entity, files, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

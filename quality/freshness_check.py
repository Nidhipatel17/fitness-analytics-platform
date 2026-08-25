"""Per-user freshness check: fail loudly if any user's workout data hasn't
synced in over 48 hours.

This is a different, finer-grained check than the source `freshness:` block
in dbt/models/staging/schema.yml. That one is table-level -- it asks "has
raw_workouts received *any* row recently" -- which one active user out of
hundreds is enough to satisfy. This asks the same question per user, so one
user's integration silently breaking doesn't hide behind everyone else's
data still flowing. Same layer (raw), different grain.

Exits 1 (and prints the offending users) if any user with prior workout
history has gone stale; exits 0 otherwise. Meant to run as an Airflow task
right after dbt build -- a nonzero exit fails that task natively.
"""

from __future__ import annotations

import argparse
import sys
from datetime import timedelta

from google.cloud import bigquery

STALE_THRESHOLD = timedelta(hours=48)

QUERY = """
select
    user_id,
    max(synced_at) as last_synced_at,
    timestamp_diff(current_timestamp(), max(synced_at), hour) as hours_since_sync
from `{project}.{dataset}.raw_workouts`
group by user_id
having timestamp_diff(current_timestamp(), max(synced_at), hour) > {threshold_hours}
order by hours_since_sync desc
"""


def check_freshness(project: str, dataset: str, threshold: timedelta = STALE_THRESHOLD) -> list[dict]:
    client = bigquery.Client(project=project)
    query = QUERY.format(project=project, dataset=dataset, threshold_hours=int(threshold.total_seconds() // 3600))
    rows = list(client.query(query).result())
    return [
        {"user_id": r.user_id, "last_synced_at": r.last_synced_at, "hours_since_sync": r.hours_since_sync}
        for r in rows
    ]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", default="raw")
    p.add_argument("--threshold-hours", type=int, default=48)
    args = p.parse_args()

    stale = check_freshness(args.project, args.dataset, timedelta(hours=args.threshold_hours))

    if stale:
        print(f"FRESHNESS CHECK FAILED: {len(stale)} user(s) with no sync in >{args.threshold_hours}h:")
        for u in stale:
            print(f"  {u['user_id']}: last synced {u['last_synced_at']} ({u['hours_since_sync']}h ago)")
        sys.exit(1)

    print(f"freshness check passed: all users with history have synced within {args.threshold_hours}h")


if __name__ == "__main__":
    main()

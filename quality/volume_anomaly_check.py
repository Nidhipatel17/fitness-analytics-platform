"""Volume anomaly check: flag if today's ingested row count is less than
50% of the trailing 7-day average -- the classic "did ingestion silently
break" alarm. Catches failures freshness_check.py can't: a source that's
still delivering *something* per user but at a fraction of its normal
volume (e.g. a broken pagination loop only returning page 1) wouldn't
necessarily trip a per-user staleness check, but it would tank the day's
row count.

Runs against raw_workouts (the ingestion layer), not the dbt marts, for the
same reason freshness_check.py does: this is about the health of the data
flow itself, not correctness of the transformed output -- dbt's own tests
already own that.

Skips (exit 0, not a failure) if there isn't enough trailing history yet to
compare against -- a brand-new project failing this check on day 2 would be
a false alarm, not a real signal.
"""

from __future__ import annotations

import argparse
import sys

from google.cloud import bigquery

MIN_TRAILING_DAYS = 3

QUERY = """
select
    date(synced_at) as sync_date,
    count(*) as row_count
from `{project}.{dataset}.raw_workouts`
where date(synced_at) between date_sub(current_date(), interval 8 day) and current_date()
group by sync_date
order by sync_date
"""


def check_volume(project: str, dataset: str, min_ratio: float = 0.5) -> dict | None:
    """Returns a dict describing the anomaly if one is found, else None."""
    client = bigquery.Client(project=project)
    query = QUERY.format(project=project, dataset=dataset)
    rows = {r.sync_date: r.row_count for r in client.query(query).result()}

    today = max(rows.keys()) if rows else None
    if today is None:
        return None

    today_count = rows.get(today, 0)
    trailing = {d: c for d, c in rows.items() if d != today}

    if len(trailing) < MIN_TRAILING_DAYS:
        return None  # not enough history to judge "normal" yet

    trailing_avg = sum(trailing.values()) / len(trailing)
    if trailing_avg == 0:
        return None  # nothing to compare against

    ratio = today_count / trailing_avg
    if ratio < min_ratio:
        return {
            "date": today,
            "today_count": today_count,
            "trailing_avg": round(trailing_avg, 1),
            "trailing_days": len(trailing),
            "ratio": round(ratio, 3),
        }
    return None


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", default="raw")
    p.add_argument("--min-ratio", type=float, default=0.5)
    args = p.parse_args()

    anomaly = check_volume(args.project, args.dataset, args.min_ratio)

    if anomaly:
        print(
            f"VOLUME ANOMALY CHECK FAILED: {anomaly['date']} had {anomaly['today_count']} rows, "
            f"only {anomaly['ratio']:.0%} of the {anomaly['trailing_days']}-day trailing average "
            f"({anomaly['trailing_avg']})"
        )
        sys.exit(1)

    print("volume anomaly check passed (or not enough history yet to judge)")


if __name__ == "__main__":
    main()

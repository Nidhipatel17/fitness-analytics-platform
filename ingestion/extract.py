"""Pull new records from a client and land them as JSON in the raw landing
zone, partitioned by synced_at date. This is the incremental-load layer:
in live mode it reads/advances a watermark via the client's get_since()/
set_since(); in backfill mode (both --since and --until given) it replays a
bounded historical window and never touches the watermark at all, so
backfill.sh can safely re-run any date range without disturbing live syncs.

Layout written under --landing-zone:
    <entity>/source=<source>/dt=<YYYY-MM-DD>/part-<run-id>.jsonl   (valid records, normalized)
    _rejects/<entity>/source=<source>/dt=<YYYY-MM-DD>/part-<run-id>.jsonl  (raw record + error)

Idempotency:
  - live mode: every run writes a new, uniquely-named file (append-only
    landing zone) and advances the watermark past everything it saw
    (including rejects, so a permanently-malformed record doesn't get
    refetched forever). load_raw.py's MERGE on natural key is what makes
    re-loading those files into BigQuery safe even if a run is retried.
  - backfill mode: the output filename is deterministic (derived from the
    window, not wall-clock time) and is *overwritten* on each replay, so
    running the same date range twice produces the same file, not two.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.clients.base_client import BaseFitnessClient
from ingestion.clients.strava_client import StravaClient
from ingestion.clients.synthetic_client import SyntheticClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("extract")

CLIENTS = {"synthetic": SyntheticClient, "strava": StravaClient}

# friendships is synthetic-only (see SyntheticClient.fetch_friendships) --
# not every source needs to support every entity, so this is keyed
# separately rather than forced into a signature every client must match.
ENTITY_CONFIG = {
    "users": {"fetch": "fetch_users", "normalize": "normalize_user", "watermark_field": "updated_at"},
    "workouts": {"fetch": "fetch_workouts", "normalize": "normalize_workout", "watermark_field": "synced_at"},
    "friendships": {"fetch": "fetch_friendships", "normalize": "normalize_friendship", "watermark_field": "created_at"},
}
ENTITY_SOURCE_SUPPORT = {
    "users": set(CLIENTS),
    "workouts": set(CLIENTS),
    "friendships": {"synthetic"},
}


def build_client(source: str, **client_kwargs: Any) -> BaseFitnessClient:
    if source not in CLIENTS:
        raise ValueError(f"unknown source {source!r}, choose from {list(CLIENTS)}")
    return CLIENTS[source](**client_kwargs)


def _dt_of(record: dict[str, Any], field_name: str) -> str:
    return record[field_name][:10]


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _run_id(since: datetime | None, until: datetime | None) -> tuple[str, bool]:
    """Returns (id, is_backfill). Backfill ids are deterministic so replays
    overwrite the same file instead of piling up duplicates."""
    if until is not None:
        since_s = since.isoformat() if since else "epoch"
        return f"backfill-{since_s}-{until.isoformat()}".replace(":", ""), True
    now = datetime.now(UTC)
    return now.strftime("%Y%m%dT%H%M%S%f"), False


def extract_entity(
    client: BaseFitnessClient,
    entity: str,
    landing_zone: Path,
    since: datetime | None,
    until: datetime | None,
) -> tuple[int, int, datetime | None]:
    """Returns (valid_count, rejected_count, max_synced_at_seen)."""
    cfg = ENTITY_CONFIG[entity]
    fetch = getattr(client, cfg["fetch"])
    normalize = getattr(client, cfg["normalize"])
    watermark_field = cfg["watermark_field"]

    run_id, _ = _run_id(since, until)
    valid_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    rejects_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    max_synced_at: datetime | None = None

    raw_records = list(fetch(since=since, until=until))
    for raw in raw_records:
        ts_raw = raw.get(watermark_field)
        if ts_raw:
            ts = datetime.fromisoformat(ts_raw)
            if max_synced_at is None or ts > max_synced_at:
                max_synced_at = ts
        try:
            normalized = normalize(raw)
            valid_by_date[_dt_of(normalized, watermark_field)].append(normalized)
        except ValueError as exc:
            dt = ts_raw[:10] if ts_raw else datetime.now(UTC).date().isoformat()
            rejects_by_date[dt].append({"error": str(exc), "raw": raw})
            logger.warning("rejected %s record: %s", entity, exc)

    for dt, records in valid_by_date.items():
        out = landing_zone / entity / f"source={client.source_name}" / f"dt={dt}" / f"part-{run_id}.jsonl"
        _write_jsonl(out, records)
        logger.info("wrote %d %s records -> %s", len(records), entity, out)

    for dt, records in rejects_by_date.items():
        out = landing_zone / "_rejects" / entity / f"source={client.source_name}" / f"dt={dt}" / f"part-{run_id}.jsonl"
        _write_jsonl(out, records)
        logger.info("wrote %d rejected %s records -> %s", len(records), entity, out)

    total_valid = sum(len(v) for v in valid_by_date.values())
    total_rejected = sum(len(v) for v in rejects_by_date.values())
    return total_valid, total_rejected, max_synced_at


def run(
    source: str,
    landing_zone: Path,
    entities: Iterable[str] = ("users", "workouts"),
    since_override: datetime | None = None,
    until_override: datetime | None = None,
    client_kwargs: dict[str, Any] | None = None,
) -> None:
    client = build_client(source, **(client_kwargs or {}))
    is_backfill = until_override is not None

    for entity in entities:
        since = since_override if is_backfill else client.get_since(entity=entity)
        until = until_override
        valid, rejected, max_synced_at = extract_entity(client, entity, landing_zone, since, until)
        logger.info(
            "%s/%s: since=%s until=%s -> %d valid, %d rejected",
            source, entity, since, until, valid, rejected,
        )
        if not is_backfill and max_synced_at is not None:
            client.set_since(max_synced_at, entity=entity)
            logger.info("%s/%s: watermark advanced to %s", source, entity, max_synced_at)


def _parse_cli_timestamp(value: str | None) -> datetime | None:
    """Everything downstream (watermarks, client filtering) is timezone-aware
    UTC. A CLI value with no offset -- e.g. --since 2026-08-01, the natural
    way to type it -- would otherwise crash comparisons deep inside the
    client with "can't compare offset-naive and offset-aware datetimes".
    Assume UTC for a bare value rather than rejecting it, since this whole
    pipeline is UTC-only anyway."""
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", choices=list(CLIENTS), default="synthetic")
    p.add_argument("--landing-zone", type=Path, default=Path("landing"))
    p.add_argument("--entity", choices=["users", "workouts", "friendships", "all"], default="all")
    p.add_argument(
        "--since", type=str, default=None,
        help="ISO timestamp (UTC assumed if no offset given); backfill mode requires this with --until",
    )
    p.add_argument(
        "--until", type=str, default=None,
        help="ISO timestamp (UTC assumed if no offset given); presence of --until triggers backfill/replay mode",
    )
    p.add_argument("--seed", type=int, default=None, help="synthetic source only: RNG seed for reproducible generation")
    p.add_argument("--malformed-rate", type=float, default=None, help="synthetic source only")
    p.add_argument(
        "--anomaly-rate", type=float, default=None,
        help="synthetic source only; set to 0 for deterministic, anomaly-free seed data (e.g. in CI, where a "
             "randomly-injected business-rule violation would make dbt test flaky rather than actually broken)",
    )
    p.add_argument("--duplicate-rate", type=float, default=None, help="synthetic source only")
    p.add_argument("--late-arrival-rate", type=float, default=None, help="synthetic source only")
    args = p.parse_args()

    # "all" means "every entity this source supports" -- friendships is
    # synthetic-only, so it's opt-in via --entity friendships explicitly
    # rather than silently included/excluded depending on --source.
    entities = ("users", "workouts") if args.entity == "all" else (args.entity,)
    unsupported = [e for e in entities if args.source not in ENTITY_SOURCE_SUPPORT[e]]
    if unsupported:
        p.error(f"--source {args.source} doesn't support entity/entities: {unsupported}")

    since_override = _parse_cli_timestamp(args.since)
    until_override = _parse_cli_timestamp(args.until)
    if until_override is not None and since_override is None:
        p.error("--until requires --since (bounded backfill window)")

    rate_flags = {
        "seed": args.seed,
        "malformed_rate": args.malformed_rate,
        "anomaly_rate": args.anomaly_rate,
        "duplicate_rate": args.duplicate_rate,
        "late_arrival_rate": args.late_arrival_rate,
    }
    given_rate_flags = {k: v for k, v in rate_flags.items() if v is not None}
    if given_rate_flags and args.source != "synthetic":
        p.error(f"--source {args.source} doesn't accept {list(given_rate_flags)} (synthetic-only)")

    run(args.source, args.landing_zone, entities, since_override, until_override, client_kwargs=given_rate_flags)


if __name__ == "__main__":
    main()

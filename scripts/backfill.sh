#!/usr/bin/env bash
# Replay extract + load_raw for an arbitrary past date range, one UTC day at
# a time. Safe to re-run: extract.py's backfill mode writes a deterministic
# filename per window (overwritten, not appended) and load_raw.py MERGEs on
# natural key -- so running this twice for the same range is a no-op, not a
# duplicate.
#
# Usage:
#   scripts/backfill.sh <source> <start-date YYYY-MM-DD> <end-date YYYY-MM-DD> [landing-zone] [project] [dataset] [--dry-run]
#
# Example:
#   scripts/backfill.sh synthetic 2026-08-01 2026-08-07
#   scripts/backfill.sh synthetic 2026-08-01 2026-08-07 landing fitness-analytics-dev raw --dry-run

set -euo pipefail

SOURCE="${1:?source required (synthetic|strava)}"
START_DATE="${2:?start date required, YYYY-MM-DD}"
END_DATE="${3:?end date required, YYYY-MM-DD}"
LANDING_ZONE="${4:-landing}"
PROJECT="${5:-fitness-analytics-dev}"
DATASET="${6:-raw}"
DRY_RUN="${7:-}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "backfilling ${SOURCE} from ${START_DATE} to ${END_DATE} (inclusive, UTC days) into ${LANDING_ZONE}"

# Enumerate UTC days with python so this works the same on macOS (BSD date)
# and Linux (GNU date) without juggling two `date` dialects.
DAYS=$(python3 - "$START_DATE" "$END_DATE" <<'PY'
import sys
from datetime import date, timedelta

start = date.fromisoformat(sys.argv[1])
end = date.fromisoformat(sys.argv[2])
d = start
while d <= end:
    print(d.isoformat())
    d += timedelta(days=1)
PY
)

for DAY in $DAYS; do
    NEXT_DAY=$(python3 -c "from datetime import date, timedelta; print((date.fromisoformat('${DAY}') + timedelta(days=1)).isoformat())")
    SINCE="${DAY}T00:00:00+00:00"
    UNTIL="${NEXT_DAY}T00:00:00+00:00"

    echo "--- ${DAY} ---"
    python3 "${REPO_ROOT}/ingestion/extract.py" \
        --source "${SOURCE}" \
        --landing-zone "${LANDING_ZONE}" \
        --entity all \
        --since "${SINCE}" \
        --until "${UNTIL}"

    LOAD_ARGS=(--landing-zone "${LANDING_ZONE}" --source "${SOURCE}" --entity all --dt "${DAY}" --project "${PROJECT}" --dataset "${DATASET}")
    if [[ "${DRY_RUN}" == "--dry-run" ]]; then
        LOAD_ARGS+=(--dry-run)
    fi
    python3 "${REPO_ROOT}/ingestion/load_raw.py" "${LOAD_ARGS[@]}"
done

echo "backfill complete: ${SOURCE} ${START_DATE}..${END_DATE}"

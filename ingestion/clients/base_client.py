"""Abstract interface every ingestion source (synthetic, Strava, ...) implements.

Canonical (post-normalize) shapes
----------------------------------
user:
    {
        "user_id": str,
        "source": str,
        "display_name": str,
        "email": str,
        "age": int,
        "weight_kg": float,
        "height_cm": float,
        "fitness_goal": str,          # "lose_weight" | "build_endurance" | "build_strength" | "maintain"
        "updated_at": str (ISO 8601), # drives dim_user SCD2 downstream
    }

workout:
    {
        "workout_id": str,
        "user_id": str,
        "source": str,
        "workout_type": str,          # "run" | "ride" | "swim" | "strength" | "walk" | ...
        "start_time": str (ISO 8601),
        "end_time": str (ISO 8601),
        "device_id": str,
        "synced_at": str (ISO 8601),  # when the source made this record available -- the watermark field
        "distance_meters": float | None,
        "calories": float | None,
        "samples": [
            {
                "ts": str (ISO 8601),
                "heart_rate_bpm": int | None,
                "lat": float | None,
                "lon": float | None,
                "elevation_m": float | None,
            },
            ...
        ],
    }

Incremental loads are driven by `synced_at`, not `start_time`: a workout that
happened last week but only became visible to us today (a late-arriving
record, or a watch that synced late) has `start_time` in the past and
`synced_at` ~= now. Watermarking on `synced_at` is what makes `get_since()`
correct for that case.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
import json

DEFAULT_STATE_DIR = Path(__file__).resolve().parents[2] / ".state"


class BaseFitnessClient(ABC):
    """Common contract for pulling workout/user data from a source.

    Subclasses implement `fetch_users`, `fetch_workouts`, `normalize_user`,
    and `normalize_workout`. Watermark persistence (`get_since`/`set_since`)
    is shared here so every source is incremental the same way.
    """

    #: short, filesystem-safe identifier, e.g. "synthetic", "strava"
    source_name: str = "base"

    def __init__(self, state_dir: Path | str = DEFAULT_STATE_DIR):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _watermark_path(self, entity: str) -> Path:
        return self.state_dir / f"{self.source_name}_{entity}_watermark.json"

    def get_since(self, entity: str = "workouts") -> Optional[datetime]:
        """Return the cursor of the last successful extract for `entity`
        ("workouts" or "users"), or None if never synced (implies a full
        backfill). Each entity gets its own watermark -- workouts watermark
        on synced_at, users watermark on updated_at, and they advance at
        different rates."""
        path = self._watermark_path(entity)
        if not path.exists():
            return None
        raw = json.loads(path.read_text())
        value = raw.get("watermark")
        return datetime.fromisoformat(value) if value else None

    def set_since(self, watermark: datetime, entity: str = "workouts") -> None:
        """Persist the new watermark for `entity`. Callers should only do this
        after raw data has been durably written -- never before, or a crash
        between "advance watermark" and "write data" silently drops records."""
        self._watermark_path(entity).write_text(json.dumps({"watermark": watermark.isoformat()}))

    @abstractmethod
    def fetch_users(
        self, since: Optional[datetime] = None, until: Optional[datetime] = None
    ) -> Iterable[dict[str, Any]]:
        """Yield raw (source-shaped, not-yet-normalized) user records updated
        in (since, until]. `since=None` means full history; `until=None` means
        no upper bound (live tail). Passing a concrete `until` puts the client
        in bounded-replay mode: it must return the exact same records every
        time for the same window, and must not mutate any external/simulated
        state -- this is what backfill.sh relies on for idempotency."""
        raise NotImplementedError

    @abstractmethod
    def fetch_workouts(
        self, since: Optional[datetime] = None, until: Optional[datetime] = None
    ) -> Iterable[dict[str, Any]]:
        """Yield raw (source-shaped, not-yet-normalized) workout records with
        synced_at in (since, until]. Same replay contract as `fetch_users`."""
        raise NotImplementedError

    @abstractmethod
    def normalize_user(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Map one raw user record to the canonical user shape."""
        raise NotImplementedError

    @abstractmethod
    def normalize_workout(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Map one raw workout record to the canonical workout shape."""
        raise NotImplementedError


def utcnow() -> datetime:
    return datetime.now(timezone.utc)

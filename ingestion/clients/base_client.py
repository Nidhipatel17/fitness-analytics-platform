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


"""

import json
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_STATE_DIR = Path(__file__).resolve().parents[2] / ".state"


class BaseFitnessClient(ABC):
  

    #: short, filesystem-safe identifier, e.g. "synthetic", "strava"
    source_name: str = "base"

    def __init__(self, state_dir: Path | str = DEFAULT_STATE_DIR):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _watermark_path(self, entity: str) -> Path:
        return self.state_dir / f"{self.source_name}_{entity}_watermark.json"

    def get_since(self, entity: str = "workouts") -> datetime | None:
    
        path = self._watermark_path(entity)
        if not path.exists():
            return None
        raw = json.loads(path.read_text())
        value = raw.get("watermark")
        return datetime.fromisoformat(value) if value else None

    def set_since(self, watermark: datetime, entity: str = "workouts") -> None:
  
        self._watermark_path(entity).write_text(json.dumps({"watermark": watermark.isoformat()}))

    @abstractmethod
    def fetch_users(
        self, since: datetime | None = None, until: datetime | None = None
    ) -> Iterable[dict[str, Any]]:
     
        raise NotImplementedError

    @abstractmethod
    def fetch_workouts(
        self, since: datetime | None = None, until: datetime | None = None
    ) -> Iterable[dict[str, Any]]:
        """Yield raw  Same replay contract as `fetch_users`."""
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
    return datetime.now(UTC)

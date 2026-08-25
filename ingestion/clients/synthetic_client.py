"""Synthetic data source: a fake but internally-consistent fitness API.

Why synthetic-first: a real API won't reliably hand back malformed payloads,
duplicate deliveries, or late-arriving records on demand, and those three
behaviors are exactly what the extract/load layer has to prove it handles.
This client simulates a real backend that accumulates history across calls
(persisted to a small JSON "server state" file) and gives the caller direct
knobs over how badly it misbehaves.

Two modes, matching the BaseFitnessClient replay contract:
  - live mode (`until=None`): each call may mint new users/workouts, evolve
    existing users (weight/goal drift, for dim_user SCD2), and re-deliver a
    duplicate. The simulated backend advances -- this models "time passing".
  - bounded-replay mode (`until` given): pure read against already-recorded
    history, no mutation. Calling it twice for the same [since, until] window
    returns byte-identical records. This is what backfill.sh depends on.
"""

from __future__ import annotations

import copy
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .base_client import DEFAULT_STATE_DIR, BaseFitnessClient, utcnow

WORKOUT_TYPES = ["run", "ride", "swim", "strength", "walk", "hike", "rower"]
FITNESS_GOALS = ["lose_weight", "build_endurance", "build_strength", "maintain"]
FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Avery",
    "Priya", "Wei", "Diego", "Fatima", "Noah", "Ines", "Sam", "Lena",
]
LAST_NAMES = [
    "Kim", "Patel", "Garcia", "Chen", "Okafor", "Rossi", "Nguyen", "Silva",
    "Novak", "Haddad", "Berg", "Costa",
]


@dataclass
class _BatchStats:
    """Counts from the most recent fetch_workouts() call, exposed for tests
    and for operators eyeballing what a run actually did."""

    new: int = 0
    malformed: int = 0
    anomalous: int = 0
    duplicate: int = 0
    late_arriving: int = 0


class SyntheticClient(BaseFitnessClient):
    source_name = "synthetic"

    def __init__(
        self,
        num_users: int = 25,
        new_workouts_per_fetch: int = 15,
        malformed_rate: float = 0.05,
        anomaly_rate: float = 0.05,
        duplicate_rate: float = 0.10,
        late_arrival_rate: float = 0.15,
        seed: int | None = None,
        state_dir: Path | str = DEFAULT_STATE_DIR,
        store_path: Path | str | None = None,
    ):
        super().__init__(state_dir)
        self.num_users = num_users
        self.new_workouts_per_fetch = new_workouts_per_fetch
        self.malformed_rate = malformed_rate
        self.anomaly_rate = anomaly_rate
        self.duplicate_rate = duplicate_rate
        self.late_arrival_rate = late_arrival_rate
        self.rng = random.Random(seed)
        self.store_path = Path(store_path) if store_path else self.state_dir / "synthetic_backend.json"
        self.last_batch_stats = _BatchStats()
        self.backend: dict[str, Any] = self._load_or_bootstrap_backend()

    # ---------------------------------------------------------------- backend

    def _load_or_bootstrap_backend(self) -> dict[str, Any]:
        if self.store_path.exists():
            return json.loads(self.store_path.read_text())
        now = utcnow()
        users = [self._make_user(i, now) for i in range(self.num_users)]
        friendships = self._generate_friendships(users, now)
        return {"users": users, "workouts": [], "friendships": friendships, "next_workout_seq": 0}

    def _generate_friendships(self, users: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
        """Static at bootstrap, not incrementally synced like workouts/users
        -- a synthetic social graph doesn't need to evolve for this
        project's purposes. Symmetric: each edge is generated once, then
        written both directions, so a leaderboard query never needs an OR
        or a UNION to find "my friends"."""
        user_ids = [u["user_id"] for u in users]
        edges: set[tuple[str, str]] = set()
        for uid in user_ids:
            candidates = [u for u in user_ids if u != uid]
            n_friends = min(self.rng.randint(3, 6), len(candidates))
            for fid in self.rng.sample(candidates, n_friends):
                edges.add(tuple(sorted((uid, fid))))

        friendships = []
        for a, b in edges:
            ts = now.isoformat()
            friendships.append({"user_id": a, "friend_user_id": b, "source": self.source_name, "created_at": ts})
            friendships.append({"user_id": b, "friend_user_id": a, "source": self.source_name, "created_at": ts})
        return friendships

    def _save_backend(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        self.store_path.write_text(json.dumps(self.backend, indent=2))

    def _next_workout_id(self) -> str:
        self.backend["next_workout_seq"] += 1
        return f"syn-{self.backend['next_workout_seq']:08d}"

    def _make_user(self, i: int, now: datetime) -> dict[str, Any]:
        first = self.rng.choice(FIRST_NAMES)
        last = self.rng.choice(LAST_NAMES)
        return {
            "user_id": f"u-{i:04d}",
            "source": self.source_name,
            "display_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "age": self.rng.randint(18, 65),
            "weight_kg": round(self.rng.uniform(52, 100), 1),
            "height_cm": round(self.rng.uniform(155, 195), 1),
            "fitness_goal": self.rng.choice(FITNESS_GOALS),
            "updated_at": now.isoformat(),
            "_device_id": f"device-{i:04d}",
        }

    # ---------------------------------------------------------------- users

    def _maybe_evolve_users(self, now: datetime) -> None:
        for u in self.backend["users"]:
            if self.rng.random() < 0.15:
                u["weight_kg"] = round(u["weight_kg"] + self.rng.uniform(-1.5, 1.5), 1)
                if self.rng.random() < 0.2:
                    u["fitness_goal"] = self.rng.choice(FITNESS_GOALS)
                u["updated_at"] = now.isoformat()

    def fetch_users(self, since=None, until=None):
        if until is None:
            self._maybe_evolve_users(utcnow())
            self._save_backend()
        return self._filter_window(self.backend["users"], "updated_at", since, until)

    def normalize_user(self, raw: dict[str, Any]) -> dict[str, Any]:
        required = ["user_id", "display_name", "email", "age", "weight_kg", "height_cm", "fitness_goal", "updated_at"]
        missing = [f for f in required if f not in raw or raw[f] is None]
        if missing:
            raise ValueError(f"malformed user record {raw.get('user_id')!r}: missing/null {missing}")
        return {
            "user_id": str(raw["user_id"]),
            "source": self.source_name,
            "display_name": str(raw["display_name"]),
            "email": str(raw["email"]),
            "age": int(raw["age"]),
            "weight_kg": float(raw["weight_kg"]),
            "height_cm": float(raw["height_cm"]),
            "fitness_goal": str(raw["fitness_goal"]),
            "updated_at": raw["updated_at"],
        }

    # ---------------------------------------------------------- friendships

    def fetch_friendships(self, since=None, until=None) -> list[dict[str, Any]]:
        """Not part of BaseFitnessClient's interface -- synthetic-only.
        Strava has no comparable "friends" graph reachable through the API
        surface this project pulls from, so this isn't something every
        source could reasonably implement the same way. since/until use the
        same _filter_window mechanism as fetch_users/fetch_workouts (keyed
        on created_at) purely so extract.py can treat this as a uniform
        third entity instead of a special case -- the backend list itself
        is static after bootstrap either way."""
        return self._filter_window(self.backend.get("friendships", []), "created_at", since, until)

    def normalize_friendship(self, raw: dict[str, Any]) -> dict[str, Any]:
        required = ["user_id", "friend_user_id", "created_at"]
        missing = [f for f in required if f not in raw or raw[f] is None]
        if missing:
            raise ValueError(f"malformed friendship record: missing/null {missing}")
        return {
            "user_id": str(raw["user_id"]),
            "friend_user_id": str(raw["friend_user_id"]),
            "source": self.source_name,
            "created_at": raw["created_at"],
        }

    # ------------------------------------------------------------- workouts

    def _generate_sample(self, start: datetime, minute_offset: int, workout_type: str) -> dict[str, Any]:
        ts = start + timedelta(minutes=minute_offset)
        base_hr = {"run": 150, "ride": 140, "swim": 135, "strength": 120, "walk": 100, "hike": 125, "rower": 145}
        hr = base_hr.get(workout_type, 130) + self.rng.randint(-10, 15)
        outdoor = workout_type in ("run", "ride", "walk", "hike")
        return {
            "ts": ts.isoformat(),
            "heart_rate_bpm": max(60, hr),
            "lat": round(37.75 + self.rng.uniform(-0.05, 0.05), 6) if outdoor else None,
            "lon": round(-122.42 + self.rng.uniform(-0.05, 0.05), 6) if outdoor else None,
            "elevation_m": round(self.rng.uniform(0, 80), 1) if outdoor else None,
        }

    def _generate_new_workout(self, user: dict[str, Any], now: datetime) -> dict[str, Any]:
        is_late = self.rng.random() < self.late_arrival_rate
        if is_late:
            start = now - timedelta(days=self.rng.uniform(1, 14), hours=self.rng.uniform(0, 23))
            self.last_batch_stats.late_arriving += 1
        else:
            start = now - timedelta(hours=self.rng.uniform(0, 6))
        duration_min = self.rng.uniform(15, 90)
        end = start + timedelta(minutes=duration_min)
        workout_type = self.rng.choice(WORKOUT_TYPES)
        n_samples = max(1, int(duration_min))
        samples = [self._generate_sample(start, i, workout_type) for i in range(n_samples)]
        distance = None
        if workout_type in ("run", "ride", "walk", "hike"):
            pace_m_per_min = {"run": 170, "ride": 450, "walk": 80, "hike": 90}[workout_type]
            distance = round(duration_min * pace_m_per_min * self.rng.uniform(0.85, 1.15), 1)
        return {
            "workout_id": self._next_workout_id(),
            "user_id": user["user_id"],
            "source": self.source_name,
            "workout_type": workout_type,
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "device_id": user["_device_id"],
            "synced_at": now.isoformat(),
            "distance_meters": distance,
            "calories": round(duration_min * self.rng.uniform(6, 12), 1),
            "samples": samples,
        }

    def _corrupt_structurally(self, w: dict[str, Any]) -> dict[str, Any]:
        kind = self.rng.choice(
            ["missing_device_id", "null_user_id", "calories_wrong_type", "samples_wrong_type", "missing_workout_type"]
        )
        if kind == "missing_device_id":
            w.pop("device_id", None)
        elif kind == "null_user_id":
            w["user_id"] = None
        elif kind == "calories_wrong_type":
            w["calories"] = "unknown"
        elif kind == "samples_wrong_type":
            w["samples"] = "not-a-list"
        elif kind == "missing_workout_type":
            w.pop("workout_type", None)
        self.last_batch_stats.malformed += 1
        return w

    def _inject_anomaly(self, w: dict[str, Any]) -> dict[str, Any]:
        kind = self.rng.choice(["long_duration", "negative_distance"])
        if kind == "long_duration":
            start = datetime.fromisoformat(w["start_time"])
            w["end_time"] = (start + timedelta(hours=self.rng.uniform(25, 72))).isoformat()
        else:
            w["distance_meters"] = -abs(w.get("distance_meters") or 1000.0)
        self.last_batch_stats.anomalous += 1
        return w

    def _maybe_duplicate_existing(self, now: datetime) -> dict[str, Any] | None:
        if not self.backend["workouts"] or self.rng.random() >= self.duplicate_rate:
            return None
        dup = copy.deepcopy(self.rng.choice(self.backend["workouts"]))
        dup["synced_at"] = now.isoformat()
        self.last_batch_stats.duplicate += 1
        return dup

    def fetch_workouts(self, since=None, until=None):
        if until is not None:
            return self._filter_window(self.backend["workouts"], "synced_at", since, until)

        now = utcnow()
        self.last_batch_stats = _BatchStats()
        users_by_id = {u["user_id"]: u for u in self.backend["users"]}
        new_records = []
        for _ in range(self.new_workouts_per_fetch):
            user = self.rng.choice(list(users_by_id.values()))
            w = self._generate_new_workout(user, now)
            roll = self.rng.random()
            if roll < self.malformed_rate:
                w = self._corrupt_structurally(w)
            elif roll < self.malformed_rate + self.anomaly_rate:
                w = self._inject_anomaly(w)
            new_records.append(w)
            self.last_batch_stats.new += 1

        dup = self._maybe_duplicate_existing(now)
        if dup:
            new_records.append(dup)

        self.backend["workouts"].extend(new_records)
        self._save_backend()
        return self._filter_window(self.backend["workouts"], "synced_at", since, None)

    def normalize_workout(self, raw: dict[str, Any]) -> dict[str, Any]:
        required = ["workout_id", "user_id", "workout_type", "start_time", "end_time", "device_id", "synced_at"]
        missing = [f for f in required if f not in raw or raw[f] is None]
        if missing:
            raise ValueError(f"malformed workout record {raw.get('workout_id')!r}: missing/null {missing}")
        if not isinstance(raw.get("samples", []), list):
            raise ValueError(f"malformed workout record {raw['workout_id']!r}: samples is not a list")
        try:
            calories = float(raw["calories"]) if raw.get("calories") is not None else None
        except (TypeError, ValueError) as exc:
            raise ValueError(f"malformed workout record {raw['workout_id']!r}: bad calories value") from exc

        return {
            "workout_id": str(raw["workout_id"]),
            "user_id": str(raw["user_id"]),
            "source": self.source_name,
            "workout_type": str(raw["workout_type"]),
            "start_time": raw["start_time"],
            "end_time": raw["end_time"],
            "device_id": str(raw["device_id"]),
            "synced_at": raw["synced_at"],
            "distance_meters": float(raw["distance_meters"]) if raw.get("distance_meters") is not None else None,
            "calories": calories,
            "samples": raw.get("samples", []),
        }

    # ---------------------------------------------------------------- utils

    @staticmethod
    def _filter_window(records: list[dict[str, Any]], field_name: str, since: datetime | None, until: datetime | None):
        out = []
        for r in records:
            ts = datetime.fromisoformat(r[field_name])
            if since is not None and ts <= since:
                continue
            if until is not None and ts > until:
                continue
            out.append(copy.deepcopy(r))
        return out

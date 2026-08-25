
from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from .base_client import DEFAULT_STATE_DIR, BaseFitnessClient

TOKEN_URL = "https://www.strava.com/oauth/token"
API_BASE = "https://www.strava.com/api/v3"

# Strava's activity "type" values we care about, mapped to our canonical vocabulary.
TYPE_MAP = {
    "Run": "run", "TrailRun": "run", "VirtualRun": "run",
    "Ride": "ride", "VirtualRide": "ride", "MountainBikeRide": "ride",
    "Swim": "swim",
    "Walk": "walk", "Hike": "hike",
    "WeightTraining": "strength", "Workout": "strength",
    "Rowing": "rower",
}


class StravaAuthError(RuntimeError):
    pass


class StravaClient(BaseFitnessClient):
    source_name = "strava"

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        fetch_streams: bool = True,
        per_page: int = 100,
        state_dir: Path | str = DEFAULT_STATE_DIR,
        session: requests.Session | None = None,
    ):
        super().__init__(state_dir)
        self.client_id = client_id or os.environ.get("STRAVA_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("STRAVA_CLIENT_SECRET")
        self.refresh_token = refresh_token or os.environ.get("STRAVA_REFRESH_TOKEN")
        self.fetch_streams = fetch_streams
        self.per_page = per_page
        self.session = session or requests.Session()
        self._access_token: str | None = None
        self._access_token_expires_at: float = 0.0

    # ------------------------------------------------------------- auth

    def _ensure_access_token(self) -> str:
        if self._access_token and time.time() < self._access_token_expires_at - 60:
            return self._access_token
        if not (self.client_id and self.client_secret and self.refresh_token):
            raise StravaAuthError(
                "missing STRAVA_CLIENT_ID / STRAVA_CLIENT_SECRET / STRAVA_REFRESH_TOKEN"
            )
        resp = self.session.post(
            TOKEN_URL,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._access_token = payload["access_token"]
        self._access_token_expires_at = payload["expires_at"]
        # Strava may rotate the refresh token; keep using the latest one for this process.
        self.refresh_token = payload.get("refresh_token", self.refresh_token)
        return self._access_token

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        token = self._ensure_access_token()
        url = f"{API_BASE}{path}"
        for attempt in range(5):
            resp = self.session.get(
                url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=30
            )
            if resp.status_code == 429:
                # simple backoff against Strava's 100-req/15-min rate limit
                time.sleep(min(2 ** attempt, 30))
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    # ------------------------------------------------------------- users

    def fetch_users(self, since=None, until=None):
 
        athlete = self._get("/athlete")
        yield athlete

    def normalize_user(self, raw: dict[str, Any]) -> dict[str, Any]:
        required = ["id"]
        missing = [f for f in required if raw.get(f) is None]
        if missing:
            raise ValueError(f"malformed strava athlete record: missing {missing}")
        weight_kg = raw.get("weight")  # only present if athlete shared it
        updated_at = raw.get("updated_at") or datetime.now(UTC).isoformat()
        return {
            "user_id": f"strava-{raw['id']}",
            "source": self.source_name,
            "display_name": (
                " ".join(filter(None, [raw.get("firstname"), raw.get("lastname")])) or f"Athlete {raw['id']}"
            ),
            "email": raw.get("email") or f"strava-{raw['id']}@unknown.example",
            "age": None,  # Strava doesn't expose birthdate via this scope
            "weight_kg": float(weight_kg) if weight_kg else None,
            "height_cm": None,
            "fitness_goal": None,
            "updated_at": updated_at,
        }

    # ---------------------------------------------------------- workouts

    def fetch_workouts(self, since=None, until=None):

        pulled_at = datetime.now(UTC).isoformat()
        params: dict[str, Any] = {"per_page": self.per_page, "page": 1}
        if since is not None:
            params["after"] = int(since.timestamp())
        if until is not None:
            params["before"] = int(until.timestamp())

        page = 1
        while True:
            params["page"] = page
            batch = self._get("/athlete/activities", params=params)
            if not batch:
                break
            for activity in batch:
                activity["_synced_at"] = pulled_at
                if self.fetch_streams:
                    activity["_streams"] = self._fetch_activity_streams(activity["id"])
                yield activity
            if len(batch) < self.per_page:
                break
            page += 1

    def _fetch_activity_streams(self, activity_id: int) -> dict[str, Any]:
        keys = "time,heartrate,latlng,altitude"
        try:
            return self._get(
                f"/activities/{activity_id}/streams",
                params={"keys": keys, "key_by_type": "true"},
            )
        except requests.HTTPError:
            # streams can 404 for manually-entered activities with no recorded data
            return {}

    def normalize_workout(self, raw: dict[str, Any]) -> dict[str, Any]:
        required = ["id", "athlete", "type", "start_date", "elapsed_time"]
        missing = [f for f in required if raw.get(f) is None]
        if missing:
            raise ValueError(f"malformed strava activity {raw.get('id')!r}: missing {missing}")

        start = datetime.fromisoformat(raw["start_date"].replace("Z", "+00:00"))
        end = start.timestamp() + float(raw["elapsed_time"])
        end_iso = datetime.fromtimestamp(end, tz=UTC).isoformat()
        synced_at = raw.get("_synced_at") or datetime.now(UTC).isoformat()

        return {
            "workout_id": f"strava-{raw['id']}",
            "user_id": f"strava-{raw['athlete']['id']}",
            "source": self.source_name,
            "workout_type": TYPE_MAP.get(raw["type"], raw["type"].lower()),
            "start_time": start.isoformat(),
            "end_time": end_iso,
            "device_id": raw.get("device_name") or "strava-unknown-device",
            "synced_at": synced_at,
            "distance_meters": float(raw["distance"]) if raw.get("distance") is not None else None,
            "calories": float(raw["calories"]) if raw.get("calories") is not None else None,
            "samples": self._normalize_streams(raw.get("_streams") or {}, start),
        }

    @staticmethod
    def _normalize_streams(streams: dict[str, Any], start: datetime) -> list[dict[str, Any]]:
        times = (streams.get("time") or {}).get("data") or []
        hr = (streams.get("heartrate") or {}).get("data") or []
        latlng = (streams.get("latlng") or {}).get("data") or []
        alt = (streams.get("altitude") or {}).get("data") or []
        n = len(times)
        samples = []
        for i in range(n):
            ts = datetime.fromtimestamp(start.timestamp() + times[i], tz=UTC).isoformat()
            lat, lon = (latlng[i][0], latlng[i][1]) if i < len(latlng) else (None, None)
            samples.append({
                "ts": ts,
                "heart_rate_bpm": hr[i] if i < len(hr) else None,
                "lat": lat,
                "lon": lon,
                "elevation_m": alt[i] if i < len(alt) else None,
            })
        return samples

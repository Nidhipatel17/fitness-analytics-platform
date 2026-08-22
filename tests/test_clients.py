import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.clients.synthetic_client import SyntheticClient


def make_client(tmp_path, **kwargs):
    defaults = dict(
        num_users=5,
        new_workouts_per_fetch=10,
        malformed_rate=0.0,
        anomaly_rate=0.0,
        duplicate_rate=0.0,
        late_arrival_rate=0.0,
        seed=42,
        state_dir=tmp_path / "state",
        store_path=tmp_path / "state" / "backend.json",
    )
    defaults.update(kwargs)
    return SyntheticClient(**defaults)


# ------------------------------------------------------------------- users


def test_bootstrap_creates_requested_number_of_users(tmp_path):
    client = make_client(tmp_path, num_users=7)
    users = list(client.fetch_users(since=None))
    assert len(users) == 7
    normalized = [client.normalize_user(u) for u in users]
    for u in normalized:
        assert u["user_id"]
        assert u["fitness_goal"] in {"lose_weight", "build_endurance", "build_strength", "maintain"}


def test_user_watermark_only_returns_evolved_users(tmp_path):
    client = make_client(tmp_path, num_users=20)
    all_users = list(client.fetch_users(since=None))
    watermark = max(datetime.fromisoformat(u["updated_at"]) for u in all_users)
    client.set_since(watermark, entity="users")

    since = client.get_since(entity="users")
    second_batch = list(client.fetch_users(since=since))
    # every returned user must have been touched strictly after the watermark
    for u in second_batch:
        assert datetime.fromisoformat(u["updated_at"]) > since


# ---------------------------------------------------------------- workouts


def test_live_fetch_generates_requested_volume(tmp_path):
    client = make_client(tmp_path, new_workouts_per_fetch=12)
    raw = list(client.fetch_workouts(since=None))
    assert client.last_batch_stats.new == 12
    assert len(raw) >= 12  # >= because a duplicate could also land in this batch


def test_watermark_prevents_refetching_same_workouts(tmp_path):
    client = make_client(tmp_path, new_workouts_per_fetch=10, duplicate_rate=0.0)
    first = list(client.fetch_workouts(since=None))
    watermark = max(datetime.fromisoformat(w["synced_at"]) for w in first)
    client.set_since(watermark, entity="workouts")

    since = client.get_since(entity="workouts")
    second = list(client.fetch_workouts(since=since))
    first_ids = {w["workout_id"] for w in first}
    second_ids = {w["workout_id"] for w in second}
    assert first_ids.isdisjoint(second_ids)


def test_malformed_records_fail_normalization(tmp_path):
    client = make_client(tmp_path, new_workouts_per_fetch=20, malformed_rate=1.0, anomaly_rate=0.0)
    raw = list(client.fetch_workouts(since=None))
    assert client.last_batch_stats.malformed == 20
    failures = 0
    for w in raw:
        try:
            client.normalize_workout(w)
        except ValueError:
            failures += 1
    assert failures == 20


def test_anomalous_records_pass_normalization_but_violate_business_rule(tmp_path):
    client = make_client(tmp_path, new_workouts_per_fetch=20, malformed_rate=0.0, anomaly_rate=1.0)
    raw = list(client.fetch_workouts(since=None))
    assert client.last_batch_stats.anomalous == 20
    flagged = 0
    for w in raw:
        normalized = client.normalize_workout(w)  # must not raise -- schema is valid
        start = datetime.fromisoformat(normalized["start_time"])
        end = datetime.fromisoformat(normalized["end_time"])
        over_24h = (end - start) > timedelta(hours=24)
        negative_distance = (normalized["distance_meters"] or 0) < 0
        assert over_24h or negative_distance, "every injected anomaly must trip one business rule"
        if over_24h or negative_distance:
            flagged += 1
    assert flagged == 20


def test_duplicate_delivery_reuses_an_existing_workout_id(tmp_path):
    client = make_client(tmp_path, new_workouts_per_fetch=5, duplicate_rate=0.0)
    first = list(client.fetch_workouts(since=None))
    first_ids = {w["workout_id"] for w in first}

    client.duplicate_rate = 1.0
    second = list(client.fetch_workouts(since=None))
    second_ids = [w["workout_id"] for w in second]
    assert any(wid in first_ids for wid in second_ids), "expected a re-delivered (duplicate) workout_id"


def test_late_arrival_produces_start_time_well_before_synced_at(tmp_path):
    client = make_client(tmp_path, new_workouts_per_fetch=10, late_arrival_rate=1.0)
    raw = list(client.fetch_workouts(since=None))
    assert client.last_batch_stats.late_arriving == 10
    for w in raw:
        start = datetime.fromisoformat(w["start_time"])
        synced = datetime.fromisoformat(w["synced_at"])
        assert (synced - start) > timedelta(days=1)


# ------------------------------------------------------------ bounded replay


def test_bounded_replay_is_idempotent_and_does_not_mutate_backend(tmp_path):
    store_path = tmp_path / "state" / "backend.json"
    seed_client = make_client(tmp_path, new_workouts_per_fetch=15, late_arrival_rate=0.5)
    seed_client.fetch_workouts(since=None)  # populate history, including late-arriving records

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=30)
    until = now + timedelta(days=1)

    reader_a = SyntheticClient(state_dir=tmp_path / "state", store_path=store_path, seed=1)
    reader_b = SyntheticClient(state_dir=tmp_path / "state", store_path=store_path, seed=2)

    batch_a = list(reader_a.fetch_workouts(since=since, until=until))
    count_after_a = len(reader_a.backend["workouts"])
    batch_b = list(reader_b.fetch_workouts(since=since, until=until))
    count_after_b = len(reader_b.backend["workouts"])

    assert count_after_a == count_after_b  # replay must not add/remove backend records
    assert {w["workout_id"] for w in batch_a} == {w["workout_id"] for w in batch_b}
    assert len(batch_a) == len(batch_b)


def test_bounded_replay_respects_window_bounds(tmp_path):
    client = make_client(tmp_path, new_workouts_per_fetch=10)
    client.fetch_workouts(since=None)
    far_future_since = datetime.now(timezone.utc) + timedelta(days=365)
    far_future_until = far_future_since + timedelta(days=1)
    empty = list(client.fetch_workouts(since=far_future_since, until=far_future_until))
    assert empty == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))

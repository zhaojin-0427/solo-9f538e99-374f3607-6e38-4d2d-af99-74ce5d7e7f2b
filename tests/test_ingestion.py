"""Event ingestion: idempotency, conflict rejection and validation."""

from __future__ import annotations

import pytest

from tests.conftest import conversion, touchpoint


def base_events(user_id: str = "u1") -> dict:
    return {
        "user_id": user_id,
        "touchpoints": [
            touchpoint("t1", "2026-11-20T10:00:00Z", "SEO"),
            touchpoint("t2", "2026-11-20T11:00:00Z", "Ads"),
        ],
        "conversions": [conversion("c1", "2026-11-20T12:00:00Z")],
    }


def test_ingest_returns_counts_and_ingested_at(client):
    response = client.post("/api/events", json=base_events())
    assert response.status_code == 200
    data = response.json()
    assert data["received"] == {
        "touchpoints": 2,
        "conversions": 1,
        "inserted": 3,
        "duplicates_ignored": 0,
    }
    assert data["ingested_at"].endswith("Z")


def test_duplicate_submission_is_idempotent(client):
    first = client.post("/api/events", json=base_events()).json()
    second = client.post("/api/events", json=base_events()).json()
    assert second["received"]["inserted"] == 0
    assert second["received"]["duplicates_ignored"] == 3
    # Same committed snapshot timestamp semantics: identical events stored once.
    events = client.get("/api/users/u1/events").json()["events"]
    assert [event["event_id"] for event in events] == ["t1", "t2", "c1"]
    assert {event["ingested_at"] for event in events} == {first["ingested_at"]}


def test_conflicting_event_id_rejects_whole_batch(client):
    client.post("/api/events", json=base_events())
    changed = base_events()
    changed["touchpoints"][0]["channel"] = "Changed"
    changed["conversions"].append(
        conversion("c2", "2026-11-20T13:00:00Z")
    )
    response = client.post("/api/events", json=changed)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "event_id_conflict"
    events = client.get("/api/users/u1/events").json()["events"]
    assert len(events) == 3  # nothing from the rejected batch leaked in


def test_in_batch_duplicate_id_with_conflicting_content_rejected(client):
    payload = base_events()
    payload["conversions"].append(
        conversion("t1", "2026-11-20T20:00:00Z")
    )
    response = client.post("/api/events", json=payload)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "duplicate_event_id"
    assert client.get("/api/users").json()["count"] == 0


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p.update(user_id=""),
        lambda p: p["touchpoints"][0].update(occurred_at="not-a-time"),
        lambda p: p["touchpoints"][0].update(channel=""),
        lambda p: p["touchpoints"][0].update(surprise="x"),
        lambda p: p["conversions"][0].update(id=""),
        lambda p: p["conversions"][0].update(occurred_at="2026-13-45T99:00:00Z"),
        lambda p: p.update(touchpoints=[], conversions=[]),
    ],
)
def test_invalid_payload_rejected(client, mutation):
    payload = base_events()
    mutation(payload)
    response = client.post("/api/events", json=payload)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_payload"
    assert client.get("/api/users").json()["count"] == 0


def test_naive_timestamps_treated_as_utc(client):
    payload = base_events()
    for item in payload["touchpoints"]:
        item["occurred_at"] = item["occurred_at"].replace("Z", "")
    response = client.post("/api/events", json=payload)
    assert response.status_code == 200
    stored = client.get("/api/users/u1/events").json()["events"][0]
    assert stored["occurred_at"].endswith("Z")


def test_users_endpoint_lists_ingested_users(client):
    client.post("/api/events", json=base_events("u1"))
    client.post("/api/events", json=base_events("u2"))
    data = client.get("/api/users").json()
    assert data["count"] == 2
    by_id = {row["user_id"]: row for row in data["users"]}
    assert by_id["u1"]["touchpoints"] == 2
    assert by_id["u1"]["conversions"] == 1


def test_unknown_user_events_returns_404(client):
    response = client.get("/api/users/nobody/events")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "user_not_found"


def test_partial_new_batch_mixes_inserts_and_duplicates(client):
    client.post("/api/events", json=base_events())
    follow_up = {
        "user_id": "u1",
        "touchpoints": [touchpoint("t3", "2026-11-21T08:00:00Z", "Email")],
        "conversions": [],
    }
    response = client.post("/api/events", json=follow_up).json()
    assert response["received"]["inserted"] == 1
    assert response["received"]["duplicates_ignored"] == 0
    assert len(client.get("/api/users/u1/events").json()["events"]) == 4

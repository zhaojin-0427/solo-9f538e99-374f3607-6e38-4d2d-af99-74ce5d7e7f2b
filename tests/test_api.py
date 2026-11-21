"""HTTP-level smoke tests: routing, status codes and wire formats."""

from __future__ import annotations

from tests.conftest import conversion, touchpoint


def test_health_and_index(client):
    assert client.get("/health").json() == {"status": "ok"}
    page = client.get("/")
    assert page.status_code == 200
    assert "成交预测" in page.text


def test_invalid_json_returns_structured_422(client):
    response = client.post(
        "/api/events",
        content="{not json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_payload"


def test_open_batch_validation_errors(client):
    # Empty name list.
    assert (
        client.post(
            "/api/batches",
            json={"user_ids": [], "observation_minutes": 60},
        ).status_code
        == 422
    )
    # Non-positive observation window.
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    bad_minutes = client.post(
        "/api/batches", json={"user_ids": ["u1"], "observation_minutes": 0}
    )
    assert bad_minutes.status_code == 422
    # Unknown extra field is rejected.
    assert (
        client.post(
            "/api/events",
            json={
                "user_id": "u9",
                "touchpoints": [
                    {
                        "id": "t",
                        "occurred_at": "2026-11-21T10:00:00Z",
                        "channel": "SEO",
                        "extra": 1,
                    }
                ],
                "conversions": [],
            },
        ).status_code
        == 422
    )


def test_full_batch_lifecycle_over_http(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [
                touchpoint("t1", "2026-11-20T10:00:00Z", "Ads"),
                touchpoint("t2", "2026-11-20T12:00:00Z", "SEO"),
            ],
            "conversions": [conversion("c1", "2026-11-21T12:05:00Z")],
        },
    )
    client.post(
        "/api/events",
        json={
            "user_id": "u2",
            "touchpoints": [touchpoint("t3", "2026-11-20T08:00:00Z", "SEO")],
            "conversions": [],
        },
    )

    opened = client.post(
        "/api/batches",
        json={"user_ids": ["u2", "u1"], "observation_minutes": 30},
    )
    assert opened.status_code == 201
    batch_id = opened.json()["batch_id"]

    listing = client.get("/api/batches").json()
    assert listing["count"] == 1
    assert listing["batches"][0]["batch_id"] == batch_id

    # Not due yet.
    early = client.post(f"/api/batches/{batch_id}/close")
    assert early.status_code == 409
    assert early.json()["error"]["code"] == "batch_not_due"

    # CSV is unavailable until published.
    assert client.get(f"/api/batches/{batch_id}/csv").status_code == 409

    client.clock.advance(minutes=31)
    closed = client.post(f"/api/batches/{batch_id}/close")
    assert closed.status_code == 200
    assert closed.json()["status"] == "published"

    csv_response = client.get(f"/api/batches/{batch_id}/csv")
    assert csv_response.status_code == 200
    assert csv_response.headers["content-type"].startswith("text/csv")
    assert (
        csv_response.headers["content-disposition"]
        == f'attachment; filename="{batch_id}.csv"'
    )
    lines = csv_response.text.lstrip("﻿").strip().split("\n")
    assert lines == [
        "user_id,Ads,SEO,label",
        "u1,1,1,1",
        "u2,0,1,0",
    ]


def test_batch_ids_are_sequential(client):
    for user_id in ("u1", "u2"):
        client.post(
            "/api/events",
            json={
                "user_id": user_id,
                "touchpoints": [touchpoint("t", "2026-11-21T10:00:00Z", "SEO")],
                "conversions": [],
            },
        )
    first = client.post(
        "/api/batches", json={"user_ids": ["u1"], "observation_minutes": 10}
    ).json()
    second = client.post(
        "/api/batches", json={"user_ids": ["u2"], "observation_minutes": 10}
    ).json()
    assert first["batch_id"] == "B000001"
    assert second["batch_id"] == "B000002"


def test_oversized_body_rejected(client, monkeypatch):
    from app import main as main_module

    monkeypatch.setattr(main_module, "MAX_BODY_BYTES", 100)
    response = client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "payload_too_large"

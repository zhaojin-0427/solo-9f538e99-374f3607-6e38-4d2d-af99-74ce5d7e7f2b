"""Batch snapshot semantics: frozen features, late data, labels and CSV."""

from __future__ import annotations

from tests.conftest import conversion, touchpoint


def open_payload(*user_ids: str, minutes: int = 120) -> dict:
    return {"user_ids": list(user_ids), "observation_minutes": minutes}


def test_open_batch_fixes_name_list_and_window(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    response = client.post("/api/batches", json=open_payload("u1", minutes=60))
    assert response.status_code == 201
    batch = response.json()
    assert batch["batch_id"] == "B000001"
    assert batch["status"] == "open"
    assert batch["observation_start"] == "2026-11-21T12:00:00.000000Z"
    assert batch["observation_end"] == "2026-11-21T13:00:00.000000Z"
    assert batch["due_at"] == batch["observation_end"]
    assert batch["members"] == [
        {"user_id": "u1", "features": {"SEO": 1}, "label": None}
    ]


def test_open_batch_rejects_unknown_users(client):
    response = client.post("/api/batches", json=open_payload("ghost"))
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "user_not_found"


def test_open_batch_rejects_duplicate_user_ids(client):
    response = client.post(
        "/api/batches",
        json={"user_ids": ["u1", "u1"], "observation_minutes": 60},
    )
    assert response.status_code == 422


def test_feature_window_is_seven_days_with_inclusive_bounds(client):
    # t_old: just outside the seven-day lookback (excluded)
    # t_edge_start: exactly seven days before open (included, lower bound)
    # t_edge_open: exactly at open time (included, upper bound)
    # t_future: event time after open but already committed (excluded)
    payload = {
        "user_id": "u1",
        "touchpoints": [
            touchpoint("t_old", "2026-11-14T11:59:59Z", "Old"),
            touchpoint("t_edge_start", "2026-11-14T12:00:00Z", "Edge"),
            touchpoint("t_mid", "2026-11-18T09:00:00Z", "SEO"),
            touchpoint("t_edge_open", "2026-11-21T12:00:00Z", "SEO"),
            touchpoint("t_future", "2026-11-22T00:00:00Z", "Future"),
        ],
        "conversions": [],
    }
    client.post("/api/events", json=payload)
    batch = client.post("/api/batches", json=open_payload("u1")).json()
    features = batch["members"][0]["features"]
    assert features == {"Edge": 1, "SEO": 2}
    assert batch["channels"] == ["Edge", "SEO"]


def test_late_touchpoints_never_enter_frozen_features(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    batch = client.post("/api/batches", json=open_payload("u1")).json()
    assert batch["members"][0]["features"] == {"SEO": 1}

    # A late arrival whose event time is inside the seven-day window.
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t_late", "2026-11-21T11:00:00Z", "Ads")],
            "conversions": [],
        },
    )
    refreshed = client.get(f"/api/batches/{batch['batch_id']}").json()
    assert refreshed["members"][0]["features"] == {"SEO": 1}


def test_close_before_due_is_rejected(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    batch = client.post("/api/batches", json=open_payload("u1", minutes=60)).json()
    response = client.post(f"/api/batches/{batch['batch_id']}/close")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "batch_not_due"
    # The failed early attempt must not move the batch out of open.
    assert client.get(f"/api/batches/{batch['batch_id']}").json()["status"] == "open"


def test_label_uses_observation_window_half_open(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [],
            "conversions": [
                conversion("c_before", "2026-11-21T11:59:59Z"),
                conversion("c_start", "2026-11-21T12:00:00Z"),
                conversion("c_mid", "2026-11-21T12:30:00Z"),
                conversion("c_end", "2026-11-21T13:00:00Z"),
                conversion("c_after", "2026-11-21T13:00:01Z"),
            ],
        },
    )
    client.post(
        "/api/events",
        json={
            "user_id": "u2",
            "touchpoints": [],
            "conversions": [conversion("c0", "2026-11-21T12:10:00Z")],
        },
    )
    # A user with only a conversion outside the observation window -> label 0.
    client.post(
        "/api/events",
        json={
            "user_id": "u3",
            "touchpoints": [],
            "conversions": [conversion("c_old", "2026-11-21T09:00:00Z")],
        },
    )
    batch = client.post(
        "/api/batches", json=open_payload("u1", "u2", "u3", minutes=60)
    ).json()

    client.clock.advance(minutes=61)
    closed = client.post(f"/api/batches/{batch['batch_id']}/close").json()
    assert closed["status"] == "published"

    detail = client.get(f"/api/batches/{batch['batch_id']}").json()
    labels = {member["user_id"]: member["label"] for member in detail["members"]}
    assert labels == {"u1": 1, "u2": 1, "u3": 0}

    csv_text = client.get(f"/api/batches/{batch['batch_id']}/csv").text
    # BOM prefix plus header plus one row per listed user.
    assert csv_text.startswith("﻿")
    lines = csv_text.lstrip("﻿").strip().split("\n")
    assert lines[0] == "user_id,label"
    assert lines[1] == "u1,1"  # c_start and c_mid are inside [start, end)
    assert lines[2] == "u2,1"
    assert lines[3] == "u3,0"


def test_late_conversion_is_excluded_from_label(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [
                touchpoint("t0", "2026-11-21T10:00:00Z", "SEO"),
            ],
            "conversions": [],
        },
    )
    batch = client.post("/api/batches", json=open_payload("u1", minutes=60)).json()
    client.clock.advance(minutes=61)

    # The conversion is committed *after* the close claim is accepted but
    # before the label query runs — exactly the late-arrival case the
    # first-acceptance snapshot must keep out of the sample.
    def late_arrival(database, batch_id, first_close_at):
        client.post(
            "/api/events",
            json={
                "user_id": "u1",
                "touchpoints": [],
                "conversions": [conversion("c_late", "2026-11-21T12:30:00Z")],
            },
        )

    client.app.state.db.on_before_build_csv = late_arrival
    closed = client.post(f"/api/batches/{batch['batch_id']}/close").json()
    client.app.state.db.on_before_build_csv = None
    assert closed["status"] == "published"
    csv_text = client.get(f"/api/batches/{batch['batch_id']}/csv").text
    assert csv_text.lstrip("﻿").strip().split("\n") == [
        "user_id,SEO,label",
        "u1,1,0",
    ]


def test_csv_contains_one_row_per_user_and_all_channel_columns(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [
                touchpoint("a", "2026-11-20T10:00:00Z", "Ads"),
                touchpoint("b", "2026-11-20T11:00:00Z", "Ads"),
                touchpoint("c", "2026-11-20T12:00:00Z", "SEO"),
            ],
            "conversions": [conversion("c1", "2026-11-21T12:30:00Z")],
        },
    )
    client.post(
        "/api/events",
        json={
            "user_id": "u2",
            "touchpoints": [touchpoint("d", "2026-11-20T10:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    batch = client.post(
        "/api/batches", json=open_payload("u1", "u2", minutes=60)
    ).json()
    client.clock.advance(minutes=61)
    client.post(f"/api/batches/{batch['batch_id']}/close")

    response = client.get(f"/api/batches/{batch['batch_id']}/csv")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert (
        response.headers["content-disposition"]
        == 'attachment; filename="B000001.csv"'
    )
    lines = response.text.lstrip("﻿").strip().split("\n")
    # Channels are sorted; missing channels materialise as zero.
    assert lines == [
        "user_id,Ads,SEO,label",
        "u1,2,1,1",
        "u2,0,1,0",
    ]


def test_csv_download_before_publish_returns_409(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    batch = client.post("/api/batches", json=open_payload("u1")).json()
    response = client.get(f"/api/batches/{batch['batch_id']}/csv")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "batch_not_published"


def test_close_is_idempotent_and_publishes_single_csv(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
            "conversions": [conversion("c1", "2026-11-21T12:15:00Z")],
        },
    )
    batch = client.post("/api/batches", json=open_payload("u1", minutes=60)).json()
    client.clock.advance(minutes=61)
    first = client.post(f"/api/batches/{batch['batch_id']}/close").json()
    second = client.post(f"/api/batches/{batch['batch_id']}/close").json()
    third = client.post(f"/api/batches/{batch['batch_id']}/close").json()
    assert first["status"] == second["status"] == third["status"] == "published"
    assert first["closed_at"] == second["closed_at"] == third["closed_at"]
    assert first["first_close_at"] == second["first_close_at"]
    csv_one = client.get(f"/api/batches/{batch['batch_id']}/csv").text
    csv_two = client.get(f"/api/batches/{batch['batch_id']}/csv").text
    assert csv_one == csv_two


def test_unknown_batch_endpoints_return_404(client):
    assert client.get("/api/batches/B999999").status_code == 404
    assert client.post("/api/batches/B999999/close").status_code == 404
    assert client.get("/api/batches/B999999/csv").status_code == 404


def test_interleaved_ingestion_during_open_window_does_not_change_sample(client):
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t1", "2026-11-21T09:00:00Z", "SEO")],
            "conversions": [],
        },
    )
    batch = client.post("/api/batches", json=open_payload("u1", minutes=60)).json()

    # While the batch is open, new events keep landing.  Touchpoints can no
    # longer change the frozen features; a conversion committed before the
    # close acceptance still belongs to the label snapshot.
    client.clock.advance(minutes=30)
    client.post(
        "/api/events",
        json={
            "user_id": "u1",
            "touchpoints": [touchpoint("t2", "2026-11-21T09:30:00Z", "Ads")],
            "conversions": [conversion("c1", "2026-11-21T12:20:00Z")],
        },
    )
    # A brand-new user created after open is not on the frozen name list.
    client.post(
        "/api/events",
        json={
            "user_id": "u_late",
            "touchpoints": [touchpoint("x", "2026-11-21T12:00:00Z", "SEO")],
            "conversions": [],
        },
    )

    # Another conversion lands while generation is in progress, i.e. after
    # the first close acceptance.  It must not flip the label semantics.
    def late_arrival(database, batch_id, first_close_at):
        client.post(
            "/api/events",
            json={
                "user_id": "u1",
                "touchpoints": [],
                "conversions": [conversion("c_after_claim", "2026-11-21T12:40:00Z")],
            },
        )

    client.clock.advance(minutes=31)
    client.app.state.db.on_before_build_csv = late_arrival
    client.post(f"/api/batches/{batch['batch_id']}/close")
    client.app.state.db.on_before_build_csv = None
    lines = (
        client.get(f"/api/batches/{batch['batch_id']}/csv")
        .text.lstrip("﻿")
        .strip()
        .split("\n")
    )
    # Only the pre-open SEO touchpoint is a feature; c1 (committed before
    # the close claim) makes the label 1; c_after_claim is excluded;
    # u_late never appears because the name list was frozen at open time.
    assert lines == ["user_id,SEO,label", "u1,1,1"]

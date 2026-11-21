"""Concurrency, retry-after-failure and restart persistence guarantees."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app import db as db_module
from app.db import Database
from app.main import create_app
from tests.conftest import Clock, conversion, touchpoint


def seed_user(client, user_id: str = "u1", with_conversion: bool = True) -> None:
    client.post(
        "/api/events",
        json={
            "user_id": user_id,
            "touchpoints": [
                touchpoint("t1", "2026-11-21T10:00:00Z", "SEO"),
                touchpoint("t2", "2026-11-21T10:30:00Z", "Ads"),
            ],
            "conversions": (
                [conversion("c1", "2026-11-21T12:20:00Z")]
                if with_conversion
                else []
            ),
        },
    )


def test_concurrent_close_publishes_one_consistent_csv(client):
    seed_user(client)
    batch = client.post(
        "/api/batches",
        json={"user_ids": ["u1"], "observation_minutes": 60},
    ).json()
    client.clock.advance(minutes=61)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(
            pool.map(
                lambda _: client.post(f"/api/batches/{batch['batch_id']}/close"),
                range(16),
            )
        )

    assert all(response.status_code == 200 for response in responses)
    assert all(
        response.json()["status"] == "published" for response in responses
    )
    first_close_values = {
        response.json()["first_close_at"] for response in responses
    }
    assert len(first_close_values) == 1
    closed_values = {response.json()["closed_at"] for response in responses}
    assert len(closed_values) == 1

    csv_text = client.get(f"/api/batches/{batch['batch_id']}/csv").text
    assert csv_text.lstrip("﻿").strip().split("\n") == [
        "user_id,Ads,SEO,label",
        "u1,1,1,1",
    ]


def test_retry_after_generation_failure_keeps_first_claim_scope(
    tmp_path, monkeypatch
):
    db_path = str(tmp_path / "retry.db")
    clock = Clock(datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(db_module, "utc_now", clock.now)

    app = create_app(db_path)
    with TestClient(app) as first_client:
        seed_user(first_client, with_conversion=False)
        batch = first_client.post(
            "/api/batches",
            json={"user_ids": ["u1"], "observation_minutes": 60},
        ).json()
        batch_id = batch["batch_id"]
        clock.advance(minutes=61)

        # Force the first accepted close to fail after the claim is stored.
        original_build = Database._build_csv
        calls = {"n": 0}

        def flaky_build(self, bid, first_close_at):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated generation crash")
            return original_build(self, bid, first_close_at)

        monkeypatch.setattr(Database, "_build_csv", flaky_build)
        failed = first_client.post(f"/api/batches/{batch_id}/close")
        assert failed.status_code == 500
        assert failed.json()["error"]["code"] == "batch_generation_failed"

        stored = first_client.get(f"/api/batches/{batch_id}").json()
        assert stored["status"] == "failed"
        first_claim = stored["first_close_at"]

        # A conversion committed between failure and retry must be ignored:
        # the retry reuses the original first-acceptance scope.
        first_client.post(
            "/api/events",
            json={
                "user_id": "u1",
                "touchpoints": [],
                "conversions": [conversion("c_late", "2026-11-21T12:30:00Z")],
            },
        )

        retried = first_client.post(f"/api/batches/{batch_id}/close")
        assert retried.status_code == 200
        assert retried.json()["status"] == "published"
        assert retried.json()["first_close_at"] == first_claim

        csv_text = first_client.get(f"/api/batches/{batch_id}/csv").text
        assert csv_text.lstrip("﻿").strip().split("\n")[-1] == "u1,1,1,0"


def test_snapshot_survives_process_restart(tmp_path, monkeypatch):
    db_path = str(tmp_path / "restart.db")
    clock = Clock(datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))

    def make_client() -> TestClient:
        monkeypatch.setattr(db_module, "utc_now", clock.now)
        return TestClient(create_app(db_path))

    with make_client() as first_client:
        seed_user(first_client)
        batch = first_client.post(
            "/api/batches",
            json={"user_ids": ["u1"], "observation_minutes": 60},
        ).json()
        batch_id = batch["batch_id"]
        # Events arriving after open must never leak into the frozen sample.
        clock.advance(minutes=30)
        first_client.post(
            "/api/events",
            json={
                "user_id": "u1",
                "touchpoints": [touchpoint("t_late", "2026-11-21T11:00:00Z", "Email")],
                "conversions": [],
            },
        )

    # Simulate a full restart with a brand-new app instance on the same file.
    clock.advance(minutes=31)
    with make_client() as restarted_client:
        # The late data stays visible as raw events but outside the sample.
        raw = restarted_client.get("/api/users/u1/events").json()["events"]
        assert len(raw) == 4

        batch_view = restarted_client.get(f"/api/batches/{batch_id}").json()
        assert batch_view["status"] == "open"
        assert batch_view["members"][0]["features"] == {"Ads": 1, "SEO": 1}

        closed = restarted_client.post(f"/api/batches/{batch_id}/close").json()
        assert closed["status"] == "published"
        csv_text = restarted_client.get(f"/api/batches/{batch_id}/csv").text
        assert csv_text.lstrip("﻿").strip().split("\n") == [
            "user_id,Ads,SEO,label",
            "u1,1,1,1",
        ]


def test_interrupted_closing_state_recovers_on_restart(tmp_path, monkeypatch):
    """A worker that dies after claiming (status=closing) must not block close."""

    db_path = str(tmp_path / "interrupted.db")
    clock = Clock(datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(db_module, "utc_now", clock.now)

    with TestClient(create_app(db_path)) as client:
        seed_user(client)
        batch = client.post(
            "/api/batches",
            json={"user_ids": ["u1"], "observation_minutes": 60},
        ).json()
        batch_id = batch["batch_id"]
        clock.advance(minutes=61)
        database = client.app.state.db
        with database.transaction() as connection:
            claim_at = database._commit_time(connection)
            connection.execute(
                "UPDATE batches SET status = 'closing', first_close_at = ? "
                "WHERE batch_id = ?",
                (claim_at, batch_id),
            )

    # A conversion arriving after the original claim stays out of scope.
    clock.advance(minutes=5)
    with TestClient(create_app(db_path)) as restarted:
        restarted.post(
            "/api/events",
            json={
                "user_id": "u1",
                "touchpoints": [],
                "conversions": [conversion("c_after", "2026-11-21T12:40:00Z")],
            },
        )
        closed = restarted.post(f"/api/batches/{batch_id}/close").json()
        assert closed["status"] == "published"
        assert closed["first_close_at"] == claim_at.split("#", 1)[0]
        csv_text = restarted.get(f"/api/batches/{batch_id}/csv").text
        assert csv_text.lstrip("﻿").strip().split("\n")[-1] == "u1,1,1,1"


def test_open_and_ingest_transactions_serialise(tmp_path, monkeypatch):
    """Snapshot and ingestion commit under BEGIN IMMEDIATE, so no event is
    counted twice or lost when they race."""

    from app.models import EventBatchRequest

    db_path = str(tmp_path / "serial.db")
    clock = Clock(datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(db_module, "utc_now", clock.now)
    database = Database(db_path)
    database.initialize()

    request = EventBatchRequest(
        user_id="u1",
        touchpoints=[touchpoint("t1", "2026-11-21T10:00:00Z", "SEO")],
        conversions=[],
    )

    with ThreadPoolExecutor(max_workers=8) as pool:
        ingest_results = list(
            pool.map(lambda _: database.ingest_events(request), range(12))
        )
        open_futures = [
            pool.submit(database.open_batch, ["u1"], 60) for _ in range(8)
        ]
        opened = [future.result() for future in open_futures]

    assert sum(r["received"]["inserted"] for r in ingest_results) == 1
    assert len({batch["batch_id"] for batch in opened}) == 8
    for batch in opened:
        assert batch["members"][0]["features"] in ({}, {"SEO": 1})
    database.close()

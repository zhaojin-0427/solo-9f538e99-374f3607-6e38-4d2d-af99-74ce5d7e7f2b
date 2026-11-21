"""Shared fixtures: frozen clock and an in-process TestClient."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app.main import create_app


class Clock:
    """Mutable frozen clock replacing ``app.db.utc_now`` in tests."""

    def __init__(self, start: datetime) -> None:
        self.current = start

    def now(self) -> datetime:
        return self.current

    def advance(self, **kwargs) -> None:
        self.current += timedelta(**kwargs)

    def set(self, value: datetime) -> None:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        self.current = value.astimezone(timezone.utc)


@pytest.fixture()
def clock(monkeypatch) -> Clock:
    fixed = Clock(datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(db_module, "utc_now", fixed.now)
    return fixed


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # Every test gets its own frozen clock (defaults unused by most cases).
    fixed = Clock(datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(db_module, "utc_now", fixed.now)
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as test_client:
        test_client.clock = fixed  # type: ignore[attr-defined]
        yield test_client


def touchpoint(event_id: str, occurred_at: str, channel: str) -> dict:
    return {"id": event_id, "occurred_at": occurred_at, "channel": channel}


def conversion(event_id: str, occurred_at: str) -> dict:
    return {"id": event_id, "occurred_at": occurred_at}

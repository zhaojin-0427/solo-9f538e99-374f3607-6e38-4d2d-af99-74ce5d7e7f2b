"""Request models and strict validation for the sample-batch service."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_validator,
    model_validator,
)


MAX_EVENTS_PER_BATCH = 5_000
MAX_BATCH_USERS = 2_000
# Observation windows longer than ~10 years are almost certainly client bugs.
MAX_OBSERVATION_MINUTES = 10 * 365 * 24 * 60
# The feature look-back is fixed by the product spec at seven days.
FEATURE_LOOKBACK_DAYS = 7
FEATURE_LOOKBACK_SECONDS = FEATURE_LOOKBACK_DAYS * 24 * 60 * 60


def parse_timestamp(value: object) -> datetime:
    """Parse an ISO-8601 timestamp and normalise it to UTC.

    Naive timestamps are interpreted as UTC, which keeps local-only clients
    usable without sacrificing deterministic ordering.
    """

    if not isinstance(value, str):
        raise ValueError("occurred_at 必须是 ISO 8601 格式的字符串")
    raw = value.strip()
    if not raw:
        raise ValueError("occurred_at 不能为空")
    candidate = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError("occurred_at 不是合法的 ISO 8601 时间") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class TouchpointIn(StrictModel):
    id: str = Field(min_length=1, max_length=200)
    occurred_at: datetime
    channel: str = Field(min_length=1, max_length=100)

    @field_validator("occurred_at", mode="before")
    @classmethod
    def _validate_occurred_at(cls, value: object) -> datetime:
        return parse_timestamp(value)


class ConversionIn(StrictModel):
    id: str = Field(min_length=1, max_length=200)
    occurred_at: datetime

    @field_validator("occurred_at", mode="before")
    @classmethod
    def _validate_occurred_at(cls, value: object) -> datetime:
        return parse_timestamp(value)


class EventBatchRequest(StrictModel):
    """A submitted, committed snapshot of one user's events."""

    user_id: str = Field(min_length=1, max_length=200)
    touchpoints: List[TouchpointIn] = Field(max_length=MAX_EVENTS_PER_BATCH)
    conversions: List[ConversionIn] = Field(max_length=MAX_EVENTS_PER_BATCH)

    @model_validator(mode="after")
    def _require_events(self) -> "EventBatchRequest":
        if not self.touchpoints and not self.conversions:
            raise ValueError("批次至少需要包含一个触点或成交事件")
        return self


class OpenBatchRequest(StrictModel):
    user_ids: List[str] = Field(min_length=1, max_length=MAX_BATCH_USERS)
    observation_minutes: StrictInt = Field(gt=0, le=MAX_OBSERVATION_MINUTES)

    @field_validator("user_ids")
    @classmethod
    def _validate_user_ids(cls, value: List[str]) -> List[str]:
        cleaned = [item.strip() for item in value]
        if any(not item for item in cleaned):
            raise ValueError("user_ids 不能包含空字符串")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("user_ids 不能重复")
        if any(len(item) > 200 for item in cleaned):
            raise ValueError("单个 user_id 长度不能超过 200")
        return cleaned


class ApiError(Exception):
    """An expected API-level error rendered as structured JSON."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        errors: List[dict] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.errors = errors or []


__all__ = [
    "ApiError",
    "ConversionIn",
    "EventBatchRequest",
    "FEATURE_LOOKBACK_SECONDS",
    "MAX_BATCH_USERS",
    "MAX_EVENTS_PER_BATCH",
    "OpenBatchRequest",
    "TouchpointIn",
    "parse_timestamp",
]

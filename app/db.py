"""SQLite persistence with snapshot-based sample batches.

Key correctness points implemented here:

* Every event carries ``ingested_at``: the commit time of the request that
  introduced it.  A batch snapshot only ever sees events whose
  ``ingested_at`` is strictly earlier than the snapshot timestamp, so late
  arrivals can never retroactively change a frozen sample.
* Opening a batch fixes the name list, the observation window and the
  touchpoint feature matrix in one ``BEGIN IMMEDIATE`` transaction.
* Closing a batch is a compare-and-set on the batch status.  The first
  accepted close claim wins; retries (including after a process restart)
  reuse the same claim timestamp and therefore the same conversion scope.
* A batch publishes exactly one CSV, stored verbatim and downloadable by
  batch id.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from .models import (
    ApiError,
    EventBatchRequest,
    FEATURE_LOOKBACK_SECONDS,
    TouchpointIn,
    ConversionIn,
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id     TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    user_id      TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN ('touchpoint', 'conversion')),
    occurred_at  TEXT NOT NULL,
    channel      TEXT,
    ingested_at  TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    PRIMARY KEY (user_id, event_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_events_user_type_time
    ON events (user_id, event_type, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_ingest
    ON events (ingested_at);

CREATE TABLE IF NOT EXISTS batches (
    batch_id           TEXT PRIMARY KEY,
    user_count         INTEGER NOT NULL,
    observation_start  TEXT NOT NULL,
    observation_end    TEXT NOT NULL,
    opened_at          TEXT NOT NULL,
    due_at             TEXT NOT NULL,
    channels_json      TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('open', 'closing', 'published', 'failed')),
    first_close_at     TEXT,
    closed_at          TEXT,
    failure_reason     TEXT,
    csv_content        TEXT
);

-- Monotonic counter appended to commit timestamps.  Wall clocks can tick
-- at the same microsecond (and tests freeze them entirely); the suffix
-- guarantees every committed ingested_at is strictly ordered, which is
-- what the snapshot predicates (ingested_at < snapshot_at) rely on.
CREATE TABLE IF NOT EXISTS commit_clock (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    counter INTEGER NOT NULL
);
INSERT INTO commit_clock (id, counter) VALUES (1, 0)
    ON CONFLICT(id) DO NOTHING;

CREATE TABLE IF NOT EXISTS batch_members (
    batch_id      TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    feature_json  TEXT NOT NULL,
    PRIMARY KEY (batch_id, user_id),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);
"""


def utc_now() -> datetime:
    """Single clock for the module so tests can freeze time deterministically."""

    return datetime.now(timezone.utc)


def to_db_time(value: datetime) -> str:
    """Canonical fixed-width UTC timestamp so text ordering is chronological."""

    utc = value.astimezone(timezone.utc)
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T"
        f"{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def from_db_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_commit_time(value: str) -> datetime:
    """Inverse of the commit timestamps produced by ``Database._commit_time``."""

    stamped = value.split("#", 1)[0]
    return datetime.strptime(stamped, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=timezone.utc
    )


def event_payload_hash(event: TouchpointIn | ConversionIn) -> str:
    """Stable hash of the event identity fields used for conflict detection."""

    if isinstance(event, TouchpointIn):
        payload = {
            "type": "touchpoint",
            "id": event.id,
            "occurred_at": to_db_time(event.occurred_at),
            "channel": event.channel,
        }
    else:
        payload = {
            "type": "conversion",
            "id": event.id,
            "occurred_at": to_db_time(event.occurred_at),
        }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    import hashlib

    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class Database:
    """Repository over a single SQLite file (or a shared in-memory DB)."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._keeper: Optional[sqlite3.Connection] = None
        # Serialises in-process close workers per batch; cross-process and
        # crash safety still comes from the status compare-and-set in SQL.
        self._close_locks: Dict[str, threading.Lock] = {}
        self._close_locks_guard = threading.Lock()
        if path == ":memory:":
            self._uri = "file:sample_batch_memory?mode=memory&cache=shared"
            self._keeper = sqlite3.connect(
                self._uri, uri=True, timeout=30.0, isolation_level=None
            )
        else:
            self._uri = path

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._uri,
            uri=self.path == ":memory:",
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def close(self) -> None:
        if self._keeper is not None:
            self._keeper.close()
            self._keeper = None

    def initialize(self) -> None:
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(SCHEMA)
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        finally:
            connection.close()

    def _commit_time(self, connection: sqlite3.Connection) -> str:
        """Return a strictly increasing commit timestamp.

        Format stays fixed-width and lexicographically chronological while a
        12-digit monotonic suffix breaks ties between identical wall-clock
        readings, so ``ingested_at < snapshot_at`` is a reliable
        before/after predicate even under frozen or coarse clocks.
        """

        row = connection.execute(
            "UPDATE commit_clock SET counter = counter + 1 RETURNING counter"
        ).fetchone()
        return f"{to_db_time(utc_now())}#{row['counter']:012d}"

    @staticmethod
    def _display_time(commit_value: str) -> str:
        """Strip the monotonic suffix for externally visible timestamps."""

        return commit_value.split("#", 1)[0]

    def _lock_for(self, batch_id: str) -> threading.Lock:
        with self._close_locks_guard:
            lock = self._close_locks.get(batch_id)
            if lock is None:
                lock = threading.Lock()
                self._close_locks[batch_id] = lock
            return lock

    # -- event ingestion ----------------------------------------------------

    def ingest_events(self, request: EventBatchRequest) -> dict:
        """Atomically insert one submitted event snapshot.

        Duplicate ``(user_id, event_id)`` pairs with identical content are
        idempotently ignored; a reused id with different content rejects the
        whole batch with 409.
        """

        prepared = self._prepare_events(request)

        with self.transaction() as connection:
            now = self._commit_time(connection)
            existing = self._load_hashes(connection, request.user_id)
            conflicts = sorted(
                event_id
                for event_id, payload_hash, _ in prepared
                if event_id in existing and existing[event_id] != payload_hash
            )
            if conflicts:
                raise ApiError(
                    409,
                    "event_id_conflict",
                    "事件 ID 已存在但内容不同，整批请求未写入。",
                    [{"event_id": event_id} for event_id in conflicts],
                )

            duplicates = sum(
                1 for event_id, _, _ in prepared if event_id in existing
            )
            inserted = len(prepared) - duplicates

            display_now = self._display_time(now)
            connection.execute(
                """
                INSERT INTO users (user_id, created_at, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET updated_at = excluded.updated_at
                """,
                (request.user_id, display_now, display_now),
            )
            for event_id, payload_hash, event in prepared:
                if event_id in existing:
                    continue
                self._insert_event(
                    connection, request.user_id, event, payload_hash, now
                )

            return {
                "user_id": request.user_id,
                "received": {
                    "touchpoints": len(request.touchpoints),
                    "conversions": len(request.conversions),
                    "inserted": inserted,
                    "duplicates_ignored": duplicates,
                },
                "ingested_at": display_now,
            }

    def _prepare_events(
        self, request: EventBatchRequest
    ) -> List[tuple[str, str, object]]:
        seen: Dict[str, str] = {}
        prepared: List[tuple[str, str, object]] = []
        conflicts: List[str] = []
        for event in list(request.touchpoints) + list(request.conversions):
            payload_hash = event_payload_hash(event)  # type: ignore[arg-type]
            previous = seen.get(event.id)
            if previous is None:
                seen[event.id] = payload_hash
                prepared.append((event.id, payload_hash, event))
            elif previous != payload_hash:
                conflicts.append(event.id)
        if conflicts:
            raise ApiError(
                422,
                "duplicate_event_id",
                "同一批请求中事件 ID 被不同内容重复使用。",
                [{"event_id": event_id} for event_id in sorted(set(conflicts))],
            )
        return prepared

    def _load_hashes(
        self, connection: sqlite3.Connection, user_id: str
    ) -> Dict[str, str]:
        rows = connection.execute(
            "SELECT event_id, payload_hash FROM events WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        return {row["event_id"]: row["payload_hash"] for row in rows}

    def _insert_event(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        event: object,
        payload_hash: str,
        ingested_at: str,
    ) -> None:
        if isinstance(event, TouchpointIn):
            connection.execute(
                """
                INSERT INTO events (user_id, event_id, event_type, occurred_at,
                                    channel, ingested_at, payload_hash)
                VALUES (?, ?, 'touchpoint', ?, ?, ?, ?)
                """,
                (
                    user_id,
                    event.id,
                    to_db_time(event.occurred_at),
                    event.channel,
                    ingested_at,
                    payload_hash,
                ),
            )
        else:
            assert isinstance(event, ConversionIn)
            connection.execute(
                """
                INSERT INTO events (user_id, event_id, event_type, occurred_at,
                                    channel, ingested_at, payload_hash)
                VALUES (?, ?, 'conversion', ?, NULL, ?, ?)
                """,
                (
                    user_id,
                    event.id,
                    to_db_time(event.occurred_at),
                    ingested_at,
                    payload_hash,
                ),
            )

    # -- reads --------------------------------------------------------------

    def list_users(self) -> List[dict]:
        with self.read() as connection:
            rows = connection.execute(
                """
                SELECT u.user_id, u.created_at, u.updated_at,
                       COALESCE(SUM(e.event_type = 'touchpoint'), 0) AS touchpoints,
                       COALESCE(SUM(e.event_type = 'conversion'), 0) AS conversions
                FROM users u
                LEFT JOIN events e ON e.user_id = u.user_id
                GROUP BY u.user_id
                ORDER BY u.updated_at DESC, u.user_id ASC
                """
            ).fetchall()
        return [
            {
                "user_id": row["user_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "touchpoints": row["touchpoints"],
                "conversions": row["conversions"],
            }
            for row in rows
        ]

    def get_user_events(self, user_id: str) -> dict:
        with self.read() as connection:
            user = connection.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if user is None:
                raise ApiError(404, "user_not_found", f"用户 {user_id!r} 不存在。")
            rows = connection.execute(
                "SELECT * FROM events WHERE user_id = ? "
                "ORDER BY occurred_at ASC, event_id ASC",
                (user_id,),
            ).fetchall()
        return {
            "user_id": user_id,
            "events": [
                {
                    "event_id": row["event_id"],
                    "type": row["event_type"],
                    "occurred_at": row["occurred_at"],
                    "channel": row["channel"],
                    "ingested_at": self._display_time(row["ingested_at"]),
                }
                for row in rows
            ],
        }

    def health(self) -> bool:
        try:
            with self.read() as connection:
                connection.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    # -- batch lifecycle ----------------------------------------------------

    def open_batch(
        self, user_ids: List[str], observation_minutes: int
    ) -> dict:
        """Freeze a name list and a touchpoint feature snapshot."""

        with self.transaction() as connection:
            opened_commit = self._commit_time(connection)
            opened_dt = parse_commit_time(opened_commit)
            window_start_dt = opened_dt - timedelta(
                seconds=FEATURE_LOOKBACK_SECONDS
            )
            observation_end_dt = opened_dt + timedelta(
                minutes=observation_minutes
            )
            opened_at = self._display_time(opened_commit)
            window_start = to_db_time(window_start_dt)
            observation_end = to_db_time(observation_end_dt)

            placeholders = ",".join("?" for _ in user_ids)
            found_rows = connection.execute(
                f"SELECT user_id FROM users WHERE user_id IN ({placeholders})",
                user_ids,
            ).fetchall()
            found = {row["user_id"] for row in found_rows}
            missing = sorted(set(user_ids) - found)
            if missing:
                raise ApiError(
                    404,
                    "user_not_found",
                    "名单中存在尚未入库的用户，无法开批。",
                    [{"user_id": user_id} for user_id in missing],
                )

            batch_id = self._next_batch_id(connection)

            # Features: touchpoints committed before this transaction and
            # occurring inside the preceding seven days (both bounds
            # inclusive on the event-time axis).  The ingest-time predicate
            # uses the suffix-bearing commit timestamp, while event-time
            # predicates compare plain ISO timestamps.
            feature_rows = connection.execute(
                """
                SELECT e.user_id, e.channel, COUNT(*) AS cnt
                FROM events e
                WHERE e.event_type = 'touchpoint'
                  AND e.user_id IN ({placeholders})
                  AND e.ingested_at < ?
                  AND e.occurred_at >= ?
                  AND e.occurred_at <= ?
                GROUP BY e.user_id, e.channel
                """.format(placeholders=placeholders),
                (*user_ids, opened_commit, window_start, opened_at),
            ).fetchall()

            features: Dict[str, Dict[str, int]] = {
                user_id: {} for user_id in user_ids
            }
            channel_set: set[str] = set()
            for row in feature_rows:
                features[row["user_id"]][row["channel"]] = row["cnt"]
                channel_set.add(row["channel"])
            channels = sorted(channel_set)

            connection.execute(
                """
                INSERT INTO batches (batch_id, user_count, observation_start,
                                     observation_end, opened_at, due_at,
                                     channels_json, status, first_close_at,
                                     closed_at, failure_reason, csv_content)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'open', NULL, NULL, NULL, NULL)
                """,
                (
                    batch_id,
                    len(user_ids),
                    opened_at,
                    observation_end,
                    opened_commit,
                    observation_end,
                    json.dumps(channels, ensure_ascii=False),
                ),
            )
            for user_id in user_ids:
                connection.execute(
                    """
                    INSERT INTO batch_members (batch_id, user_id, feature_json)
                    VALUES (?, ?, ?)
                    """,
                    (
                        batch_id,
                        user_id,
                        json.dumps(features[user_id], ensure_ascii=False),
                    ),
                )

        return self.get_batch(batch_id)

    @staticmethod
    def _next_batch_id(connection: sqlite3.Connection) -> str:
        row = connection.execute(
            "SELECT COUNT(*) AS n FROM batches"
        ).fetchone()
        return f"B{row['n'] + 1:06d}"

    def list_batches(self) -> List[dict]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT * FROM batches ORDER BY opened_at DESC, batch_id DESC"
            ).fetchall()
        return [self._batch_summary(dict(row)) for row in rows]

    def get_batch(self, batch_id: str) -> dict:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise ApiError(404, "batch_not_found", f"批次 {batch_id!r} 不存在。")
            members = connection.execute(
                "SELECT user_id, feature_json FROM batch_members "
                "WHERE batch_id = ? ORDER BY user_id ASC",
                (batch_id,),
            ).fetchall()
        summary = self._batch_summary(dict(row))
        labels_by_user: Dict[str, int] = {}
        if row["status"] == "published" and row["csv_content"]:
            reader = csv.DictReader(io.StringIO(row["csv_content"]))
            for csv_row in reader:
                labels_by_user[csv_row["user_id"]] = int(csv_row["label"])
        summary["members"] = [
            {
                "user_id": member["user_id"],
                "features": json.loads(member["feature_json"]),
                "label": labels_by_user.get(member["user_id"]),
            }
            for member in members
        ]
        return summary

    @staticmethod
    def _batch_summary(row: dict) -> dict:
        return {
            "batch_id": row["batch_id"],
            "status": row["status"],
            "user_count": row["user_count"],
            "observation_start": row["observation_start"],
            "observation_end": row["observation_end"],
            "opened_at": Database._display_time(row["opened_at"]),
            "due_at": row["due_at"],
            "first_close_at": (
                Database._display_time(row["first_close_at"])
                if row["first_close_at"]
                else None
            ),
            "closed_at": (
                Database._display_time(row["closed_at"])
                if row["closed_at"]
                else None
            ),
            "channels": json.loads(row["channels_json"]),
            "failure_reason": row["failure_reason"],
        }

    def close_batch(self, batch_id: str) -> dict:
        """Close a due batch, idempotently, using the first-accepted scope.

        In-process callers serialise on a per-batch lock: the first request
        claims and publishes, while concurrent callers wait and then return
        the identical published result.  Across processes (or after a
        restart) a leftover ``closing`` claim is resumed with its original
        ``first_close_at``, so the conversion scope is never widened.
        """

        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if row is None:
            raise ApiError(404, "batch_not_found", f"批次 {batch_id!r} 不存在。")

        if row["status"] == "published":
            return self.get_batch(batch_id)

        due_at = from_db_time(row["due_at"])
        if utc_now() < due_at:
            raise ApiError(
                409,
                "batch_not_due",
                "观察期尚未结束，不能结批。",
                [{"batch_id": batch_id, "due_at": row["due_at"]}],
            )

        lock = self._lock_for(batch_id)
        with lock:
            # Another in-process request may have published while we waited.
            current = self.get_batch(batch_id)
            if current["status"] == "published":
                return current
            claim = self._claim_close(batch_id)
            try:
                csv_text = self._build_csv(batch_id, claim["first_close_at"])
            except Exception as exc:  # pragma: no cover - defensive
                self._mark_failed(batch_id, str(exc))
                raise ApiError(
                    500,
                    "batch_generation_failed",
                    f"样本生成失败，可安全重试：{exc}",
                ) from exc
            self._publish(batch_id, csv_text)
        return self.get_batch(batch_id)

    def _claim_close(self, batch_id: str) -> dict:
        """Claim (or re-join) the close workflow.

        The compare-and-set guarantees that ``first_close_at`` is fixed by
        the first accepted close request forever:

        * ``open`` -> ``closing`` with a fresh claim timestamp;
        * ``failed`` -> ``closing`` reusing the original claim so a retry
          sees the same conversion scope;
        * ``closing`` -> the existing claim is reused.  This makes a retry
          after a process restart safe, and any duplicate worker computes
          the same deterministic CSV from the same frozen scope, so a
          duplicated publish is byte-identical.
        """

        first_close_commit = None
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT status, first_close_at FROM batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if row is None:
                raise ApiError(
                    404, "batch_not_found", f"批次 {batch_id!r} 不存在。"
                )
            if row["status"] == "open":
                first_close_commit = self._commit_time(connection)
                connection.execute(
                    "UPDATE batches SET status = 'closing', first_close_at = ? "
                    "WHERE batch_id = ? AND status = 'open'",
                    (first_close_commit, batch_id),
                )
                if connection.total_changes == 0:  # pragma: no cover - race guard
                    refreshed = connection.execute(
                        "SELECT first_close_at FROM batches WHERE batch_id = ?",
                        (batch_id,),
                    ).fetchone()
                    first_close_commit = refreshed["first_close_at"]
            else:
                # closing / failed both carry the immutable first claim.
                first_close_commit = row["first_close_at"]
                connection.execute(
                    "UPDATE batches SET status = 'closing', failure_reason = NULL "
                    "WHERE batch_id = ?",
                    (batch_id,),
                )
        return {"first_close_at": first_close_commit}

    # Test-only hook invoked after the close claim is fixed and before the
    # label query runs.  Production code never sets it; tests use it to
    # simulate events landing while generation is in progress.
    on_before_build_csv = None

    def _build_csv(self, batch_id: str, first_close_at: str) -> str:
        """Compute labels against the first-close snapshot and render CSV."""

        if self.on_before_build_csv is not None:
            self.on_before_build_csv(self, batch_id, first_close_at)

        with self.read() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise ApiError(
                    404, "batch_not_found", f"批次 {batch_id!r} 不存在。"
                )
            channels: List[str] = json.loads(batch["channels_json"])
            members = connection.execute(
                "SELECT user_id, feature_json FROM batch_members "
                "WHERE batch_id = ? ORDER BY user_id ASC",
                (batch_id,),
            ).fetchall()

            # Conversion scope: committed before the first close acceptance
            # and occurring inside the observation window [start, end).
            label_rows = connection.execute(
                """
                SELECT e.user_id, COUNT(*) AS cnt
                FROM events e
                WHERE e.event_type = 'conversion'
                  AND e.ingested_at < ?
                  AND e.occurred_at >= ?
                  AND e.occurred_at < ?
                GROUP BY e.user_id
                """,
                (
                    first_close_at,
                    batch["observation_start"],
                    batch["observation_end"],
                ),
            ).fetchall()
        positive_users = {row["user_id"] for row in label_rows if row["cnt"] > 0}

        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["user_id", *channels, "label"])
        for member in members:
            features = json.loads(member["feature_json"])
            writer.writerow(
                [
                    member["user_id"],
                    *[features.get(channel, 0) for channel in channels],
                    1 if member["user_id"] in positive_users else 0,
                ]
            )
        return buffer.getvalue()

    def _publish(self, batch_id: str, csv_text: str) -> None:
        with self.transaction() as connection:
            closed_at = self._commit_time(connection)
            connection.execute(
                """
                UPDATE batches
                SET status = 'published', csv_content = ?, closed_at = ?,
                    failure_reason = NULL
                WHERE batch_id = ?
                """,
                (csv_text, closed_at, batch_id),
            )

    def _mark_failed(self, batch_id: str, reason: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE batches SET status = 'failed', failure_reason = ? "
                "WHERE batch_id = ?",
                (reason[:500], batch_id),
            )

    def download_csv(self, batch_id: str) -> tuple[str, str]:
        """Return ``(csv_text, filename)`` for a published batch."""

        with self.read() as connection:
            row = connection.execute(
                "SELECT status, csv_content FROM batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
        if row is None:
            raise ApiError(404, "batch_not_found", f"批次 {batch_id!r} 不存在。")
        if row["status"] != "published" or row["csv_content"] is None:
            raise ApiError(
                409,
                "batch_not_published",
                "该批次尚未发布 CSV，请先在观察期结束后结批。",
                [{"batch_id": batch_id, "status": row["status"]}],
            )
        return row["csv_content"], f"{batch_id}.csv"


__all__ = ["Database", "from_db_time", "to_db_time", "utc_now"]

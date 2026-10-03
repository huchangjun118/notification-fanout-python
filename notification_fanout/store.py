"""Durable storage for notifications (SQLite, stdlib only).

Durability / consistency guarantees:

* ``(event_id, channel)`` has a UNIQUE constraint: even under a racy double
  trigger, the same event can only ever create one notification per channel,
  while the same event on *different* channels stays independent.
* Every state update is a compare-and-swap
  (``UPDATE ... WHERE id=? AND status IN (...)``), so two concurrent workers
  can never write contradictory terminal states.
* Every real channel invocation is appended to ``delivery_attempts``, which
  also backs the per-user/channel frequency cap and explains retries.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .models import (
    Channel,
    Notification,
    NotificationStatus,
    StateChange,
    Urgency,
)

_TERMINAL = (
    NotificationStatus.DELIVERED.value,
    NotificationStatus.DEAD_LETTER.value,
    NotificationStatus.DROPPED.value,
)


def _iso(dt: datetime) -> str:
    """Serialize to an ISO string normalized to UTC.

    Normalization keeps lexicographic comparisons (used by MIN/>= queries)
    correct even if callers supply different UTC offsets.
    """
    from datetime import timezone

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class NotificationStore:
    """Thread-safe SQLite-backed notification repository."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        # One guarded connection: SQLite serializes writes and all our
        # transactions are short, so a single lock is sufficient and removes
        # cross-thread connection pitfalls.
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,  # explicit BEGIN/COMMIT
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS notifications (
                    id                  TEXT PRIMARY KEY,
                    event_id            TEXT NOT NULL,
                    user_id             TEXT NOT NULL,
                    channel             TEXT NOT NULL,
                    urgency             TEXT NOT NULL,
                    title               TEXT NOT NULL,
                    body                TEXT NOT NULL DEFAULT '',
                    status              TEXT NOT NULL,
                    not_before          TEXT NOT NULL,
                    attempts            INTEGER NOT NULL DEFAULT 0,
                    max_attempts        INTEGER NOT NULL DEFAULT 5,
                    created_at          TEXT NOT NULL,
                    updated_at          TEXT NOT NULL,
                    delivered_at        TEXT,
                    dead_letter_reason  TEXT NOT NULL DEFAULT '',
                    history             TEXT NOT NULL DEFAULT '[]',
                    metadata            TEXT NOT NULL DEFAULT '{}',
                    UNIQUE(event_id, channel)
                );
                CREATE INDEX IF NOT EXISTS idx_notifications_due
                    ON notifications(status, not_before);
                CREATE INDEX IF NOT EXISTS idx_notifications_event
                    ON notifications(event_id);
                CREATE INDEX IF NOT EXISTS idx_notifications_user
                    ON notifications(user_id, channel);

                CREATE TABLE IF NOT EXISTS delivery_attempts (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_id  TEXT NOT NULL,
                    attempt_no       INTEGER NOT NULL,
                    at               TEXT NOT NULL,
                    success          INTEGER NOT NULL,
                    error_kind       TEXT NOT NULL DEFAULT '',
                    error_message    TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(notification_id) REFERENCES notifications(id)
                );
                CREATE INDEX IF NOT EXISTS idx_attempts_user
                    ON delivery_attempts(notification_id, at);
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------- serialization
    @staticmethod
    def _row_to_notification(row: sqlite3.Row) -> Notification:
        return Notification(
            id=row["id"],
            event_id=row["event_id"],
            user_id=row["user_id"],
            channel=Channel(row["channel"]),
            urgency=Urgency(row["urgency"]),
            payload_title=row["title"],
            payload_body=row["body"],
            status=NotificationStatus(row["status"]),
            not_before=datetime.fromisoformat(row["not_before"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            delivered_at=_parse_dt(row["delivered_at"]),
            dead_letter_reason=row["dead_letter_reason"],
            history=[
                StateChange(
                    status=NotificationStatus(h["status"]),
                    at=datetime.fromisoformat(h["at"]),
                    reason=h.get("reason", ""),
                    attempt=h.get("attempt", 0),
                )
                for h in json.loads(row["history"])
            ],
            metadata=json.loads(row["metadata"]),
        )

    def _insert_row(self, cur: sqlite3.Cursor, n: Notification) -> None:
        cur.execute(
            """INSERT INTO notifications (
                id, event_id, user_id, channel, urgency, title, body, status,
                not_before, attempts, max_attempts, created_at, updated_at,
                delivered_at, dead_letter_reason, history, metadata
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                n.id,
                n.event_id,
                n.user_id,
                n.channel.value,
                n.urgency.value,
                n.payload_title,
                n.payload_body,
                n.status.value,
                _iso(n.not_before),
                n.attempts,
                n.max_attempts,
                _iso(n.created_at),
                _iso(n.updated_at),
                _iso(n.delivered_at) if n.delivered_at else None,
                n.dead_letter_reason,
                json.dumps(
                    [
                        {
                            "status": c.status.value,
                            "at": c.at.isoformat(),
                            "reason": c.reason,
                            "attempt": c.attempt,
                        }
                        for c in n.history
                    ]
                ),
                json.dumps(n.metadata),
            ),
        )

    # ----------------------------------------------------------------- commands
    def insert_notification(self, n: Notification) -> bool:
        """Insert a new notification.

        Returns ``True`` if inserted, ``False`` if a notification for the same
        ``(event_id, channel)`` already exists (duplicate trigger suppressed at
        the storage layer).
        """
        with self._lock:
            try:
                self._insert_row(self._conn.cursor(), n)
            except sqlite3.IntegrityError:
                return False
            return True

    def get(self, notification_id: str) -> Notification | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM notifications WHERE id=?", (notification_id,)
            ).fetchone()
            return self._row_to_notification(row) if row else None

    def get_by_event_channel(
        self, event_id: str, channel: Channel
    ) -> Notification | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM notifications WHERE event_id=? AND channel=?",
                (event_id, channel.value),
            ).fetchone()
            return self._row_to_notification(row) if row else None

    def list_by_event(self, event_id: str) -> list[Notification]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM notifications WHERE event_id=? ORDER BY channel",
                (event_id,),
            ).fetchall()
            return [self._row_to_notification(r) for r in rows]

    def list_non_terminal(self) -> list[Notification]:
        """All notifications not in a terminal state (startup recovery)."""
        placeholders = ",".join("?" for _ in _TERMINAL)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM notifications WHERE status NOT IN "
                f"({placeholders}) ORDER BY not_before",
                _TERMINAL,
            ).fetchall()
            return [self._row_to_notification(r) for r in rows]

    def list_dead_letters(self, limit: int = 100) -> list[Notification]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM notifications WHERE status=? "
                "ORDER BY updated_at DESC LIMIT ?",
                (NotificationStatus.DEAD_LETTER.value, limit),
            ).fetchall()
            return [self._row_to_notification(r) for r in rows]

    def compare_and_update(
        self,
        n: Notification,
        expected: Iterable[NotificationStatus] | None = None,
    ) -> bool:
        """Persist ``n`` only if its stored status is in ``expected``.

        ``expected=None`` allows any *non-terminal* stored state (terminal
        states are immutable and always rejected). Returns ``True`` on success.
        """
        expected_values = (
            [s.value for s in expected]
            if expected is not None
            else [s.value for s in NotificationStatus if s.value not in _TERMINAL]
        )
        if not expected_values:
            return False
        placeholders = ",".join("?" for _ in expected_values)
        n.updated_at = n.updated_at  # keep caller-provided timestamp
        with self._lock:
            cur = self._conn.execute(
                f"""UPDATE notifications SET
                      status=?, not_before=?, attempts=?, max_attempts=?,
                      updated_at=?, delivered_at=?, dead_letter_reason=?,
                      history=?, metadata=?
                    WHERE id=? AND status IN ({placeholders})""",
                (
                    n.status.value,
                    _iso(n.not_before),
                    n.attempts,
                    n.max_attempts,
                    _iso(n.updated_at),
                    _iso(n.delivered_at) if n.delivered_at else None,
                    n.dead_letter_reason,
                    json.dumps(
                        [
                            {
                                "status": c.status.value,
                                "at": c.at.isoformat(),
                                "reason": c.reason,
                                "attempt": c.attempt,
                            }
                            for c in n.history
                        ]
                    ),
                    json.dumps(n.metadata),
                    n.id,
                    *expected_values,
                ),
            )
            return cur.rowcount == 1

    def record_attempt(
        self,
        notification_id: str,
        attempt_no: int,
        at: datetime,
        success: bool,
        error_kind: str = "",
        error_message: str = "",
    ) -> None:
        """Append one real delivery attempt (used by frequency accounting)."""
        with self._lock:
            self._conn.execute(
                """INSERT INTO delivery_attempts
                   (notification_id, attempt_no, at, success,
                    error_kind, error_message)
                   VALUES (?,?,?,?,?,?)""",
                (
                    notification_id,
                    attempt_no,
                    _iso(at),
                    1 if success else 0,
                    error_kind,
                    error_message,
                ),
            )

    def count_attempts_since(
        self, user_id: str, channel: Channel, since: datetime
    ) -> int:
        """Count channel send attempts for one user since a point in time.

        Counts every actual invocation (successful or not): a failing downstream
        still consumes downstream capacity, so it must count against the cap.
        """
        with self._lock:
            row = self._conn.execute(
                """SELECT COUNT(*) FROM delivery_attempts a
                   JOIN notifications n ON n.id = a.notification_id
                   WHERE n.user_id=? AND n.channel=? AND a.at >= ?""",
                (user_id, channel.value, _iso(since)),
            ).fetchone()
            return int(row[0])

    def oldest_attempt_at(
        self, user_id: str, channel: Channel, since: datetime
    ) -> datetime | None:
        """Timestamp of the oldest counted attempt inside the cap window.

        Used to compute when a frequency-capped notification may be deferred
        to (``oldest + window`` frees one slot).
        """
        with self._lock:
            row = self._conn.execute(
                """SELECT MIN(a.at) FROM delivery_attempts a
                   JOIN notifications n ON n.id = a.notification_id
                   WHERE n.user_id=? AND n.channel=? AND a.at >= ?""",
                (user_id, channel.value, _iso(since)),
            ).fetchone()
            return _parse_dt(row[0]) if row and row[0] else None

    def list_attempts(self, notification_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM delivery_attempts WHERE notification_id=? "
                    "ORDER BY attempt_no",
                    (notification_id,),
                ).fetchall()
            )

"""SQLite-backed persistence. Notifications survive process restarts.

All state transitions use conditional UPDATEs so that concurrent workers can
never produce contradictory terminal states: only one worker can win a
transition, and terminal rows are never updated again.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

from .models import (
    Notification,
    NotificationStatus,
    StatusChange,
    UserPreferences,
    idempotency_key,
)

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    channels_json TEXT NOT NULL DEFAULT '{}',
    quiet_start_hour INTEGER,
    quiet_end_hour INTEGER
);
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    scheduled_at REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    urgent INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT NOT NULL UNIQUE,
    last_error TEXT,
    reason TEXT,
    leased_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(event_id, channel)
);
CREATE TABLE IF NOT EXISTS status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    notification_id INTEGER NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT,
    ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letters (
    notification_id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    reason TEXT NOT NULL,
    ts REAL NOT NULL
);
"""

_TERMINAL = "'sent','skipped','dropped','dead_letter'"


class Repository:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def _row_to_notification(self, row: sqlite3.Row) -> Notification:
        return Notification(
            id=row["id"],
            event_id=row["event_id"],
            user_id=row["user_id"],
            channel=row["channel"],
            payload=json.loads(row["payload_json"]),
            status=NotificationStatus(row["status"]),
            scheduled_at=row["scheduled_at"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            urgent=bool(row["urgent"]),
            idempotency_key=row["idempotency_key"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_error=row["last_error"],
            reason=row["reason"],
            leased_at=row["leased_at"],
        )

    def _add_history(
        self,
        notification_id: int,
        from_status: NotificationStatus | None,
        to_status: NotificationStatus,
        reason: str | None,
        now: float,
    ) -> None:
        self._conn.execute(
            "INSERT INTO status_history (notification_id, from_status, to_status, reason, ts) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                notification_id,
                from_status.value if from_status else None,
                to_status.value,
                reason,
                now,
            ),
        )

    # -- preferences -----------------------------------------------------
    def get_preferences(self, user_id: str) -> UserPreferences | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
        if row is None:
            return None
        return UserPreferences(
            user_id=row["user_id"],
            channels=json.loads(row["channels_json"]),
            quiet_start_hour=row["quiet_start_hour"],
            quiet_end_hour=row["quiet_end_hour"],
        )

    def upsert_preferences(self, prefs: UserPreferences) -> UserPreferences:
        with self._lock:
            self._conn.execute(
                "INSERT INTO users (user_id, channels_json, quiet_start_hour, quiet_end_hour) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                "channels_json = excluded.channels_json, "
                "quiet_start_hour = excluded.quiet_start_hour, "
                "quiet_end_hour = excluded.quiet_end_hour",
                (
                    prefs.user_id,
                    json.dumps(prefs.channels),
                    prefs.quiet_start_hour,
                    prefs.quiet_end_hour,
                ),
            )
            self._conn.commit()
        return prefs

    # -- notifications ---------------------------------------------------
    def create_notification(
        self,
        *,
        event_id: str,
        user_id: str,
        channel: str,
        payload: dict[str, Any],
        scheduled_at: float,
        urgent: bool,
        max_attempts: int,
        now: float,
    ) -> Notification:
        """Insert a notification, returning the existing one on idempotent replay."""
        key = idempotency_key(event_id, channel)
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM notifications WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if existing is not None:
                return self._row_to_notification(existing)
            cur = self._conn.execute(
                "INSERT INTO notifications "
                "(event_id, user_id, channel, payload_json, status, scheduled_at, "
                " attempts, max_attempts, urgent, idempotency_key, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?, ?)",
                (
                    event_id,
                    user_id,
                    channel,
                    json.dumps(payload),
                    scheduled_at,
                    max_attempts,
                    1 if urgent else 0,
                    key,
                    now,
                    now,
                ),
            )
            self._add_history(cur.lastrowid, None, NotificationStatus.PENDING, "created", now)
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM notifications WHERE id = ?", (cur.lastrowid,)
            ).fetchone()
            return self._row_to_notification(row)

    def get_notification(self, notification_id: int) -> Notification | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM notifications WHERE id = ?", (notification_id,)
            ).fetchone()
        return self._row_to_notification(row) if row else None

    def get_by_event(self, event_id: str) -> list[Notification]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM notifications WHERE event_id = ? ORDER BY id",
                (event_id,),
            ).fetchall()
        return [self._row_to_notification(r) for r in rows]

    def fetch_due(self, now: float, limit: int) -> list[Notification]:
        """Return PENDING notifications whose scheduled time has arrived."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM notifications "
                "WHERE status = 'pending' AND scheduled_at <= ? "
                "ORDER BY scheduled_at, id LIMIT ?",
                (now, limit),
            ).fetchall()
        return [self._row_to_notification(r) for r in rows]

    def count_sent_since(self, user_id: str, channel: str, since: float) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM notifications "
                "WHERE user_id = ? AND channel = ? AND status = 'sent' AND updated_at >= ?",
                (user_id, channel, since),
            ).fetchone()
        return int(row["c"])

    def latest_sent_ts(self, user_id: str, channel: str) -> float | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(updated_at) AS ts FROM notifications "
                "WHERE user_id = ? AND channel = ? AND status = 'sent'",
                (user_id, channel),
            ).fetchone()
        return row["ts"]

    # -- state transitions (all conditional) ------------------------------
    def try_claim(self, notification_id: int, now: float) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE notifications SET status = 'sending', leased_at = ?, updated_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (now, now, notification_id),
            )
            if cur.rowcount:
                self._add_history(
                    notification_id,
                    NotificationStatus.PENDING,
                    NotificationStatus.SENDING,
                    "claimed by worker",
                    now,
                )
                self._conn.commit()
            return cur.rowcount > 0

    def _terminal_transition(
        self,
        notification_id: int,
        to_status: NotificationStatus,
        reason: str,
        now: float,
        *,
        dead_letter: bool = False,
    ) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM notifications WHERE id = ?", (notification_id,)
            ).fetchone()
            if row is None or row["status"] in (
                NotificationStatus.SENT.value,
                NotificationStatus.SKIPPED.value,
                NotificationStatus.DROPPED.value,
                NotificationStatus.DEAD_LETTER.value,
            ):
                return False
            cur = self._conn.execute(
                f"UPDATE notifications SET status = ?, reason = ?, updated_at = ?, "
                f"attempts = attempts + CASE WHEN ? = 'sending' THEN 1 ELSE 0 END "
                f"WHERE id = ? AND status NOT IN ({_TERMINAL})",
                (to_status.value, reason, now, row["status"], notification_id),
            )
            if cur.rowcount == 0:
                return False
            if dead_letter:
                n = self._conn.execute(
                    "SELECT event_id, user_id, channel FROM notifications WHERE id = ?",
                    (notification_id,),
                ).fetchone()
                self._conn.execute(
                    "INSERT OR REPLACE INTO dead_letters "
                    "(notification_id, event_id, user_id, channel, reason, ts) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        notification_id,
                        n["event_id"],
                        n["user_id"],
                        n["channel"],
                        reason,
                        now,
                    ),
                )
            self._add_history(
                notification_id,
                NotificationStatus(row["status"]),
                to_status,
                reason,
                now,
            )
            self._conn.commit()
            return True

    def mark_sent(self, notification_id: int, reason: str | None = None) -> bool:
        return self._terminal_transition(
            notification_id,
            NotificationStatus.SENT,
            reason or "delivered",
            time.time(),
        )

    def mark_skipped(self, notification_id: int, reason: str) -> bool:
        return self._terminal_transition(
            notification_id, NotificationStatus.SKIPPED, reason, time.time()
        )

    def mark_dropped(self, notification_id: int, reason: str) -> bool:
        return self._terminal_transition(
            notification_id, NotificationStatus.DROPPED, reason, time.time()
        )

    def mark_dead_letter(self, notification_id: int, reason: str) -> bool:
        return self._terminal_transition(
            notification_id,
            NotificationStatus.DEAD_LETTER,
            reason,
            time.time(),
            dead_letter=True,
        )

    def requeue_for_retry(
        self, notification_id: int, next_at: float, error: str, now: float
    ) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM notifications WHERE id = ?", (notification_id,)
            ).fetchone()
            if row is None or row["status"] != NotificationStatus.SENDING.value:
                return False
            cur = self._conn.execute(
                "UPDATE notifications SET status = 'pending', scheduled_at = ?, "
                "attempts = attempts + 1, last_error = ?, leased_at = NULL, updated_at = ? "
                "WHERE id = ? AND status = 'sending'",
                (next_at, error, now, notification_id),
            )
            if cur.rowcount:
                self._add_history(
                    notification_id,
                    NotificationStatus.SENDING,
                    NotificationStatus.PENDING,
                    f"retry scheduled: {error}",
                    now,
                )
                self._conn.commit()
            return cur.rowcount > 0

    def requeue_deferred(
        self, notification_id: int, next_at: float, reason: str
    ) -> bool:
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM notifications WHERE id = ?", (notification_id,)
            ).fetchone()
            if row is None or row["status"] != NotificationStatus.PENDING.value:
                return False
            cur = self._conn.execute(
                "UPDATE notifications SET scheduled_at = ?, reason = ?, updated_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (next_at, reason, now, notification_id),
            )
            if cur.rowcount:
                self._add_history(
                    notification_id,
                    NotificationStatus.PENDING,
                    NotificationStatus.PENDING,
                    reason,
                    now,
                )
                self._conn.commit()
            return cur.rowcount > 0

    def reset_stale_leases(self, now: float, lease_timeout: float) -> int:
        """Recover SENDING rows whose worker died (restart / crash)."""
        with self._lock:
            cutoff = now - lease_timeout
            rows = self._conn.execute(
                "SELECT id FROM notifications "
                "WHERE status = 'sending' AND leased_at IS NOT NULL AND leased_at < ?",
                (cutoff,),
            ).fetchall()
            for r in rows:
                self._conn.execute(
                    "UPDATE notifications SET status = 'pending', leased_at = NULL, "
                    "reason = 'lease recovered after worker loss', updated_at = ? "
                    "WHERE id = ? AND status = 'sending'",
                    (now, r["id"]),
                )
                self._add_history(
                    r["id"],
                    NotificationStatus.SENDING,
                    NotificationStatus.PENDING,
                    "stale lease reset",
                    now,
                )
            self._conn.commit()
            return len(rows)

    # -- queries ----------------------------------------------------------
    def list_dead_letters(self, limit: int = 100) -> list[Notification]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT n.* FROM dead_letters d "
                "JOIN notifications n ON n.id = d.notification_id "
                "ORDER BY d.ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_notification(r) for r in rows]

    def list_history(self, notification_id: int) -> list[StatusChange]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM status_history WHERE notification_id = ? ORDER BY id",
                (notification_id,),
            ).fetchall()
        return [
            StatusChange(
                id=r["id"],
                notification_id=r["notification_id"],
                from_status=NotificationStatus(r["from_status"]) if r["from_status"] else None,
                to_status=NotificationStatus(r["to_status"]),
                reason=r["reason"],
                ts=r["ts"],
            )
            for r in rows
        ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

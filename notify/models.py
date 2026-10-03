"""Core domain types for the notification fanout service."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class NotificationStatus(str, Enum):
    PENDING = "pending"          # waiting for its scheduled time or a retry slot
    SENDING = "sending"          # leased by a dispatch worker
    SENT = "sent"                # terminal: delivered successfully
    SKIPPED = "skipped"          # terminal: channel disabled by user preference
    DROPPED = "dropped"          # terminal: dropped by frequency-cap policy
    DEAD_LETTER = "dead_letter"  # terminal: failed and no longer retried


TERMINAL_STATUSES = frozenset(
    {
        NotificationStatus.SENT,
        NotificationStatus.SKIPPED,
        NotificationStatus.DROPPED,
        NotificationStatus.DEAD_LETTER,
    }
)


class ChannelError(Exception):
    """Base class for channel delivery failures."""

    retryable: bool = False

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class RetryableError(ChannelError):
    """Transient failure (timeout, 5xx, 429): safe to retry with backoff."""

    def __init__(self, message: str = "retryable channel failure") -> None:
        super().__init__(message, retryable=True)


class NonRetryableError(ChannelError):
    """Permanent failure (bad request, 4xx): must not be replayed."""

    def __init__(self, message: str = "non-retryable channel failure") -> None:
        super().__init__(message, retryable=False)


@dataclass
class Notification:
    id: int
    event_id: str
    user_id: str
    channel: str
    payload: dict[str, Any]
    status: NotificationStatus
    scheduled_at: float
    attempts: int
    max_attempts: int
    urgent: bool
    idempotency_key: str
    created_at: float
    updated_at: float
    last_error: str | None = None
    reason: str | None = None
    leased_at: float | None = None


@dataclass
class UserPreferences:
    user_id: str
    channels: dict[str, bool] = field(default_factory=dict)
    quiet_start_hour: int | None = None
    quiet_end_hour: int | None = None


@dataclass
class StatusChange:
    id: int
    notification_id: int
    from_status: NotificationStatus | None
    to_status: NotificationStatus
    reason: str | None
    ts: float


@dataclass
class IngestEvent:
    event_id: str
    user_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    channels: list[str] | None = None
    scheduled_at: float | None = None
    urgent: bool = False
    max_attempts: int | None = None


def idempotency_key(event_id: str, channel: str) -> str:
    """Same event on the same channel deduplicates; different channels do not."""
    return f"{event_id}::{channel}"

"""Domain model types for notification fan-out.

A business :class:`Event` is routed to one :class:`Notification` per subscribed
channel.  Notifications are the unit of scheduling, delivery, retry and
state-query; events only carry the correlation id used for idempotency.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    """Generate a prefixed random identifier."""
    return f"{prefix}_{uuid.uuid4().hex}"


class Channel(str, enum.Enum):
    """Supported notification channels."""

    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    INBOX = "inbox"


class Urgency(str, enum.Enum):
    """Event urgency; urgent events bypass quiet hours and frequency caps."""

    NORMAL = "normal"
    URGENT = "urgent"


class NotificationStatus(str, enum.Enum):
    """Lifecycle states of a single (event, channel) notification.

    ``DELIVERING`` is an in-flight, *non-terminal* state: if the process dies
    while delivering, the notification is recovered and retried on restart.
    The terminal states (:attr:`DELIVERED`, :attr:`DEAD_LETTER`,
    :attr:`DROPPED`) are unique and immutable once entered.
    """

    PENDING = "pending"        # waiting for its earliest delivery time
    DELIVERING = "delivering"  # picked up by a worker, channel call in flight
    DELIVERED = "delivered"    # terminal: acknowledged by the channel
    RETRYING = "retrying"      # a retryable failure happened, backing off
    DEAD_LETTER = "dead_letter"  # terminal: retries exhausted / fatal failure
    DROPPED = "dropped"        # terminal: frequency policy decided to discard


class ExceedPolicy(str, enum.Enum):
    """What happens when a notification exceeds the frequency cap."""

    DEFER = "defer"  # push the notification past the cap window
    DROP = "drop"    # discard the notification permanently (explainable)


class ErrorKind(str, enum.Enum):
    """Classification of delivery failures.

    Only :attr:`RETRYABLE` failures are retried; :attr:`FATAL` failures move
    the notification straight to the dead-letter queue and are never replayed.
    """

    RETRYABLE = "retryable"
    FATAL = "fatal"


@dataclass
class Subscription:
    """A user's per-channel subscription preference."""

    user_id: str
    enabled_channels: frozenset[Channel]

    def is_enabled(self, channel: Channel) -> bool:
        return channel in self.enabled_channels


@dataclass
class Event:
    """An upstream business event to be notified about."""

    event_type: str
    user_id: str
    title: str
    body: str = ""
    urgency: Urgency = Urgency.NORMAL
    #: earliest delivery time; ``None`` means "as soon as possible"
    not_before: datetime | None = None
    #: caller-supplied correlation id; the same id is delivered at most once
    #: per channel (defaults to a random id if absent)
    event_id: str = field(default_factory=lambda: new_id("evt"))
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class DeliveryResult:
    """Outcome of one channel delivery attempt."""

    success: bool
    error_kind: ErrorKind | None = None
    error_message: str = ""


@dataclass
class StateChange:
    """An append-only record explaining how a notification reached a state."""

    status: NotificationStatus
    at: datetime
    reason: str = ""
    attempt: int = 0


@dataclass
class Notification:
    """A schedulable, durable unit of delivery for one event on one channel."""

    id: str
    event_id: str
    user_id: str
    channel: Channel
    urgency: Urgency
    payload_title: str
    payload_body: str
    status: NotificationStatus
    not_before: datetime
    attempts: int = 0
    max_attempts: int = 5
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    delivered_at: datetime | None = None
    dead_letter_reason: str = ""
    history: list[StateChange] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            NotificationStatus.DELIVERED,
            NotificationStatus.DEAD_LETTER,
            NotificationStatus.DROPPED,
        )

    def explain(self) -> str:
        """Human-readable explanation of the current terminal/current state."""
        lines = [f"notification={self.id} event={self.event_id} "
                 f"user={self.user_id} channel={self.channel.value} "
                 f"status={self.status.value} attempts={self.attempts}"]
        for change in self.history:
            stamp = change.at.astimezone(timezone.utc).isoformat()
            lines.append(f"  {stamp} -> {change.status.value}"
                         f" (attempt {change.attempt}) {change.reason}")
        if self.status is NotificationStatus.DEAD_LETTER:
            lines.append(f"  dead-letter reason: {self.dead_letter_reason}")
        return "\n".join(lines)

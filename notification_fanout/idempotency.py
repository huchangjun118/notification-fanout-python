"""Idempotency support.

Idempotency scope is **(event_id, channel)**: the same event triggered twice
must produce a single delivery on a given channel, while the same event on two
different channels are independent notifications and are never merged.

The store's ``UNIQUE(event_id, channel)`` constraint is the durable guarantee;
this module provides the key derivation and a small in-process short-circuit
cache that avoids redundant work before hitting the database.
"""

from __future__ import annotations

import threading

from .models import Channel

# Reserved metadata key carrying the caller-provided idempotency key.
IDEMPOTENCY_KEY_META = "idempotency_key"


def dedupe_key(event_id: str, channel: Channel) -> tuple[str, Channel]:
    """Return the canonical dedupe key for an event/channel pair."""
    return (event_id, channel)


class IdempotencyCache:
    """Process-local cache of notification ids keyed by (event_id, channel).

    This is only an optimisation; correctness survives restarts and races via
    the database unique constraint. Entries are remembered for the process
    lifetime (bounded by the number of distinct triggered pairs).
    """

    def __init__(self) -> None:
        self._seen: dict[tuple[str, Channel], str] = {}
        self._lock = threading.Lock()

    def remember(self, event_id: str, channel: Channel, notification_id: str) -> str | None:
        """Record a new pair. Returns the existing notification id if duplicate."""
        key = dedupe_key(event_id, channel)
        with self._lock:
            existing = self._seen.get(key)
            if existing is not None:
                return existing
            self._seen[key] = notification_id
            return None

    def get(self, event_id: str, channel: Channel) -> str | None:
        with self._lock:
            return self._seen.get(dedupe_key(event_id, channel))

    def warm(self, event_id: str, channel: Channel, notification_id: str) -> None:
        """Populate from a database read (e.g. duplicate trigger recovery)."""
        with self._lock:
            self._seen.setdefault(dedupe_key(event_id, channel), notification_id)

"""Route events to channels according to user subscription preferences.

One event expands to one :class:`Notification` per *enabled* channel; disabled
channels are skipped (and recorded in the routing trace for explainability).
Channel failures are isolated downstream (each notification is scheduled and
delivered independently), so routing itself never removes another channel.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .models import (
    Channel,
    Event,
    Notification,
    NotificationStatus,
    StateChange,
    Subscription,
    new_id,
)


class SubscriptionBook:
    """In-memory, thread-safe store of user subscription preferences."""

    def __init__(self) -> None:
        self._subs: dict[str, Subscription] = {}
        self._lock = threading.Lock()

    def put(self, subscription: Subscription) -> None:
        with self._lock:
            self._subs[subscription.user_id] = subscription

    def enable(self, user_id: str, channel: Channel) -> None:
        with self._lock:
            sub = self._subs.get(user_id)
            channels = set(sub.enabled_channels if sub else ()) | {channel}
            self._subs[user_id] = Subscription(user_id, frozenset(channels))

    def disable(self, user_id: str, channel: Channel) -> None:
        with self._lock:
            sub = self._subs.get(user_id)
            if not sub:
                return
            channels = set(sub.enabled_channels) - {channel}
            self._subs[user_id] = Subscription(user_id, frozenset(channels))

    def get(self, user_id: str) -> Subscription | None:
        with self._lock:
            return self._subs.get(user_id)

    def enabled_channels(self, user_id: str) -> frozenset[Channel]:
        sub = self.get(user_id)
        return sub.enabled_channels if sub else frozenset()


@dataclass
class RoutingTrace:
    """Explainable result of expanding an event."""

    event_id: str
    planned: list[Notification] = field(default_factory=list)
    skipped_disabled: list[Channel] = field(default_factory=list)
    skipped_unsupported: list[Channel] = field(default_factory=list)

    @property
    def channel_count(self) -> int:
        return len(self.planned)


class Router:
    """Expands events into per-channel notifications."""

    def __init__(
        self,
        subscriptions: SubscriptionBook,
        supported_channels: frozenset[Channel] | None = None,
        max_attempts: int = 5,
    ) -> None:
        self._subscriptions = subscriptions
        self._supported = supported_channels or frozenset(Channel)
        self._max_attempts = max_attempts

    def route(
        self, event: Event, now_getter
    ) -> RoutingTrace:
        """Build one notification per subscribed+supported channel.

        ``now_getter`` is a zero-arg callable returning "now" (injected clock)
        used to stamp creation and the default earliest-delivery time.
        """
        trace = RoutingTrace(event_id=event.event_id)
        enabled = self._subscriptions.enabled_channels(event.user_id)
        not_before = event.not_before or now_getter()
        for channel in Channel:  # stable enum order
            if channel not in self._supported:
                if channel in enabled:
                    trace.skipped_unsupported.append(channel)
                continue
            if channel not in enabled:
                trace.skipped_disabled.append(channel)
                continue
            now = now_getter()
            notification = Notification(
                id=new_id("ntf"),
                event_id=event.event_id,
                user_id=event.user_id,
                channel=channel,
                urgency=event.urgency,
                payload_title=event.title,
                payload_body=event.body,
                status=NotificationStatus.PENDING,
                not_before=not_before,
                max_attempts=self._max_attempts,
                created_at=now,
                updated_at=now,
                metadata=dict(event.metadata),
                history=[
                    StateChange(
                        status=NotificationStatus.PENDING,
                        at=now,
                        reason=f"routed to {channel.value}",
                    )
                ],
            )
            trace.planned.append(notification)
        return trace

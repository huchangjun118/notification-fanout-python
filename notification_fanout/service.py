"""High-level notification service facade.

Wires subscriptions, routing, persistence and the scheduler behind one small
API used by tests and the HTTP layer. Key guarantees visible here:

* :meth:`trigger_event` is idempotent per (event_id, channel): duplicate
  triggers return the *existing* notifications without creating new sends;
  different channels of the same event remain separate notifications.
* query methods return a consistent snapshot; terminal state is unique because
  every transition goes through the store's compare-and-swap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .channels import ChannelRegistry, InProcessChannel
from .clock import Clock, SystemClock
from .config import AtomicConfig, DispatchConfig
from .idempotency import IdempotencyCache
from .models import (
    Channel,
    Event,
    Notification,
    Subscription,
)
from .policy import FrequencyCap, QuietHours, RetryPolicy
from .router import Router, RoutingTrace, SubscriptionBook
from .scheduler import Scheduler
from .store import NotificationStore


@dataclass
class TriggerResult:
    """Result of triggering one event: per-channel notifications + trace."""

    event_id: str
    notifications: list[Notification] = field(default_factory=list)
    duplicate: bool = False
    skipped_disabled: list[Channel] = field(default_factory=list)
    skipped_unsupported: list[Channel] = field(default_factory=list)

    @property
    def channel_count(self) -> int:
        return len(self.notifications)


class NotificationService:
    def __init__(
        self,
        store: NotificationStore | None = None,
        channels: ChannelRegistry | None = None,
        config: AtomicConfig | None = None,
        clock: Clock | None = None,
        retry_policy: RetryPolicy | None = None,
        quiet_hours: QuietHours | None = None,
        frequency_caps: dict[Channel, FrequencyCap] | None = None,
        subscriptions: SubscriptionBook | None = None,
        auto_start: bool = True,
    ) -> None:
        self.clock = clock or SystemClock()
        self.store = store or NotificationStore(":memory:")
        self.channels = channels or self._default_registry()
        self.config = config or AtomicConfig(DispatchConfig())
        self.subscriptions = subscriptions or SubscriptionBook()
        supported = frozenset(
            ch for ch in Channel if self.channels.has(ch)
        )
        max_attempts = (retry_policy or RetryPolicy()).max_attempts
        self.router = Router(
            self.subscriptions,
            supported_channels=supported,
            max_attempts=max_attempts,
        )
        self.idempotency = IdempotencyCache()
        self._scheduler: Scheduler | None = None
        self._scheduler_args = dict(
            retry_policy=retry_policy,
            quiet_hours=quiet_hours,
            frequency_caps=frequency_caps,
        )
        if auto_start:
            self.start()

    @staticmethod
    def _default_registry() -> ChannelRegistry:
        registry = ChannelRegistry()
        for channel in Channel:
            registry.register(InProcessChannel(kind=channel))
        return registry

    # --------------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Create and start the scheduler (also performs recovery)."""
        if self._scheduler is not None:
            return
        self._scheduler = Scheduler(
            store=self.store,
            channels=self.channels,
            config=self.config,
            clock=self.clock,
            **self._scheduler_args,
        )

    def stop(self) -> None:
        if self._scheduler is not None:
            self._scheduler.stop()
            self._scheduler = None

    @property
    def scheduler(self) -> Scheduler:
        if self._scheduler is None:
            raise RuntimeError("service is stopped; call start() first")
        return self._scheduler

    # ----------------------------------------------------------- subscriptions
    def set_subscription(self, subscription: Subscription) -> None:
        self.subscriptions.put(subscription)

    def enable_channel(self, user_id: str, channel: Channel) -> None:
        self.subscriptions.enable(user_id, channel)

    def disable_channel(self, user_id: str, channel: Channel) -> None:
        self.subscriptions.disable(user_id, channel)

    # ------------------------------------------------------------------ trigger
    def trigger(self, event: Event) -> TriggerResult:
        """Route an event and schedule its per-channel notifications.

        Returns existing notifications on duplicate ``event_id`` (with
        ``duplicate=True``) instead of creating new ones.
        """
        trace: RoutingTrace = self.router.route(event, self.clock.now)
        result = TriggerResult(
            event_id=event.event_id,
            skipped_disabled=list(trace.skipped_disabled),
            skipped_unsupported=list(trace.skipped_unsupported),
        )

        for notification in trace.planned:
            existing = self.idempotency.get(event.event_id, notification.channel)
            if existing is not None:
                stored = self.store.get(existing)
                if stored is not None:
                    result.notifications.append(stored)
                    result.duplicate = True
                    continue
            inserted = self.store.insert_notification(notification)
            if not inserted:
                # Durable unique constraint caught a concurrent/restart duplicate.
                stored = self.store.get_by_event_channel(
                    event.event_id, notification.channel
                )
                if stored is not None:
                    self.idempotency.warm(
                        event.event_id, notification.channel, stored.id
                    )
                    result.notifications.append(stored)
                    result.duplicate = True
                continue
            self.idempotency.remember(
                event.event_id, notification.channel, notification.id
            )
            result.notifications.append(notification)
            self.scheduler.schedule(notification)
        return result

    # ------------------------------------------------------------------- queries
    def get_notification(self, notification_id: str) -> Notification | None:
        return self.store.get(notification_id)

    def get_event_notifications(self, event_id: str) -> list[Notification]:
        return self.store.list_by_event(event_id)

    def get_dead_letters(self, limit: int = 100) -> list[Notification]:
        return self.store.list_dead_letters(limit)

    def explain(self, notification_id: str) -> str:
        n = self.store.get(notification_id)
        if n is None:
            return f"notification {notification_id} not found"
        return n.explain()

    def wait_idle(self, timeout: float | None = None) -> bool:
        return self.scheduler.wait_idle(timeout=timeout)

    def run_due(self, timeout: float = 5.0) -> None:
        """Test helper: drain notifications due at the current clock time."""
        self.scheduler.run_due(timeout=timeout)

    # ------------------------------------------------------------- config hot-swap
    def update_config(self, **changes) -> DispatchConfig:
        """Update rate/concurrency; affects subsequent sends only."""
        return self.config.update(**changes)

    def replace_config(self, config: DispatchConfig) -> DispatchConfig:
        new = self.config.replace(config)
        self.scheduler.update_rates()
        return new

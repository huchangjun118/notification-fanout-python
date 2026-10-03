"""User-facing notification fan-out and scheduling toolkit.

Typical wiring::

    from notification_fanout import (
        NotificationService, NotificationStore, ChannelRegistry,
        InProcessChannel, DispatchConfig, AtomicConfig,
        RetryPolicy, QuietHours, FrequencyCap,
    )
"""

from .channels import (
    ChannelRegistry,
    InProcessChannel,
    LoopbackHttpChannel,
)
from .clock import Clock, FakeClock, SystemClock
from .config import (
    AtomicConfig,
    ChannelRateConfig,
    DispatchConfig,
    TokenBucket,
)
from .idempotency import IdempotencyCache
from .models import (
    Channel,
    ErrorKind,
    Event,
    ExceedPolicy,
    Notification,
    NotificationStatus,
    StateChange,
    Subscription,
    Urgency,
)
from .policy import FrequencyCap, QuietHours, RetryPolicy
from .router import Router, SubscriptionBook
from .scheduler import Scheduler
from .service import NotificationService, TriggerResult
from .store import NotificationStore

__all__ = [
    "Channel",
    "ChannelRateConfig",
    "ChannelRegistry",
    "Clock",
    "DispatchConfig",
    "ErrorKind",
    "Event",
    "ExceedPolicy",
    "FakeClock",
    "FrequencyCap",
    "IdempotencyCache",
    "InProcessChannel",
    "LoopbackHttpChannel",
    "Notification",
    "NotificationService",
    "NotificationStatus",
    "NotificationStore",
    "AtomicConfig",
    "QuietHours",
    "RetryPolicy",
    "Router",
    "Scheduler",
    "StateChange",
    "Subscription",
    "SubscriptionBook",
    "SystemClock",
    "TokenBucket",
    "TriggerResult",
]
__version__ = "0.1.0"

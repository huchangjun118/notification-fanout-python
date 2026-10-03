"""Dispatch rate / concurrency configuration.

* :class:`DispatchConfig` is an immutable snapshot: ``rate_per_second`` is the
  *channel-wide* cap across all users (``0`` disables rate limiting), and
  ``max_concurrency`` bounds total in-flight deliveries across all channels.
* Per-user-per-channel *serialization* is enforced by the scheduler's keyed
  queues; this config bounds the remaining two dimensions.
* :class:`AtomicConfig` hands out consistent snapshots, so a config change
  applies only to sends that acquire capacity afterwards -- never to an
  in-flight delivery.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, replace

from .models import Channel


@dataclass(frozen=True)
class ChannelRateConfig:
    rate_per_second: float = 0.0  # 0 == unlimited
    burst: int = 1                 # token bucket burst allowance


@dataclass(frozen=True)
class DispatchConfig:
    """Immutable dispatch configuration snapshot."""

    max_concurrency: int = 8
    default_rate_per_second: float = 0.0
    default_burst: int = 1
    channels: dict[Channel, ChannelRateConfig] = field(default_factory=dict)

    def channel_rate(self, channel: Channel) -> ChannelRateConfig:
        override = self.channels.get(channel)
        if override is not None:
            return override
        return ChannelRateConfig(
            rate_per_second=self.default_rate_per_second,
            burst=max(1, self.default_burst),
        )


class AtomicConfig:
    """Holds the current :class:`DispatchConfig` and swaps it atomically."""

    def __init__(self, config: DispatchConfig | None = None) -> None:
        self._config = config or DispatchConfig()
        self._lock = threading.RLock()
        self._listeners: list = []

    def snapshot(self) -> DispatchConfig:
        with self._lock:
            return self._config

    def update(self, **changes) -> DispatchConfig:
        """Apply top-level changes; future capacity acquisitions see them."""
        with self._lock:
            new = replace(self._config, **changes)
            self._config = new
            for listener in list(self._listeners):
                listener(new)
        return new

    def replace(self, config: DispatchConfig) -> DispatchConfig:
        with self._lock:
            self._config = config
            for listener in list(self._listeners):
                listener(config)
        return config

    def on_change(self, listener) -> None:
        self._listeners.append(listener)


class TokenBucket:
    """A thread-safe token bucket driven by an injectable monotonic clock.

    Rates are mutable at runtime: lowering ``rate_per_second`` only affects
    subsequent :meth:`acquire` calls (already-granted sends proceed).
    """

    def __init__(self, rate_per_second: float, burst: int, clock) -> None:
        self.rate_per_second = max(0.0, rate_per_second)
        self.burst = max(1, burst)
        self._clock = clock
        self._tokens = float(self.burst)
        self._updated = clock.monotonic()
        self._lock = threading.Lock()

    def configure(self, rate_per_second: float, burst: int) -> None:
        with self._lock:
            self._refill_locked()
            self.rate_per_second = max(0.0, rate_per_second)
            self.burst = max(1, burst)
            self._tokens = min(self._tokens, float(self.burst))

    def _refill_locked(self) -> None:
        now = self._clock.monotonic()
        elapsed = max(0.0, now - self._updated)
        if self.rate_per_second > 0:
            self._tokens = min(
                float(self.burst),
                self._tokens + elapsed * self.rate_per_second,
            )
        else:
            self._tokens = float(self.burst)
        self._updated = now

    def try_acquire(self) -> bool:
        with self._lock:
            self._refill_locked()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def time_until_token(self) -> float:
        """Seconds until one token is available (0 if it is available now)."""
        with self._lock:
            self._refill_locked()
            if self._tokens >= 1.0 or self.rate_per_second <= 0:
                return 0.0
            return (1.0 - self._tokens) / self.rate_per_second

    def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate_per_second
            self._clock.sleep(max(0.0, wait))

"""Per-key serialization and rate limiting.

Keys are (user_id, channel): the same user on the same channel is serialized
and rate limited, while different keys proceed in parallel.
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable


class KeyedLocks:
    """Non-blocking per-key locks so a busy key never blocks the scheduler loop."""

    def __init__(self) -> None:
        self._locks: dict[tuple[str, str], threading.Lock] = {}
        self._guard = threading.Lock()

    def try_lock(self, key: tuple[str, str]) -> bool:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
        return lock.acquire(blocking=False)

    def unlock(self, key: tuple[str, str]) -> None:
        with self._guard:
            lock = self._locks.get(key)
            if lock is not None:
                lock.release()


class TokenBucket:
    """Token bucket rate limiter with burst capacity."""

    def __init__(self, rate_per_second: float, burst: int) -> None:
        self.rate = rate_per_second
        self.burst = float(burst)
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def try_acquire(self, rate_per_second: float | None = None) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            if rate_per_second is not None:
                self.rate = rate_per_second
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class KeyedRateLimiter:
    """Per-key token bucket. ``rate_provider`` lets config changes apply to
    subsequent acquires without rebuilding the limiter."""

    def __init__(self, rate_provider: Callable[[], tuple[float, int]]) -> None:
        self._rate_provider = rate_provider
        self._buckets: dict[tuple[str, str], TokenBucket] = {}
        self._guard = threading.Lock()

    def try_acquire(self, key: tuple[str, str]) -> bool:
        rate, burst = self._rate_provider()
        with self._guard:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(rate, burst)
                self._buckets[key] = bucket
        return bucket.try_acquire(rate)

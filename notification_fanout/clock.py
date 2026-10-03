"""Injectable time source.

Production code uses :class:`SystemClock`; tests use :class:`FakeClock` to
advance virtual time deterministically (including scheduled delivery, quiet
hours and retry backoff).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Protocol

from .models import utc_now


class Clock(Protocol):
    """Minimal time + sleeping interface used by the scheduler."""

    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...

    def monotonic(self) -> float: ...


class SystemClock:
    """Real wall clock."""

    def now(self) -> datetime:
        return utc_now()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)

    def monotonic(self) -> float:
        return time.monotonic()


class FakeClock:
    """Deterministic virtual clock.

    :meth:`sleep` advances virtual time instantly instead of blocking, so
    tests can drive timers without real waiting.
    """

    def __init__(self, start: datetime | None = None) -> None:
        if start is not None and start.tzinfo is None:
            raise ValueError("FakeClock start must be timezone-aware")
        self._now = start or utc_now()
        self._mono = 0.0

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._mono

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self.advance(timedelta(seconds=seconds))

    def advance(self, delta: timedelta) -> datetime:
        """Move the clock forward (or, rarely, backward)."""
        self._now = self._now + delta
        self._mono += max(0.0, delta.total_seconds())
        return self._now

    def set(self, value: datetime) -> None:
        """Pin the clock to an absolute time."""
        if value.tzinfo is None:
            raise ValueError("time must be timezone-aware")
        self._mono += max(0.0, (value - self._now).total_seconds())
        self._now = value

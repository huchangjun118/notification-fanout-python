"""Scheduling policies: quiet hours, frequency caps, retry backoff.

These types are pure data + pure evaluation; the scheduler owns *when* to call
them and how to record the outcome, so policies remain trivially testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from .models import ExceedPolicy


@dataclass(frozen=True)
class QuietHours:
    """A recurring daily quiet window.

    Times are wall-clock times in ``tzname`` (IANA name, defaults to UTC).
    ``start >= end`` denotes an overnight window (e.g. 22:00-08:00); equal
    endpoints mean "never quiet".
    """

    start: time
    end: time
    tzname: str = "UTC"

    def __post_init__(self) -> None:
        # Validate tz name eagerly.
        ZoneInfo(self.tzname)

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.tzname)

    def _local(self, at: datetime) -> datetime:
        return at.astimezone(self.tz)

    def covers(self, at: datetime) -> bool:
        """Whether ``at`` falls inside the quiet window."""
        if self.start == self.end:
            return False
        local_t = self._local(at).timetz().replace(tzinfo=None)
        if self.start < self.end:
            return self.start <= local_t < self.end
        # overnight: e.g. 22:00 -> 08:00
        return local_t >= self.start or local_t < self.end

    def next_opening(self, at: datetime) -> datetime:
        """Earliest instant at/after ``at`` when the window ends."""
        local_dt = self._local(at)
        if not self.covers(at):
            return at

        def opening_on(date) -> datetime:
            naive = datetime.combine(date, self.end)
            return naive.replace(tzinfo=self.tz)

        candidate = opening_on(local_dt.date())
        if candidate <= local_dt:
            candidate = opening_on(local_dt.date() + timedelta(days=1))
        return candidate.astimezone().astimezone(at.tzinfo)


@dataclass(frozen=True)
class FrequencyCap:
    """Per-user, per-channel send cap inside a rolling time window.

    Only *actual channel invocations* count against the cap (the store records
    them at send time), so deferred/unsent notifications do not consume quota.
    """

    max_sends: int
    window: timedelta
    exceed_policy: ExceedPolicy = ExceedPolicy.DEFER

    def window_start(self, now: datetime) -> datetime:
        return now - self.window


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff for retryable failures.

    ``max_attempts`` counts the first attempt, so ``max_attempts=1`` means no
    retry. Delay for attempt ``n`` (1-based, the attempt that just failed) is::

        min(max_delay, base_delay * multiplier ** (n - 1))
    """

    max_attempts: int = 5
    base_delay: timedelta = timedelta(seconds=1)
    multiplier: float = 2.0
    max_delay: timedelta = timedelta(minutes=5)

    def backoff_delay(self, failed_attempt: int) -> timedelta:
        raw = self.base_delay.total_seconds() * (
            self.multiplier ** max(0, failed_attempt - 1)
        )
        return timedelta(seconds=min(self.max_delay.total_seconds(), raw))

    def should_retry(self, attempts_done: int) -> bool:
        return attempts_done < self.max_attempts


def apply_quiet_hours(
    not_before: datetime, quiet: QuietHours | None, urgent: bool
) -> tuple[datetime, str]:
    """Possibly defer a scheduled time past quiet hours.

    Returns ``(possibly_adjusted_not_before, reason)``. Urgent notifications
    bypass quiet hours; empty reason means "no adjustment".
    """
    if quiet is None or urgent or not quiet.covers(not_before):
        return not_before, ""
    opening = quiet.next_opening(not_before)
    return (
        opening,
        f"deferred into quiet hours {quiet.start.isoformat()}-"
        f"{quiet.end.isoformat()} ({quiet.tzname}); resumes at "
        f"{opening.isoformat()}",
    )

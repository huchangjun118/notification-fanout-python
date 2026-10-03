"""Delivery policy: quiet hours and frequency caps.

Policy is evaluated at dispatch time (not at ingest), so configuration changes
only affect subsequent sends. Decisions are explainable via a reason string.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum

from .config import Config
from .models import Notification, UserPreferences


class PolicyAction(str, Enum):
    SEND = "send"
    DEFER_QUIET = "defer_quiet"
    DEFER_FREQUENCY = "defer_frequency"
    DROP_FREQUENCY = "drop_frequency"


@dataclass
class PolicyDecision:
    action: PolicyAction
    reason: str
    next_at: float | None = None


def _local_hour(ts: float) -> int:
    return time.localtime(ts).tm_hour


def in_quiet_window(ts: float, start_hour: int, end_hour: int) -> bool:
    """Whether ``ts`` falls inside the quiet window [start_hour, end_hour).

    A window that wraps midnight (e.g. 22 -> 8) is supported.
    """
    hour = _local_hour(ts)
    if start_hour == end_hour:
        return False
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    # wraps midnight
    return hour >= start_hour or hour < end_hour


def next_quiet_end(ts: float, end_hour: int) -> float:
    """Epoch timestamp of the next ``end_hour`` (today or tomorrow), always > ts."""
    struct = time.localtime(ts)
    candidate_hour = end_hour
    candidate = time.mktime(
        (struct.tm_year, struct.tm_mon, struct.tm_mday, candidate_hour, 0, 0, 0, 0, -1)
    )
    if candidate <= ts:
        candidate += 24 * 3600
    return candidate


def evaluate(
    notification: Notification,
    prefs: UserPreferences | None,
    config: Config,
    *,
    sent_in_window: int,
    oldest_sent_ts: float | None,
    now: float,
) -> PolicyDecision:
    """Decide whether a due notification may send, or must defer/drop.

    Order: quiet hours first (non-urgent only), then the frequency cap.
    """
    start = (
        prefs.quiet_start_hour
        if prefs is not None and prefs.quiet_start_hour is not None
        else config.quiet_start_hour
    )
    end = (
        prefs.quiet_end_hour
        if prefs is not None and prefs.quiet_end_hour is not None
        else config.quiet_end_hour
    )

    if not notification.urgent and in_quiet_window(now, start, end):
        until = next_quiet_end(now, end)
        return PolicyDecision(
            PolicyAction.DEFER_QUIET,
            f"non-urgent notification deferred until quiet window ends at hour {end}",
            until,
        )

    if sent_in_window >= config.freq_cap:
        if config.freq_strategy == "drop":
            return PolicyDecision(
                PolicyAction.DROP_FREQUENCY,
                f"frequency cap exceeded: {sent_in_window} sends in window "
                f"(cap {config.freq_cap}); dropped per policy",
            )
        if oldest_sent_ts is not None:
            next_at = oldest_sent_ts + config.freq_window_seconds
        else:
            next_at = now + config.freq_window_seconds
        return PolicyDecision(
            PolicyAction.DEFER_FREQUENCY,
            f"frequency cap exceeded: {sent_in_window} sends in window "
            f"(cap {config.freq_cap}); deferred to next allowed slot",
            next_at,
        )

    return PolicyDecision(PolicyAction.SEND, "policy allow")

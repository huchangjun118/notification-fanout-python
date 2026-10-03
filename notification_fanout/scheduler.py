"""Scheduling and fan-out delivery engine.

Concurrency model (slow channels cannot block fast ones):

* one **isolated thread pool per channel** -- a backed-up email channel can
  never consume threads needed by push/sms/inbox;
* per ``(user_id, channel)`` a FIFO keyed queue with a single pump in that
  channel's pool, so one user's sends on a channel are serial while different
  users/channels proceed in parallel;
* a global, runtime-adjustable semaphore caps total in-flight deliveries;
* per-channel token buckets cap channel send rate; config swaps affect only
  subsequent capacity acquisitions.

Timing model: one daemon scheduler thread pops due notifications from a
min-heap; sleeping is interruptible via a condition variable. Tests inject a
:class:`FakeClock`, advance virtual time and call :meth:`Scheduler.run_due`
to drain everything that is due deterministically (deferred items whose
``not_before`` is still in the virtual future are never forced early).
"""

from __future__ import annotations

import heapq
import threading
import time
import traceback
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from .channels import ChannelRegistry
from .clock import Clock, FakeClock, SystemClock
from .config import AtomicConfig, DispatchConfig, TokenBucket
from .models import (
    Channel,
    DeliveryResult,
    ErrorKind,
    ExceedPolicy,
    Notification,
    NotificationStatus,
    StateChange,
    Urgency,
)
from .policy import FrequencyCap, QuietHours, RetryPolicy, apply_quiet_hours
from .store import NotificationStore

# Per-channel pool ceiling (generous; the global semaphore does real capping).
_CHANNEL_WORKERS = 64
# Real-clock polling granularity.
_TICK = 0.05
# How long run_due busy-waits for a queued pump to start between checks.
_DRAIN_STEP = 0.01


class _AdjustableSemaphore:
    """Global concurrency gate whose limit can change at runtime.

    Raising the limit wakes waiters immediately; lowering it never cancels
    in-flight work -- only subsequent acquisitions are constrained.
    """

    def __init__(self, limit: int) -> None:
        self._limit = max(1, limit)
        self._in_flight = 0
        self._cond = threading.Condition()

    def acquire(self) -> None:
        with self._cond:
            while self._in_flight >= self._limit:
                self._cond.wait()
            self._in_flight += 1

    def release(self) -> None:
        with self._cond:
            self._in_flight -= 1
            self._cond.notify_all()

    def set_limit(self, limit: int) -> None:
        with self._cond:
            self._limit = max(1, limit)
            self._cond.notify_all()


@dataclass(order=True)
class _HeapEntry:
    not_before: datetime
    seq: int
    notification_id: str = field(compare=False)


class _KeyedRunner:
    """Serial execution of tasks sharing a string key, on one executor."""

    def __init__(self, executor: ThreadPoolExecutor) -> None:
        self._executor = executor
        self._queues: dict[str, deque[Callable[[], None]]] = defaultdict(deque)
        self._running: dict[str, bool] = {}
        self._lock = threading.Lock()

    def submit(self, key: str, task: Callable[[], None]) -> None:
        kick = False
        with self._lock:
            self._queues[key].append(task)
            if not self._running.get(key):
                self._running[key] = True
                kick = True
        if kick:
            self._executor.submit(self._pump, key)

    def _pump(self, key: str) -> None:
        while True:
            with self._lock:
                queue = self._queues[key]
                task = queue.popleft() if queue else None
                if task is None:
                    self._running[key] = False
                    return
            try:
                task()
            except Exception:
                traceback.print_exc()

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=False)


class Scheduler:
    """Owns due-timing, policy evaluation and delivery execution."""

    def __init__(
        self,
        store: NotificationStore,
        channels: ChannelRegistry,
        config: AtomicConfig | None = None,
        clock: Clock | None = None,
        retry_policy: RetryPolicy | None = None,
        quiet_hours: QuietHours | None = None,
        frequency_caps: dict[Channel, FrequencyCap] | None = None,
        recover: bool = True,
    ) -> None:
        self.store = store
        self.channels = channels
        self.config = config or AtomicConfig(DispatchConfig())
        self.clock = clock or SystemClock()
        self.retry_policy = retry_policy or RetryPolicy()
        self.quiet_hours = quiet_hours
        self.frequency_caps = frequency_caps or {}

        self._heap: list[_HeapEntry] = []
        self._seq = 0
        self._cond = threading.Condition()
        self._inflight = 0
        self._idle = threading.Condition()
        self._stopping = False
        self._sem = _AdjustableSemaphore(self.config.snapshot().max_concurrency)
        self.config.on_change(
            lambda cfg: self._sem.set_limit(cfg.max_concurrency)
        )
        self.config.on_change(lambda _cfg: self.update_rates())
        self._runners: dict[Channel, _KeyedRunner] = {}
        self._buckets: dict[Channel, TokenBucket] = {}
        self._setup_pools_and_buckets()

        if recover:
            self.recover()
        self._thread = threading.Thread(
            target=self._loop, name="notification-scheduler", daemon=True
        )
        self._thread.start()

    # ------------------------------------------------------------ infrastructure
    def _setup_pools_and_buckets(self) -> None:
        cfg = self.config.snapshot()
        for channel in Channel:
            if not self.channels.has(channel):
                continue
            executor = ThreadPoolExecutor(
                max_workers=_CHANNEL_WORKERS,
                thread_name_prefix=f"channel-{channel.value}",
            )
            self._runners[channel] = _KeyedRunner(executor)
            rate = cfg.channel_rate(channel)
            self._buckets[channel] = TokenBucket(
                rate.rate_per_second, rate.burst, self.clock
            )

    def update_rates(self) -> None:
        """Re-apply the current config snapshot to all token buckets."""
        cfg = self.config.snapshot()
        for channel, bucket in self._buckets.items():
            rate = cfg.channel_rate(channel)
            bucket.configure(rate.rate_per_second, rate.burst)

    # ------------------------------------------------------------- enqueue / GC
    def schedule(self, notification: Notification) -> None:
        """Add a persisted notification to the timing heap."""
        with self._cond:
            heapq.heappush(
                self._heap,
                _HeapEntry(notification.not_before, self._next_seq(), notification.id),
            )
            self._cond.notify_all()

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def recover(self) -> None:
        """Reload non-terminal notifications after a process restart.

        ``DELIVERING`` rows come from a process that died mid-call: the channel
        ack is unknown, so they are marked ``RETRYING`` and re-queued
        (at-least-once; the ``(event_id, channel)`` idempotency still prevents
        duplicate *new* triggers). Terminal rows are never replayed, and every
        due/future row is re-armed, so nothing is lost.
        """
        now = self.clock.now()
        for n in self.store.list_non_terminal():
            if n.status is NotificationStatus.DELIVERING:
                n.status = NotificationStatus.RETRYING
                n.updated_at = now
                n.history.append(
                    StateChange(
                        status=NotificationStatus.RETRYING,
                        at=now,
                        reason="recovered from interrupted delivery on restart",
                        attempt=n.attempts,
                    )
                )
                self.store.compare_and_update(
                    n, expected=[NotificationStatus.DELIVERING]
                )
            self.schedule(n)

    # ------------------------------------------------------------------ main loop
    def stop(self) -> None:
        with self._cond:
            self._stopping = True
            self._cond.notify_all()
        self._thread.join(timeout=2)
        for runner in self._runners.values():
            runner.shutdown()

    def _loop(self) -> None:
        virtual = isinstance(self.clock, FakeClock)
        while True:
            with self._cond:
                while not self._stopping and not self._heap:
                    self._cond.wait(timeout=None if virtual else _TICK)
                if self._stopping:
                    return
                entry = self._heap[0]
                now = self.clock.now()
                wait = (entry.not_before - now).total_seconds()
                if wait > 0:
                    self._cond.wait(
                        timeout=None if virtual else min(_TICK, wait)
                    )
                    continue
                heapq.heappop(self._heap)
            self._dispatch(entry.notification_id)

    def wakeup(self) -> None:
        """Interrupt the timer sleep (call after advancing a FakeClock)."""
        with self._cond:
            self._cond.notify_all()

    def run_due(self, timeout: float = 5.0) -> None:
        """Drain every notification due at the current (virtual) clock time.

        Items deferred beyond "now" stay in the heap and are not sent early.
        Blocks until the heap has no due item *and* nothing is in flight
        (including tasks sitting in per-user queues). The deadline is measured
        in *real* wall-clock time so it works regardless of clock injection.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.wakeup()
            with self._cond:
                due = bool(self._heap) and self._heap[0].not_before <= self.clock.now()
            with self._idle:
                busy = self._inflight > 0
            if not due and not busy:
                return
            # Yield real wall-clock time to the scheduler and channel threads.
            time.sleep(_DRAIN_STEP)
        raise TimeoutError("run_due timed out with pending work")

    def wait_idle(self, timeout: float | None = None) -> bool:
        """Block until nothing is due and nothing is in flight.

        Uses a two-consecutive-quiet-probe guard so a notification that has
        just been pushed to the heap (not yet popped by the scheduler thread)
        cannot make the caller observe a spurious idle state.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        quiet_probes = 0
        with self._idle:
            while True:
                with self._cond:
                    due = bool(self._heap) and self._heap[0].not_before <= self.clock.now()
                busy = self._inflight > 0
                if not due and not busy:
                    quiet_probes += 1
                    if quiet_probes >= 2:
                        return True
                else:
                    quiet_probes = 0
                if deadline is not None and time.monotonic() >= deadline:
                    return False
                self._idle.wait(timeout=_TICK)

    def _enter_inflight(self) -> None:
        with self._idle:
            self._inflight += 1

    def _exit_inflight(self) -> None:
        with self._idle:
            self._inflight -= 1
            if self._inflight == 0:
                self._idle.notify_all()

    # ------------------------------------------------------------------ dispatch
    def _dispatch(self, notification_id: str) -> None:
        # The in-flight counter is held across the hand-off to the keyed
        # runner, so run_due/wait_idle cannot observe an idle gap while a task
        # is merely queued in a per-user queue.
        self._enter_inflight()
        handed_off = False
        try:
            n = self.store.get(notification_id)
            if n is None or n.is_terminal:
                return
            if n.not_before > self.clock.now():
                self.schedule(n)  # re-armed early (e.g. retry race)
                return

            # Isolated per-channel pool; serial per (user, channel). Policy
            # checks happen inside the serial pump (_deliver), so the "check
            # cap -> record attempt" sequence for one user+channel is atomic.
            runner = self._runners.get(n.channel)
            if runner is None:
                self._dead_letter(
                    n, "configuration: no channel implementation registered"
                )
                return
            runner.submit(f"{n.user_id}:{n.channel.value}", lambda: self._deliver(n.id))
            handed_off = True
        finally:
            if not handed_off:
                self._exit_inflight()

    # ------------------------------------------------------------------ delivery
    def _deliver(self, notification_id: str) -> None:
        # Owns the in-flight count acquired in _dispatch; decrements exactly
        # once when the attempt (and any rescheduling) is settled.
        try:
            n = self.store.get(notification_id)
            if n is None or n.is_terminal:
                return
            now = self.clock.now()
            urgent = n.urgency is Urgency.URGENT

            # 1) quiet hours (urgent bypasses). Evaluated inside the serial
            # pump so a user's notifications queue behind the deferral.
            adjusted, quiet_reason = apply_quiet_hours(
                max(n.not_before, now), self.quiet_hours, urgent
            )
            if quiet_reason:
                n.not_before = adjusted
                n.updated_at = now
                n.history.append(
                    StateChange(
                        NotificationStatus.PENDING, now, quiet_reason, n.attempts
                    )
                )
                if self.store.compare_and_update(n):
                    self.schedule(n)
                return

            # 2) frequency cap (urgent bypasses). Only actual channel
            # invocations count; the matching record_attempt below is what
            # consumes a slot, and serialization makes this atomic per key.
            cap = self.frequency_caps.get(n.channel)
            if cap is not None and not urgent:
                window_start = cap.window_start(now)
                used = self.store.count_attempts_since(
                    n.user_id, n.channel, window_start
                )
                if used >= cap.max_sends:
                    if cap.exceed_policy is ExceedPolicy.DROP:
                        n.status = NotificationStatus.DROPPED
                        n.updated_at = now
                        n.history.append(
                            StateChange(
                                NotificationStatus.DROPPED,
                                now,
                                f"frequency cap {cap.max_sends}/"
                                f"{int(cap.window.total_seconds())}s exceeded; "
                                "policy=drop",
                                n.attempts,
                            )
                        )
                        self.store.compare_and_update(n)
                        return
                    oldest = self.store.oldest_attempt_at(
                        n.user_id, n.channel, window_start
                    )
                    slot = (oldest or now) + cap.window
                    slot, _ = apply_quiet_hours(slot, self.quiet_hours, False)
                    n.not_before = slot
                    n.updated_at = now
                    n.history.append(
                        StateChange(
                            NotificationStatus.PENDING,
                            now,
                            f"frequency cap {cap.max_sends}/"
                            f"{int(cap.window.total_seconds())}s exceeded; "
                            f"deferred to {slot.isoformat()}",
                            n.attempts,
                        )
                    )
                    if self.store.compare_and_update(n):
                        self.schedule(n)
                    return

            # 3) claim: only one worker moves PENDING/RETRYING -> DELIVERING.
            n.status = NotificationStatus.DELIVERING
            n.attempts += 1
            n.updated_at = now
            n.history.append(
                StateChange(
                    NotificationStatus.DELIVERING,
                    now,
                    f"attempt {n.attempts} starting",
                    n.attempts,
                )
            )
            if not self.store.compare_and_update(
                n,
                expected=[
                    NotificationStatus.PENDING,
                    NotificationStatus.RETRYING,
                ],
            ):
                return  # a concurrent/duplicate dispatch already owns it

            # Channel-wide rate limit (token bucket). With a FakeClock, sleeps
            # advance virtual time instantly.
            bucket = self._buckets.get(n.channel)
            if bucket is not None:
                bucket.acquire()

            # Global cross-channel concurrency gate.
            self._sem.acquire()
            try:
                channel = self.channels.get(n.channel)
                try:
                    result = channel.deliver(n)
                except Exception as exc:
                    result = DeliveryResult(
                        success=False,
                        error_kind=ErrorKind.FATAL,
                        error_message=(
                            f"channel raised {type(exc).__name__}: {exc}"
                        ),
                    )
            finally:
                self._sem.release()

            finished_at = self.clock.now()
            self.store.record_attempt(
                n.id,
                n.attempts,
                finished_at,
                result.success,
                result.error_kind.value if result.error_kind else "",
                result.error_message,
            )

            if result.success:
                n.status = NotificationStatus.DELIVERED
                n.delivered_at = finished_at
                n.updated_at = finished_at
                n.history.append(
                    StateChange(
                        NotificationStatus.DELIVERED,
                        finished_at,
                        f"delivered on attempt {n.attempts}",
                        n.attempts,
                    )
                )
                self.store.compare_and_update(
                    n, expected=[NotificationStatus.DELIVERING]
                )
                return

            if result.error_kind is ErrorKind.FATAL:
                self._dead_letter(
                    n,
                    f"non-retryable failure: {result.error_message}",
                    expected=[NotificationStatus.DELIVERING],
                )
                return

            # Retryable failure: back off, respecting quiet hours.
            if self.retry_policy.should_retry(n.attempts):
                delay = self.retry_policy.backoff_delay(n.attempts)
                retry_at = finished_at + delay
                retry_at, quiet_reason = apply_quiet_hours(
                    retry_at,
                    self.quiet_hours,
                    n.urgency is Urgency.URGENT,
                )
                n.status = NotificationStatus.RETRYING
                n.not_before = retry_at
                n.updated_at = finished_at
                reason = (
                    f"retryable failure (attempt {n.attempts}): "
                    f"{result.error_message}; retry at {retry_at.isoformat()}"
                )
                if quiet_reason:
                    reason += f"; {quiet_reason}"
                n.history.append(
                    StateChange(
                        NotificationStatus.RETRYING,
                        finished_at,
                        reason,
                        n.attempts,
                    )
                )
                if self.store.compare_and_update(
                    n, expected=[NotificationStatus.DELIVERING]
                ):
                    self.schedule(n)
            else:
                self._dead_letter(
                    n,
                    f"retries exhausted after {n.attempts} attempts; "
                    f"last error: {result.error_message}",
                    expected=[NotificationStatus.DELIVERING],
                )
        finally:
            self._exit_inflight()

    # ------------------------------------------------------------------ terminal
    def _dead_letter(
        self,
        n: Notification,
        reason: str,
        expected: list[NotificationStatus] | None = None,
    ) -> None:
        now = self.clock.now()
        n.status = NotificationStatus.DEAD_LETTER
        n.dead_letter_reason = reason
        n.updated_at = now
        n.history.append(
            StateChange(
                NotificationStatus.DEAD_LETTER, now, reason, n.attempts
            )
        )
        self.store.compare_and_update(n, expected=expected)

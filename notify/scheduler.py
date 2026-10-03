"""Background dispatch scheduler.

Each tick: reset stale leases, fetch due notifications, apply policy, then
dispatch. Dispatch uses a bounded thread pool; per-key locks and token
buckets are acquired non-blockingly so a slow or busy channel is requeued
with a small delay instead of blocking other keys.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .channels import Channel
from .config import ConfigHolder
from .models import NonRetryableError, Notification, RetryableError
from .policy import PolicyAction, evaluate
from .ratelimit import KeyedLocks, KeyedRateLimiter
from .repository import Repository

logger = logging.getLogger("notify.scheduler")

# Small requeue delay when a key is busy (prior send in flight / rate limited).
_BUSY_DELAY = 0.4
_BUSY_JITTER = 0.8


class Scheduler:
    def __init__(
        self,
        repo: Repository,
        channels: dict[str, Channel],
        config_holder: ConfigHolder,
    ) -> None:
        self.repo = repo
        self.channels = channels
        self.config_holder = config_holder
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._locks = KeyedLocks()
        self._limiter = KeyedRateLimiter(self._rate_provider)

    def _rate_provider(self) -> tuple[float, int]:
        cfg = self.config_holder.get()
        return cfg.rate_per_second, cfg.rate_burst

    def start(self) -> None:
        cfg = self.config_holder.get()
        self._executor = ThreadPoolExecutor(
            max_workers=cfg.dispatch_workers, thread_name_prefix="dispatch"
        )
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)
        self._thread.start()
        logger.info("scheduler started (workers=%d)", cfg.dispatch_workers)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
        logger.info("scheduler stopped")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("scheduler tick failed")
            self._stop.wait(self.config_holder.get().tick_interval)

    def _tick(self) -> None:
        cfg = self.config_holder.get()
        now = time.time()
        self.repo.reset_stale_leases(now, cfg.lease_timeout)
        due = self.repo.fetch_due(now, cfg.batch_size)
        for notification in due:
            try:
                self._consider(notification, now)
            except Exception:
                logger.exception("failed to consider notification %s", notification.id)

    def _consider(self, notification: Notification, now: float) -> None:
        cfg = self.config_holder.get()
        prefs = self.repo.get_preferences(notification.user_id)
        sent_in_window = self.repo.count_sent_since(
            notification.user_id,
            notification.channel,
            now - cfg.freq_window_seconds,
        )
        oldest = self.repo.latest_sent_ts(notification.user_id, notification.channel)
        decision = evaluate(
            notification,
            prefs,
            cfg,
            sent_in_window=sent_in_window,
            oldest_sent_ts=oldest,
            now=now,
        )
        if decision.action in (PolicyAction.DEFER_QUIET, PolicyAction.DEFER_FREQUENCY):
            self.repo.requeue_deferred(notification.id, decision.next_at, decision.reason)
            return
        if decision.action == PolicyAction.DROP_FREQUENCY:
            self.repo.mark_dropped(notification.id, decision.reason)
            return

        key = (notification.user_id, notification.channel)
        if not self._locks.try_lock(key):
            self._requeue_busy(notification, "waiting for prior send on same user/channel")
            return
        if not self._limiter.try_acquire(key):
            self._locks.unlock(key)
            self._requeue_busy(notification, "rate limiter busy for same user/channel")
            return

        if not self.repo.try_claim(notification.id, now):
            self._locks.unlock(key)
            return

        assert self._executor is not None
        self._executor.submit(self._dispatch, notification)

    def _requeue_busy(self, notification: Notification, reason: str) -> None:
        next_at = time.time() + _BUSY_DELAY + random.random() * _BUSY_JITTER
        self.repo.requeue_deferred(notification.id, next_at, reason)

    def _dispatch(self, notification: Notification) -> None:
        key = (notification.user_id, notification.channel)
        try:
            channel = self.channels.get(notification.channel)
            if channel is None:
                self.repo.mark_dead_letter(
                    notification.id, f"no channel registered for {notification.channel!r}"
                )
                return
            channel.send(notification)
            self.repo.mark_sent(notification.id)
        except NonRetryableError as exc:
            self.repo.mark_dead_letter(
                notification.id, f"non-retryable failure, not replayed: {exc}"
            )
        except RetryableError as exc:
            self._handle_retry(notification, str(exc))
        except Exception as exc:  # defensive: treat unknown errors as retryable
            self._handle_retry(notification, f"unexpected error: {exc}")
        finally:
            self._locks.unlock(key)

    def _handle_retry(self, notification: Notification, error: str) -> None:
        cfg = self.config_holder.get()
        fresh = self.repo.get_notification(notification.id)
        if fresh is None:
            return
        # fresh.attempts = attempts before this attempt; this was attempt number attempts+1
        if fresh.attempts + 1 >= fresh.max_attempts:
            self.repo.mark_dead_letter(
                notification.id,
                f"max attempts ({fresh.max_attempts}) exhausted; last error: {error}",
            )
            return
        delay = min(
            cfg.retry_base_delay * (2 ** fresh.attempts),
            cfg.retry_max_delay,
        )
        delay *= 0.5 + random.random()  # jitter in [0.5, 1.5)
        next_at = time.time() + delay
        self.repo.requeue_for_retry(notification.id, next_at, error, time.time())

"""Channel abstractions: in-process injectable channels and a loopback HTTP channel."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Protocol

from .models import NonRetryableError, Notification, RetryableError


class Channel(Protocol):
    """A delivery target. Implementations may be in-process or loopback HTTP."""

    name: str

    def send(self, notification: Notification) -> None:
        """Deliver the notification.

        Raises:
            RetryableError: transient failure, safe to retry with backoff.
            NonRetryableError: permanent failure, never replay.
        """
        ...


class InMemoryChannel:
    """In-process channel that records deliveries for tests.

    Failure behaviour is configurable: fail ``fail_n_times`` then succeed,
    fail forever, or fail with a non-retryable error. A ``delay`` simulates
    a slow downstream without blocking the whole service.
    """

    def __init__(
        self,
        name: str,
        *,
        fail_n_times: int = 0,
        fail_forever: bool = False,
        non_retryable: bool = False,
        error_message: str = "simulated channel failure",
        delay: float = 0.0,
    ) -> None:
        self.name = name
        self.fail_n_times = fail_n_times
        self.fail_forever = fail_forever
        self.non_retryable = non_retryable
        self.error_message = error_message
        self.delay = delay
        self.sent: list[dict[str, Any]] = []
        self.attempts = 0

    def send(self, notification: Notification) -> None:
        if self.delay:
            time.sleep(self.delay)
        self.attempts += 1
        if self.fail_forever or self.attempts <= self.fail_n_times:
            if self.non_retryable:
                raise NonRetryableError(f"{self.name}: {self.error_message}")
            raise RetryableError(f"{self.name}: {self.error_message}")
        self.sent.append(
            {
                "notification_id": notification.id,
                "event_id": notification.event_id,
                "user_id": notification.user_id,
                "channel": notification.channel,
                "payload": notification.payload,
                "attempt": self.attempts,
                "ts": time.time(),
            }
        )

    def reset(self) -> None:
        self.sent.clear()
        self.attempts = 0


class LoopbackHttpChannel:
    """Channel that POSTs JSON to a loopback HTTP endpoint (stdlib only)."""

    def __init__(self, name: str, base_url: str, timeout: float = 5.0) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def send(self, notification: Notification) -> None:
        body = json.dumps(
            {
                "notification_id": notification.id,
                "event_id": notification.event_id,
                "user_id": notification.user_id,
                "channel": notification.channel,
                "payload": notification.payload,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/send",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                status = resp.status
        except urllib.error.HTTPError as exc:
            status = exc.code
        except urllib.error.URLError as exc:
            raise RetryableError(f"{self.name}: connection error: {exc.reason}") from exc
        except TimeoutError as exc:
            raise RetryableError(f"{self.name}: timeout") from exc

        if 200 <= status < 300:
            return
        if status == 429 or 500 <= status < 600:
            raise RetryableError(f"{self.name}: downstream returned {status}")
        raise NonRetryableError(f"{self.name}: downstream returned {status}")

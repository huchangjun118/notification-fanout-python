"""Delivery channels.

Two channel families are provided, both dependency-free and hitting nothing
external:

* :class:`InProcessChannel` -- calls an injectable Python callable (or scripted
  behaviour); the default target for tests and embedded use.
* :class:`LoopbackHttpChannel` -- POSTs via stdlib ``urllib`` to a loopback
  address (e.g. a local FastAPI/uvicorn sink), simulating a real network
  channel without external credentials.

A channel returns :class:`DeliveryResult`; raising is treated as a fatal
configuration error, while :meth:`DeliveryResult.success` ``False`` with
:attr:`ErrorKind.RETRYABLE` drives the retry policy.
"""

from __future__ import annotations

import json
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from urllib import error as urllib_error
from urllib import request as urllib_request

from .models import Channel, DeliveryResult, ErrorKind, Notification


class DeliveryHandler(Protocol):
    """Signature of an injectable in-process send function."""

    def __call__(self, notification: Notification) -> DeliveryResult | None: ...


@dataclass
class InProcessChannel:
    """Channel that delegates delivery to an in-process callable.

    * ``handler`` receives the notification and returns ``None``/``True`` on
      success, a :class:`DeliveryResult`, or raises ``RuntimeError`` for a
      scripted failure.
    * Every attempt is recorded in :attr:`deliveries` (thread-safe) so tests can
      assert *who actually got sent and how many times*.
    * Optional scripted failure queue lets tests pre-program
      retryable/fatal responses per attempt.
    """

    kind: Channel
    handler: Callable[[Notification], DeliveryResult | None] | None = None
    deliveries: list[Notification] = field(default_factory=list)
    scripted: deque[DeliveryResult] = field(default_factory=deque)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def script_failure(
        self,
        error_kind: ErrorKind = ErrorKind.RETRYABLE,
        message: str = "scripted failure",
    ) -> None:
        """Queue one failure result to be returned on the next attempt."""
        self.scripted.append(
            DeliveryResult(
                success=False, error_kind=error_kind, error_message=message
            )
        )

    def deliver(self, notification: Notification) -> DeliveryResult:
        try:
            if self.scripted:
                result = self.scripted.popleft()
            elif self.handler is not None:
                outcome = self.handler(notification)
                result = (
                    outcome
                    if isinstance(outcome, DeliveryResult)
                    else DeliveryResult(success=True)
                )
            else:
                result = DeliveryResult(success=True)
        except Exception as exc:  # handler misbehaves -> fatal, not an infinite loop
            result = DeliveryResult(
                success=False,
                error_kind=ErrorKind.FATAL,
                error_message=f"handler raised {type(exc).__name__}: {exc}",
            )
        with self._lock:
            self.deliveries.append(notification)
        return result


@dataclass
class LoopbackHttpChannel:
    """Channel that POSTs each notification as JSON over loopback HTTP.

    Only 127.0.0.1 / localhost targets are accepted (no external dependencies).
    Connection errors / HTTP 5xx / HTTP 429 are retryable; other 4xx are fatal.
    """

    kind: Channel
    url: str
    timeout: float = 2.0
    deliveries: list[Notification] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self) -> None:
        host = self.url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
        if host not in ("127.0.0.1", "localhost", "::1", "[::1]"):
            raise ValueError("LoopbackHttpChannel only accepts loopback targets")

    def deliver(self, notification: Notification) -> DeliveryResult:
        body = json.dumps(
            {
                "notification_id": notification.id,
                "event_id": notification.event_id,
                "user_id": notification.user_id,
                "channel": notification.channel.value,
                "urgency": notification.urgency.value,
                "title": notification.payload_title,
                "body": notification.payload_body,
                "attempt": notification.attempts,
                "sent_at": datetime.now().astimezone().isoformat(),
                "metadata": notification.metadata,
            }
        ).encode("utf-8")
        req = urllib_request.Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=self.timeout) as resp:
                ok = 200 <= resp.status < 300
        except urllib_error.HTTPError as exc:
            with self._lock:
                self.deliveries.append(notification)
            if exc.code == 429 or 500 <= exc.code < 600:
                kind = ErrorKind.RETRYABLE
            else:
                kind = ErrorKind.FATAL
            return DeliveryResult(
                success=False,
                error_kind=kind,
                error_message=f"HTTP {exc.code} {exc.reason}",
            )
        except (urllib_error.URLError, TimeoutError, OSError) as exc:
            with self._lock:
                self.deliveries.append(notification)
            return DeliveryResult(
                success=False,
                error_kind=ErrorKind.RETRYABLE,
                error_message=f"connection error: {exc}",
            )
        with self._lock:
            self.deliveries.append(notification)
        if ok:
            return DeliveryResult(success=True)
        return DeliveryResult(
            success=False, error_kind=ErrorKind.RETRYABLE, error_message="non-2xx"
        )


class ChannelRegistry:
    """Maps :class:`Channel` values to concrete channel implementations."""

    def __init__(
        self, channels: dict[Channel, InProcessChannel | LoopbackHttpChannel]
        | None = None,
    ) -> None:
        self._channels: dict[Channel, object] = dict(channels or {})

    def register(self, channel: object) -> None:
        kind = getattr(channel, "kind", None)
        if not isinstance(kind, Channel):
            raise ValueError("channel must expose a Channel 'kind' attribute")
        self._channels[kind] = channel

    def get(self, channel: Channel) -> object:
        if channel not in self._channels:
            raise KeyError(f"no channel registered for {channel.value}")
        return self._channels[channel]

    def has(self, channel: Channel) -> bool:
        return channel in self._channels

"""End-to-end delivery through a real loopback HTTP sink (stdlib client).

Spins a tiny stdlib ``http.server`` on 127.0.0.1 with an ephemeral port, so
the exercise covers real sockets without any external service or credentials.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from notification_fanout.channels import ChannelRegistry, LoopbackHttpChannel
from notification_fanout.clock import SystemClock
from notification_fanout.models import (
    Channel,
    DeliveryResult,
    ErrorKind,
    Event,
    NotificationStatus,
    Subscription,
)
from notification_fanout.service import NotificationService
from notification_fanout.store import NotificationStore


class _Sink:
    def __init__(self, status=200):
        self.received = []
        self.lock = threading.Lock()
        self.status = status

    def handler(self, sink):
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                with outer.lock:
                    outer.received.append(body)
                self.send_response(outer.status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":true}')

            def log_message(self, *args):
                pass

        return H


def _start_server(sink):
    server = HTTPServer(("127.0.0.1", 0), sink.handler(sink))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_loopback_http_delivery_success():
    sink = _Sink(status=200)
    server, _ = _start_server(sink)
    try:
        port = server.server_address[1]
        http_channel = LoopbackHttpChannel(
            kind=Channel.EMAIL, url=f"http://127.0.0.1:{port}/send", timeout=2
        )
        reg = ChannelRegistry()
        reg.register(http_channel)
        svc = NotificationService(
            store=NotificationStore(":memory:"),
            channels=reg,
            clock=SystemClock(),
        )
        svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
        svc.trigger(
            Event(event_type="x", user_id="u1", title="hello", event_id="e1")
        )
        assert svc.wait_idle(timeout=3)
        n = svc.get_event_notifications("e1")[0]
        assert n.status is NotificationStatus.DELIVERED
        assert len(sink.received) == 1
        assert sink.received[0]["event_id"] == "e1"
        assert sink.received[0]["channel"] == "email"
        svc.stop()
    finally:
        server.shutdown()


def test_loopback_http_rejects_non_loopback():
    import pytest

    with pytest.raises(ValueError):
        LoopbackHttpChannel(
            kind=Channel.EMAIL, url="http://8.8.8.8/send"
        )


def test_loopback_http_connection_refused_is_retryable_then_dead_letter():
    # Nothing listens on this port (bound + closed).
    sock_server = HTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    port = sock_server.server_address[1]
    sock_server.server_close()

    from datetime import timedelta

    from notification_fanout.policy import RetryPolicy

    http_channel = LoopbackHttpChannel(
        kind=Channel.EMAIL,
        url=f"http://127.0.0.1:{port}/send",
        timeout=0.5,
    )
    reg = ChannelRegistry()
    reg.register(http_channel)
    svc = NotificationService(
        store=NotificationStore(":memory:"),
        channels=reg,
        clock=SystemClock(),
        retry_policy=RetryPolicy(
            max_attempts=2,
            base_delay=timedelta(milliseconds=10),
            multiplier=1,
        ),
    )
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    assert svc.wait_idle(timeout=5)
    n = svc.get_event_notifications("e1")[0]
    assert n.status is NotificationStatus.DEAD_LETTER
    assert n.attempts == 2
    assert "connection error" in n.dead_letter_reason
    svc.stop()


def test_loopback_http_4xx_is_fatal():
    sink = _Sink(status=400)
    server, _ = _start_server(sink)
    try:
        port = server.server_address[1]
        http_channel = LoopbackHttpChannel(
            kind=Channel.EMAIL,
            url=f"http://127.0.0.1:{port}/send",
            timeout=2,
        )
        reg = ChannelRegistry()
        reg.register(http_channel)
        svc = NotificationService(
            store=NotificationStore(":memory:"),
            channels=reg,
            clock=SystemClock(),
        )
        svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
        svc.trigger(
            Event(event_type="x", user_id="u1", title="hi", event_id="e1")
        )
        assert svc.wait_idle(timeout=3)
        n = svc.get_event_notifications("e1")[0]
        assert n.status is NotificationStatus.DEAD_LETTER
        assert n.attempts == 1
        svc.stop()
    finally:
        server.shutdown()

"""Loopback HTTP channel delivery against a real local HTTP server."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from fastapi.testclient import TestClient

from notify.channels import LoopbackHttpChannel
from notify.models import IngestEvent
from tests.conftest import wait_terminal


class _Handler(BaseHTTPRequestHandler):
    status_code = 200
    received: list = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        _Handler.received.append(json.loads(body))
        self.send_response(self.status_code)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def http_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    _Handler.received = []
    _Handler.status_code = 200
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()


def test_loopback_http_delivery(make_app, http_server):
    port = http_server.server_address[1]
    channels = {"webhook": LoopbackHttpChannel("webhook", f"http://127.0.0.1:{port}")}
    app = make_app(channels=channels)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="http-1", user_id="u1", channels=["webhook"],
                        payload={"hello": "world"})
        )
        n = app.state.repo.get_by_event("http-1")[0]
        result = wait_terminal(app, n.id, timeout=5)
        assert result.status.value == "sent"
        assert len(_Handler.received) == 1
        assert _Handler.received[0]["event_id"] == "http-1"
        assert _Handler.received[0]["payload"] == {"hello": "world"}


def test_loopback_http_500_retries_then_dead_letter(make_app, http_server):
    _Handler.status_code = 500
    port = http_server.server_address[1]
    channels = {"webhook": LoopbackHttpChannel("webhook", f"http://127.0.0.1:{port}")}
    app = make_app(channels=channels, default_max_attempts=3, retry_base_delay=0.02)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="http-2", user_id="u1", channels=["webhook"])
        )
        n = app.state.repo.get_by_event("http-2")[0]
        result = wait_terminal(app, n.id, timeout=10)
        assert result.status.value == "dead_letter"
        assert result.attempts == 3
        assert "500" in result.reason
        assert len(_Handler.received) == 3  # retried, then stopped


def test_loopback_http_400_is_non_retryable(make_app, http_server):
    _Handler.status_code = 400
    port = http_server.server_address[1]
    channels = {"webhook": LoopbackHttpChannel("webhook", f"http://127.0.0.1:{port}")}
    app = make_app(channels=channels, default_max_attempts=5)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="http-3", user_id="u1", channels=["webhook"])
        )
        n = app.state.repo.get_by_event("http-3")[0]
        result = wait_terminal(app, n.id, timeout=5)
        assert result.status.value == "dead_letter"
        assert result.attempts == 1
        assert "non-retryable" in result.reason
        assert len(_Handler.received) == 1  # never replayed

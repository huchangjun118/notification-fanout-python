"""Failure retry with backoff and dead-letter semantics."""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from notify.channels import InMemoryChannel
from notify.models import IngestEvent
from tests.conftest import wait_terminal


def test_retryable_failure_then_success(make_app):
    channels = {"email": InMemoryChannel("email", fail_n_times=2)}
    app = make_app(channels=channels, default_max_attempts=5,
                   retry_base_delay=0.05, retry_max_delay=1.0)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="r-1", user_id="u1", channels=["email"])
        )
        n = app.state.repo.get_by_event("r-1")[0]
        result = wait_terminal(app, n.id, timeout=10)
        assert result.status.value == "sent"
        assert result.attempts == 3  # 2 failed + 1 succeeded
        assert len(channels["email"].sent) == 1
        assert channels["email"].attempts == 3


def test_retryable_exhaustion_enters_dead_letter(make_app):
    channels = {"email": InMemoryChannel("email", fail_forever=True)}
    app = make_app(channels=channels, default_max_attempts=4,
                   retry_base_delay=0.02, retry_max_delay=0.2)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="r-2", user_id="u1", channels=["email"])
        )
        n = app.state.repo.get_by_event("r-2")[0]
        result = wait_terminal(app, n.id, timeout=10)
        assert result.status.value == "dead_letter"
        assert result.attempts == 4
        assert "max attempts" in result.reason
        assert "simulated channel failure" in result.reason
        assert channels["email"].sent == []

        dl = app.state.repo.list_dead_letters()
        assert len(dl) == 1
        assert dl[0].id == n.id


def test_non_retryable_failure_is_not_replayed(make_app):
    channels = {"email": InMemoryChannel("email", fail_forever=True, non_retryable=True)}
    app = make_app(channels=channels, default_max_attempts=5, retry_base_delay=0.05)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="r-3", user_id="u1", channels=["email"])
        )
        n = app.state.repo.get_by_event("r-3")[0]
        result = wait_terminal(app, n.id, timeout=5)
        assert result.status.value == "dead_letter"
        assert result.attempts == 1
        assert "non-retryable" in result.reason
        time.sleep(0.5)
        # never replayed: the channel was called exactly once
        assert channels["email"].attempts == 1
        assert channels["email"].sent == []


def test_dead_letter_endpoint(make_app):
    channels = {"email": InMemoryChannel("email", fail_forever=True, non_retryable=True)}
    app = make_app(channels=channels, default_max_attempts=3)
    with TestClient(app) as client:
        client.post("/events", json={"event_id": "r-4", "user_id": "u1",
                                     "channels": ["email"]})
        n = app.state.repo.get_by_event("r-4")[0]
        wait_terminal(app, n.id, timeout=5)
        r = client.get("/dead-letters")
        assert r.status_code == 200
        items = r.json()["dead_letters"]
        assert len(items) == 1
        assert items[0]["id"] == n.id
        assert items[0]["status"] == "dead_letter"
        assert items[0]["reason"]


def test_backoff_spacing_increases(make_app):
    channels = {"email": InMemoryChannel("email", fail_forever=True)}
    app = make_app(channels=channels, default_max_attempts=4,
                   retry_base_delay=0.1, retry_max_delay=10.0)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="r-5", user_id="u1", channels=["email"])
        )
        n = app.state.repo.get_by_event("r-5")[0]
        wait_terminal(app, n.id, timeout=10)
        history = app.state.repo.list_history(n.id)
        # retry transitions should be spaced out by increasing backoff
        retry_times = [h.ts for h in history if h.to_status.value == "pending" and "retry" in (h.reason or "")]
        assert len(retry_times) >= 2
        gaps = [b - a for a, b in zip(retry_times, retry_times[1:])]
        # backoff gaps should be non-decreasing (jitter may compress one step)
        assert gaps[-1] >= gaps[0] * 0.8

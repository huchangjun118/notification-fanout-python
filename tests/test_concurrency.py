"""Concurrency: per-key serialization/rate limiting, parallel keys, unique terminal states."""
from __future__ import annotations

import threading
import time

from fastapi.testclient import TestClient

from notify.channels import InMemoryChannel
from notify.models import IngestEvent
from tests.conftest import wait_terminal


def test_same_user_channel_is_serialized(make_app):
    channels = {"email": InMemoryChannel("email", delay=0.3)}
    app = make_app(channels=channels)
    with TestClient(app):
        for i in range(2):
            app.state.service.ingest(
                IngestEvent(event_id=f"ser-{i}", user_id="u1", channels=["email"])
            )
        ns = [app.state.repo.get_by_event(f"ser-{i}")[0] for i in range(2)]
        for n in ns:
            wait_terminal(app, n.id, timeout=10)
        sent = sorted(channels["email"].sent, key=lambda s: s["ts"])
        assert len(sent) == 2
        gap = sent[1]["ts"] - sent[0]["ts"]
        assert gap >= 0.25  # second send started only after the first finished


def test_rate_limiter_spaces_same_key_sends(make_app):
    channels = {"email": InMemoryChannel("email")}
    app = make_app(channels=channels, rate_per_second=1.0, rate_burst=1)
    with TestClient(app):
        for i in range(2):
            app.state.service.ingest(
                IngestEvent(event_id=f"rl-{i}", user_id="u1", channels=["email"])
            )
        ns = [app.state.repo.get_by_event(f"rl-{i}")[0] for i in range(2)]
        t0 = time.time()
        for n in ns:
            wait_terminal(app, n.id, timeout=10)
        sent = sorted(channels["email"].sent, key=lambda s: s["ts"])
        assert len(sent) == 2
        gap = sent[1]["ts"] - sent[0]["ts"]
        assert gap >= 0.8  # token bucket spaced the two sends ~1s apart
        assert time.time() - t0 < 5


def test_slow_channel_does_not_block_fast_channel(make_app):
    channels = {
        "email": InMemoryChannel("email", delay=0.8),  # slow downstream
        "sms": InMemoryChannel("sms"),
    }
    app = make_app(channels=channels, dispatch_workers=16)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="slow-1", user_id="u1", channels=["email"])
        )
        app.state.service.ingest(
            IngestEvent(event_id="fast-1", user_id="u2", channels=["sms"])
        )
        slow = app.state.repo.get_by_event("slow-1")[0]
        fast = app.state.repo.get_by_event("fast-1")[0]
        t0 = time.time()
        wait_terminal(app, fast.id, timeout=5)
        fast_time = time.time() - t0
        wait_terminal(app, slow.id, timeout=5)
        total = time.time() - t0
        assert fast_time < 1.0      # fast channel finished quickly
        assert total < 1.5          # both ran in parallel, not 0.8+serial
        assert channels["sms"].sent[0]["ts"] < channels["email"].sent[0]["ts"]


def test_concurrent_claim_has_single_winner(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app):
        n = app.state.repo.create_notification(
            event_id="claim-1", user_id="u1", channel="email", payload={},
            scheduled_at=time.time() + 3600, urgent=False, max_attempts=5, now=time.time(),
        )
        wins = []

        def claim():
            wins.append(app.state.repo.try_claim(n.id, time.time()))

        threads = [threading.Thread(target=claim) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sum(wins) == 1
        fresh = app.state.repo.get_notification(n.id)
        assert fresh.status.value == "sending"


def test_terminal_state_is_never_overwritten(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="term-1", user_id="u1", channels=["email"])
        )
        n = app.state.repo.get_by_event("term-1")[0]
        wait_terminal(app, n.id, timeout=5)

        results = []

        def mark():
            results.append(app.state.repo.mark_dead_letter(n.id, "late conflicting update"))

        threads = [threading.Thread(target=mark) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sum(results) == 0  # terminal rows reject all later transitions
        fresh = app.state.repo.get_notification(n.id)
        assert fresh.status.value == "sent"
        history = app.state.repo.list_history(n.id)
        assert all(h.to_status.value != "dead_letter" for h in history)

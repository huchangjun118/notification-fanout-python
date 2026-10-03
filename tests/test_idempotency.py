"""Idempotency: same event+channel dedupes; different channels never merge."""
from __future__ import annotations

import threading

from fastapi.testclient import TestClient

from tests.conftest import wait_terminal


def test_duplicate_trigger_creates_single_delivery(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        body = {"event_id": "idem-1", "user_id": "u1"}
        r1 = client.post("/events", json=body)
        r2 = client.post("/events", json=body)
        ids1 = [n["id"] for n in r1.json()["notifications"]]
        ids2 = [n["id"] for n in r2.json()["notifications"]]
        assert ids1 == ids2  # same notification rows returned

        for i in ids1:
            wait_terminal(app, i)
        for ch in channels.values():
            assert len(ch.sent) == 1


def test_different_channels_are_not_deduped(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        client.post("/events", json={"event_id": "idem-2", "user_id": "u1",
                                     "channels": ["email"]})
        client.post("/events", json={"event_id": "idem-2", "user_id": "u1",
                                     "channels": ["sms"]})
        client.post("/events", json={"event_id": "idem-2", "user_id": "u1",
                                     "channels": ["email", "sms"]})
        email_n = app.state.repo.get_by_event("idem-2")
        assert len(email_n) == 2  # one per channel, not merged
        wait_terminal(app, email_n[0].id)
        wait_terminal(app, email_n[1].id)
        assert len(channels["email"].sent) == 1
        assert len(channels["sms"].sent) == 1


def test_concurrent_duplicate_triggers(make_app, channels):
    app = make_app(channels=channels)
    errors = []
    client = TestClient(app)

    def trigger():
        try:
            client.post("/events", json={"event_id": "idem-3", "user_id": "u1",
                                         "channels": ["email"]})
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    with client:
        threads = [threading.Thread(target=trigger) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

        notes = app.state.repo.get_by_event("idem-3")
        assert len(notes) == 1
        wait_terminal(app, notes[0].id)
        assert len(channels["email"].sent) == 1

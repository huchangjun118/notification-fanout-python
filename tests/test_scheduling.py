"""Scheduled/delayed delivery and restart recovery."""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from notify.channels import InMemoryChannel
from notify.models import IngestEvent
from tests.conftest import wait_terminal


def test_delayed_delivery_not_sent_early(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        future = time.time() + 1.5
        client.post("/events", json={"event_id": "sched-1", "user_id": "u1",
                                     "channels": ["email"], "scheduled_at": future})
        n = app.state.repo.get_by_event("sched-1")[0]

        time.sleep(0.6)
        mid = app.state.repo.get_notification(n.id)
        assert mid.status.value == "pending"
        assert channels["email"].sent == []  # not sent before its time

        wait_terminal(app, n.id, timeout=5)
        assert len(channels["email"].sent) == 1
        assert channels["email"].sent[0]["ts"] >= future - 0.05


def test_restart_recovers_future_notification(make_app, tmp_path):
    db = str(tmp_path / "restart.db")
    ch1 = {"email": InMemoryChannel("email")}
    future = time.time() + 2.0

    app1 = make_app(channels=ch1, db_path=db)
    with TestClient(app1):
        app1.state.service.ingest(
            IngestEvent(event_id="rest-1", user_id="u1",
                        channels=["email"], scheduled_at=future)
        )
        nid = app1.state.repo.get_by_event("rest-1")[0].id
    # process "restarts": app1 torn down, new app on the same db file
    ch2 = {"email": InMemoryChannel("email")}
    app2 = make_app(channels=ch2, db_path=db)
    with TestClient(app2):
        n = app2.state.repo.get_notification(nid)
        assert n.status.value == "pending"
        assert abs(n.scheduled_at - future) < 0.01

        # idempotent replay after restart returns the same row
        replay = app2.state.service.ingest(
            IngestEvent(event_id="rest-1", user_id="u1",
                        channels=["email"], scheduled_at=future)
        )
        assert replay[0].id == nid

        wait_terminal(app2, nid, timeout=5)
        assert len(ch2["email"].sent) == 1
        assert ch2["email"].sent[0]["ts"] >= future - 0.05


def test_due_notification_survives_restart(make_app, tmp_path):
    db = str(tmp_path / "restart-due.db")
    ch1 = {"email": InMemoryChannel("email")}
    app1 = make_app(channels=ch1, db_path=db)
    with TestClient(app1):
        # insert an already-due row directly, then "crash" without dispatching
        n = app1.state.repo.create_notification(
            event_id="rest-due", user_id="u1", channel="email", payload={},
            scheduled_at=time.time() - 1, urgent=False, max_attempts=5, now=time.time(),
        )
        nid = n.id

    ch2 = {"email": InMemoryChannel("email")}
    app2 = make_app(channels=ch2, db_path=db)
    with TestClient(app2):
        wait_terminal(app2, nid, timeout=5)
        assert len(ch2["email"].sent) == 1


def test_stale_lease_is_recovered_and_not_duplicated(make_app, channels):
    app = make_app(channels=channels, lease_timeout=0.2)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="lease-1", user_id="u1", channels=["email"])
        )
        n = app.state.repo.get_by_event("lease-1")[0]
        # simulate a worker that died after claiming: SENDING with an old lease
        with app.state.repo._lock:
            app.state.repo._conn.execute(
                "UPDATE notifications SET status='sending', leased_at=? WHERE id=?",
                (time.time() - 100, n.id),
            )
            app.state.repo._conn.commit()

        wait_terminal(app, n.id, timeout=5)
        fresh = app.state.repo.get_notification(n.id)
        assert fresh.status.value == "sent"
        assert len(channels["email"].sent) == 1  # no duplicate delivery

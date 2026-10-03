"""HTTP API behaviour via FastAPI TestClient (in-process, no network)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from notification_fanout.api import create_app
from notification_fanout.models import Channel, ErrorKind


@pytest.fixture
def client(service_factory):
    svc = service_factory()
    svc.set_subscription.__self__  # service is live
    return TestClient(create_app(svc)), svc


def test_trigger_and_query_lifecycle(client, channels):
    http, svc = client
    r = http.put(
        "/subscriptions/u1",
        json={"enabled_channels": ["email", "push"]},
    )
    assert r.status_code == 200

    r = http.post(
        "/events",
        json={
            "event_type": "login",
            "user_id": "u1",
            "title": "welcome",
            "event_id": "evt-1",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["duplicate"] is False
    assert len(data["scheduled"]) == 2
    assert "sms" in data["skipped_disabled"]

    svc.run_due()

    r = http.get("/events/evt-1")
    assert r.status_code == 200
    event = r.json()
    assert event["terminal"] is True
    assert event["aggregate_status"] == "delivered"
    assert len(event["notifications"]) == 2


def test_duplicate_trigger_is_idempotent(client, channels):
    http, svc = client
    http.put("/subscriptions/u1", json={"enabled_channels": ["email"]})
    payload = {
        "event_type": "login",
        "user_id": "u1",
        "title": "hi",
        "event_id": "evt-dup",
    }
    first = http.post("/events", json=payload).json()
    second = http.post("/events", json=payload).json()
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert second["scheduled"][0]["id"] == first["scheduled"][0]["id"]
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1


def test_explain_endpoint(client, channels):
    http, svc = client
    channels[Channel.EMAIL].script_failure(ErrorKind.FATAL, "bad")
    http.put("/subscriptions/u1", json={"enabled_channels": ["email"]})
    http.post(
        "/events",
        json={"event_type": "x", "user_id": "u1", "title": "h", "event_id": "e"},
    )
    svc.run_due()
    nid = svc.get_event_notifications("e")[0].id
    r = http.get(f"/notifications/{nid}/explain")
    assert r.status_code == 200
    assert "dead_letter" in r.json()["explanation"]
    assert http.get("/notifications/nope").status_code == 404


def test_dead_letters_endpoint(client, channels):
    http, svc = client
    channels[Channel.EMAIL].script_failure(ErrorKind.FATAL, "x")
    http.put("/subscriptions/u1", json={"enabled_channels": ["email"]})
    http.post(
        "/events",
        json={"event_type": "x", "user_id": "u1", "title": "h", "event_id": "e"},
    )
    svc.run_due()
    r = http.get("/dead-letters")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["dead_letters"][0]["dead_letter_reason"]


def test_config_patch_applies_to_future_sends(client):
    http, _svc = client
    assert http.get("/config").json()["max_concurrency"] == 8
    r = http.patch("/config", json={"max_concurrency": 3})
    assert r.status_code == 200
    assert r.json()["max_concurrency"] == 3

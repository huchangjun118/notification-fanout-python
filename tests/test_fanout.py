"""Routing/fanout semantics: multi-channel delivery, disabled channels, isolation."""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from tests.conftest import wait_terminal


def test_event_fans_out_to_all_enabled_channels(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        r = client.post("/events", json={"event_id": "e1", "user_id": "u1",
                                          "payload": {"k": "v"}})
        assert r.status_code == 200
        notes = r.json()["notifications"]
        assert {n["channel"] for n in notes} == {"email", "sms", "push", "webhook"}
        ids = [n["id"] for n in notes]

        results = [wait_terminal(app, i) for i in ids]
        assert all(n.status.value == "sent" for n in results)
        for ch in channels.values():
            assert len(ch.sent) == 1
            assert ch.sent[0]["event_id"] == "e1"
            assert ch.sent[0]["payload"] == {"k": "v"}


def test_disabled_channel_is_skipped_and_never_delivered(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        client.put("/users/u1/preferences", json={"channels": {"sms": False, "push": False}})
        r = client.post("/events", json={"event_id": "e2", "user_id": "u1"})
        notes = r.json()["notifications"]
        by_channel = {n["channel"]: n for n in notes}
        assert by_channel["sms"]["status"] == "skipped"
        assert by_channel["push"]["status"] == "skipped"
        assert "disabled by user preference" in by_channel["sms"]["reason"]

        wait_terminal(app, by_channel["email"]["id"])
        wait_terminal(app, by_channel["webhook"]["id"])
        assert channels["sms"].sent == []
        assert channels["push"].sent == []
        assert len(channels["email"].sent) == 1
        assert len(channels["webhook"].sent) == 1


def test_explicit_channel_list_respects_preferences(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        client.put("/users/u1/preferences", json={"channels": {"sms": False}})
        r = client.post("/events", json={"event_id": "e3", "user_id": "u1",
                                          "channels": ["email", "sms", "unknown"]})
        notes = r.json()["notifications"]
        by_channel = {n["channel"]: n for n in notes}
        assert by_channel["sms"]["status"] == "skipped"
        assert by_channel["unknown"]["status"] == "skipped"
        wait_terminal(app, by_channel["email"]["id"])
        assert len(channels["email"].sent) == 1
        assert channels["sms"].sent == []


def test_one_channel_failure_does_not_affect_others(make_app, channels):
    channels["sms"].fail_forever = True
    app = make_app(channels=channels, default_max_attempts=2, retry_base_delay=0.05)
    with TestClient(app) as client:
        r = client.post("/events", json={"event_id": "e4", "user_id": "u1"})
        notes = r.json()["notifications"]
        ids = {n["channel"]: n["id"] for n in notes}

        email = wait_terminal(app, ids["email"])
        webhook = wait_terminal(app, ids["webhook"])
        push = wait_terminal(app, ids["push"])
        sms = wait_terminal(app, ids["sms"])

        assert email.status.value == "sent"
        assert webhook.status.value == "sent"
        assert push.status.value == "sent"
        assert sms.status.value == "dead_letter"
        assert len(channels["email"].sent) == 1
        assert len(channels["webhook"].sent) == 1
        assert len(channels["push"].sent) == 1
        assert channels["sms"].sent == []


def test_default_preferences_enable_all_channels(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        r = client.get("/users/nobody/preferences")
        assert r.json()["channels"] == {"email": True, "sms": True, "push": True, "webhook": True}

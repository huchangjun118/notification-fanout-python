"""HTTP API surface tests."""
from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import wait_terminal


def test_health_and_ingest(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        r = client.post("/events", json={"event_id": "api-1", "user_id": "u1",
                                         "payload": {"a": 1}})
        assert r.status_code == 200
        body = r.json()
        assert body["event_id"] == "api-1"
        assert len(body["notifications"]) == 4


def test_event_and_notification_status(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        client.post("/events", json={"event_id": "api-2", "user_id": "u1",
                                     "channels": ["email"]})
        r = client.get("/events/api-2")
        notes = r.json()["notifications"]
        assert len(notes) == 1
        nid = notes[0]["id"]
        wait_terminal(app, nid, timeout=5)

        r2 = client.get(f"/notifications/{nid}")
        assert r2.status_code == 200
        assert r2.json()["status"] == "sent"

        r3 = client.get("/notifications/999999")
        assert r3.status_code == 404


def test_preferences_endpoints(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        r = client.get("/users/u1/preferences")
        assert r.json()["channels"] == {"email": True, "sms": True, "push": True, "webhook": True}

        r = client.put("/users/u1/preferences", json={"channels": {"sms": False},
                                                      "quiet_start_hour": 23})
        assert r.status_code == 200
        assert r.json()["channels"]["sms"] is False
        assert r.json()["quiet_start_hour"] == 23

        r = client.get("/users/u1/preferences")
        assert r.json()["channels"]["sms"] is False


def test_history_endpoint(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        client.post("/events", json={"event_id": "api-3", "user_id": "u1",
                                     "channels": ["email"]})
        nid = app.state.repo.get_by_event("api-3")[0].id
        wait_terminal(app, nid, timeout=5)
        r = client.get(f"/notifications/{nid}/history")
        assert r.status_code == 200
        transitions = [(h["from_status"], h["to_status"]) for h in r.json()["history"]]
        assert ("pending", "sending") in transitions
        assert ("sending", "sent") in transitions


def test_config_endpoint_rejects_bad_values(make_app, channels):
    app = make_app(channels=channels)
    with TestClient(app) as client:
        r = client.put("/admin/config", json={"freq_strategy": "nope"})
        assert r.status_code == 400
        r = client.put("/admin/config", json={"quiet_start_hour": 25})
        assert r.status_code == 400
        r = client.put("/admin/config", json={"unknown_key": 1})
        assert r.status_code == 422  # pydantic rejects unknown fields
        r = client.get("/admin/config")
        assert r.status_code == 200
        assert r.json()["freq_strategy"] == "defer"


def test_config_change_affects_subsequent_sends(make_app, channels):
    app = make_app(channels=channels, freq_cap=1, freq_strategy="drop")
    with TestClient(app) as client:
        client.post("/events", json={"event_id": "cc-1", "user_id": "u1",
                                     "channels": ["email"]})
        client.post("/events", json={"event_id": "cc-2", "user_id": "u1",
                                     "channels": ["email"]})
        wait_terminal(app, app.state.repo.get_by_event("cc-1")[0].id, timeout=5)
        dropped = wait_terminal(app, app.state.repo.get_by_event("cc-2")[0].id, timeout=5)
        assert dropped.status.value == "dropped"

        client.put("/admin/config", json={"freq_cap": 100})
        client.post("/events", json={"event_id": "cc-3", "user_id": "u1",
                                     "channels": ["email"]})
        sent = wait_terminal(app, app.state.repo.get_by_event("cc-3")[0].id, timeout=5)
        assert sent.status.value == "sent"

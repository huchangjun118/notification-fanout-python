"""Quiet hours and frequency-cap policy."""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from notify.models import IngestEvent
from notify.policy import in_quiet_window, next_quiet_end
from tests.conftest import wait_terminal


def _ts(hour: int, minute: int = 0) -> float:
    return time.mktime((2026, 10, 3, hour, minute, 0, 5, 276, -1))


def test_quiet_window_midnight_wrap():
    assert in_quiet_window(_ts(22), 22, 8) is True
    assert in_quiet_window(_ts(23), 22, 8) is True
    assert in_quiet_window(_ts(7, 59), 22, 8) is True
    assert in_quiet_window(_ts(8), 22, 8) is False  # end exclusive
    assert in_quiet_window(_ts(10), 22, 8) is False
    assert in_quiet_window(_ts(0), 22, 8) is True
    # same start/end disables the window
    assert in_quiet_window(_ts(23), 0, 0) is False


def test_next_quiet_end():
    eight_am = _ts(8)
    assert next_quiet_end(_ts(23), 8) == eight_am + 24 * 3600  # -> tomorrow 8
    assert next_quiet_end(_ts(7), 8) == eight_am               # -> today 8
    assert next_quiet_end(_ts(10), 8) == eight_am + 24 * 3600


def test_non_urgent_deferred_until_quiet_end(make_app, channels, monkeypatch):
    app = make_app(channels=channels, quiet_start_hour=22, quiet_end_hour=8)
    fake_night = time.struct_time((2026, 10, 3, 23, 0, 0, 5, 276, 0))
    monkeypatch.setattr("notify.policy.time.localtime", lambda ts: fake_night)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="q-1", user_id="u1", channels=["email"], urgent=False)
        )
        n = app.state.repo.get_by_event("q-1")[0]
        time.sleep(0.5)
        n = app.state.repo.get_notification(n.id)
        assert n.status.value == "pending"
        assert "quiet" in n.reason
        assert n.scheduled_at > time.time() + 3600  # pushed to 08:00
        assert channels["email"].sent == []

        # simulate the window ending (due + urgent): it must now be delivered
        with app.state.repo._lock:
            app.state.repo._conn.execute(
                "UPDATE notifications SET scheduled_at=?, urgent=1 WHERE id=?",
                (time.time() - 1, n.id),
            )
            app.state.repo._conn.commit()
        wait_terminal(app, n.id, timeout=5)
        assert len(channels["email"].sent) == 1


def test_urgent_bypasses_quiet_hours(make_app, channels, monkeypatch):
    app = make_app(channels=channels, quiet_start_hour=22, quiet_end_hour=8)
    fake_night = time.struct_time((2026, 10, 3, 23, 0, 0, 5, 276, 0))
    monkeypatch.setattr("notify.policy.time.localtime", lambda ts: fake_night)
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="q-2", user_id="u1", channels=["email"], urgent=True)
        )
        n = app.state.repo.get_by_event("q-2")[0]
        result = wait_terminal(app, n.id, timeout=5)
        assert result.status.value == "sent"  # urgent went out despite quiet hours
        assert len(channels["email"].sent) == 1


def test_user_quiet_hours_override_default(make_app, channels, monkeypatch):
    app = make_app(channels=channels)
    fake_night = time.struct_time((2026, 10, 3, 23, 0, 0, 5, 276, 0))
    monkeypatch.setattr("notify.policy.time.localtime", lambda ts: fake_night)
    with TestClient(app):
        app.state.service.update_preferences(
            "u1", quiet_start_hour=0, quiet_end_hour=0  # disabled for this user
        )
        app.state.service.ingest(
            IngestEvent(event_id="q-3", user_id="u1", channels=["email"], urgent=False)
        )
        n = app.state.repo.get_by_event("q-3")[0]
        wait_terminal(app, n.id, timeout=5)
        assert len(channels["email"].sent) == 1


def test_frequency_cap_defers(make_app, channels):
    app = make_app(
        channels=channels, freq_cap=2, freq_window_seconds=3600, freq_strategy="defer"
    )
    with TestClient(app):
        ids = []
        for i in range(3):
            app.state.service.ingest(
                IngestEvent(event_id=f"f-{i}", user_id="u1", channels=["email"])
            )
            ids.append(app.state.repo.get_by_event(f"f-{i}")[0].id)

        wait_terminal(app, ids[0], timeout=5)
        wait_terminal(app, ids[1], timeout=5)

        deadline = time.time() + 5
        third = None
        while time.time() < deadline:
            third = app.state.repo.get_notification(ids[2])
            if third.status.value == "pending" and "frequency" in (third.reason or ""):
                break
            time.sleep(0.05)
        assert third.status.value == "pending"
        assert "frequency" in third.reason
        assert third.scheduled_at > time.time() + 1800  # deferred to next slot
        assert len(channels["email"].sent) == 2

        # config change raises the cap; the deferred send is now allowed
        app.state.config_holder.update(freq_cap=100)
        with app.state.repo._lock:
            app.state.repo._conn.execute(
                "UPDATE notifications SET scheduled_at=? WHERE id=?",
                (time.time() - 1, ids[2]),
            )
            app.state.repo._conn.commit()
        wait_terminal(app, ids[2], timeout=5)
        assert len(channels["email"].sent) == 3


def test_frequency_cap_drop(make_app, channels):
    app = make_app(
        channels=channels, freq_cap=1, freq_window_seconds=3600, freq_strategy="drop"
    )
    with TestClient(app):
        app.state.service.ingest(
            IngestEvent(event_id="fd-0", user_id="u1", channels=["email"])
        )
        app.state.service.ingest(
            IngestEvent(event_id="fd-1", user_id="u1", channels=["email"])
        )
        n0 = app.state.repo.get_by_event("fd-0")[0]
        n1 = app.state.repo.get_by_event("fd-1")[0]
        wait_terminal(app, n0.id, timeout=5)
        dropped = wait_terminal(app, n1.id, timeout=5)
        assert dropped.status.value == "dropped"
        assert "frequency" in dropped.reason
        assert len(channels["email"].sent) == 1


def test_frequency_cap_is_per_user_and_channel(make_app, channels):
    app = make_app(
        channels=channels, freq_cap=1, freq_window_seconds=3600, freq_strategy="drop"
    )
    with TestClient(app):
        for user in ("u1", "u2"):
            for ch in ("email", "sms"):
                app.state.service.ingest(
                    IngestEvent(event_id=f"fx-{user}-{ch}", user_id=user, channels=[ch])
                )
        time.sleep(1.0)
        # 4 different (user, channel) keys -> each gets one send, nothing dropped
        for user in ("u1", "u2"):
            for ch in ("email", "sms"):
                n = app.state.repo.get_by_event(f"fx-{user}-{ch}")[0]
                assert n.status.value == "sent"

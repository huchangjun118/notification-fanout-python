"""Shared test helpers."""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from notify.app import create_app
from notify.channels import InMemoryChannel
from notify.config import Config

TERMINAL = {"sent", "dead_letter", "dropped", "skipped"}


@pytest.fixture
def make_app(tmp_path):
    """Factory for apps with a temp SQLite db and fast scheduler ticks."""

    def _make(channels=None, db_path=None, **config_overrides):
        db = db_path or str(tmp_path / f"test-{time.time_ns()}.db")
        defaults = dict(
            tick_interval=0.05,
            rate_per_second=100.0,
            rate_burst=100,
            freq_cap=10_000,
            quiet_start_hour=0,
            quiet_end_hour=0,
        )
        defaults.update(config_overrides)
        return create_app(config=Config(db_path=db, **defaults), channels=channels)

    return _make


@pytest.fixture
def channels():
    return {
        name: InMemoryChannel(name)
        for name in ("email", "sms", "push", "webhook")
    }


def wait_terminal(app, notification_id: int, timeout: float = 10.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = app.state.repo.get_notification(notification_id)
        if last is not None and last.status.value in TERMINAL:
            return last
        time.sleep(0.02)
    raise AssertionError(
        f"notification {notification_id} not terminal within {timeout}s; last={last}"
    )


def wait_all_terminal(app, notification_ids, timeout: float = 10.0):
    deadline = time.time() + timeout
    results = []
    while time.time() < deadline:
        results = [app.state.repo.get_notification(i) for i in notification_ids]
        if all(n is not None and n.status.value in TERMINAL for n in results):
            return results
        time.sleep(0.02)
    raise AssertionError(f"not all terminal within {timeout}s: {[n and n.status.value for n in results]}")

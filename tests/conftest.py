"""Shared pytest fixtures and builders."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from notification_fanout.channels import ChannelRegistry, InProcessChannel
from notification_fanout.clock import FakeClock
from notification_fanout.config import AtomicConfig, DispatchConfig
from notification_fanout.models import Channel, Subscription
from notification_fanout.policy import FrequencyCap, QuietHours, RetryPolicy
from notification_fanout.service import NotificationService
from notification_fanout.store import NotificationStore

ALL_CHANNELS = frozenset(Channel)


@pytest.fixture
def clock():
    # Fixed, fully timezone-aware start time.
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return FakeClock(start)


@pytest.fixture
def registry():
    reg = ChannelRegistry()
    for ch in Channel:
        reg.register(InProcessChannel(kind=ch))
    return reg


@pytest.fixture
def channels(registry):
    return {ch: registry.get(ch) for ch in Channel}


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "notifications.db"


def make_service(
    clock,
    registry,
    db=":memory:",
    retry_policy=None,
    quiet_hours=None,
    frequency_caps=None,
    config=None,
    auto_start=True,
):
    store = NotificationStore(db)
    return NotificationService(
        store=store,
        channels=registry,
        config=config or AtomicConfig(DispatchConfig()),
        clock=clock,
        retry_policy=retry_policy or RetryPolicy(),
        quiet_hours=quiet_hours,
        frequency_caps=frequency_caps,
        auto_start=auto_start,
    )


@pytest.fixture
def service_factory(clock, registry):
    created = []

    def _factory(db=":memory:", **kwargs):
        svc = make_service(clock, registry, db=db, **kwargs)
        created.append(svc)
        return svc

    yield _factory

    for svc in created:
        try:
            svc.stop()
        except Exception:
            pass

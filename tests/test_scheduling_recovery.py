"""Scheduled / delayed delivery and restart recovery."""

from __future__ import annotations

from datetime import timedelta

from notification_fanout.channels import ChannelRegistry, InProcessChannel
from notification_fanout.models import (
    Channel,
    Event,
    NotificationStatus,
    Subscription,
)
from notification_fanout.policy import RetryPolicy
from notification_fanout.service import NotificationService


def test_delayed_notification_not_sent_early(service_factory, channels, clock):
    svc = service_factory()
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    event = Event(
        event_type="reminder",
        user_id="u1",
        title="later",
        event_id="e1",
        not_before=clock.now() + timedelta(hours=2),
    )
    svc.trigger(event)
    svc.run_due()
    assert channels[Channel.EMAIL].deliveries == []
    n = svc.get_event_notifications("e1")[0]
    assert n.status is NotificationStatus.PENDING
    # still early one moment before
    clock.advance(timedelta(hours=2) - timedelta(seconds=1))
    svc.run_due()
    assert channels[Channel.EMAIL].deliveries == []
    # due exactly at not_before
    clock.advance(timedelta(seconds=1))
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    assert (
        svc.get_event_notifications("e1")[0].status
        is NotificationStatus.DELIVERED
    )


def test_restart_recovers_pending_and_delivers_once(service_factory, channels, clock, db_path):
    """A pending notification survives restart: not lost and not duplicated."""
    registry1 = ChannelRegistry()
    email1 = InProcessChannel(Channel.EMAIL)
    registry1.register(email1)
    svc1 = service_factory(db=str(db_path))
    svc1.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc1.trigger(
        Event(
            event_type="reminder",
            user_id="u1",
            title="later",
            event_id="e1",
            not_before=clock.now() + timedelta(hours=1),
        )
    )
    svc1.run_due()
    assert email1.deliveries == []  # not due
    svc1.stop()

    # restart with a fresh process-like service against the same DB/clock
    registry2 = ChannelRegistry()
    email2 = InProcessChannel(Channel.EMAIL)
    registry2.register(email2)
    svc2 = NotificationService(
        store=__import__(
            "notification_fanout.store", fromlist=["NotificationStore"]
        ).NotificationStore(str(db_path)),
        channels=registry2,
        clock=clock,
        retry_policy=RetryPolicy(),
    )
    svc2.run_due()
    assert email2.deliveries == []  # time still not reached
    clock.advance(timedelta(hours=2))
    svc2.run_due()
    assert len(email2.deliveries) == 1
    notes = svc2.get_event_notifications("e1")
    assert len(notes) == 1
    assert notes[0].status is NotificationStatus.DELIVERED
    svc2.stop()

    # restart again: terminal notification must never replay
    registry3 = ChannelRegistry()
    email3 = InProcessChannel(Channel.EMAIL)
    registry3.register(email3)
    from notification_fanout.store import NotificationStore

    svc3 = NotificationService(
        store=NotificationStore(str(db_path)),
        channels=registry3,
        clock=clock,
    )
    svc3.run_due()
    assert email3.deliveries == []
    svc3.stop()


def test_restart_recovers_interrupted_delivery(service_factory, channels, clock, db_path):
    """A notification left DELIVERING by a crash is retried, not lost/replayed."""
    # Phase 1: insert a notification and manually move it to DELIVERING,
    # simulating a process killed mid channel call (no attempt recorded).
    svc1 = service_factory(db=str(db_path))
    svc1.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    result = svc1.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    nid = result.notifications[0].id
    # stop scheduler before it picks the row up, then simulate the crash state
    svc1.stop()
    n = svc1.store.get(nid)
    n.status = NotificationStatus.DELIVERING
    n.attempts = 1
    assert svc1.store.compare_and_update(n)

    # restart: recovery must requeue and deliver exactly once
    registry2 = ChannelRegistry()
    email2 = InProcessChannel(Channel.EMAIL)
    registry2.register(email2)
    from notification_fanout.store import NotificationStore

    svc2 = NotificationService(
        store=NotificationStore(str(db_path)),
        channels=registry2,
        clock=clock,
    )
    svc2.run_due()
    assert len(email2.deliveries) == 1
    recovered = svc2.get_notification(nid)
    assert recovered.status is NotificationStatus.DELIVERED
    svc2.stop()


def test_duplicate_trigger_after_restart_remains_idempotent(service_factory, channels, clock, db_path):
    svc1 = service_factory(db=str(db_path))
    svc1.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc1.trigger(
        Event(
            event_type="x",
            user_id="u1",
            title="later",
            event_id="e1",
            not_before=clock.now() + timedelta(hours=5),
        )
    )
    svc1.stop()

    from notification_fanout.store import NotificationStore

    registry2 = ChannelRegistry()
    email2 = InProcessChannel(Channel.EMAIL)
    registry2.register(email2)
    svc2 = NotificationService(
        store=NotificationStore(str(db_path)),
        channels=registry2,
        clock=clock,
    )
    svc2.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    again = svc2.trigger(
        Event(event_type="x", user_id="u1", title="later", event_id="e1")
    )
    assert again.duplicate is True
    assert len(again.notifications) == 1
    svc2.run_due()
    assert email2.deliveries == []
    svc2.stop()

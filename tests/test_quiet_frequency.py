"""Quiet hours, frequency caps and explainability of the outcome."""

from __future__ import annotations

from datetime import time, timedelta, timezone

from notification_fanout.models import (
    Channel,
    Event,
    ExceedPolicy,
    NotificationStatus,
    Subscription,
    Urgency,
)
from notification_fanout.policy import FrequencyCap, QuietHours


def test_quiet_hours_defers_normal_notification(service_factory, channels, clock):
    quiet = QuietHours(start=time(22), end=time(8), tzname="UTC")
    # pin clock inside the window: 23:00 UTC
    clock.set(clock.now().replace(hour=23, minute=0, second=0, microsecond=0))
    svc = service_factory(quiet_hours=quiet)
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    svc.run_due()
    assert channels[Channel.EMAIL].deliveries == []
    n = svc.get_event_notifications("e1")[0]
    assert n.status is NotificationStatus.PENDING
    assert any("quiet hours" in c.reason for c in n.history)
    # opening at 08:00 next day
    clock.set(
        clock.now().replace(hour=8, minute=0, second=0) + timedelta(days=1)
        if clock.now().hour >= 8
        else clock.now().replace(hour=8, minute=0, second=0)
    )
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    assert (
        svc.get_event_notifications("e1")[0].status
        is NotificationStatus.DELIVERED
    )


def test_urgent_bypasses_quiet_hours(service_factory, channels, clock):
    quiet = QuietHours(start=time(22), end=time(8))
    clock.set(clock.now().replace(hour=23, minute=0, second=0, microsecond=0))
    svc = service_factory(quiet_hours=quiet)
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(
        Event(
            event_type="security",
            user_id="u1",
            title="ALERT",
            event_id="e1",
            urgency=Urgency.URGENT,
        )
    )
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    assert (
        svc.get_event_notifications("e1")[0].status
        is NotificationStatus.DELIVERED
    )


def test_frequency_cap_defers_to_window_end(service_factory, channels, clock):
    cap = FrequencyCap(
        max_sends=2,
        window=timedelta(minutes=10),
        exceed_policy=ExceedPolicy.DEFER,
    )
    svc = service_factory(frequency_caps={Channel.EMAIL: cap})
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    for i in range(3):
        svc.trigger(
            Event(event_type="x", user_id="u1", title=f"n{i}", event_id=f"e{i}")
        )
    svc.run_due()
    # two sent, third deferred
    assert len(channels[Channel.EMAIL].deliveries) == 2
    third = svc.get_event_notifications("e2")[0]
    assert third.status is NotificationStatus.PENDING
    assert any("frequency cap" in c.reason for c in third.history)
    # nothing happens mid-window
    clock.advance(timedelta(minutes=5))
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 2
    # after window, deferred one goes out
    clock.advance(timedelta(minutes=6))
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 3
    assert (
        svc.get_event_notifications("e2")[0].status
        is NotificationStatus.DELIVERED
    )


def test_frequency_cap_drop_policy_terminates_dropped(service_factory, channels):
    cap = FrequencyCap(
        max_sends=1, window=timedelta(minutes=10), exceed_policy=ExceedPolicy.DROP
    )
    svc = service_factory(frequency_caps={Channel.EMAIL: cap})
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(Event(event_type="x", user_id="u1", title="a", event_id="e1"))
    svc.trigger(Event(event_type="x", user_id="u1", title="b", event_id="e2"))
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    kept = svc.get_event_notifications("e1")[0]
    dropped = svc.get_event_notifications("e2")[0]
    assert kept.status is NotificationStatus.DELIVERED
    assert dropped.status is NotificationStatus.DROPPED
    assert dropped.is_terminal
    assert "policy=drop" in dropped.history[-1].reason
    # explainability string contains the decision
    explanation = svc.explain(dropped.id)
    assert "dropped" in explanation and "frequency cap" in explanation


def test_frequency_cap_is_per_user_and_channel(service_factory, channels):
    cap = FrequencyCap(max_sends=1, window=timedelta(minutes=10))
    svc = service_factory(frequency_caps={
        Channel.EMAIL: cap,
        Channel.SMS: cap,
    })
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.EMAIL, Channel.SMS}))
    )
    svc.set_subscription(Subscription("u2", frozenset({Channel.EMAIL})))
    svc.trigger(Event(event_type="x", user_id="u1", title="1", event_id="a"))
    svc.trigger(Event(event_type="x", user_id="u1", title="2", event_id="b"))
    svc.trigger(Event(event_type="x", user_id="u2", title="3", event_id="c"))
    svc.run_due()
    # u1: one email sent (one deferred), one sms sent; u2 email independent
    assert len(channels[Channel.EMAIL].deliveries) == 2
    assert len(channels[Channel.SMS].deliveries) == 1
    deferred = svc.get_event_notifications("b")[0]
    assert deferred.channel is Channel.EMAIL
    assert deferred.status is NotificationStatus.PENDING


def test_failed_attempts_consume_frequency_capacity(service_factory, channels):
    from notification_fanout.models import ErrorKind

    cap = FrequencyCap(max_sends=1, window=timedelta(minutes=10))
    channels[Channel.EMAIL].script_failure(
        ErrorKind.RETRYABLE, "downstream busy"
    )
    svc = service_factory(frequency_caps={Channel.EMAIL: cap})
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(Event(event_type="x", user_id="u1", title="a", event_id="e1"))
    svc.trigger(Event(event_type="x", user_id="u1", title="b", event_id="e2"))
    svc.run_due()
    # first consumed the only slot with a failed attempt; second is deferred
    assert len(channels[Channel.EMAIL].deliveries) == 1
    second = svc.get_event_notifications("e2")[0]
    assert second.status is NotificationStatus.PENDING

"""Routing preferences, fan-out isolation and per-channel idempotency."""

from __future__ import annotations

from notification_fanout.models import (
    Channel,
    ErrorKind,
    Event,
    NotificationStatus,
    Subscription,
)


def test_only_enabled_channels_receive(service_factory, channels):
    svc = service_factory()
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.EMAIL, Channel.PUSH}))
    )
    result = svc.trigger(
        Event(event_type="login", user_id="u1", title="hi", event_id="e1")
    )
    routed = {n.channel for n in result.notifications}
    assert routed == {Channel.EMAIL, Channel.PUSH}
    assert Channel.SMS in result.skipped_disabled
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    assert len(channels[Channel.PUSH].deliveries) == 1
    assert channels[Channel.SMS].deliveries == []
    assert channels[Channel.INBOX].deliveries == []


def test_disabled_channel_is_skipped_even_after_enable(service_factory, channels):
    svc = service_factory()
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(
        Event(event_type="login", user_id="u1", title="hi", event_id="e1")
    )
    svc.run_due()
    svc.disable_channel("u1", Channel.EMAIL)
    result = svc.trigger(
        Event(event_type="login", user_id="u1", title="hi2", event_id="e2")
    )
    assert result.notifications == []
    svc.run_due()
    # still just the first event's single send
    assert len(channels[Channel.EMAIL].deliveries) == 1


def test_duplicate_event_sends_once_per_channel(service_factory, channels):
    svc = service_factory()
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.EMAIL, Channel.SMS, Channel.PUSH}))
    )
    event = Event(event_type="login", user_id="u1", title="hi", event_id="dup1")
    first = svc.trigger(event)
    second = svc.trigger(event)
    third = svc.trigger(event)
    assert first.duplicate is False
    assert second.duplicate is True
    assert third.duplicate is True
    # same notification identities returned
    assert {n.id for n in first.notifications} == {
        n.id for n in second.notifications
    }
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    assert len(channels[Channel.SMS].deliveries) == 1
    assert len(channels[Channel.PUSH].deliveries) == 1


def test_same_event_different_channels_are_independent(service_factory, channels):
    """No cross-channel merging: e1 produces one send on EACH channel."""
    svc = service_factory()
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.EMAIL, Channel.SMS}))
    )
    svc.trigger(
        Event(event_type="login", user_id="u1", title="hi", event_id="e1")
    )
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    assert len(channels[Channel.SMS].deliveries) == 1
    notes = {n.channel: n for n in svc.get_event_notifications("e1")}
    assert notes[Channel.EMAIL].id != notes[Channel.SMS].id


def test_failure_on_one_channel_does_not_block_others(service_factory, channels):
    channels[Channel.EMAIL].script_failure(
        ErrorKind.FATAL, "address bounced"
    )
    svc = service_factory()
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.EMAIL, Channel.SMS, Channel.PUSH}))
    )
    svc.trigger(
        Event(event_type="login", user_id="u1", title="hi", event_id="e1")
    )
    svc.run_due()
    # email dead-lettered, others delivered
    notes = {n.channel: n for n in svc.get_event_notifications("e1")}
    assert notes[Channel.EMAIL].status is NotificationStatus.DEAD_LETTER
    assert notes[Channel.SMS].status is NotificationStatus.DELIVERED
    assert notes[Channel.PUSH].status is NotificationStatus.DELIVERED


def test_fatal_failure_is_not_replayed(service_factory, channels, clock):
    channels[Channel.EMAIL].script_failure(ErrorKind.FATAL, "bad recipient")
    from datetime import timedelta

    from notification_fanout.policy import RetryPolicy

    svc = service_factory(
        retry_policy=RetryPolicy(max_attempts=5, base_delay=timedelta(seconds=1))
    )
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(
        Event(event_type="login", user_id="u1", title="hi", event_id="e1")
    )
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1  # one attempt only
    # advance past all backoff windows: still no replay
    for _ in range(10):
        clock.advance(timedelta(seconds=60))
        svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    n = svc.get_event_notifications("e1")[0]
    assert n.status is NotificationStatus.DEAD_LETTER
    assert "bad recipient" in n.dead_letter_reason

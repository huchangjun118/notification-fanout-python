"""Retry backoff, dead-letter exhaustion and non-retryable failures."""

from __future__ import annotations

from datetime import timedelta

from notification_fanout.models import (
    Channel,
    ErrorKind,
    Event,
    NotificationStatus,
    Subscription,
)
from notification_fanout.policy import RetryPolicy


def test_retryable_failures_backoff_then_succeed(service_factory, channels, clock):
    policy = RetryPolicy(
        max_attempts=4,
        base_delay=timedelta(seconds=1),
        multiplier=2,
        max_delay=timedelta(seconds=10),
    )
    channels[Channel.EMAIL].script_failure(ErrorKind.RETRYABLE, "fail 1")
    channels[Channel.EMAIL].script_failure(ErrorKind.RETRYABLE, "fail 2")
    svc = service_factory(retry_policy=policy)
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    result = svc.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    nid = result.notifications[0].id

    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1
    n = svc.get_notification(nid)
    assert n.status is NotificationStatus.RETRYING
    assert n.attempts == 1
    assert "retry at" in n.history[-1].reason

    # before backoff: still nothing new
    clock.advance(timedelta(seconds=1) - timedelta(milliseconds=1))
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 1

    clock.advance(timedelta(milliseconds=1))  # t=1s -> attempt 2 fails
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 2
    assert svc.get_notification(nid).status is NotificationStatus.RETRYING

    clock.advance(timedelta(seconds=2))  # t=3s -> attempt 3 succeeds
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 3
    n = svc.get_notification(nid)
    assert n.status is NotificationStatus.DELIVERED
    assert n.attempts == 3
    assert n.delivered_at is not None


def test_exhausting_retries_moves_to_dead_letter(service_factory, channels, clock):
    policy = RetryPolicy(
        max_attempts=3,
        base_delay=timedelta(seconds=1),
        multiplier=1,
    )
    for _ in range(5):
        channels[Channel.EMAIL].script_failure(
            ErrorKind.RETRYABLE, "persistent outage"
        )
    svc = service_factory(retry_policy=policy)
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    result = svc.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    nid = result.notifications[0].id

    svc.run_due()
    for _ in range(3):
        clock.advance(timedelta(seconds=1))
        svc.run_due()

    n = svc.get_notification(nid)
    assert n.status is NotificationStatus.DEAD_LETTER
    assert n.attempts == 3
    assert len(channels[Channel.EMAIL].deliveries) == 3
    assert "retries exhausted" in n.dead_letter_reason
    assert "persistent outage" in n.dead_letter_reason
    # listed in the dead-letter queue
    dead = svc.get_dead_letters()
    assert any(d.id == nid for d in dead)

    # further time progression never replays it
    clock.advance(timedelta(hours=1))
    svc.run_due()
    assert len(channels[Channel.EMAIL].deliveries) == 3


def test_fatal_failure_goes_straight_to_dead_letter(service_factory, channels):
    channels[Channel.EMAIL].script_failure(
        ErrorKind.FATAL, "invalid mailbox"
    )
    svc = service_factory()
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    svc.run_due()
    n = svc.get_event_notifications("e1")[0]
    assert n.status is NotificationStatus.DEAD_LETTER
    assert n.attempts == 1
    assert "non-retryable" in n.dead_letter_reason
    assert len(channels[Channel.EMAIL].deliveries) == 1


def test_other_channels_keep_working_while_one_retries(
    service_factory, channels, clock
):
    policy = RetryPolicy(max_attempts=3, base_delay=timedelta(hours=1))
    channels[Channel.EMAIL].script_failure(
        ErrorKind.RETRYABLE, "email slow outage"
    )
    svc = service_factory(retry_policy=policy)
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.EMAIL, Channel.PUSH}))
    )
    svc.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    svc.run_due()
    # push already done even though email is backing off for an hour
    notes = {n.channel: n for n in svc.get_event_notifications("e1")}
    assert notes[Channel.PUSH].status is NotificationStatus.DELIVERED
    assert notes[Channel.EMAIL].status is NotificationStatus.RETRYING
    assert len(channels[Channel.PUSH].deliveries) == 1


def test_explanation_records_full_failure_history(service_factory, channels, clock):
    policy = RetryPolicy(
        max_attempts=2, base_delay=timedelta(seconds=1), multiplier=1
    )
    channels[Channel.EMAIL].script_failure(ErrorKind.RETRYABLE, "boom-1")
    channels[Channel.EMAIL].script_failure(ErrorKind.RETRYABLE, "boom-2")
    svc = service_factory(retry_policy=policy)
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    result = svc.trigger(
        Event(event_type="x", user_id="u1", title="hi", event_id="e1")
    )
    nid = result.notifications[0].id
    svc.run_due()
    clock.advance(timedelta(seconds=1))
    svc.run_due()
    explanation = svc.explain(nid)
    assert "boom-1" in explanation
    assert "boom-2" in explanation
    assert "dead_letter" in explanation
    attempts = svc.store.list_attempts(nid)
    assert len(attempts) == 2
    assert all(row["success"] == 0 for row in attempts)

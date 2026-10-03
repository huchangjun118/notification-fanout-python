"""Final-consistent, unique terminal state under races."""

from __future__ import annotations

import threading

from notification_fanout.models import (
    Channel,
    Notification,
    NotificationStatus,
    StateChange,
)
from notification_fanout.store import NotificationStore
from notification_fanout.models import utc_now


def test_terminal_state_is_immutable_and_unique(tmp_path):
    store = NotificationStore(str(tmp_path / "s.db"))
    now = utc_now()
    n = Notification(
        id="n1",
        event_id="e1",
        user_id="u1",
        channel=Channel.EMAIL,
        urgency=__import__(
            "notification_fanout.models", fromlist=["Urgency"]
        ).Urgency.NORMAL,
        payload_title="t",
        payload_body="",
        status=NotificationStatus.PENDING,
        not_before=now,
        history=[StateChange(NotificationStatus.PENDING, now, "created")],
    )
    assert store.insert_notification(n)

    # Two workers racing to terminate with *different* outcomes.
    outcomes = {"delivered": 0, "dead": 0}
    barrier = threading.Barrier(2)

    def mark_delivered():
        local = store.get("n1")
        barrier.wait()
        local.status = NotificationStatus.DELIVERED
        local.delivered_at = utc_now()
        if store.compare_and_update(local):
            outcomes["delivered"] += 1

    def mark_dead():
        local = store.get("n1")
        barrier.wait()
        local.status = NotificationStatus.DEAD_LETTER
        local.dead_letter_reason = "contender"
        if store.compare_and_update(local):
            outcomes["dead"] += 1

    t1 = threading.Thread(target=mark_delivered)
    t2 = threading.Thread(target=mark_dead)
    t1.start(); t2.start(); t1.join(); t2.join()

    # exactly one terminal writer won
    assert outcomes["delivered"] + outcomes["dead"] == 1
    final = store.get("n1")
    assert final.is_terminal
    # the losing update cannot overwrite the winner afterwards
    final.status = NotificationStatus.PENDING
    assert not store.compare_and_update(final)
    assert store.get("n1").is_terminal
    store.close()


def test_event_terminal_states_are_consistent_across_queries(
    service_factory, channels
):
    """Repeated queries after settling always return the same terminal rows."""
    svc = service_factory()
    svc.set_subscription(
        __import__(
            "notification_fanout.models", fromlist=["Subscription"]
        ).Subscription("u1", frozenset({Channel.EMAIL, Channel.SMS}))
    )
    from notification_fanout.models import Event

    svc.trigger(Event(event_type="x", user_id="u1", title="t", event_id="e1"))
    svc.run_due()
    snapshots = []
    for _ in range(5):
        rows = svc.get_event_notifications("e1")
        snapshots.append((
            sorted((n.channel.value, n.status.value) for n in rows),
            sorted(n.id for n in rows),
        ))
    assert all(s == snapshots[0] for s in snapshots)
    statuses = snapshots[0][0]
    assert all(status == "delivered" for _channel, status in statuses)

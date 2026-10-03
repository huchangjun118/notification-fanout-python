"""Concurrent triggers, serialization and slow-channel isolation."""

from __future__ import annotations

import threading
import time
from datetime import timedelta

from notification_fanout.channels import InProcessChannel
from notification_fanout.config import AtomicConfig, DispatchConfig
from notification_fanout.models import (
    Channel,
    DeliveryResult,
    Event,
    NotificationStatus,
    Subscription,
)
from notification_fanout.service import NotificationService
from notification_fanout.store import NotificationStore
from notification_fanout.clock import SystemClock


def test_concurrent_duplicate_triggers_yield_one_delivery():
    """N threads triggering the same event produce exactly one send/channel."""
    store = NotificationStore(":memory:")
    email = InProcessChannel(kind=Channel.EMAIL)

    def make_svc():
        from notification_fanout.channels import ChannelRegistry

        reg = ChannelRegistry()
        reg.register(email)
        return NotificationService(
            store=store, channels=reg, clock=SystemClock(), auto_start=False
        )

    # subscriptions are held in the in-memory book; share one service but call
    # trigger from many threads to stress the unique constraint.
    svc = make_svc()
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    svc.start()

    barrier = threading.Barrier(8)

    def trigger():
        barrier.wait()
        svc.trigger(
            Event(event_type="x", user_id="u1", title="hi", event_id="race-1")
        )

    threads = [threading.Thread(target=trigger) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert svc.wait_idle(timeout=3)
    assert len(email.deliveries) == 1
    notes = svc.get_event_notifications("race-1")
    assert len(notes) == 1
    assert notes[0].status is NotificationStatus.DELIVERED
    svc.stop()


def test_same_user_channel_sends_are_serial():
    """Concurrent sends for one user+channel never overlap in the channel."""
    overlaps = {"current": 0, "max": 0}
    lock = threading.Lock()

    def slow_handler(notification):
        with lock:
            overlaps["current"] += 1
            overlaps["max"] = max(overlaps["max"], overlaps["current"])
        time.sleep(0.05)
        with lock:
            overlaps["current"] -= 1
        return DeliveryResult(success=True)

    email = InProcessChannel(kind=Channel.EMAIL, handler=slow_handler)
    from notification_fanout.channels import ChannelRegistry

    reg = ChannelRegistry()
    reg.register(email)
    svc = NotificationService(
        store=NotificationStore(":memory:"),
        channels=reg,
        clock=SystemClock(),
    )
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    for i in range(6):
        svc.trigger(
            Event(event_type="x", user_id="u1", title=f"n{i}", event_id=f"e{i}")
        )
    assert svc.wait_idle(timeout=5)
    assert overlaps["max"] == 1
    assert len(email.deliveries) == 6
    svc.stop()


def test_slow_channel_does_not_block_fast_channel():
    """A blocked SMS channel must not delay PUSH/EMAIL delivery."""
    release = threading.Event()
    entered = threading.Event()

    def blocking_handler(_n):
        entered.set()
        release.wait(timeout=2)
        return DeliveryResult(success=True)

    sms = InProcessChannel(kind=Channel.SMS, handler=blocking_handler)
    push = InProcessChannel(kind=Channel.PUSH)
    email = InProcessChannel(kind=Channel.EMAIL)
    from notification_fanout.channels import ChannelRegistry

    reg = ChannelRegistry()
    reg.register(sms)
    reg.register(push)
    reg.register(email)
    svc = NotificationService(
        store=NotificationStore(":memory:"),
        channels=reg,
        clock=SystemClock(),
        config=AtomicConfig(DispatchConfig(max_concurrency=8)),
    )
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.SMS, Channel.PUSH, Channel.EMAIL}))
    )
    svc.trigger(
        Event(event_type="x", user_id="u1", title="blocked", event_id="slow")
    )
    # wait until sms is inside the handler, then fire other channels
    assert entered.wait(timeout=2)
    svc.trigger(
        Event(event_type="x", user_id="u1", title="fast", event_id="fast")
    )
    # fast channels complete while SMS is still blocked
    deadline = time.time() + 2
    while time.time() < deadline:
        notes = {n.channel: n for n in svc.get_event_notifications("fast")}
        if (
            notes.get(Channel.PUSH)
            and notes[Channel.PUSH].status is NotificationStatus.DELIVERED
            and notes.get(Channel.EMAIL)
            and notes[Channel.EMAIL].status is NotificationStatus.DELIVERED
        ):
            break
        time.sleep(0.01)
    notes = {n.channel: n for n in svc.get_event_notifications("fast")}
    assert notes[Channel.PUSH].status is NotificationStatus.DELIVERED
    assert notes[Channel.EMAIL].status is NotificationStatus.DELIVERED
    slow_notes = {n.channel: n for n in svc.get_event_notifications("slow")}
    assert slow_notes[Channel.SMS].status is NotificationStatus.DELIVERING
    release.set()
    assert svc.wait_idle(timeout=3)
    svc.stop()


def test_global_concurrency_limit_is_respected():
    lock = threading.Lock()
    state = {"current": 0, "max": 0}

    def handler(_n):
        with lock:
            state["current"] += 1
            state["max"] = max(state["max"], state["current"])
        time.sleep(0.05)
        with lock:
            state["current"] -= 1
        return DeliveryResult(success=True)

    from notification_fanout.channels import ChannelRegistry

    reg = ChannelRegistry()
    chans = {}
    for ch in (Channel.EMAIL, Channel.SMS, Channel.PUSH, Channel.INBOX):
        c = InProcessChannel(kind=ch, handler=handler)
        chans[ch] = c
        reg.register(c)
    svc = NotificationService(
        store=NotificationStore(":memory:"),
        channels=reg,
        clock=SystemClock(),
        config=AtomicConfig(DispatchConfig(max_concurrency=2)),
    )
    for ch in chans:
        svc.set_subscription(
            Subscription(
                "u1",
                svc.subscriptions.enabled_channels("u1") | frozenset({ch}),
            )
        )
    # multiple events -> 8+ notifications across 4 channels
    for i in range(2):
        svc.trigger(
            Event(event_type="x", user_id="u1", title=f"n{i}", event_id=f"e{i}")
        )
    assert svc.wait_idle(timeout=5)
    assert state["max"] <= 2
    svc.stop()


def test_config_change_only_affects_future_sends():
    lock = threading.Lock()
    state = {"current": 0, "max_first": 0, "max_second": 0, "phase": 0}

    def handler(_n):
        with lock:
            state["current"] += 1
            bucket = "max_first" if state["phase"] == 0 else "max_second"
            state[bucket] = max(state[bucket], state["current"])
        time.sleep(0.05)
        with lock:
            state["current"] -= 1
        return DeliveryResult(success=True)

    from notification_fanout.channels import ChannelRegistry

    reg = ChannelRegistry()
    for ch in (Channel.EMAIL, Channel.SMS):
        reg.register(InProcessChannel(kind=ch, handler=handler))
    svc = NotificationService(
        store=NotificationStore(":memory:"),
        channels=reg,
        clock=SystemClock(),
        config=AtomicConfig(DispatchConfig(max_concurrency=4)),
    )
    svc.set_subscription(
        Subscription("u1", frozenset({Channel.EMAIL, Channel.SMS}))
    )
    # unique users so per-user serialization does not serialize them
    for i in range(4):
        svc.set_subscription(
            Subscription(f"u{i}", frozenset({Channel.EMAIL, Channel.SMS}))
        )
    for i in range(4):
        svc.trigger(
            Event(event_type="x", user_id=f"u{i}", title="a", event_id=f"a{i}")
        )
    # tighten the limit while sends are in flight
    time.sleep(0.01)
    svc.update_config(max_concurrency=1)
    assert svc.wait_idle(timeout=5)
    with lock:
        state["phase"] = 1
    for i in range(4):
        svc.trigger(
            Event(event_type="x", user_id=f"u{i}", title="b", event_id=f"b{i}")
        )
    assert svc.wait_idle(timeout=5)
    # second wave was dispatched under the tightened cap
    assert state["max_second"] <= 1
    svc.stop()

"""Channel send-rate limiting (token bucket) under FakeClock."""

from __future__ import annotations

from datetime import timedelta

from notification_fanout.config import (
    AtomicConfig,
    ChannelRateConfig,
    DispatchConfig,
)
from notification_fanout.models import Channel, Event, Subscription


def test_channel_rate_limit_spaces_sends(service_factory, channels, clock):
    config = AtomicConfig(
        DispatchConfig(
            max_concurrency=8,
            channels={
                Channel.EMAIL: ChannelRateConfig(rate_per_second=2, burst=1)
            },
        )
    )
    svc = service_factory(config=config)
    # refresh bucket from config snapshot
    svc.scheduler.update_rates()
    svc.set_subscription(Subscription("u1", frozenset({Channel.EMAIL})))
    # single user -> per-user serial pump; spacing therefore reflects the
    # channel-wide token bucket deterministically.
    for i in range(3):
        svc.trigger(
            Event(event_type="x", user_id="u1", title="n", event_id=f"e{i}")
        )
    svc.run_due()
    # burst=1: first goes immediately; token bucket sleeps advance the fake
    # clock, so eventually all are sent but spaced at >= 0.5s
    sent_times = sorted(
        n.delivered_at for n in [svc.get_event_notifications(f"e{i}")[0] for i in range(3)]
    )
    assert all(sv.scheduler is not None for sv in [svc])
    assert sent_times[0] < sent_times[1] < sent_times[2]
    assert (sent_times[1] - sent_times[0]) >= timedelta(seconds=0.49)
    assert (sent_times[2] - sent_times[1]) >= timedelta(seconds=0.49)
    assert len(channels[Channel.EMAIL].deliveries) == 3


def test_config_rate_change_takes_effect_without_restart(
    service_factory, channels, clock
):
    config = AtomicConfig(
        DispatchConfig(
            channels={
                Channel.SMS: ChannelRateConfig(rate_per_second=100, burst=100)
            }
        )
    )
    svc = service_factory(config=config)
    svc.scheduler.update_rates()
    svc.set_subscription(Subscription("u1", frozenset({Channel.SMS})))
    svc.trigger(Event(event_type="x", user_id="u1", title="a", event_id="a"))
    svc.run_due()
    first = svc.get_event_notifications("a")[0]
    assert first.delivered_at is not None
    t0 = first.delivered_at

    # tighten to 1/s
    config.replace(
        DispatchConfig(
            channels={Channel.SMS: ChannelRateConfig(rate_per_second=1, burst=1)}
        )
    )
    svc.scheduler.update_rates()
    # same user -> per-user serial pump, so the second send can only start
    # after the first finished and its timestamp was recorded.
    svc.trigger(Event(event_type="x", user_id="u1", title="b", event_id="b"))
    svc.trigger(Event(event_type="x", user_id="u1", title="c", event_id="c"))
    svc.run_due()
    tb = svc.get_event_notifications("b")[0].delivered_at
    tc = svc.get_event_notifications("c")[0].delivered_at
    assert (tc - tb) >= timedelta(seconds=0.99)

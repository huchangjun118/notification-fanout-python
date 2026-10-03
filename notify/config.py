"""Runtime configuration. Changes only affect subsequent dispatches."""
from __future__ import annotations

import threading
from dataclasses import dataclass, fields


@dataclass
class Config:
    db_path: str = "notifications.db"
    tick_interval: float = 0.2          # scheduler scan interval (seconds)
    dispatch_workers: int = 16         # max concurrent dispatch workers
    batch_size: int = 100              # max notifications claimed per tick
    default_max_attempts: int = 5
    retry_base_delay: float = 0.5      # initial backoff (seconds)
    retry_max_delay: float = 30.0      # backoff cap (seconds)
    lease_timeout: float = 30.0        # stale SENDING leases reset after this
    rate_per_second: float = 2.0       # per user-channel token bucket refill
    rate_burst: int = 1                # per user-channel burst capacity
    freq_cap: int = 20                 # max deliveries per window per user-channel
    freq_window_seconds: float = 3600.0
    freq_strategy: str = "defer"       # "defer" (to next slot) or "drop"
    quiet_start_hour: int = 22         # default quiet window start (local hour, inclusive)
    quiet_end_hour: int = 8            # default quiet window end (local hour, exclusive)


class ConfigHolder:
    """Thread-safe config holder; readers always see the latest committed config."""

    def __init__(self, config: Config | None = None) -> None:
        self._config = config or Config()
        self._lock = threading.Lock()

    def get(self) -> Config:
        with self._lock:
            return self._config

    def update(self, **kwargs: object) -> Config:
        with self._lock:
            current = self._config
            valid = {f.name: f for f in fields(current)}
            unknown = set(kwargs) - set(valid)
            if unknown:
                raise ValueError(f"unknown config keys: {sorted(unknown)}")
            new_values = {**{f.name: getattr(current, f.name) for f in fields(current)}, **kwargs}
            if new_values["freq_strategy"] not in ("defer", "drop"):
                raise ValueError("freq_strategy must be 'defer' or 'drop'")
            if not (0 <= new_values["quiet_start_hour"] <= 23 and 0 <= new_values["quiet_end_hour"] <= 23):
                raise ValueError("quiet hours must be in 0..23")
            if new_values["dispatch_workers"] < 1 or new_values["batch_size"] < 1:
                raise ValueError("dispatch_workers and batch_size must be >= 1")
            self._config = Config(**new_values)
            return self._config

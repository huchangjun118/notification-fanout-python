"""Application service: event ingestion, preferences, and status queries."""
from __future__ import annotations

import time
from typing import Any

from .channels import Channel
from .config import ConfigHolder
from .models import IngestEvent, Notification, UserPreferences
from .repository import Repository


class NotificationService:
    def __init__(
        self,
        repo: Repository,
        channels: dict[str, Channel],
        config_holder: ConfigHolder,
    ) -> None:
        self.repo = repo
        self.channels = channels
        self.config_holder = config_holder

    def _default_preferences(self, user_id: str) -> UserPreferences:
        return UserPreferences(
            user_id=user_id,
            channels={name: True for name in self.channels},
        )

    def ingest(self, event: IngestEvent) -> list[Notification]:
        now = time.time()
        cfg = self.config_holder.get()
        prefs = self.repo.get_preferences(event.user_id)

        if event.channels is not None:
            requested = list(event.channels)
        else:
            # Fan out to every known channel; disabled ones become skipped records.
            requested = list(self.channels)

        scheduled_at = event.scheduled_at if event.scheduled_at is not None else now
        max_attempts = event.max_attempts or cfg.default_max_attempts

        results: list[Notification] = []
        for channel in requested:
            if channel not in self.channels:
                # Unknown target: record a terminal skip so the result is explainable.
                n = self.repo.create_notification(
                    event_id=event.event_id,
                    user_id=event.user_id,
                    channel=channel,
                    payload=event.payload,
                    scheduled_at=scheduled_at,
                    urgent=event.urgent,
                    max_attempts=max_attempts,
                    now=now,
                )
                if n.status.value == "pending":
                    self.repo.mark_skipped(
                        n.id, f"channel {channel!r} is not registered"
                    )
                    n = self.repo.get_notification(n.id) or n
                results.append(n)
                continue

            if prefs is not None and not prefs.channels.get(channel, True):
                n = self.repo.create_notification(
                    event_id=event.event_id,
                    user_id=event.user_id,
                    channel=channel,
                    payload=event.payload,
                    scheduled_at=scheduled_at,
                    urgent=event.urgent,
                    max_attempts=max_attempts,
                    now=now,
                )
                if n.status.value == "pending":
                    self.repo.mark_skipped(
                        n.id,
                        f"channel {channel!r} disabled by user preference",
                    )
                    n = self.repo.get_notification(n.id) or n
                results.append(n)
                continue

            n = self.repo.create_notification(
                event_id=event.event_id,
                user_id=event.user_id,
                channel=channel,
                payload=event.payload,
                scheduled_at=scheduled_at,
                urgent=event.urgent,
                max_attempts=max_attempts,
                now=now,
            )
            results.append(n)
        return results

    def get_notification(self, notification_id: int) -> Notification | None:
        return self.repo.get_notification(notification_id)

    def get_event_notifications(self, event_id: str) -> list[Notification]:
        return self.repo.get_by_event(event_id)

    def get_preferences(self, user_id: str) -> UserPreferences:
        prefs = self.repo.get_preferences(user_id)
        if prefs is None:
            return self._default_preferences(user_id)
        return prefs

    def update_preferences(
        self,
        user_id: str,
        *,
        channels: dict[str, bool] | None = None,
        quiet_start_hour: int | None = None,
        quiet_end_hour: int | None = None,
    ) -> UserPreferences:
        prefs = self.repo.get_preferences(user_id) or self._default_preferences(user_id)
        if channels is not None:
            prefs.channels.update(channels)
        if quiet_start_hour is not None:
            prefs.quiet_start_hour = quiet_start_hour
        if quiet_end_hour is not None:
            prefs.quiet_end_hour = quiet_end_hour
        return self.repo.upsert_preferences(prefs)

    def list_dead_letters(self, limit: int = 100) -> list[Notification]:
        return self.repo.list_dead_letters(limit)

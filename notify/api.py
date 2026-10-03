"""FastAPI HTTP API for the notification service."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .config import ConfigHolder
from .models import IngestEvent, Notification, UserPreferences
from .service import NotificationService


class _ForbidExtra(BaseModel):
    model_config = {"extra": "forbid"}


class EventRequest(_ForbidExtra):
    event_id: str
    user_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
    channels: list[str] | None = None
    scheduled_at: float | None = None
    urgent: bool = False
    max_attempts: int | None = None


class PreferencesRequest(_ForbidExtra):
    channels: dict[str, bool] | None = None
    quiet_start_hour: int | None = None
    quiet_end_hour: int | None = None


class ConfigRequest(_ForbidExtra):
    db_path: str | None = None
    tick_interval: float | None = None
    dispatch_workers: int | None = None
    batch_size: int | None = None
    default_max_attempts: int | None = None
    retry_base_delay: float | None = None
    retry_max_delay: float | None = None
    lease_timeout: float | None = None
    rate_per_second: float | None = None
    rate_burst: int | None = None
    freq_cap: int | None = None
    freq_window_seconds: float | None = None
    freq_strategy: str | None = None
    quiet_start_hour: int | None = None
    quiet_end_hour: int | None = None


def _notification_dict(n: Notification) -> dict[str, Any]:
    return {
        "id": n.id,
        "event_id": n.event_id,
        "user_id": n.user_id,
        "channel": n.channel,
        "payload": n.payload,
        "status": n.status.value,
        "scheduled_at": n.scheduled_at,
        "attempts": n.attempts,
        "max_attempts": n.max_attempts,
        "urgent": n.urgent,
        "idempotency_key": n.idempotency_key,
        "last_error": n.last_error,
        "reason": n.reason,
        "created_at": n.created_at,
        "updated_at": n.updated_at,
    }


def _preferences_dict(p: UserPreferences) -> dict[str, Any]:
    return {
        "user_id": p.user_id,
        "channels": p.channels,
        "quiet_start_hour": p.quiet_start_hour,
        "quiet_end_hour": p.quiet_end_hour,
    }


def create_router(service: NotificationService, config_holder: ConfigHolder) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.post("/events")
    def ingest(req: EventRequest) -> dict[str, Any]:
        notifications = service.ingest(
            IngestEvent(
                event_id=req.event_id,
                user_id=req.user_id,
                payload=req.payload,
                channels=req.channels,
                scheduled_at=req.scheduled_at,
                urgent=req.urgent,
                max_attempts=req.max_attempts,
            )
        )
        return {
            "event_id": req.event_id,
            "notifications": [_notification_dict(n) for n in notifications],
        }

    @router.get("/events/{event_id}")
    def event_status(event_id: str) -> dict[str, Any]:
        notifications = service.get_event_notifications(event_id)
        return {
            "event_id": event_id,
            "notifications": [_notification_dict(n) for n in notifications],
        }

    @router.get("/notifications/{notification_id}")
    def notification_status(notification_id: int) -> dict[str, Any]:
        n = service.get_notification(notification_id)
        if n is None:
            raise HTTPException(status_code=404, detail="notification not found")
        return _notification_dict(n)

    @router.get("/notifications/{notification_id}/history")
    def notification_history(notification_id: int) -> dict[str, Any]:
        n = service.get_notification(notification_id)
        if n is None:
            raise HTTPException(status_code=404, detail="notification not found")
        history = service.repo.list_history(notification_id)
        return {
            "notification_id": notification_id,
            "history": [
                {
                    "from_status": h.from_status.value if h.from_status else None,
                    "to_status": h.to_status.value,
                    "reason": h.reason,
                    "ts": h.ts,
                }
                for h in history
            ],
        }

    @router.get("/users/{user_id}/preferences")
    def get_preferences(user_id: str) -> dict[str, Any]:
        return _preferences_dict(service.get_preferences(user_id))

    @router.put("/users/{user_id}/preferences")
    def update_preferences(user_id: str, req: PreferencesRequest) -> dict[str, Any]:
        prefs = service.update_preferences(
            user_id,
            channels=req.channels,
            quiet_start_hour=req.quiet_start_hour,
            quiet_end_hour=req.quiet_end_hour,
        )
        return _preferences_dict(prefs)

    @router.get("/dead-letters")
    def dead_letters(limit: int = 100) -> dict[str, Any]:
        items = service.list_dead_letters(limit)
        return {"dead_letters": [_notification_dict(n) for n in items]}

    @router.get("/admin/config")
    def get_config() -> dict[str, Any]:
        return vars(config_holder.get())

    @router.put("/admin/config")
    def update_config(req: ConfigRequest) -> dict[str, Any]:
        updates = {k: v for k, v in req.model_dump().items() if v is not None}
        try:
            cfg = config_holder.update(**updates)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return vars(cfg)

    return router

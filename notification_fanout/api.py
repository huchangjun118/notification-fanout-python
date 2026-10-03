"""FastAPI HTTP surface for the notification service.

The app is constructed with an injectable :class:`NotificationService`, so the
HTTP layer can be tested in-process (TestClient) with fake channels/clock; the
delivery targets themselves remain in-process or loopback-only.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .models import (
    Channel,
    Event,
    Subscription,
    Urgency,
)
from .service import NotificationService


# ---------------------------------------------------------------- request models
class TriggerRequest(BaseModel):
    event_type: str
    user_id: str
    title: str
    body: str = ""
    urgency: Urgency = Urgency.NORMAL
    not_before: datetime | None = None
    event_id: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class SubscriptionRequest(BaseModel):
    enabled_channels: list[Channel]


class ConfigRequest(BaseModel):
    max_concurrency: int | None = None
    default_rate_per_second: float | None = None
    default_burst: int | None = None


# -------------------------------------------------------------- serialisation
def _notification_to_dict(n) -> dict[str, Any]:
    return {
        "id": n.id,
        "event_id": n.event_id,
        "user_id": n.user_id,
        "channel": n.channel.value,
        "urgency": n.urgency.value,
        "status": n.status.value,
        "terminal": n.is_terminal,
        "not_before": n.not_before.isoformat(),
        "attempts": n.attempts,
        "max_attempts": n.max_attempts,
        "created_at": n.created_at.isoformat(),
        "updated_at": n.updated_at.isoformat(),
        "delivered_at": n.delivered_at.isoformat() if n.delivered_at else None,
        "dead_letter_reason": n.dead_letter_reason,
        "history": [
            {
                "status": c.status.value,
                "at": c.at.isoformat(),
                "reason": c.reason,
                "attempt": c.attempt,
            }
            for c in n.history
        ],
    }


def create_app(service: NotificationService) -> FastAPI:
    app = FastAPI(title="notification-fanout", version="0.1.0")
    app.state.service = service

    @app.post("/events")
    def trigger_event(req: TriggerRequest) -> dict[str, Any]:
        event = Event(
            event_type=req.event_type,
            user_id=req.user_id,
            title=req.title,
            body=req.body,
            urgency=req.urgency,
            not_before=req.not_before,
            event_id=req.event_id or _auto_event_id(),
            metadata=dict(req.metadata),
        )
        result = service.trigger(event)
        return {
            "event_id": result.event_id,
            "duplicate": result.duplicate,
            "scheduled": [_notification_to_dict(n) for n in result.notifications],
            "skipped_disabled": [c.value for c in result.skipped_disabled],
            "skipped_unsupported": [c.value for c in result.skipped_unsupported],
        }

    @app.get("/notifications/{notification_id}")
    def get_notification(notification_id: str) -> dict[str, Any]:
        n = service.get_notification(notification_id)
        if n is None:
            raise HTTPException(status_code=404, detail="notification not found")
        return _notification_to_dict(n)

    @app.get("/notifications/{notification_id}/explain")
    def explain_notification(notification_id: str) -> dict[str, str]:
        if service.get_notification(notification_id) is None:
            raise HTTPException(status_code=404, detail="notification not found")
        return {"explanation": service.explain(notification_id)}

    @app.get("/events/{event_id}")
    def get_event(event_id: str) -> dict[str, Any]:
        notifications = service.get_event_notifications(event_id)
        if not notifications:
            raise HTTPException(status_code=404, detail="event not found")
        statuses = {n.status.value for n in notifications}
        terminal = all(n.is_terminal for n in notifications)
        # Unique aggregate terminal state only when all channels agree;
        # otherwise per-channel states remain authoritative.
        aggregate = (
            next(iter(statuses)) if terminal and len(statuses) == 1 else None
        )
        return {
            "event_id": event_id,
            "terminal": terminal,
            "aggregate_status": aggregate,
            "notifications": [_notification_to_dict(n) for n in notifications],
        }

    @app.get("/dead-letters")
    def list_dead_letters(limit: int = 100) -> dict[str, Any]:
        items = service.get_dead_letters(limit=limit)
        return {
            "count": len(items),
            "dead_letters": [
                {
                    **_notification_to_dict(n),
                    "dead_letter_reason": n.dead_letter_reason,
                }
                for n in items
            ],
        }

    @app.put("/subscriptions/{user_id}")
    def put_subscription(user_id: str, req: SubscriptionRequest) -> dict[str, Any]:
        service.set_subscription(
            Subscription(user_id=user_id, enabled_channels=frozenset(req.enabled_channels))
        )
        return {
            "user_id": user_id,
            "enabled_channels": [c.value for c in req.enabled_channels],
        }

    @app.get("/config")
    def get_config() -> dict[str, Any]:
        cfg = service.config.snapshot()
        return {
            "max_concurrency": cfg.max_concurrency,
            "default_rate_per_second": cfg.default_rate_per_second,
            "default_burst": cfg.default_burst,
            "channels": {
                ch.value: {
                    "rate_per_second": rc.rate_per_second,
                    "burst": rc.burst,
                }
                for ch, rc in cfg.channels.items()
            },
        }

    @app.patch("/config")
    def patch_config(req: ConfigRequest) -> dict[str, Any]:
        changes = {k: v for k, v in req.model_dump().items() if v is not None}
        if changes:
            service.update_config(**changes)
        return get_config()

    return app


def _auto_event_id() -> str:
    from .models import new_id

    return new_id("evt")

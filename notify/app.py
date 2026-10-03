"""FastAPI application factory: wires repository, channels, service, scheduler."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from .api import create_router
from .channels import Channel, InMemoryChannel
from .config import Config, ConfigHolder
from .repository import Repository
from .scheduler import Scheduler
from .service import NotificationService


def default_channels() -> dict[str, Channel]:
    """In-process channels used when no external targets are configured."""
    return {
        name: InMemoryChannel(name)
        for name in ("email", "sms", "push", "webhook")
    }


def create_app(
    config: Config | None = None,
    channels: dict[str, Channel] | None = None,
) -> FastAPI:
    config_holder = ConfigHolder(config)
    cfg = config_holder.get()
    repo = Repository(cfg.db_path)
    channel_map: dict[str, Channel] = dict(channels) if channels is not None else default_channels()
    service = NotificationService(repo, channel_map, config_holder)
    scheduler = Scheduler(repo, channel_map, config_holder)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        scheduler.start()
        try:
            yield
        finally:
            scheduler.stop()
            repo.close()

    app = FastAPI(title="notification-fanout", lifespan=lifespan)
    app.include_router(create_router(service, config_holder))

    # Expose internals for tests / operational tooling.
    app.state.repo = repo
    app.state.channels = channel_map
    app.state.service = service
    app.state.scheduler = scheduler
    app.state.config_holder = config_holder
    return app

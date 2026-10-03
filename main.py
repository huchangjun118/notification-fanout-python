"""Development entrypoint: starts the HTTP API with in-process channels.

Delivery targets are in-process queues (no external services/credentials).
Run with::

    uv run python main.py [--db path/to/notifications.db]
"""

from __future__ import annotations

import argparse

import uvicorn

from notification_fanout.api import create_app
from notification_fanout.service import NotificationService
from notification_fanout.store import NotificationStore


def build_service(db_path: str = ":memory:") -> NotificationService:
    store = NotificationStore(db_path)
    return NotificationService(store=store)


app = create_app(build_service(":memory:"))


def main() -> None:
    parser = argparse.ArgumentParser(description="notification-fanout server")
    parser.add_argument("--db", default=":memory:", help="SQLite path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    application = create_app(build_service(args.db))
    uvicorn.run(application, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

from __future__ import annotations

import asyncio
import os
from typing import Any

from fastapi import FastAPI

from universal_runtime.adapters.kafka import AioKafkaRuntimeEventSubscriber

_TERMINAL_TYPES = {
    "run.completed",
    "run.failed",
    "run.cancelled",
    "run.timeout",
    "run.interrupted",
}


def attach_runtime_event_fanout(app: FastAPI) -> FastAPI:
    """Feed each Gateway replica's live SSE queues from Kafka.

    Framework state/history stays in the adapter's persistence provider. This
    bridge only transports transient live Runtime events and stores no duplicate
    event journal in PostgreSQL.
    """

    @app.on_event("startup")
    async def start_event_fanout() -> None:
        bootstrap_servers = os.environ.get("UR_KAFKA_BOOTSTRAP_SERVERS")
        if not bootstrap_servers:
            return
        subscriber = AioKafkaRuntimeEventSubscriber(
            bootstrap_servers=bootstrap_servers,
            prefix=os.environ.get("UR_TOPIC_PREFIX", "rt"),
            environment=os.environ.get("UR_KAFKA_ENVIRONMENT", "local"),
            application_id=os.environ.get("UR_APPLICATION_ID", "default"),
            gateway_instance_id=os.environ.get("UR_INSTANCE_ID", "gateway"),
        )

        async def handle(event: Any) -> None:
            run_id = event.identity.run_id
            subscribers = list(app.state.run_queues.get(run_id, ()))
            for queue in subscribers:
                await queue.put(event)
            if event.type in _TERMINAL_TYPES:
                for queue in app.state.run_queues.pop(run_id, ()):
                    await queue.put(None)

        app.state.runtime_event_subscriber = subscriber
        app.state.runtime_event_task = asyncio.create_task(subscriber.run(handle))

    @app.on_event("shutdown")
    async def stop_event_fanout() -> None:
        task = getattr(app.state, "runtime_event_task", None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        subscriber = getattr(app.state, "runtime_event_subscriber", None)
        if subscriber is not None:
            await subscriber.close()

    return app

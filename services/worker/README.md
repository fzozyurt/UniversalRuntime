# Worker Service

Responsibilities:

- load application config and graph,
- register capabilities and available slots,
- execute leases with bounded async concurrency,
- emit ordered runtime events,
- use managed LangGraph persistence,
- host optional FastAPI custom application surface,
- graceful drain and shutdown.

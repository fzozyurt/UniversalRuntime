# Target Repository Structure

```text
UniversalRuntime/
├── AGENTS.md
├── README.md
├── runtime.example.yaml
├── pyproject.toml
├── Makefile
├── contracts/
│   ├── openapi/
│   ├── proto/runtime/v1/
│   ├── config/
│   ├── events/
│   ├── kafka/
│   └── database/
├── migrations/
│   ├── env.py
│   └── versions/
├── src/universal_runtime/
│   ├── domain/
│   ├── application/
│   │   └── migration_coordination.py
│   ├── ports/
│   ├── adapters/
│   │   ├── langgraph/
│   │   ├── fastapi/
│   │   │   ├── alembic/
│   │   │   └── router_registry.py
│   │   ├── memory/
│   │   ├── postgres/
│   │   └── kafka/
│   ├── services/
│   │   ├── gateway/
│   │   │   ├── app.py
│   │   │   ├── compat_app.py
│   │   │   ├── worker_control.py
│   │   │   ├── event_fanout.py
│   │   │   └── routes/
│   │   ├── worker/
│   │   │   ├── main.py
│   │   │   ├── registration.py
│   │   │   ├── migrations.py
│   │   │   └── execution.py
│   │   └── all/
│   ├── transport/
│   ├── telemetry/
│   └── bootstrap/
├── services/
│   ├── gateway/
│   └── worker/
├── deployment/
│   ├── compose/
│   └── kubernetes/
├── examples/
│   └── phase1-agent/
│       └── src/phase1_agent/http/
│           └── <route-group>/
│               ├── routes.py
│               └── schema.py
├── tests/
│   ├── contract/
│   ├── compatibility/
│   ├── integration/
│   ├── e2e/
│   └── fixtures/
├── skills/
├── prompts/
├── scripts/
└── docs/
```

## Ownership rules

- contracts are language-neutral and reviewed before implementation changes,
- Gateway and Worker are the only distributed Runtime services,
- Kafka consumer groups replace a dedicated Dispatcher,
- framework persistence remains adapter-owned,
- custom FastAPI runs in an application deployment rather than inside Gateway,
- Runtime owns Alembic environments; applications own revision files,
- each FastAPI route group contains `routes.py` and `schema.py`,
- adapter-specific dependencies remain optional extras,
- generated code is excluded from hand-written source directories.

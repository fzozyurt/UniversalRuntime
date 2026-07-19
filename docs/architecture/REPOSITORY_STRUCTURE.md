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
├── src/universal_runtime/
│   ├── domain/
│   ├── application/
│   ├── ports/
│   ├── adapters/
│   │   ├── langgraph/
│   │   ├── fastapi/
│   │   ├── memory/
│   │   ├── postgres/
│   │   └── kafka/
│   ├── transport/
│   │   ├── http/
│   │   └── grpc/
│   ├── configuration/
│   ├── telemetry/
│   └── bootstrap/
├── services/
│   ├── gateway/
│   ├── dispatcher/
│   ├── worker/
│   └── event_projector/
├── deployment/
│   ├── docker/
│   ├── kubernetes/
│   └── helm/
├── examples/
├── tests/
│   ├── contract/
│   ├── compatibility/
│   ├── integration/
│   └── e2e/
├── skills/
├── prompts/
├── scripts/
└── docs/
```

## Ownership rules

- contracts are language-neutral and reviewed before implementation changes,
- core SDK is one package but internally modular,
- services are composition roots over shared application/domain contracts,
- adapter-specific dependencies are optional extras,
- generated code is excluded from hand-written source directories.

# Blueprint Manifest

This archive contains:

- root engineering contract (`AGENTS.md`),
- 14 implementation skills,
- 16 ordered implementation prompts including ASAITK DeepAgent acceptance,
- OpenAPI 3.1 seed contract,
- protobuf execution and worker-control contracts,
- JSON Schema config and runtime-event contracts,
- Kafka, PostgreSQL and A2A mapping contracts,
- repository and Phase 1 architecture standards,
- Docker/HAProxy/Kubernetes deployment blueprint,
- Python domain/port reference interfaces,
- CI, quality, architecture and contract validation scaffolding.

Validation performed before packaging:

- JSON schemas are valid Draft 2020-12 schemas,
- `runtime.example.yaml` validates against the config schema,
- OpenAPI YAML parses and contains paths,
- protobuf files compile with `grpcio-tools`,
- Python reference files compile,
- architecture guard passes.

This is a contract and implementation blueprint, not a claim that Phase 1 runtime implementation is complete.

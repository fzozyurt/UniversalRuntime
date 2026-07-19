# Local and Docker Test Plan

## Local InMemory

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev,langgraph,fastapi,a2a]'
cp runtime.example.yaml runtime.yaml
UR_PROFILE=local UR_MODE=standalone runtime-launcher
pytest tests/e2e/test_local_standalone.py -q
```

No PostgreSQL, Kafka or Docker is required for this profile.

## Docker topology

The checked-in Compose topology is under `deployment/compose/`. It uses the
already available, digest-pinned Python, PostgreSQL and Kafka images; set a
password explicitly before starting it:

```bash
export POSTGRES_PASSWORD='local-only-change-me'
docker compose -f deployment/compose/docker-compose.yml up --build -d
```

The platform image is built from `container/Dockerfile`; user graph code belongs
in an application image based on `container/Dockerfile.agent-base`. The
deterministic `examples/phase1-agent` app exercises both a compiled LangGraph
and a LangChain `create_agent` with a model-free tool call.

Target Compose topology:

- HAProxy
- Gateway x2
- Dispatcher x2
- Worker x2
- PostgreSQL
- Kafka-compatible broker
- optional OTel Collector

Validation:

1. Create application config through Gateway.
2. Start one interactive chat run and multiple batch runs.
3. Verify interactive work is selected first.
4. Kill Worker A during execution.
5. Verify retry attempt is created and state resumes where supported.
6. Send traffic through HAProxy and confirm both Gateway replicas respond.
7. Verify state/history through the official LangGraph SDK.
8. Mount a sample FastAPI app and call it through the Gateway prefix.
9. Read Agent Card and call A2A stream endpoint.

## External integration markers

Use pytest markers:

- `postgres`
- `kafka`
- `docker`
- `a2a`
- `compatibility`

External tests may skip only when the required DSN/service is explicitly unavailable. CI integration stages must provide the services and allow no skips.

# MVP Deployment Guide: All Mode, Kubernetes/Helm ve Observability

## Amaç

UniversalRuntime MVP production profilinde PostgreSQL checkpoint/store, Kafka command queue, gRPC Worker ve LangGraph SDK uyumlu Gateway kullanır. `local` profil InMemory içindir; production'da InMemory persistence kullanılmaz.

## Topology

`gateway`: public HTTP/SSE ve SDK; assistant/thread/run/state/history API.
`dispatcher`: Kafka'dan command alır, Worker gRPC stream'ini açar, event/status'u Postgres'e yazar.
`worker`: graph entrypoint'lerini import eder; LangGraph, LangChain create_agent ve Deep Agents çalıştırır.
`all`: Gateway + Dispatcher + Worker aynı process/pod; PostgreSQL, Kafka ve gRPC yine kullanılır. InMemory değildir.

Production split:

```text
public -> Gateway Service -> Gateway x2 -> Kafka -> Dispatcher x2 -> Worker x2+ (gRPC) -> PostgreSQL
```

## All mode

Tek graph:

```yaml
UR_MODE: all
UR_PROFILE: production
UR_APPLICATION_ID: phase1_agent
UR_APPLICATION_ENTRYPOINT: phase1_agent.graph:build_graph
UR_DATABASE_URL: postgresql+psycopg://runtime:secret@postgres:5432/runtime
UR_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
UR_GATEWAY_PORT: "8080"
UR_GRPC_PORT: "9090"
```

İki graph aynı application image içinde:

```yaml
UR_MODE: all
UR_PROFILE: production
UR_APPLICATION_ID: phase1_agent
UR_WORKSPACE_ID: default
UR_WORKSPACE_KEY: default
UR_APPLICATION_ENTRYPOINTS: phase1_agent.graph:build_graph,phase1_agent.deep_agent:build_deep_agent
```

Gateway aynı workspace altında `LangGraph` ve `phase1-deep-agent` assistant'larını register eder. Assistant ID pod ID değildir. All mode küçük MVP/smoke test içindir; bağımsız scale için split topology kullanın.

## Docker Compose

```powershell
$env:POSTGRES_PASSWORD = "local-only-change-me"
docker compose -f deployment/compose/docker-compose.yml up --build -d
docker compose -f deployment/compose/docker-compose.yml ps
```

Compact:

```powershell
$env:POSTGRES_PASSWORD = "local-only-change-me"
docker compose -f deployment/compose/docker-compose.yml --profile compact up --build -d all postgres kafka
```

## Environment

| Değişken | Örnek | Açıklama |
|---|---|---|
| `UR_MODE` | `gateway` | `all`, `gateway`, `dispatcher`, `worker`, `projector`, `migrate` |
| `UR_PROFILE` | `production` | `local` InMemory; `production` managed services |
| `UR_INSTANCE_ID` | `worker-1` | Process/pod kimliği; assistant değildir |
| `UR_APPLICATION_ID` | `phase1_agent` | Application ve checkpoint schema key |
| `UR_WORKSPACE_ID` | `default` | Auto-register metadata workspace |
| `UR_WORKSPACE_KEY` | `default` | Framework schema key |
| `UR_APPLICATION_ENTRYPOINT` | `module:factory` | Tek graph |
| `UR_APPLICATION_ENTRYPOINTS` | `module:a,module:b` | Çoklu graph |
| `UR_DATABASE_URL` | `postgresql+psycopg://...` | PostgreSQL DSN |
| `UR_DB_POOL_SIZE` | `30` | Gateway pool |
| `UR_DB_MAX_OVERFLOW` | `10` | Gateway overflow |
| `UR_WORKER_DB_POOL_SIZE` | `30` | Worker pool |
| `UR_KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap |
| `UR_TOPIC_PREFIX` | `rt` | Topic prefix |
| `UR_KAFKA_ENVIRONMENT` | `production` | Topic environment |
| `UR_GATEWAY_HOST` | `0.0.0.0` | HTTP bind |
| `UR_GATEWAY_PORT` | `8080` | HTTP port |
| `UR_GRPC_HOST` | `0.0.0.0` | gRPC bind |
| `UR_GRPC_PORT` | `9090` | Worker gRPC port |
| `UR_WORKER_TARGETS` | `worker-1:9090,worker-2:9090` | Dispatcher gRPC targets |
| `UR_GATEWAY_REGISTER_URL` | `http://gateway:8080/internal/workers/register` | Worker self-register |
| `UR_WORKER_ADVERTISE_TARGET` | `worker-1:9090` | Advertised gRPC target |
| `UR_WORKER_MAX_CONCURRENCY` | `8` | Worker semaphore |
| `UR_WORKER_DRAIN_TIMEOUT_SECONDS` | `30` | Graceful drain |

Connection hesabı: `pod_count * (pool_size + max_overflow)`. PostgreSQL max_connections buna göre ayarlanmalıdır.

## Persistence ve migration

Startup sırası: platform migration -> LangGraph native `setup()` -> application migration -> Gateway/Worker rollout.

Beklenen framework tabloları:

```text
rt_s_<workspace>_<application>_<environment>.checkpoints
rt_s_<workspace>_<application>_<environment>.checkpoint_blobs
rt_s_<workspace>_<application>_<environment>.checkpoint_writes
rt_s_<workspace>_<application>_<environment>.checkpoint_migrations
rt_s_<workspace>_<application>_<environment>.store
rt_s_<workspace>_<application>_<environment>.store_migrations
```

Deep Agent factory persistence injection kabul etmelidir:

```python
def build_deep_agent(checkpointer=None, store=None):
    return create_deep_agent(model=model, tools=tools, checkpointer=checkpointer, store=store)
```

Compiled singleton production managed persistence için kullanılmamalıdır.

## Kubernetes/Helm

Repository'de tam Helm chart yerine deployment contract ve ConfigMap örneği bulunur. Chart şu kaynakları üretmelidir: Gateway Deployment/Service, Dispatcher Deployment, Worker Deployment/internal gRPC Service, Projector, ConfigMap, Secret reference, migration Job, ServiceAccount, PDB ve NetworkPolicy.

Örnek `values-production.yaml`:

```yaml
image:
  repository: ghcr.io/example/universal-runtime
  tag: mvp
application:
  id: phase1_agent
  workspaceId: default
  entrypoints:
    - phase1_agent.graph:build_graph
    - phase1_agent.deep_agent:build_deep_agent
runtime:
  profile: production
  topicPrefix: rt
  kafkaEnvironment: production
  workerMaxConcurrency: 8
  dbPoolSize: 30
  dbMaxOverflow: 10
gateway: { replicas: 2, serviceType: ClusterIP }
dispatcher: { replicas: 2 }
worker: { replicas: 2, grpcPort: 9090 }
postgres: { existingSecret: runtime-postgres }
kafka: { bootstrapServers: kafka-bootstrap:9092 }
migration: { enabled: true }
```

Secret ConfigMap'e yazılmaz:

```yaml
- name: UR_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: runtime-postgres
      key: database-url
```

Deploy:

```bash
helm upgrade --install universal-runtime ./deployment/helm -n universal-runtime --create-namespace -f values-production.yaml
kubectl rollout status deployment/universal-runtime-gateway -n universal-runtime
kubectl rollout status deployment/universal-runtime-worker -n universal-runtime
```

Chart yoksa aynı env'leri `deployment/kubernetes/configmap.yaml` ve Deployment manifestlerine uygulayın. Worker ve Dispatcher public expose edilmez.

## OTel/OpenLIT/Instana

OTel-only önerisi:

```yaml
UR_OBSERVABILITY_ENABLED: "true"
UR_OPENLIT_ENABLED: "false"
OTEL_SERVICE_NAME: universal-runtime-gateway
OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4317
OTEL_EXPORTER_OTLP_PROTOCOL: grpc
OTEL_TRACES_EXPORTER: otlp
OTEL_METRICS_EXPORTER: otlp
OTEL_LOGS_EXPORTER: none
UR_OBSERVABILITY_CONTENT_CAPTURE: metadata
```

OpenLIT kullanacaksan `UR_OPENLIT_ENABLED=true` ve `OPENLIT_ENABLED=true` yap; OpenLIT aktif OTel provider üzerine bağlanır, ikinci provider kurmamalıdır.

Python `PYTHONPATH` sadece import path'tir; OTel context propagation veya checkpoint kimliğini değiştirmez. Source path span attribute olarak runtime tarafından eklenmiyor. Instana'nın çalışmaması için runtime pod'larına Instana init injection, `sitecustomize`, `instana` package veya Instana injection annotation eklemeyin; `INSTANA_AGENT_HOST` gibi değişkenleri vermeyin. OTel zero-code launcher yalnızca `UR_OBSERVABILITY_ENABLED=true` iken çalışır.

Container entrypoint allow-list'inde `all` bulunmalıdır. OTel ve Instana'yı aynı process'e otomatik olarak iki farklı global instrumentation provider şeklinde yüklemeyin.

## Smoke test

```python
import asyncio
from langgraph_sdk import get_client

async def main():
    client = get_client(url="http://runtime.example.com")
    assistants = await client.assistants.search()
    assert {a["assistant_id"] for a in assistants} >= {"LangGraph", "phase1-deep-agent"}
    thread = await client.threads.create()
    async for part in client.runs.stream(thread["thread_id"], "phase1-deep-agent", input={"messages": [{"role": "user", "content": "selam"}]}, stream_mode="values", stream_subgraphs=True):
        print(part)
    state = await client.threads.get_state(thread["thread_id"])
    history = await client.threads.get_history(thread["thread_id"])
    assert state["checkpoint"]["thread_id"] == thread["thread_id"]
    assert history

asyncio.run(main())
```

Beklenen DeepAgents event'leri: human mesajı, root `task`, `tools:<subagent-run-id>` namespace, tool call, tool result ve final AI mesajı.

## Troubleshooting

- `503`: Gateway readiness, HAProxy backend, PostgreSQL ve Kafka health kontrolü.
- Run `pending`: Dispatcher pod'ları, Kafka consumer lag ve `UR_WORKER_TARGETS` kontrolü.
- History `[]`: Assistant id ile adapter eşleşmesi ve framework checkpoint schema kontrolü.
- `charAt`: Text-only content block listesi frontend için string'e normalize edilmelidir.
- Deep Agent persistence error: factory `checkpointer` ve `store` kabul etmelidir.

## MVP checklist

## A2A ve katman sinirlari

A2A Gateway tarafinda edge adapter olarak calisir. Gateway user code import etmez; graph detection ve adapter olusturma Worker veya All profilinin icindedir. Dispatcher yalnizca Kafka run komutlarini Worker gRPC sozlesmesine tasir. Ileride Agno icin yeni adapter eklenebilir; domain ve application katmanlarina framework tipi sizmaz.

Production A2A icin `UR_A2A_ENABLED=true`, tek assistant secmek icin `UR_A2A_ASSISTANT_ID` ve public kart adresi icin `UR_A2A_PUBLIC_URL` ayarlanir. A2A normal runtime start_run/cancel/stream akisina route edilir. Production task persistence A2A SDK `DatabaseTaskStore` ile ortak PostgreSQL engine/pool uzerinden tutulur; InMemory task store yalnizca local/test profilidir.

## Python, UBI9 ve telemetry

Container UBI9 Python 3.12 tabanlidir. Projenin sozlesmesi `>=3.12,<3.14` oldugu icin 3.14 kullanilmaz. `PYTHONPATH` yalnizca import arama yoludur; OTel context propagation `contextvars` ile yapilir ve source path degistirmek gerekmez.

`UR_OBSERVABILITY_ENABLED=true` auto-instrumentation entrypoint'ini calistirir. Runtime bootstrap mevcut OTel SDK provider'ini reuse eder; ikinci provider kurmaz. OpenLIT ayni aktif OTel provider'a baglanir ve GenAI span/metriklerini ortak OTLP endpoint'ine gonderir. Instana auto-injection/sitecustomize ile ikinci provider kurulmamalidir; Instana backend gerekiyorsa OTel Collector exporter tarafinda ortak hedef olarak yapilandirilmalidir.

- [ ] production profile
- [ ] migration ve framework setup
- [ ] Kafka topics
- [ ] Gateway/Dispatcher/Worker ready
- [ ] assistant auto-register
- [ ] SDK stream/state/history
- [ ] DeepAgents root/subagent/tool
- [ ] checkpoint history
- [ ] OTel/OpenLIT modu bilinçli seçildi
- [ ] Instana auto-injection kapalı

# UniversalRuntime — Phase 1 Runtime

UniversalRuntime, LangGraph / LangChain `create_agent` / Deep Agents uyumlu, Gateway tarafından yönetilen ve Kafka üzerinden yatay ölçeklenen bir AI runtime çekirdeğidir.

## Temel sözleşmeler

- Gateway public LangGraph-compatible ve native HTTP API yüzeyini sağlar.
- Gateway kullanıcı application modüllerini import etmez.
- Run komutu doğrudan `rt.<environment>.<application_id>.runs.*.v1` topic’ine yazılır.
- Aynı application’ın Worker replikaları ortak Kafka consumer group ile yükü paylaşır.
- Worker registration, migration coordination ve heartbeat internal gRPC üzerinden yürür.
- Worker ancak application migration başarıyla tamamlandıktan sonra Kafka tüketmeye başlar.
- LangGraph state/history için `AsyncPostgresSaver` ve `AsyncPostgresStore` authoritative kaynaktır.
- Runtime event journal veya projector ile framework history ikinci kez PostgreSQL’e yazılmaz.
- Canlı RuntimeEvent akışı Kafka execution-events topic’i üzerinden bütün Gateway replikalarına fan-out edilir.
- Application migration environment’ı Runtime’a aittir; application yalnız revision dosyalarını taşır.
- Application FastAPI ayrı process/deployment olarak çalışır ve Gateway yalnız reverse proxy yapar.
- FastAPI router’ları folder convention ile otomatik keşfedilir: her endpoint klasöründe `routes.py` ve `schema.py` bulunur.

## Kafka topic sözleşmesi

```text
rt.<environment>.<application_id>.runs.short_queue.v1
rt.<environment>.<application_id>.runs.long_queue.v1
rt.<environment>.<application_id>.execution.events.v1
rt.<environment>.<application_id>.deadletter.v1
```

Partition key aynı thread içindeki sıralamayı korur:

```text
<application_id>:<thread_id-or-run_id>
```

## Worker başlangıç akışı

```text
Worker gRPC server starts
        |
        v
Gateway WorkerControl.Register
        |
        +-- migration current ----------> ready
        |
        +-- migration owner = this pod -> WorkerControl.Migrate -> ready
        |
        +-- another pod migrating ------> wait for DB state -> ready
        |
        v
Kafka consumer starts
```

## FastAPI convention

```text
application/http/
  assistants/
    routes.py
    schema.py
  admin/
    audit/
      routes.py
      schema.py
```

Runtime otomatik olarak:

- folder path’inden URL prefix üretir,
- `assistants` için `Assistants`, nested folder için `Admin / Audit` tag’i üretir,
- deterministic `operationId` üretir,
- OpenAPI request schema ve example kontrolü yapar.

LangGraph SDK endpoint path’leri protocol contract olduğu için folder prefix dönüşümünden muaftır; yalnız tag/schema/example metadata’sı otomatik uygulanır.

## Kalite kapıları

```bash
make quality
make test
make compatibility
```

CI şu kontrolleri uygular ve raporları artifact olarak saklar:

- Ruff check ve format,
- mypy,
- architecture guard,
- contract validation,
- pytest ve en az `%80` coverage.

## Ana dokümanlar

- `docs/architecture/PHASE1_ARCHITECTURE.md`
- `docs/compatibility/LANGGRAPH_COMPATIBILITY_MATRIX.md`
- `docs/standards/NAMING_AND_CODING.md`
- `contracts/config/runtime-application.schema.json`
- `contracts/proto/runtime/v1/*.proto`
- `contracts/openapi/universal-runtime-phase1.yaml`

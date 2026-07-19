# UniversalRuntime — Phase 1 Blueprint

Bu paket, boş bir repoda **LangGraph / LangChain `create_agent` / Deep Agents uyumlu, Gateway ile yönetilen, düşük gecikmeli streaming runtime MVP** geliştirmek için contract-first bir başlangıç setidir.

Bu bir bitmiş ürün değildir. Aşağıdakileri sabitler:

- mimari sınırlar ve klasör yapısı,
- public API ve internal gRPC sözleşmeleri,
- SDK config ve adapter modeli,
- InMemory local profil ile PostgreSQL/Kafka production profili,
- LangGraph API compatibility kapsamı,
- managed PostgreSQL checkpointer/store yaklaşımı,
- interactive/normal/batch öncelikli kuyruk,
- event-driven output projection,
- FastAPI custom HTTP mount,
- A2A discovery ve streaming başlangıç yüzeyi,
- migration, observability, test ve coverage standartları,
- geliştirme ajanına sırayla verilecek promptlar.

## Başlangıç

1. Bu klasörün içeriğini repo köküne kopyalayın.
2. Önce `AGENTS.md` dosyasını geliştirme ajanına okutun.
3. `prompts/IMPLEMENTATION_SEQUENCE.md` sırasını takip edin.
4. Her adımda ilgili `skills/*/SKILL.md` dosyasını bağlama ekleyin.
5. Her merge öncesi kalite kapılarını çalıştırın:

```bash
make quality
make test
make compatibility
```

## Phase 1 başarı tanımı

Phase 1 şu durumda tamamlanır:

- resmî `langgraph_sdk` ile assistant/thread/run/stream/state/history akışları geçer,
- LangGraph, LangChain `create_agent` ve Deep Agents compiled graph olarak otomatik algılanır,
- local profil yalnız InMemory bileşenlerle çalışır,
- production profil PostgreSQL + Kafka + gRPC kullanır,
- Gateway application config revision’larını yönetir,
- interactive çağrılar batch işlere göre önceliklidir,
- aynı thread için aynı anda en fazla bir aktif run vardır,
- worker concurrency ENV ve deployment config ile sınırlanabilir,
- managed LangGraph `AsyncPostgresSaver` ve `AsyncPostgresStore` bağlanır,
- FastAPI custom routes Gateway arkasında çalışır,
- A2A Agent Card ve streaming adapter yüzeyi mevcuttur,
- coverage en az `%80`, Ruff, mypy ve architecture guard temizdir.

## Ana dokümanlar

- `AGENTS.md` — ajan ve geliştirici kuralları
- `docs/architecture/PHASE1_ARCHITECTURE.md`
- `docs/compatibility/LANGGRAPH_COMPATIBILITY_MATRIX.md`
- `docs/standards/NAMING_AND_CODING.md`
- `contracts/config/runtime-application.schema.json`
- `contracts/proto/runtime/v1/*.proto`
- `contracts/openapi/universal-runtime-phase1.yaml`
- `prompts/IMPLEMENTATION_SEQUENCE.md`

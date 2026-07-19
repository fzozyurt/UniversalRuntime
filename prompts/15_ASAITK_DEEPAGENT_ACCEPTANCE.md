# Prompt 15 — ASAITK DeepAgent Acceptance

`AGENTS.md`, `skills/langgraph-adapter/SKILL.md` ve `fzozyurt/asaitk-deepagent` projesinin mevcut `build_agent()` entrypointini incele.

Amaç: UniversalRuntime'ın uygulama image'ında `build_agent()` tarafından dönen compiled Deep Agents/LangGraph graph'ını explicit entrypoint ile yükleyebilmesi.

Acceptance:

1. Projeyi test fixture veya editable dependency olarak yükle.
2. Gerçek dış servislere gitmeyen test settings/fake model kullan.
3. `thread_id`, run metadata ve context'in graph çağrısına ulaştığını kanıtla.
4. Subagent tool event namespace'lerini stream'de doğrula.
5. Managed persistence ile graph kodunun production checkpointer tanımlamasına gerek olmadığını doğrula.
6. FastAPI yüzeyi varsa Gateway prefix altında smoke test yap.
7. External credentials gerektiren testleri yalnız ayrı integration marker'a koy; core acceptance testlerini skip etme.

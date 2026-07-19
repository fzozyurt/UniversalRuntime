# Prompt 00 — Master Context

Repo kökündeki `AGENTS.md`, tüm `docs/decisions/*`, contract dosyaları ve ilgili `skills/*/SKILL.md` dosyasını oku. UniversalRuntime Phase 1'i contract-first, Hexagonal/DDD sınırlarıyla geliştir.

Ana hedef: resmi `langgraph_sdk` ile çağrılabilen; LangGraph, LangChain `create_agent` ve Deep Agents graph'larını çalıştıran; local InMemory ve production PostgreSQL/Kafka/gRPC profilleri olan; Gateway-managed config, streaming, priority queue, FastAPI ve A2A yüzeyi sunan MVP.

Asla yapılmış işi varsayma. Her aşamada mevcut repo durumunu, testleri ve coverage'ı yeniden doğrula. Çalışmayan ya da test edilmemiş özelliği tamamlandı diye raporlama.

Compatibility route'larını native response envelope ile bozma. Framework checkpoint özelliğini universal core'a zorlama; LangGraph adapter içinde upstream saver/store kullan.

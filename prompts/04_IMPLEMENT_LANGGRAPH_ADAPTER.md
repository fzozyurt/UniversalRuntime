# 04 IMPLEMENT LANGGRAPH ADAPTER

Önce `AGENTS.md` ve `skills/langgraph-adapter/SKILL.md` dosyasını oku.

## Görev

LangGraph adapterını tamamla. Explicit entrypoint, compiled graph, builder/factory, LangChain create_agent ve Deep Agents detection ekle. Local InMemory saver/store otomatik bağlansın. Config/context/runtime IDs korumalı merge ile inject edilsin. Native stream v1/v2, tool, custom ve subgraph namespace dönüşümü yapılsın.

Resmi langgraph_sdk ile assistant/thread/run/stream/state/history ve interrupt/resume testleri yaz. Fake model/deterministic graph kullan; harici API key gerektirme.

## Çalışma biçimi

1. Mevcut kodu ve contractları incele.
2. Küçük, test edilebilir vertical slice uygula.
3. Contract/implementation/test/docs'u aynı committe tut.
4. Hata ve cancellation yollarını test et.
5. Aşağıdaki çıktıları raporla:
   - değişen dosyalar,
   - komutlar ve gerçek sonuçları,
   - coverage,
   - kalan riskler.

## Yasaklar

- Test geçmeden tamamlandı demek.
- Compatibility API'yi native envelope ile sarmak.
- Domain katmanına framework/infrastructure import etmek.
- Unbounded asyncio task oluşturmak.
- Upstream LangGraph checkpoint tablolarını fork etmek.

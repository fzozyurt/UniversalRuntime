# 09 IMPLEMENT STREAMING PROJECTOR

Önce `AGENTS.md` ve `skills/streaming-events/SKILL.md` dosyasını oku.

## Görev

Event gateway/replay ve lifecycle projector ekle. RuntimeEvent sequence, batching, SSE conversion, Last-Event-ID/sequence cursor ve local AsyncQueue/Kafka publisher implement et. Projector final output/status'u DB'ye, opsiyonel plugin ile OpenSearch'e yazabilsin.

Execution hot path analytics sink'i beklememeli.

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

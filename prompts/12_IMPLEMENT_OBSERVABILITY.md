# 12 IMPLEMENT OBSERVABILITY

Önce `AGENTS.md` ve `skills/observability/SKILL.md` dosyasını oku.

## Görev

OpenTelemetry bootstrap ve shell launcher davranışını uygula. UR_OBSERVABILITY_ENABLED false iken sade başlasın; true iken zero-code instrument çalışsın. Optional OpenLIT adapter, background runtime.run span, FastAPI/HTTPX/gRPC/SQLAlchemy/Kafka trace context ve exception recording ekle. Secret/prompt capture policy testleri yaz.

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

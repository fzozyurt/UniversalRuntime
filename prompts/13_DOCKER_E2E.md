# 13 DOCKER E2E

Önce `AGENTS.md` ve `skills/docker-e2e/SKILL.md` dosyasını oku.

## Görev

Tek image için standalone/gateway/dispatcher/worker/projector modlarını tamamla. Compose: HAProxy, Gateway x2, Dispatcher x2, Worker x2, PostgreSQL, Kafka ve opsiyonel OTel. Migration startup sırasını kur.

E2E: official SDK, priority, worker kill/retry, HAProxy LB, FastAPI, A2A ve persistence.

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

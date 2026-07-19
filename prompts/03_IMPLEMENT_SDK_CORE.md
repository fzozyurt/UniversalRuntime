# 03 IMPLEMENT SDK CORE

Önce `AGENTS.md` ve `skills/sdk-core/SKILL.md` dosyasını oku.

## Görev

Canonical identity, execution request/event, adapter registry, capabilities, typed errors, config loader/interpolation/redaction, InMemory repositories ve local priority queue uygula. Standalone composition root ve launcher çalışsın.

Interactive priority, bounded semaphore ve deterministic shutdown unit testleri ekle.

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

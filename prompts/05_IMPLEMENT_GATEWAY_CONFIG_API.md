# 05 IMPLEMENT GATEWAY CONFIG API

Önce `AGENTS.md` ve `skills/gateway/SKILL.md` dosyasını oku.

## Görev

Gateway compatibility ve native management API'sini uygula. Application config validate/create revision/activate endpointleri, immutable hash ve precedence modelini kur. Thread/run/assistant domain handlers ve transactional outbox ekle.

Gateway hiçbir user module import etmemeli. Compatibility responses resmi SDK testleriyle birebir doğrulanmalı.

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

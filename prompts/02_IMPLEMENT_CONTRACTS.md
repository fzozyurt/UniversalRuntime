# 02 IMPLEMENT CONTRACTS

Önce `AGENTS.md` ve `skills/contract-first/SKILL.md` dosyasını oku.

## Görev

Contract dosyalarını implementation-ready hale getir. OpenAPI bileşenlerini DTO'larla eşleştir, protobuf'u Buf/protoc ile derlenebilir yap, config ve event schema testlerini ekle. Topic/config prefix override kurallarını test et.

Compatibility endpoint kapsamı matrix ile aynı olmalı; eksikler açık TODO/issue olarak tutulmalı.

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

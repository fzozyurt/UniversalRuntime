# 11 IMPLEMENT A2A

Önce `AGENTS.md` ve `skills/a2a/SKILL.md` dosyasını oku.

## Görev

A2A Agent Card ve message/stream adapterını uygula. Assistant metadata -> skills/card, context -> thread, task -> run, artifacts/status -> runtime events mapping yap. Yalnız gerçek capabilities'i advertise et. A2A SDK ile discovery ve streaming testleri ekle.

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

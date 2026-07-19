# 10 IMPLEMENT FASTAPI

Önce `AGENTS.md` ve `skills/fastapi/SKILL.md` dosyasını oku.

## Görev

FastAPI explicit entrypoint ve AST/isolated detection uygula. Worker içinde custom app'i ayrı route surface olarak çalıştır, Gateway prefix proxy/root_path/forwarded headers/trace propagation ekle. App DB Alembic migration Job contractını ve schema isolation testlerini ekle.

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

# 06 IMPLEMENT POSTGRES

Önce `AGENTS.md` ve `skills/postgres/SKILL.md` dosyasını oku.

## Görev

SQLAlchemy async platform modellerini, Alembic migrationlarını, shared audit mixinlerini, schema naming ve advisory migration lock'u uygula. LangGraph AsyncPostgresSaver/Store setup ve app-specific fixed search_path pool oluştur. Outbox/inbox ve run event batch tablolarını ekle.

Gerçek PostgreSQL integration testlerinde restart/state/history, isolation, lock ve idempotency doğrula.

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

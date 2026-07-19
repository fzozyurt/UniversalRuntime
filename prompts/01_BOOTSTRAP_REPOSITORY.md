# 01 BOOTSTRAP REPOSITORY

Önce `AGENTS.md` ve `skills/repo-bootstrap/SKILL.md` dosyasını oku.

## Görev

Repo iskeletini gerçek Python paketi ve servis composition root'ları olarak kur. `pyproject.toml`, kalite araçları, CI, Makefile, launcher CLI skeleton, architecture guard ve contract validator çalışsın.

Acceptance: editable install; Ruff; format; strict mypy; architecture guard; contract validation; başlangıç testleri.

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

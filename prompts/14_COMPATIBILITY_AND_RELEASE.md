# 14 COMPATIBILITY AND RELEASE

Önce `AGENTS.md` ve `skills/quality-release/SKILL.md` dosyasını oku.

## Görev

Compatibility matrix'i gerçek test sonuçlarıyla güncelle. Desteklenen langgraph/langgraph_sdk/langchain/deepagents sürümlerini pinle ve test matrisi oluştur. Ruff, format, strict mypy, architecture, contracts, >=80 coverage, package build ve container smoke geçmeden Phase 1'i tamamlandı işaretleme. Draft PR/release notes hazırla.

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

# 07 IMPLEMENT KAFKA DISPATCH

Önce `AGENTS.md` ve `skills/kafka-dispatch/SKILL.md` dosyasını oku.

## Görev

Kafka priority topics ve dispatcher uygula. Configurable prefix/topic override, partition key, weighted fair scheduling, age promotion, delayed commit, retry/dead letter ve inbox dedup ekle.

Test: çok sayıda batch kuyruktayken yeni interactive run önce seçilir; batch starvation yaşamaz; aynı thread sırası bozulmaz.

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

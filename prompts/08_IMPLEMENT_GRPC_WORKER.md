# 08 IMPLEMENT GRPC WORKER

Önce `AGENTS.md` ve `skills/grpc-worker/SKILL.md` dosyasını oku.

## Görev

Protobuf codegen, WorkerControl ve Execution servislerini uygula. Register/config hash/capabilities, bidi work stream, leases, heartbeat, cancellation, drain ve standard health ekle. Worker max concurrency config+ENV+policy min değeriyle çözülmeli.

Scalar/list/object/null payload roundtrip ve shutdown testleri zorunlu.

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

# nft-intelligence-layer

## التشغيل القديم

`main.py` هو البوت الأصلي كما هو.

## التشغيل المتقدم

`advanced_bot.py` يضيف:

- لوحة أزرار Telegram.
- محافظ متعددة مع تشغيل/إيقاف لكل محفظة.
- تخزين المحافظ محليًا بشكل مشفر في `wallets.json`.
- شرط وجود موقع أو حساب X قبل تنفيذ mint.
- مراقبة غاز أسرع مع انتظار قراءات أرخص ضمن حد الشبكة.
- مراقبة mint يدوي من Telegram باستخدام رابط OpenSea أو slug والكمية.
- محاولة mint لكل محفظة نشطة، بما يشمل مراحل التأهيل إذا أعطت OpenSea معاملة جاهزة للمحفظة.
- فحص NFTs في المحافظ وعرض floor price المتاح.
- نشر listing حقيقي بعد موافقة المستخدم: approval transaction ثم توقيع Seaport ثم إرسال order إلى OpenSea.
- حفظ عمليات الشراء الناجحة في `advanced_state.json` حتى لا تتكرر بعد إعادة التشغيل.
- قفل nonce لكل محفظة حتى لا تتصادم المعاملات عند تنفيذ أكثر من عملية بسرعة.
- Health endpoint على `PORT` حتى يعمل كبوت مستضاف كـ Web Service.

شغّل النسخة المتقدمة من:

```bash
python advanced_bot.py
```

المتغيرات الجديدة في `.env.example`:

- `ADVANCED_WALLET_PASSWORD`: كلمة سر تشفير المحافظ المحلية.
- `ADVANCED_AUTO_BUY`: تشغيل/إيقاف الالتقاط التلقائي.
- `ADVANCED_AUTO_BUY_PAID_DROPS`: السماح بشراء drops مدفوعة تلقائيًا.
- `ADVANCED_GAS_LOW_FACTOR`: مدى تشدد انتظار الغاز المنخفض.
- `ADVANCED_MAX_GAS_USD_ROBINHOOD` و`ADVANCED_MAX_GAS_USD_ETHEREUM`: حد الغاز بالدولار لكل شبكة.
- `ADVANCED_DROPS_DISCOVERY_SECONDS`: فاصل فحص drops القادمة والنشطة من OpenSea.
- `ADVANCED_ENABLE_HEALTH_SERVER` و`PORT`: لتشغيل health endpoint عند الاستضافة.
- `OPENSEA_SCOPED_TOKEN` و`OPENSEA_LISTING_ENABLED`: لتفعيل نشر listing الحقيقي من OpenSea.

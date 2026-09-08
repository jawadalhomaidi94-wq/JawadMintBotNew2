# OpenSea Mint Guardian V4.2

إصدار Railway + Alchemy + Telegram مع اكتشاف تلقائي للـFree Mint وإدارة عدة محافظ.

## أهم ما في V4.2

- **Auto Free Mint** مفعّل افتراضيًا: يفحص OpenSea كل **15 ثانية** بدون الحاجة لإرسال رابط.
- يفحص `recently_minted` و`featured` و`upcoming` على جميع الشبكات المفعلة، وبحد أقصى 100 نتيجة لكل صفحة.
- عند أول تشغيل يعمل Deep Scan لعدة صفحات (`AUTO_FREE_INITIAL_PAGES=3`) لالتقاط المنتات المجانية الموجودة أصلًا، ثم يفحص أحدث النتائج كل 15 ثانية.
- لا يعتبر السعر المجهول مجانيًا؛ الاكتشاف التلقائي لا يقبل إلا مرحلة سعرها **0 صراحةً**، ثم توجد حماية ثانية داخل `buyer.py` تمنع أي معاملة مدفوعة في الوضع التلقائي.
- كل المحافظ **النشطة** والمناسبة للشبكة تدخل في الـFree Mint تلقائيًا. المحفظة المتوقفة لا تشارك.
- الكمية المطلوبة لكل محفظة تبدأ من الكمية التي ضبطتها للمحفظة، ثم تُخفض حسب `maxPerWallet` والـremaining supply إن توفرت.
- إذا رفض OpenSea الكمية بسبب حد المحفظة/المعروض (HTTP 422)، يجرب 1 ثم يستخدم بحثًا ثنائيًا لإيجاد **أعلى كمية يقبلها OpenSea** بدل فشل العملية كلها.
- الاكتشافات التي لم تُنفذ تكون صامتة افتراضيًا حتى لا يمتلئ Telegram؛ عمليات Mint المرسلة/المؤكدة تُرسل دائمًا.
- نجاح Auto Mint يُسجل في SQLite لكل Drop + Wallet، حتى لا يعيد نفس المنت تلقائيًا بعد Restart/Redeploy.
- سقف الغاز بالدولار من V4.1 مستمر، مثل `MAX_GAS_USD=0.08`. إذا تجاوز الغاز السقف، ينتظر ويعيد المحاولة بدل الدفع.
- الروابط التي ترسلها بنفسك تبقى **Manual Watch**: تُحفظ في SQLite وتراقب موعد الـPublic والمراحل المستقبلية.
- الـPaid Mint لا يحدث تلقائيًا من الاكتشاف؛ فقط للمراقبة اليدوية وبعد اختيار المحافظ صراحةً.

## الفرق بين الوضعين

### 1. Auto Free Mint
لا ترسل أي رابط. البوت يعمل في الخلفية:

```text
OpenSea discovery
  ↓ كل 15 ثانية
Free stage نشطة + السعر 0
  ↓
فحص الشبكة / المعروض / maxPerWallet
  ↓
جميع المحافظ النشطة
  ↓
اختيار أعلى كمية صالحة لكل محفظة
  ↓
فحص الغاز <= MAX_GAS_USD
  ↓
Mint
```

### 2. Manual Watch
ترسل رابط OpenSea للمشروع عندما تريد مراقبته حتى يفتح Public. يبقى محفوظًا بعد Restart. إذا كانت المرحلة مجانية ينفذها تلقائيًا، وإذا كانت مدفوعة يطلب منك اختيار المحافظ.

## Variables الجديدة

```env
AUTO_FREE_MINTS=true
AUTO_FREE_SCAN_SECONDS=15
AUTO_FREE_DROP_TYPES=recently_minted,featured,upcoming
AUTO_FREE_DROP_LIMIT=100
AUTO_FREE_INITIAL_PAGES=3
AUTO_FREE_DETAIL_WORKERS=8
AUTO_FREE_NOTIFY_DISCOVERY=false
AUTO_FREE_CANDIDATE_TTL=90
```

الإعدادات الحالية المهمة تبقى كما هي:

```env
ENABLED_CHAINS=ethereum,ink,robinhood
MAX_GAS_NATIVE=0
MAX_GAS_USD=0.08
GAS_STRATEGY=smart
GAS_LIMIT_BUFFER=1.08
GAS_OVER_BUDGET_RETRY_SECONDS=2
ALLOW_PAID_MINTS=true
```

`ALLOW_PAID_MINTS=true` لا يسمح للاكتشاف التلقائي بشراء Mint مدفوع؛ هو يخص الروابط اليدوية فقط، ويظل اختيار المحافظ مطلوبًا قبل التوقيع.

## الترقية من V4.1

استبدل في GitHub:

- `main.py`
- `buyer.py`
- `storage.py`
- اختياريًا `.env.example` و`README.md`

لا تغير `WALLET_ENCRYPTION_KEY` ولا تحذف Railway Volume. قاعدة البيانات تتم ترقيتها تلقائيًا بإضافة حقل كمية الـMint الفعلية في سجل العمليات.

## أمان

- استخدم محافظ Mint مخصصة وأرصدة صغيرة.
- لا تغير `WALLET_ENCRYPTION_KEY` بعد تخزين المحافظ.
- لا تشارك Private Keys أو Seed Phrase.
- `MAX_GAS_USD=0.08` يعني أن البوت لا يتعمد إرسال معاملة يتجاوز أقصى تقدير غازها 8 سنتات؛ إذا تعذر حساب السعر بالدولار يفشل بأمان ولا يرسل.

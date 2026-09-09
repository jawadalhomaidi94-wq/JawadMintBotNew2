# OpenSea Mint Guardian V4.7

نسخة محسّنة فوق **V4.6** التي أصلحت أزرار Telegram. لم يتم تغيير البنية التي أصبحت تعمل لدى المستخدم؛ V4.7 تضيف تنظيم الرسائل، روابط المنت، معالجة Rate Limit، وتسريع الاكتشاف.

## ما بقي كما هو

- Telegram command worker مستقل؛ `/start` والأزرار لا تنتظر فحوص الـMint.
- محافظ مشفرة بالأسماء مع تشغيل/إيقاف وكمية مستقلة.
- Railway Volume + SQLite migration بدون حذف البيانات.
- Free Mint تلقائي لكل المحافظ النشطة.
- Paid Mint لا يُشترى إلا بعد موافقة المستخدم، اختيار المحافظ، وكمية كل محفظة.
- Stage/qualification planner والكمية التراكمية بين المراحل.
- إذا كان حد المرحلة `1..100` فهو الهدف؛ إذا كان `>100` أو غير محدود/غير معروف فالهدف `30`.
- سقف الغاز بالدولار، واستراتيجية `smart`.

## V4.7 — معالجة OpenSea 429

OpenSea REST أصبح له مدير مركزي واحد داخل `OpenSeaClient`:

- يقرأ `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`.
- عند `429` يحترم `Retry-After` ولا يعيد الضرب على OpenSea أثناء فترة التهدئة.
- يترك Reserve لطلبات الـMint المهمة ويؤجل backfill إذا كانت الحصة منخفضة.
- يمنع انفجار طلبات عدة محافظ في اللحظة نفسها عبر REST concurrency + start pacing.
- Cache قصير لبيانات Drop، وأطول لبيانات collection/contract.
- رسائل `429` لا تملأ Telegram افتراضيًا؛ تظهر في Logs ويُعاد المحاولة آليًا.
- فحص أهلية OpenSea يستخدم `quantity=1` فقط لمعرفة نعم/لا؛ البحث عن أعلى كمية لا يحدث إلا عند التنفيذ عند الحاجة، وهذا يوفر عدة POSTs لكل محفظة.

> لا يتم تخفيض سرعة Stream أو القراءة المباشرة من SeaDrop بسبب هذا الـlimiter.

## V4.7 — سرعة الاكتشاف

ترتيب الاكتشاف:

1. **OpenSea Stream** لحظي — أعلى أولوية.
2. إذا أعطى Stream الشبكة + العقد، يتم فحص **SeaDrop on-chain مباشرةً قبل REST**.
3. `upcoming` يُفحص افتراضيًا كل 15 ثانية لاكتشاف المراحل/الـPublic المجدول مبكرًا.
4. Global Mint Events كل 30 ثانية كتعويض عن أي Stream event مفقود.
5. `featured/recently_minted` backfill أبطأ افتراضيًا كل 60 ثانية حتى لا تستهلك REST quota.

الـStream candidate يدخل Priority Queue ويُعالج قبل catalog work، كما أن المرشحين النشطين المجانيين وPublic الوشيك لهم أولوية داخل حلقة التنفيذ.

## الرسائل والتنظيم

كل رسالة/قسم متعلق بمنت يعرض رابطًا قابلًا للنسخ متى أمكن:

`🔗 رابط المنت (للنسخ): https://opensea.io/collection/...`

القائمة الرئيسية تحتوي أقسامًا مستقلة:

- `🎟 التأهيل`
- `👀 المراقبة`
- `🆓 المجانية المأخوذة`
- `💳 المنتات المدفوعة`
- `🧪 فحص الأهلية`
- `📜 سجل العمليات`

### التأهيل بدون Spam

للمشروع متعدد مراحل التأهيل:

- رسالة منظمة واحدة عند اكتشاف جدول مراحل اليوم.
- الفحوص التفصيلية للمحافظ تعمل بصمت وتُحفظ في قسم التأهيل.
- رسالة واحدة فقط عند بدء كل **مرحلة جديدة غير مدفوعة**.
- المرحلة المدفوعة لا تُدفع تلقائيًا إلى Telegram ولا تظهر في رسالة جدول التأهيل؛ تبقى داخل `💳 المنتات المدفوعة`.

### المدفوع

المدفوع يُحفظ بصمت. عند فتح زر `💳 المنتات المدفوعة` يظهر:

- اسم المشروع والشبكة.
- وقت الفتح.
- السعر الحقيقي بالعملة الأصلية (`ETH` للشبكات الحالية).
- القيمة التقريبية بـ `USDT`.
- حالة خطة الشراء.
- رابط المنت.

## Railway

لا تحذف الـVolume ولا تغيّر `WALLET_ENCRYPTION_KEY` بعد حفظ المحافظ.

الملفات الأساسية:

- `main.py`
- `buyer.py`
- `storage.py`
- `health.py`
- `requirements.txt`
- `railway.json`

القيم الجديدة كلها لها Defaults؛ لا يلزم تعديل Variables الحالية لتشغيل V4.7. راجع `.env.example` إذا أردت التحكم في pacing/cache/scan intervals.

## أهم القيم الافتراضية الجديدة

```env
OPENSEA_REST_CONCURRENCY=2
OPENSEA_MIN_REQUEST_INTERVAL=0.08
OPENSEA_RATE_RESERVE=6
OPENSEA_ELIGIBILITY_WORKERS=2
SILENT_RATE_LIMIT_TELEGRAM=true

AUTO_STREAM_FAST_PATH=true
AUTO_UPCOMING_SCAN_SECONDS=15
AUTO_EVENT_SCAN_SECONDS=30
AUTO_DROP_SCAN_SECONDS=60

STAGE_SUMMARY_NOTIFICATIONS=true
STAGE_OPEN_NOTIFICATIONS=true
```

## ملاحظة السرعة

لا يمكن لأي بوت ضمان أن يكون دائمًا أول معاملة في البلوك؛ وقت وصول Stream/RPC، ازدحام الشبكة، ترتيب المعاملات، وسقف الغاز كلها عوامل خارج البرنامج. V4.7 صُممت لتجعل **المسار الحي لا يعتمد على REST polling** قدر الإمكان، مع منع 429 من استهلاك وقت الفرصة.

# OpenSea Mint Guardian V4.9 — Ultra Race Lane

V4.9 مبنية مباشرة فوق V4.8 وتحافظ على واجهة Telegram والمحافظ والمراقبة والتأهيل والمدفوع وإعدادات الغاز. التغيير الأساسي هو إزالة المسار البطيء من أمام المنت الحرج: الـPublic المجاني المجدول يُجهز ويُوقع قبل الفتح ثم يُبث مباشرة عند وقت الفتح، وأحداث Stream/SeaDrop تدخل Race Lane مستقل ولا تنتظر طابور الـmetadata أو الحلقة الرئيسية.

## ⚡ V4.9 Ultra Race Lane

### لماذا V4.8 كان يمكن أن يتأخر؟
في V4.8 كان حدث Stream يدخل إلى مرشح/طابور ثم تمر العملية عبر تحديث المرشح والمراحل ومحاولة المنت. كذلك بعض معلومات الرسوم والسعر كانت قد تُقرأ وقت التنفيذ. هذا مقبول للمراقبة العادية، لكنه غير مناسب لمنت ينفد خلال 20 ثانية.

### المسار الجديد للـPublic المعروف مسبقًا
1. يحتفظ مدير المراحل بوقت فتح Public.
2. قبل الفتح بـ `RACE_PREWARM_SECONDS` (افتراضي 2 ثانية) يقرأ إعداد Public SeaDrop مرة واحدة.
3. يحسب الكمية المطلوبة لكل محفظة مع خصم ما أخذته في المراحل السابقة.
4. يجلب nonces والأرصدة للمحافظ بالتوازي.
5. يجهز ويوقع معاملات كل المحافظ **قبل** وقت الفتح.
6. عند timestamp الفتح، Race Scheduler (tick افتراضي 10ms) يرسل الـraw transactions مباشرة بالتوازي.
7. لا يوجد OpenSea REST eligibility ولا Drops refresh ولا HTTP price lookup أمام البث.

### المسار الجديد للمنتات التي تبدأ بدون جدول معروف
- OpenSea Stream يرسل إشارة مباشرة إلى Race Executor قبل طابور الـmetadata.
- يوجد اشتراك WebSocket مباشر في Logs عقد SeaDrop عبر Alchemy لكل شبكة مفعلة؛ تحديثات Public وMint activity يمكن أن تكشف العقد قبل/أسرع من REST catalog.
- عند وجود Public SeaDrop نشط، يُبنى batch للمحافظ مباشرة.
- Gas estimate يتم **مرة لكل كمية مختلفة** بدل مرة لكل محفظة.
- البث يستخدم ThreadPool دائم بدل إنشاء Executor جديد لكل عملية.

### Hot Cache للرسوم والسعر
- Fee fields تُحدّث في الخلفية افتراضيًا كل `0.50s`.
- سعر native/USD يُحدّث في الخلفية افتراضيًا كل `30s`.
- الـRace Lane يقرأ النسخة المخزنة في الذاكرة ولا ينتظر HTTP وقت الفتح.
- قفل Price Oracle لا يُمسك أثناء HTTP، لذلك تحديث السعر بالخلفية لا يوقف مسار المنت.

### استراتيجية الغاز في Race Lane
`RACE_GAS_STRATEGY=fast` منفصلة عن استراتيجية الغاز العادية. قبل التوقيع تُقصّ fee fields إلى الحد الفعلي الذي ضبطته من Telegram (Global / network / project). بهذا لا يرفض الـRace transaction فقط لأن استراتيجية fast وضعت headroom أكبر من ميزانية الغاز؛ يحاول استخدام أسرع bid ممكن داخل الحد.

> إذا كان **base fee الحقيقي نفسه** أعلى من الحد الذي حددته، فلا توجد خوارزمية تستطيع ضمان الإدراج مع الالتزام بذلك الحد. لمشروع مهم استخدم استثناء الغاز الموجود أصلًا في `⚙️ الإعدادات`.

### حماية التكرار والتزامن
- Stage واحد لا يمكن أن يملأ Executor بطلبات launch مكررة؛ يوجد `race_queued/race_active`.
- المحافظ التي لديها transaction pending لا تُعاد.
- سجل SQLite والحماية حسب chain + contract ما زالا فعالين.
- Telegram command worker يبقى مستقلًا عن Race Lane.

### Logs تشخيصية مهمة
```text
Race market warmer ready ...
Race scheduler ready ...
SeaDrop chain log stream connected | ethereum
Race prewarm ready | ... | wallets=... | prep=...s
RACE submitted | ... | wallets=... | launch-path=...s
```
وإذا لم تُرسل أي معاملة سيظهر مثل:
```text
Race prewarm blocked | ... | statuses=gas_usd_too_high
RACE no-submit | ... | statuses=...
```
وهكذا يصبح سبب عدم المنت واضحًا بدل أن يبدو كأنه تأخير مجهول.

### إعدادات V4.9 الاختيارية
```env
RACE_LANE_ENABLED=true
RACE_PREWARM_SECONDS=2.0
RACE_SCHEDULER_TICK=0.01
RACE_RETRY_SECONDS=0.05
RACE_LAUNCH_WINDOW_SECONDS=8
RACE_PUBLIC_GAS_LIMIT=300000
RACE_OPEN_OFFSET_MS=0
RACE_STREAM_WORKERS=16
RACE_GAS_STRATEGY=fast
RACE_FEE_REFRESH_SECONDS=0.50
RACE_PRICE_REFRESH_SECONDS=30
SEADROP_WSS_DISCOVERY=true
RPC_BROADCAST_POOL_WORKERS=32
```
لا تحتاج إضافتها إلى Railway لكي تعمل؛ هذه هي القيم الافتراضية في الكود.

> `RACE_PUBLIC_GAS_LIMIT` هو gas limit للمعاملة المجدولة التي تُوقَّع قبل الفتح، وليس مبلغًا يتم دفعه تلقائيًا. الغاز غير المستخدم لا يُدفع، لكن فحص سقف الغاز يبقى محافظًا ويحسب worst-case من هذا الحد.

> لا يمكن لأي بوت ضمان الفوز بكل Drop: توقيت البلوك، ازدحام الشبكة، RPC، سياسة الغاز والمنافسة عوامل خارج التطبيق. V4.9 يزيل الانتظار الداخلي القابل للإزالة من أمام البث.

---

## أهم السلوكيات

### 1) Free Mint تلقائي
- OpenSea Stream هو مسار الاكتشاف الفوري.
- REST Mint Events وDrops تبقى مسارات احتياطية/جدولة.
- عند اكتشاف Public مجاني لا ينتظر البوت فحص أهلية OpenSea قبل الإرسال.
- كل المحافظ النشطة تدخل التنفيذ مباشرة.
- إذا كان عقد الـNFT معروفًا وPublic SeaDrop مهيأ، يفضّل البوت `mintPublic` مباشرة on-chain بدل الاعتماد على OpenSea REST.
- إذا كان سعر الـPublic غير معروف في metadata، يعامل المرحلة كمسار سريع ما لم تكن مثبتة كمدفوعة؛ إذا اتضح on-chain أنها مدفوعة يتوقف قبل التوقيع وينقلها لقسم المدفوع.
- في آخر ثوانٍ قبل Public، REST refresh لا يسبق مسار التنفيذ.

### 2) كمية الـMint التلقائي
- إذا كان Max/Wallet بين 1 و100: الهدف التراكمي هو الحد المعلن.
- إذا كان Max/Wallet أكبر من 100 أو غير محدود/غير معروف: الهدف التلقائي 30.
- عبر المراحل يتم أخذ الفرق فقط. مثال: مرحلة 1 حدها 2، مرحلة 2 حدها 5 => يأخذ 3 إضافية.

### 3) التأهيل والمراقبة بدون Spam
- الفحص والتخزين يعملان في الخلفية.
- لا تُرسل رسائل جدول المراحل أو فتح مراحل التأهيل تلقائيًا افتراضيًا.
- التفاصيل تظهر عند الضغط على `🎟 التأهيل` أو `👀 المراقبة`.
- إشعارات دورة المعاملة نفسها تبقى فعالة: إرسال Mint / تأكيد / Revert.
- يمكن تشغيل إشعارات المراحل يدويًا من `⚙️ الإعدادات` إذا رغبت.

### 4) Public المدفوع
- المنت المدفوع لا يتم توقيعه تلقائيًا.
- داخل `💳 المنتات المدفوعة` يوجد زر `➕ إضافة رابط منت مدفوع`.
- بعد إرسال الرابط يقرأ البوت السعر والموعد، ثم تختار المحافظ والكمية لكل محفظة.
- لا يتم الشراء قبل `🚀 تأكيد خطة الشراء`.
- يعرض السعر Native (ETH على الشبكات الحالية) والقيمة التقريبية بالدولار/USDT.

### 5) حدود الغاز من Telegram
من `⚙️ الإعدادات` يمكن تعديل:
- الحد العام بالدولار.
- حد Ethereum.
- حد Ink.
- حد Robinhood.
- إعادة أي شبكة إلى وراثة الحد العام.

الإعدادات تُحفظ في SQLite على Railway Volume، لذلك لا تحتاج تعديل Railway Variables كل مرة.

متغيرات `MAX_GAS_USD` القديمة أصبحت **fallback لأول تشغيل فقط**. بعد حفظ قيمة من Telegram، القيمة المحفوظة هي المستخدمة.

### 6) استثناء غاز لمشروع مهم
من `⚙️ الإعدادات > 🔥 استثناء منت من حد الغاز`:
1. أرسل رابط المشروع.
2. اختر:
   - `🔥 بدون حد لهذا المنت`
   - `⛽ حد خاص بالدولار`
   - `🛡 استخدام حد الشبكة`

داخل صفحة المنت المدفوع يوجد أيضًا زر تجاوز/إعادة حد الغاز للمشروع.

> ⚠️ اختيار "بدون حد" يسمح برسوم أعلى من الحدود العامة، لذلك استخدمه فقط لمشروع تريد اقتناصه حتى لو ارتفع الغاز.

## Telegram الرئيسية

- ➕ إضافة محفظة
- 👛 المحافظ
- 🎟 التأهيل
- 👀 المراقبة
- 🆓 المجانية المأخوذة
- 💳 المنتات المدفوعة
- 🧪 فحص الأهلية
- 📜 سجل العمليات
- 🌐 الشبكات
- ⚙️ الإعدادات

## Railway

استبدل الملفات التالية في GitHub:

- `main.py`
- `buyer.py`
- `storage.py`
- `requirements.txt`
- `health.py`
- `railway.json`

ولا تغيّر `WALLET_ENCRYPTION_KEY` ولا تحذف Railway Volume.

## متغيرات أساسية

```env
OPENSEA_API_KEY=
ALCHEMY_API_KEY=
ENABLED_CHAINS=ethereum,ink,robinhood

TELEGRAM_BOT_TOKEN=
TELEGRAM_ALLOWED_CHAT_IDS=
TELEGRAM_ALLOW_ANY_CHAT=false

WALLET_ENCRYPTION_KEY=
DATA_DIR=./data

ALLOW_PAID_MINTS=true
MAX_MINT_PRICE_NATIVE=0
MAX_TOTAL_NATIVE=0
MAX_GAS_NATIVE=0

# fallback فقط قبل حفظ الإعداد من Telegram
MAX_GAS_USD=0.08

GAS_STRATEGY=smart
GAS_LIMIT_BUFFER=1.08

AUTO_FREE_MINTS=true
AUTO_FREE_STREAM=true
AUTO_STREAM_FAST_PATH=true

PUBLIC_PREOPEN_WINDOW_SECONDS=5
PUBLIC_FAST_RETRY_SECONDS=0.10

QUALIFICATION_RECHECK_SECONDS=15
```

## ملاحظات السرعة

V4.9 لا يحاول جعل كل REST requests أسرع لأن ذلك يؤدي إلى `429`. بدلاً من ذلك:
- Stream/on-chain هما مسار السرعة.
- REST محمي بـrate limiter وRetry-After.
- Public المجاني يتجاوز eligibility preflight ويذهب مباشرة لمحاولة التنفيذ.
- SeaDrop المباشر يُفضّل عندما يمكن استخدامه.
- حلقة التنفيذ تعمل بتردد أعلى، بينما Telegram يبقى في worker مستقل.

لا يمكن ضمان أن أي بوت سيلحق كل Drop محدود؛ زمن البلوك، RPC، ازدحام الشبكة، وسرعة نفاد الـsupply عوامل خارجية. V4.9 يقلل التأخيرات التي كانت داخل البوت نفسه دون تجاوز حماية الغاز أو توقيع Paid Mint بدون موافقة.

# OpenSea Mint Guardian V4

إصدار مخصص للعمل على Railway مع Alchemy وTelegram، ويدعم عدة محافظ وشبكات.

## ما الجديد في V4

- واجهة Telegram ورسائل المستخدم بالعربية.
- إضافة المحفظة على خطوتين: اسم مميز ثم Private Key.
- تشغيل وإيقاف كل محفظة من Telegram بدون حذفها.
- إعادة تسمية المحفظة من Telegram.
- تحديد كمية Mint افتراضية مستقلة لكل محفظة.
- عرض حالة المحفظة وأرصدة الشبكات من صفحة تفاصيلها.
- Free Mint: يحاول تلقائيًا لكل المحافظ النشطة المناسبة للشبكة.
- Paid Mint: لا يتم توقيعه لأي محفظة إلا بعد اختيارها صراحةً للمشروع.
- اختيار عدة محافظ أو كل المحافظ للمراحل المدفوعة.
- خيار «تجاهل المدفوع فقط» مع استمرار مراقبة المشروع لالتقاط أي مرحلة مجانية لاحقًا.
- حفظ اختيار محافظ الـPaid Mint في SQLite على Railway Volume، ويستمر بعد Restart/Redeploy.
- ترقية تلقائية لقاعدة بيانات V3؛ لا يلزم حذف قاعدة البيانات أو الـVolume.
- أوامر Telegram بأوصاف عربية تلقائيًا.
- إشعارات عربية عند إرسال المعاملة، تأكيدها، فشلها، نقص الرصيد أو أخطاء الشبكة.

## منطق المحافظ

المحفظة النشطة تدخل في كل Free Mint تلقائيًا. المحفظة المتوقفة تبقى محفوظة ومشفرة في قاعدة البيانات لكنها لا تشارك في التنفيذ حتى تعيد تشغيلها.

عند اكتشاف Mint مدفوع، يقوم البوت بمنع التوقيع أولًا ويعرض قائمة المحافظ النشطة. بعد تحديد المحافظ وتأكيد الاختيار، يسمح فقط لهذه المحافظ بشراء الـMint المدفوع. المحافظ الأخرى تبقى قادرة على التقاط Mint مجاني إذا ظهرت مرحلة مجانية لاحقًا.

## الترقية من V3 على Railway

استبدل الملفات التالية في GitHub:

- `main.py`
- `buyer.py`
- `storage.py`

ويمكنك كذلك استبدال `README.md` للتوثيق. لا تحتاج إلى تغيير Railway Variables الحالية ولا تغيير `WALLET_ENCRYPTION_KEY`.

لا تحذف Railway Volume. عند تشغيل V4 لأول مرة سيضيف أعمدة V4 إلى جدول `watches` تلقائيًا مع الاحتفاظ بالبيانات الحالية.

## Variables المطلوبة

نفس إعدادات V3 تعمل بدون تعديل، وأهمها:

```env
OPENSEA_API_KEY=...
ALCHEMY_API_KEY=...
ENABLED_CHAINS=ethereum,ink,robinhood

TELEGRAM_BOT_TOKEN=...
TELEGRAM_ALLOWED_CHAT_IDS=...
TELEGRAM_ALLOW_ANY_CHAT=false

WALLET_ENCRYPTION_KEY=...
DISPLAY_TIMEZONE=Asia/Aden

QUANTITY=1
MAX_PARALLEL_WALLETS=10

ALLOW_PAID_MINTS=true
MAX_MINT_PRICE_NATIVE=0
MAX_GAS_NATIVE=0
MAX_TOTAL_NATIVE=0

GAS_STRATEGY=fast
GAS_LIMIT_BUFFER=1.15

RPC_TIMEOUT=5
RPC_BROADCAST_WORKERS=4
STAGE_REFRESH_SECONDS=20
FAST_STAGE_REFRESH_SECONDS=3
FAST_REFRESH_WINDOW=120
PREOPEN_PROBE_SECONDS=1.5
OPEN_RETRY_INTERVAL=0.30
MONITOR_RETRY_INTERVAL=2
ELIGIBILITY_RETRY_SECONDS=10
RATE_LIMIT_RETRY_SECONDS=5
RECEIPT_CHECK_SECONDS=5
DROP_LIMIT=25
START_PAUSED=false
HTTP_TIMEOUT=7
LOG_LEVEL=INFO
```

`ALLOW_PAID_MINTS=true` يعني أن البوت يسمح بالمنت المدفوع، لكن V4 ما زال يشترط اختيار المحافظ يدويًا لكل مشروع قبل توقيع أي عملية مدفوعة.

## استخدام Telegram

أرسل `/start` لفتح القائمة العربية.

### إضافة محفظة

اضغط «➕ إضافة محفظة»، أدخل اسمًا مثل `Ink-01` ثم أرسل Private Key. يحاول البوت حذف رسالة المفتاح فورًا ويخزنه مشفرًا باستخدام `WALLET_ENCRYPTION_KEY`.

### إدارة المحافظ

من «👛 المحافظ» اضغط اسم المحفظة، ثم يمكنك:

- إيقافها أو تشغيلها.
- تغيير الاسم.
- تغيير كمية الـMint.
- عرض الأرصدة.
- حذفها بعد رسالة تأكيد.

### مراقبة Mint

أرسل رابط OpenSea مباشرة. يقوم البوت بجلب المراحل، الشبكة، موعد Public ويفحص أهلية المحافظ النشطة.

إذا اكتشف مرحلة مدفوعة، تظهر شاشة اختيار المحافظ. يمكنك تحديد المحافظ واحدة واحدة، تحديد الكل، إلغاء التحديد، تأكيد الاختيار، أو تجاهل المدفوع فقط.

## ملاحظات أمان

- استخدم محافظ Mint مخصصة، لا تستخدم محفظتك الرئيسية.
- لا تشارك Private Keys أو Seed Phrase مع أي شخص.
- لا تغير `WALLET_ENCRYPTION_KEY` بعد تخزين المحافظ.
- إذا كانت حدود السعر والغاز مساوية للصفر فهي غير محدودة؛ استخدم أرصدة صغيرة أو ضع حدودًا إذا أردت تقليل المخاطر.

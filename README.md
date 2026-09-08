# OpenSea Mint Guardian V4.6

نسخة إصلاح واجهة Telegram مع الحفاظ على منطق V4.5/V4.4 للمراحل والاكتشاف والـMint.

## أهم إصلاح في V4.6

المشكلة التي ظهرت ابتداءً من V4.4 لم تكن من `callback_data` نفسها. V4.4 أضاف فحوص المراحل/التأهيل إلى الحلقة الرئيسية، وبعض هذه الفحوص ينتظر RPC/OpenSea لكل المحافظ. نتيجة ذلك كانت رسائل `/start` وضغطات Inline Keyboard تصل إلى `command_queue` لكن قد تتأخر معالجتها أثناء انشغال حلقة الـMint.

V4.6 يفصل العمل إلى مسارين مستقلين:

- `TelegramController`: مبني على آلية polling المستخدمة في V4.3/V4.2 (`getUpdates` + message/callback_query).
- `telegram-command-worker`: يستهلك أوامر Telegram في Thread مستقل عن حلقة Mint/Stage.
- حلقة الـMint/Stage تظل مستقلة ولا يمكنها تجويع واجهة Telegram.

عند التشغيل يجب أن ترى:

```text
Telegram command worker ready
Telegram listener enabled
Mint Guardian V4.6 starting
```

وعند إرسال `/start`:

```text
Telegram message received | chat_id=... | text=/start
```

وعند الضغط على زر:

```text
Telegram callback received | chat_id=... | data=...
```

## تنظيم Telegram

القائمة الرئيسية:

- إضافة محفظة / المحافظ
- التأهيل / المراقبة
- المجانية المأخوذة / المنتات المدفوعة
- فحص الأهلية / سجل العمليات
- الشبكات / الإعدادات
- إيقاف/استئناف التنفيذ

### المنتات المدفوعة

يتم اكتشافها وحفظها بصمت. لا يتم إرسال Prompt تلقائي عند الاكتشاف. تظهر فقط عند فتح قسم `💳 المنتات المدفوعة`. تظهر قيمة Native والسعر التقريبي USDT، ولا يتم أي Paid Mint إلا بعد اختيار المحافظ والكميات وتأكيد الخطة.

### المنتات المجانية

تستمر بالعمل تلقائيًا كما في الإصدارات السابقة. قسم `🆓 المجانية المأخوذة` يعرض النتائج المؤكدة عند الطلب فقط.

## إعدادات الغاز الحالية

```env
MAX_GAS_NATIVE=0
MAX_GAS_USD=0.08
GAS_STRATEGY=smart
GAS_LIMIT_BUFFER=1.08
GAS_OVER_BUDGET_RETRY_SECONDS=2
```

## Railway

لا تغيّر `WALLET_ENCRYPTION_KEY` ولا تحذف Railway Volume. استبدل ملفات المشروع ثم Push إلى GitHub ليعمل Railway redeploy.

## ملفات يجب تحديثها

- `main.py`
- `buyer.py`
- `storage.py`
- `requirements.txt`
- `.env.example` اختياري


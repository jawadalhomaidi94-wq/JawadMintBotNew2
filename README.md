# OpenSea Mint Guardian V3

بوت Mint متعدد الشبكات والمحافظ، مخصص للعمل على **Railway** مع **Alchemy** و**Telegram BotFather**.

## أهم ما يفعله

- يدعم Ethereum وInk وRobinhood Chain، ويمكن توسيعه بسهولة لشبكات EVM أخرى.
- يستخدم `ALCHEMY_API_KEY` واحدًا ويكوّن RPC المناسب لكل شبكة تلقائيًا.
- يقبل رابط OpenSea مباشرة من Telegram ويضيفه للمراقبة الدائمة.
- يقرأ مراحل الـDrop، ويعرض موعد الـPublic والـAllowlist/المراحل الأخرى إن ظهرت في OpenSea.
- يبدأ probing قبل وقت الفتح بقليل، ثم يعيد المحاولة بسرعة عند الفتح.
- ينفذ لكل المحافظ بالتوازي بدل التنفيذ المتسلسل.
- يفحص أهلية كل محفظة عبر OpenSea mint preflight؛ `200` تعني أن OpenSea أعادت معاملة جاهزة، `422` تعني أن شرط mint لم يتحقق حاليًا، و`409` تعني أن المرحلة ليست نشطة بعد.
- إذا كانت Allowlist مفتوحة والمحفظة مؤهلة، يمكنه الـmint فورًا ولا ينتظر Public.
- إذا لم تكن المحفظة مؤهلة في المرحلة الحالية، تبقى تحت المراقبة للمرحلة التالية/Public.
- يدعم Free Mint وPaid Mint.
- يدعم `MAX_MINT_PRICE_NATIVE=0` و`MAX_GAS_NATIVE=0` و`MAX_TOTAL_NATIVE=0` بمعنى لا يوجد حد من جهة البوت.
- يقدر الغاز ويتحقق من الرصيد قبل التوقيع.
- يبث نفس المعاملة الموقعة إلى أكثر من RPC إن أضفت RPCs إضافية.
- يرسل تنبيه Telegram عند Submitted ثم Confirmed أو Reverted مع رابط Explorer.
- زر Panic/Pause يوقف التوقيع فورًا مع استمرار المراقبة.
- زر Add Wallet يضيف محفظة جديدة إلى جميع الـwatches الحالية فورًا.
- المفاتيح الخاصة مشفرة داخل SQLite باستخدام Fernet، ولا تخزن كنص واضح في قاعدة البيانات.
- قاعدة البيانات تحفظ المحافظ، الـwatches، وسجل عمليات mint عبر إعادة نشر Railway.
- endpoint `/health` جاهز لـRailway healthcheck.

---

## 1) الملفات

```text
main.py          # orchestration + Telegram UI + watcher
buyer.py         # OpenSea + RPC + eligibility + signing/broadcast
storage.py       # encrypted SQLite persistence
health.py        # Railway /health endpoint
requirements.txt
railway.json
Dockerfile
Procfile
.env.example
```

---

## 2) إنشاء Telegram Bot من BotFather

1. افتح `@BotFather`.
2. أرسل `/newbot`.
3. اختر الاسم والـusername.
4. انسخ Token وضعه في Railway:

```env
TELEGRAM_BOT_TOKEN=123456:ABC...
```

ابدأ محادثة خاصة مع البوت. إذا لم تضف Chat ID بعد، سيخبرك البوت أن المحادثة غير مصرح بها ويعرض رقم Chat ID؛ انسخه إلى:

```env
TELEGRAM_ALLOWED_CHAT_IDS=123456789
```

ثم أعد Deploy.

> يفضل إبقاء `TELEGRAM_ALLOW_ANY_CHAT=false` دائمًا لأن هذا البوت قادر على التوقيع والصرف.

---

## 3) OpenSea API Key

ضع المفتاح في:

```env
OPENSEA_API_KEY=...
```

البوت يستخدم Drops API وendpoint بناء معاملة mint:

```text
POST /api/v2/drops/{slug}/mint
```

العنوان `minter` يتغير لكل محفظة، لذلك فحص الأهلية وبناء transaction يتمان لكل محفظة على حدة.

---

## 4) Alchemy

يكفي غالبًا متغير واحد:

```env
ALCHEMY_API_KEY=YOUR_KEY
```

ويقوم البوت بتكوين:

```text
Ethereum : https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
Ink      : https://ink-mainnet.g.alchemy.com/v2/YOUR_KEY
Robinhood: https://robinhood-mainnet.g.alchemy.com/v2/YOUR_KEY
```

يمكن إضافة RPCs احتياطية:

```env
ETHEREUM_RPC_URLS=https://rpc-2,...
INK_RPC_URLS=https://rpc-2,...
ROBINHOOD_RPC_URLS=https://rpc-2,...
```

Alchemy يوضع أولًا، ثم RPCs التي تضيفها، ثم RPC العام إن كان موجودًا.

---

## 5) إنشاء مفتاح تشفير المحافظ

أنشئه **مرة واحدة فقط**:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

ضع الناتج في Railway:

```env
WALLET_ENCRYPTION_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx=
```

### مهم جدًا

لا تغير هذا المفتاح بعد تخزين محافظ في قاعدة البيانات. تغييره سيجعل البوت غير قادر على فك تشفير المفاتيح القديمة.

احتفظ بنسخة آمنة خارج Railway.

---

## 6) Railway Volume

البوت يحتاج persistent storage حتى لا تختفي المحافظ والـwatches عند Deploy جديد.

في Railway:

1. افتح Service.
2. أضف Volume.
3. Mount path مقترح:

```text
/data
```

Railway يضيف تلقائيًا:

```text
RAILWAY_VOLUME_MOUNT_PATH=/data
```

والبوت سيستخدم تلقائيًا:

```text
/data/mint_guardian.db
```

لا تحتاج لضبط `BOT_DB_PATH` إلا إذا أردت مسارًا خاصًا.

### لا تستخدم أكثر من Replica واحد

Telegram long polling + SQLite Volume مصممان هنا لخدمة واحدة. لا تفعل Horizontal Scaling لهذا البوت بهذه النسخة.

---

## 7) متغيرات Railway الأساسية

ابدأ بهذه:

```env
OPENSEA_API_KEY=...
ALCHEMY_API_KEY=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_ALLOWED_CHAT_IDS=123456789
TELEGRAM_ALLOW_ANY_CHAT=false

WALLET_ENCRYPTION_KEY=...

ENABLED_CHAINS=ethereum,ink,robinhood
DISPLAY_TIMEZONE=Asia/Aden

ALLOW_PAID_MINTS=true
MAX_MINT_PRICE_NATIVE=0
MAX_GAS_NATIVE=0
MAX_TOTAL_NATIVE=0

GAS_STRATEGY=fast
QUANTITY=1
MAX_PARALLEL_WALLETS=10

PREOPEN_PROBE_SECONDS=1.5
OPEN_RETRY_INTERVAL=0.30
FAST_STAGE_REFRESH_SECONDS=3
STAGE_REFRESH_SECONDS=20
RECEIPT_CHECK_SECONDS=5
```

`0` في حدود السعر/الغاز/الإجمالي يعني **unlimited**.

إذا أردت حماية مثلًا Ethereum من غاز شديد الارتفاع:

```env
ETHEREUM_MAX_GAS_NATIVE=0.004
```

مع إبقاء Ink وRobinhood بدون حد:

```env
INK_MAX_GAS_NATIVE=0
ROBINHOOD_MAX_GAS_NATIVE=0
```

---

## 8) التشغيل على Railway

ارفع المشروع إلى GitHub ثم اربطه بـRailway.

`railway.json` جاهز بـ:

```json
{
  "deploy": {
    "startCommand": "python main.py",
    "healthcheckPath": "/health",
    "healthcheckTimeout": 120,
    "restartPolicyType": "ON_FAILURE"
  }
}
```

Railway يحقن `PORT` تلقائيًا، والبوت يستمع عليه للـhealthcheck.

---

## 9) واجهة Telegram

عند `/start` تظهر لوحة:

- `➕ Add Wallet`
- `👛 Wallets`
- `🎯 Watches`
- `🧪 Check Eligibility`
- `🌐 Networks`
- `📜 History`
- `⏸ Pause / ▶ Resume`
- `⚙️ Settings`

### إضافة محفظة

اضغط:

```text
➕ Add Wallet
```

ثم أرسل Private Key لمحفظة mint مخصصة في **المحادثة الخاصة** مع البوت.

البوت:

1. يحاول حذف رسالة المفتاح من Telegram.
2. يتحقق من المفتاح.
3. يستخرج العنوان.
4. يشفر المفتاح بـFernet.
5. يخزنه في SQLite على Railway Volume.
6. يضيف المحفظة لكل الـwatches الموجودة.
7. يفحص أهلها للمشاريع الحالية.
8. إذا كانت مرحلة قابلة للـmint ومؤهلة يبدأ لها تلقائيًا.

> حذف الرسالة من Telegram هو best-effort وليس ضمانًا أن المفتاح لم يمر عبر Telegram. استخدم **محافظ mint مخصصة فقط** ولا تستورد محفظتك الرئيسية أو seed phrase.

### حذف محفظة

```text
/deletewallet 0xYOUR_ADDRESS
```

---

## 10) إرسال Mint

أرسل الرابط مباشرة:

```text
https://opensea.io/collection/example
```

أو:

```text
/watch https://opensea.io/collection/example
```

إذا فشل اكتشاف الشبكة:

```text
/watch ethereum https://opensea.io/collection/example
/watch ink https://opensea.io/collection/example
/watch robinhood https://opensea.io/collection/example
```

سيعرض مثلًا:

```text
🎯 Watching example
Chain: ink
Public opens: 2026-09-08 18:00:00 +03
Wallets: 5
Auto paid mint: ON (paid + free)
Mint-price cap: unlimited

• Allowlist: 2026-09-08 17:00:00 +03 | price=0
• Public: 2026-09-08 18:00:00 +03 | price=0.003
```

ثم يجري Eligibility Matrix لكل المحافظ.

---

## 11) ماذا يعني Eligibility؟

أمثلة:

```text
✅ wallet-1: eligible_now | mint=0 ETH
❌ wallet-2: not_eligible_now
⏳ wallet-3: not_active_yet
```

`eligible_now` لا يعني أن transaction تم إرسالها؛ يعني أن OpenSea استطاعت بناء بيانات mint لهذه المحفظة حاليًا.

عندما يكون التنفيذ مفعّلًا، الخطوة التالية هي estimate gas ثم التوقيع ثم البث.

إذا كانت wallet غير مؤهلة في Allowlist، لا يتم حذفها من الـwatch؛ تعيد المحاولة لاحقًا وتدخل Public عند فتحه.

---

## 12) Paid Mint + High Gas

الإعداد المطلوب لوضعك:

```env
ALLOW_PAID_MINTS=true
MAX_MINT_PRICE_NATIVE=0
MAX_GAS_NATIVE=0
MAX_TOTAL_NATIVE=0
```

هذا يعني أن البوت لا يوقف العملية بسبب السعر أو الغاز من جهته.

### تحذير مهم

هذا الوضع قادر على صرف رصيد المحفظة تلقائيًا. استخدم محافظ فيها فقط الميزانية التي تقبل خسارتها، ولا تمولها بأكثر من المطلوب للمشاريع التي تراقبها.

يمكنك لاحقًا إضافة حد مثل:

```env
MAX_MINT_PRICE_NATIVE=0.02
MAX_TOTAL_NATIVE=0.03
```

---

## 13) سرعة الإطلاق

الإعداد الافتراضي:

```env
PREOPEN_PROBE_SECONDS=1.5
OPEN_RETRY_INTERVAL=0.30
GAS_STRATEGY=fast
MAX_PARALLEL_WALLETS=10
```

لـ10 محافظ، التنفيذ يعمل concurrent.

لا تجعل `OPEN_RETRY_INTERVAL` منخفضًا جدًا لأن OpenSea API قد يرد `429`، والـAPI نفسه هو المسار الذي يبني calldata/target/value لكل محفظة.

---

## 14) إشعارات النجاح

عند البث:

```text
🚀 MINT SUBMITTED
Drop: example
Chain: robinhood
Wallet: wallet-2 0x12ab…cdef
Quantity: 1
Mint value: 0.002 ETH
Max gas estimate: 0.00001 ETH
TX: 0x...
Explorer link
```

وبعد receipt:

```text
✅ MINT CONFIRMED
```

أو:

```text
❌ MINT REVERTED
```

---

## 15) Panic / Pause

في Telegram:

```text
/panic
```

أو زر `⏸ Pause`.

هذا يوقف توقيع وإرسال أي mint جديد، لكن البوت يبقى يراقب المواعيد.

للتشغيل:

```text
/resume
```

---

## 16) أوامر Telegram

```text
/start
/menu
/status
/wallets
/chains
/history
/eligibility
/watch [chain] <url-or-slug>
/remove <slug>
/deletewallet <0xaddress>
/pause
/panic
/resume
/cancel
```

---

## 17) أمان التشغيل

- لا ترفع `.env` إلى GitHub.
- لا تستخدم seed phrase في البوت.
- استخدم Private Key لمحفظة mint منفصلة.
- لا تستخدم المحفظة التي تحفظ فيها أصولك الأساسية.
- احتفظ بنسخة من `WALLET_ENCRYPTION_KEY` بمكان آمن.
- اجعل Telegram control مقتصرًا على Chat ID الخاص بك.
- لا تفعل `TELEGRAM_ALLOW_ANY_CHAT=true`.
- استخدم Railway Volume للـDB.
- راقب رصيد المحافظ لأن Paid Mint + unlimited caps يستطيع صرف الرصيد المتاح.

---

## 18) اختبار محلي

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python main.py
```

---

## 19) ملاحظات التصميم

هذه النسخة تعتمد على OpenSea لبناء mint transaction. هذا مهم لأن Drop يمكن أن يستخدم SeaDrop أو منطق مرحلة/allowlist مختلف، وOpenSea backend يختار المرحلة النشطة المؤهلة للـminter ويعيد `target + calldata + value` الجاهزة للتوقيع.

كل محفظة تحصل على طلب build-mint مستقل، ثم transaction مستقل، ثم nonce مستقل، لذلك دعم multi-wallet حقيقي وليس مجرد إرسال NFT لمحفظة واحدة ثم توزيعه لاحقًا.

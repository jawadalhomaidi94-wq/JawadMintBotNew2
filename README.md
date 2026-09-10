# OpenSea Mint Guardian V4.12.0 — Isolated Collection Offers

V4.12.0 is built directly on the uploaded working V4.11.3 codebase. The existing Mint/Race behavior is intentionally preserved: OpenSea Stream discovery, SeaDrop WSS discovery, Race signal coalescing, prewarm/signing, nonce-safe parallel broadcast, stage/qualification planning, Protected Free Mint Shield, gas budgets, SQLite persistence, Railway Volume behavior, and Telegram wallet management remain in place.

The new feature is a separate, user-triggered **Collection Offer subsystem**. It does not poll from `Bot.run()`, does not register with the Race scheduler, and does not share the Race signal/prep/launch executors.

## New Telegram Offer flow

Collection Offers can be opened from:

- `💳 المنتات المدفوعة` → choose a project → `💰 تقديم Collection Offer`
- `🎟 التأهيل` → `💰 تقديم Collection Offer` → choose a qualification project
- Main menu → `📨 عروضي الحالية`
- `/offers`

Typical flow:

```text
💳 المنتات المدفوعة / 🎟 التأهيل
        ↓
اختر المشروع
        ↓
💰 تقديم Collection Offer
        ↓
📊 Top 3 Collection Offers الحالية
        ↓
[🏆 Top Offer] [✍️ عرض مخصص]
[السعر 1] [السعر 2] [السعر 3]
        ↓
👛 اختيار المحافظ
        ↓
🔢 كمية مستقلة لكل محفظة
        ↓
🔎 مراجعة WETH + Approval + المدة
        ↓
[✅ تأكيد تقديم العرض]
        ↓
EIP-712 sign + OpenSea POST
```

Pressing `🏆 Top Offer` does **not** sign or submit anything. The bot fetches the live collection offers, reads the current top price, adds the configured USDT increment, and shows the proposal. The current Top Offer is checked again immediately before final signing. If it changed, signing is stopped and the user must review and confirm the new price again.

## USDT UI, WETH execution

The Telegram interface accepts and displays offer prices in USDT-style USD values. At review/signing time the bot obtains a fresh ETH/USD price and converts the commitment to WETH:

```text
💵 Offer: 12.50 USDT / NFT
Ξ Actual OpenSea value: ≈ 0.00317 WETH / NFT
👛 Wallet: Wallet-1
🔢 Quantity: 2
⏳ Duration: 24 hours
```

The Seaport order itself uses WETH. No WETH is transferred when the offer is merely created; it is a signed marketplace order that can be fulfilled later.

## Top Offer increment settings

Telegram → `⚙️ الإعدادات` → `💰 Offers`:

```text
0.01 USDT
0.05 USDT
0.10 USDT
0.25 USDT
✏️ custom
```

Default: `0.05 USDT`.

Offer duration is also configurable. Default: `24 hours`.

Both values are persisted in SQLite and survive Railway restarts/redeploys.

## Wallet funding and WETH Approval

Before final confirmation, each selected wallet is checked independently:

- WETH balance
- WETH allowance to the OpenSea conduit
- required WETH for `price × quantity`
- wallet enabled state / chain support

If WETH is insufficient, the Offer cannot be signed. If allowance is insufficient, Telegram shows an explicit `🔓 Approval` button. Approval is **never automatic** and is a separate on-chain transaction. V4.12.0 approves only the reviewed amount rather than granting an unlimited allowance.

The Approval transaction respects the bot's existing native/USD gas limits.

## Real OpenSea Collection Offers

The implementation uses the current OpenSea V2 criteria-offer flow:

- `POST /api/v2/offers/build`
- `POST /api/v2/offers`
- `GET /api/v2/offers/collection/{slug}`
- `GET /api/v2/offers/collection/{slug}/all`
- `GET /api/v2/account/{address}/offers`
- `POST /api/v2/orders/chain/{chain}/protocol/{protocol_address}/{order_hash}/cancel`

Orders are built for Seaport 1.6 and use OpenSea offer protection. Required collection fees returned by the Collection API are added to the Seaport consideration. Existing older Seaport 1.5 order addresses are recognized when signing an off-chain cancellation.

## My Offers

`📨 عروضي الحالية` refreshes active offers from OpenSea for the enabled wallets and reconciles them into the separate local `offer_history` table.

Each offer supports:

- details
- off-chain SignedZone cancellation
- raise/reprice flow

For `⬆️ رفع العرض`, V4.12.0 refreshes market prices and requires a new confirmation. The previous active offer is cancelled before the replacement is posted so the bot does not intentionally leave two active commitments for the same raise operation.

## Offer storage is separate from Mint storage

V4.12.0 adds:

```text
offer_history
```

It does not reuse `mint_history`, qualification quantity accounting, watch history, or Race state. This prevents Offers from changing cumulative Mint calculations or duplicate-Mint protection.

## Race Lane isolation

The Offer module lives in `offers.py` and has its own executor:

```text
Offer System
  offer-api executor       (read-only/background UI work)
  offer-sign executor      (approval/final sign/cancel work)
  per-session offer price snapshot/cache
  OpenSea offer reads/build/post
  WETH funding/approval checks
  EIP-712 offer signing
  offer_history

Race Lane (unchanged critical path)
  race-signal
  race-prep
  race-launch
  SeaDrop WSS
  OpenSea Stream
```

There is deliberately no Offer timer, no Offer polling worker, and no Offer call in:

- `_race_scheduler_loop()`
- `_launch_candidate_race()`
- `_fast_live_contract_signal()`
- `_queue_mint_event()`

Offer GET/build calls use the existing OpenSea client's **background** REST class, preserving the configured REST reserve for Mint/stage work. The final user-confirmed Offer POST uses normal priority; it never receives Race/critical priority.

The existing Race settings remain unchanged:

```env
RACE_PREWARM_SECONDS=2.5
RACE_SCHEDULER_TICK=0.005
RACE_RETRY_SECONDS=0.04
RACE_STREAM_WORKERS=24
RACE_PREP_WORKERS=8
RACE_LAUNCH_WORKERS=8
RACE_GAS_STRATEGY=fast
RACE_FEE_REFRESH_SECONDS=0.35
SEADROP_WSS_DISCOVERY=true
```

## New optional environment variables

No new variable is required for normal Offer creation if the existing OpenSea API key, RPCs, Alchemy key and wallet encryption configuration are already present.

Optional:

```env
# Dedicated Offer read/API executor only; default 2, max 4.
OFFER_API_WORKERS=2

# Dedicated financial/signing executor; default 1, max 2.
OFFER_SIGN_WORKERS=1

# Optional wallet-scoped OpenSea token for deployments/accounts that require it
# for an authenticated off-chain order action. Normal SignedZone cancellation
# is attempted with X-API-KEY + offererSignature.
OPENSEA_SCOPED_TOKEN=
```

Keep the existing `WALLET_ENCRYPTION_KEY` unchanged when upgrading, otherwise previously stored private keys cannot be decrypted.

## Files

```text
main.py       existing bot + minimal Offer UI integration points
buyer.py      existing Mint/RPC code + OpenSea Offer API methods
offers.py     NEW isolated Collection Offer service/controller
storage.py    existing DB + separate offer_history persistence
health.py     unchanged
railway.json  unchanged
requirements.txt
README.md
CHANGELOG.md
VERSION
```

## Upgrade / Railway

1. Back up the current Railway Volume/database.
2. Keep the same `WALLET_ENCRYPTION_KEY`.
3. Replace the project files with this package.
4. Redeploy normally with the existing `railway.json`.
5. On startup, SQLite creates `offer_history` automatically without deleting the existing tables.
6. Open Telegram → `⚙️ الإعدادات` → `💰 Offers` to review the Top increment and duration.

Expected startup logs include:

```text
Mint Guardian V4.12.0 starting
RACE LANE V4.11.3 STABLE (unchanged) ready
Race signal coalescer ready
Protected Free Mint Shield ready
Offer subsystem V4.12.0 ready | ... | loop-hooks=0
Collection Offers ready | isolated-executor=True | main-loop-polling=False | Race-hooks=0
```

## Important execution policy

- Free Mint behavior remains automatic according to the existing V4.11.3 policies.
- Paid Mint still requires the existing explicit paid-Mint plan confirmation.
- Collection Offers are always user-triggered.
- Selecting Top Offer does not sign.
- Selecting a price does not sign.
- Selecting wallets/quantity does not sign.
- WETH Approval requires its own explicit button press.
- The Offer itself requires the final `✅ تأكيد تقديم العرض` action.


## V4.13.0 — USDT display and Wallet balances

Telegram now displays native-token amounts together with an approximate USDT equivalent across qualification, paid/free mint, gas/fee, eligibility, history, and Offer financial messages. The Wallets section includes a manual `💰 عرض الرصيد` action that reads balances on enabled networks and shows the native balance plus its approximate USDT value. Balance reads are isolated from the mint Main Loop and Race Lane.


## Ultra Race V4.13.0
Known stages prewarm earlier. Fast live signals reuse their already-read SeaDrop public configuration, and free live Race uses the configured static race gas limit so estimateGas is not in front of broadcast. The existing protection/qualification decisions are unchanged.

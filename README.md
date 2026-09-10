# OpenSea Mint Guardian V4.14.2 — Stability Fix

V4.14.2 is a focused stability release on top of V4.14.1 Direct Fan-Out. It preserves the same Admin-first direct in-memory Race path and per-user isolation while fixing the production traceback observed after a successful tenant mint and preventing repeated on-chain limit checks for the same wallet/stage.

## V4.14.2 fixes

- Fixed `AttributeError: 'StoredWallet' object has no attribute 'supports_chain'` in final-Public completion/accounting. Persisted `StoredWallet` and runtime `WalletConfig` now share compatible chain checks, and `main.py` uses a defensive compatibility helper at wallet boundaries.
- Same-stage terminal cache: when the chain returns `Wallet already reached the on-chain public mint limit`, `sold out`, or `no on-chain supply remains`, that wallet is final for that exact stage and duplicate Stream/SeaDrop signals do not re-run the precondition RPC.
- A genuinely new stage automatically clears the terminal guard, so qualification/future Public stages still work normally.
- Race work now skips `state.final` wallets, closing the repeated `RACE no-submit ... Wallet already reached...` loop seen in production logs.
- Reverted transactions are also kept final for the same stage instead of being reopened by duplicate live signals.
- Added candidate-level exception isolation in the main tenant loop: an unexpected project-specific exception is logged and isolated instead of terminating the entire tenant runtime thread.
- Added startup marker: `V4.14.2 stability guards ready | stored-wallet-compat=True | same-stage-terminal-cache=True | candidate-isolation=True`.

## Speed / behavior preserved

- Direct tenant handoff remains zero-poll for live events.
- Admin Race task is still queued first, followed immediately by tenant fan-out.
- Tenants still reuse the Admin-resolved Public/SeaDrop snapshot; no extra SeaDrop read is added to the direct hot path.
- Per-user Safe Protection, gas caps, notifications, pause/resume, permissions, wallet ownership and databases remain independent.
- Qualification and Final Public behavior remain unchanged except for eliminating same-stage duplicate retries after a terminal on-chain result.

---

# OpenSea Mint Guardian V4.14.1 — Direct Multi-User Race Fan-Out

V4.14.1 is a latency-focused upgrade over V4.14.0. The multi-user model remains unchanged, but live global mint signals no longer wait for the tenant mirror loop. Admin performs the Stream/SeaDrop resolution once, queues Admin first, and directly pushes the already-resolved stage into every active tenant Race lane.

## V4.14.1 direct path

```text
OpenSea Stream / SeaDrop WSS
            │
            ▼
     Admin signal resolver
     (one SeaDrop read)
            │
      ┌─────┴───────────────┐
      │                     │
      ▼                     ▼
Admin Race queued       Direct RAM fan-out
first                   to active tenants
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                  User A    User B    User C
                  own gas   own gas   own gas
                  own safe  own safe  own safe
                  own wallets / DB / notifications
```

Important behavior:
- No tenant SeaDrop re-read is inserted in front of a direct live Free Mint.
- No tenant SQLite wallet sync is inserted in front of the direct live broadcast.
- Admin's launch task is queued before user fan-out; there is no artificial sleep between Admin and users.
- A user's `Free Mint Shield`, gas caps, permissions and pause state are evaluated only for that user.
- Safe Protection is still per-user, but project social identity is resolved once globally by Admin whenever at least one active protected tenant needs it. Protection-OFF users launch without waiting; protection-ON users consume the shared PASS/REJECT result and never duplicate the OpenSea social REST call.
- A social PASS wakes only protected tenants and reuses the already-resolved Public SeaDrop snapshot, so the shield does not add another SeaDrop RPC read.
- Qualification/drop metadata wakes tenant planners immediately.
- When Final Public opens, stale allowlist backoff is cleared in RAM before launch and all active compatible wallets become eligible for the Public Race according to the existing cumulative quantity rules.
- A late allowlist eligibility response is discarded if Public has already taken over, preventing cross-stage state corruption.
- Periodic tenant mirroring remains only as a 1-second default recovery fallback and is not the normal speed path.

Optional recovery tuning:

```env
TENANT_SHARED_SYNC_FALLBACK_SECONDS=1.0
```

There is normally no reason to lower this value for speed; live/future stage updates are pushed directly.

---

# OpenSea Mint Guardian V4.13.1 — Ultra Race Recovery

V4.13.1 is an in-place performance/reliability upgrade over the working V4.12.x line. It preserves the existing Free Mint, qualification, Safe Protection, paid-mint confirmation, multi-wallet, Collection Offer, Telegram, SQLite, and Railway behavior. The changes are concentrated around Race latency, gas/balance recovery, stale fee handling, 24-hour visible history, and cache maintenance.

## What V4.13.1 fixes

### 1. Native-gas top-up auto resume

`insufficient_balance` no longer behaves like a sticky failure. Whenever any mint path (prewarm, live Race, scheduled Race, or fallback execution) sees insufficient native gas, the wallet enters a dedicated RAM-only balance watcher.

- Default recheck: `0.35s`.
- Reads are deduplicated by `(chain, wallet)`: several blocked projects for the same wallet still use one balance read per cycle.
- Any real balance increase wakes a fresh mint attempt immediately while the stage is open.
- If the balance reaches the previously reported requirement, it also wakes immediately.
- The old prepared transaction is discarded and rebuilt so nonce/gas are current.
- Healthy wallets never wait for this watcher and Race executors are not shared with it.

Optional tuning:

```env
LOW_BALANCE_RECHECK_SECONDS=0.35
LOW_BALANCE_RETRY_SECONDS=0.35
```

The code accepts a minimum of `0.15s`, but very aggressive values can rate-limit public RPC providers.

### 2. Stale prewarm gas fee recovery

A pre-signed transaction can become invalid if `baseFee` rises between prewarm and the Public opening. V4.13.1:

- refreshes prepared fee fields from the already-warmed in-memory fee snapshot before broadcast, without another network request;
- classifies `max fee per gas less than block base fee` separately;
- if a provider rejects a stale fee, performs one live fee refresh, re-signs locally, and re-broadcasts immediately;
- never intentionally clamps `maxFeePerGas` below a `baseFee` explicitly reported by the provider;
- if the configured native/USD gas budget cannot fund even the current base fee, it reports a gas-budget status instead of repeatedly sending an invalid transaction.

### 3. Faster live Free Mint hot path

The Stream/SeaDrop signal already reads the public SeaDrop configuration. Earlier code then entered Race and read the same configuration a second time. V4.13.1 passes the first verified result directly into Race.

This removes one duplicate RPC round-trip from the live path without changing mint eligibility, quantity, gas policy, Safe Protection, or target checks.

The fee warmer also uses a persistent executor instead of creating a new ThreadPool every refresh cycle, reducing scheduler/GC noise near the Race workers.

Default Race values:

```env
RACE_PREWARM_SECONDS=6.0
RACE_SCHEDULER_TICK=0.005
RACE_RETRY_SECONDS=0.04
RACE_PUBLIC_GAS_LIMIT=300000
RACE_FEE_REFRESH_SECONDS=0.20
RACE_STREAM_WORKERS=24
RACE_PREP_WORKERS=8
RACE_LAUNCH_WORKERS=8
RACE_GAS_STRATEGY=fast
SEADROP_WSS_DISCOVERY=true
```

### 4. Final Public priority for qualification projects

If an allowlist/qualification stage overlaps the final Public stage, the active Public stage now takes priority. This prevents an older qualification stage from hiding a newly opened Public.

When a free Public opens:

- all active compatible wallets are activated immediately;
- the previous-stage low-balance state is cleared;
- cumulative confirmed quantity is preserved;
- a wallet that already minted during qualification only requests the additional amount still available/required for the Public;
- no OpenSea eligibility REST preflight is placed in front of a free Public Race.

### 5. Final Public completion / monitoring cleanup

After the final Public mint is confirmed and all active wallets are resolved, the project is immediately archived from active Monitoring/Qualification. The confirmed mint remains visible in `🆓 المجانية المأخوذة` / history for the configured 24-hour window.

A project is **not** prematurely archived while another active wallet is still recoverable because of low balance, gas budget, pending receipt, or another retryable condition.

### 6. 24-hour visible Mint history without losing cumulative totals

Old `mint_history` rows are compacted into `mint_totals` before deletion. Therefore:

- visible operation/mint history is automatically cleaned after 24 hours by default;
- cumulative per-wallet/per-project mint totals remain correct after cleanup;
- old confirmed quantities still prevent accidental over-minting later.

Optional:

```env
MINT_HISTORY_RETENTION_SECONDS=86400
MAINTENANCE_INTERVAL_SECONDS=300
CACHE_RETENTION_SECONDS=3600
```

### 7. Safe Protection remains enabled and faster where possible

The rule remains unchanged for ordinary auto-discovered Free Mints without qualification:

```text
X account OR website must be present
```

Qualification/manual projects keep their existing exemptions.

V4.13.1 also:

- reuses X/website identity already present in discovery metadata instead of making a duplicate Collection REST request;
- when OpenSea responds with `429`, respects the actual OpenSea cooldown instead of retrying the same social lookup every few seconds;
- does not weaken a rejection (`X=False` and `website=False`).

### 8. USD price oracle fallback

Alchemy remains the first native/USD source. If it is temporarily unavailable/rate-limited, the bot can fall back to Coinbase and then Binance. Race launch itself still uses warmed/cached price data so HTTP price discovery does not sit in front of broadcast.

### 9. Ethereum RPC fallback

Ethereum now includes this fallback after configured/Alchemy RPCs:

```text
https://ethereum-rpc.publicnode.com
```

A dedicated provider should still be configured for latency-sensitive production/Race use.

### 10. Manual live wallet balance view

Telegram `👛 المحافظ` now includes `💰 عرض الأرصدة الآن`, plus a per-wallet `🔄 تحديث الرصيد`. This is manual-only and not connected to Race/discovery loops. Where an ETH/USD price is available it also displays the approximate USDT value.

## Collection Offers

The V4.12 Collection Offer subsystem is preserved unchanged and isolated:

- Telegram price entry/display in USDT-style USD;
- actual Seaport offer in WETH;
- Top Offer + configurable increment;
- final confirmation before signing;
- multi-wallet quantities;
- WETH balance/allowance checks and explicit Approval;
- current account offers from OpenSea;
- cancel and raise;
- separate `offer_history` table;
- separate `offer-api` and `offer-sign` executors;
- no Offer polling/hook in Race Lane.

## Reading Railway logs

Expected non-fatal states include:

- `Free social trust rejected ... X=False | website=False` — Safe Protection intentionally rejected that ordinary auto Free Mint.
- `precondition_failed ... Wallet already reached ...` — wallet already reached the on-chain limit.
- `Sold out according to on-chain getMintStats` — no supply remains.
- OpenSea Stream `4002 ... Service restarting` followed by `OpenSea Stream connected` — automatic reconnect.
- provider `429` — RPC/API rate limit; fallback/provider configuration matters.

Important recovery logs in this release:

```text
Low-balance auto-resume ready | recheck=0.35s | isolated=True
Gas top-up detected | ...
Race fee refreshed | ...
FINAL PUBLIC COMPLETE | ... | archived=True
Maintenance cleanup | ...
```

## Upgrade on Railway

1. Back up the Railway Volume/database.
2. Keep exactly the same `WALLET_ENCRYPTION_KEY`; changing it makes existing encrypted private keys unreadable.
3. Replace the project files with this release.
4. Keep your current environment settings unless you intentionally want to tune the new optional values above.
5. Redeploy with the included `railway.json` (`python main.py`).
6. Check startup logs for working RPC pools. A chain with no verified RPC cannot mint on that chain.

## Files

```text
main.py
buyer.py
offers.py
storage.py
health.py
requirements.txt
railway.json
README.md
CHANGELOG.md
RELEASE_VALIDATION.md
VERSION
```

## Security / execution behavior retained

- Private keys remain Fernet-encrypted in SQLite.
- Telegram raw private-key input is not logged.
- Pause stops signing/broadcast while monitoring continues.
- Paid Mint still requires the existing explicit paid plan.
- Collection Offers still require explicit user confirmation.
- Free Safe Protection policy is unchanged.
- Gas/native/total-spend guards remain enforced according to the configured policy.

## V4.14.0 Multi-User
The Railway `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ALLOWED_CHAT_IDS` remain the Admin control plane. Open **👥 إدارة المستخدمين** to create a tenant using username, numeric Telegram ID, the tenant's Telegram Bot Token and wallet limit. The tenant bot is restricted to that exact Telegram ID.

Each tenant receives its own SQLite database (`tenant_<id>.db`) and therefore its own wallets, watches, qualification state, history, offers and persisted settings. Admin remains unlimited. User wallet limits are enforced server-side.

Global OpenSea/SeaDrop discovery stays on the Admin engine and is mirrored in RAM to tenants. Tenant engines reuse Admin's verified RPC pools, price oracle and fee cache. This is intentional: adding users does not create one OpenSea catalog scanner or fee warmer per user. Manual watch/eligibility actions remain private to the user who requested them.

Per-user settings include gas limits, Free Mint Shield, notifications and independent execution pause. Suspending a user removes only that tenant from execution; it does not pause Admin or other tenants.

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

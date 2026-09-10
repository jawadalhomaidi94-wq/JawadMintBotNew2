# OpenSea Mint Guardian V4.11.3 — Race Signal Coalescer

V4.11.3 is built directly on the working V4.11.2 package. It keeps the same transaction builder, nonce coordination, Protected Free Mint Shield, balance alerts, Telegram UI, SQLite persistence, Railway Volume behavior, and Race timing.

## New: stronger duplicate-event suppression without slowing the first signal

Dense OpenSea `item_transferred` bursts and SeaDrop WSS logs can repeatedly point to the same `(chain + contract + stage)`. V4.11.2 could therefore spend several Race workers doing the same `getPublicDrop`/SeaDrop read while the real mint path was already active.

V4.11.3 adds a RAM-only signal coalescer:

1. The **first** signal for a contract still enters the Race lane immediately.
2. While that contract is being resolved, all additional Stream/SeaDrop signals are merged into one pending hint instead of starting more workers.
3. Once the SeaDrop stage fingerprint is known, duplicate signals for the same stage are suppressed for a short source-specific quiet window.
4. OpenSea Stream duplicates use a longer quiet window because `item_transferred` mainly represents mint activity, not a new drop configuration.
5. SeaDrop WSS uses a shorter quiet window so an actual on-chain configuration change can still be detected quickly.
6. `social-pass` is never suppressed. It remains a high-priority execution-unlock event and bypasses the stage quiet window.
7. Scheduled Race retries are independent of this coalescer and keep the same `RACE_RETRY_SECONDS=0.04` timing.

This means event storms no longer queue many redundant RPC reads in front of unrelated projects, while first-discovery latency remains unchanged apart from a tiny in-memory lock/check.

Default coalescer values:

```env
RACE_SIGNAL_STREAM_QUIET_SECONDS=1.50
RACE_SIGNAL_SEADROP_QUIET_SECONDS=0.40
RACE_SIGNAL_UNKNOWN_QUIET_SECONDS=0.20
```

You do not need to add these variables to Railway unless you want to override the defaults.

Expected startup diagnostic:

```text
Race signal coalescer ready | single-flight=True | stream-quiet=1.50s | seadrop-quiet=0.40s | unknown-quiet=0.20s
```

## Existing V4.11.2 behavior preserved

- Protected Free Mint Shield is ON by default and accepts X **or** external website.
- `social-pass` immediately wakes the fast contract path even if OpenSea stage metadata arrived later.
- Manual watches and qualification/allowlist projects are exempt from the social shield.
- Paid Public still requires explicit approval, wallet selection, and quantity.
- `getMintStats(minter)` limits quantity to the wallet's actual remaining on-chain SeaDrop allowance.
- Sold-out or already-at-limit wallets are skipped before a guaranteed-revert transaction is broadcast.
- Insufficient native gas balance is classified as `insufficient_balance` and always sends a Telegram alert with project, network, and wallet.
- Pause/Panic stops Race signing and broadcasting while monitoring/discovery continue.
- Telegram private input and RPC/API credentials remain redacted from logs.
- Same-wallet concurrent Race transactions remain nonce-safe and retry once with the latest pending nonce on an actual `nonce too low` response.
- Auto-Free `ThreadPoolExecutor(max_workers=0)` crash remains fixed.

## Race speed preserved

The speed settings are unchanged:

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

The coalescer does **not** sit in front of the scheduled 40ms Race retry loop. Its purpose is only to stop duplicate discovery events from consuming extra signal workers and RPC reads.

## Expected startup logs

```text
Mint Guardian V4.11.3 starting
RACE LANE V4.11.3 STABLE ready
Race signal coalescer ready | single-flight=True | stream-quiet=1.50s | seadrop-quiet=0.40s | unknown-quiet=0.20s
Protected Free Mint Shield ready | enabled=True | rule=X-or-website
```

## Deployment

Replace the project files with this package and redeploy through GitHub/Railway. Keep the existing Railway Volume and keep the existing `WALLET_ENCRYPTION_KEY` unchanged.

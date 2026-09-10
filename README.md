# OpenSea Mint Guardian V4.11.2 — Protected Race + Nonce Sync Hotfix

V4.11.2 is built directly on the working V4.11.1 package. It keeps the same Race timing, stable Web3/RpcPool broadcast path, Protected Free Mint Shield, wallet-limit checks, balance alerts, Telegram UI, SQLite persistence, and Railway-volume behavior.

## Fixed: protected Free Mint passes X/Website but never launches

The social check can finish before OpenSea `stage_plans` are populated. In V4.11.1 a PASS only launched immediately when `current_plan_for_candidate()` already returned an active Public stage. That could leave a valid protected free mint waiting until another Stream/catalog event.

V4.11.2 keeps the fast direct launch when a current Public plan is already known, and adds a mandatory fallback:

1. X/Website check returns PASS.
2. Active wallets are synchronized and marked due.
3. If an active Public plan already exists, Race launches immediately.
4. Otherwise the PASS itself is submitted as a new `social-pass` fast contract signal.
5. SeaDrop is read on-chain immediately; if Public is live, Race launches without waiting for another catalog scan or Stream event.

A `social-pass` wake is exempt from the normal 200ms fast-signal dedupe because it is an execution-unlock event rather than duplicate discovery noise.

Expected diagnostic log when the fallback is used:

```text
Social PASS fast wake | <project> | <chain> | contract=0x....
```

## Fixed: concurrent mints can reuse a stale nonce

Multiple projects can prewarm transactions for the same wallet using the same pending nonce. If another mint broadcasts first, the older pre-signed transaction can be rejected with `nonce too low`.

V4.11.2 adds nonce-safe serialization **only per wallet + chain** while preserving parallel broadcasting across different wallets:

- the original V4.11.1 raw transaction is sent with zero extra nonce RPC when no local conflict is known;
- after this process successfully broadcasts a transaction, a local next-nonce floor is updated;
- another prewarmed transaction from the same wallet is re-signed immediately with that next nonce before broadcast;
- if the RPC still returns `nonce too low` (for example because of an external wallet transaction), the bot reads the current pending nonce, re-signs, and retries once immediately;
- underlying transmission still uses the proven `RpcPool.broadcast_raw_transaction()` path. No V4.10 raw-HTTP broadcaster was reintroduced.

Possible diagnostic logs:

```text
Race nonce refreshed | chain=... | wallet=0x... | old=36 | new=37
Race nonce retry | chain=... | wallet=0x... | nonce=37
```

## Existing V4.11.1 fixes preserved

- Protected Free Mint Shield is ON by default and accepts X **or** external website.
- Manual watches and qualification/allowlist projects are exempt from the social shield.
- Paid Public requires explicit approval, wallet selection, and quantity.
- `getMintStats(minter)` is used to reduce quantity to the wallet's remaining on-chain SeaDrop allowance.
- A wallet already at its public cap is skipped without broadcasting a guaranteed-revert transaction.
- Insufficient native gas balance is classified as `insufficient_balance` and always sends a Telegram alert containing project, network, and wallet.
- Pause/Panic stops Race signing and broadcasting; monitoring/discovery remain active.
- Telegram private input and RPC API credentials are redacted from logs.
- Auto-Free `ThreadPoolExecutor(max_workers=0)` crash remains fixed.

## Race speed preserved

Defaults remain:

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

The nonce fix does **not** add a fresh nonce request in front of every normal Race send. A network pending-nonce request is added only after an actual provider `nonce too low` response. Same-wallet concurrent transactions are coordinated locally; different wallets remain parallel.

## Expected startup logs

```text
Mint Guardian V4.11.2 starting
RACE LANE V4.11.2 STABLE ready
Protected Free Mint Shield ready | enabled=True | rule=X-or-website
```

## Deployment

Replace the project files with this package and redeploy through GitHub/Railway. Keep the existing Railway Volume and keep the existing `WALLET_ENCRYPTION_KEY` unchanged.

# OpenSea Mint Guardian V4.10.1 — Stable Speed Hotfix

This release is intentionally based on the known-working V4.9 transaction path.

## Why V4.10 failed

Railway logs showed two regressions:

1. Auto-free REST discovery could create `ThreadPoolExecutor(max_workers=0)` when catalog filtering produced an empty list.
2. The experimental V4.10 race transaction/RPC path repeatedly returned `rpc_or_tx_error` even though Stream discovery was fast.

## What V4.10.1 does

- Restores `buyer.py` exactly to the V4.9 transaction construction/signing/broadcast behavior.
- Removes the V4.10 experimental raw-HTTP broadcaster, wallet JSON-RPC batching, manual calldata cache, and Ethereum pending-transaction lane.
- Keeps the V4.9 OpenSea Stream + SeaDrop + scheduled Public race logic.
- Separates live signal, prewarm, and scheduled launch into independent thread pools so discovery/prewarm work cannot queue in front of a ready Public launch.
- Fixes auto-free detail discovery so an empty filtered set returns cleanly and never constructs a zero-worker executor.
- Keeps all Telegram/UI, qualification, monitoring, paid-mint, per-project gas, SQLite, and Railway Volume behavior from V4.9.
- Adds the first failure detail to `RACE no-submit` logs for diagnostics.

## Stable race defaults

```env
RACE_LANE_ENABLED=true
RACE_PREWARM_SECONDS=2.5
RACE_SCHEDULER_TICK=0.005
RACE_RETRY_SECONDS=0.04
RACE_LAUNCH_WINDOW_SECONDS=8
RACE_STREAM_WORKERS=24
RACE_PREP_WORKERS=8
RACE_LAUNCH_WORKERS=8
RACE_GAS_STRATEGY=fast
RACE_FEE_REFRESH_SECONDS=0.35
RPC_BROADCAST_WORKERS=4
```

Existing Railway environment values override these defaults. `RPC_BROADCAST_WORKERS=4` is the known-working V4.9 setting.

## Deployment

Replace `main.py`, `buyer.py`, and `storage.py` (and `requirements.txt` if desired), commit/push, then let Railway redeploy.

Do **not** change `WALLET_ENCRYPTION_KEY` and do **not** delete the Railway Volume.

Expected startup line:

```text
RACE LANE V4.10.1 STABLE ready
```

The auto-free discovery error `max_workers must be greater than 0` should disappear completely.

# OpenSea Mint Guardian V4.10 — Ultra Race II

V4.10 is a speed-focused upgrade built directly on V4.9. It preserves the Telegram UI, wallets, stage monitoring, qualification history, paid-mint approval, per-project gas overrides, SQLite persistence, OpenSea Stream, Mint Events, Drops discovery and SeaDrop direct minting.

## What is faster in V4.10

### 1. Three isolated race executors
The hot path is no longer shared with preparation work:
- `race-signal`: live Stream / mempool signals.
- `race-prep`: pre-signing and scheduled preparation.
- `race-launch`: scheduled launch/broadcast only.

Heavy discovery or prewarming therefore cannot queue ahead of a prepared Public mint.

### 2. Ethereum SeaDrop mempool discovery
When enabled, Ethereum Mainnet subscribes to Alchemy `alchemy_pendingTransactions` filtered to the SeaDrop contract. For `mintPublic(...)` transactions, V4.10 extracts the NFT contract from calldata and starts the fast path before waiting for a mined `item_transferred` event.

This is Ethereum-only because filtered Alchemy pending transactions are not available on all enabled chains. Existing OpenSea Stream and SeaDrop log discovery remain active as fallbacks.

### 3. No duplicate SeaDrop public read on live signals
A live signal now reads `getPublicDrop` once and passes that exact snapshot into the launch function. V4.9 could read it once in the signal handler and again in the launch path.

### 4. No live `estimateGas` wait by default
`RACE_LIVE_STATIC_GAS=true` uses a conservative quantity-aware gas limit in the urgent live path. This removes an RPC simulation round-trip before signing/broadcasting. Unused gas is not charged merely because the gas limit is higher.

The static limit begins at `RACE_PUBLIC_GAS_LIMIT` and grows by `RACE_GAS_PER_EXTRA_TOKEN` per additional NFT, capped by `RACE_MAX_STATIC_GAS`.

### 5. Batched wallet runtime reads
Pending nonce and balance reads for all wallets are sent as a single JSON-RPC batch when the provider supports it. If batching is unavailable, V4.10 automatically falls back to parallel RPC calls.

### 6. Cached signing accounts and calldata
Private keys are parsed to signing accounts once per process, and identical `mintPublic` calldata is cached by project/fee-recipient/quantity. This removes repeated CPU work from multi-wallet races.

### 7. Direct persistent-HTTP raw transaction fanout
Raw transactions are sent directly with persistent HTTP sessions to the verified RPC pool instead of adding another Web3.py serialization layer. The same signed transaction is fanned out to the available RPCs at once.

After the first accepted broadcast, short quiet re-broadcast waves are scheduled by default (`35ms`, `120ms`) to improve propagation without delaying the first send.

## Default race settings

```env
RACE_LANE_ENABLED=true
RACE_PREWARM_SECONDS=2.5
RACE_SCHEDULER_TICK=0.002
RACE_RETRY_SECONDS=0.025
RACE_LAUNCH_WINDOW_SECONDS=8
RACE_PUBLIC_GAS_LIMIT=300000
RACE_STREAM_WORKERS=24
RACE_SIGNAL_WORKERS=32
RACE_PREP_WORKERS=12
RACE_LAUNCH_WORKERS=16
RACE_LIVE_STATIC_GAS=true
RACE_GAS_PER_EXTRA_TOKEN=22000
RACE_MAX_STATIC_GAS=3000000
RACE_GAS_STRATEGY=fast
RACE_FEE_REFRESH_SECONDS=0.25
RACE_PRICE_REFRESH_SECONDS=30
SEADROP_WSS_DISCOVERY=true
ETHEREUM_PENDING_SEADROP=true
RPC_BURST_REBROADCAST=true
RPC_BURST_DELAYS_MS=35,120
```

You do not need to add these variables to Railway unless you want to override the built-in defaults.

## Gas policy remains unchanged
Telegram-managed gas caps and per-project exceptions remain authoritative. The speed upgrade does not silently remove the user's gas budget.

## Important limitation
No software can guarantee winning a sold-out mint. Network propagation, sequencer/miner ordering, RPC latency, contract rules and competing transactions are external factors. V4.10 focuses on removing avoidable latency inside the bot before the raw transaction reaches the network.

## Upgrade
Replace `main.py`, `buyer.py`, `storage.py`, `requirements.txt`, `.env.example`, `health.py`, and `railway.json`, then redeploy. Do not change `WALLET_ENCRYPTION_KEY` and do not delete the Railway volume.

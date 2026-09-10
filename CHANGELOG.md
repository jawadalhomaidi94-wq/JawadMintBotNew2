# Changelog

## V4.13.1 — Ultra Race Recovery

### Fixed
- Insufficient native-gas failures now enter a dedicated balance recheck path regardless of whether the failure happened during prewarm, scheduled Race, live Race, or fallback execution.
- Any detected wallet top-up wakes a fresh attempt while the mint stage remains active; insufficient balance is no longer a sticky state.
- Active low-balance Free Mint candidates are retained while their Public stage is open so the top-up watcher can recover even without a new Stream event.
- Stale pre-signed EIP-1559 transactions are detected when providers report `max fee per gas less than block base fee`; fee fields are refreshed, re-signed, and retried.
- Gas-budget clamping will not intentionally produce a replacement `maxFeePerGas` below a provider-reported current `baseFee`.
- Overlapping qualification/allowlist stages can no longer hide an active final Public stage.
- New stage transition clears stale low-balance retry state without deleting cumulative confirmed quantities.
- OpenSea social-trust 429s respect the server/client cooldown instead of producing repeated API attempts during the cooldown.
- Ethereum has a best-effort PublicNode fallback when configured/Alchemy RPCs are unavailable.

### Faster
- Live Stream/SeaDrop signals pass their already-read public config directly into Race, eliminating a duplicate SeaDrop RPC read from the live hot path.
- Fee warmer uses a persistent executor instead of allocating a ThreadPool every refresh interval.
- Prewarm default increased to 6 seconds and fee warming default reduced to 0.20 seconds while launch still performs no added healthy-path balance RPC.
- Positive X/website identity present in discovery metadata is reused by Safe Protection, avoiding an unnecessary Collection REST lookup.
- Native/USD oracle has Alchemy → Coinbase → Binance fallback and remains warmed outside launch.

### Added
- RAM-only low-balance fields and isolated `balance-recheck` executor/thread.
- `mint_totals` compaction table so visible history can be deleted after 24h without losing cumulative anti-remint quantities.
- Low-priority maintenance worker for 24h history compaction and stale in-memory/SQLite cache cleanup.
- Immediate final-Public archive after confirmed execution is resolved for all active wallets.
- Manual Telegram wallet balance view and refresh buttons.

### Preserved
- Free Mint Safe Protection rule: X OR website for ordinary auto Free Mints without qualification.
- Qualification/manual exemptions and stage quantity behavior.
- Paid Mint explicit confirmation flow.
- V4.12 Collection Offers and their executor/storage isolation.
- Existing multi-wallet parallel broadcast, nonce-safe retry, pause behavior, encrypted wallet persistence, and Railway startup model.

## V4.12.0 — Isolated Collection Offers
- Added real OpenSea Collection Offer flow using USDT-facing pricing and WETH/Seaport execution.
- Added Top Offer, wallet quantities, WETH Approval, account offers, cancellation/raise, separate offer storage, and isolated Offer executors.

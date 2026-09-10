# V4.13.0 — Ultra Race

- Preserves V4.12.2 behavior and safety decisions.
- Removes a duplicate SeaDrop public-config RPC read from fast-signal launches.
- Removes estimateGas from FREE live Race by using the existing conservative static Race gas limit.
- Moves default prewarm lead from 2.5s to 6.0s for known monitoring/qualification stages.
- Existing all-wallet parallel broadcast, nonce safety, Protected Free Mint Shield, qualification logic, 24h cleanup and Offers isolation remain unchanged.

# V4.12.2

- Added USDT equivalent display beside native-token prices/fees in qualification, paid mints, free mints, eligibility, mint transaction notifications, history, and wallet balance views.
- Offer review/approval/funding messages now show WETH plus the approximate USDT equivalent where applicable.
- Added `💰 عرض الرصيد` in Wallets plus per-wallet balance refresh.
- Balance reads are manual-only and run in a dedicated executor, outside Main Loop and Race Lane.
- Wallet balance view shows each enabled network's native balance (ETH/POL) and its approximate USDT value.
- Gas-limit UI now consistently uses USDT wording and accepts values typed with `USDT` or `$`.
- Mint/Race execution algorithms remain unchanged from V4.12.1.

# V4.12.1

- Fixed qualification first-check/recheck logic.
- Final free Public is armed immediately for every enabled wallet.
- Eligible free pre-Public/allowlist stages execute their cumulative available quantity automatically.
- FREE SeaDrop Race no longer aborts solely on a temporarily missing native/USD oracle; paid mint remains fail-closed.
- ETH/USD background cache now has Coinbase and Binance fallbacks.
- Free Public fallback never performs a synchronous USD-price HTTP lookup in front of mint.
- Final Public projects leave Monitoring/Qualification after all active wallets are resolved.
- Today's Mints and visible history retain only the last 24 hours.
- Confirmed rows older than 24h are compacted into mint_totals before deletion, preserving cumulative/anti-duplicate state.
- Low-priority cache/history cleanup is isolated from Race Lane.
- V4.12.0 Offers remain isolated from mint execution.

# Changelog

## V4.12.0 — Isolated Collection Offers

### Added
- Real OpenSea Collection/criteria Offer flow using OpenSea API V2 + Seaport.
- Dedicated `offers.py` module with separate `offer-api` and `offer-sign` executors plus per-session market snapshots.
- USDT-facing pricing with WETH order execution.
- Live Top 3 collection offers and fresh Top Offer + configurable increment.
- Custom offer price and current-market-price shortcuts.
- Multi-wallet selection with independent quantity per wallet.
- Fresh WETH balance and conduit allowance checks before signing.
- Explicit exact-amount WETH Approval transaction with existing gas guards.
- Final confirmation gate and a second Top Offer check immediately before signing.
- Active OpenSea account offers view, local reconciliation, cancellation and raise flow.
- Separate `offer_history` SQLite table.
- Offer settings for Top increment and default duration.
- `/offers` Telegram command.

### Preserved
- No Offer polling in the main loop.
- No Offer hooks in Race scheduler / launch / live signal / Stream mint-event path.
- Existing race signal/prep/launch executors and timings.
- Existing Free Mint, qualification, paid Mint, nonce, gas, storage and Railway behavior.

### Compatibility
- Built on the uploaded V4.11.3 baseline.
- New offers are created as Seaport 1.6 orders.
- Off-chain cancellation recognizes Seaport 1.6 and older 1.5 orders.

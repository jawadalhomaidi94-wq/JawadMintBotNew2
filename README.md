# OpenSea Mint Guardian V4.11.1 — Stable Safety Hotfix

V4.11.1 is a conservative hotfix built on V4.11. The protected Free Mint shield and the stable V4.10.1/V4.9 Race transaction path remain in place. The changes below target the exact issues observed in Railway logs without redesigning the successful speed architecture.

## Fixed: SeaDrop wallet-limit simulation failures

SeaDrop enforces the public wallet cap using the NFT contract's `getMintStats(minter)` values. A wallet may have minted outside this bot, so SQLite history alone is not authoritative.

V4.11.1 now:

- reads `getMintStats(minter)` from the NFT contract;
- subtracts the wallet's actual on-chain minted count from `maxTotalMintableByWallet`;
- lowers the requested quantity to the remaining allowance;
- detects a wallet that already reached its cap and does not send a guaranteed-revert transaction;
- uses the reported current/max supply to avoid knowingly preparing more wallet transactions than the remaining on-chain supply.

For a scheduled Public mint these calls happen during Race prewarm, before the opening timestamp. For a surprise live mint the wallet-stat reads run concurrently.

## Fixed: Pause now stops Race Lane

`⏸ إيقاف التنفيذ`, `/pause`, and `/panic` now stop both the normal mint path and Race Lane signing/broadcast. Prepared signed bundles are discarded when pausing so Resume does not send stale nonce/fee data. Discovery and monitoring continue while paused.

Transactions already broadcast before Pause cannot be cancelled by the bot.

## New: native-balance failure alert

An `insufficient_balance` failure is always treated as a transaction-safety notification, even when routine monitoring/stage notifications are disabled. Telegram shows:

- project;
- free/paid type when known;
- network;
- wallet name and short address;
- estimated mint value and gas when available;
- copyable mint link.

Live Race intentionally skips one balance read for speed. If the RPC itself replies with messages such as `insufficient funds for gas`, V4.11.1 classifies the response as `insufficient_balance` instead of the generic `rpc_or_tx_error`, so the same Telegram alert is sent. The wallet is retried after `LOW_BALANCE_RETRY_SECONDS` (default 2s) while the opportunity remains active, without Telegram spam for every Race tick.

## Fixed: sensitive data in logs

Raw Telegram input is no longer logged. Non-command input is represented only as `<private-input>` plus its length. RPC endpoints are printed with API credentials redacted, and configured Alchemy/OpenSea/Telegram/encryption secrets are filtered from emitted log messages.

If an older deployment already printed a wallet private key or API key into Railway logs, rotate that credential. This hotfix prevents future raw-input logging; it cannot make an already-exposed key secret again.

## Existing V4.11 behavior preserved

- Protected Free Mint Shield is ON by default.
- Automatic non-qualification Free Mints require X or an external website while the shield is ON.
- Manual watches and qualification/allowlist projects remain exempt from the social shield.
- Paid Public still requires explicit user approval, wallet selection, and quantity.
- OpenSea Stream, SeaDrop WSS, REST fallback, qualification tracking, cumulative quantities, SQLite persistence, Telegram UI, and project-specific gas settings remain available.
- The known Public Race scheduler defaults remain `prewarm=2.5s`, `tick=0.005s`, and `retry=0.04s`.

## New optional setting

```env
LOW_BALANCE_RETRY_SECONDS=2
```

No new Railway variable is required; the default is built into the code.

## Expected startup logs

```text
Mint Guardian V4.11.1 starting
RACE LANE V4.11.1 STABLE ready
Protected Free Mint Shield ready | enabled=True | rule=X-or-website
```

RPC startup logs should now show a redacted endpoint, for example `/v2/***`, not the API credential. Raw wallet keys must never appear in Telegram receive logs.

## Deployment

Replace the project files with this package and redeploy through GitHub/Railway. Keep the existing Railway Volume and keep the existing `WALLET_ENCRYPTION_KEY` value unchanged.

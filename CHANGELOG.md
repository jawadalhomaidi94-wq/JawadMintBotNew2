# V4.14.7 — Telegram Inbound Recovery

- Fixed a regression where Telegram could send the startup notification but never enter `getUpdates`, making `/start`, `/wallets`, and callbacks appear completely dead.
- Removed synchronous `setMyCommands` from the listener startup path.
- Added asynchronous polling-mode enforcement with `deleteWebhook(drop_pending_updates=false)`.
- Added visible WARN diagnostics for Telegram polling failures, especially HTTP 409 conflicts.
- Added poll-session reset/retry after 409 and repeated network errors.
- Added duplicate Bot Token protection between Admin and tenants and between tenant bots.
- Kept the V4.14.6 responsive outbound/ACK/UI-RPC isolation.
- Critical Race, Direct Fan-Out, Safe Protection, Qualification, and Paid Mint functions are unchanged from V4.14.6.

---

# Mint Guardian V4.14.6 — Telegram I/O Isolation & Responsive UI

## Fixed
- Fixed the V4.14.5 callback regression where `answerCallbackQuery` was executed synchronously inside the Telegram long-poll listener before the callback was queued. A slow Telegram API response could therefore make buttons appear dead even though `callback received` was logged.
- Callback actions are now queued first and callback ACKs run on a dedicated non-blocking Telegram ACK worker.
- `sendMessage`, `editMessageText`, and `deleteMessage` now run on a dedicated outbound Telegram worker, so Telegram network latency cannot stall the serialized command worker.
- Wallet balance and wallet detail RPC reads no longer run inside the Telegram command worker. UI opens immediately from local/cache state and live balances refresh on isolated UI network/RPC executors.
- Manual UI balance reads run in parallel across supported chains and reuse cached USD prices only; they never compete with Race, low-balance, discovery, or fee-warmer executors.
- Added slow-command diagnostics to expose any future callback/command that blocks the command worker for 250ms or more.

## Preserved
- V4.14.5 paid-mint immediate execution and paid failure notifications are unchanged.
- V4.14.4 Direct Fan-Out, Race, Safe Protection, Qualification, stage recovery, per-mint balance checks, low-balance wake, tenant isolation, and Offers logic are unchanged.

---

# Mint Guardian V4.14.4 — Reliability Gate & Per-Mint Balance

## Fixed
- A protected project that reaches `Social PASS` is no longer dropped when the first SeaDrop read lands during a short configuration/provider transition. A dedicated fast stage-recovery lane retries on-chain resolution from 50ms onward, then backs off gradually for a bounded 30s window.
- Stage recovery is cancelled immediately once a configured Public is resolved and then uses the existing Admin-first Direct Tenant Fan-Out; it never waits for the 15s catalog scan.
- On unresolved social/SeaDrop recovery signals, one already-verified secondary RPC is probed when available. The healthy first-read path is still one primary RPC call.
- Every newly detected mint now checks the native gas balance of every participating wallet before signing/broadcast, including surprise live Public mints. Qualification/normal paths already had their balance guard and remain protected.
- Live Race no longer uses `skip_balance_check=True`. Pending nonce and native balance are fetched concurrently per wallet, preserving latency while adding the required balance gate.
- Prepared bundles retain their checked balance snapshot. If the warmed EIP-1559 fee rises before broadcast and the new maximum requirement exceeds that checked balance, the wallet is converted to `insufficient_balance` instead of knowingly broadcasting an underfunded transaction.
- Existing V4.14.3 low-balance latch/watcher remains authoritative: insufficient wallets are removed from repeated Stream/Race churn and a real top-up wakes a fresh nonce/fee/balance attempt while the stage remains open.

## Preserved
- V4.14.1 zero-poll Direct Tenant Fan-Out and Admin-first ordering.
- Independent per-user Safe Protection, gas settings, pause/resume, notifications, permissions, wallets, history and Offers.
- Safe Protection rule remains X OR Website for ordinary automatic Free Mints when enabled by that user.
- Qualification/allowlist behavior and Final Public takeover remain unchanged.
- V4.14.2 StoredWallet compatibility, terminal same-stage cache and candidate exception isolation.
- V4.14.3 friendly Telegram alerts and low-balance latch.
- Offers remain isolated from the Race hot path.

## New runtime markers
- `Fast stage recovery ready | first=50ms | max=30.0s | fallback-RPC=True`
- `V4.14.4 guards ready | ... | per-mint-balance=True | stage-recovery=True`

---

# Mint Guardian V4.14.3 — Friendly Alerts & Low-Balance Latch

## Fixed
- Telegram insufficient-balance alerts no longer expose raw RPC/Python payloads such as `{'code': -32000, 'message': ...}`. Raw provider details remain in Railway logs only.
- Insufficient-balance alerts now show a localized reason plus current balance, approximate required balance, and approximate shortfall when the provider supplies `have/want` or preflight balance data.
- Same-stage Stream/SeaDrop signals no longer reopen a wallet already waiting for a gas top-up.
- Low-balance wallets are latched out of Race until the isolated balance watcher detects funding; a genuinely new stage still resets them normally.
- The 5ms Race scheduler now uses a RAM-only ready-wallet gate before entering SeaDrop/RPC work, preventing repeated RPC/prewarm/broadcast churn while all wallets are low-balance/terminal/backed off.

## Preserved
- V4.14.2 StoredWallet compatibility, same-stage terminal cache and candidate exception isolation.
- V4.14.1 Direct Tenant Fan-Out, Admin-first ordering and shared resolved Public snapshot.
- Independent per-user Safe Protection, gas caps, pause/resume, notifications, permissions, wallets, history and Offers.
- V4.13.1 low-balance watcher behavior: a real top-up re-arms the wallet immediately while the stage remains open.

---

# Mint Guardian V4.14.2 — Stability Fix

## Fixed
- Production tenant-thread crash after receipt/final-Public processing: `StoredWallet` now supports chain compatibility and all main wallet filtering uses a defensive `wallet_supports_chain(...)` helper.
- Same-stage duplicate terminal retries: Race work skips final wallets, and duplicate live Public signals no longer reopen a wallet already final for that stage.
- On-chain terminal results (`wallet limit reached`, `sold out`, `no on-chain supply remains`) are tagged with the current stage and remain suppressed until a genuinely new stage opens.
- Reverted transactions remain final for the same stage instead of being reactivated by a later Stream burst.
- Candidate processing is exception-isolated so one bad/stale project cannot terminate a tenant's entire main runtime thread.

## Preserved
- V4.14.1 Direct Tenant Fan-Out and Admin-first ordering.
- Shared Admin-resolved SeaDrop/Public snapshot with no tenant re-read in the direct hot path.
- Independent per-user Safe Protection, gas limits, pause/resume, notifications, permissions, wallets, history and Offers.
- V4.13.1 stale-fee recovery, low-balance auto-resume, quantity logic, RPC fallbacks, 24-hour visible history and cumulative anti-remint accounting.

---

# Mint Guardian V4.14.1 — Direct Tenant Fan-Out

## Ultra Race multi-user latency upgrade
- Removed the tenant live-mint dependency on the former ~20ms shared-candidate mirror loop.
- Admin resolves a Stream/SeaDrop contract stage once, queues the Admin Race launch first, then pushes the exact in-memory `public`/stage snapshot directly to every active tenant Race lane.
- Tenant direct Race does **not** repeat the SeaDrop RPC read and does **not** run SQLite wallet synchronization before the live launch.
- Each tenant still applies its own permissions, active wallets, gas limits, pause state and Safe Protection setting before signing/broadcast.
- Admin pause or Admin Safe-Protection outcome cannot block another tenant whose own settings allow execution.
- Direct event de-duplication prevents the same stage burst from spawning duplicate tenant jobs.
- Future Public stage snapshots are also pushed immediately so tenant schedulers can prewarm before opening.
- OpenSea qualification/drop metadata is pushed through an event-driven tenant wake-up instead of waiting for the next 20ms mirror poll. The tenant main planner remains the single writer for qualification-stage mutation.
- The old shared-candidate sync is retained only as a recovery/sanity fallback (`TENANT_SHARED_SYNC_FALLBACK_SECONDS=1.0` by default).
- Final Public has a RAM-only wallet activation step that clears stale allowlist backoff before Race without touching SQLite.
- Added a stale-qualification-result guard: an old allowlist RPC result cannot overwrite wallet state after a newer/final Public stage has become active.
- The last Admin-resolved Public snapshot is held briefly in tenant RAM so a Safe-Protection social PASS can launch without re-reading SeaDrop.
- Safe Protection remains independently selectable per user, but X/Website identity resolution is now centralized: if any active tenant has protection ON, Admin performs one project-level OpenSea social lookup and pushes PASS/REJECT/ERROR to tenants.
- Protected tenants never duplicate the OpenSea social REST lookup; protection-OFF tenants do not wait for the social result, and a social PASS wake is delivered only to protection-ON tenants.
- Enabling Safe Protection on a tenant immediately asks the Admin discovery engine to verify already-known auto-free projects, without restarting any bot.

## Preserved
- V4.14.0 multi-user isolation, independent per-user Bot Token/Telegram ID, permissions, wallet limits, settings, pause, notifications, history, offers and databases.
- V4.13.1 Race transaction builder/broadcaster, prewarm, fee warmer, stale EIP-1559 retry, low-balance auto-resume, quantity logic, 24-hour visible history and RPC fallbacks.
- Safe Protection remains per tenant: ordinary auto Free Mint requires X OR website only for tenants that enabled the shield; qualification/manual exemptions remain unchanged.
- Collection Offers remain isolated from the Race hot path.

---

# Mint Guardian V4.14.0 Multi-User Race

## Added
- Admin/user multi-tenant architecture while preserving the V4.13.1 Mint/Race engine.
- Railway Telegram bot remains the Admin bot. Admin can create users with username, Telegram ID, encrypted Bot Token, wallet limit and granular permissions.
- One private Telegram bot per user; exact Telegram ID allowlist is enforced for that bot.
- User enable/suspend/resume without stopping Admin or other users.
- Unlimited Admin wallets; enforced per-user wallet limits.
- Separate encrypted SQLite tenant database per user for wallets, settings, watches, qualification state, offers and mint history.
- Central encrypted users registry and audit log.
- Global wallet ownership claims prevent one wallet being attached to two tenants.
- Granular server-side permission checks; hiding buttons is not treated as authorization.
- Per-user settings: global/per-chain gas cap, Free Mint protection, stage notifications, all notifications, and independent pause/resume.
- Admin user management UI in Telegram with live permission toggles and wallet-limit editing.
- Shared auto-discovery fan-out: Admin performs global OpenSea/Stream/SeaDrop discovery once; tenant engines receive RAM-only project metadata and keep independent wallet state.
- Shared verified RPC pools, price oracle and warmed fee cache across tenants to avoid multiplying RPC/Alchemy load.
- Admin-first execution: Admin sees/processes the source Race event before it is mirrored to tenant lanes; tenant wallets then execute concurrently in isolated Race executors.

## Preserved from V4.13.1
- Ultra Race prewarm, fee warmer, stale EIP-1559 refresh/re-sign, low-balance auto-resume, 24h visible history compaction, cumulative anti-remint totals, Safe Protection X-or-website rule, final Public behavior, quantity rules, paid-mint confirmation, Offers isolation and RPC fallbacks.

## Isolation guarantees
- No user's Telegram messages are routed to another user's bot.
- Wallets/settings/history/offers/watches are physically separated by tenant DB.
- A user's pause/settings do not change Admin or another user.
- User bots cannot manage users.
- User Bot Tokens and wallet private keys are encrypted at rest.

## V4.14.5 — Paid Mint & Telegram UI Hotfix

### Fixed
- Confirming an already-open paid Public mint now triggers an immediate fresh on-chain SeaDrop read and Race launch; it no longer waits for another Stream event or scheduler transition.
- Paid mint failures after explicit confirmation are persisted to mint history and always notify the owning tenant, including balance, gas-cap, price-cap, precondition, and RPC/transaction failures.
- Paid confirmation refreshes the active on-chain stage so stale OpenSea metadata cannot leave a confirmed purchase idle.
- Telegram inline callback taps are acknowledged before command processing, removing the long Telegram spinner while keeping command work isolated from Race.
- Paid selector redraw no longer performs a blocking SeaDrop RPC price lookup on every button press; it reuses already-resolved stage data.

### Preserved
- V4.14.4 Reliability Gate, tenant isolation, Admin priority, Safe Protection, qualification/Public behavior, low-balance auto-resume, gas caps, Offers isolation, and shared discovery/Race architecture.

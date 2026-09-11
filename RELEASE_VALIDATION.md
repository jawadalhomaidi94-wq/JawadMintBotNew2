# V4.14.4 Release Validation

## Production issues addressed
1. `Social PASS` could be logged without a later `RACE submitted/no-submit` when the first SeaDrop read did not yet expose a configured Public. V4.14.4 arms a dedicated short-interval stage recovery instead of dropping that transition.
2. Live Race previously passed `skip_balance_check=True`, so an underfunded wallet could reach RPC broadcast before the provider classified it. V4.14.4 checks balance for every live mint before signing/broadcast.

## Validation completed
- `python -m py_compile` across all Python files — PASS.
- Offline regression: `Social PASS` + unresolved first SeaDrop read creates a recovery record instead of dropping the project — PASS.
- Offline regression: resolving the stage cancels recovery immediately — PASS.
- Offline scheduler regression: the dedicated recovery lane emits forced resolution retries independently of catalog polling — PASS.
- Offline per-mint balance regression: pending nonce and balance calls execute concurrently; two simulated 60ms RPC calls completed the runtime gate in ~61ms rather than serial ~120ms — PASS.
- Underfunded live wallet regression: no signed Race entry is produced and result is `insufficient_balance` — PASS.
- Funded live wallet regression: signed entry is produced and its checked `balance_wei` snapshot is retained — PASS.
- Warmed-fee increase regression: if the new maximum gas requirement exceeds the checked balance snapshot, the entry is removed and converted to `insufficient_balance` before broadcast — PASS.
- Direct Tenant Fan-Out methods and multi-user isolation module are regression-compared against V4.14.3; no tenant routing/settings/permission redesign is included in this release.
- Friendly insufficient-balance Telegram formatting from V4.14.3 remains present.
- Final ZIP integrity is validated after packaging.

## Runtime behavior expected
- Healthy first signal: one primary SeaDrop read, then the existing Admin-first direct fan-out.
- Unresolved first signal: recovery starts at 50ms and backs off gradually for up to 30s; no 15s scan dependency.
- Every participating wallet: native balance check on the mint's actual chain before signing/broadcast.
- Insufficient wallet: stage latch + 0.35s isolated balance watcher; a true top-up re-arms a fresh transaction while the stage is open.

## Live-chain note
No funded blockchain transaction is sent during build validation. Network-dependent behavior is validated from the supplied production logs plus deterministic offline regression tests.

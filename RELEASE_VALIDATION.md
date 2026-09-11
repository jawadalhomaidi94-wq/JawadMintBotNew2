# V4.14.3 Release Validation

## Scope
V4.14.3 is a focused notification/low-balance retry patch over V4.14.2. The Direct Fan-Out architecture and transaction builder/broadcaster are not redesigned.

## Validation completed
- `python -m py_compile` across all Python files — PASS.
- Raw RPC insufficient-funds payload regression using the exact production-style `{'code': -32000, 'message': ... have ... want ...}` text — PASS; no raw dict/prefix appears in the Telegram-friendly lines.
- Exact `have/want` conversion to native ETH plus shortfall calculation — PASS.
- Preflight form `Wallet balance ... wei < estimated requirement ... wei` parsing — PASS.
- Low-balance Race gate: `next_attempt=inf` is not considered ready — PASS.
- Same-stage live Public signal preserves `insufficient_balance` and the latch — PASS.
- New-stage live Public signal resets the latch and makes the wallet ready again — PASS.
- Scheduler/launch source inspection confirms the RAM-only ready-state gate runs before SeaDrop/RPC work.
- Final ZIP integrity — validated after packaging.

## Intentionally unchanged from V4.14.2
- `buyer.py`
- `storage.py`
- `multi_user.py`
- `offers.py`
- `health.py`
- `requirements.txt`
- `railway.json`

Only `main.py`, `VERSION`, and release documentation are changed in V4.14.3.

## Live-chain note
No funded blockchain transaction is sent during build validation. Runtime behavior is validated with deterministic regression tests and the production log patterns supplied by the user.

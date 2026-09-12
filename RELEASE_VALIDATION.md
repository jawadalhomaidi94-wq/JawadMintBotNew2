# V4.14.8 Release Validation

- `python -m py_compile *.py`: PASS.
- Exact regression reproduction/fix test: `api("getUpdates", timeout=10, ...)` reaches a fake HTTP session without `TypeError`: PASS.
- Telegram form timeout is preserved as `data["timeout"] == 10`: PASS.
- HTTP/socket timeout is independently applied as `(4.0, 15.0)`: PASS.
- Listener simulation accepted `/start`, advanced update offset, and queued the command: PASS.
- AST audit: every `_post_api` call uses `request_timeout`; no `_post_api` call uses the reserved Telegram form keyword `timeout`: PASS.
- Source audit found no other function accepting both a named `timeout` argument and arbitrary `**data`: PASS.
- Critical mint function AST comparison against V4.14.7: Direct Fan-Out, fast signal/stage, Safe Protection, Qualification, paid immediate execution, and insufficient-balance handling are unchanged: PASS.
- `buyer.py`, `storage.py`, `offers.py`, `multi_user.py`, `health.py`, `requirements.txt`, and `railway.json` hashes unchanged from V4.14.7: PASS.
- No funded blockchain transaction was performed during build validation.

---

# V4.14.7 Release Validation

- `python -m py_compile *.py`: PASS.
- Telegram inbound queue test: `/start` was accepted into the command queue without depending on setup/outbound network I/O: PASS.
- Non-blocking startup test: `setMyCommands` is queued to the outbound worker and is no longer called synchronously before the first `getUpdates`: PASS.
- HTTP 409 recovery test: polling session reset/retry path executed without terminating the listener: PASS.
- Duplicate Admin Bot Token rejection on new tenant creation: PASS.
- Legacy duplicate Admin Bot Token supervisor guard: PASS; duplicate listener is skipped.
- AST comparison against V4.14.6: `submit_fast_contract_signal`, `_fast_live_contract_signal`, `_consume_shared_fast_stage`, `receive_shared_fast_stage`, `fanout_shared_fast_stage`, `social_protection_allows`, `stage_qualification_check`, `_execute_confirmed_paid_now`, and `_paid_failure_notice` are unchanged: PASS.
- `buyer.py`, `storage.py`, `offers.py`, `health.py`, `railway.json`, and `requirements.txt` hashes unchanged from V4.14.6: PASS.
- No funded blockchain transaction was performed during build validation.

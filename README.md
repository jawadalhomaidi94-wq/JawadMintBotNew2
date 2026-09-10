# OpenSea Mint Guardian V4.11 — Protected Free Mint Shield

V4.11 is built directly on the known-working V4.10.1/V4.9 transaction path. The Race transaction construction/signing/broadcast logic is intentionally unchanged.

## New: Protected Free Mint Shield

The shield is **enabled by default** and can be toggled from Telegram > ⚙️ الإعدادات.

When enabled, it applies only to:

- automatically discovered Free Public mints;
- projects that are not qualification/allowlist projects;
- projects the user did not add manually.

A project passes if OpenSea collection metadata contains **at least one** of:

- an X/Twitter identity (`twitter_username` / X/Twitter link), or
- an external project website (`external_url` / website link).

No X/Twitter API is required in V4.11. Follower count, account age, recent posts, suspended status, and verification are intentionally reserved for a later version.

## Safety behavior

Protected mode is fail-closed. If the project has neither X nor a website, or OpenSea metadata cannot be verified yet, the automatic non-qualification free mint is not sent. This prevents gas spend on anonymous/spam drops.

Manual watches, qualification/allowlist projects, and paid projects are exempt from this shield. Paid Public still requires the existing explicit wallet/quantity confirmation.

If the shield is disabled from Telegram, behavior immediately returns to V4.10.1: all otherwise-valid automatic Free Mints may proceed.

## Speed design

The social check is project-level, never wallet-level:

```text
Project discovered
   ├─ Race/prewarm preparation
   └─ OpenSea social identity lookup (parallel, once per project)
            ↓
       RAM + SQLite cache
            ↓
PASS → Race lane can broadcast to all active wallets
```

The social metadata worker has its own executor and never occupies Race signal/prewarm/launch workers. For projects known before Public, verification is normally cached before opening. For a completely new project first seen only after Public is already live, protected mode necessarily waits for the one identity lookup before spending gas.

Cache defaults:

```env
SOCIAL_TRUST_PASS_TTL_SECONDS=86400
SOCIAL_TRUST_REJECT_TTL_SECONDS=120
SOCIAL_TRUST_ERROR_RETRY_SECONDS=5
SOCIAL_TRUST_WORKERS=2
```

## Telegram setting

Inside ⚙️ الإعدادات:

- `🛡 حماية Free Mint: مفعلة` — only X/Website-backed automatic non-qualification free mints are allowed.
- `⚠️ حماية Free Mint: متوقفة` — use the previous V4.10.1 behavior and allow all automatic free mints.

The setting is persisted in SQLite on the Railway Volume.

## Persistence

V4.11 adds an additive `social_trust_cache` SQLite table. Existing wallets, watches, paid plans, gas settings, qualification data, and mint history remain intact.

Do **not** change `WALLET_ENCRYPTION_KEY` and do **not** delete the Railway Volume.

## Expected startup logs

```text
Mint Guardian V4.11 starting
RACE LANE V4.11 STABLE ready
Protected Free Mint Shield ready | enabled=True | rule=X-or-website
```

## Deployment

Replace `main.py` and `storage.py`. `buyer.py` remains the stable transaction path used by V4.10.1; replacing it with the V4.11 copy is safe because it is unchanged. Commit/push and let Railway redeploy.

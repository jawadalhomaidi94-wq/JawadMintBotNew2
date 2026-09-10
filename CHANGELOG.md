# Mint Guardian V4.14.1 — Direct Tenant Race Fan-Out

## Speed improvement
- Removed the 20ms shared-candidate polling loop as a dependency of live tenant mint launch.
- Admin resolves each Stream/SeaDrop contract signal once, then immediately enqueues the already-resolved event into every eligible active tenant Race executor.
- Tenant live launch reuses Admin's resolved SeaDrop public configuration (`public_hint`), so tenants do not repeat the discovery SeaDrop RPC before Race.
- The existing 20ms shared-candidate synchronization remains only as recovery/backfill for metadata and missed/non-live state; it is no longer the live signal transport.
- Configurable `TENANT_ADMIN_HEADSTART_SECONDS` defaults to 0.002 seconds. Event enqueue is immediate; the tiny head-start is applied inside tenant workers so Admin keeps launch priority without a polling delay.
- `DIRECT_TENANT_FANOUT=true` enables the new path by default.

## Isolation preserved
- Each tenant still applies its own `free_social_protection_enabled` setting before launch.
- Protection ON: ordinary auto Free Mint requires X OR Website.
- Protection OFF: that tenant may proceed without the social gate.
- Qualification-tracked projects preserve the existing protection exemption and per-wallet qualification behavior.
- Each tenant still applies its own pause state, permissions, wallets, gas limits, history, notifications and database.
- Suspended tenants and tenants without the required permission are excluded before executor submission and checked again inside the tenant Bot.
- Paid mint behavior remains explicit and tenant-local.

## Race behavior preserved
- V4.13.1/V4.14.0 Race preparation, broadcast, stale-fee recovery, low-balance auto-resume, nonce safety, RPC pools, fee cache and price oracle remain intact.
- Admin remains the global discovery owner and has priority.
- Tenants share the verified RPC pools and hot market caches; no per-user OpenSea scanner or fee warmer was introduced.
- Offers remain isolated from Race.

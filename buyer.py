from __future__ import annotations

import logging
import math
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

import requests
from eth_account import Account
from web3 import Web3
from web3.exceptions import TransactionNotFound

log = logging.getLogger("mint-buyer")


CHAIN_CONFIGS: dict[str, dict[str, Any]] = {
    "ethereum": {
        "chain_id": 1,
        "native_symbol": "ETH",
        "opensea_chain": "ethereum",
        "alchemy_slug": "eth-mainnet",
        "explorer": "https://etherscan.io/tx/",
        "default_rpcs": ["https://ethereum-rpc.publicnode.com"],
    },
    "ink": {
        "chain_id": 57073,
        "native_symbol": "ETH",
        "opensea_chain": "ink",
        "alchemy_slug": "ink-mainnet",
        "explorer": "https://explorer.inkonchain.com/tx/",
        "default_rpcs": ["https://rpc-gel.inkonchain.com", "https://rpc-qnd.inkonchain.com"],
    },
    "robinhood": {
        "chain_id": 4663,
        "native_symbol": "ETH",
        "opensea_chain": "robinhood",
        "alchemy_slug": "robinhood-mainnet",
        "explorer": "https://robinhoodchain.blockscout.com/tx/",
        "default_rpcs": ["https://rpc.mainnet.chain.robinhood.com"],
    },
    "base": {
        "chain_id": 8453,
        "native_symbol": "ETH",
        "opensea_chain": "base",
        "alchemy_slug": "base-mainnet",
        "explorer": "https://basescan.org/tx/",
        "default_rpcs": [],
    },
    "arbitrum": {
        "chain_id": 42161,
        "native_symbol": "ETH",
        "opensea_chain": "arbitrum",
        "alchemy_slug": "arb-mainnet",
        "explorer": "https://arbiscan.io/tx/",
        "default_rpcs": [],
    },
    "optimism": {
        "chain_id": 10,
        "native_symbol": "ETH",
        "opensea_chain": "optimism",
        "alchemy_slug": "opt-mainnet",
        "explorer": "https://optimistic.etherscan.io/tx/",
        "default_rpcs": [],
    },
    "polygon": {
        "chain_id": 137,
        "native_symbol": "POL",
        "opensea_chain": "polygon",
        "alchemy_slug": "polygon-mainnet",
        "explorer": "https://polygonscan.com/tx/",
        "default_rpcs": [],
    },
}

CHAIN_ALIASES = {
    "eth": "ethereum",
    "mainnet": "ethereum",
    "ethereum-mainnet": "ethereum",
    "rh": "robinhood",
    "hood": "robinhood",
    "robinhood-chain": "robinhood",
    "robinhood_chain": "robinhood",
}


@dataclass(frozen=True)
class WalletConfig:
    name: str
    private_key: str
    address: str
    quantity: int = 1
    chains: tuple[str, ...] = ()

    def supports_chain(self, chain: str) -> bool:
        return not self.chains or normalize_chain(chain) in self.chains


@dataclass
class EligibilityResult:
    eligible: bool | None
    status: str
    mint_value_native: Decimal | None = None
    target: str | None = None
    detail: str | None = None
    quantity_used: int | None = None


@dataclass
class MintResult:
    ok: bool
    status: str
    tx_hash: str | None = None
    mint_value_native: Decimal | None = None
    gas_cost_native: Decimal | None = None
    gas_cost_usd: Decimal | None = None
    total_max_native: Decimal | None = None
    detail: str | None = None
    rpc: str | None = None
    target: str | None = None
    quantity_used: int | None = None


class OpenSeaClient:
    BASE_URL = "https://api.opensea.io/api/v2"

    def __init__(self, api_key: str, timeout: float = 8.0):
        if not api_key:
            raise ValueError("OPENSEA_API_KEY is required")
        self.timeout = timeout
        self.api_key = api_key
        self._local = threading.local()

        # V4.9: all REST calls share one rate-limit state even though each
        # worker thread has its own requests.Session. OpenSea uses one bucket
        # per account/key, so a per-thread limiter would still create bursts.
        self._rate_lock = threading.Lock()
        self._blocked_until = 0.0
        self._rate_limit: int | None = None
        self._rate_remaining: int | None = None
        self._rate_reset_at = 0.0
        self._last_429_log = 0.0
        # Keep REST traffic fast but non-bursty. Stream/on-chain discovery is
        # intentionally outside this limiter, so real-time free-mint detection
        # is not slowed by OpenSea REST quotas.
        self._next_request_at = 0.0
        self.rest_concurrency = max(1, min(int(os.getenv("OPENSEA_REST_CONCURRENCY", "2")), 8))
        self.min_request_interval = max(0.0, float(os.getenv("OPENSEA_MIN_REQUEST_INTERVAL", "0.08")))
        self.rate_reserve = max(0, int(os.getenv("OPENSEA_RATE_RESERVE", "6")))
        self._rest_slots = threading.BoundedSemaphore(self.rest_concurrency)
        self._cache_lock = threading.Lock()
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.drop_cache_seconds = max(0.0, float(os.getenv("OPENSEA_DROP_CACHE_SECONDS", "3")))
        self.collection_cache_seconds = max(0.0, float(os.getenv("OPENSEA_COLLECTION_CACHE_SECONDS", "60")))
        self.contract_cache_seconds = max(0.0, float(os.getenv("OPENSEA_CONTRACT_CACHE_SECONDS", "120")))
        self.list_cache_seconds = max(0.0, float(os.getenv("OPENSEA_LIST_CACHE_SECONDS", "4")))

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "X-API-KEY": self.api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "OpenSea-Mint-Guardian/4.13.1",
            })
            self._local.session = session
        return session

    @staticmethod
    def _header_int(headers: Any, name: str) -> int | None:
        try:
            value = headers.get(name)
            return int(float(value)) if value not in (None, "") else None
        except Exception:
            return None

    def prune_cache(self, *, max_age_seconds: float = 3600.0) -> int:
        """Drop stale in-memory REST cache entries without touching rate state."""
        cutoff = time.time() - max(60.0, float(max_age_seconds))
        removed = 0
        with self._cache_lock:
            for key, (created, _payload) in list(self._cache.items()):
                if created < cutoff:
                    self._cache.pop(key, None)
                    removed += 1
        return removed

    def cooldown_remaining(self) -> float:
        with self._rate_lock:
            return max(0.0, self._blocked_until - time.time())

    def rate_status(self) -> dict[str, Any]:
        with self._rate_lock:
            return {
                "limit": self._rate_limit,
                "remaining": self._rate_remaining,
                "reset_at": self._rate_reset_at or None,
                "cooldown": max(0.0, self._blocked_until - time.time()),
            }

    def _rate_limited_error(self, wait_seconds: float, detail: str = "local cooldown") -> requests.HTTPError:
        wait_seconds = max(0.1, float(wait_seconds))
        response = requests.Response()
        response.status_code = 429
        response.headers["Retry-After"] = str(max(1, int(math.ceil(wait_seconds))))
        response._content = (
            '{"errors":["Rate limit cooldown active"],"detail":"' + detail.replace('"', "'") + '"}'
        ).encode("utf-8")
        return requests.HTTPError(
            f"OpenSea 429: rate-limit cooldown active; retry after {wait_seconds:.2f}s",
            response=response,
        )

    def _update_rate_state(self, response: requests.Response) -> None:
        now = time.time()
        limit = self._header_int(response.headers, "X-RateLimit-Limit")
        remaining = self._header_int(response.headers, "X-RateLimit-Remaining")
        reset = self._header_int(response.headers, "X-RateLimit-Reset")
        retry_after = self._header_int(response.headers, "Retry-After")
        with self._rate_lock:
            if limit is not None:
                self._rate_limit = limit
            if remaining is not None:
                self._rate_remaining = remaining
            if reset is not None:
                self._rate_reset_at = float(reset)

            if response.status_code == 429:
                if retry_after is not None:
                    wait = max(1.0, float(retry_after))
                elif reset is not None and reset > now:
                    wait = max(1.0, float(reset) - now)
                else:
                    wait = 5.0
                # Small jitter prevents all wallet workers from waking at once.
                self._blocked_until = max(self._blocked_until, now + wait + random.uniform(0.05, 0.30))
                if now - self._last_429_log >= 20.0:
                    log.warning(
                        "OpenSea REST rate limit reached | retry_after=%.1fs | remaining=%s | limit=%s",
                        wait, self._rate_remaining, self._rate_limit,
                    )
                    self._last_429_log = now
            elif remaining is not None and remaining <= 0 and reset is not None and reset > now:
                self._blocked_until = max(self._blocked_until, float(reset) + 0.05)

    def _reserve_rest_slot(self, request_class: str) -> None:
        """Reserve a REST start slot without sleeping through long cooldowns.

        ``critical`` is used for ready-to-sign mint builders, ``normal`` for
        user/stage metadata, and ``background`` for catalog/event backfill.
        Background work preserves a token reserve for mint-time calls.
        """
        now = time.time()
        with self._rate_lock:
            cooldown = max(0.0, self._blocked_until - now)
            if cooldown > 0:
                raise self._rate_limited_error(cooldown, "server Retry-After cooldown")

            remaining = self._rate_remaining
            reset_at = float(self._rate_reset_at or 0.0)
            if remaining is not None and reset_at > now:
                reserve = 0 if request_class == "critical" else (2 if request_class == "normal" else self.rate_reserve)
                if remaining <= reserve:
                    wait = max(0.2, reset_at - now + 0.05)
                    raise self._rate_limited_error(wait, f"REST quota reserved for {request_class} calls")

            # Serialize request *starts* just enough to avoid a many-wallet
            # thundering herd. Actual HTTP work may still overlap up to
            # OPENSEA_REST_CONCURRENCY.
            scheduled = max(now, self._next_request_at)
            wait = max(0.0, scheduled - now)
            self._next_request_at = scheduled + self.min_request_interval
            # Pessimistically reserve one known token so another worker cannot
            # see the same last token before this response updates the headers.
            if remaining is not None:
                self._rate_remaining = max(0, remaining - 1)

        if wait > 0:
            time.sleep(wait)

    @staticmethod
    def _cache_key(method: str, path: str, kwargs: dict[str, Any]) -> str:
        params = kwargs.get("params") or {}
        if isinstance(params, dict):
            param_bits = "&".join(f"{k}={params[k]}" for k in sorted(params))
        else:
            param_bits = str(params)
        return f"{method.upper()}:{path}?{param_bits}"

    def _request(
        self, method: str, path: str, *, cache_ttl: float = 0.0,
        request_class: str = "normal", **kwargs
    ) -> dict[str, Any]:
        method = method.upper()
        cache_key = self._cache_key(method, path, kwargs) if method == "GET" and cache_ttl > 0 else ""
        now = time.time()
        if cache_key:
            with self._cache_lock:
                cached = self._cache.get(cache_key)
                if cached and cached[0] > now:
                    return cached[1]

        # Do not queue dozens of wallet calls behind a sleeping lock. A caller
        # that encounters a real/synthetic 429 gets a retry time and returns to
        # the bot loop, while Stream/on-chain detection keeps running.
        with self._rest_slots:
            self._reserve_rest_slot(request_class)
            r = self._session().request(method, f"{self.BASE_URL}{path}", timeout=self.timeout, **kwargs)
            self._update_rate_state(r)

        if r.status_code >= 400:
            text = r.text[:1500]
            raise requests.HTTPError(f"OpenSea {r.status_code}: {text}", response=r)
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected OpenSea API response")
        if cache_key:
            with self._cache_lock:
                self._cache[cache_key] = (time.time() + cache_ttl, data)
                if len(self._cache) > 2000:
                    cutoff = time.time()
                    self._cache = {k: v for k, v in self._cache.items() if v[0] > cutoff}
        return data

    def get_chains(self) -> dict[str, Any]:
        return self._request("GET", "/chains")

    def get_drops(
        self,
        drop_type: str,
        chain: str | None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"type": drop_type, "limit": max(1, min(limit, 100))}
        if chain:
            params["chains"] = chain
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/drops", params=params, cache_ttl=self.list_cache_seconds, request_class="background")

    def get_drop(self, slug: str) -> dict[str, Any]:
        return self._request("GET", f"/drops/{slug}", cache_ttl=self.drop_cache_seconds)

    def get_collection(self, slug: str) -> dict[str, Any]:
        return self._request("GET", f"/collections/{slug}", cache_ttl=self.collection_cache_seconds)

    # ---------- V4.12.0 Collection Offers (user-triggered only) ----------
    # These calls intentionally use the background REST class so they preserve
    # the existing API-token reserve for mint-time builders and stage refreshes.
    # The final POST is normal priority, but never critical/Race priority.
    def get_collection_for_offer(self, slug: str) -> dict[str, Any]:
        # Same Collection API/cache as get_collection(), but Offer-only reads
        # preserve the REST reserve for mint/stage work when a network request is needed.
        return self._request(
            "GET", f"/collections/{slug}",
            cache_ttl=self.collection_cache_seconds, request_class="background",
        )

    def get_collection_offers(self, slug: str, limit: int = 20, cursor: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": max(1, min(int(limit), 200))}
        if cursor:
            params["next"] = cursor
        return self._request("GET", f"/offers/collection/{slug}", params=params, request_class="background")

    def get_all_collection_offers(
        self, slug: str, *, maker: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": max(1, min(int(limit), 200))}
        if maker:
            params["maker"] = maker
        if cursor:
            params["next"] = cursor
        return self._request("GET", f"/offers/collection/{slug}/all", params=params, request_class="background")

    def get_account_offers(
        self,
        address: str,
        *,
        limit: int = 50,
        after: str | None = None,
        collection_slugs: list[str] | None = None,
        chains: list[str] | None = None,
    ) -> dict[str, Any]:
        params: list[tuple[str, Any]] = [("limit", max(1, min(int(limit), 50)))]
        if after:
            params.append(("after", after))
        for slug in collection_slugs or []:
            params.append(("collection_slugs", slug))
        for chain in chains or []:
            params.append(("chains", chain))
        return self._request("GET", f"/account/{address}/offers", params=params, request_class="background")

    def build_collection_offer(
        self,
        *,
        offerer: str,
        quantity: int,
        slug: str,
        protocol_address: str,
        offer_protection_enabled: bool = True,
    ) -> dict[str, Any]:
        body = {
            "criteria": {"collection": {"slug": slug}},
            "offer_protection_enabled": bool(offer_protection_enabled),
            "offerer": offerer,
            "protocol_address": protocol_address,
            "quantity": max(1, min(int(quantity), 100)),
        }
        return self._request("POST", "/offers/build", json=body, request_class="background")

    def post_collection_offer(
        self, *, slug: str, protocol_address: str, protocol_data: dict[str, Any]
    ) -> dict[str, Any]:
        # IMPORTANT: protocol_data.parameters is a Seaport struct and its inner
        # keys must stay camelCase. requests/json sends this body verbatim.
        body = {
            "criteria": {"collection": {"slug": slug}},
            "protocol_address": protocol_address,
            "protocol_data": protocol_data,
        }
        return self._request("POST", "/offers", json=body, request_class="normal")

    def get_order(self, *, chain: str, protocol_address: str, order_hash: str) -> dict[str, Any]:
        return self._request(
            "GET", f"/orders/chain/{chain}/protocol/{protocol_address}/{order_hash}",
            request_class="background",
        )

    def cancel_order(
        self,
        *,
        chain: str,
        protocol_address: str,
        order_hash: str,
        offerer_signature: str,
        auth_token: str | None = None,
    ) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else None
        kwargs: dict[str, Any] = {"json": {"offererSignature": offerer_signature}}
        if headers:
            kwargs["headers"] = headers
        return self._request(
            "POST", f"/orders/chain/{chain}/protocol/{protocol_address}/{order_hash}/cancel",
            request_class="normal", **kwargs,
        )

    def get_contract(self, chain: str, address: str) -> dict[str, Any]:
        return self._request("GET", f"/chain/{chain}/contract/{address}", cache_ttl=self.contract_cache_seconds)

    def get_events(
        self,
        *,
        event_type: str = "mint",
        after: int | None = None,
        before: int | None = None,
        limit: int = 200,
        cursor: str | None = None,
        chain: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"event_type": event_type, "limit": max(1, min(int(limit), 200))}
        if chain:
            params["chain"] = chain
        if after is not None:
            params["after"] = int(after)
        if before is not None:
            params["before"] = int(before)
        if cursor:
            params["next"] = cursor
        return self._request("GET", "/events", params=params, request_class="background")

    def build_mint(self, slug: str, minter: str, quantity: int) -> dict[str, Any]:
        return self._request("POST", f"/drops/{slug}/mint", json={"minter": minter, "quantity": quantity}, request_class="critical")


class RpcPool:
    """Verified RPC pool with fastest-read selection and parallel broadcast."""

    def __init__(self, chain: str, urls: Iterable[str], timeout: float = 5.0, broadcast_workers: int = 4):
        chain = normalize_chain(chain)
        if chain not in CHAIN_CONFIGS:
            raise ValueError(f"Unsupported chain: {chain}")
        self.chain = chain
        self.chain_id = int(CHAIN_CONFIGS[chain]["chain_id"])
        self.timeout = timeout
        self.broadcast_workers = max(1, broadcast_workers)
        # V4.9: keep a persistent broadcast pool. Creating a new executor for
        # every wallet/transaction adds avoidable latency exactly when a public
        # mint opens. The pool is intentionally larger than broadcast_workers
        # because several wallets may race at the same instant.
        self._broadcast_pool_workers = max(
            self.broadcast_workers,
            min(64, max(8, int(os.getenv("RPC_BROADCAST_POOL_WORKERS", str(self.broadcast_workers * 8)))))
        )
        self._broadcast_executor = ThreadPoolExecutor(
            max_workers=self._broadcast_pool_workers,
            thread_name_prefix=f"rpc-broadcast-{chain}",
        )
        self.clients: list[tuple[float, str, Web3]] = []

        import time
        seen: set[str] = set()
        for raw_url in urls:
            url = str(raw_url).strip()
            if not url or url in seen:
                continue
            seen.add(url)
            try:
                started = time.perf_counter()
                w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": timeout}))
                actual = int(w3.eth.chain_id)
                latency = time.perf_counter() - started
                if actual != self.chain_id:
                    log.warning("Ignoring RPC wrong chain id %s: got %s expected %s", _safe_rpc_url_for_log(url), actual, self.chain_id)
                    continue
                self.clients.append((latency, url, w3))
            except Exception as exc:
                log.warning("RPC unavailable %s: %s", _safe_rpc_url_for_log(url), exc)

        self.clients.sort(key=lambda item: item[0])
        if not self.clients:
            raise ConnectionError(f"No working RPC configured for {chain}")

    @property
    def primary(self) -> Web3:
        return self.clients[0][2]

    @property
    def primary_url(self) -> str:
        return self.clients[0][1]

    @property
    def urls(self) -> list[str]:
        return [url for _, url, _ in self.clients]

    def broadcast_raw_transaction(self, raw_tx: bytes) -> tuple[str, str]:
        """Broadcast the same signed transaction to verified RPCs in parallel.

        V4.9 uses one persistent executor for the lifetime of the process, so a
        multi-wallet public race does not repeatedly create/destroy thread pools.
        """
        def send(item: tuple[float, str, Web3]) -> tuple[str, str]:
            _, url, w3 = item
            tx_hash = w3.eth.send_raw_transaction(raw_tx)
            return tx_hash.hex(), url

        targets = self.clients[: max(1, min(self.broadcast_workers, len(self.clients)))]
        futures = [self._broadcast_executor.submit(send, item) for item in targets]
        errors: list[str] = []
        for future in as_completed(futures):
            try:
                tx_hash, url = future.result()
                for f in futures:
                    if f is not future:
                        f.cancel()
                return tx_hash, url
            except Exception as exc:
                errors.append(str(exc))
        raise RuntimeError("All RPC broadcasts failed: " + " | ".join(errors[:4]))

    def receipt_status(self, tx_hash: str) -> int | None:
        try:
            receipt = self.primary.eth.get_transaction_receipt(tx_hash)
            return int(receipt.get("status", 0))
        except TransactionNotFound:
            return None
        except Exception:
            return None

    def balance_native(self, address: str) -> Decimal:
        wei = int(self.primary.eth.get_balance(Web3.to_checksum_address(address)))
        return Decimal(wei) / Decimal(10**18)


def normalize_chain(value: str) -> str:
    v = str(value or "").strip().lower().replace(" ", "-")
    return CHAIN_ALIASES.get(v, v)


def default_rpcs(chain: str) -> list[str]:
    return list(CHAIN_CONFIGS.get(normalize_chain(chain), {}).get("default_rpcs", []))


def alchemy_rpc(chain: str, api_key: str) -> str | None:
    config = CHAIN_CONFIGS.get(normalize_chain(chain))
    if not config or not api_key or not config.get("alchemy_slug"):
        return None
    return f"https://{config['alchemy_slug']}.g.alchemy.com/v2/{api_key}"


def native_symbol(chain: str) -> str:
    return str(CHAIN_CONFIGS[normalize_chain(chain)]["native_symbol"])


def opensea_chain_name(chain: str) -> str:
    return str(CHAIN_CONFIGS[normalize_chain(chain)]["opensea_chain"])


def explorer_tx_url(chain: str, tx_hash: str) -> str:
    return str(CHAIN_CONFIGS.get(normalize_chain(chain), {}).get("explorer", "")) + tx_hash


def _int_value(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("0x"):
            return int(s, 16)
        if s == "":
            return 0
        return int(Decimal(s))
    raise ValueError(f"Cannot parse integer value: {value!r}")


def normalize_mint_tx(payload: dict[str, Any]) -> tuple[str, str, int]:
    obj = payload.get("transaction") if isinstance(payload.get("transaction"), dict) else payload
    to = obj.get("to") or obj.get("target")
    data = obj.get("data") or obj.get("calldata")
    value = _int_value(obj.get("value", 0))
    if not to or not data:
        raise ValueError("OpenSea mint response did not include target/to and calldata/data")
    if not Web3.is_address(to):
        raise ValueError(f"Invalid mint target: {to}")
    if not isinstance(data, str) or not data.startswith("0x"):
        raise ValueError("Invalid calldata returned by OpenSea")
    return Web3.to_checksum_address(to), data, value


def _priority_fee(w3: Web3) -> int:
    try:
        return int(w3.eth.max_priority_fee)
    except Exception:
        return int(Web3.to_wei(0.02, "gwei"))


def build_fee_fields(w3: Web3, strategy: str) -> dict[str, int]:
    """Build fee fields with a cost-first strategy.

    `smart` aims for the next few blocks without the large 2x+ maxFee headroom
    used by the old `fast` mode. The hard USD budget is enforced separately
    before signing, so this function focuses on bidding efficiently rather
    than aggressively.
    """
    strategy = (strategy or "smart").strip().lower()
    latest = w3.eth.get_block("latest")
    base_fee = latest.get("baseFeePerGas")
    if base_fee is None:
        gp = int(w3.eth.gas_price)
        multiplier = {
            "economy": 0.90,
            "smart": 1.00,
            "balanced": 1.05,
            "fast": 1.15,
            "turbo": 1.30,
        }.get(strategy, 1.0)
        return {"gasPrice": max(1, int(gp * multiplier))}

    base_fee = int(base_fee)
    network_priority = max(1, _priority_fee(w3))
    # Small floor so very-low-fee L2 transactions are still accepted.
    floor_priority = max(1, int(Web3.to_wei(0.005, "gwei")))

    if strategy == "economy":
        max_priority = max(floor_priority, int(network_priority * 0.55))
        max_fee = int(base_fee * 1.08) + max_priority
    elif strategy == "smart":
        # Ethereum base fee can move up by at most 12.5% per full block. 1.15x
        # gives enough headroom for the next block without the old 2.10x bid.
        max_priority = max(floor_priority, int(network_priority * 0.75))
        max_fee = int(base_fee * 1.15) + max_priority
    elif strategy == "fast":
        max_priority = max(floor_priority, int(network_priority * 1.05))
        max_fee = int(base_fee * 1.35) + max_priority
    elif strategy == "turbo":
        max_priority = max(floor_priority, int(network_priority * 1.35))
        max_fee = int(base_fee * 1.70) + max_priority
    else:
        max_priority = max(floor_priority, int(network_priority * 0.75))
        max_fee = int(base_fee * 1.15) + max_priority
    return {"maxPriorityFeePerGas": max_priority, "maxFeePerGas": max_fee, "type": 2}


def max_gas_cost_wei(gas_limit: int, fee_fields: dict[str, int]) -> int:
    per_gas = fee_fields.get("maxFeePerGas") or fee_fields.get("gasPrice") or 0
    return int(gas_limit) * int(per_gas)


def _http_status(exc: requests.HTTPError) -> int | None:
    return exc.response.status_code if exc.response is not None else None


def _classify_tx_exception_status(exc: Exception | str) -> str:
    """Classify common RPC send failures without changing the stable broadcaster.

    Race can skip a balance read for speed, so provider errors are also part of
    the safety/status layer. Nonce errors are separated so the race broadcaster
    can refresh the pending nonce, re-sign, and retry immediately.
    """
    text = str(exc or "").lower()
    nonce_markers = (
        "nonce too low",
        "nonce has already been used",
        "already used nonce",
        "old nonce",
        "invalid transaction nonce",
    )
    if any(marker in text for marker in nonce_markers):
        return "nonce_too_low"
    fee_low_markers = (
        "max fee per gas less than block base fee",
        "fee cap less than block base fee",
        "maxfeepergas less than block basefee",
        "max fee per gas less than base fee",
    )
    if any(marker in text for marker in fee_low_markers):
        return "fee_too_low"
    insufficient_markers = (
        "insufficient funds",
        "insufficient balance",
        "funds for gas",
        "gas * price + value",
        "gas required exceeds allowance (0)",
        "sender doesn't have enough funds",
        "sender does not have enough funds",
        "not enough funds",
    )
    return "insufficient_balance" if any(marker in text for marker in insufficient_markers) else "rpc_or_tx_error"


_RACE_NONCE_GUARD = threading.RLock()
_RACE_NONCE_LOCKS: dict[tuple[int, str], threading.Lock] = {}
_RACE_LOCAL_NEXT_NONCE: dict[tuple[int, str], int] = {}


def _race_nonce_lock(chain_id: int, address: str) -> threading.Lock:
    key = (int(chain_id), address.lower())
    with _RACE_NONCE_GUARD:
        lock = _RACE_NONCE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _RACE_NONCE_LOCKS[key] = lock
        return lock


def _race_choose_nonce(rpc_pool: "RpcPool", address: str) -> int:
    """Choose a nonce safe against another concurrent mint from this process.

    The provider's pending nonce is authoritative after restarts/external sends;
    the local floor closes the tiny propagation window between two successful
    broadcasts from the same wallet on the same chain.
    """
    address = Web3.to_checksum_address(address)
    network_pending = int(rpc_pool.primary.eth.get_transaction_count(address, "pending"))
    key = (int(rpc_pool.chain_id), address.lower())
    with _RACE_NONCE_GUARD:
        local_next = _RACE_LOCAL_NEXT_NONCE.get(key, network_pending)
        return max(network_pending, int(local_next))


def _race_local_nonce_floor(rpc_pool: "RpcPool", address: str) -> int | None:
    key = (int(rpc_pool.chain_id), address.lower())
    with _RACE_NONCE_GUARD:
        value = _RACE_LOCAL_NEXT_NONCE.get(key)
        return None if value is None else int(value)


def _race_mark_nonce_submitted(rpc_pool: "RpcPool", address: str, nonce: int) -> None:
    key = (int(rpc_pool.chain_id), address.lower())
    with _RACE_NONCE_GUARD:
        _RACE_LOCAL_NEXT_NONCE[key] = max(int(_RACE_LOCAL_NEXT_NONCE.get(key, 0)), int(nonce) + 1)


def _safe_rpc_url_for_log(url: str) -> str:
    """Never print API credentials embedded in RPC URLs."""
    text = str(url or "")
    text = re.sub(r"(/v2/)[^/?\s]+", r"\1***", text, flags=re.IGNORECASE)
    text = re.sub(r"([?&](?:api[_-]?key|key|token)=)[^&\s]+", r"\1***", text, flags=re.IGNORECASE)
    return text


def _build_mint_best_quantity(
    opensea: OpenSeaClient,
    slug: str,
    address: str,
    desired_quantity: int,
) -> tuple[dict[str, Any], int]:
    """Build mint data using the highest quantity OpenSea accepts.

    If the requested quantity is above a per-wallet or remaining-supply limit,
    OpenSea returns HTTP 422. We first probe quantity=1 to distinguish a real
    eligibility failure from a quantity-limit failure, then binary-search the
    highest accepted quantity. This keeps API calls logarithmic instead of
    trying every quantity one by one.
    """
    desired = max(1, min(int(desired_quantity), 100))
    try:
        return opensea.build_mint(slug, address, desired), desired
    except requests.HTTPError as original:
        if _http_status(original) != 422 or desired <= 1:
            raise

        # If even one token is rejected, the wallet/stage itself is not mintable.
        try:
            best_payload = opensea.build_mint(slug, address, 1)
        except requests.HTTPError:
            raise original

        best = 1
        low, high = 2, desired - 1
        while low <= high:
            mid = (low + high) // 2
            try:
                payload = opensea.build_mint(slug, address, mid)
                best = mid
                best_payload = payload
                low = mid + 1
            except requests.HTTPError as exc:
                if _http_status(exc) == 422:
                    high = mid - 1
                    continue
                raise
        return best_payload, best


def check_eligibility(opensea: OpenSeaClient, slug: str, wallet: WalletConfig, quantity: int) -> EligibilityResult:
    """Preflight through OpenSea. Does not sign or broadcast anything.

    V4.4 also probes the highest quantity OpenSea accepts so the eligibility
    screen reflects the actual usable quantity rather than failing just because
    the wallet default is above max-per-wallet.
    """
    try:
        payload, quantity_used = _build_mint_best_quantity(opensea, slug, wallet.address, quantity)
        target, _data, value = normalize_mint_tx(payload)
        return EligibilityResult(
            True,
            "eligible_now",
            mint_value_native=Decimal(value) / Decimal(10**18),
            target=target,
            detail="OpenSea returned ready-to-sign mint transaction data.",
            quantity_used=quantity_used,
        )
    except requests.HTTPError as exc:
        code = _http_status(exc)
        if code == 409:
            return EligibilityResult(None, "not_active_yet", detail=str(exc))
        if code == 422:
            return EligibilityResult(False, "not_eligible_now", detail=str(exc))
        if code == 429:
            return EligibilityResult(None, "rate_limited", detail=str(exc))
        return EligibilityResult(None, "opensea_error", detail=str(exc))
    except Exception as exc:
        return EligibilityResult(None, "preflight_error", detail=str(exc))


def mint_drop(
    *,
    rpc_pool: RpcPool,
    wallet: WalletConfig,
    opensea: OpenSeaClient,
    slug: str,
    quantity: int,
    gas_strategy: str,
    gas_limit_buffer: float,
    max_gas_native: Decimal,
    allow_paid: bool,
    max_mint_price_native: Decimal,
    max_total_native: Decimal,
    allowed_targets: set[str],
    paid_wallet_allowed: bool = True,
    max_gas_usd: Decimal = Decimal("0"),
    native_usd_price: Decimal | None = None,
) -> MintResult:
    account = Account.from_key(wallet.private_key)
    address = Web3.to_checksum_address(account.address)
    w3 = rpc_pool.primary

    requested_quantity = max(1, min(int(quantity), 100))
    try:
        mint_payload, quantity_used = _build_mint_best_quantity(opensea, slug, address, requested_quantity)
        target, calldata, value = normalize_mint_tx(mint_payload)
    except requests.HTTPError as exc:
        code = _http_status(exc)
        if code == 409:
            return MintResult(False, "not_mintable_yet", detail=str(exc))
        if code == 422:
            return MintResult(False, "precondition_failed", detail=str(exc))
        if code == 429:
            return MintResult(False, "rate_limited", detail=str(exc))
        return MintResult(False, "opensea_error", detail=str(exc))
    except Exception as exc:
        return MintResult(False, "bad_mint_payload", detail=str(exc))

    mint_value_native = Decimal(value) / Decimal(10**18)
    # Paid policy is checked before wallet-selection logic. This is important
    # for V4.2 auto-discovery, which is strictly free-only and must never open
    # a paid-selection flow. Manual watches with allow_paid=True still require
    # explicit wallet selection before any paid transaction is signed.
    if value > 0 and not allow_paid:
        return MintResult(False, "paid_not_allowed", mint_value_native=mint_value_native, target=target,
                          detail=f"Paid mint refused by policy: {mint_value_native} {native_symbol(rpc_pool.chain)}",
                          quantity_used=quantity_used)
    if value > 0 and not paid_wallet_allowed:
        return MintResult(
            False,
            "paid_wallet_selection_required",
            mint_value_native=mint_value_native,
            target=target,
            detail=f"Paid mint requires wallet selection: {mint_value_native} {native_symbol(rpc_pool.chain)}",
            quantity_used=quantity_used,
        )
    if max_mint_price_native > 0 and mint_value_native > max_mint_price_native:
        return MintResult(False, "mint_price_too_high", mint_value_native=mint_value_native, target=target,
                          detail=f"Mint price {mint_value_native} > cap {max_mint_price_native}",
                          quantity_used=quantity_used)
    if allowed_targets and target.lower() not in allowed_targets:
        return MintResult(False, "target_not_allowed", mint_value_native=mint_value_native, target=target,
                          detail=f"Refused target contract {target}", quantity_used=quantity_used)

    try:
        nonce = int(w3.eth.get_transaction_count(address, "pending"))
        base_tx = {
            "chainId": rpc_pool.chain_id,
            "from": address,
            "to": target,
            "data": calldata,
            "value": value,
            "nonce": nonce,
        }
        estimated = int(w3.eth.estimate_gas(base_tx))
        gas_limit = max(estimated, math.ceil(estimated * gas_limit_buffer))
        fees = build_fee_fields(w3, gas_strategy)
        gas_cost_wei = max_gas_cost_wei(gas_limit, fees)
        gas_cost_native = Decimal(gas_cost_wei) / Decimal(10**18)
        gas_cost_usd = (gas_cost_native * native_usd_price) if native_usd_price is not None else None
        total_max_native = mint_value_native + gas_cost_native

        # V4.1: hard USD gas budget. If a USD budget is enabled and we cannot
        # resolve the native-token USD price, fail closed instead of spending
        # without knowing the dollar cost.
        if max_gas_usd > 0:
            if gas_cost_usd is None:
                return MintResult(False, "gas_price_unavailable", mint_value_native=mint_value_native,
                                  gas_cost_native=gas_cost_native, gas_cost_usd=None,
                                  total_max_native=total_max_native, target=target,
                                  detail="USD gas budget is enabled but native/USD price is unavailable",
                                  quantity_used=quantity_used)
            if gas_cost_usd > max_gas_usd:
                return MintResult(False, "gas_usd_too_high", mint_value_native=mint_value_native,
                                  gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd,
                                  total_max_native=total_max_native, target=target,
                                  detail=f"Estimated max gas ${gas_cost_usd:.6f} > USD cap ${max_gas_usd}",
                                  quantity_used=quantity_used)

        if max_gas_native > 0 and gas_cost_native > max_gas_native:
            return MintResult(False, "gas_too_high", mint_value_native=mint_value_native,
                              gas_cost_native=gas_cost_native, total_max_native=total_max_native, target=target,
                              detail=f"Estimated max gas {gas_cost_native} > cap {max_gas_native}",
                              quantity_used=quantity_used)
        if max_total_native > 0 and total_max_native > max_total_native:
            return MintResult(False, "total_spend_too_high", mint_value_native=mint_value_native,
                              gas_cost_native=gas_cost_native, total_max_native=total_max_native, target=target,
                              detail=f"Estimated total max {total_max_native} > cap {max_total_native}",
                              quantity_used=quantity_used)

        balance = int(w3.eth.get_balance(address))
        needed = gas_cost_wei + value
        if balance < needed:
            return MintResult(False, "insufficient_balance", mint_value_native=mint_value_native,
                              gas_cost_native=gas_cost_native, total_max_native=total_max_native, target=target,
                              detail=f"Wallet balance {balance} wei < estimated requirement {needed} wei",
                              quantity_used=quantity_used)

        tx = {**base_tx, "gas": gas_limit, **fees}
        signed = account.sign_transaction(tx)
        raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
        tx_hash, rpc_url = rpc_pool.broadcast_raw_transaction(raw)
        return MintResult(
            True,
            "submitted",
            tx_hash=tx_hash,
            mint_value_native=mint_value_native,
            gas_cost_native=gas_cost_native,
            gas_cost_usd=gas_cost_usd,
            total_max_native=total_max_native,
            rpc=rpc_url,
            target=target,
            quantity_used=quantity_used,
            detail=(f"Quantity adjusted from {requested_quantity} to {quantity_used}." if quantity_used != requested_quantity else None),
        )
    except Exception as exc:
        return MintResult(False, _classify_tx_exception_status(exc), mint_value_native=mint_value_native, target=target, detail=str(exc),
                          quantity_used=locals().get("quantity_used"))


# ---------------------------------------------------------------------------
# V4.3 — direct SeaDrop public-mint fallback
# ---------------------------------------------------------------------------
# The automatic discovery path no longer depends on a collection being present
# in GET /drops. OpenSea Stream / Events can reveal a freshly minted NFT
# contract, then these helpers read and simulate the public SeaDrop directly.

SEADROP_ADDRESS = Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "feeRecipient", "type": "address"},
            {"name": "minterIfNotPayer", "type": "address"},
            {"name": "quantity", "type": "uint256"},
        ],
        "name": "mintPublic",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getPublicDrop",
        "outputs": [{
            "components": [
                {"name": "mintPrice", "type": "uint80"},
                {"name": "startTime", "type": "uint48"},
                {"name": "endTime", "type": "uint48"},
                {"name": "maxTotalMintableByWallet", "type": "uint16"},
                {"name": "feeBps", "type": "uint16"},
                {"name": "restrictFeeRecipients", "type": "bool"},
            ],
            "name": "",
            "type": "tuple",
        }],
        "stateMutability": "view",
        "type": "function",
    },
]

SUPPLY_ABI = [
    {"inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "maxSupply", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
]


# Every ERC721 SeaDrop token implements getMintStats(minter). SeaDrop itself
# calls this method before minting to enforce max-per-wallet and max-supply.
# Reading it lets the bot account for tokens minted outside this bot/DB and
# avoids paying gas for a transaction that is guaranteed to exceed a wallet cap.
NFT_MINT_STATS_ABI = [
    {
        "inputs": [{"name": "minter", "type": "address"}],
        "name": "getMintStats",
        "outputs": [
            {"name": "minterNumMinted", "type": "uint256"},
            {"name": "currentTotalSupply", "type": "uint256"},
            {"name": "maxSupply", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]


def read_nft_mint_stats(w3: Web3, nft_contract: str, minter: str) -> dict[str, int] | None:
    """Return SeaDrop token mint stats for one wallet, or None if unavailable."""
    try:
        token = w3.eth.contract(address=Web3.to_checksum_address(nft_contract), abi=NFT_MINT_STATS_ABI)
        raw = token.functions.getMintStats(Web3.to_checksum_address(minter)).call()
        if not isinstance(raw, (list, tuple)) or len(raw) < 3:
            return None
        return {
            "minter_num_minted": int(raw[0]),
            "current_total_supply": int(raw[1]),
            "max_supply": int(raw[2]),
        }
    except Exception as exc:
        log.debug("NFT getMintStats unavailable %s %s: %s", nft_contract, minter, exc)
        return None


def _adjust_quantity_from_mint_stats(
    *,
    desired: int,
    max_per_wallet: int | None,
    stats: dict[str, int] | None,
) -> tuple[int, str | None]:
    """Cap desired quantity using authoritative on-chain SeaDrop token stats."""
    q = max(1, int(desired))
    if not stats:
        return q, None
    if max_per_wallet:
        remaining_wallet = max(0, int(max_per_wallet) - int(stats.get("minter_num_minted", 0)))
        if remaining_wallet <= 0:
            return 0, "wallet_limit_reached"
        q = min(q, remaining_wallet)
    max_supply = int(stats.get("max_supply", 0))
    current_supply = int(stats.get("current_total_supply", 0))
    if max_supply > 0:
        remaining_supply = max(0, max_supply - current_supply)
        if remaining_supply <= 0:
            return 0, "sold_out"
        q = min(q, remaining_supply)
    return max(0, q), None


def seadrop_is_deployed(w3: Web3) -> bool:
    try:
        code = bytes(w3.eth.get_code(SEADROP_ADDRESS))
        return bool(code and code != b"\\x00")
    except Exception:
        return False


def _token_supply(w3: Web3, nft_contract: str) -> tuple[int | None, int | None, int | None]:
    try:
        token = w3.eth.contract(address=Web3.to_checksum_address(nft_contract), abi=SUPPLY_ABI)
        total = int(token.functions.totalSupply().call())
    except Exception:
        total = None
    try:
        token = w3.eth.contract(address=Web3.to_checksum_address(nft_contract), abi=SUPPLY_ABI)
        maximum = int(token.functions.maxSupply().call())
    except Exception:
        maximum = None
    remaining = None if total is None or maximum is None else max(0, maximum - total)
    return maximum, total, remaining


def read_seadrop_public_drop(w3: Web3, nft_contract: str) -> dict[str, Any] | None:
    """Read public SeaDrop configuration directly from chain.

    Returns None when SeaDrop is not deployed/reachable on this chain or the NFT
    address is invalid. A zeroed tuple is marked configured=False instead of
    being mistaken for an active free mint.
    """
    if not Web3.is_address(nft_contract) or not seadrop_is_deployed(w3):
        return None
    nft = Web3.to_checksum_address(nft_contract)
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        raw = seadrop.functions.getPublicDrop(nft).call()
        price = int(raw[0])
        start = int(raw[1])
        end = int(raw[2])
        max_wallet = int(raw[3])
        fee_bps = int(raw[4])
        restricted = bool(raw[5])
        recipients = []
        try:
            recipients = [Web3.to_checksum_address(x) for x in seadrop.functions.getAllowedFeeRecipients(nft).call()]
        except Exception:
            recipients = []
        maximum, total, remaining = _token_supply(w3, nft)
        # Allowed fee recipients are configured independently from the public
        # stage, so they must not make an all-zero PublicDrop look active.
        configured = bool(start or end or max_wallet or price or fee_bps)
        return {
            "contract_address": nft,
            "mint_price_wei": price,
            "start_time": start,
            "end_time": end,
            "max_per_wallet": max_wallet if max_wallet > 0 else None,
            "fee_bps": fee_bps,
            "restrict_fee_recipients": restricted,
            "fee_recipients": recipients,
            "configured": configured,
            "max_supply": maximum,
            "total_supply": total,
            "remaining_supply": remaining,
        }
    except Exception as exc:
        log.debug("SeaDrop public-drop read failed %s: %s", nft_contract, exc)
        return None


def _seadrop_fee_recipient(public: dict[str, Any]) -> str | None:
    recipients = list(public.get("fee_recipients") or [])
    if recipients:
        return Web3.to_checksum_address(recipients[0])
    if bool(public.get("restrict_fee_recipients")):
        return None
    # For unrestricted public drops any recipient is accepted. For a free mint
    # feeBps produces no payment, so zero address avoids inventing attribution.
    return ZERO_ADDRESS


def _seadrop_base_tx(
    w3: Web3,
    *,
    payer: str,
    nft_contract: str,
    fee_recipient: str,
    quantity: int,
    mint_price_wei: int,
    chain_id: int,
    nonce: int | None = None,
) -> dict[str, Any]:
    seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
    fn = seadrop.functions.mintPublic(
        Web3.to_checksum_address(nft_contract),
        Web3.to_checksum_address(fee_recipient),
        ZERO_ADDRESS,
        int(quantity),
    )
    data = fn._encode_transaction_data()
    tx: dict[str, Any] = {
        "chainId": int(chain_id),
        "from": Web3.to_checksum_address(payer),
        "to": SEADROP_ADDRESS,
        "data": data,
        "value": int(mint_price_wei) * int(quantity),
    }
    if nonce is not None:
        tx["nonce"] = int(nonce)
    return tx


def _best_seadrop_quantity(
    w3: Web3,
    *,
    payer: str,
    nft_contract: str,
    fee_recipient: str,
    desired_quantity: int,
    mint_price_wei: int,
    chain_id: int,
) -> tuple[int, int, dict[str, Any]]:
    """Return highest quantity whose mintPublic call successfully simulates."""
    desired = max(1, min(int(desired_quantity), 100))

    def simulate(qty: int) -> tuple[int, dict[str, Any]]:
        tx = _seadrop_base_tx(
            w3, payer=payer, nft_contract=nft_contract, fee_recipient=fee_recipient,
            quantity=qty, mint_price_wei=mint_price_wei, chain_id=chain_id,
        )
        estimated = int(w3.eth.estimate_gas(tx))
        return estimated, tx

    try:
        estimated, tx = simulate(desired)
        return desired, estimated, tx
    except Exception as original:
        if desired <= 1:
            raise original
        try:
            best_est, best_tx = simulate(1)
        except Exception:
            raise original
        best = 1
        low, high = 2, desired - 1
        while low <= high:
            mid = (low + high) // 2
            try:
                est, tx = simulate(mid)
                best, best_est, best_tx = mid, est, tx
                low = mid + 1
            except Exception:
                high = mid - 1
        return best, best_est, best_tx


def check_seadrop_eligibility(
    rpc_pool: RpcPool,
    wallet: WalletConfig,
    nft_contract: str,
    quantity: int,
) -> EligibilityResult:
    """On-chain public SeaDrop preflight without signing or broadcasting."""
    w3 = rpc_pool.primary
    public = read_seadrop_public_drop(w3, nft_contract)
    if not public or not public.get("configured"):
        return EligibilityResult(None, "seadrop_not_configured", detail="No configured public SeaDrop was found on-chain.")
    now = int(__import__("time").time())
    start = int(public.get("start_time") or 0)
    end = int(public.get("end_time") or 0)
    if start and now < start:
        return EligibilityResult(None, "not_active_yet", mint_value_native=Decimal(int(public["mint_price_wei"])) / Decimal(10**18), detail=f"Public mint opens at {start}")
    if end and now >= end:
        return EligibilityResult(False, "stage_ended", mint_value_native=Decimal(int(public["mint_price_wei"])) / Decimal(10**18), detail=f"Public mint ended at {end}")
    if public.get("remaining_supply") is not None and int(public["remaining_supply"]) <= 0:
        return EligibilityResult(False, "sold_out", detail="On-chain maxSupply - totalSupply is zero.")
    fee_recipient = _seadrop_fee_recipient(public)
    if not fee_recipient:
        return EligibilityResult(False, "no_fee_recipient", detail="SeaDrop restricts fee recipients but none were returned.")
    desired = max(1, min(int(quantity), 100))
    max_wallet = int(public.get("max_per_wallet") or 0) or None
    if max_wallet:
        desired = min(desired, max_wallet)
    if public.get("remaining_supply") is not None:
        desired = min(desired, max(1, int(public["remaining_supply"])))

    # The bot DB is not authoritative: the wallet may have minted previously
    # outside this bot. Use the NFT's SeaDrop getMintStats before simulation.
    stats = read_nft_mint_stats(w3, nft_contract, wallet.address)
    desired, blocked = _adjust_quantity_from_mint_stats(
        desired=desired, max_per_wallet=max_wallet, stats=stats,
    )
    if desired <= 0:
        return EligibilityResult(
            False, "not_eligible_now",
            mint_value_native=Decimal(int(public["mint_price_wei"])) / Decimal(10**18),
            target=SEADROP_ADDRESS,
            detail=("Wallet already reached the on-chain public mint limit." if blocked == "wallet_limit_reached" else "Sold out according to on-chain getMintStats."),
            quantity_used=0,
        )
    try:
        used, _estimated, _tx = _best_seadrop_quantity(
            w3, payer=wallet.address, nft_contract=nft_contract, fee_recipient=fee_recipient,
            desired_quantity=desired, mint_price_wei=int(public["mint_price_wei"]), chain_id=rpc_pool.chain_id,
        )
        return EligibilityResult(
            True, "eligible_now",
            mint_value_native=(Decimal(int(public["mint_price_wei"])) * Decimal(used)) / Decimal(10**18),
            target=SEADROP_ADDRESS,
            detail="Direct SeaDrop mintPublic simulation succeeded.",
            quantity_used=used,
        )
    except Exception as exc:
        return EligibilityResult(False, "not_eligible_now", mint_value_native=Decimal(int(public["mint_price_wei"])) / Decimal(10**18), target=SEADROP_ADDRESS, detail=str(exc))


def mint_seadrop_public(
    *,
    rpc_pool: RpcPool,
    wallet: WalletConfig,
    nft_contract: str,
    quantity: int,
    gas_strategy: str,
    gas_limit_buffer: float,
    max_gas_native: Decimal,
    allow_paid: bool,
    max_mint_price_native: Decimal,
    max_total_native: Decimal,
    allowed_targets: set[str],
    paid_wallet_allowed: bool = True,
    max_gas_usd: Decimal = Decimal("0"),
    native_usd_price: Decimal | None = None,
) -> MintResult:
    """Mint a public SeaDrop directly, preserving all V4.x safety guards."""
    account = Account.from_key(wallet.private_key)
    address = Web3.to_checksum_address(account.address)
    w3 = rpc_pool.primary
    public = read_seadrop_public_drop(w3, nft_contract)
    if not public or not public.get("configured"):
        return MintResult(False, "seadrop_not_configured", detail="No configured public SeaDrop was found on-chain.")

    now = int(__import__("time").time())
    start = int(public.get("start_time") or 0)
    end = int(public.get("end_time") or 0)
    if start and now < start:
        return MintResult(False, "not_mintable_yet", detail=f"Public mint opens at {start}")
    if end and now >= end:
        return MintResult(False, "precondition_failed", detail=f"Public mint ended at {end}")
    remaining = public.get("remaining_supply")
    if remaining is not None and int(remaining) <= 0:
        return MintResult(False, "precondition_failed", detail="Sold out according to on-chain supply.")

    price_each = int(public.get("mint_price_wei") or 0)
    desired = max(1, min(int(quantity), 100))
    max_wallet = int(public.get("max_per_wallet") or 0) or None
    if max_wallet:
        desired = min(desired, max_wallet)
    if remaining is not None:
        desired = min(desired, max(1, int(remaining)))

    # Account for mints made outside this bot. SeaDrop enforces the same stats
    # immediately before minting, so this prevents guaranteed per-wallet reverts.
    stats = read_nft_mint_stats(w3, nft_contract, address)
    desired, blocked = _adjust_quantity_from_mint_stats(
        desired=desired, max_per_wallet=max_wallet, stats=stats,
    )
    if desired <= 0:
        return MintResult(
            False, "precondition_failed", target=SEADROP_ADDRESS, quantity_used=0,
            detail=("Wallet already reached the on-chain public mint limit." if blocked == "wallet_limit_reached" else "Sold out according to on-chain getMintStats."),
        )

    # Paid policy is enforced before any simulation/signature work.
    preliminary_value = price_each * desired
    preliminary_native = Decimal(preliminary_value) / Decimal(10**18)
    if preliminary_value > 0 and not allow_paid:
        return MintResult(False, "paid_not_allowed", mint_value_native=preliminary_native, target=SEADROP_ADDRESS, detail="Paid direct SeaDrop mint refused by policy.", quantity_used=desired)
    if preliminary_value > 0 and not paid_wallet_allowed:
        return MintResult(False, "paid_wallet_selection_required", mint_value_native=preliminary_native, target=SEADROP_ADDRESS, detail="Paid direct SeaDrop mint requires explicit wallet selection.", quantity_used=desired)

    fee_recipient = _seadrop_fee_recipient(public)
    if not fee_recipient:
        return MintResult(False, "no_fee_recipient", target=SEADROP_ADDRESS, detail="Restricted SeaDrop has no allowed fee recipient.")
    if allowed_targets and SEADROP_ADDRESS.lower() not in allowed_targets:
        return MintResult(False, "target_not_allowed", target=SEADROP_ADDRESS, detail=f"Refused target contract {SEADROP_ADDRESS}")

    try:
        quantity_used, estimated, _sim_tx = _best_seadrop_quantity(
            w3, payer=address, nft_contract=nft_contract, fee_recipient=fee_recipient,
            desired_quantity=desired, mint_price_wei=price_each, chain_id=rpc_pool.chain_id,
        )
        value = price_each * quantity_used
        mint_value_native = Decimal(value) / Decimal(10**18)
        if max_mint_price_native > 0 and mint_value_native > max_mint_price_native:
            return MintResult(False, "mint_price_too_high", mint_value_native=mint_value_native, target=SEADROP_ADDRESS, quantity_used=quantity_used, detail=f"Mint price {mint_value_native} > cap {max_mint_price_native}")

        nonce = int(w3.eth.get_transaction_count(address, "pending"))
        base_tx = _seadrop_base_tx(
            w3, payer=address, nft_contract=nft_contract, fee_recipient=fee_recipient,
            quantity=quantity_used, mint_price_wei=price_each, chain_id=rpc_pool.chain_id, nonce=nonce,
        )
        gas_limit = max(estimated, math.ceil(estimated * gas_limit_buffer))
        fees = build_fee_fields(w3, gas_strategy)
        gas_cost_wei = max_gas_cost_wei(gas_limit, fees)
        gas_cost_native = Decimal(gas_cost_wei) / Decimal(10**18)
        gas_cost_usd = gas_cost_native * native_usd_price if native_usd_price is not None else None
        total_max_native = mint_value_native + gas_cost_native

        if max_gas_usd > 0:
            if gas_cost_usd is None:
                return MintResult(False, "gas_price_unavailable", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=quantity_used, detail="USD gas budget enabled but native/USD price unavailable")
            if gas_cost_usd > max_gas_usd:
                return MintResult(False, "gas_usd_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=quantity_used, detail=f"Estimated max gas ${gas_cost_usd:.6f} > USD cap ${max_gas_usd}")
        if max_gas_native > 0 and gas_cost_native > max_gas_native:
            return MintResult(False, "gas_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=quantity_used, detail=f"Estimated max gas {gas_cost_native} > cap {max_gas_native}")
        if max_total_native > 0 and total_max_native > max_total_native:
            return MintResult(False, "total_spend_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=quantity_used, detail=f"Estimated total max {total_max_native} > cap {max_total_native}")

        balance = int(w3.eth.get_balance(address))
        needed = gas_cost_wei + value
        if balance < needed:
            return MintResult(False, "insufficient_balance", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=quantity_used, detail=f"Wallet balance {balance} wei < estimated requirement {needed} wei")

        tx = {**base_tx, "gas": gas_limit, **fees}
        signed = account.sign_transaction(tx)
        raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
        tx_hash, rpc_url = rpc_pool.broadcast_raw_transaction(raw)
        return MintResult(True, "submitted", tx_hash=tx_hash, mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, rpc=rpc_url, target=SEADROP_ADDRESS, quantity_used=quantity_used, detail=(f"Direct SeaDrop; quantity adjusted from {quantity} to {quantity_used}." if quantity_used != quantity else "Direct SeaDrop mintPublic."))
    except Exception as exc:
        return MintResult(False, _classify_tx_exception_status(exc), target=SEADROP_ADDRESS, detail=str(exc))

# ---------------------------------------------------------------------------
# V4.9 — Ultra-fast public SeaDrop race lane
# ---------------------------------------------------------------------------

def read_seadrop_public_fast(w3: Web3, nft_contract: str) -> dict[str, Any] | None:
    """Read only the data needed to race a SeaDrop public mint.

    The normal reader also probes contract bytecode and token supply. Those are
    useful for dashboards, but they add several RPC round trips. The race lane
    deliberately performs only getPublicDrop and, when required, one allowed
    fee-recipient read.
    """
    if not Web3.is_address(nft_contract):
        return None
    nft = Web3.to_checksum_address(nft_contract)
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        raw = seadrop.functions.getPublicDrop(nft).call()
        price = int(raw[0])
        start = int(raw[1])
        end = int(raw[2])
        max_wallet = int(raw[3])
        fee_bps = int(raw[4])
        restricted = bool(raw[5])
        configured = bool(start or end or max_wallet or price or fee_bps)
        recipients: list[str] = []
        if configured and restricted:
            try:
                recipients = [
                    Web3.to_checksum_address(x)
                    for x in seadrop.functions.getAllowedFeeRecipients(nft).call()
                ]
            except Exception:
                recipients = []
        return {
            "contract_address": nft,
            "mint_price_wei": price,
            "start_time": start,
            "end_time": end,
            "max_per_wallet": max_wallet if max_wallet > 0 else None,
            "fee_bps": fee_bps,
            "restrict_fee_recipients": restricted,
            "fee_recipients": recipients,
            "configured": configured,
            # Intentionally omitted on the hot path. Supply is enforced by the
            # contract and can be refreshed by the background metadata lane.
            "max_supply": None,
            "total_supply": None,
            "remaining_supply": None,
        }
    except Exception as exc:
        log.debug("SeaDrop fast public read failed %s: %s", nft_contract, exc)
        return None


def _race_result_for_all(
    wallet_quantities: list[tuple[WalletConfig, int]],
    status: str,
    *,
    detail: str,
    mint_value_native: Decimal | None = None,
    target: str | None = None,
) -> dict[str, MintResult]:
    return {
        wallet.address.lower(): MintResult(
            False,
            status,
            mint_value_native=mint_value_native,
            target=target,
            detail=detail,
            quantity_used=max(1, min(int(quantity), 100)),
        )
        for wallet, quantity in wallet_quantities
    }


def _clamp_fee_fields_to_budget(
    fees: dict[str, int],
    *,
    gas_limit: int,
    max_gas_native: Decimal,
    max_gas_usd: Decimal,
    native_usd_price: Decimal | None,
) -> dict[str, int]:
    """Cap fee-per-gas fields to the user's configured gas budget.

    The race lane can start from a faster fee strategy and then trim the bid so
    the signed transaction's worst-case gas spend still respects the same USD
    / native caps. This is faster than simply rejecting a high-headroom `fast`
    quote and waiting for a later retry.
    """
    budget_wei: int | None = None
    if max_gas_native > 0:
        budget_wei = int(max_gas_native * Decimal(10**18))
    if max_gas_usd > 0 and native_usd_price is not None and native_usd_price > 0:
        usd_budget_wei = int((max_gas_usd / native_usd_price) * Decimal(10**18))
        budget_wei = usd_budget_wei if budget_wei is None else min(budget_wei, usd_budget_wei)
    if budget_wei is None or gas_limit <= 0:
        return dict(fees)

    per_gas_cap = max(1, budget_wei // int(gas_limit))
    out = dict(fees)
    if "gasPrice" in out:
        out["gasPrice"] = max(1, min(int(out["gasPrice"]), per_gas_cap))
        return out

    if "maxFeePerGas" in out:
        max_fee = max(1, min(int(out["maxFeePerGas"]), per_gas_cap))
        out["maxFeePerGas"] = max_fee
        if "maxPriorityFeePerGas" in out:
            out["maxPriorityFeePerGas"] = max(1, min(int(out["maxPriorityFeePerGas"]), max_fee))
    return out


def prepare_seadrop_race_transactions(
    *,
    rpc_pool: RpcPool,
    wallet_quantities: list[tuple[WalletConfig, int]],
    nft_contract: str,
    gas_strategy: str,
    gas_limit_buffer: float,
    max_gas_native: Decimal,
    max_total_native: Decimal,
    max_gas_usd: Decimal,
    native_usd_price: Decimal | None,
    allowed_targets: set[str],
    allow_paid: bool = False,
    paid_wallet_addresses: set[str] | None = None,
    max_mint_price_native: Decimal = Decimal("0"),
    fee_fields_override: dict[str, int] | None = None,
    public_override: dict[str, Any] | None = None,
    allow_before_start: bool = False,
    static_gas_limit: int | None = None,
    skip_balance_check: bool = False,
    clamp_fees_to_gas_budget: bool = False,
    allow_missing_usd_for_free: bool = False,
) -> dict[str, Any]:
    """Pre-build and sign a batch of SeaDrop public transactions.

    For a *scheduled* public opening, pass ``allow_before_start=True`` and a
    conservative ``static_gas_limit``. This allows the expensive work (public
    config, fee fields, nonces, balance checks and signatures) to finish before
    the opening timestamp; the launch step then performs only raw broadcasts.

    For a live mint discovered from Stream after it has already started, leave
    ``static_gas_limit=None``. One gas estimate is shared by every wallet that
    uses the same quantity instead of re-running the full SeaDrop read and gas
    simulation per wallet.
    """
    cleaned: list[tuple[WalletConfig, int]] = []
    for wallet, quantity in wallet_quantities:
        q = max(1, min(int(quantity), 100))
        cleaned.append((wallet, q))
    if not cleaned:
        return {"entries": [], "results": {}, "prepared_at": time.time()}

    w3 = rpc_pool.primary
    public = public_override or read_seadrop_public_fast(w3, nft_contract)
    if not public or not public.get("configured"):
        return {
            "entries": [],
            "results": _race_result_for_all(cleaned, "seadrop_not_configured", detail="No configured public SeaDrop was found."),
            "prepared_at": time.time(),
        }

    now = int(time.time())
    start = int(public.get("start_time") or 0)
    end = int(public.get("end_time") or 0)
    if start and now < start and not allow_before_start:
        return {
            "entries": [],
            "results": _race_result_for_all(cleaned, "not_mintable_yet", detail=f"Public mint opens at {start}"),
            "prepared_at": time.time(),
            "public": public,
        }
    if end and now >= end:
        return {
            "entries": [],
            "results": _race_result_for_all(cleaned, "precondition_failed", detail=f"Public mint ended at {end}"),
            "prepared_at": time.time(),
            "public": public,
        }

    price_each = int(public.get("mint_price_wei") or 0)
    max_per_wallet = int(public.get("max_per_wallet") or 0) or None
    paid_set = {x.lower() for x in (paid_wallet_addresses or set())}
    adjusted: list[tuple[WalletConfig, int]] = []
    results: dict[str, MintResult] = {}

    # Authoritative per-wallet mint counts. These calls run concurrently. For a
    # scheduled Public they happen during prewarm, before the opening timestamp.
    # For a surprise live mint they add only one parallel eth_call round-trip,
    # preventing guaranteed reverts (and wasted gas) when a wallet minted before.
    mint_stats: dict[str, dict[str, int] | None] = {}
    if max_per_wallet:
        def read_stats(wallet: WalletConfig) -> tuple[str, dict[str, int] | None]:
            return wallet.address.lower(), read_nft_mint_stats(w3, nft_contract, wallet.address)
        with ThreadPoolExecutor(max_workers=max(1, min(32, len(cleaned)))) as stats_executor:
            stats_futures = [stats_executor.submit(read_stats, wallet) for wallet, _q in cleaned]
            for future in as_completed(stats_futures):
                try:
                    addr_l, stats = future.result()
                    mint_stats[addr_l] = stats
                except Exception:
                    pass

    # Use the smallest observed global remaining supply to avoid knowingly
    # preparing more transactions than the NFT can still mint.
    global_remaining: int | None = None
    for stats in mint_stats.values():
        if not stats:
            continue
        maximum = int(stats.get("max_supply", 0))
        current = int(stats.get("current_total_supply", 0))
        if maximum > 0:
            remaining_now = max(0, maximum - current)
            global_remaining = remaining_now if global_remaining is None else min(global_remaining, remaining_now)

    for wallet, quantity in cleaned:
        q = min(quantity, max_per_wallet) if max_per_wallet else quantity
        q = max(1, min(q, 100))
        q, blocked = _adjust_quantity_from_mint_stats(
            desired=q, max_per_wallet=max_per_wallet, stats=mint_stats.get(wallet.address.lower()),
        )
        if q <= 0:
            results[wallet.address.lower()] = MintResult(
                False, "precondition_failed", target=SEADROP_ADDRESS, quantity_used=0,
                detail=("Wallet already reached the on-chain public mint limit." if blocked == "wallet_limit_reached" else "Sold out according to on-chain getMintStats."),
            )
            continue
        if global_remaining is not None:
            if global_remaining <= 0:
                results[wallet.address.lower()] = MintResult(False, "precondition_failed", target=SEADROP_ADDRESS, quantity_used=0, detail="No on-chain supply remains.")
                continue
            q = min(q, global_remaining)
            global_remaining -= q
        value = price_each * q
        value_native = Decimal(value) / Decimal(10**18)
        if value > 0 and not allow_paid:
            results[wallet.address.lower()] = MintResult(
                False, "paid_not_allowed", mint_value_native=value_native,
                target=SEADROP_ADDRESS, quantity_used=q,
                detail="Paid direct SeaDrop race refused by policy.",
            )
            continue
        if value > 0 and wallet.address.lower() not in paid_set:
            results[wallet.address.lower()] = MintResult(
                False, "paid_wallet_selection_required", mint_value_native=value_native,
                target=SEADROP_ADDRESS, quantity_used=q,
                detail="Paid direct SeaDrop race requires explicit wallet selection.",
            )
            continue
        if max_mint_price_native > 0 and value_native > max_mint_price_native:
            results[wallet.address.lower()] = MintResult(
                False, "mint_price_too_high", mint_value_native=value_native,
                target=SEADROP_ADDRESS, quantity_used=q,
                detail=f"Mint price {value_native} > cap {max_mint_price_native}",
            )
            continue
        adjusted.append((wallet, q))

    if not adjusted:
        return {"entries": [], "results": results, "prepared_at": time.time(), "public": public}

    fee_recipient = _seadrop_fee_recipient(public)
    if not fee_recipient:
        results.update(_race_result_for_all(adjusted, "no_fee_recipient", detail="Restricted SeaDrop has no allowed fee recipient.", target=SEADROP_ADDRESS))
        return {"entries": [], "results": results, "prepared_at": time.time(), "public": public}
    if allowed_targets and SEADROP_ADDRESS.lower() not in allowed_targets:
        results.update(_race_result_for_all(adjusted, "target_not_allowed", detail=f"Refused target contract {SEADROP_ADDRESS}", target=SEADROP_ADDRESS))
        return {"entries": [], "results": results, "prepared_at": time.time(), "public": public}

    # One fee snapshot for the whole project, not once per wallet. The race
    # lane can inject a background-refreshed snapshot so the launch path does
    # not spend an RPC round-trip fetching base/priority fees.
    try:
        fees = dict(fee_fields_override) if fee_fields_override else build_fee_fields(w3, gas_strategy)
    except Exception as exc:
        results.update(_race_result_for_all(adjusted, "rpc_or_tx_error", detail=f"Fee read failed: {exc}", target=SEADROP_ADDRESS))
        return {"entries": [], "results": results, "prepared_at": time.time(), "public": public}

    gas_by_qty: dict[int, int] = {}
    if static_gas_limit is not None and int(static_gas_limit) > 21_000:
        for _wallet, q in adjusted:
            gas_by_qty[q] = int(static_gas_limit)
    else:
        # The mint is already live, so estimate once per distinct quantity.
        groups: dict[int, WalletConfig] = {}
        for wallet, q in adjusted:
            groups.setdefault(q, wallet)
        for q, sample_wallet in groups.items():
            try:
                sample_tx = _seadrop_base_tx(
                    w3,
                    payer=sample_wallet.address,
                    nft_contract=nft_contract,
                    fee_recipient=fee_recipient,
                    quantity=q,
                    mint_price_wei=price_each,
                    chain_id=rpc_pool.chain_id,
                )
                estimated = int(w3.eth.estimate_gas(sample_tx))
                gas_by_qty[q] = max(estimated, math.ceil(estimated * gas_limit_buffer))
            except Exception as exc:
                # Keep only this quantity group out of the batch; the caller can
                # retry on the next fast tick or fall back to the normal path.
                for wallet, wallet_q in adjusted:
                    if wallet_q == q:
                        results[wallet.address.lower()] = MintResult(
                            False, "simulation_failed", target=SEADROP_ADDRESS,
                            quantity_used=q, detail=str(exc),
                        )

    eligible = [(w, q) for w, q in adjusted if q in gas_by_qty and w.address.lower() not in results]
    if not eligible:
        return {"entries": [], "results": results, "prepared_at": time.time(), "public": public}

    # Fetch pending nonces (and, during prewarm, balances) concurrently. These
    # calls finish before opening for scheduled races.
    wallet_runtime: dict[str, tuple[int, int | None]] = {}

    def read_wallet_runtime(item: tuple[WalletConfig, int]) -> tuple[str, int, int | None]:
        wallet, _q = item
        address = Web3.to_checksum_address(wallet.address)
        nonce = int(w3.eth.get_transaction_count(address, "pending"))
        balance = None if skip_balance_check else int(w3.eth.get_balance(address))
        return wallet.address.lower(), nonce, balance

    with ThreadPoolExecutor(max_workers=min(32, len(eligible))) as executor:
        futures = [executor.submit(read_wallet_runtime, item) for item in eligible]
        for future in as_completed(futures):
            try:
                addr, nonce, balance = future.result()
                wallet_runtime[addr] = (nonce, balance)
            except Exception as exc:
                log.debug("Race wallet prewarm failed: %s", exc)

    entries: list[dict[str, Any]] = []
    for wallet, q in eligible:
        addr_l = wallet.address.lower()
        runtime = wallet_runtime.get(addr_l)
        if runtime is None:
            results[addr_l] = MintResult(False, "rpc_or_tx_error", target=SEADROP_ADDRESS, quantity_used=q, detail="Could not prefetch pending nonce.")
            continue
        nonce, balance = runtime
        gas_limit = int(gas_by_qty[q])
        tx_fees = (
            _clamp_fee_fields_to_budget(
                fees, gas_limit=gas_limit, max_gas_native=max_gas_native,
                max_gas_usd=max_gas_usd, native_usd_price=native_usd_price,
            )
            if clamp_fees_to_gas_budget else dict(fees)
        )
        gas_cost_wei = max_gas_cost_wei(gas_limit, tx_fees)
        gas_cost_native = Decimal(gas_cost_wei) / Decimal(10**18)
        gas_cost_usd = gas_cost_native * native_usd_price if native_usd_price is not None else None
        value = price_each * q
        mint_value_native = Decimal(value) / Decimal(10**18)
        total_max_native = mint_value_native + gas_cost_native

        if max_gas_usd > 0:
            if gas_cost_usd is None:
                # V4.13.1: free Race may continue when the USD oracle is temporarily
                # unavailable. This matches the prior Ultra-Race behavior: the
                # transaction is still constrained by the native cap (if set) and
                # by the wallet's actual balance/provider acceptance. Paid mints
                # remain fail-closed.
                if not (allow_missing_usd_for_free and value == 0):
                    results[addr_l] = MintResult(False, "gas_price_unavailable", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=q, detail="USD gas budget enabled but native/USD price unavailable")
                    continue
            elif gas_cost_usd > max_gas_usd:
                results[addr_l] = MintResult(False, "gas_usd_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=q, detail=f"Estimated max gas ${gas_cost_usd:.6f} > USD cap ${max_gas_usd}")
                continue
        if max_gas_native > 0 and gas_cost_native > max_gas_native:
            results[addr_l] = MintResult(False, "gas_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=q, detail=f"Estimated max gas {gas_cost_native} > cap {max_gas_native}")
            continue
        if max_total_native > 0 and total_max_native > max_total_native:
            results[addr_l] = MintResult(False, "total_spend_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=q, detail=f"Estimated total max {total_max_native} > cap {max_total_native}")
            continue
        if balance is not None and balance < gas_cost_wei + value:
            results[addr_l] = MintResult(False, "insufficient_balance", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=q, detail=f"Wallet balance {balance} wei < estimated requirement {gas_cost_wei + value} wei")
            continue

        account = Account.from_key(wallet.private_key)
        base_tx = _seadrop_base_tx(
            w3,
            payer=wallet.address,
            nft_contract=nft_contract,
            fee_recipient=fee_recipient,
            quantity=q,
            mint_price_wei=price_each,
            chain_id=rpc_pool.chain_id,
            nonce=nonce,
        )
        tx = {**base_tx, "gas": gas_limit, **tx_fees}
        try:
            signed = account.sign_transaction(tx)
            raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
        except Exception as exc:
            results[addr_l] = MintResult(False, "signing_failed", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=q, detail=str(exc))
            continue
        entries.append({
            "wallet": wallet,
            "quantity": q,
            "raw": raw,
            "nonce": nonce,
            "gas_limit": gas_limit,
            "gas_cost_native": gas_cost_native,
            "gas_cost_usd": gas_cost_usd,
            "mint_value_native": mint_value_native,
            "total_max_native": total_max_native,
            "fee_fields": tx_fees,
        })

    return {
        "entries": entries,
        "results": results,
        "public": public,
        "fee_recipient": fee_recipient,
        "fees": fees,
        "nft_contract": Web3.to_checksum_address(nft_contract),
        "prepared_at": time.time(),
        "static_gas": static_gas_limit is not None,
        "chain_id": int(rpc_pool.chain_id),
        "gas_strategy": gas_strategy,
        "max_gas_native": max_gas_native,
        "max_gas_usd": max_gas_usd,
        "native_usd_price": native_usd_price,
        "clamp_fees_to_gas_budget": bool(clamp_fees_to_gas_budget),
        "allow_missing_usd_for_free": bool(allow_missing_usd_for_free),
    }


def refresh_seadrop_race_bundle_fees(
    bundle: dict[str, Any],
    fee_fields: dict[str, int] | None,
    *,
    native_usd_price: Decimal | None = None,
    w3: Web3 | None = None,
) -> dict[str, Any]:
    """Locally re-sign a prepared bundle when the warmed fee snapshot moved up.

    No RPC call is performed here. The caller supplies the already-warmed fee
    snapshot. This closes the common prewarm gap where a transaction is signed
    a few seconds early and the chain base fee rises before opening.
    """
    if not fee_fields or not bundle.get("entries"):
        return bundle
    public = dict(bundle.get("public") or {})
    fee_recipient = bundle.get("fee_recipient") or public.get("fee_recipient")
    nft_contract = bundle.get("nft_contract")
    if not fee_recipient or not nft_contract:
        return bundle

    old_fees = dict(bundle.get("fees") or {})
    old_cap = int(old_fees.get("maxFeePerGas") or old_fees.get("gasPrice") or 0)
    new_cap = int(fee_fields.get("maxFeePerGas") or fee_fields.get("gasPrice") or 0)
    if new_cap <= old_cap:
        return bundle

    max_gas_native = Decimal(str(bundle.get("max_gas_native") or "0"))
    max_gas_usd = Decimal(str(bundle.get("max_gas_usd") or "0"))
    price_usd = native_usd_price if native_usd_price is not None else bundle.get("native_usd_price")
    if price_usd is not None and not isinstance(price_usd, Decimal):
        price_usd = Decimal(str(price_usd))
    allow_missing_usd = bool(bundle.get("allow_missing_usd_for_free"))
    clamp = bool(bundle.get("clamp_fees_to_gas_budget"))
    price_each = int(public.get("mint_price_wei") or 0)

    refreshed: list[dict[str, Any]] = []
    for entry in list(bundle.get("entries") or []):
        gas_limit = int(entry.get("gas_limit") or 0)
        candidate_fees = dict(fee_fields)
        if clamp:
            candidate_fees = _clamp_fee_fields_to_budget(
                candidate_fees, gas_limit=gas_limit, max_gas_native=max_gas_native,
                max_gas_usd=max_gas_usd, native_usd_price=price_usd,
            )
        gas_cost_wei = max_gas_cost_wei(gas_limit, candidate_fees)
        gas_cost_native = Decimal(gas_cost_wei) / Decimal(10**18)
        gas_cost_usd = gas_cost_native * price_usd if price_usd is not None else None
        if max_gas_native > 0 and gas_cost_native > max_gas_native:
            return bundle
        if max_gas_usd > 0:
            if gas_cost_usd is None and not (allow_missing_usd and price_each == 0):
                return bundle
            if gas_cost_usd is not None and gas_cost_usd > max_gas_usd:
                return bundle

        wallet: WalletConfig = entry["wallet"]
        base_tx = _seadrop_base_tx(
            w3 or Web3(), payer=wallet.address, nft_contract=nft_contract, fee_recipient=fee_recipient,
            quantity=int(entry["quantity"]), mint_price_wei=price_each,
            chain_id=int(bundle.get("chain_id") or 0) or None, nonce=int(entry["nonce"]),
        )
        # Encoding/signing is local-only. ``w3`` may be the live primary Web3
        # instance, but this helper performs no network request.
        tx = {**base_tx, "gas": gas_limit, **candidate_fees}
        account = Account.from_key(wallet.private_key)
        signed = account.sign_transaction(tx)
        raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
        updated = dict(entry)
        updated.update({
            "raw": raw,
            "fee_fields": candidate_fees,
            "gas_cost_native": gas_cost_native,
            "gas_cost_usd": gas_cost_usd,
            "total_max_native": Decimal(price_each * int(entry["quantity"])) / Decimal(10**18) + gas_cost_native,
        })
        refreshed.append(updated)
    bundle = dict(bundle)
    bundle["entries"] = refreshed
    bundle["fees"] = dict(fee_fields)
    bundle["native_usd_price"] = price_usd
    bundle["fee_refreshed_at"] = time.time()
    return bundle


def broadcast_seadrop_race_transactions(
    *,
    rpc_pool: RpcPool,
    bundle: dict[str, Any],
    max_parallel_wallets: int = 10,
) -> dict[str, MintResult]:
    """Broadcast a pre-signed SeaDrop race bundle concurrently.

    V4.11.3 keeps the proven RpcPool broadcaster from V4.11.1, but serializes
    broadcasts only per wallet+chain long enough to guarantee a fresh nonce.
    Different wallets still broadcast fully in parallel. If the provider still
    reports ``nonce too low``, the entry is re-signed once with a freshly read
    pending nonce and re-broadcast immediately.
    """
    results: dict[str, MintResult] = dict(bundle.get("results") or {})
    entries = list(bundle.get("entries") or [])
    if not entries:
        return results

    public = dict(bundle.get("public") or {})
    fee_recipient = bundle.get("fee_recipient") or public.get("fee_recipient")
    nft_contract = bundle.get("nft_contract")
    price_each = int(public.get("mint_price_wei") or 0)

    def resign(entry: dict[str, Any], nonce: int, fee_fields: dict[str, int] | None = None):
        wallet: WalletConfig = entry["wallet"]
        if not nft_contract or not fee_recipient:
            raise RuntimeError("Race bundle missing SeaDrop contract metadata for nonce/fee refresh.")
        base_tx = _seadrop_base_tx(
            rpc_pool.primary,
            payer=wallet.address,
            nft_contract=nft_contract,
            fee_recipient=fee_recipient,
            quantity=int(entry["quantity"]),
            mint_price_wei=price_each,
            chain_id=rpc_pool.chain_id,
            nonce=int(nonce),
        )
        chosen_fees = dict(fee_fields if fee_fields is not None else (entry.get("fee_fields") or {}))
        tx = {**base_tx, "gas": int(entry["gas_limit"]), **chosen_fees}
        account = Account.from_key(wallet.private_key)
        signed = account.sign_transaction(tx)
        return getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")

    def refreshed_fee_fields(
        entry: dict[str, Any], rejected_error: Exception | str | None = None
    ) -> tuple[dict[str, int] | None, MintResult | None]:
        """Get one live fee quote after an RPC explicitly rejects a stale prewarm fee.

        This path is only entered after ``fee_too_low``; normal successful races
        pay zero extra RPC latency. The user's configured gas limits are checked
        again before the replacement signature is broadcast.
        """
        try:
            fields = build_fee_fields(rpc_pool.primary, str(bundle.get("gas_strategy") or "fast"))
        except Exception as exc:
            return None, MintResult(False, "rpc_or_tx_error", target=SEADROP_ADDRESS, quantity_used=int(entry.get("quantity") or 1), detail=f"Live fee refresh failed: {exc}")

        gas_limit = int(entry.get("gas_limit") or 0)
        max_gas_native = Decimal(str(bundle.get("max_gas_native") or "0"))
        max_gas_usd = Decimal(str(bundle.get("max_gas_usd") or "0"))
        price_usd = bundle.get("native_usd_price")
        if price_usd is not None and not isinstance(price_usd, Decimal):
            price_usd = Decimal(str(price_usd))
        unclamped_fields = dict(fields)
        if bool(bundle.get("clamp_fees_to_gas_budget")):
            fields = _clamp_fee_fields_to_budget(
                fields, gas_limit=gas_limit, max_gas_native=max_gas_native,
                max_gas_usd=max_gas_usd, native_usd_price=price_usd,
            )

        # If the provider explicitly told us the current block base fee, never
        # re-sign a replacement below that fee just to satisfy a user budget.
        # Such a transaction is invalid and only wastes the race opportunity.
        # This check adds zero RPC calls: the baseFee comes from the rejection.
        rejected_text = str(rejected_error or "")
        base_match = re.search(r"base\s*fee(?:pergas)?\s*[:=]\s*(\d+)|basefee\s*[:=]\s*(\d+)", rejected_text, flags=re.IGNORECASE)
        rejected_base_fee = None
        if base_match:
            try:
                rejected_base_fee = int(base_match.group(1) or base_match.group(2))
            except Exception:
                rejected_base_fee = None
        if rejected_base_fee is not None and "maxFeePerGas" in fields:
            fee_cap = int(fields.get("maxFeePerGas") or 0)
            if fee_cap < rejected_base_fee:
                min_native = Decimal(gas_limit * rejected_base_fee) / Decimal(10**18)
                min_usd = min_native * price_usd if price_usd is not None else None
                mint_value_native = Decimal(price_each * int(entry.get("quantity") or 1)) / Decimal(10**18)
                status = "gas_usd_too_high" if max_gas_usd > 0 and min_usd is not None else "gas_too_high"
                cap_text = (
                    f"USD cap ${max_gas_usd}" if status == "gas_usd_too_high"
                    else f"native cap {max_gas_native}"
                )
                return None, MintResult(
                    False, status, mint_value_native=mint_value_native,
                    gas_cost_native=min_native, gas_cost_usd=min_usd,
                    total_max_native=mint_value_native + min_native, target=SEADROP_ADDRESS,
                    quantity_used=int(entry.get("quantity") or 1),
                    detail=(f"Current block base fee requires at least {rejected_base_fee} wei/gas; "
                            f"configured {cap_text} cannot fund a valid replacement at gasLimit={gas_limit}."),
                )

        gas_cost_wei = max_gas_cost_wei(gas_limit, fields)
        gas_cost_native = Decimal(gas_cost_wei) / Decimal(10**18)
        gas_cost_usd = gas_cost_native * price_usd if price_usd is not None else None
        mint_value_native = Decimal(price_each * int(entry.get("quantity") or 1)) / Decimal(10**18)
        total_max_native = mint_value_native + gas_cost_native

        if max_gas_native > 0 and gas_cost_native > max_gas_native:
            return None, MintResult(False, "gas_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=int(entry.get("quantity") or 1), detail=f"Fresh gas estimate {gas_cost_native} > native cap {max_gas_native}")
        if max_gas_usd > 0:
            allow_missing = bool(bundle.get("allow_missing_usd_for_free")) and price_each == 0
            if gas_cost_usd is None and not allow_missing:
                return None, MintResult(False, "gas_price_unavailable", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=int(entry.get("quantity") or 1), detail="USD gas budget enabled but native/USD price unavailable during live fee refresh")
            if gas_cost_usd is not None and gas_cost_usd > max_gas_usd:
                return None, MintResult(False, "gas_usd_too_high", mint_value_native=mint_value_native, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd, total_max_native=total_max_native, target=SEADROP_ADDRESS, quantity_used=int(entry.get("quantity") or 1), detail=f"Fresh max gas ${gas_cost_usd:.6f} > USD cap ${max_gas_usd}")
        return fields, None

    def send(entry: dict[str, Any]) -> tuple[str, MintResult]:
        wallet: WalletConfig = entry["wallet"]
        addr_l = wallet.address.lower()
        lock = _race_nonce_lock(rpc_pool.chain_id, wallet.address)
        with lock:
            chosen_nonce = int(entry.get("nonce") or 0)
            raw = entry["raw"]
            try:
                # Zero-RPC fast path: when this process has already submitted a
                # transaction for the same wallet+chain after prewarm, its local
                # nonce floor proves this signature is stale. Re-sign immediately.
                # If there is no local evidence, broadcast the original V4.11.1
                # raw transaction without adding a nonce RPC round-trip.
                local_floor = _race_local_nonce_floor(rpc_pool, wallet.address)
                if local_floor is not None and local_floor > chosen_nonce:
                    chosen_nonce = int(local_floor)
                    raw = resign(entry, chosen_nonce)
                    log.info(
                        "Race nonce refreshed | chain=%s | wallet=%s | old=%s | new=%s",
                        rpc_pool.chain, wallet.address[:6] + "…" + wallet.address[-4:],
                        entry.get("nonce"), chosen_nonce,
                    )

                try:
                    tx_hash, rpc_url = rpc_pool.broadcast_raw_transaction(raw)
                except Exception as first_exc:
                    status = _classify_tx_exception_status(first_exc)
                    if status == "nonce_too_low":
                        # A different transaction may have landed between the first
                        # nonce read and broadcast. Refresh and retry once immediately.
                        retry_nonce = max(_race_choose_nonce(rpc_pool, wallet.address), chosen_nonce + 1)
                        raw = resign(entry, retry_nonce)
                        chosen_nonce = retry_nonce
                        log.info(
                            "Race nonce retry | chain=%s | wallet=%s | nonce=%s",
                            rpc_pool.chain, wallet.address[:6] + "…" + wallet.address[-4:], chosen_nonce,
                        )
                        tx_hash, rpc_url = rpc_pool.broadcast_raw_transaction(raw)
                    elif status == "fee_too_low":
                        # A pre-signed transaction can become stale if the chain's
                        # base fee jumps between prewarm and opening. Refresh one
                        # live fee snapshot, re-sign locally, and retry immediately
                        # instead of waiting for another Stream event.
                        fresh_fields, blocked = refreshed_fee_fields(entry, first_exc)
                        if blocked is not None:
                            return addr_l, blocked
                        assert fresh_fields is not None
                        raw = resign(entry, chosen_nonce, fresh_fields)
                        log.info(
                            "Race fee refreshed | chain=%s | wallet=%s | old=%s | new=%s",
                            rpc_pool.chain, wallet.address[:6] + "…" + wallet.address[-4:],
                            (entry.get("fee_fields") or {}).get("maxFeePerGas") or (entry.get("fee_fields") or {}).get("gasPrice"),
                            fresh_fields.get("maxFeePerGas") or fresh_fields.get("gasPrice"),
                        )
                        tx_hash, rpc_url = rpc_pool.broadcast_raw_transaction(raw)
                        # Keep result accounting aligned with the replacement tx.
                        gas_cost_wei = max_gas_cost_wei(int(entry["gas_limit"]), fresh_fields)
                        entry["fee_fields"] = fresh_fields
                        entry["gas_cost_native"] = Decimal(gas_cost_wei) / Decimal(10**18)
                        price_usd = bundle.get("native_usd_price")
                        if price_usd is not None and not isinstance(price_usd, Decimal):
                            price_usd = Decimal(str(price_usd))
                        entry["gas_cost_usd"] = entry["gas_cost_native"] * price_usd if price_usd is not None else None
                        entry["total_max_native"] = entry["mint_value_native"] + entry["gas_cost_native"]
                    else:
                        raise

                _race_mark_nonce_submitted(rpc_pool, wallet.address, chosen_nonce)
                return addr_l, MintResult(
                    True,
                    "submitted",
                    tx_hash=tx_hash,
                    mint_value_native=entry["mint_value_native"],
                    gas_cost_native=entry["gas_cost_native"],
                    gas_cost_usd=entry["gas_cost_usd"],
                    total_max_native=entry["total_max_native"],
                    detail=f"V4.13.1 nonce/fee-safe race-lane SeaDrop transaction (nonce={chosen_nonce}).",
                    rpc=rpc_url,
                    target=SEADROP_ADDRESS,
                    quantity_used=int(entry["quantity"]),
                )
            except Exception as exc:
                return addr_l, MintResult(
                    False,
                    _classify_tx_exception_status(exc),
                    mint_value_native=entry["mint_value_native"],
                    gas_cost_native=entry["gas_cost_native"],
                    gas_cost_usd=entry["gas_cost_usd"],
                    total_max_native=entry["total_max_native"],
                    detail=str(exc),
                    target=SEADROP_ADDRESS,
                    quantity_used=int(entry["quantity"]),
                )

    # Nonce locks serialize only transactions from the *same* wallet. Separate
    # wallets remain parallel, preserving the multi-wallet race behavior.
    with ThreadPoolExecutor(max_workers=max(1, min(int(max_parallel_wallets), len(entries)))) as executor:
        futures = [executor.submit(send, entry) for entry in entries]
        for future in as_completed(futures):
            addr, result = future.result()
            results[addr] = result
    return results

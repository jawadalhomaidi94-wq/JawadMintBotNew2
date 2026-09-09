from __future__ import annotations

import logging
import math
import os
import random
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
        "default_rpcs": [],
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

        # V4.7: all REST calls share one rate-limit state even though each
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
                "User-Agent": "OpenSea-Mint-Guardian/4.7",
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
                    log.warning("Ignoring RPC wrong chain id %s: got %s expected %s", url, actual, self.chain_id)
                    continue
                self.clients.append((latency, url, w3))
            except Exception as exc:
                log.warning("RPC unavailable %s: %s", url, exc)

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
        def send(item: tuple[float, str, Web3]) -> tuple[str, str]:
            _, url, w3 = item
            tx_hash = w3.eth.send_raw_transaction(raw_tx)
            return tx_hash.hex(), url

        workers = min(self.broadcast_workers, len(self.clients))
        errors: list[str] = []
        executor = ThreadPoolExecutor(max_workers=workers)
        closed = False
        try:
            futures = [executor.submit(send, item) for item in self.clients]
            for future in as_completed(futures):
                try:
                    tx_hash, url = future.result()
                    for f in futures:
                        if f is not future:
                            f.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    closed = True
                    return tx_hash, url
                except Exception as exc:
                    errors.append(str(exc))
        finally:
            if not closed:
                executor.shutdown(wait=False, cancel_futures=True)
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
        return MintResult(False, "rpc_or_tx_error", mint_value_native=mint_value_native, target=target, detail=str(exc),
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
    if public.get("max_per_wallet"):
        desired = min(desired, int(public["max_per_wallet"]))
    if public.get("remaining_supply") is not None:
        desired = min(desired, max(1, int(public["remaining_supply"])))
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
    if public.get("max_per_wallet"):
        desired = min(desired, int(public["max_per_wallet"]))
    if remaining is not None:
        desired = min(desired, max(1, int(remaining)))

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
        return MintResult(False, "rpc_or_tx_error", target=SEADROP_ADDRESS, detail=str(exc))

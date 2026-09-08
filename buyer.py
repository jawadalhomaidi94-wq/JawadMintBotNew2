from __future__ import annotations

import logging
import math
import threading
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

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "X-API-KEY": self.api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "OpenSea-Mint-Guardian/4.2",
            })
            self._local.session = session
        return session

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        r = self._session().request(method, f"{self.BASE_URL}{path}", timeout=self.timeout, **kwargs)
        if r.status_code >= 400:
            text = r.text[:1500]
            raise requests.HTTPError(f"OpenSea {r.status_code}: {text}", response=r)
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected OpenSea API response")
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
        return self._request("GET", "/drops", params=params)

    def get_drop(self, slug: str) -> dict[str, Any]:
        return self._request("GET", f"/drops/{slug}")

    def get_collection(self, slug: str) -> dict[str, Any]:
        return self._request("GET", f"/collections/{slug}")

    def build_mint(self, slug: str, minter: str, quantity: int) -> dict[str, Any]:
        return self._request("POST", f"/drops/{slug}/mint", json={"minter": minter, "quantity": quantity})


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
    """Preflight through OpenSea. Does not sign or broadcast anything."""
    try:
        payload = opensea.build_mint(slug, wallet.address, quantity)
        target, _data, value = normalize_mint_tx(payload)
        return EligibilityResult(
            True,
            "eligible_now",
            mint_value_native=Decimal(value) / Decimal(10**18),
            target=target,
            detail="OpenSea returned ready-to-sign mint transaction data.",
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

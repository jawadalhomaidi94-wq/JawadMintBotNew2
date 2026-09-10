from __future__ import annotations

"""OpenSea Collection Offer subsystem for Mint Guardian V4.12.2.

This module is intentionally isolated from the mint Race Lane.  Nothing here is
called from the main candidate loop, Stream handler, SeaDrop WSS worker, or Race
scheduler.  Network work happens only after an explicit Telegram action and is
submitted to a dedicated offer executor by :class:`OfferControllerMixin`.
"""

import hashlib
import json
import logging
import math
import os
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Any

from eth_account import Account
from eth_account.messages import encode_typed_data
from web3 import Web3

from buyer import (
    CHAIN_CONFIGS,
    OpenSeaClient,
    RpcPool,
    WalletConfig,
    build_fee_fields,
    max_gas_cost_wei,
    native_symbol,
    normalize_chain,
    opensea_chain_name,
)

log = logging.getLogger("opensea-offers")

# Current OpenSea / Seaport constants.  New OpenSea orders use Seaport 1.6.
SEAPORT_1_6 = Web3.to_checksum_address("0x0000000000000068F116a894984e2DB1123eB395")
SEAPORT_1_5 = Web3.to_checksum_address("0x00000000000000ADc04C56Bf30aC9d3c0aAF14dC")
SIGNED_ZONE = Web3.to_checksum_address("0x000056f7000000ece9003ca63978907a00ffd100")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# OpenSea conduit used by most EVM chains.
OPENSEA_CONDUIT_KEY = "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000"
OPENSEA_CONDUIT_ADDRESS = Web3.to_checksum_address("0x1e0049783f008a0085193e00003d00cd54003c71")
# Current OpenSea SDK uses this same default conduit on Robinhood Chain too.

WETH_BY_CHAIN: dict[str, str] = {
    "ethereum": Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    "ink": Web3.to_checksum_address("0x4200000000000000000000000000000000000006"),
    "robinhood": Web3.to_checksum_address("0x0bd7d308f8e1639fab988df18a8011f41eacad73"),
    "base": Web3.to_checksum_address("0x4200000000000000000000000000000000000006"),
    "optimism": Web3.to_checksum_address("0x4200000000000000000000000000000000000006"),
    "arbitrum": Web3.to_checksum_address("0x82af49447d8a07e3bd95bd0d56f35241523fbab1"),
    "polygon": Web3.to_checksum_address("0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"),
}

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

SEAPORT_COUNTER_ABI = [
    {
        "inputs": [{"name": "offerer", "type": "address"}],
        "name": "getCounter",
        "outputs": [{"name": "counter", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

ORDER_EIP712_TYPES = {
    "OrderComponents": [
        {"name": "offerer", "type": "address"},
        {"name": "zone", "type": "address"},
        {"name": "offer", "type": "OfferItem[]"},
        {"name": "consideration", "type": "ConsiderationItem[]"},
        {"name": "orderType", "type": "uint8"},
        {"name": "startTime", "type": "uint256"},
        {"name": "endTime", "type": "uint256"},
        {"name": "zoneHash", "type": "bytes32"},
        {"name": "salt", "type": "uint256"},
        {"name": "conduitKey", "type": "bytes32"},
        {"name": "counter", "type": "uint256"},
    ],
    "OfferItem": [
        {"name": "itemType", "type": "uint8"},
        {"name": "token", "type": "address"},
        {"name": "identifierOrCriteria", "type": "uint256"},
        {"name": "startAmount", "type": "uint256"},
        {"name": "endAmount", "type": "uint256"},
    ],
    "ConsiderationItem": [
        {"name": "itemType", "type": "uint8"},
        {"name": "token", "type": "address"},
        {"name": "identifierOrCriteria", "type": "uint256"},
        {"name": "startAmount", "type": "uint256"},
        {"name": "endAmount", "type": "uint256"},
        {"name": "recipient", "type": "address"},
    ],
}

ORDER_HASH_EIP712_TYPES = {
    "OrderHash": [{"name": "orderHash", "type": "bytes32"}],
}


@dataclass
class OfferFundingCheck:
    wallet_address: str
    weth_balance_wei: int
    allowance_wei: int
    required_wei: int
    weth_balance: Decimal
    allowance: Decimal
    required_weth: Decimal
    balance_ok: bool
    approval_ok: bool
    spender: str
    weth_address: str


@dataclass
class OfferCreateResult:
    ok: bool
    status: str
    order_hash: str | None = None
    response: dict[str, Any] | None = None
    protocol_data: dict[str, Any] | None = None
    total_weth: Decimal | None = None
    detail: str | None = None


@dataclass
class ApprovalResult:
    ok: bool
    status: str
    tx_hash: str | None = None
    detail: str | None = None
    gas_cost_native: Decimal | None = None
    gas_cost_usd: Decimal | None = None


def _as_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _fmt_decimal(value: Decimal | None, places: int = 6) -> str:
    if value is None:
        return "غير متاح"
    text = f"{value:.{places}f}".rstrip("0").rstrip(".")
    return text or "0"


def _checksum_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    if not Web3.is_address(text):
        return None
    return Web3.to_checksum_address(text)


def _extract_rows(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def _offer_quantity(row: dict[str, Any]) -> int:
    for key in ("remaining_quantity", "remainingQuantity", "quantity"):
        value = row.get(key)
        try:
            if value is not None and int(value) > 0:
                return int(value)
        except (TypeError, ValueError):
            pass
    protocol = row.get("protocol_data") or row.get("protocolData") or {}
    params = protocol.get("parameters") if isinstance(protocol, dict) else {}
    consideration = params.get("consideration") if isinstance(params, dict) else None
    if isinstance(consideration, list) and consideration:
        first = consideration[0] if isinstance(consideration[0], dict) else {}
        for key in ("startAmount", "endAmount", "start_amount", "end_amount"):
            try:
                amount = int(first.get(key))
                if amount > 0:
                    return amount
            except (TypeError, ValueError):
                continue
    return 1


def _price_parts(row: dict[str, Any]) -> tuple[Decimal | None, str | None, int]:
    price = row.get("price")
    if not isinstance(price, dict):
        return None, None, 18
    current = price.get("current")
    if not isinstance(current, dict):
        return None, None, 18
    symbol = str(current.get("currency") or "").upper().strip() or None
    try:
        decimals = int(current.get("decimals", 18))
    except (TypeError, ValueError):
        decimals = 18
    raw = current.get("value")
    try:
        value = Decimal(str(raw)) / (Decimal(10) ** decimals)
    except (InvalidOperation, TypeError, ValueError):
        return None, symbol, decimals
    return value, symbol, decimals


def _offer_slug(row: dict[str, Any]) -> str | None:
    criteria = row.get("criteria")
    if isinstance(criteria, dict):
        collection = criteria.get("collection")
        if isinstance(collection, dict) and collection.get("slug"):
            return str(collection["slug"])
    asset = row.get("asset")
    if isinstance(asset, dict):
        collection = asset.get("collection")
        if isinstance(collection, dict) and collection.get("slug"):
            return str(collection["slug"])
        if isinstance(collection, str) and collection:
            return collection
    collection = row.get("collection")
    if isinstance(collection, dict) and collection.get("slug"):
        return str(collection["slug"])
    return None


def _offer_chain(row: dict[str, Any], fallback: str = "ethereum") -> str:
    raw = row.get("chain") or row.get("chain_name") or row.get("chainName") or fallback
    return normalize_chain(str(raw))


def _offer_hash(row: dict[str, Any]) -> str | None:
    value = row.get("order_hash") or row.get("orderHash") or row.get("order_hash_v2")
    text = str(value or "").strip()
    return text if text.startswith("0x") else None


def _protocol_address(row: dict[str, Any]) -> str:
    value = row.get("protocol_address") or row.get("protocolAddress") or SEAPORT_1_6
    return _checksum_or_none(value) or SEAPORT_1_6


class CollectionOfferService:
    """Real OpenSea Collection Offer builder/signing helper.

    The service intentionally has no threads, timers, Stream listeners, or main-loop
    hooks.  Its methods are synchronous and are invoked only by the offer executor.
    """

    def __init__(
        self,
        opensea: OpenSeaClient,
        store: Any,
        rpc_pools: dict[str, RpcPool],
        price_oracle: Any,
        *,
        scoped_token: str = "",
    ) -> None:
        self.opensea = opensea
        self.store = store
        self.rpc_pools = rpc_pools
        self.price_oracle = price_oracle
        self.scoped_token = scoped_token.strip()

    @staticmethod
    def weth_address(chain: str) -> str:
        chain = normalize_chain(chain)
        if chain not in WETH_BY_CHAIN:
            raise ValueError(f"Collection Offers غير مدعومة بعد على الشبكة: {chain}")
        return WETH_BY_CHAIN[chain]

    @staticmethod
    def conduit(chain: str) -> tuple[str, str]:
        # OpenSea's current SDK uses the same default conduit for the supported
        # EVM chains in this bot, including Robinhood Chain.
        _ = normalize_chain(chain)
        return OPENSEA_CONDUIT_KEY, OPENSEA_CONDUIT_ADDRESS

    def eth_usd(self) -> Decimal | None:
        return self.price_oracle.get_usd("ETH")

    @staticmethod
    def usdt_to_weth(unit_usdt: Decimal, quantity: int, eth_usd: Decimal) -> tuple[int, Decimal]:
        if unit_usdt <= 0 or quantity <= 0 or eth_usd <= 0:
            raise ValueError("Offer price, quantity and ETH/USD must be positive")
        total_usdt = unit_usdt * Decimal(quantity)
        weth = total_usdt / eth_usd
        wei = int((weth * Decimal(10**18)).to_integral_value(rounding=ROUND_CEILING))
        return max(1, wei), Decimal(max(1, wei)) / Decimal(10**18)

    def funding_check(self, chain: str, wallet: WalletConfig, required_wei: int) -> OfferFundingCheck:
        chain = normalize_chain(chain)
        pool = self.rpc_pools.get(chain)
        if not pool:
            raise ValueError(f"No working RPC for {chain}")
        weth = self.weth_address(chain)
        _key, spender = self.conduit(chain)
        token = pool.primary.eth.contract(address=weth, abi=ERC20_ABI)
        owner = Web3.to_checksum_address(wallet.address)
        balance = int(token.functions.balanceOf(owner).call())
        allowance = int(token.functions.allowance(owner, spender).call())
        return OfferFundingCheck(
            wallet_address=owner,
            weth_balance_wei=balance,
            allowance_wei=allowance,
            required_wei=int(required_wei),
            weth_balance=Decimal(balance) / Decimal(10**18),
            allowance=Decimal(allowance) / Decimal(10**18),
            required_weth=Decimal(required_wei) / Decimal(10**18),
            balance_ok=balance >= int(required_wei),
            approval_ok=allowance >= int(required_wei),
            spender=spender,
            weth_address=weth,
        )

    def approve_exact(
        self,
        chain: str,
        wallet: WalletConfig,
        amount_wei: int,
        *,
        gas_strategy: str = "smart",
        gas_limit_buffer: float = 1.08,
        max_gas_native: Decimal = Decimal("0"),
        max_gas_usd: Decimal = Decimal("0"),
        native_usd_price: Decimal | None = None,
    ) -> ApprovalResult:
        """Approve only the amount requested by this offer review.

        This is never called automatically.  The Telegram UI exposes a separate
        explicit Approval button before the final Offer confirmation.
        """
        chain = normalize_chain(chain)
        pool = self.rpc_pools.get(chain)
        if not pool:
            return ApprovalResult(False, "rpc_unavailable", detail=f"No RPC for {chain}")
        try:
            weth = self.weth_address(chain)
            _conduit_key, spender = self.conduit(chain)
            account = Account.from_key(wallet.private_key)
            owner = Web3.to_checksum_address(account.address)
            token = pool.primary.eth.contract(address=weth, abi=ERC20_ABI)
            fn = token.functions.approve(spender, int(amount_wei))
            nonce = int(pool.primary.eth.get_transaction_count(owner, "pending"))
            base_tx = fn.build_transaction({
                "from": owner,
                "nonce": nonce,
                "chainId": pool.chain_id,
            })
            estimated = int(pool.primary.eth.estimate_gas(base_tx))
            gas_limit = max(estimated, math.ceil(estimated * max(1.0, gas_limit_buffer)))
            fees = build_fee_fields(pool.primary, gas_strategy)
            gas_cost_wei = max_gas_cost_wei(gas_limit, fees)
            gas_cost_native = Decimal(gas_cost_wei) / Decimal(10**18)
            gas_cost_usd = gas_cost_native * native_usd_price if native_usd_price is not None else None
            if max_gas_native > 0 and gas_cost_native > max_gas_native:
                return ApprovalResult(False, "gas_too_high", detail=f"Approval gas {gas_cost_native} > cap {max_gas_native}", gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd)
            if max_gas_usd > 0:
                if gas_cost_usd is None:
                    return ApprovalResult(False, "gas_price_unavailable", detail="USD gas cap enabled but native/USD price unavailable", gas_cost_native=gas_cost_native)
                if gas_cost_usd > max_gas_usd:
                    return ApprovalResult(False, "gas_usd_too_high", detail=f"Approval gas {gas_cost_usd:.6f} USDT > cap {max_gas_usd} USDT", gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd)
            balance_native = int(pool.primary.eth.get_balance(owner))
            if balance_native < gas_cost_wei:
                return ApprovalResult(False, "insufficient_native_gas", detail="Native balance is insufficient for Approval gas", gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd)
            tx = {**base_tx, "gas": gas_limit, **fees}
            signed = account.sign_transaction(tx)
            raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
            tx_hash, _rpc = pool.broadcast_raw_transaction(raw)
            return ApprovalResult(True, "submitted", tx_hash=tx_hash, gas_cost_native=gas_cost_native, gas_cost_usd=gas_cost_usd)
        except Exception as exc:
            return ApprovalResult(False, "approval_error", detail=str(exc))

    def _required_fee_items(self, collection: dict[str, Any], token: str, total_wei: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        rows = collection.get("fees") if isinstance(collection, dict) else None
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not bool(row.get("required")):
                continue
            recipient = _checksum_or_none(row.get("recipient"))
            if not recipient:
                continue
            # OpenSea's collection response represents e.g. 1.0 as 1 percent.
            percentage = _as_decimal(row.get("fee"))
            if percentage <= 0:
                continue
            amount = int((Decimal(total_wei) * percentage / Decimal(100)).to_integral_value(rounding=ROUND_CEILING))
            if amount <= 0:
                continue
            out.append({
                "itemType": 1,
                "token": token,
                "identifierOrCriteria": "0",
                "startAmount": str(amount),
                "endAmount": str(amount),
                "recipient": recipient,
            })
        return out

    def offer_commitment(
        self, *, slug: str, chain: str, unit_usdt: Decimal, quantity: int, eth_usd: Decimal
    ) -> tuple[int, int, int, Decimal, Decimal, Decimal]:
        """Return bid, required fees and conservative total funding requirement.

        OpenSea-required fees are extra WETH consideration items on an Offer.
        Balance/allowance checks therefore cover bid + required fees, while the
        displayed Offer price remains the user's per-NFT bid.
        """
        bid_wei, bid_weth = self.usdt_to_weth(unit_usdt, quantity, eth_usd)
        collection = self.opensea.get_collection_for_offer(slug)
        weth = self.weth_address(chain)
        fee_items = self._required_fee_items(collection, weth, bid_wei)
        fee_wei = sum(int(item.get("startAmount") or 0) for item in fee_items)
        commitment_wei = int(bid_wei) + int(fee_wei)
        fee_weth = Decimal(fee_wei) / Decimal(10**18)
        commitment_weth = Decimal(commitment_wei) / Decimal(10**18)
        return bid_wei, fee_wei, commitment_wei, bid_weth, fee_weth, commitment_weth

    @staticmethod
    def _normalise_consideration_item(item: dict[str, Any], offerer: str) -> dict[str, Any]:
        # Build Offer currently returns a Seaport-shaped item.  Accept either
        # camelCase or old snake_case defensively, but always post camelCase.
        return {
            "itemType": int(item.get("itemType", item.get("item_type", 4))),
            "token": Web3.to_checksum_address(str(item.get("token"))),
            "identifierOrCriteria": str(item.get("identifierOrCriteria", item.get("identifier_or_criteria", "0"))),
            "startAmount": str(item.get("startAmount", item.get("start_amount", "1"))),
            "endAmount": str(item.get("endAmount", item.get("end_amount", item.get("startAmount", "1")))),
            "recipient": Web3.to_checksum_address(str(item.get("recipient") or offerer)),
        }

    def build_order_components(
        self,
        *,
        chain: str,
        wallet: WalletConfig,
        slug: str,
        quantity: int,
        total_weth_wei: int,
        duration_hours: int,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Build the exact OpenSea/Seaport order components, but do not sign."""
        chain = normalize_chain(chain)
        pool = self.rpc_pools.get(chain)
        if not pool:
            raise ValueError(f"No working RPC for {chain}")
        quantity = max(1, min(int(quantity), 100))
        account = Account.from_key(wallet.private_key)
        offerer = Web3.to_checksum_address(account.address)
        weth = self.weth_address(chain)
        conduit_key, _spender = self.conduit(chain)

        build = self.opensea.build_collection_offer(
            offerer=offerer,
            quantity=quantity,
            slug=slug,
            protocol_address=SEAPORT_1_6,
            offer_protection_enabled=True,
        )
        partial = build.get("partialParameters") or build.get("partial_parameters") or {}
        consideration_raw = partial.get("consideration") if isinstance(partial, dict) else None
        if not isinstance(consideration_raw, list) or not consideration_raw or not isinstance(consideration_raw[0], dict):
            raise ValueError("OpenSea build offer response did not contain partialParameters.consideration[0]")
        nft_item = self._normalise_consideration_item(consideration_raw[0], offerer)

        collection = self.opensea.get_collection_for_offer(slug)
        fee_items = self._required_fee_items(collection, weth, int(total_weth_wei))
        consideration = [nft_item, *fee_items]

        zone = _checksum_or_none(partial.get("zone")) or SIGNED_ZONE
        zone_hash = str(partial.get("zoneHash") or partial.get("zone_hash") or ("0x" + "00" * 32))
        if not zone_hash.startswith("0x") or len(zone_hash) != 66:
            raise ValueError("OpenSea returned an invalid zoneHash")

        seaport = pool.primary.eth.contract(address=SEAPORT_1_6, abi=SEAPORT_COUNTER_ABI)
        counter = int(seaport.functions.getCounter(offerer).call())
        now = int(time.time())
        end = now + max(1, min(int(duration_hours), 24 * 30)) * 3600
        salt = secrets.randbits(256)

        components = {
            "offerer": offerer,
            "zone": zone,
            "offer": [{
                "itemType": 1,
                "token": weth,
                "identifierOrCriteria": "0",
                "startAmount": str(int(total_weth_wei)),
                "endAmount": str(int(total_weth_wei)),
            }],
            "consideration": consideration,
            "orderType": 2,  # PARTIAL_RESTRICTED
            "startTime": str(max(0, now - 1)),
            "endTime": str(end),
            "zoneHash": zone_hash,
            "salt": str(salt),
            "conduitKey": conduit_key,
            "counter": str(counter),
            "totalOriginalConsiderationItems": str(len(consideration)),
        }
        criteria = {"collection": {"slug": slug}}
        return components, criteria, build

    @staticmethod
    def _eip712_message_components(components: dict[str, Any]) -> dict[str, Any]:
        # totalOriginalConsiderationItems is an OrderParameters field used when
        # fulfilling; it is deliberately not part of Seaport's OrderComponents
        # EIP-712 type hash.
        return {
            key: components[key]
            for key in (
                "offerer", "zone", "offer", "consideration", "orderType",
                "startTime", "endTime", "zoneHash", "salt", "conduitKey", "counter",
            )
        }

    def sign_components(self, chain: str, wallet: WalletConfig, components: dict[str, Any]) -> str:
        chain = normalize_chain(chain)
        pool = self.rpc_pools[chain]
        typed = {
            "types": {"EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ], **ORDER_EIP712_TYPES},
            "primaryType": "OrderComponents",
            "domain": {
                "name": "Seaport",
                "version": "1.6",
                "chainId": int(pool.chain_id),
                "verifyingContract": SEAPORT_1_6,
            },
            "message": self._eip712_message_components(components),
        }
        signable = encode_typed_data(full_message=typed)
        signed = Account.from_key(wallet.private_key).sign_message(signable)
        return "0x" + bytes(signed.signature).hex()

    def create_collection_offer(
        self,
        *,
        chain: str,
        wallet: WalletConfig,
        slug: str,
        quantity: int,
        unit_usdt: Decimal,
        duration_hours: int,
        eth_usd: Decimal | None = None,
    ) -> OfferCreateResult:
        """Final financial action: recheck funding -> build -> sign -> post."""
        try:
            eth_usd = eth_usd or self.eth_usd()
            if eth_usd is None or eth_usd <= 0:
                return OfferCreateResult(False, "eth_price_unavailable", detail="ETH/USD price is unavailable")
            bid_wei, _fee_wei, commitment_wei, total_weth, _fee_weth, commitment_weth = self.offer_commitment(
                slug=slug, chain=chain, unit_usdt=unit_usdt, quantity=quantity, eth_usd=eth_usd
            )
            check = self.funding_check(chain, wallet, commitment_wei)
            if not check.balance_ok:
                return OfferCreateResult(False, "insufficient_weth", total_weth=total_weth, detail=f"WETH balance {check.weth_balance} (≈ {check.weth_balance * eth_usd} USDT) < fee-inclusive requirement {commitment_weth} WETH (≈ {commitment_weth * eth_usd} USDT)")
            if not check.approval_ok:
                return OfferCreateResult(False, "approval_required", total_weth=total_weth, detail=f"WETH allowance {check.allowance} (≈ {check.allowance * eth_usd} USDT) < fee-inclusive requirement {commitment_weth} WETH (≈ {commitment_weth * eth_usd} USDT)")

            components, criteria, _build = self.build_order_components(
                chain=chain,
                wallet=wallet,
                slug=slug,
                quantity=quantity,
                total_weth_wei=bid_wei,
                duration_hours=duration_hours,
            )
            # One last zero-cost allowance/balance check immediately before the
            # private key is used for signing.  This closes the review->confirm gap.
            final_check = self.funding_check(chain, wallet, commitment_wei)
            if not final_check.balance_ok:
                return OfferCreateResult(False, "insufficient_weth", total_weth=total_weth, detail="WETH balance changed before signing (fee-inclusive)")
            if not final_check.approval_ok:
                return OfferCreateResult(False, "approval_required", total_weth=total_weth, detail="WETH allowance changed before signing (fee-inclusive)")

            signature = self.sign_components(chain, wallet, components)
            protocol_data = {"parameters": components, "signature": signature}
            response = self.opensea.post_collection_offer(
                slug=slug,
                protocol_address=SEAPORT_1_6,
                protocol_data=protocol_data,
            )
            order_hash = _offer_hash(response)
            if not order_hash:
                # OpenSea's current response is the Offer directly.  Keep the
                # result usable even if a future response wraps it one level.
                nested = response.get("offer") if isinstance(response.get("offer"), dict) else None
                if nested:
                    order_hash = _offer_hash(nested)
                    response = nested
            if not order_hash:
                return OfferCreateResult(False, "post_missing_order_hash", response=response, protocol_data=protocol_data, total_weth=total_weth, detail="OpenSea accepted the request but no order_hash was returned")
            return OfferCreateResult(True, "created", order_hash=order_hash, response=response, protocol_data=protocol_data, total_weth=total_weth)
        except Exception as exc:
            return OfferCreateResult(False, "offer_error", detail=str(exc))

    def market_snapshot(self, slug: str, chain: str, limit: int = 20) -> dict[str, Any]:
        payload = self.opensea.get_collection_offers(slug, limit=max(3, min(int(limit), 50)))
        rows = _extract_rows(payload, "offers", "orders")
        eth_usd = self.eth_usd()
        converted: list[dict[str, Any]] = []
        for row in rows:
            total, symbol, _decimals = _price_parts(row)
            if total is None or total <= 0:
                continue
            qty = max(1, _offer_quantity(row))
            unit_token = total / Decimal(qty)
            unit_usdt: Decimal | None = None
            if symbol in {"WETH", "ETH"}:
                if eth_usd is not None:
                    unit_usdt = unit_token * eth_usd
            elif symbol in {"USDT", "USDC", "DAI"}:
                unit_usdt = unit_token
            if unit_usdt is None:
                continue
            converted.append({
                "row": row,
                "order_hash": _offer_hash(row),
                "symbol": symbol or "?",
                "quantity": qty,
                "total_token": total,
                "unit_token": unit_token,
                "unit_usdt": unit_usdt,
            })
        converted.sort(key=lambda x: x["unit_usdt"], reverse=True)
        return {
            "slug": slug,
            "chain": normalize_chain(chain),
            "eth_usd": eth_usd,
            "offers": converted,
            "top_usdt": converted[0]["unit_usdt"] if converted else Decimal("0"),
            "checked_at": time.time(),
        }

    def account_active_offers(self, wallet_address: str, *, chains: list[str] | None = None, limit: int = 50) -> list[dict[str, Any]]:
        payload = self.opensea.get_account_offers(
            wallet_address,
            limit=max(1, min(int(limit), 50)),
            chains=[opensea_chain_name(c) for c in (chains or []) if normalize_chain(c) in CHAIN_CONFIGS] or None,
        )
        return _extract_rows(payload, "offers", "orders")

    def cancel_offer(self, *, chain: str, wallet: WalletConfig, protocol_address: str, order_hash: str) -> dict[str, Any]:
        chain = normalize_chain(chain)
        pool = self.rpc_pools.get(chain)
        if not pool:
            raise ValueError(f"No RPC for {chain}")
        protocol = _checksum_or_none(protocol_address) or SEAPORT_1_6
        protocol_version = "1.5" if protocol.lower() == SEAPORT_1_5.lower() else "1.6"
        if not isinstance(order_hash, str) or not order_hash.startswith("0x") or len(order_hash) != 66:
            raise ValueError("Invalid order hash")
        typed = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                **ORDER_HASH_EIP712_TYPES,
            },
            "primaryType": "OrderHash",
            "domain": {
                "name": "Seaport",
                "version": protocol_version,
                "chainId": int(pool.chain_id),
                "verifyingContract": protocol,
            },
            "message": {"orderHash": order_hash},
        }
        signed = Account.from_key(wallet.private_key).sign_message(encode_typed_data(full_message=typed))
        signature = "0x" + bytes(signed.signature).hex()
        return self.opensea.cancel_order(
            chain=opensea_chain_name(chain),
            protocol_address=protocol,
            order_hash=order_hash,
            offerer_signature=signature,
            auth_token=self.scoped_token or None,
        )

    @staticmethod
    def remote_offer_to_record(row: dict[str, Any], wallet_name: str, wallet_address: str) -> dict[str, Any] | None:
        order_hash = _offer_hash(row)
        slug = _offer_slug(row)
        if not order_hash or not slug:
            return None
        chain = _offer_chain(row)
        total, symbol, _ = _price_parts(row)
        qty = max(1, _offer_quantity(row))
        unit = (total / Decimal(qty)) if total is not None else None
        protocol = row.get("protocol_data") or row.get("protocolData")
        params = protocol.get("parameters") if isinstance(protocol, dict) else {}
        start = None
        end = None
        if isinstance(params, dict):
            try:
                start = float(params.get("startTime")) if params.get("startTime") is not None else None
            except (TypeError, ValueError):
                pass
            try:
                end = float(params.get("endTime")) if params.get("endTime") is not None else None
            except (TypeError, ValueError):
                pass
        for key in ("start_time", "startTime"):
            if start is None and row.get(key) is not None:
                try:
                    start = float(row[key])
                except (TypeError, ValueError):
                    pass
        for key in ("end_time", "endTime"):
            if end is None and row.get(key) is not None:
                try:
                    end = float(row[key])
                except (TypeError, ValueError):
                    pass
        return {
            "slug": slug,
            "chain": chain,
            "wallet_name": wallet_name,
            "wallet_address": wallet_address,
            "order_hash": order_hash,
            "protocol_address": _protocol_address(row),
            "currency_symbol": symbol or "WETH",
            "unit_price_weth": str(unit) if unit is not None and (symbol or "").upper() in {"WETH", "ETH"} else None,
            "quantity": qty,
            "total_weth": str(total) if total is not None and (symbol or "").upper() in {"WETH", "ETH"} else None,
            "start_time": start,
            "end_time": end,
            "status": "active",
            "response_json": json.dumps(row, ensure_ascii=False)[:50000],
        }


class OfferControllerMixin:
    """Telegram controller mixin.  Requires the existing Bot attributes/methods."""

    OFFER_SESSION_TTL = 20 * 60

    def init_offer_subsystem(self) -> None:
        workers = max(1, min(int(os.getenv("OFFER_API_WORKERS", "2")), 4))
        sign_workers = max(1, min(int(os.getenv("OFFER_SIGN_WORKERS", "1")), 2))
        self.offer_api_executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="offer-api")
        self.offer_sign_executor = ThreadPoolExecutor(max_workers=sign_workers, thread_name_prefix="offer-sign")
        self.offer_lock = threading.RLock()
        self.offer_sessions: dict[str, dict[str, Any]] = {}
        self.offer_current_session: dict[str, str] = {}
        self.pending_offer_custom_price: dict[str, dict[str, Any]] = {}
        self.pending_offer_quantity: dict[str, dict[str, Any]] = {}
        self.pending_offer_setting: dict[str, dict[str, Any]] = {}
        try:
            self.offer_top_increment_usdt = max(Decimal("0"), Decimal(str(self.store.get_setting("offer_top_increment_usdt", "0.05"))))
        except Exception:
            self.offer_top_increment_usdt = Decimal("0.05")
        try:
            self.offer_duration_hours = max(1, min(int(self.store.get_setting("offer_duration_hours", "24")), 24 * 30))
        except Exception:
            self.offer_duration_hours = 24
        self.offer_service = CollectionOfferService(
            self.opensea,
            self.store,
            self.rpc_pools,
            self.price_oracle,
            scoped_token=os.getenv("OPENSEA_SCOPED_TOKEN", "").strip(),
        )
        log.info(
            "Offer subsystem V4.12.2 ready | api-workers=%s | sign-workers=%s | top_increment=%s USDT | duration=%sh | loop-hooks=0",
            workers, sign_workers, self.offer_top_increment_usdt, self.offer_duration_hours,
        )

    def offer_clear_pending_inputs(self, chat_id: str) -> None:
        self.pending_offer_custom_price.pop(chat_id, None)
        self.pending_offer_quantity.pop(chat_id, None)
        self.pending_offer_setting.pop(chat_id, None)

    def _offer_session(self, sid: str) -> dict[str, Any] | None:
        with self.offer_lock:
            session = self.offer_sessions.get(sid)
            if not session:
                return None
            if time.time() > float(session.get("expiry", 0)):
                self.offer_sessions.pop(sid, None)
                return None
            return session

    def _new_offer_session(self, chat_id: str, *, slug: str, chain: str, candidate_token: str = "", source_offer_id: int | None = None) -> tuple[str, dict[str, Any]]:
        seed = f"{chat_id}|{slug}|{chain}|{time.time_ns()}|{secrets.token_hex(4)}"
        sid = hashlib.sha1(seed.encode()).hexdigest()[:10]
        session = {
            "id": sid,
            "chat_id": str(chat_id),
            "slug": str(slug),
            "chain": normalize_chain(chain),
            "candidate_token": candidate_token,
            "source_offer_id": source_offer_id,
            "mode": "",
            "unit_usdt": None,
            "top_snapshot_usdt": None,
            "market": None,
            "selected": {},  # wallet_id -> quantity
            "duration_hours": int(self.offer_duration_hours),
            "created_at": time.time(),
            "expiry": time.time() + self.OFFER_SESSION_TTL,
        }
        with self.offer_lock:
            self.offer_sessions[sid] = session
            self.offer_current_session[str(chat_id)] = sid
        return sid, session

    @staticmethod
    def _offer_chain_label(chain: str) -> str:
        return {
            "ethereum": "Ethereum",
            "ink": "Ink",
            "robinhood": "Robinhood Chain",
            "base": "Base",
            "arbitrum": "Arbitrum",
            "optimism": "Optimism",
            "polygon": "Polygon",
        }.get(normalize_chain(chain), chain)

    def _offer_active_wallets(self, chain: str) -> list[Any]:
        rows = []
        enabled = self.store.list_wallets(enabled_only=True)
        configs = {w.address.lower(): w for w in self.wallets if w.supports_chain(chain)}
        for stored in enabled:
            if stored.address.lower() in configs:
                rows.append(stored)
        return rows

    def _offer_wallet_config(self, stored: Any) -> WalletConfig:
        return WalletConfig(
            name=stored.name,
            private_key=stored.private_key,
            address=Web3.to_checksum_address(stored.address),
            quantity=stored.quantity,
            chains=tuple(normalize_chain(x) for x in stored.chains),
        )

    def _offer_submit(self, chat_id: str, fn, *args) -> None:
        """Submit read-only/background Offer work; never shares a Race executor."""
        try:
            self.offer_api_executor.submit(fn, *args)
        except Exception as exc:
            self.telegram.send(chat_id, f"⚠️ تعذر تشغيل مهمة Offers: {exc}")

    def _offer_submit_financial(self, chat_id: str, fn, *args) -> None:
        """Submit signing/financial work to a second executor isolated from reads and Race."""
        try:
            self.offer_sign_executor.submit(fn, *args)
        except Exception as exc:
            self.telegram.send(chat_id, f"⚠️ تعذر تشغيل مهمة توقيع Offers: {exc}")

    def _offer_market_text(self, session: dict[str, Any]) -> str:
        market = session.get("market") or {}
        offers = list(market.get("offers") or [])
        lines = [
            "💰 Collection Offer",
            "",
            f"📦 المشروع: {session['slug']}",
            f"🌐 الشبكة: {self._offer_chain_label(session['chain'])}",
            "💵 الإدخال والعرض في Telegram: USDT",
            "Ξ التنفيذ الفعلي على OpenSea: WETH",
            "",
            "📊 أفضل Collection Offers الحالية:",
        ]
        if not offers:
            lines.append("لا توجد عروض قابلة للتحويل إلى USDT حاليًا.")
        else:
            medals = ["🥇", "🥈", "🥉"]
            for idx, item in enumerate(offers[:3]):
                lines.append(
                    f"{medals[idx]} {_fmt_decimal(item['unit_usdt'], 4)} USDT/NFT"
                    f" | {_fmt_decimal(item['unit_token'], 8)} {item['symbol']}"
                )
        if session.get("unit_usdt") is not None:
            mode = "Top Offer" if session.get("mode") == "top" else "مخصص/سعر حالي"
            lines.extend(["", f"🎯 عرضك الحالي: {_fmt_decimal(session['unit_usdt'], 4)} USDT لكل NFT ({mode})"])
        lines.extend([
            "",
            f"🏆 زيادة Top Offer: {_fmt_decimal(self.offer_top_increment_usdt, 4)} USDT",
            f"⏳ المدة: {session.get('duration_hours', self.offer_duration_hours)} ساعة",
            "",
            "لن يتم توقيع أو إرسال أي Offer قبل شاشة المراجعة والتأكيد النهائي.",
        ])
        return "\n".join(lines)[:3900]

    def _offer_market_buttons(self, session: dict[str, Any]) -> list[list[tuple[str, str]]]:
        sid = session["id"]
        rows: list[list[tuple[str, str]]] = [[("🏆 Top Offer", f"oft:{sid}"), ("✍️ عرض مخصص", f"ofcustom:{sid}")]]
        market = session.get("market") or {}
        offers = list(market.get("offers") or [])
        if offers:
            button_row = []
            for idx, item in enumerate(offers[:3]):
                button_row.append((f"{_fmt_decimal(item['unit_usdt'], 2)} USDT", f"ofm:{sid}:{idx}"))
            rows.append(button_row)
        if session.get("unit_usdt") is not None:
            rows.extend(self._offer_wallet_buttons(session))
        rows.append([("🔄 تحديث العروض", f"ofrefresh:{sid}"), ("📨 عروضي", "offers_mine")])
        rows.append([("❌ إلغاء", f"ofclose:{sid}")])
        return rows

    def _offer_wallet_buttons(self, session: dict[str, Any]) -> list[list[tuple[str, str]]]:
        sid = session["id"]
        selected = session.get("selected") or {}
        rows: list[list[tuple[str, str]]] = []
        for stored in self._offer_active_wallets(session["chain"])[:20]:
            key = str(stored.id)
            picked = key in selected
            qty = int(selected.get(key, 1))
            rows.append([
                (("✅ " if picked else "⬜ ") + stored.name, f"ofw:{sid}:{stored.id}"),
                (f"🔢 {qty}", f"ofq:{sid}:{stored.id}"),
            ])
        rows.append([("✅ تحديد الكل", f"ofa:{sid}"), ("🧹 مسح", f"ofx:{sid}")])
        rows.append([("➡️ مراجعة العرض", f"ofr:{sid}")])
        return rows

    def _offer_load_market_worker(self, chat_id: str, sid: str, message_id: int = 0) -> None:
        session = self._offer_session(sid)
        if not session:
            return
        try:
            market = self.offer_service.market_snapshot(session["slug"], session["chain"])
            with self.offer_lock:
                session["market"] = market
                session["expiry"] = time.time() + self.OFFER_SESSION_TTL
            text = self._offer_market_text(session)
            buttons = self._offer_market_buttons(session)
            if message_id:
                self.telegram.edit(chat_id, message_id, text, buttons)
            else:
                self.telegram.send(chat_id, text, buttons)
        except Exception as exc:
            self.telegram.send(chat_id, f"⚠️ تعذر جلب Collection Offers من OpenSea.\n{str(exc)[:900]}")

    def _offer_start_from_candidate(self, event: dict[str, Any], candidate: Any) -> None:
        chat_id = event["chat_id"]
        if normalize_chain(candidate.chain) not in WETH_BY_CHAIN:
            self.telegram.send(chat_id, f"⚠️ عروض WETH غير مهيأة لهذه الشبكة: {candidate.chain}")
            return
        sid, _ = self._new_offer_session(
            chat_id,
            slug=candidate.slug,
            chain=candidate.chain,
            candidate_token=self.candidate_token(candidate),
        )
        self.edit_or_send(event, "⏳ جارٍ جلب أفضل Collection Offers الحالية من OpenSea…", [[("❌ إلغاء", f"ofclose:{sid}")]])
        self._offer_submit(chat_id, self._offer_load_market_worker, chat_id, sid, int(event.get("message_id", 0) or 0))

    def _offer_start_raise(self, event: dict[str, Any], row: dict[str, Any]) -> None:
        chat_id = event["chat_id"]
        sid, session = self._new_offer_session(
            chat_id,
            slug=str(row["slug"]),
            chain=str(row["chain"]),
            source_offer_id=int(row["id"]),
        )
        wallet = self.store.get_wallet_by_address(str(row["wallet_address"]))
        if wallet and wallet.enabled:
            session["selected"] = {str(wallet.id): max(1, min(int(row.get("quantity") or 1), 100))}
        self.edit_or_send(event, "⏳ جارٍ تحديث Top Offer قبل إعداد الرفع…", [[("❌ إلغاء", f"ofclose:{sid}")]])
        self._offer_submit(chat_id, self._offer_load_market_worker, chat_id, sid, int(event.get("message_id", 0) or 0))

    def _offer_set_top_worker(self, chat_id: str, sid: str, message_id: int = 0) -> None:
        session = self._offer_session(sid)
        if not session:
            return
        try:
            market = self.offer_service.market_snapshot(session["slug"], session["chain"])
            top = Decimal(str(market.get("top_usdt") or "0"))
            proposed = top + self.offer_top_increment_usdt
            if proposed <= 0:
                proposed = max(Decimal("0.01"), self.offer_top_increment_usdt)
            with self.offer_lock:
                session["market"] = market
                session["mode"] = "top"
                session["top_snapshot_usdt"] = top
                session["unit_usdt"] = proposed
                session["expiry"] = time.time() + self.OFFER_SESSION_TTL
            prefix = (
                f"🏆 Top Offer الحالي: {_fmt_decimal(top, 4)} USDT\n"
                f"➕ الزيادة: {_fmt_decimal(self.offer_top_increment_usdt, 4)} USDT\n"
                f"🎯 عرضك: {_fmt_decimal(proposed, 4)} USDT لكل NFT\n\n"
            )
            text = prefix + self._offer_market_text(session)
            buttons = self._offer_market_buttons(session)
            if message_id:
                self.telegram.edit(chat_id, message_id, text[:3900], buttons)
            else:
                self.telegram.send(chat_id, text[:3900], buttons)
        except Exception as exc:
            self.telegram.send(chat_id, f"⚠️ تعذر تحديث Top Offer: {str(exc)[:800]}")

    def _offer_review_worker(self, chat_id: str, sid: str, message_id: int = 0, *, final_guard: bool = False) -> None:
        session = self._offer_session(sid)
        if not session:
            return
        selected = dict(session.get("selected") or {})
        unit_usdt = session.get("unit_usdt")
        if not selected or unit_usdt is None:
            self.telegram.send(chat_id, "⚠️ اختر السعر ومحفظة واحدة على الأقل أولًا.")
            return
        try:
            unit_usdt = Decimal(str(unit_usdt))
            # Financial guard: Top Offer is re-read immediately before the final
            # signature.  Any price change updates the proposal and requires a new
            # explicit confirmation rather than silently increasing spend.
            if final_guard and session.get("mode") == "top":
                market = self.offer_service.market_snapshot(session["slug"], session["chain"])
                top_now = Decimal(str(market.get("top_usdt") or "0"))
                proposed_now = top_now + self.offer_top_increment_usdt
                if proposed_now <= 0:
                    proposed_now = max(Decimal("0.01"), self.offer_top_increment_usdt)
                if abs(proposed_now - unit_usdt) >= Decimal("0.0001"):
                    with self.offer_lock:
                        session["market"] = market
                        session["top_snapshot_usdt"] = top_now
                        session["unit_usdt"] = proposed_now
                    self.telegram.send(
                        chat_id,
                        "⚠️ تغير Top Offer منذ آخر مراجعة، لذلك لم يتم التوقيع.\n\n"
                        f"السعر السابق: {_fmt_decimal(unit_usdt, 4)} USDT\n"
                        f"Top الحالي: {_fmt_decimal(top_now, 4)} USDT\n"
                        f"عرضك الجديد: {_fmt_decimal(proposed_now, 4)} USDT\n\n"
                        "راجع الأرصدة والـApproval ثم أكد مرة أخرى.",
                        [[("🔎 مراجعة بالسعر الجديد", f"ofr:{sid}"), ("❌ إلغاء", f"ofclose:{sid}")]],
                    )
                    return

            eth_usd = self.offer_service.eth_usd()
            if eth_usd is None or eth_usd <= 0:
                self.telegram.send(chat_id, "⚠️ تعذر جلب ETH/USDT، ولن يتم حساب أو توقيع Offer بدون سعر تحويل حديث.")
                return
            checks: list[tuple[Any, int, int, Decimal, Decimal, Decimal, OfferFundingCheck]] = []
            for wallet_id, qty_raw in selected.items():
                stored = self.store.get_wallet_by_id(int(wallet_id))
                if not stored or not stored.enabled:
                    continue
                qty = max(1, min(int(qty_raw), 100))
                _bid_wei, _fee_wei, required_wei, bid_weth, fee_weth, required_weth = self.offer_service.offer_commitment(
                    slug=session["slug"], chain=session["chain"], unit_usdt=unit_usdt, quantity=qty, eth_usd=eth_usd
                )
                check = self.offer_service.funding_check(session["chain"], self._offer_wallet_config(stored), required_wei)
                checks.append((stored, qty, required_wei, bid_weth, fee_weth, required_weth, check))
            if not checks:
                self.telegram.send(chat_id, "⚠️ لا توجد محافظ نشطة صالحة ضمن الاختيار.")
                return

            lines = [
                "🔎 مراجعة Collection Offer",
                "",
                f"📦 المشروع: {session['slug']}",
                f"🌐 الشبكة: {self._offer_chain_label(session['chain'])}",
                f"💵 العرض لكل NFT: {_fmt_decimal(unit_usdt, 4)} USDT",
                f"Ξ القيمة الفعلية لكل NFT: ≈ {_fmt_decimal(unit_usdt / eth_usd, 8)} WETH",
                f"📈 1 ETH/WETH: ≈ {_fmt_decimal(eth_usd, 2)} USDT",
                f"⏳ المدة: {session.get('duration_hours', self.offer_duration_hours)} ساعة",
                "",
            ]
            all_ready = True
            approval_buttons: list[list[tuple[str, str]]] = []
            for stored, qty, _required_wei, bid_weth, fee_weth, required_weth, check in checks:
                total_usdt = unit_usdt * Decimal(qty)
                fee_usdt = fee_weth * eth_usd
                commitment_usdt = required_weth * eth_usd
                b = "✅" if check.balance_ok else "⚠️"
                a = "✅" if check.approval_ok else "⚠️"
                lines.extend([
                    f"👛 {stored.name}",
                    f"🔢 الكمية: {qty} | قيمة العرض: {_fmt_decimal(total_usdt, 4)} USDT",
                    f"Ξ قيمة Offer: ≈ {_fmt_decimal(bid_weth, 8)} WETH",
                    f"🧾 الرسوم المطلوبة: ≈ {_fmt_decimal(fee_weth, 8)} WETH (≈ {_fmt_decimal(fee_usdt, 4)} USDT)",
                    f"🛡 إجمالي WETH المطلوب/Approval: ≈ {_fmt_decimal(required_weth, 8)} WETH (≈ {_fmt_decimal(commitment_usdt, 4)} USDT)",
                    f"{b} WETH: {_fmt_decimal(check.weth_balance, 8)} (≈ {_fmt_decimal(check.weth_balance * eth_usd, 4)} USDT)",
                    f"{a} Approval: {_fmt_decimal(check.allowance, 8)} WETH (≈ {_fmt_decimal(check.allowance * eth_usd, 4)} USDT)",
                    "",
                ])
                if not check.balance_ok or not check.approval_ok:
                    all_ready = False
                if check.balance_ok and not check.approval_ok:
                    approval_buttons.append([(f"🔓 Approval — {stored.name}", f"ofapprove:{sid}:{stored.id}")])

            buttons = approval_buttons
            if all_ready:
                buttons.append([("✅ تأكيد تقديم العرض", f"ofconfirm:{sid}")])
            buttons.append([("⬅️ تعديل", f"ofback:{sid}"), ("❌ إلغاء", f"ofclose:{sid}")])
            lines.append("ℹ️ الـOffer لا يسحب WETH الآن؛ يتم الالتزام به كأمر Seaport ويمكن للبائع قبوله لاحقًا.")
            text = "\n".join(lines)[:3900]
            if message_id:
                self.telegram.edit(chat_id, message_id, text, buttons)
            else:
                self.telegram.send(chat_id, text, buttons)
        except Exception as exc:
            self.telegram.send(chat_id, f"⚠️ فشل فحص العرض: {str(exc)[:1000]}")

    def _offer_approval_worker(self, chat_id: str, sid: str, wallet_id: int) -> None:
        session = self._offer_session(sid)
        if not session:
            return
        stored = self.store.get_wallet_by_id(wallet_id)
        qty = int((session.get("selected") or {}).get(str(wallet_id), 0) or 0)
        if not stored or not stored.enabled or qty <= 0 or session.get("unit_usdt") is None:
            self.telegram.send(chat_id, "⚠️ المحفظة أو جلسة العرض لم تعد صالحة.")
            return
        try:
            unit = Decimal(str(session["unit_usdt"]))
            eth_usd = self.offer_service.eth_usd()
            if eth_usd is None:
                self.telegram.send(chat_id, "⚠️ تعذر جلب سعر ETH/USDT.")
                return
            _bid_wei, _fee_wei, required_wei, _bid_weth, _fee_weth, required_weth = self.offer_service.offer_commitment(
                slug=session["slug"], chain=session["chain"], unit_usdt=unit, quantity=qty, eth_usd=eth_usd
            )
            wallet = self._offer_wallet_config(stored)
            check = self.offer_service.funding_check(session["chain"], wallet, required_wei)
            if not check.balance_ok:
                self.telegram.send(
                    chat_id,
                    "⚠️ WETH غير كافٍ. "
                    f"الموجود {_fmt_decimal(check.weth_balance, 8)} WETH (≈ {_fmt_decimal(check.weth_balance * eth_usd, 4)} USDT)، "
                    f"المطلوب {_fmt_decimal(required_weth, 8)} WETH (≈ {_fmt_decimal(required_weth * eth_usd, 4)} USDT)."
                )
                return
            if check.approval_ok:
                self.telegram.send(chat_id, "✅ الـApproval موجود بالفعل.", [[("🔎 إعادة المراجعة", f"ofr:{sid}")]])
                return
            result = self.offer_service.approve_exact(
                session["chain"],
                wallet,
                required_wei,
                gas_strategy=self.gas_strategy,
                gas_limit_buffer=self.gas_limit_buffer,
                max_gas_native=self.max_gas_for_chain(session["chain"]),
                max_gas_usd=self.max_gas_usd_for_chain(session["chain"]),
                native_usd_price=self.native_usd_price_for_chain(session["chain"]),
            )
            if result.ok:
                url = str(CHAIN_CONFIGS.get(session["chain"], {}).get("explorer", "")) + str(result.tx_hash or "")
                self.telegram.send(
                    chat_id,
                    "🔓 تم إرسال معاملة WETH Approval\n\n"
                    f"👛 {stored.name}\n"
                    f"Ξ المبلغ الموافق عليه: {_fmt_decimal(required_weth, 8)} WETH (≈ {_fmt_decimal(required_weth * eth_usd, 4)} USDT)\n"
                    + (
                        f"⛽ أقصى تقدير: {_fmt_decimal(result.gas_cost_native, 8)} {native_symbol(session['chain'])}"
                        + (f" (≈ {_fmt_decimal(result.gas_cost_usd, 4)} USDT)" if result.gas_cost_usd is not None else "")
                        + "\n"
                        if result.gas_cost_native is not None
                        else (f"⛽ أقصى تقدير: ≈ {_fmt_decimal(result.gas_cost_usd, 4)} USDT\n" if result.gas_cost_usd is not None else "")
                    )
                    + f"🔎 {url}\n\n"
                    "بعد تأكيد المعاملة على الشبكة اضغط إعادة المراجعة.",
                    [[("🔎 إعادة المراجعة", f"ofr:{sid}"), ("❌ إلغاء", f"ofclose:{sid}")]],
                )
            else:
                self.telegram.send(chat_id, f"⚠️ لم يتم إرسال Approval: {result.status}\n{str(result.detail or '')[:700]}")
        except Exception as exc:
            self.telegram.send(chat_id, f"⚠️ خطأ Approval: {str(exc)[:800]}")

    def _offer_confirm_worker(self, chat_id: str, sid: str) -> None:
        session = self._offer_session(sid)
        if not session:
            return
        # Run the final Top guard/review logic inline first.  For custom prices it
        # reaches creation immediately after a fresh funding check below.
        if session.get("mode") == "top":
            try:
                market = self.offer_service.market_snapshot(session["slug"], session["chain"])
                top_now = Decimal(str(market.get("top_usdt") or "0"))
                proposed_now = top_now + self.offer_top_increment_usdt
                if proposed_now <= 0:
                    proposed_now = max(Decimal("0.01"), self.offer_top_increment_usdt)
                current = Decimal(str(session.get("unit_usdt") or "0"))
                if abs(proposed_now - current) >= Decimal("0.0001"):
                    with self.offer_lock:
                        session["market"] = market
                        session["top_snapshot_usdt"] = top_now
                        session["unit_usdt"] = proposed_now
                    self.telegram.send(
                        chat_id,
                        "⚠️ تغير Top Offer قبل التوقيع. لم أوقع أي Order.\n\n"
                        f"Top الحالي: {_fmt_decimal(top_now, 4)} USDT\n"
                        f"عرضك الجديد: {_fmt_decimal(proposed_now, 4)} USDT/NFT\n\n"
                        "اضغط المراجعة ثم أكد السعر الجديد.",
                        [[("🔎 مراجعة بالسعر الجديد", f"ofr:{sid}"), ("❌ إلغاء", f"ofclose:{sid}")]],
                    )
                    return
            except Exception as exc:
                self.telegram.send(chat_id, f"⚠️ تعذر إعادة فحص Top Offer، لذلك أوقفت التوقيع: {str(exc)[:700]}")
                return

        selected = dict(session.get("selected") or {})
        if not selected or session.get("unit_usdt") is None:
            self.telegram.send(chat_id, "⚠️ الجلسة ناقصة؛ أعد إعداد العرض.")
            return
        unit_usdt = Decimal(str(session["unit_usdt"]))
        eth_usd = self.offer_service.eth_usd()
        if eth_usd is None or eth_usd <= 0:
            self.telegram.send(chat_id, "⚠️ سعر ETH/USDT غير متاح؛ لم يتم التوقيع.")
            return

        # Final all-wallet funding guard before any destructive "raise" action.
        # This prevents cancelling an existing offer only to discover that one
        # of the replacement wallets lost balance/allowance after review.
        final_preflight_errors: list[str] = []
        for wallet_id, qty_raw in selected.items():
            stored_check = self.store.get_wallet_by_id(int(wallet_id))
            if not stored_check or not stored_check.enabled:
                final_preflight_errors.append(f"wallet-{wallet_id}: المحفظة غير متاحة")
                continue
            qty_check = max(1, min(int(qty_raw), 100))
            _bid_wei, _fee_wei, required_wei, _bid_weth, _fee_weth, required_weth = self.offer_service.offer_commitment(
                slug=session["slug"], chain=session["chain"], unit_usdt=unit_usdt, quantity=qty_check, eth_usd=eth_usd
            )
            funding = self.offer_service.funding_check(
                session["chain"], self._offer_wallet_config(stored_check), required_wei
            )
            if not funding.balance_ok:
                final_preflight_errors.append(
                    f"{stored_check.name}: WETH غير كافٍ — "
                    f"{_fmt_decimal(funding.weth_balance, 8)} WETH (≈ {_fmt_decimal(funding.weth_balance * eth_usd, 4)} USDT) "
                    f"< {_fmt_decimal(required_weth, 8)} WETH (≈ {_fmt_decimal(required_weth * eth_usd, 4)} USDT)"
                )
            elif not funding.approval_ok:
                final_preflight_errors.append(
                    f"{stored_check.name}: Approval غير كافٍ — "
                    f"{_fmt_decimal(funding.allowance, 8)} WETH (≈ {_fmt_decimal(funding.allowance * eth_usd, 4)} USDT) "
                    f"< {_fmt_decimal(required_weth, 8)} WETH (≈ {_fmt_decimal(required_weth * eth_usd, 4)} USDT)"
                )
        if final_preflight_errors:
            self.telegram.send(
                chat_id,
                "⚠️ تغيرت جاهزية إحدى المحافظ قبل التوقيع، لذلك لم أوقع ولم ألغِ أي عرض قديم.\n\n"
                + "\n".join(final_preflight_errors[:8])
                + "\n\nأعد المراجعة ثم أكد مرة أخرى.",
                [[("🔎 إعادة المراجعة", f"ofr:{sid}"), ("❌ إلغاء", f"ofclose:{sid}")]],
            )
            return

        source_offer_id = session.get("source_offer_id")
        if source_offer_id:
            old = self.store.get_offer(int(source_offer_id))
            if old and str(old.get("status")) == "active":
                stored_old = self.store.get_wallet_by_address(str(old.get("wallet_address") or ""))
                if not stored_old:
                    self.telegram.send(chat_id, "⚠️ لا أستطيع رفع العرض لأن محفظة العرض القديم لم تعد موجودة.")
                    return
                try:
                    self.offer_service.cancel_offer(
                        chain=str(old["chain"]), wallet=self._offer_wallet_config(stored_old),
                        protocol_address=str(old.get("protocol_address") or SEAPORT_1_6),
                        order_hash=str(old["order_hash"]),
                    )
                    self.store.set_offer_status(int(old["id"]), "cancelled", detail="Cancelled before raise")
                except Exception as exc:
                    self.telegram.send(chat_id, f"⚠️ تعذر إلغاء العرض القديم، لذلك لم أرسل العرض الجديد لتجنب وجود التزامين.\n{str(exc)[:800]}")
                    return

        successes = []
        failures = []
        for wallet_id, qty_raw in selected.items():
            stored = self.store.get_wallet_by_id(int(wallet_id))
            if not stored or not stored.enabled:
                failures.append((f"wallet-{wallet_id}", "المحفظة غير متاحة"))
                continue
            qty = max(1, min(int(qty_raw), 100))
            wallet = self._offer_wallet_config(stored)
            result = self.offer_service.create_collection_offer(
                chain=session["chain"], wallet=wallet, slug=session["slug"], quantity=qty,
                unit_usdt=unit_usdt, duration_hours=int(session.get("duration_hours") or self.offer_duration_hours),
                eth_usd=eth_usd,
            )
            if result.ok and result.order_hash:
                response = result.response or {}
                protocol = _protocol_address(response)
                _params = (result.protocol_data or {}).get("parameters") or {}
                try:
                    _start_time = float(_params.get("startTime"))
                except (TypeError, ValueError):
                    _start_time = time.time()
                try:
                    _end_time = float(_params.get("endTime"))
                except (TypeError, ValueError):
                    _end_time = time.time() + int(session.get("duration_hours") or self.offer_duration_hours) * 3600
                offer_id = self.store.record_offer(
                    slug=session["slug"], chain=session["chain"], wallet_name=stored.name,
                    wallet_address=stored.address, order_hash=result.order_hash,
                    protocol_address=protocol, currency_address=self.offer_service.weth_address(session["chain"]),
                    currency_symbol="WETH", unit_price_usdt=str(unit_usdt),
                    unit_price_weth=str((result.total_weth or Decimal("0")) / Decimal(qty)),
                    quantity=qty, total_weth=str(result.total_weth or "0"),
                    start_time=_start_time, end_time=_end_time,
                    status="active", protocol_data_json=json.dumps(result.protocol_data or {}, ensure_ascii=False),
                    response_json=json.dumps(response, ensure_ascii=False)[:50000], detail="created by V4.12.2 offer subsystem",
                )
                successes.append((stored, qty, result, offer_id))
            else:
                failures.append((stored.name, f"{result.status}: {result.detail or ''}"))

        lines = ["📨 نتيجة تقديم Collection Offer", "", f"📦 {session['slug']}"]
        for stored, qty, result, _offer_id in successes:
            lines.extend([
                f"✅ {stored.name} ×{qty}",
                f"💵 {_fmt_decimal(unit_usdt, 4)} USDT/NFT",
                f"Ξ الإجمالي ≈ {_fmt_decimal(result.total_weth, 8)} WETH (≈ {_fmt_decimal(unit_usdt * Decimal(qty), 4)} USDT)",
                f"🧾 {result.order_hash}",
                "",
            ])
        for name, detail in failures:
            lines.extend([f"❌ {name}: {detail[:500]}", ""])
        if successes:
            lines.append("الـOffer أصبح Order موقّعًا ومُرسلًا إلى OpenSea؛ لا يتم خصم WETH إلا عند تنفيذ/قبول العرض.")
        buttons = [[("📨 عروضي الحالية", "offers_mine"), ("🏠 الرئيسية", "menu")]]
        self.telegram.send(chat_id, "\n".join(lines)[:3900], buttons)
        with self.offer_lock:
            self.offer_sessions.pop(sid, None)
            if self.offer_current_session.get(str(chat_id)) == sid:
                self.offer_current_session.pop(str(chat_id), None)

    def _offers_mine_worker(self, chat_id: str, message_id: int = 0) -> None:
        rows_out: list[dict[str, Any]] = []
        errors: list[str] = []
        eth_usd = self.offer_service.eth_usd()
        for stored in self.store.list_wallets(enabled_only=True):
            try:
                remote = self.offer_service.account_active_offers(stored.address, chains=list(stored.chains) or list(self.enabled_chains), limit=50)
                for item in remote:
                    rec = self.offer_service.remote_offer_to_record(item, stored.name, stored.address)
                    if not rec:
                        continue
                    if rec.get("unit_price_weth") and eth_usd is not None:
                        rec["unit_price_usdt"] = str(_as_decimal(rec["unit_price_weth"]) * eth_usd)
                    rec_id = self.store.upsert_remote_offer(**rec)
                    saved = self.store.get_offer(rec_id)
                    if saved:
                        rows_out.append(saved)
            except Exception as exc:
                errors.append(f"{stored.name}: {str(exc)[:120]}")
                # A 429/cooldown is intentionally not retried here; offer reads
                # use the background quota class so mint-time REST keeps priority.
                if "429" in str(exc):
                    break
        # Include locally-created active rows even if profile pagination/rate-limit
        # prevented them from appearing in this refresh.
        seen_ids = {int(r["id"]) for r in rows_out if r.get("id") is not None}
        for row in self.store.list_offers(status="active", limit=100):
            end_ts = float(row.get("end_time") or 0)
            if end_ts and end_ts <= time.time():
                self.store.set_offer_status(int(row["id"]), "expired", detail="local expiry reached")
                continue
            if int(row["id"]) not in seen_ids:
                rows_out.append(row)
        rows_out.sort(key=lambda r: float(r.get("created_at") or 0), reverse=True)
        lines = ["📨 عروضي الحالية", ""]
        buttons: list[list[tuple[str, str]]] = []
        if not rows_out:
            lines.append("لا توجد Collection Offers نشطة معروفة حاليًا.")
        for row in rows_out[:25]:
            unit = row.get("unit_price_usdt")
            qty = int(row.get("quantity") or 1)
            end = float(row.get("end_time") or 0)
            remain = max(0, int(end - time.time())) if end else 0
            remain_text = f"{remain // 3600}س {remain % 3600 // 60}د" if remain else "غير معروف"
            lines.extend([
                f"📦 {row.get('slug')}",
                f"👛 {row.get('wallet_name')}",
                f"💵 {(_fmt_decimal(_as_decimal(unit), 4) + ' USDT/NFT') if unit else 'السعر USDT غير محفوظ'} | ×{qty}",
                f"⏳ المتبقي: {remain_text}",
                "",
            ])
            buttons.append([(f"📦 {str(row.get('slug'))[:28]} — {str(row.get('wallet_name'))[:18]}", f"ofdetail:{int(row['id'])}")])
        if errors:
            lines.append("⚠️ تعذر تحديث بعض المحافظ: " + " | ".join(errors[:3]))
        buttons.append([("🔄 تحديث", "offers_mine"), ("🏠 الرئيسية", "menu")])
        text = "\n".join(lines)[:3900]
        if message_id:
            self.telegram.edit(chat_id, message_id, text, buttons)
        else:
            self.telegram.send(chat_id, text, buttons)

    def _offer_detail_text(self, row: dict[str, Any]) -> str:
        unit = row.get("unit_price_usdt")
        qty = int(row.get("quantity") or 1)
        total = row.get("total_weth")
        return (
            "📨 تفاصيل Offer\n\n"
            f"📦 المشروع: {row.get('slug')}\n"
            f"🌐 الشبكة: {self._offer_chain_label(str(row.get('chain') or ''))}\n"
            f"👛 المحفظة: {row.get('wallet_name')}\n"
            f"💵 السعر: {(_fmt_decimal(_as_decimal(unit), 4) + ' USDT/NFT') if unit else 'غير محفوظ'}\n"
            f"🔢 الكمية: {qty}\n"
            f"Ξ WETH الإجمالي: {total or 'غير محفوظ'}"
            + (f" (≈ {_fmt_decimal(_as_decimal(unit) * Decimal(qty), 4)} USDT)\n" if unit else "\n")
            + f"📌 الحالة: {row.get('status')}\n"
            f"🧾 Order Hash:\n{row.get('order_hash')}"
        )[:3900]

    def _offer_cancel_worker(self, chat_id: str, offer_id: int) -> None:
        row = self.store.get_offer(offer_id)
        if not row:
            self.telegram.send(chat_id, "⚠️ لم يتم العثور على العرض.")
            return
        stored = self.store.get_wallet_by_address(str(row.get("wallet_address") or ""))
        if not stored:
            self.telegram.send(chat_id, "⚠️ محفظة هذا العرض لم تعد موجودة في البوت، لذلك لا أستطيع توقيع الإلغاء.")
            return
        try:
            self.offer_service.cancel_offer(
                chain=str(row.get("chain") or "ethereum"), wallet=self._offer_wallet_config(stored),
                protocol_address=str(row.get("protocol_address") or SEAPORT_1_6), order_hash=str(row.get("order_hash") or ""),
            )
            self.store.set_offer_status(offer_id, "cancelled", detail="offchain SignedZone cancellation")
            self.telegram.send(chat_id, "✅ تم إرسال إلغاء العرض إلى OpenSea (Off-chain / SignedZone).", [[("📨 عروضي", "offers_mine")]])
        except Exception as exc:
            self.store.set_offer_status(offer_id, str(row.get("status") or "active"), detail=f"cancel failed: {exc}")
            self.telegram.send(chat_id, f"⚠️ تعذر إلغاء العرض عبر OpenSea.\n{str(exc)[:1000]}", [[("📨 عروضي", "offers_mine")]])

    def offer_qualification_buttons(self) -> list[list[tuple[str, str]]]:
        rows: list[list[tuple[str, str]]] = []
        for candidate in self.candidates.values():
            if candidate.qualification_tracked:
                rows.append([(f"💰 {candidate.slug}", f"ofp:{self.candidate_token(candidate)}")])
                if len(rows) >= 35:
                    break
        rows.append([("📨 عروضي الحالية", "offers_mine"), ("↩️ التأهيل", "qualification_menu")])
        return rows

    def offer_settings_text(self) -> str:
        return (
            "💰 إعدادات Collection Offers\n\n"
            f"🏆 زيادة Top Offer: {_fmt_decimal(self.offer_top_increment_usdt, 4)} USDT\n"
            f"⏳ مدة العرض الافتراضية: {self.offer_duration_hours} ساعة\n"
            "Ξ عملة التنفيذ: WETH\n"
            "💵 واجهة الإدخال: USDT\n\n"
            "هذه الإعدادات تخص Offers فقط ولا تدخل في Race Lane أو Mint Scheduler."
        )

    def offer_settings_buttons(self) -> list[list[tuple[str, str]]]:
        return [
            [(f"🏆 زيادة Top: {_fmt_decimal(self.offer_top_increment_usdt, 4)}", "ofincmenu")],
            [(f"⏳ المدة: {self.offer_duration_hours}h", "ofdurmenu")],
            [("↩️ الإعدادات", "settings")],
        ]

    def handle_offer_callback(self, event: dict[str, Any]) -> bool:
        data = str(event.get("data") or "")
        chat_id = str(event.get("chat_id") or "")
        if data == "offers_mine":
            self.edit_or_send(event, "⏳ جارٍ جلب العروض النشطة من OpenSea للمحافظ النشطة…", [[("🏠 الرئيسية", "menu")]])
            self._offer_submit(chat_id, self._offers_mine_worker, chat_id, int(event.get("message_id", 0) or 0))
            return True
        if data == "offer_qualification_list":
            candidates = [c for c in self.candidates.values() if c.qualification_tracked]
            text = "💰 تقديم Offer من قسم التأهيل\n\nاختر المشروع:" if candidates else "لا توجد مشاريع تأهيل نشطة في الذاكرة حاليًا."
            self.edit_or_send(event, text, self.offer_qualification_buttons())
            return True
        if data == "offer_settings":
            self.edit_or_send(event, self.offer_settings_text(), self.offer_settings_buttons())
            return True
        if data == "ofincmenu":
            self.edit_or_send(event, "🏆 اختر مقدار الزيادة فوق Top Offer:", [
                [("0.01 USDT", "ofinc:0.01"), ("0.05 USDT", "ofinc:0.05")],
                [("0.10 USDT", "ofinc:0.10"), ("0.25 USDT", "ofinc:0.25")],
                [("✏️ مخصص", "ofinccustom"), ("↩️ Offers", "offer_settings")],
            ])
            return True
        if data.startswith("ofinc:"):
            try:
                value = max(Decimal("0"), Decimal(data.split(":", 1)[1]))
            except Exception:
                return True
            self.offer_top_increment_usdt = value
            self.store.set_setting("offer_top_increment_usdt", str(value))
            self.edit_or_send(event, self.offer_settings_text(), self.offer_settings_buttons())
            return True
        if data == "ofinccustom":
            self.offer_clear_pending_inputs(chat_id)
            self.pending_offer_setting[chat_id] = {"kind": "increment", "expiry": time.time() + 180}
            self.telegram.send(chat_id, "✏️ أرسل مقدار الزيادة فوق Top Offer بالـUSDT، مثال: 0.03\n/cancel للإلغاء.")
            return True
        if data == "ofdurmenu":
            self.edit_or_send(event, "⏳ اختر مدة Collection Offer:", [
                [("1 ساعة", "ofdur:1"), ("6 ساعات", "ofdur:6")],
                [("24 ساعة", "ofdur:24"), ("72 ساعة", "ofdur:72")],
                [("7 أيام", "ofdur:168"), ("✏️ مخصص", "ofd_custom")],
                [("↩️ Offers", "offer_settings")],
            ])
            return True
        if data.startswith("ofdur:"):
            try:
                hours = max(1, min(int(data.split(":", 1)[1]), 24 * 30))
            except Exception:
                return True
            self.offer_duration_hours = hours
            self.store.set_setting("offer_duration_hours", str(hours))
            self.edit_or_send(event, self.offer_settings_text(), self.offer_settings_buttons())
            return True
        if data == "ofd_custom":
            self.offer_clear_pending_inputs(chat_id)
            self.pending_offer_setting[chat_id] = {"kind": "duration", "expiry": time.time() + 180}
            self.telegram.send(chat_id, "✏️ أرسل مدة العرض بالساعات (1 إلى 720).\n/cancel للإلغاء.")
            return True
        if data.startswith("ofp:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if candidate:
                self._offer_start_from_candidate(event, candidate)
            else:
                self.telegram.send(chat_id, "⚠️ لم يعد المشروع موجودًا في المراقبة النشطة.")
            return True
        if data.startswith("ofrefresh:"):
            sid = data.split(":", 1)[1]
            if self._offer_session(sid):
                self.edit_or_send(event, "⏳ جارٍ تحديث العروض الحالية…", [[("❌ إلغاء", f"ofclose:{sid}")]])
                self._offer_submit(chat_id, self._offer_load_market_worker, chat_id, sid, int(event.get("message_id", 0) or 0))
            return True
        if data.startswith("oft:"):
            sid = data.split(":", 1)[1]
            if self._offer_session(sid):
                self.edit_or_send(event, "⏳ جارٍ قراءة Top Offer مباشرة من OpenSea…", [[("❌ إلغاء", f"ofclose:{sid}")]])
                self._offer_submit(chat_id, self._offer_set_top_worker, chat_id, sid, int(event.get("message_id", 0) or 0))
            return True
        if data.startswith("ofm:"):
            parts = data.split(":")
            if len(parts) == 3:
                session = self._offer_session(parts[1])
                if session:
                    try:
                        idx = int(parts[2])
                        item = list((session.get("market") or {}).get("offers") or [])[idx]
                        session["unit_usdt"] = Decimal(str(item["unit_usdt"]))
                        session["mode"] = "market"
                        self.edit_or_send(event, self._offer_market_text(session), self._offer_market_buttons(session))
                    except Exception:
                        pass
            return True
        if data.startswith("ofcustom:"):
            sid = data.split(":", 1)[1]
            if self._offer_session(sid):
                self.offer_clear_pending_inputs(chat_id)
                self.pending_offer_custom_price[chat_id] = {"sid": sid, "expiry": time.time() + 300}
                self.telegram.send(chat_id, "✍️ أرسل السعر لكل NFT بالـUSDT.\nمثال: 12.50\n/cancel للإلغاء.")
            return True
        if data.startswith("ofw:"):
            parts = data.split(":")
            if len(parts) == 3:
                session = self._offer_session(parts[1])
                stored = self.store.get_wallet_by_id(int(parts[2])) if parts[2].isdigit() else None
                if session and stored and stored.enabled:
                    selected = session.setdefault("selected", {})
                    key = str(stored.id)
                    if key in selected:
                        selected.pop(key, None)
                    else:
                        selected[key] = 1
                    self.edit_or_send(event, self._offer_market_text(session), self._offer_market_buttons(session))
            return True
        if data.startswith("ofq:"):
            parts = data.split(":")
            if len(parts) == 3:
                session = self._offer_session(parts[1])
                stored = self.store.get_wallet_by_id(int(parts[2])) if parts[2].isdigit() else None
                if session and stored and stored.enabled:
                    self.offer_clear_pending_inputs(chat_id)
                    self.pending_offer_quantity[chat_id] = {"sid": parts[1], "wallet_id": stored.id, "expiry": time.time() + 300}
                    self.telegram.send(chat_id, f"🔢 أرسل كمية Offer للمحفظة «{stored.name}» من 1 إلى 100.\n/cancel للإلغاء.")
            return True
        if data.startswith("ofa:") or data.startswith("ofx:"):
            sid = data.split(":", 1)[1]
            session = self._offer_session(sid)
            if session:
                if data.startswith("ofa:"):
                    session["selected"] = {str(w.id): 1 for w in self._offer_active_wallets(session["chain"])[:20]}
                else:
                    session["selected"] = {}
                self.edit_or_send(event, self._offer_market_text(session), self._offer_market_buttons(session))
            return True
        if data.startswith("ofr:"):
            sid = data.split(":", 1)[1]
            if self._offer_session(sid):
                self.edit_or_send(event, "⏳ جارٍ فحص WETH والـApproval لكل محفظة…", [[("❌ إلغاء", f"ofclose:{sid}")]])
                self._offer_submit(chat_id, self._offer_review_worker, chat_id, sid, int(event.get("message_id", 0) or 0))
            return True
        if data.startswith("ofapprove:"):
            parts = data.split(":")
            if len(parts) == 3 and parts[2].isdigit() and self._offer_session(parts[1]):
                self.telegram.send(chat_id, "⏳ جارٍ إعادة فحص المبلغ ثم إرسال WETH Approval…")
                self._offer_submit_financial(chat_id, self._offer_approval_worker, chat_id, parts[1], int(parts[2]))
            return True
        if data.startswith("ofconfirm:"):
            sid = data.split(":", 1)[1]
            if self._offer_session(sid):
                self.telegram.send(chat_id, "⏳ جارٍ إجراء الفحص النهائي، ثم توقيع وإرسال Offer فقط إذا بقي السعر والرصيد والـApproval صالحين…")
                self._offer_submit_financial(chat_id, self._offer_confirm_worker, chat_id, sid)
            return True
        if data.startswith("ofback:"):
            sid = data.split(":", 1)[1]
            session = self._offer_session(sid)
            if session:
                self.edit_or_send(event, self._offer_market_text(session), self._offer_market_buttons(session))
            return True
        if data.startswith("ofclose:"):
            sid = data.split(":", 1)[1]
            with self.offer_lock:
                self.offer_sessions.pop(sid, None)
                if self.offer_current_session.get(chat_id) == sid:
                    self.offer_current_session.pop(chat_id, None)
            self.offer_clear_pending_inputs(chat_id)
            self.edit_or_send(event, "❌ تم إلغاء إعداد الـOffer. لم يتم توقيع أو إرسال أي عرض.", self.menu_buttons())
            return True
        if data.startswith("ofdetail:"):
            raw = data.split(":", 1)[1]
            if raw.isdigit():
                row = self.store.get_offer(int(raw))
                if row:
                    buttons = []
                    if str(row.get("status")) == "active":
                        buttons.append([("❌ إلغاء العرض", f"ofcancelask:{int(row['id'])}"), ("⬆️ رفع العرض", f"ofraise:{int(row['id'])}")])
                    buttons.append([("↩️ عروضي", "offers_mine")])
                    self.edit_or_send(event, self._offer_detail_text(row), buttons)
            return True
        if data.startswith("ofcancelask:"):
            raw = data.split(":", 1)[1]
            if raw.isdigit():
                row = self.store.get_offer(int(raw))
                if row:
                    self.edit_or_send(event, "⚠️ تأكيد إلغاء هذا Offer من OpenSea؟\nلن يتم إرسال معاملة غاز؛ سيستخدم SignedZone off-chain cancellation.", [[("✅ نعم، إلغاء", f"ofcancel:{int(row['id'])}"), ("❌ تراجع", f"ofdetail:{int(row['id'])}")]])
            return True
        if data.startswith("ofcancel:"):
            raw = data.split(":", 1)[1]
            if raw.isdigit():
                self.telegram.send(chat_id, "⏳ جارٍ توقيع طلب الإلغاء وإرساله إلى OpenSea…")
                self._offer_submit_financial(chat_id, self._offer_cancel_worker, chat_id, int(raw))
            return True
        if data.startswith("ofraise:"):
            raw = data.split(":", 1)[1]
            if raw.isdigit():
                row = self.store.get_offer(int(raw))
                if row:
                    self._offer_start_raise(event, row)
            return True
        return False

    def handle_offer_message(self, event: dict[str, Any]) -> bool:
        chat_id = str(event.get("chat_id") or "")
        text = str(event.get("text") or "").strip()
        command = text.split()[0].lower().split("@", 1)[0] if text else ""
        if command == "/offers":
            self.telegram.send(chat_id, "⏳ جارٍ جلب عروضي الحالية…")
            self._offer_submit(chat_id, self._offers_mine_worker, chat_id, 0)
            return True

        pending = self.pending_offer_custom_price.get(chat_id)
        if pending:
            if time.time() > float(pending.get("expiry", 0)):
                self.pending_offer_custom_price.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة إدخال سعر الـOffer.")
                return True
            session = self._offer_session(str(pending.get("sid") or ""))
            if not session:
                self.pending_offer_custom_price.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت جلسة الـOffer.")
                return True
            try:
                value = Decimal(text.replace("USDT", "").replace("$", "").strip())
            except Exception:
                self.telegram.send(chat_id, "⚠️ أرسل رقمًا صحيحًا مثل 12.50 أو /cancel.")
                return True
            if value <= 0 or value > Decimal("1000000"):
                self.telegram.send(chat_id, "⚠️ السعر يجب أن يكون أكبر من صفر وأقل من 1,000,000 USDT.")
                return True
            session["unit_usdt"] = value
            session["mode"] = "custom"
            self.pending_offer_custom_price.pop(chat_id, None)
            self.telegram.send(chat_id, self._offer_market_text(session), self._offer_market_buttons(session))
            return True

        pending_q = self.pending_offer_quantity.get(chat_id)
        if pending_q:
            if time.time() > float(pending_q.get("expiry", 0)):
                self.pending_offer_quantity.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة إدخال الكمية.")
                return True
            session = self._offer_session(str(pending_q.get("sid") or ""))
            stored = self.store.get_wallet_by_id(int(pending_q.get("wallet_id") or 0))
            if not session or not stored or not stored.enabled:
                self.pending_offer_quantity.pop(chat_id, None)
                self.telegram.send(chat_id, "⚠️ انتهت الجلسة أو المحفظة غير متاحة.")
                return True
            try:
                qty = int(text)
            except ValueError:
                self.telegram.send(chat_id, "⚠️ أرسل رقمًا صحيحًا من 1 إلى 100 أو /cancel.")
                return True
            if qty < 1 or qty > 100:
                self.telegram.send(chat_id, "⚠️ الكمية يجب أن تكون من 1 إلى 100.")
                return True
            session.setdefault("selected", {})[str(stored.id)] = qty
            self.pending_offer_quantity.pop(chat_id, None)
            self.telegram.send(chat_id, f"✅ {stored.name}: كمية الـOffer = {qty}\n\n" + self._offer_market_text(session), self._offer_market_buttons(session))
            return True

        pending_setting = self.pending_offer_setting.get(chat_id)
        if pending_setting:
            if time.time() > float(pending_setting.get("expiry", 0)):
                self.pending_offer_setting.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة تعديل إعداد Offers.", self.offer_settings_buttons())
                return True
            kind = str(pending_setting.get("kind") or "")
            if kind == "increment":
                try:
                    value = Decimal(text.replace("USDT", "").replace("$", "").strip())
                except Exception:
                    self.telegram.send(chat_id, "⚠️ أرسل رقمًا صحيحًا، مثال 0.03.")
                    return True
                if value < 0 or value > Decimal("1000"):
                    self.telegram.send(chat_id, "⚠️ الزيادة يجب أن تكون بين 0 و1000 USDT.")
                    return True
                self.offer_top_increment_usdt = value
                self.store.set_setting("offer_top_increment_usdt", str(value))
            elif kind == "duration":
                try:
                    hours = int(text)
                except ValueError:
                    self.telegram.send(chat_id, "⚠️ أرسل عدد ساعات صحيحًا من 1 إلى 720.")
                    return True
                if hours < 1 or hours > 720:
                    self.telegram.send(chat_id, "⚠️ المدة يجب أن تكون من 1 إلى 720 ساعة.")
                    return True
                self.offer_duration_hours = hours
                self.store.set_setting("offer_duration_hours", str(hours))
            else:
                return False
            self.pending_offer_setting.pop(chat_id, None)
            self.telegram.send(chat_id, "✅ تم حفظ إعداد Offers.\n\n" + self.offer_settings_text(), self.offer_settings_buttons())
            return True
        return False

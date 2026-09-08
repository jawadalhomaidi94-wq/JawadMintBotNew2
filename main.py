from __future__ import annotations

import asyncio
import json
import hashlib
import logging
import os
import queue
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse
from zoneinfo import ZoneInfo

import requests
import websockets
from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

from buyer import (
    CHAIN_CONFIGS,
    EligibilityResult,
    OpenSeaClient,
    RpcPool,
    WalletConfig,
    alchemy_rpc,
    check_eligibility,
    check_seadrop_eligibility,
    default_rpcs,
    explorer_tx_url,
    mint_drop,
    mint_seadrop_public,
    native_symbol,
    normalize_chain,
    opensea_chain_name,
    read_seadrop_public_drop,
    SEADROP_ADDRESS,
    ZERO_ADDRESS,
)
from health import start_health_server
from storage import SecureStore

load_dotenv()
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("opensea-mint-guardian")

STOP = False


def stop_handler(*_):
    global STOP
    STOP = True


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def env_decimal(name: str, default: str) -> Decimal:
    try:
        return Decimal(os.getenv(name, default))
    except (InvalidOperation, TypeError):
        return Decimal(default)


def csv_values(value: str | None) -> list[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def short_address(address: str) -> str:
    return f"{address[:6]}…{address[-4:]}" if len(address) >= 12 else address


def parse_time(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number / 1000.0 if number > 10_000_000_000 else number
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            return parse_time(int(text))
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            return None
    return None


def stage_start(stage: dict[str, Any]) -> float | None:
    for key in ("startTime", "start_time", "start", "startDate", "startsAt", "starts_at"):
        if key in stage:
            return parse_time(stage[key])
    return None


def stage_end(stage: dict[str, Any]) -> float | None:
    for key in ("endTime", "end_time", "end", "endDate", "endsAt", "ends_at"):
        if key in stage:
            return parse_time(stage[key])
    return None


def stage_label(stage: dict[str, Any]) -> str:
    for key in ("label", "name", "stageName", "stage_name", "type", "kind"):
        value = stage.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "مرحلة Mint"


def is_public_stage(stage: dict[str, Any]) -> bool:
    # SeaDrop drop metadata commonly exposes an explicit isPublic boolean.
    for key in ("isPublic", "is_public", "public"):
        if isinstance(stage.get(key), bool):
            return bool(stage.get(key))
    labels: list[str] = []
    for key in ("label", "name", "stageName", "stage_name", "type", "kind"):
        if isinstance(stage.get(key), str):
            labels.append(str(stage[key]).lower())
    if any("public" in x or "عام" in x for x in labels):
        return True
    allow_values = [
        stage.get("allowlist"), stage.get("allow_list"), stage.get("merkleRoot"), stage.get("merkle_root"),
        stage.get("allowList"), stage.get("allowListURI"), stage.get("allow_list_uri"),
    ]
    return not labels and not any(v not in (None, "", False, [], {}) for v in allow_values)


def max_per_wallet(stage: dict[str, Any]) -> int | None:
    for key in (
        "maxPerWallet", "max_per_wallet", "walletLimit", "wallet_limit",
        "maxTotalMintableByWallet", "max_total_mintable_by_wallet",
    ):
        try:
            value = int(stage.get(key))
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
    return None


def extract_price_hint(stage: dict[str, Any]) -> str:
    value = None
    for key in ("price", "mintPrice", "mint_price"):
        if key in stage:
            value = stage.get(key)
            break
    if value is None:
        return "غير معروف"
    if isinstance(value, dict):
        for path in ("value", "raw", "wei", "amount"):
            v = value.get(path)
            if v is not None:
                if isinstance(v, dict):
                    v = v.get("value") or v.get("raw") or v.get("wei")
                if v is not None:
                    return str(v)
    return str(value)


def _positive_numeric(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, dict):
        preferred = ("value", "raw", "wei", "amount", "price", "mintPrice", "mint_price")
        for key in preferred:
            if key in value and _positive_numeric(value.get(key)):
                return True
        return False
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value)) > 0
        except Exception:
            return False
    if isinstance(value, str):
        match = re.search(r"-?\d+(?:\.\d+)?", value.replace(",", ""))
        if not match:
            return False
        try:
            return Decimal(match.group(0)) > 0
        except Exception:
            return False
    return False


def stage_has_paid_price(stage: dict[str, Any]) -> bool:
    for key in ("price", "mintPrice", "mint_price"):
        if key in stage:
            return _positive_numeric(stage.get(key))
    return False


def _numeric_value(value: Any) -> Decimal | None:
    if value is None or value is False:
        return None
    if isinstance(value, dict):
        for key in ("value", "raw", "wei", "amount", "price", "mintPrice", "mint_price"):
            if key in value:
                parsed = _numeric_value(value.get(key))
                if parsed is not None:
                    return parsed
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except Exception:
            return None
    if isinstance(value, str):
        match = re.search(r"-?\d+(?:\.\d+)?", value.replace(",", ""))
        if not match:
            return None
        try:
            return Decimal(match.group(0))
        except Exception:
            return None
    return None


def stage_is_explicitly_free(stage: dict[str, Any]) -> bool:
    """Return True only when the stage exposes a numeric zero price.

    Auto discovery is deliberately fail-closed: an unknown/missing price is
    never treated as free. The final mint builder still independently refuses
    any transaction whose value is greater than zero.
    """
    for key in ("price", "mintPrice", "mint_price"):
        if key in stage:
            value = _numeric_value(stage.get(key))
            return value is not None and value == 0
    return False


def stage_is_active(stage: dict[str, Any], now: float | None = None) -> bool:
    now = time.time() if now is None else now
    start = stage_start(stage)
    end = stage_end(stage)
    return (start is None or start <= now) and (end is None or end > now)


def active_free_wallet_limit(drop: dict[str, Any]) -> int | None:
    limits = [
        limit for stage in get_stages(drop)
        if stage_is_active(stage) and stage_is_explicitly_free(stage)
        for limit in [max_per_wallet(stage)] if limit
    ]
    return max(limits) if limits else None


def has_active_free_stage(drop: dict[str, Any]) -> bool:
    return any(stage_is_active(stage) and stage_is_explicitly_free(stage) for stage in get_stages(drop))


def stage_identity(stage: dict[str, Any], index: int) -> str:
    for key in ("uuid", "id", "stageId", "stage_id"):
        value = stage.get(key)
        if value not in (None, ""):
            return str(value)[:80]
    raw = "|".join([
        str(index), stage_label(stage), str(stage_start(stage) or ""), str(stage_end(stage) or ""),
        extract_price_hint(stage), str(max_per_wallet(stage) or ""), str(is_public_stage(stage)),
    ])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def build_stage_plans(drop: dict[str, Any]) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    for index, stage in enumerate(get_stages(drop)):
        start = stage_start(stage)
        end = stage_end(stage)
        free = stage_is_explicitly_free(stage)
        paid = stage_has_paid_price(stage)
        plans.append({
            "key": stage_identity(stage, index),
            "index": index,
            "label": stage_label(stage),
            "start": start,
            "end": end,
            "is_public": is_public_stage(stage),
            "is_free": free,
            "is_paid": paid,
            "wallet_limit": max_per_wallet(stage),
            "price": extract_price_hint(stage),
        })
    plans.sort(key=lambda x: (float(x.get("start") or 0), int(x.get("index") or 0)))
    return plans


def plan_is_active(plan: dict[str, Any], now: float | None = None) -> bool:
    now = time.time() if now is None else now
    start = plan.get("start")
    end = plan.get("end")
    return (start is None or float(start) <= now) and (end is None or float(end) > now)


def active_plans(plans: list[dict[str, Any]], now: float | None = None) -> list[dict[str, Any]]:
    return [p for p in plans if plan_is_active(p, now)]


def next_future_plan(plans: list[dict[str, Any]], now: float | None = None) -> dict[str, Any] | None:
    now = time.time() if now is None else now
    future = [p for p in plans if p.get("start") is not None and float(p["start"]) > now]
    return min(future, key=lambda p: float(p["start"])) if future else None


def final_public_plan(plans: list[dict[str, Any]]) -> dict[str, Any] | None:
    publics = [p for p in plans if p.get("is_public")]
    if not publics:
        return None
    return max(publics, key=lambda p: (float(p.get("start") or 0), int(p.get("index") or 0)))


def has_qualification_stages(plans: list[dict[str, Any]]) -> bool:
    return len(plans) > 1 or any(not bool(p.get("is_public")) for p in plans)


def _first_int(payload: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass
    return None


def remaining_supply(drop: dict[str, Any]) -> int | None:
    maximum = _first_int(drop, "maxSupply", "max_supply")
    total = _first_int(drop, "totalSupply", "total_supply", "minted", "mintedSupply", "minted_supply")
    if maximum is None or total is None:
        return None
    return max(0, maximum - total)


def pagination_cursor(payload: dict[str, Any]) -> str | None:
    for key in ("next", "next_cursor", "nextCursor", "cursor"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    data = payload.get("data")
    return pagination_cursor(data) if isinstance(data, dict) else None


def chain_label(chain: str) -> str:
    return {
        "ethereum": "إيثيريوم",
        "ink": "إنك",
        "robinhood": "روبن هود",
        "base": "بيس",
        "arbitrum": "أربيتروم",
        "optimism": "أوبتيمزم",
        "polygon": "بوليجون",
    }.get(normalize_chain(chain), chain)


def eligibility_label(status: str) -> str:
    return {
        "eligible_now": "مؤهلة الآن",
        "not_eligible_now": "غير مؤهلة للمرحلة الحالية",
        "not_active_yet": "المرحلة لم تفتح بعد",
        "rate_limited": "تم تقييد الطلب مؤقتًا",
        "opensea_error": "خطأ من OpenSea",
        "preflight_error": "تعذر فحص الأهلية",
        "seadrop_not_configured": "لا يوجد Public SeaDrop مهيأ",
        "stage_ended": "انتهت المرحلة",
        "sold_out": "نفدت الكمية",
        "no_fee_recipient": "تعذر تحديد عنوان الرسوم",
        "unknown": "غير معروف",
    }.get(status, status)


def find_list(payload: dict[str, Any], *names: str) -> list[dict[str, Any]]:
    for name in names:
        value = payload.get(name)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    data = payload.get("data")
    if isinstance(data, dict):
        return find_list(data, *names)
    return []


def get_stages(drop: dict[str, Any]) -> list[dict[str, Any]]:
    stages = find_list(drop, "stages", "mint_stages", "mintStages")
    if stages:
        return stages
    mint = drop.get("mint")
    return find_list(mint, "stages", "mint_stages", "mintStages") if isinstance(mint, dict) else []


def get_slug(item: dict[str, Any]) -> str | None:
    for key in ("collection_slug", "collectionSlug", "slug"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    collection = item.get("collection")
    return get_slug(collection) if isinstance(collection, dict) else None


def extract_known_chains(payload: Any) -> list[str]:
    found: list[str] = []

    def walk(value: Any, depth: int = 0):
        if depth > 5:
            return
        if isinstance(value, dict):
            for key, child in value.items():
                key_l = str(key).lower()
                if key_l in {"chain", "chain_name", "chainname", "blockchain", "network"}:
                    if isinstance(child, str):
                        chain = normalize_chain(child)
                        if chain in CHAIN_CONFIGS and chain not in found:
                            found.append(chain)
                    elif isinstance(child, dict):
                        for candidate_key in ("name", "slug", "identifier", "chain"):
                            candidate_value = child.get(candidate_key)
                            if isinstance(candidate_value, str):
                                chain = normalize_chain(candidate_value)
                                if chain in CHAIN_CONFIGS and chain not in found:
                                    found.append(chain)
                walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value[:50]:
                walk(child, depth + 1)

    walk(payload)
    return found



ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")


def extract_contract_address(payload: Any) -> str | None:
    """Extract an NFT contract address without confusing wallet/from/to addresses."""
    if not isinstance(payload, dict):
        return None
    for key in ("contract_address", "contractAddress", "nft_contract", "nftContract"):
        value = payload.get(key)
        if isinstance(value, str) and Web3.is_address(value):
            return Web3.to_checksum_address(value)
    for key in ("contract", "nft", "asset"):
        value = payload.get(key)
        if isinstance(value, dict):
            for akey in ("address", "contract_address", "contractAddress"):
                address = value.get(akey)
                if isinstance(address, str) and Web3.is_address(address):
                    return Web3.to_checksum_address(address)
        elif isinstance(value, str):
            # Global Events API commonly uses compact NFT identifiers such as
            # chain/0xContract/tokenId. Extract only the 40-byte address.
            match = ADDRESS_RE.search(value)
            if match and Web3.is_address(match.group(0)):
                return Web3.to_checksum_address(match.group(0))
    for key in ("nft_id", "nftId"):
        value = payload.get(key)
        if isinstance(value, str):
            match = ADDRESS_RE.search(value)
            if match and Web3.is_address(match.group(0)):
                return Web3.to_checksum_address(match.group(0))
    for key in ("item", "asset", "nft", "token", "payload"):
        child = payload.get(key)
        if isinstance(child, dict):
            found = extract_contract_address(child)
            if found:
                return found
    return None


def extract_event_slug(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    slug = get_slug(payload)
    if slug:
        return slug
    for key in ("item", "asset", "nft", "payload"):
        child = payload.get(key)
        if isinstance(child, dict):
            found = extract_event_slug(child)
            if found:
                return found
    return None


def extract_event_chain(payload: Any) -> str | None:
    if isinstance(payload, dict):
        # Events may encode the chain inside a compact NFT identifier rather
        # than a dedicated chain field.
        for key in ("nft", "nft_id", "nftId"):
            value = payload.get(key)
            if isinstance(value, str):
                prefix = value.split("/", 1)[0].split(":", 1)[0].strip()
                chain = normalize_chain(prefix)
                if chain in CHAIN_CONFIGS:
                    return chain
        for key in ("item", "asset", "payload"):
            child = payload.get(key)
            if isinstance(child, dict):
                found = extract_event_chain(child)
                if found:
                    return found
    chains = extract_known_chains(payload)
    return chains[0] if chains else None


def event_is_zero_address_mint(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in ("from_address", "fromAddress", "from"):
        value = payload.get(key)
        if isinstance(value, str) and value.lower() == ZERO_ADDRESS.lower():
            return True
    for key in ("from_account", "fromAccount"):
        value = payload.get(key)
        if isinstance(value, dict):
            address = value.get("address")
            if isinstance(address, str) and address.lower() == ZERO_ADDRESS.lower():
                return True
    inner = payload.get("payload")
    return event_is_zero_address_mint(inner) if isinstance(inner, dict) else False


def collection_contract_candidates(payload: Any) -> list[tuple[str | None, str]]:
    """Return (chain, contract) pairs from OpenSea collection/contract metadata."""
    found: list[tuple[str | None, str]] = []
    seen: set[str] = set()

    def add(chain_value: Any, address_value: Any):
        if not isinstance(address_value, str) or not Web3.is_address(address_value):
            return
        address = Web3.to_checksum_address(address_value)
        key = address.lower()
        if key in seen:
            return
        chain = normalize_chain(chain_value) if isinstance(chain_value, str) else None
        if chain not in CHAIN_CONFIGS:
            chain = None
        seen.add(key)
        found.append((chain, address))

    def walk(value: Any, depth: int = 0):
        if depth > 5:
            return
        if isinstance(value, dict):
            chain_value = value.get("chain") or value.get("network") or value.get("blockchain")
            if isinstance(chain_value, dict):
                chain_value = chain_value.get("name") or chain_value.get("slug") or chain_value.get("identifier")
            for key in ("contract_address", "contractAddress"):
                add(chain_value, value.get(key))
            contract = value.get("contract")
            if isinstance(contract, str):
                add(chain_value, contract)
            elif isinstance(contract, dict):
                add(chain_value or contract.get("chain"), contract.get("address") or contract.get("contract_address"))
            if "address" in value and any(k in value for k in ("chain", "network", "contract_standard", "contractStandard")):
                add(chain_value, value.get("address"))
            for child in value.values():
                walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value[:100]:
                walk(child, depth + 1)

    walk(payload)
    return found


def event_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return find_list(payload, "asset_events", "events", "results", "items")


def slug_from_text(text: str) -> tuple[str | None, str | None]:
    raw = text.strip()
    if not raw:
        return None, None
    if re.fullmatch(r"[A-Za-z0-9._-]{2,200}", raw):
        return raw, None
    try:
        parsed = urlparse(raw)
        host = parsed.netloc.lower()
        if "opensea.io" not in host:
            return None, None
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        chain_hint = None
        for part in parts:
            candidate = normalize_chain(part)
            if candidate in CHAIN_CONFIGS:
                chain_hint = candidate
        if "collection" in parts:
            idx = parts.index("collection")
            if idx + 1 < len(parts):
                return parts[idx + 1], chain_hint
        qs = parse_qs(parsed.query)
        for key in ("slug", "collection"):
            if qs.get(key):
                return qs[key][0], chain_hint
        if parts:
            return parts[-1], chain_hint
    except Exception:
        pass
    return None, None


def format_ts(ts: float | None, tz: ZoneInfo) -> str:
    if ts is None:
        return "غير معروف"
    return datetime.fromtimestamp(ts, tz).strftime("%Y-%m-%d %H:%M:%S %Z")


class AlchemyPriceOracle:
    """Small cached USD price oracle using the existing Alchemy API key.

    The bot only needs a fresh native-token price to enforce a hard dollar gas
    ceiling. Results are cached so parallel wallets do not multiply API calls.
    """

    def __init__(self, api_key: str, *, ttl_seconds: float = 60.0, timeout: float = 5.0):
        self.api_key = api_key.strip()
        self.ttl_seconds = max(10.0, float(ttl_seconds))
        self.timeout = max(2.0, float(timeout))
        self._cache: dict[str, tuple[float, Decimal]] = {}
        self._lock = threading.Lock()

    def get_usd(self, symbol: str) -> Decimal | None:
        symbol = symbol.upper().strip()
        if not self.api_key or not symbol:
            return None
        now = time.time()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and now - cached[0] <= self.ttl_seconds:
                return cached[1]
            try:
                url = f"https://api.g.alchemy.com/prices/v1/{self.api_key}/tokens/by-symbol"
                response = requests.get(url, params={"symbols": symbol}, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                rows = payload.get("data", []) if isinstance(payload, dict) else []
                for row in rows if isinstance(rows, list) else []:
                    if str(row.get("symbol", "")).upper() != symbol:
                        continue
                    prices = row.get("prices", [])
                    for price in prices if isinstance(prices, list) else []:
                        if str(price.get("currency", "")).upper() == "USD":
                            value = Decimal(str(price.get("value")))
                            if value > 0:
                                self._cache[symbol] = (now, value)
                                return value
            except Exception as exc:
                log.debug("Alchemy price lookup failed for %s: %s", symbol, exc)
                # A recent stale value is safer than disabling execution entirely
                # because of one transient HTTP error. Limit stale use to 10 min.
                cached = self._cache.get(symbol)
                if cached and now - cached[0] <= 600:
                    return cached[1]
        return None


@dataclass
class WalletState:
    wallet: WalletConfig
    quantity: int = 1
    next_attempt: float = 0.0
    attempts: int = 0
    status: str = "waiting"
    last_notified_status: str = ""
    last_detail: str | None = None
    eligibility: str = "unknown"
    mint_value_native: Decimal | None = None
    submitted: bool = False
    confirmed: bool = False
    final: bool = False
    tx_hash: str | None = None
    receipt_next_check: float = 0.0
    stage_key: str = ""
    stage_label: str = ""
    target_total: int = 0
    confirmed_total: int = 0
    pending_quantity: int = 0


@dataclass
class Candidate:
    slug: str
    chain: str
    source: str
    allow_paid: bool
    max_mint_price_native: Decimal
    quantity_override: int | None = None
    public_start: float | None = None
    next_stage_start: float | None = None
    stage_lines: list[str] = field(default_factory=list)
    wallet_limit: int | None = None
    wallets: dict[str, WalletState] = field(default_factory=dict)
    next_refresh: float = 0.0
    done: bool = False
    has_paid_stage: bool = False
    paid_detected: bool = False
    paid_selection_confirmed: bool = False
    paid_wallet_addresses: set[str] = field(default_factory=set)
    paid_selection_notified: bool = False
    auto_discovered: bool = False
    last_seen_auto: float = 0.0
    remaining_supply: int | None = None
    mint_backend: str = "opensea"  # opensea | seadrop
    contract_address: str | None = None
    stage_end: float | None = None
    discovery_source: str = ""
    watch_kind: str = "manual"  # manual | auto_stage | auto_free
    stage_plans: list[dict[str, Any]] = field(default_factory=list)
    checked_stage_keys: set[str] = field(default_factory=set)
    current_stage_key: str = ""
    current_stage_label: str = ""
    current_stage_start: float | None = None
    current_stage_end: float | None = None
    current_stage_public: bool = False
    current_stage_free: bool = False
    current_stage_paid: bool = False
    current_stage_limit: int | None = None
    final_public_start: float | None = None
    final_stage_end: float | None = None
    qualification_tracked: bool = False
    paid_wallet_quantities: dict[str, int] = field(default_factory=dict)
    paid_decision: str = ""  # '' | pending | confirmed | declined
    paid_stage_key: str = ""
    paid_stage_start: float | None = None
    paid_offer_notified_stage_key: str = ""
    last_qualification_check: float = 0.0
    last_schedule_tick: float = 0.0
    processed_stage_key: str = ""

    def submitted_count(self) -> int:
        return sum(1 for s in self.wallets.values() if s.submitted)

    def confirmed_count(self) -> int:
        return sum(1 for s in self.wallets.values() if s.confirmed)


class TelegramController(threading.Thread):
    def __init__(self, bot: "Bot"):
        super().__init__(name="telegram-controller", daemon=True)
        self.bot = bot
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.allowed_chat_ids = set(csv_values(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS")))
        self.allow_any = env_bool("TELEGRAM_ALLOW_ANY_CHAT", False)
        self.offset = 0
        # Long-polling and outbound Telegram actions use separate sessions.
        # requests.Session is not guaranteed to be thread-safe and V4.4 used
        # the same object from the polling thread and background notification
        # threads, which could make /start and callbacks appear unresponsive.
        self.poll_session = requests.Session()
        self.action_session = requests.Session()
        self.action_lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.token)

    def api(self, method: str, **data: Any) -> dict[str, Any]:
        with self.action_lock:
            response = self.action_session.post(
                f"https://api.telegram.org/bot{self.token}/{method}", data=data, timeout=35
            )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def poll(self, **data: Any) -> dict[str, Any]:
        response = self.poll_session.post(
            f"https://api.telegram.org/bot{self.token}/getUpdates", data=data, timeout=35
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def send(self, chat_id: str | int, text: str, buttons: list[list[tuple[str, str]]] | None = None) -> None:
        if not self.enabled:
            return
        data: dict[str, Any] = {
            "chat_id": str(chat_id),
            "text": text[:3900],
            "disable_web_page_preview": "true",
        }
        if buttons:
            data["reply_markup"] = json.dumps({
                "inline_keyboard": [[{"text": label, "callback_data": callback} for label, callback in row] for row in buttons]
            })
        try:
            self.api("sendMessage", **data)
        except Exception as exc:
            detail = getattr(getattr(exc, "response", None), "text", "") or str(exc)
            log.warning("Telegram send failed: %s", detail[:500])

    def edit(self, chat_id: str | int, message_id: int, text: str, buttons: list[list[tuple[str, str]]] | None = None) -> None:
        data: dict[str, Any] = {
            "chat_id": str(chat_id),
            "message_id": int(message_id),
            "text": text[:3900],
            "disable_web_page_preview": "true",
        }
        if buttons is not None:
            data["reply_markup"] = json.dumps({
                "inline_keyboard": [[{"text": label, "callback_data": callback} for label, callback in row] for row in buttons]
            })
        try:
            self.api("editMessageText", **data)
        except Exception as exc:
            detail = getattr(getattr(exc, "response", None), "text", "") or str(exc)
            if "message is not modified" in detail.lower():
                return
            log.warning("Telegram edit failed; sending a fresh message instead: %s", detail[:500])
            self.send(chat_id, text, buttons)

    def answer_callback(self, callback_id: str, text: str = "") -> None:
        try:
            self.api("answerCallbackQuery", callback_query_id=callback_id, text=text[:180])
        except Exception:
            pass

    def delete_message(self, chat_id: str, message_id: int) -> None:
        try:
            self.api("deleteMessage", chat_id=chat_id, message_id=message_id)
        except Exception:
            pass

    def authorized(self, chat_id: str) -> bool:
        return self.allow_any or chat_id in self.allowed_chat_ids

    def setup_commands(self) -> None:
        # Force long-polling mode and discard stale callbacks from older
        # deployments. This prevents a leftover webhook/backlog from blocking
        # /start or the inline menu after a Railway redeploy.
        try:
            self.api("deleteWebhook", drop_pending_updates="true")
            me = self.api("getMe")
            username = ((me.get("result") or {}).get("username") or "unknown") if isinstance(me, dict) else "unknown"
            log.info("Telegram polling ready | bot=@%s", username)
        except Exception as exc:
            log.warning("Telegram initialization warning: %s", exc)
        commands = [
            {"command": "start", "description": "فتح القائمة الرئيسية"},
            {"command": "wallets", "description": "عرض وإدارة المحافظ"},
            {"command": "status", "description": "المنتات تحت المراقبة"},
            {"command": "qualification", "description": "قسم التأهيل ومراحل اليوم"},
            {"command": "watch", "description": "إضافة رابط منت للمراقبة"},
            {"command": "eligibility", "description": "فحص أهلية رابط للمحافظ النشطة"},
            {"command": "chains", "description": "حالة الشبكات والـRPC"},
            {"command": "free", "description": "المنتات المجانية التي تم أخذها"},
            {"command": "paid", "description": "المنتات المدفوعة المحفوظة"},
            {"command": "history", "description": "سجل عمليات الـMint"},
            {"command": "pause", "description": "إيقاف تنفيذ المعاملات مع استمرار المراقبة"},
            {"command": "resume", "description": "استئناف تنفيذ المعاملات"},
        ]
        try:
            self.api("setMyCommands", commands=json.dumps(commands, ensure_ascii=False))
        except Exception as exc:
            log.debug("Telegram setMyCommands failed: %s", exc)

    def run(self) -> None:
        log.info("Telegram listener enabled")
        self.setup_commands()
        while not STOP:
            try:
                payload = self.poll(
                    offset=self.offset,
                    timeout=25,
                    allowed_updates='["message","callback_query"]',
                )
                for update in payload.get("result", []):
                    if not isinstance(update, dict):
                        continue
                    self.offset = max(self.offset, int(update.get("update_id", 0)) + 1)
                    if isinstance(update.get("callback_query"), dict):
                        cb = update["callback_query"]
                        msg = cb.get("message") or {}
                        chat = msg.get("chat") or {}
                        chat_id = str(chat.get("id", ""))
                        if not chat_id or not self.authorized(chat_id):
                            continue
                        log.info("Telegram callback received | chat_id=%s | data=%s", chat_id, str(cb.get("data", ""))[:80])
                        self.bot.command_queue.put({
                            "type": "callback",
                            "chat_id": chat_id,
                            "chat_type": str(chat.get("type", "")),
                            "callback_id": str(cb.get("id", "")),
                            "message_id": int(msg.get("message_id", 0)),
                            "data": str(cb.get("data", "")),
                        })
                        continue

                    message = update.get("message") or {}
                    chat = message.get("chat") or {}
                    chat_id = str(chat.get("id", ""))
                    text = str(message.get("text", "")).strip()
                    if not chat_id or not text:
                        continue
                    if not self.authorized(chat_id):
                        self.send(chat_id, f"⛔ هذه المحادثة غير مصرح لها بالتحكم في البوت.\nمعرّف المحادثة: {chat_id}")
                        continue
                    normalized_command = text.split()[0].lower().split("@", 1)[0] if text.split() else ""
                    if normalized_command in {"/start", "/menu", "/help"}:
                        log.info("Telegram %s received | chat_id=%s", normalized_command, chat_id)
                        # Serve the main menu directly from the polling thread so
                        # heavy mint/stage work cannot starve /start.
                        self.bot.send_menu(chat_id)
                        continue
                    self.bot.command_queue.put({
                        "type": "message",
                        "chat_id": chat_id,
                        "chat_type": str(chat.get("type", "")),
                        "message_id": int(message.get("message_id", 0)),
                        "text": text,
                    })
            except Exception as exc:
                log.warning("Telegram polling error: %s", exc)
                time.sleep(2)


class Bot:
    def __init__(self):
        self.opensea = OpenSeaClient(os.getenv("OPENSEA_API_KEY", "").strip(), timeout=env_float("HTTP_TIMEOUT", 7.0))
        self.display_tz = ZoneInfo(os.getenv("DISPLAY_TIMEZONE", "Asia/Aden"))
        self.quantity_default = max(1, min(env_int("QUANTITY", 1), 100))

        self.enabled_chains: list[str] = []
        for raw in csv_values(os.getenv("ENABLED_CHAINS", "ethereum,ink,robinhood")):
            chain = normalize_chain(raw)
            if chain in CHAIN_CONFIGS and chain not in self.enabled_chains:
                self.enabled_chains.append(chain)
        if not self.enabled_chains:
            raise ValueError("No supported ENABLED_CHAINS configured")

        volume = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        data_dir = Path(volume or os.getenv("DATA_DIR", "./data"))
        db_path = os.getenv("BOT_DB_PATH", "").strip() or str(data_dir / "mint_guardian.db")
        self.store = SecureStore(db_path, os.getenv("WALLET_ENCRYPTION_KEY", "").strip())

        self.allow_paid_default = env_bool("ALLOW_PAID_MINTS", True)
        self.max_mint_price_default = env_decimal("MAX_MINT_PRICE_NATIVE", "0")
        self.max_total_native = env_decimal("MAX_TOTAL_NATIVE", "0")
        self.max_gas_native = env_decimal("MAX_GAS_NATIVE", "0")
        # V4.1 defaults are cost-first. A hard USD gas budget is enforced
        # separately from the native-token cap.
        self.max_gas_usd = env_decimal("MAX_GAS_USD", "0.08")
        self.gas_strategy = os.getenv("GAS_STRATEGY", "smart").strip().lower()
        self.gas_limit_buffer = max(1.0, env_float("GAS_LIMIT_BUFFER", 1.08))
        self.gas_over_budget_retry_seconds = max(0.5, env_float("GAS_OVER_BUDGET_RETRY_SECONDS", 2.0))
        self.price_oracle = AlchemyPriceOracle(
            os.getenv("ALCHEMY_API_KEY", "").strip(),
            ttl_seconds=env_float("PRICE_CACHE_SECONDS", 60.0),
            timeout=env_float("HTTP_TIMEOUT", 7.0),
        )
        self.allowed_targets = {
            Web3.to_checksum_address(x).lower() for x in csv_values(os.getenv("ALLOWED_TARGETS")) if Web3.is_address(x)
        }

        self.preopen_probe_seconds = max(0.0, env_float("PREOPEN_PROBE_SECONDS", 1.5))
        self.open_retry_interval = max(0.10, env_float("OPEN_RETRY_INTERVAL", 0.30))
        self.monitor_retry_interval = max(0.5, env_float("MONITOR_RETRY_INTERVAL", 2.0))
        self.eligibility_retry_seconds = max(2.0, env_float("ELIGIBILITY_RETRY_SECONDS", 10.0))
        self.rate_limit_retry_seconds = max(2.0, env_float("RATE_LIMIT_RETRY_SECONDS", 5.0))
        self.stage_refresh_seconds = max(3.0, env_float("STAGE_REFRESH_SECONDS", 20.0))
        self.fast_stage_refresh_seconds = max(1.0, env_float("FAST_STAGE_REFRESH_SECONDS", 3.0))
        self.fast_refresh_window = max(15.0, env_float("FAST_REFRESH_WINDOW", 120.0))
        self.receipt_check_seconds = max(2.0, env_float("RECEIPT_CHECK_SECONDS", 5.0))
        self.max_parallel_wallets = max(1, env_int("MAX_PARALLEL_WALLETS", 10))
        self.drop_limit = max(1, min(env_int("DROP_LIMIT", 25), 100))

        # V4.2 automatic free-mint discovery. The list scan itself runs every
        # 15 seconds by default; manual links remain persistent watches.
        self.auto_free_enabled = env_bool("AUTO_FREE_MINTS", True)
        self.auto_free_scan_seconds = max(5.0, env_float("AUTO_FREE_SCAN_SECONDS", 15.0))
        self.auto_free_drop_limit = max(1, min(env_int("AUTO_FREE_DROP_LIMIT", 100), 100))
        self.auto_free_initial_pages = max(1, min(env_int("AUTO_FREE_INITIAL_PAGES", 3), 10))
        self.auto_free_detail_workers = max(1, min(env_int("AUTO_FREE_DETAIL_WORKERS", 8), 20))
        self.auto_free_notify_discovery = env_bool("AUTO_FREE_NOTIFY_DISCOVERY", False)
        self.auto_free_candidate_ttl = max(30.0, env_float("AUTO_FREE_CANDIDATE_TTL", 90.0))
        configured_types = csv_values(os.getenv("AUTO_FREE_DROP_TYPES", "recently_minted,featured,upcoming"))
        self.auto_free_drop_types = [x for x in configured_types if x in {"recently_minted", "featured", "upcoming"}] or ["recently_minted", "featured", "upcoming"]
        self.auto_discovery_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.auto_discovery_thread: threading.Thread | None = None
        # V4.3: real-time mint discovery from OpenSea Stream plus REST mint-event
        # polling fallback. Drops scanning stays enabled as a third discovery path.
        self.auto_stream_enabled = env_bool("AUTO_FREE_STREAM", True)
        self.auto_event_fallback_enabled = env_bool("AUTO_FREE_EVENT_FALLBACK", True)
        self.auto_event_limit = max(1, min(env_int("AUTO_FREE_EVENT_LIMIT", 200), 200))
        self.auto_event_overlap_seconds = max(5, env_int("AUTO_FREE_EVENT_OVERLAP_SECONDS", 45))
        self.auto_event_initial_lookback_seconds = max(300, env_int("AUTO_FREE_EVENT_INITIAL_LOOKBACK_SECONDS", 86400))
        self.auto_event_initial_pages = max(1, min(env_int("AUTO_FREE_EVENT_INITIAL_PAGES", 3), 10))
        self.auto_event_last_after = int(time.time()) - max(60, self.auto_event_overlap_seconds)
        self.auto_event_seen: dict[str, float] = {}
        self.auto_stream_thread: threading.Thread | None = None

        # V4.4 stage planner / qualification engine.
        self.auto_stage_default_quantity = max(1, min(env_int("AUTO_STAGE_UNKNOWN_LIMIT_QTY", 30), 100))
        self.auto_stage_high_limit_threshold = max(1, env_int("AUTO_STAGE_HIGH_LIMIT_THRESHOLD", 100))
        self.auto_stage_high_limit_quantity = max(1, min(env_int("AUTO_STAGE_HIGH_LIMIT_QTY", 30), 100))
        self.stage_discovery_horizon_seconds = max(3600, env_int("STAGE_DISCOVERY_HORIZON_SECONDS", 604800))
        self.public_preopen_window_seconds = max(1.0, env_float("PUBLIC_PREOPEN_WINDOW_SECONDS", 5.0))
        self.public_fast_retry_seconds = max(0.10, env_float("PUBLIC_FAST_RETRY_SECONDS", 0.20))
        self.qualification_recheck_seconds = max(5.0, env_float("QUALIFICATION_RECHECK_SECONDS", 15.0))
        self.paused = env_bool("START_PAUSED", False)

        self.wallets: list[WalletConfig] = []
        self.rpc_pools: dict[str, RpcPool] = {}
        self.candidates: dict[str, Candidate] = {}
        self.command_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.pending_wallet_name: dict[str, float] = {}
        self.pending_wallet_import: dict[str, dict[str, Any]] = {}
        self.pending_wallet_rename: dict[str, dict[str, Any]] = {}
        self.pending_wallet_quantity: dict[str, dict[str, Any]] = {}
        self.pending_link_action: dict[str, dict[str, Any]] = {}
        self.pending_paid_quantity: dict[str, dict[str, Any]] = {}
        self.telegram = TelegramController(self)

        self.load_rpc_pools()
        self.import_env_wallets()
        self.reload_wallets()
        self.load_env_watches_to_store()

    # ---------- persistence / wallets ----------
    def import_env_wallets(self) -> None:
        keys = csv_values(os.getenv("PRIVATE_KEYS"))
        if not keys and os.getenv("PRIVATE_KEY", "").strip():
            keys = [os.getenv("PRIVATE_KEY", "").strip()]
        for index, key in enumerate(keys, 1):
            try:
                account = Account.from_key(key)
                address = Web3.to_checksum_address(account.address)
                self.store.add_wallet(
                    name=f"env-wallet-{index}", address=address, private_key=key,
                    quantity=self.quantity_default, chains=tuple(self.enabled_chains),
                )
            except Exception as exc:
                log.warning("Skipping invalid env wallet %s: %s", index, exc)

    def reload_wallets(self) -> None:
        self.wallets = [
            WalletConfig(
                name=w.name, private_key=w.private_key, address=Web3.to_checksum_address(w.address),
                quantity=w.quantity, chains=tuple(normalize_chain(x) for x in w.chains),
            )
            for w in self.store.list_wallets(enabled_only=True)
        ]
        self.sync_wallets_into_candidates()

    def project_key_for_candidate(self, candidate: Candidate) -> str:
        identity = candidate.contract_address.lower() if candidate.contract_address else candidate.slug.lower()
        return f"{candidate.chain}:{identity}"

    def auto_target_for_limit(self, limit: int | None, remaining: int | None = None) -> int:
        """V4.4 automatic quantity policy.

        Use the stage's per-wallet allowance when it is <= 100. If the
        allowance is greater than 100 (or is effectively unlimited/unknown),
        cap the automatic target at 30 by default. Remaining global supply is
        an additional upper bound.
        """
        if limit is None or int(limit) <= 0:
            target = self.auto_stage_default_quantity
        elif int(limit) > self.auto_stage_high_limit_threshold:
            target = self.auto_stage_high_limit_quantity
        else:
            target = int(limit)
        target = max(1, min(target, 100))
        if remaining is not None:
            target = min(target, max(0, int(remaining)))
        return max(0, target)

    def confirmed_total_for_wallet(self, candidate: Candidate, wallet_address: str) -> int:
        return self.store.confirmed_quantity_for_target(
            candidate.slug,
            wallet_address,
            chain=candidate.chain,
            contract_address=candidate.contract_address,
        )

    def current_plan_for_candidate(self, candidate: Candidate, now: float | None = None) -> dict[str, Any] | None:
        now = time.time() if now is None else now
        active = active_plans(candidate.stage_plans, now)
        if active:
            # OpenSea chooses the first eligible active stage. Preserve stage
            # order so our scheduling mirrors the drop metadata as closely as
            # possible; the mint builder remains the final eligibility oracle.
            return min(active, key=lambda p: int(p.get("index") or 0))
        return None

    def next_plan_for_candidate(self, candidate: Candidate, now: float | None = None) -> dict[str, Any] | None:
        return next_future_plan(candidate.stage_plans, now)

    def candidate_probe_time(self, candidate: Candidate) -> float:
        now = time.time()
        plan = self.current_plan_for_candidate(candidate, now)
        if plan:
            return now
        future = self.next_plan_for_candidate(candidate, now)
        if future and future.get("start") is not None:
            lead = self.public_preopen_window_seconds if future.get("is_public") else self.preopen_probe_seconds
            return max(now, float(future["start"]) - lead)
        target = candidate.next_stage_start or candidate.public_start
        return max(now, (target or now) - self.preopen_probe_seconds)

    def sync_wallets_into_candidates(self) -> None:
        now = time.time()
        current = {w.address.lower(): w for w in self.wallets}
        for candidate in self.candidates.values():
            for address in list(candidate.wallets):
                if address not in current:
                    state = candidate.wallets[address]
                    # A disabled wallet is removed from future execution, but a
                    # transaction already broadcast must still have its receipt
                    # tracked to completion.
                    if state.submitted and not state.confirmed and not state.final:
                        continue
                    candidate.wallets.pop(address, None)

            for wallet in self.wallets:
                key = wallet.address.lower()
                if not wallet.supports_chain(candidate.chain):
                    continue
                confirmed_total = self.confirmed_total_for_wallet(candidate, wallet.address)
                latest = self.store.latest_mint_record(
                    candidate.slug, wallet.address, chain=candidate.chain,
                    contract_address=candidate.contract_address,
                )

                if key in candidate.wallets:
                    state = candidate.wallets[key]
                    state.wallet = wallet
                    state.confirmed_total = confirmed_total
                    # Rebuild a pending tx after a process restart when the last
                    # durable event was "submitted".
                    if latest and str(latest.get("status")) == "submitted" and latest.get("tx_hash") and not state.submitted:
                        state.submitted = True
                        state.tx_hash = str(latest["tx_hash"])
                        state.pending_quantity = int(latest.get("quantity") or 0)
                        state.stage_key = str(latest.get("stage_key") or state.stage_key or "")
                        state.stage_label = str(latest.get("stage_label") or state.stage_label or "")
                        state.receipt_next_check = now
                    continue

                state = WalletState(
                    wallet=wallet,
                    quantity=1,
                    next_attempt=self.candidate_probe_time(candidate),
                    confirmed_total=confirmed_total,
                )
                if latest and str(latest.get("status")) == "submitted" and latest.get("tx_hash"):
                    state.submitted = True
                    state.tx_hash = str(latest["tx_hash"])
                    state.pending_quantity = int(latest.get("quantity") or 0)
                    state.stage_key = str(latest.get("stage_key") or "")
                    state.stage_label = str(latest.get("stage_label") or "")
                    state.receipt_next_check = now
                candidate.wallets[key] = state

                if not candidate.auto_discovered and candidate.watch_kind == "manual":
                    self.notify_all(
                        f"➕ تمت إضافة المحفظة «{wallet.name}» إلى مراقبة منت نشط\n"
                        f"العنوان: {short_address(wallet.address)}\n"
                        f"المشروع: {candidate.slug}\nالشبكة: {chain_label(candidate.chain)}\n"
                        "سيتم فحص أهليتها عند كل مرحلة جديدة، والـMint المجاني سيُنفذ تلقائيًا إذا كانت مؤهلة."
                    )

    def validate_wallet_name(self, name: str, *, exclude_id: int | None = None) -> tuple[bool, str]:
        clean = " ".join(name.strip().split())
        if len(clean) < 2 or len(clean) > 32:
            return False, "اسم المحفظة يجب أن يكون بين حرفين و32 حرفًا."
        if clean.startswith("/"):
            return False, "اسم المحفظة لا يمكن أن يبدأ بعلامة /."
        if self.store.wallet_name_exists(clean, exclude_id=exclude_id):
            return False, "هذا الاسم مستخدم لمحفظة أخرى. اختر اسمًا مختلفًا."
        return True, clean

    def add_wallet_key(self, name: str, private_key: str) -> tuple[bool, str]:
        ok_name, clean_or_error = self.validate_wallet_name(name)
        if not ok_name:
            return False, clean_or_error
        clean_name = clean_or_error
        try:
            account = Account.from_key(private_key.strip())
            address = Web3.to_checksum_address(account.address)
        except Exception:
            return False, "المفتاح الخاص غير صالح لمحفظة EVM."
        existing = self.store.get_wallet_by_address(address)
        if existing:
            return False, f"هذه المحفظة مضافة مسبقًا باسم «{existing.name}»: {short_address(address)}"
        try:
            self.store.add_wallet(
                name=clean_name, address=address, private_key=private_key.strip(),
                quantity=self.quantity_default, chains=tuple(self.enabled_chains),
            )
        except ValueError as exc:
            return False, str(exc)
        self.reload_wallets()
        return True, (
            f"✅ تمت إضافة المحفظة «{clean_name}» بنجاح وتخزين مفتاحها مشفرًا.\n"
            f"العنوان: {address}\n"
            f"الحالة: 🟢 نشطة\n"
            f"كمية المنت الافتراضية اليدوية: {self.quantity_default}\n"
            f"في المنتات التلقائية بالمراحل تُستخدم كمية المرحلة نفسها، وإذا تجاوز الحد {self.auto_stage_high_limit_threshold} فالهدف {self.auto_stage_high_limit_quantity}.\n"
            "ستدخل تلقائيًا في جميع فرص الـMint المجانية المناسبة على الشبكات المفعلة."
        )

    # ---------- RPC ----------
    def rpc_urls_for_chain(self, chain: str) -> list[str]:
        urls = csv_values(os.getenv(f"{chain.upper()}_RPC_URLS"))
        single = os.getenv(f"{chain.upper()}_RPC_URL", "").strip()
        if single:
            urls.append(single)
        alchemy_key = os.getenv("ALCHEMY_API_KEY", "").strip()
        generated = alchemy_rpc(chain, alchemy_key)
        if generated:
            urls.insert(0, generated)
        urls.extend(default_rpcs(chain))
        output: list[str] = []
        for url in urls:
            if url and url not in output:
                output.append(url)
        return output

    def load_rpc_pools(self) -> None:
        for chain in self.enabled_chains:
            urls = self.rpc_urls_for_chain(chain)
            if not urls:
                log.warning("No RPC for %s", chain)
                continue
            try:
                pool = RpcPool(
                    chain, urls,
                    timeout=env_float("RPC_TIMEOUT", 5.0),
                    broadcast_workers=env_int("RPC_BROADCAST_WORKERS", 4),
                )
                self.rpc_pools[chain] = pool
                log.info("RPC %s ready | primary=%s | verified=%s", chain, pool.primary_url, len(pool.urls))
            except Exception as exc:
                log.warning("RPC pool disabled for %s: %s", chain, exc)

    def max_gas_for_chain(self, chain: str) -> Decimal:
        raw = os.getenv(f"{chain.upper()}_MAX_GAS_NATIVE", "").strip()
        if raw:
            try:
                return Decimal(raw)
            except InvalidOperation:
                pass
        return self.max_gas_native

    def max_gas_usd_for_chain(self, chain: str) -> Decimal:
        raw = os.getenv(f"{chain.upper()}_MAX_GAS_USD", "").strip()
        if raw:
            try:
                return Decimal(raw)
            except InvalidOperation:
                pass
        return self.max_gas_usd

    def native_usd_price_for_chain(self, chain: str) -> Decimal | None:
        return self.price_oracle.get_usd(native_symbol(chain))

    # ---------- drop parsing / watch ----------
    def load_env_watches_to_store(self) -> None:
        for raw in csv_values(os.getenv("WATCH_URLS")) + csv_values(os.getenv("WATCH_SLUGS")):
            slug, chain = slug_from_text(raw)
            if slug:
                self.store.upsert_watch(
                    slug=slug, chain=chain or "", source=raw,
                    allow_paid=self.allow_paid_default,
                    max_mint_price_native=str(self.max_mint_price_default), quantity=None,
                )

    def detect_chain(self, slug: str, drop: dict[str, Any], forced_chain: str | None = None) -> str | None:
        if forced_chain:
            chain = normalize_chain(forced_chain)
            return chain if chain in self.enabled_chains else None
        for chain in extract_known_chains(drop):
            if chain in self.enabled_chains:
                return chain
        try:
            collection = self.opensea.get_collection(slug)
            for chain in extract_known_chains(collection):
                if chain in self.enabled_chains:
                    return chain
        except Exception:
            pass
        for chain in self.enabled_chains:
            for drop_type in ("upcoming", "featured", "recently_minted"):
                try:
                    payload = self.opensea.get_drops(drop_type, opensea_chain_name(chain), self.drop_limit)
                    if slug in {get_slug(x) for x in find_list(payload, "drops", "results", "items")}:
                        return chain
                except Exception:
                    continue
        return next(iter(self.rpc_pools)) if len(self.rpc_pools) == 1 else None

    def apply_stage_plans(self, candidate: Candidate, plans: list[dict[str, Any]], drop: dict[str, Any] | None = None) -> None:
        now = time.time()
        candidate.stage_plans = plans
        current = self.current_plan_for_candidate(candidate, now)
        future = self.next_plan_for_candidate(candidate, now)
        candidate.current_stage_key = str(current.get("key") or "") if current else ""
        candidate.current_stage_label = str(current.get("label") or "") if current else ""
        candidate.current_stage_start = float(current["start"]) if current and current.get("start") is not None else None
        candidate.current_stage_end = float(current["end"]) if current and current.get("end") is not None else None
        candidate.current_stage_public = bool(current.get("is_public")) if current else False
        candidate.current_stage_free = bool(current.get("is_free")) if current else False
        candidate.current_stage_paid = bool(current.get("is_paid")) if current else False
        candidate.current_stage_limit = int(current["wallet_limit"]) if current and current.get("wallet_limit") else None
        candidate.next_stage_start = float(future["start"]) if future and future.get("start") is not None else (now if current else None)

        remaining_publics = [p for p in plans if p.get("is_public") and (p.get("end") is None or float(p["end"]) > now)]
        candidate.public_start = min(
            (float(p["start"]) for p in remaining_publics if p.get("start") is not None),
            default=(now if any(plan_is_active(p, now) for p in remaining_publics) else None),
        )
        final_public = final_public_plan(plans)
        candidate.final_public_start = float(final_public["start"]) if final_public and final_public.get("start") is not None else None
        ends = [float(p["end"]) for p in plans if p.get("end") is not None]
        candidate.final_stage_end = max(ends) if ends else None
        candidate.wallet_limit = candidate.current_stage_limit
        if drop is not None:
            candidate.remaining_supply = remaining_supply(drop)
        candidate.has_paid_stage = any(bool(p.get("is_paid")) for p in plans)
        candidate.qualification_tracked = has_qualification_stages(plans)
        candidate.stage_lines = [
            f"• {p['label']}: {format_ts(p.get('start'), self.display_tz)}"
            f" → {format_ts(p.get('end'), self.display_tz)}"
            f" | {'عام' if p.get('is_public') else 'تأهيل'}"
            f" | السعر={p.get('price','غير معروف')}"
            + (f" | الحد/المحفظة={p.get('wallet_limit')}" if p.get('wallet_limit') else "")
            for p in plans
        ]

    def persist_candidate_planning(self, candidate: Candidate) -> None:
        stages_json = json.dumps(candidate.stage_plans, ensure_ascii=False)
        existing_watch = self.store.get_watch(candidate.slug)
        if existing_watch:
            self.store.update_watch_stage_metadata(
                candidate.slug,
                stages_json=stages_json,
                next_stage_start=candidate.next_stage_start,
                public_start=candidate.public_start,
                last_stage_key=candidate.current_stage_key or None,
                last_stage_label=candidate.current_stage_label or None,
            )
        if candidate.qualification_tracked:
            self.store.upsert_qualification_project(
                project_key=self.project_key_for_candidate(candidate),
                slug=candidate.slug,
                chain=candidate.chain,
                contract_address=candidate.contract_address,
                source=candidate.source,
                stages=candidate.stage_plans,
                next_stage_start=candidate.next_stage_start,
                public_start=candidate.public_start,
                final_stage_end=candidate.final_stage_end,
                last_stage_key=candidate.current_stage_key or None,
                last_stage_label=candidate.current_stage_label or None,
                last_stage_start=candidate.current_stage_start,
                last_stage_end=candidate.current_stage_end,
            )

    def stage_is_relevant_within_horizon(self, plans: list[dict[str, Any]]) -> bool:
        now = time.time()
        horizon = now + self.stage_discovery_horizon_seconds
        for plan in plans:
            start = plan.get("start")
            end = plan.get("end")
            if plan_is_active(plan, now):
                return True
            if start is not None and now < float(start) <= horizon:
                return True
            if start is None and (end is None or float(end) > now):
                return True
        return False

    def summarize_stages(self, drop: dict[str, Any]) -> tuple[list[str], float | None, float | None, int | None, bool]:
        now = time.time()
        lines: list[str] = []
        public_start = None
        next_start = None
        wallet_limit = None
        has_paid_stage = False
        for stage in get_stages(drop):
            start = stage_start(stage)
            end = stage_end(stage)
            label = stage_label(stage)
            price = extract_price_hint(stage)
            if stage_has_paid_price(stage):
                has_paid_stage = True
            limit = max_per_wallet(stage)
            if limit:
                wallet_limit = limit if wallet_limit is None else min(wallet_limit, limit)
            if is_public_stage(stage) and (end is None or end > now):
                if public_start is None or (start is not None and start < public_start):
                    public_start = start
            if end is None or end > now:
                # An already-open allowlist/public phase is actionable immediately.
                if start is None or start <= now:
                    effective = now
                else:
                    effective = start
                if next_start is None or effective < next_start:
                    next_start = effective
            lines.append(
                f"• {label}: {format_ts(start, self.display_tz)} | السعر={price}"
                + (f" | الحد/المحفظة={limit}" if limit else "")
            )
        return lines, public_start, next_start, wallet_limit, has_paid_stage

    def register_drop(
        self,
        slug: str,
        drop: dict[str, Any],
        chain_hint: str | None,
        source: str,
        *,
        allow_paid: bool,
        max_mint_price_native: Decimal,
        quantity_override: int | None = None,
        auto_discovered: bool = False,
    ) -> tuple[bool, str, Candidate | None]:
        chain = self.detect_chain(slug, drop, chain_hint)
        if not chain:
            return False, f"تعذر تحديد شبكة {slug}. استخدم /watch <network> <الرابط> عند الحاجة.", None
        if chain not in self.rpc_pools:
            return False, f"لا يوجد RPC يعمل حاليًا لشبكة {chain_label(chain)} الخاصة بالمشروع {slug}.", None
        plans = build_stage_plans(drop)
        if not plans:
            return False, f"لم تُرجع OpenSea أي مراحل Mint للمشروع {slug}.", None

        lines, public_start, next_start, _wallet_limit, has_paid_stage = self.summarize_stages(drop)
        key = f"{chain}:{slug}"
        candidate = self.candidates.get(key)
        now = time.time()
        discovered_contract = extract_contract_address(drop)

        if candidate is None:
            candidate = Candidate(
                slug=slug, chain=chain, source=source, allow_paid=allow_paid,
                max_mint_price_native=max_mint_price_native, quantity_override=quantity_override,
                auto_discovered=auto_discovered,
                last_seen_auto=now if auto_discovered else 0.0,
                contract_address=discovered_contract,
                watch_kind="auto_stage" if auto_discovered and has_qualification_stages(plans) else ("auto_free" if auto_discovered else "manual"),
                discovery_source="drops" if auto_discovered else "manual-link",
            )
            self.candidates[key] = candidate
        elif auto_discovered and not candidate.auto_discovered:
            # A manually watched project keeps its manual execution policy but
            # still receives fresh stage metadata from automatic discovery.
            candidate.last_seen_auto = now
        else:
            candidate.source = source
            candidate.allow_paid = allow_paid
            candidate.max_mint_price_native = max_mint_price_native
            candidate.quantity_override = quantity_override
            if discovered_contract and not candidate.contract_address:
                candidate.contract_address = discovered_contract
            if not auto_discovered:
                candidate.auto_discovered = False
                candidate.watch_kind = "manual"
            elif has_qualification_stages(plans):
                candidate.watch_kind = "auto_stage"

        if auto_discovered:
            candidate.last_seen_auto = now
        self.apply_stage_plans(candidate, plans, drop)
        candidate.has_paid_stage = has_paid_stage or candidate.has_paid_stage
        if not candidate.auto_discovered:
            candidate.paid_detected = candidate.paid_detected or candidate.has_paid_stage

        # Resolve the contract opportunistically. It strengthens duplicate
        # protection and cumulative-quantity accounting, but failure is not
        # fatal because OpenSea's builder can still mint by slug.
        if not candidate.contract_address:
            try:
                resolved_chain, resolved_contract = self.resolve_collection_contract(slug, chain)
                if resolved_chain == chain and resolved_contract:
                    candidate.contract_address = resolved_contract
            except Exception:
                pass

        self.sync_wallets_into_candidates()
        candidate.next_refresh = now + self._refresh_interval(candidate)
        probe_at = self.candidate_probe_time(candidate)
        for state in candidate.wallets.values():
            if not state.submitted:
                state.next_attempt = min(state.next_attempt or probe_at, probe_at)

        self.persist_candidate_planning(candidate)
        paid_text = "مسموح فقط بعد تأكيدك واختيار المحافظ والكميات" if allow_paid else "مجاني فقط"
        kind_text = f"{len(plans)} مرحلة" + (" — يحتوي مراحل تأهيل" if has_qualification_stages(plans) else "")
        message = (
            f"🎯 بدأت مراقبة: {slug}\n"
            f"الشبكة: {chain_label(chain)}\n"
            f"المراحل: {kind_text}\n"
            f"موعد أقرب Public: {format_ts(candidate.public_start, self.display_tz)}\n"
            f"المحافظ النشطة المناسبة للشبكة: {len(candidate.wallets)}\n"
            f"المنت المدفوع: {paid_text}\n\n"
            + "\n".join(candidate.stage_lines[:10])
        )
        return True, message, candidate

    def _refresh_interval(self, candidate: Candidate) -> float:
        targets = [x for x in (candidate.next_stage_start, candidate.public_start) if x is not None]
        remaining = min((x - time.time() for x in targets), default=999999)
        return self.fast_stage_refresh_seconds if remaining <= self.fast_refresh_window else self.stage_refresh_seconds

    def add_watch(self, raw: str, forced_chain: str | None = None, persist: bool = True) -> tuple[bool, str, Candidate | None]:
        slug, chain_from_input = slug_from_text(raw)
        if not slug:
            return False, "تعذر استخراج اسم المجموعة/الـDrop من رابط OpenSea.", None
        chain_hint = normalize_chain(forced_chain) if forced_chain else chain_from_input
        try:
            drop = self.opensea.get_drop(slug)
            ok, message, candidate = self.register_drop(
                slug, drop, chain_hint, raw,
                allow_paid=self.allow_paid_default,
                max_mint_price_native=self.max_mint_price_default,
            )
        except Exception as drop_exc:
            chain, contract = self.resolve_collection_contract(slug, chain_hint)
            if not chain or not contract:
                return False, f"فشل جلب Drop ولم أتمكن من تحديد SeaDrop مباشر للمشروع {slug}: {drop_exc}", None
            ok, message, candidate = self.register_onchain_candidate(
                slug, chain, contract, raw,
                allow_paid=self.allow_paid_default,
                max_mint_price_native=self.max_mint_price_default,
                auto_discovered=False, discovery_source="manual-link", allow_unconfigured=True,
            )
        if ok and persist and candidate:
            candidate.watch_kind = "manual"
            self.store.upsert_watch(
                slug=slug, chain=candidate.chain, source=raw,
                allow_paid=candidate.allow_paid,
                max_mint_price_native=str(candidate.max_mint_price_native),
                quantity=candidate.quantity_override,
                mint_backend=candidate.mint_backend,
                contract_address=candidate.contract_address,
                watch_kind="manual",
                stages_json=json.dumps(candidate.stage_plans, ensure_ascii=False),
                next_stage_start=candidate.next_stage_start,
                public_start=candidate.public_start,
            )
            self.persist_candidate_planning(candidate)
        return ok, message, candidate

    def remove_watch(self, slug_raw: str) -> str:
        slug, _ = slug_from_text(slug_raw)
        if not slug:
            return "اسم المجموعة غير صالح."
        self.store.remove_watch(slug)
        removed = 0
        for key in list(self.candidates):
            if self.candidates[key].slug.lower() == slug.lower():
                self.candidates.pop(key, None); removed += 1
        return f"🗑 تم إيقاف مراقبة {slug}. عدد المراقبات المحذوفة: {removed}."

    def bootstrap_watches(self) -> None:
        for row in self.store.list_watches():
            if STOP:
                break
            slug = str(row["slug"])
            try:
                chain_hint = str(row.get("chain") or "") or None
                source = str(row.get("source") or slug)
                stored_backend = str(row.get("mint_backend") or "opensea")
                stored_contract = str(row.get("contract_address") or "").strip()
                try:
                    if stored_backend == "seadrop" and stored_contract and Web3.is_address(stored_contract) and chain_hint:
                        ok, _message, candidate = self.register_onchain_candidate(
                            slug, normalize_chain(chain_hint), stored_contract, source,
                            allow_paid=bool(row.get("allow_paid", 1)),
                            max_mint_price_native=Decimal(str(row.get("max_mint_price_native") or "0")),
                            quantity_override=row.get("quantity"), auto_discovered=False, discovery_source="restored",
                            allow_unconfigured=True,
                        )
                    else:
                        drop = self.opensea.get_drop(slug)
                        ok, _message, candidate = self.register_drop(
                            slug, drop, chain_hint, source,
                            allow_paid=bool(row.get("allow_paid", 1)),
                            max_mint_price_native=Decimal(str(row.get("max_mint_price_native") or "0")),
                            quantity_override=row.get("quantity"),
                        )
                except Exception:
                    chain, contract = self.resolve_collection_contract(slug, chain_hint)
                    if not chain or not contract:
                        raise
                    ok, _message, candidate = self.register_onchain_candidate(
                        slug, chain, contract, source,
                        allow_paid=bool(row.get("allow_paid", 1)),
                        max_mint_price_native=Decimal(str(row.get("max_mint_price_native") or "0")),
                        quantity_override=row.get("quantity"), auto_discovered=False, discovery_source="restored",
                        allow_unconfigured=True,
                    )
                if ok and candidate:
                    candidate.watch_kind = str(row.get("watch_kind") or "manual")
                    candidate.auto_discovered = candidate.watch_kind.startswith("auto_")
                    candidate.paid_detected = bool(row.get("paid_detected", 0)) or candidate.has_paid_stage
                    candidate.paid_selection_confirmed = bool(row.get("paid_selection_confirmed", 0))
                    candidate.paid_decision = str(row.get("paid_decision") or ("confirmed" if candidate.paid_selection_confirmed else ""))
                    candidate.paid_stage_key = str(row.get("paid_stage_key") or "")
                    candidate.paid_stage_start = float(row["paid_stage_start"]) if row.get("paid_stage_start") is not None else None
                    try:
                        saved = json.loads(str(row.get("paid_wallets_json") or "[]"))
                        candidate.paid_wallet_addresses = {str(a).lower() for a in saved if isinstance(a, str)}
                    except Exception:
                        candidate.paid_wallet_addresses = set()
                    try:
                        qtys = json.loads(str(row.get("paid_wallet_quantities_json") or "{}"))
                        candidate.paid_wallet_quantities = {str(a).lower(): max(1, min(int(q), 100)) for a, q in qtys.items()} if isinstance(qtys, dict) else {}
                    except Exception:
                        candidate.paid_wallet_quantities = {}
                    self.persist_candidate_planning(candidate)
            except Exception as exc:
                log.warning("Could not restore watch %s: %s", slug, exc)

    def refresh_candidate(self, candidate: Candidate) -> None:
        if time.time() < candidate.next_refresh:
            return
        if candidate.mint_backend == "seadrop":
            self.refresh_onchain_candidate(candidate)
            return
        # One-off free candidates are refreshed by discovery. Persistent
        # multi-stage auto watches refresh themselves too so a stage transition
        # is not missed even if the drop falls out of the discovery lists.
        if candidate.auto_discovered and candidate.watch_kind == "auto_free":
            return
        try:
            drop = self.opensea.get_drop(candidate.slug)
            self.register_drop(
                candidate.slug, drop, candidate.chain, candidate.source,
                allow_paid=candidate.allow_paid,
                max_mint_price_native=candidate.max_mint_price_native,
                quantity_override=candidate.quantity_override,
            )
        except Exception as exc:
            log.debug("Refresh %s failed: %s", candidate.slug, exc)
            candidate.next_refresh = time.time() + self.fast_stage_refresh_seconds

    # ---------- V4.3 collection/on-chain SeaDrop resolution ----------
    def resolve_collection_contract(self, slug: str, chain_hint: str | None = None) -> tuple[str | None, str | None]:
        hint = normalize_chain(chain_hint) if chain_hint else None
        try:
            collection = self.opensea.get_collection(slug)
        except Exception:
            collection = {}
        candidates = collection_contract_candidates(collection)
        # First prefer explicit/matching chain metadata.
        for chain, address in candidates:
            if hint and chain == hint and chain in self.rpc_pools:
                return chain, address
        for chain, address in candidates:
            if chain in self.rpc_pools:
                return chain, address
        # Some OpenSea collection payloads omit the chain beside the contract.
        # Probe only configured chains and only accept a contract with a real
        # configured SeaDrop public stage.
        for _chain, address in candidates:
            for chain in ([hint] if hint else self.enabled_chains):
                if not chain or chain not in self.rpc_pools:
                    continue
                public = read_seadrop_public_drop(self.rpc_pools[chain].primary, address)
                if public and public.get("configured"):
                    return chain, address
        return None, None

    def resolve_slug_from_contract(self, chain: str, contract_address: str) -> str | None:
        try:
            payload = self.opensea.get_contract(opensea_chain_name(chain), contract_address)
            return get_slug(payload) or extract_event_slug(payload)
        except Exception:
            return None

    def register_onchain_candidate(
        self,
        slug: str,
        chain: str,
        contract_address: str,
        source: str,
        *,
        allow_paid: bool,
        max_mint_price_native: Decimal,
        quantity_override: int | None = None,
        auto_discovered: bool = False,
        discovery_source: str = "",
        allow_unconfigured: bool = False,
    ) -> tuple[bool, str, Candidate | None]:
        chain = normalize_chain(chain)
        if chain not in self.rpc_pools:
            return False, f"لا يوجد RPC يعمل لشبكة {chain_label(chain)}.", None
        if not Web3.is_address(contract_address):
            return False, "تعذر تحديد عقد الـNFT بصورة صحيحة.", None
        contract_address = Web3.to_checksum_address(contract_address)
        public = read_seadrop_public_drop(self.rpc_pools[chain].primary, contract_address)
        if not public:
            return False, "تعذر قراءة SeaDrop لهذا العقد على السلسلة.", None
        configured = bool(public.get("configured"))
        if not configured and not allow_unconfigured:
            return False, "لم أجد Public SeaDrop مهيأ لهذا العقد على السلسلة.", None

        price_wei = int(public.get("mint_price_wei") or 0)
        start_time = float(public.get("start_time") or 0) or None
        end_time = float(public.get("end_time") or 0) or None
        limit = int(public.get("max_per_wallet")) if public.get("max_per_wallet") else None
        remaining = int(public.get("remaining_supply")) if public.get("remaining_supply") is not None else None
        now = time.time()
        key = f"{chain}:{slug}"
        candidate = self.candidates.get(key)

        contract_key = contract_address.lower()
        contract_match_key = None
        if candidate is None:
            for existing_key, existing in self.candidates.items():
                if existing.chain == chain and existing.contract_address and existing.contract_address.lower() == contract_key:
                    candidate = existing
                    contract_match_key = existing_key
                    break

        if candidate is None:
            candidate = Candidate(
                slug=slug, chain=chain, source=source, allow_paid=allow_paid,
                max_mint_price_native=max_mint_price_native, quantity_override=quantity_override,
                auto_discovered=auto_discovered, mint_backend="seadrop",
                contract_address=contract_address,
                last_seen_auto=now if auto_discovered else 0.0,
                discovery_source=discovery_source or ("auto" if auto_discovered else "manual"),
                watch_kind="auto_free" if auto_discovered else "manual",
            )
            self.candidates[key] = candidate
        elif auto_discovered and not candidate.auto_discovered:
            candidate.last_seen_auto = now
            return True, "المشروع موجود أصلًا ضمن مراقبة يدوية.", candidate
        else:
            if not auto_discovered and contract_match_key and contract_match_key != key:
                self.candidates.pop(contract_match_key, None)
                candidate.slug = slug
                self.candidates[key] = candidate
            candidate.source = source
            candidate.allow_paid = allow_paid
            candidate.max_mint_price_native = max_mint_price_native
            candidate.quantity_override = quantity_override
            candidate.mint_backend = "seadrop"
            candidate.contract_address = contract_address
            candidate.discovery_source = discovery_source or candidate.discovery_source
            if not auto_discovered:
                candidate.auto_discovered = False
                candidate.watch_kind = "manual"

        candidate.remaining_supply = remaining
        if configured:
            plan = {
                "key": hashlib.sha1(f"seadrop|{contract_key}|{start_time}|{end_time}|{price_wei}|{limit}".encode()).hexdigest()[:16],
                "index": 0,
                "label": "Public SeaDrop",
                "start": start_time,
                "end": end_time,
                "is_public": True,
                "is_free": price_wei == 0,
                "is_paid": price_wei > 0,
                "wallet_limit": limit,
                "price": str(Decimal(price_wei) / Decimal(10**18)),
            }
            self.apply_stage_plans(candidate, [plan], None)
            candidate.stage_end = end_time
        else:
            candidate.stage_plans = []
            candidate.stage_lines = ["• Public SeaDrop: لم يتم تهيئته بعد — ستستمر المراقبة حتى يظهر الإعداد على السلسلة."]
            candidate.public_start = None
            candidate.next_stage_start = None
            candidate.current_stage_key = ""
            candidate.current_stage_label = ""
            candidate.wallet_limit = None
            candidate.has_paid_stage = False

        if not candidate.auto_discovered and candidate.has_paid_stage:
            candidate.paid_detected = True
        if auto_discovered:
            candidate.last_seen_auto = now
        candidate.next_refresh = now + self._refresh_interval(candidate)
        self.sync_wallets_into_candidates()
        probe = self.candidate_probe_time(candidate)
        for state in candidate.wallets.values():
            if not state.submitted:
                state.next_attempt = min(state.next_attempt or probe, probe)
        self.persist_candidate_planning(candidate)

        policy = "المدفوع لا يُنفذ إلا بعد تأكيدك" if allow_paid else "مجاني فقط"
        message = (
            f"🎯 بدأت مراقبة: {slug}\n"
            f"الشبكة: {chain_label(chain)}\n"
            f"المصدر: SeaDrop مباشر على السلسلة\n"
            f"العقد: {contract_address}\n"
            f"موعد الـPublic: {format_ts(candidate.public_start, self.display_tz)}\n"
            f"السياسة: {policy}\n"
            + candidate.stage_lines[0]
        )
        return True, message, candidate

    def refresh_onchain_candidate(self, candidate: Candidate) -> None:
        if not candidate.contract_address or candidate.chain not in self.rpc_pools:
            return
        public = read_seadrop_public_drop(self.rpc_pools[candidate.chain].primary, candidate.contract_address)
        if not public or not public.get("configured"):
            candidate.next_refresh = time.time() + self.fast_stage_refresh_seconds
            return
        price_wei = int(public.get("mint_price_wei") or 0)
        start_time = float(public.get("start_time") or 0) or None
        end_time = float(public.get("end_time") or 0) or None
        limit = int(public.get("max_per_wallet")) if public.get("max_per_wallet") else None
        candidate.remaining_supply = int(public.get("remaining_supply")) if public.get("remaining_supply") is not None else None
        plan = {
            "key": hashlib.sha1(f"seadrop|{candidate.contract_address.lower()}|{start_time}|{end_time}|{price_wei}|{limit}".encode()).hexdigest()[:16],
            "index": 0,
            "label": "Public SeaDrop",
            "start": start_time,
            "end": end_time,
            "is_public": True,
            "is_free": price_wei == 0,
            "is_paid": price_wei > 0,
            "wallet_limit": limit,
            "price": str(Decimal(price_wei) / Decimal(10**18)),
        }
        self.apply_stage_plans(candidate, [plan], None)
        candidate.stage_end = end_time
        if not candidate.auto_discovered and price_wei > 0:
            candidate.paid_detected = True
        candidate.next_refresh = time.time() + self._refresh_interval(candidate)
        self.sync_wallets_into_candidates()
        self.persist_candidate_planning(candidate)

    def check_link_eligibility(self, raw: str, forced_chain: str | None = None) -> str:
        slug, chain_from_input = slug_from_text(raw)
        if not slug:
            return "⚠️ تعذر استخراج اسم المجموعة من الرابط. أرسل رابط OpenSea للمجموعة/الـMint."
        hint = normalize_chain(forced_chain) if forced_chain else chain_from_input
        active_wallets = self.store.list_wallets(enabled_only=True)
        if not active_wallets:
            return "⚠️ لا توجد محافظ نشطة لفحص أهليتها."

        # Prefer OpenSea's mint builder when this is a registered Drop because it
        # understands allowlists/server-signed stages. Fall back to direct public
        # SeaDrop when /drops/{slug} doesn't exist.
        try:
            drop = self.opensea.get_drop(slug)
            chain = self.detect_chain(slug, drop, hint)
            if chain and chain in self.rpc_pools:
                lines = [f"🧪 فحص الأهلية — {slug} ({chain_label(chain)})", "المصدر: OpenSea Drop API"]
                configs = {w.address.lower(): w for w in self.wallets if w.supports_chain(chain)}
                for stored in active_wallets:
                    wallet = configs.get(stored.address.lower())
                    if not wallet:
                        continue
                    result = check_eligibility(self.opensea, slug, wallet, wallet.quantity)
                    icon = "✅" if result.eligible is True else "❌" if result.eligible is False else "⏳"
                    qty = f" | الكمية المتاحة={result.quantity_used}" if result.quantity_used else ""
                    price = f" | السعر={result.mint_value_native} {native_symbol(chain)}" if result.mint_value_native is not None else ""
                    lines.append(f"{icon} {wallet.name}: {eligibility_label(result.status)}{qty}{price}")
                return "\n".join(lines)[:3900]
        except Exception:
            pass

        chain, contract = self.resolve_collection_contract(slug, hint)
        if not chain or not contract:
            return f"⚠️ لم أتمكن من تحديد Drop أو عقد SeaDrop للمشروع {slug}."
        pool = self.rpc_pools[chain]
        lines = [f"🧪 فحص الأهلية — {slug} ({chain_label(chain)})", "المصدر: SeaDrop مباشرة من السلسلة", f"العقد: {contract}"]
        configs = {w.address.lower(): w for w in self.wallets if w.supports_chain(chain)}
        for stored in active_wallets:
            wallet = configs.get(stored.address.lower())
            if not wallet:
                continue
            result = check_seadrop_eligibility(pool, wallet, contract, wallet.quantity)
            icon = "✅" if result.eligible is True else "❌" if result.eligible is False else "⏳"
            qty = f" | الكمية المتاحة={result.quantity_used}" if result.quantity_used else ""
            price = f" | القيمة={result.mint_value_native} {native_symbol(chain)}" if result.mint_value_native is not None else ""
            lines.append(f"{icon} {wallet.name}: {eligibility_label(result.status)}{qty}{price}")
        return "\n".join(lines)[:3900]

    def _queue_mint_event(self, payload: dict[str, Any], source: str) -> None:
        chain = extract_event_chain(payload)
        if chain and chain not in self.enabled_chains:
            return
        contract = extract_contract_address(payload)
        slug = extract_event_slug(payload)
        if not contract and not slug:
            return
        key = f"{chain or 'unknown'}:{(contract or slug or '').lower()}"
        now = time.time()
        if now - self.auto_event_seen.get(key, 0.0) < 5.0:
            return
        self.auto_event_seen[key] = now
        # Keep the in-memory de-duplication map bounded during long Railway
        # uptimes. Persistent no-remint protection remains in SQLite.
        if len(self.auto_event_seen) > 5000:
            cutoff = now - 900.0
            self.auto_event_seen = {k: ts for k, ts in self.auto_event_seen.items() if ts >= cutoff}
        self.auto_discovery_queue.put({
            "kind": "mint_event", "source": source, "slug": slug,
            "chain_hint": chain, "contract_address": contract,
        })

    def _auto_mint_events_scan_once(self, deep: bool = False) -> None:
        if not self.auto_event_fallback_enabled:
            return
        before = int(time.time())
        after = max(0, (before - self.auto_event_initial_lookback_seconds) if deep else (self.auto_event_last_after - self.auto_event_overlap_seconds))
        pages = self.auto_event_initial_pages if deep else 1
        cursor = None
        any_success = False

        # OpenSea's global GET /events endpoint supports event_type/time/limit/next
        # but does not document a chain query parameter. Query globally once and
        # filter to ENABLED_CHAINS from each returned event.
        for _page in range(pages):
            try:
                payload = self.opensea.get_events(
                    event_type="mint", after=after, before=before,
                    limit=self.auto_event_limit, cursor=cursor,
                )
                for event in event_rows(payload):
                    self._queue_mint_event(event, "events")
                any_success = True
                cursor = pagination_cursor(payload)
                if not cursor:
                    break
            except Exception as exc:
                log.debug("OpenSea global mint-events fallback scan failed: %s", exc)
                break
        if any_success:
            self.auto_event_last_after = before

    def _process_auto_mint_event(self, item: dict[str, Any]) -> None:
        chain = normalize_chain(str(item.get("chain_hint") or ""))
        slug = str(item.get("slug") or "").strip() or None
        contract = str(item.get("contract_address") or "").strip() or None
        if contract and not Web3.is_address(contract):
            contract = None

        # Prefer Drop metadata first because it contains private/allowlist stage
        # schedules that getPublicDrop cannot expose.
        if slug:
            try:
                drop = self.opensea.get_drop(slug)
                resolved_chain = self.detect_chain(slug, drop, chain or None)
                if resolved_chain in self.rpc_pools:
                    plans = build_stage_plans(drop)
                    remain = remaining_supply(drop)
                    if plans and self.stage_is_relevant_within_horizon(plans) and not (remain is not None and remain <= 0):
                        ok, _message, candidate = self.register_drop(
                            slug, drop, resolved_chain, str(item.get("source") or "stream"),
                            allow_paid=True,
                            max_mint_price_native=self.max_mint_price_default,
                            auto_discovered=True,
                        )
                        if ok and candidate:
                            candidate.watch_kind = "auto_stage" if (has_qualification_stages(plans) or candidate.has_paid_stage or len(plans) > 1) else "auto_free"
                            if candidate.watch_kind == "auto_stage":
                                self.ensure_candidate_watch_persisted(candidate)
                            return
            except Exception:
                pass

        # Resolve chain/contract for drops that OpenSea does not expose through
        # GET /drops. This preserves V4.3's direct SeaDrop fallback.
        if chain not in self.rpc_pools and contract:
            for probe_chain in self.enabled_chains:
                pool = self.rpc_pools.get(probe_chain)
                if not pool:
                    continue
                public_probe = read_seadrop_public_drop(pool.primary, contract)
                if public_probe and public_probe.get("configured"):
                    chain = probe_chain
                    break
        if chain not in self.rpc_pools:
            return
        if not slug and contract:
            slug = self.resolve_slug_from_contract(chain, contract)
        if slug and not contract:
            resolved_chain, resolved_contract = self.resolve_collection_contract(slug, chain)
            if resolved_chain == chain:
                contract = resolved_contract
        if not slug:
            slug = f"contract-{contract[-10:].lower()}" if contract else None
        if not slug or not contract:
            return

        contract_l = contract.lower()
        existing = next((
            c for c in self.candidates.values()
            if c.slug.lower() == slug.lower()
            or (c.chain == chain and c.contract_address and c.contract_address.lower() == contract_l)
        ), None)
        if existing and not existing.auto_discovered:
            existing.last_seen_auto = time.time()
            return

        public = read_seadrop_public_drop(self.rpc_pools[chain].primary, contract)
        if not public or not public.get("configured"):
            return
        now = int(time.time())
        start = int(public.get("start_time") or 0)
        end = int(public.get("end_time") or 0)
        price = int(public.get("mint_price_wei") or 0)
        remaining = public.get("remaining_supply")
        if remaining is not None and int(remaining) <= 0:
            return
        active = (not start or now >= start) and (not end or now < end)
        future = bool(start and now < start)
        if not (active or future):
            return

        ok, _message, candidate = self.register_onchain_candidate(
            slug, chain, contract, str(item.get("source") or "stream"),
            allow_paid=(price > 0),
            max_mint_price_native=self.max_mint_price_default if price > 0 else Decimal("0"),
            auto_discovered=True,
            discovery_source=str(item.get("source") or "stream"),
        )
        if not ok or not candidate:
            return
        if price > 0 or future:
            candidate.watch_kind = "auto_stage"
            self.ensure_candidate_watch_persisted(candidate)
            self.maybe_offer_paid_public(candidate)
        else:
            candidate.watch_kind = "auto_free"

    async def _stream_loop(self) -> None:
        api_key = self.opensea.api_key
        url = f"wss://stream-api.opensea.io/socket/websocket?token={quote(api_key, safe='')}&vsn=2.0.0"
        ref = 1
        while not STOP:
            try:
                async with websockets.connect(url, ping_interval=None, open_timeout=15, close_timeout=5, max_size=4 * 1024 * 1024) as ws:
                    join_ref = str(ref); ref += 1
                    await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {"event_types": ["item_transferred"]}]))
                    log.info("OpenSea Stream connected | wildcard item_transferred")
                    last_heartbeat = time.time()
                    while not STOP:
                        if time.time() - last_heartbeat >= 25:
                            hb = str(ref); ref += 1
                            await ws.send(json.dumps([None, hb, "phoenix", "heartbeat", {}]))
                            last_heartbeat = time.time()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        try:
                            frame = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(frame, list) or len(frame) != 5:
                            continue
                        _join, _ref, _topic, event_name, wrapper = frame
                        if event_name != "item_transferred" or not isinstance(wrapper, dict):
                            continue
                        payload = wrapper.get("payload") if isinstance(wrapper.get("payload"), dict) else wrapper
                        if not event_is_zero_address_mint(payload):
                            continue
                        self._queue_mint_event(payload, "stream")
            except Exception as exc:
                if not STOP:
                    log.warning("OpenSea Stream disconnected: %s | retrying in 3s", exc)
                    await asyncio.sleep(3)

    def _stream_worker(self) -> None:
        try:
            asyncio.run(self._stream_loop())
        except Exception:
            if not STOP:
                log.exception("OpenSea Stream worker stopped unexpectedly")

    # ---------- automatic free-mint discovery ----------
    def _auto_free_scan_once(self, deep: bool = False) -> None:
        if not self.auto_free_enabled:
            return
        # V4.3 fallback catches mints that never appear in Drops lists.
        self._auto_mint_events_scan_once(deep=deep)
        chain_query = ",".join(opensea_chain_name(c) for c in self.enabled_chains)
        slugs: dict[str, str | None] = {}
        pages = self.auto_free_initial_pages if deep else 1

        for drop_type in self.auto_free_drop_types:
            cursor: str | None = None
            for _page in range(pages):
                if STOP:
                    return
                try:
                    payload = self.opensea.get_drops(
                        drop_type, chain_query, self.auto_free_drop_limit, cursor=cursor
                    )
                except Exception as exc:
                    log.debug("Auto-free list scan failed type=%s: %s", drop_type, exc)
                    break
                for item in find_list(payload, "drops", "results", "items"):
                    slug = get_slug(item)
                    if not slug:
                        continue
                    hints = [c for c in extract_known_chains(item) if c in self.enabled_chains]
                    slugs.setdefault(slug, hints[0] if hints else None)
                cursor = pagination_cursor(payload)
                if not cursor:
                    break

        if not slugs:
            return

        def fetch(item: tuple[str, str | None]):
            slug, hint = item
            try:
                drop = self.opensea.get_drop(slug)
                remain = remaining_supply(drop)
                plans = build_stage_plans(drop)
                is_free = has_active_free_stage(drop) and not (remain is not None and remain <= 0)
                relevant_stage = bool(plans) and self.stage_is_relevant_within_horizon(plans) and not (remain is not None and remain <= 0)
                hints = [c for c in extract_known_chains(drop) if c in self.enabled_chains]
                chain_hint = hints[0] if hints else hint
                return {
                    "slug": slug, "drop": drop, "chain_hint": chain_hint,
                    "active_free": is_free, "relevant_stage": relevant_stage,
                    "has_qualification": has_qualification_stages(plans),
                }
            except Exception as exc:
                log.debug("Auto-free detail scan failed %s: %s", slug, exc)
                return None

        workers = min(self.auto_free_detail_workers, len(slugs))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(fetch, item) for item in slugs.items()]
            for future in as_completed(futures):
                found = future.result()
                if found is not None:
                    self.auto_discovery_queue.put(found)

    def _auto_free_worker(self) -> None:
        deep = True
        while not STOP:
            started = time.time()
            try:
                self._auto_free_scan_once(deep=deep)
            except Exception:
                log.exception("Auto-free discovery scan failed")
            deep = False
            sleep_for = max(0.25, self.auto_free_scan_seconds - (time.time() - started))
            deadline = time.time() + sleep_for
            while not STOP and time.time() < deadline:
                time.sleep(min(0.5, deadline - time.time()))

    def start_auto_free_discovery(self) -> None:
        if not self.auto_free_enabled or self.auto_discovery_thread is not None:
            return
        self.auto_discovery_thread = threading.Thread(
            target=self._auto_free_worker,
            name="auto-free-discovery",
            daemon=True,
        )
        self.auto_discovery_thread.start()
        if self.auto_stream_enabled and self.auto_stream_thread is None:
            self.auto_stream_thread = threading.Thread(
                target=self._stream_worker, name="opensea-stream-discovery", daemon=True
            )
            self.auto_stream_thread.start()
        log.info(
            "Auto Free Mint enabled | scan every %.1fs | stream=%s | mint-events=%s | drops=%s",
            self.auto_free_scan_seconds, self.auto_stream_enabled, self.auto_event_fallback_enabled,
            ",".join(self.auto_free_drop_types),
        )

    def drain_auto_discovery(self) -> None:
        while True:
            try:
                item = self.auto_discovery_queue.get_nowait()
            except queue.Empty:
                break
            if item.get("kind") == "mint_event":
                self._process_auto_mint_event(item)
                continue
            slug = str(item["slug"])
            drop = item["drop"]
            hint = item.get("chain_hint")
            active_free = bool(item.get("active_free", False))
            relevant_stage = bool(item.get("relevant_stage", False))
            has_qualification = bool(item.get("has_qualification", False))

            existing = next((c for c in self.candidates.values() if c.slug.lower() == slug.lower()), None)
            if existing and not existing.auto_discovered:
                # Refresh its stage metadata without downgrading manual policy.
                self.register_drop(
                    slug, drop, existing.chain, existing.source,
                    allow_paid=existing.allow_paid,
                    max_mint_price_native=existing.max_mint_price_native,
                    quantity_override=existing.quantity_override,
                    auto_discovered=False,
                )
                continue

            if not active_free and not relevant_stage:
                if existing and existing.auto_discovered and existing.watch_kind == "auto_free":
                    pending = any(st.submitted and not st.confirmed and not st.final for st in existing.wallets.values())
                    if not pending:
                        self.candidates.pop(f"{existing.chain}:{existing.slug}", None)
                continue

            before = existing is not None
            # Multi-stage candidates need allow_paid=True only so a user-approved
            # future Public paid plan can execute. No paid transaction is signed
            # without candidate.paid_decision == confirmed.
            allow_paid = relevant_stage or has_qualification
            ok, _message, candidate = self.register_drop(
                slug, drop, hint, "auto-stage" if relevant_stage else "auto-free",
                allow_paid=allow_paid,
                max_mint_price_native=self.max_mint_price_default if allow_paid else Decimal("0"),
                auto_discovered=True,
            )
            if not ok or not candidate:
                continue
            candidate.last_seen_auto = time.time()
            if relevant_stage or has_qualification or candidate.has_paid_stage:
                candidate.watch_kind = "auto_stage"
                self.ensure_candidate_watch_persisted(candidate)
                self.persist_candidate_planning(candidate)
            else:
                candidate.watch_kind = "auto_free"

            if not before and self.auto_free_notify_discovery:
                self.notify_all(
                    f"{'🎟' if candidate.qualification_tracked else '🆓'} تم اكتشاف Mint تلقائيًا\n"
                    f"المشروع: {candidate.slug}\n"
                    f"الشبكة: {chain_label(candidate.chain)}\n"
                    f"عدد المراحل: {len(candidate.stage_plans)}\n"
                    f"المحافظ النشطة: {len(candidate.wallets)}\n"
                    f"المرحلة الحالية: {candidate.current_stage_label or 'بانتظار المرحلة القادمة'}\n"
                    "سيتم فحص الأهلية والتنفيذ المجاني تلقائيًا، والمدفوع يحتاج موافقتك."
                )

    def cleanup_auto_candidates(self) -> None:
        now = time.time()
        for key, candidate in list(self.candidates.items()):
            if not candidate.auto_discovered:
                continue
            has_pending_receipt = any(s.submitted and not s.confirmed and not s.final for s in candidate.wallets.values())
            if has_pending_receipt:
                continue
            if candidate.watch_kind == "auto_stage":
                # Multi-stage candidates are persistent until their final known
                # stage ends; process_stage_schedule archives them.
                if not candidate.done:
                    continue
            if candidate.mint_backend == "seadrop" and candidate.stage_end and now < candidate.stage_end:
                continue
            if candidate.last_seen_auto and now - candidate.last_seen_auto > self.auto_free_candidate_ttl:
                self.candidates.pop(key, None)

    # ---------- V4.4 stage / qualification planner ----------
    def ensure_candidate_watch_persisted(self, candidate: Candidate) -> None:
        if candidate.watch_kind == "auto_free" and not candidate.qualification_tracked and not candidate.has_paid_stage:
            return
        if self.store.get_watch(candidate.slug):
            self.persist_candidate_planning(candidate)
            return
        self.store.upsert_watch(
            slug=candidate.slug,
            chain=candidate.chain,
            source=candidate.source,
            allow_paid=True,
            max_mint_price_native=str(candidate.max_mint_price_native),
            quantity=candidate.quantity_override,
            mint_backend=candidate.mint_backend,
            contract_address=candidate.contract_address,
            watch_kind=candidate.watch_kind,
            stages_json=json.dumps(candidate.stage_plans, ensure_ascii=False),
            next_stage_start=candidate.next_stage_start,
            public_start=candidate.public_start,
        )

    def stage_target_total(self, candidate: Candidate, plan: dict[str, Any]) -> int:
        limit = plan.get("wallet_limit")
        try:
            parsed_limit = int(limit) if limit is not None else None
        except (TypeError, ValueError):
            parsed_limit = None
        return self.auto_target_for_limit(parsed_limit, candidate.remaining_supply)

    def set_current_stage(self, candidate: Candidate, plan: dict[str, Any] | None) -> None:
        if not plan:
            candidate.current_stage_key = ""
            candidate.current_stage_label = ""
            candidate.current_stage_start = None
            candidate.current_stage_end = None
            candidate.current_stage_public = False
            candidate.current_stage_free = False
            candidate.current_stage_paid = False
            candidate.current_stage_limit = None
            return
        candidate.current_stage_key = str(plan.get("key") or "")
        candidate.current_stage_label = str(plan.get("label") or "مرحلة Mint")
        candidate.current_stage_start = float(plan["start"]) if plan.get("start") is not None else None
        candidate.current_stage_end = float(plan["end"]) if plan.get("end") is not None else None
        candidate.current_stage_public = bool(plan.get("is_public"))
        candidate.current_stage_free = bool(plan.get("is_free"))
        candidate.current_stage_paid = bool(plan.get("is_paid"))
        candidate.current_stage_limit = int(plan["wallet_limit"]) if plan.get("wallet_limit") else None
        candidate.wallet_limit = candidate.current_stage_limit

    def stage_qualification_check(
        self,
        candidate: Candidate,
        plan: dict[str, Any],
        *,
        notify: bool = False,
        force: bool = False,
    ) -> str:
        now = time.time()
        stage_key = str(plan.get("key") or "")
        if not force and stage_key in candidate.checked_stage_keys and now - candidate.last_qualification_check < self.qualification_recheck_seconds:
            return ""
        candidate.last_qualification_check = now
        candidate.checked_stage_keys.add(stage_key)
        self.set_current_stage(candidate, plan)
        self.sync_wallets_into_candidates()
        pool = self.rpc_pools[candidate.chain]
        project_key = self.project_key_for_candidate(candidate)
        target_total = self.stage_target_total(candidate, plan)
        active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
        states = [s for s in candidate.wallets.values() if s.wallet.address.lower() in active_addresses]
        lines = [
            f"🎟 فحص مرحلة — {candidate.slug}",
            f"المرحلة: {plan.get('label')} | {'عام' if plan.get('is_public') else 'تأهيل'}",
            f"الموعد: {format_ts(plan.get('start'), self.display_tz)} → {format_ts(plan.get('end'), self.display_tz)}",
            f"الحد المعلن/المحفظة: {plan.get('wallet_limit') or 'غير محدد'} | الهدف التلقائي: {target_total or 0}",
        ]

        def one(state: WalletState):
            confirmed_total = self.confirmed_total_for_wallet(candidate, state.wallet.address)
            additional = max(0, target_total - confirmed_total)
            if additional <= 0:
                return state, confirmed_total, additional, None
            if candidate.mint_backend == "seadrop" and candidate.contract_address:
                result = check_seadrop_eligibility(pool, state.wallet, candidate.contract_address, additional)
            else:
                result = check_eligibility(self.opensea, candidate.slug, state.wallet, additional)
            return state, confirmed_total, additional, result

        if states:
            workers = min(self.max_parallel_wallets, len(states))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(one, state) for state in states]
                for future in as_completed(futures):
                    state, confirmed_total, additional, result = future.result()
                    state.confirmed_total = confirmed_total
                    state.target_total = target_total
                    state.stage_key = stage_key
                    state.stage_label = str(plan.get("label") or "مرحلة Mint")

                    if result is None:
                        status = "target_satisfied"
                        eligible = True
                        available = 0
                        lines.append(f"✅ {state.wallet.name}: الهدف محقق مسبقًا ({confirmed_total}/{target_total})")
                    else:
                        status = result.status
                        eligible = result.eligible
                        available = int(result.quantity_used or 0)
                        state.eligibility = status
                        state.mint_value_native = result.mint_value_native
                        icon = "✅" if result.eligible is True else "❌" if result.eligible is False else "⏳"
                        value_text = "" if result.mint_value_native is None else f" | القيمة={result.mint_value_native} {native_symbol(candidate.chain)}"
                        lines.append(
                            f"{icon} {state.wallet.name}: {eligibility_label(status)}"
                            f" | مكتسب={confirmed_total} | إضافي مطلوب={additional}"
                            + (f" | متاح الآن={available}" if available else "") + value_text
                        )

                        # Automatic stage execution is strictly free. Paid
                        # private/allowlist stages are recorded but never signed
                        # without an explicit paid plan.
                        if result.eligible is True and additional > 0:
                            actual = min(additional, available or additional)
                            is_free_result = (result.mint_value_native or Decimal("0")) == 0
                            if is_free_result:
                                state.quantity = max(1, actual)
                                state.next_attempt = now
                                state.final = False
                            elif plan.get("is_public"):
                                candidate.paid_detected = True

                    self.store.record_qualification_wallet(
                        project_key=project_key,
                        stage_key=stage_key,
                        stage_label=str(plan.get("label") or "مرحلة Mint"),
                        stage_start=float(plan["start"]) if plan.get("start") is not None else None,
                        stage_end=float(plan["end"]) if plan.get("end") is not None else None,
                        wallet_name=state.wallet.name,
                        wallet_address=state.wallet.address,
                        eligibility_status=status,
                        eligible=eligible,
                        stage_limit=int(plan["wallet_limit"]) if plan.get("wallet_limit") else None,
                        target_total=target_total,
                        additional_needed=additional,
                        quantity_available=available,
                    )

        self.store.upsert_qualification_project(
            project_key=project_key,
            slug=candidate.slug,
            chain=candidate.chain,
            contract_address=candidate.contract_address,
            source=candidate.source,
            stages=candidate.stage_plans,
            next_stage_start=candidate.next_stage_start,
            public_start=candidate.public_start,
            final_stage_end=candidate.final_stage_end,
            last_stage_key=stage_key,
            last_stage_label=str(plan.get("label") or "مرحلة Mint"),
            last_stage_start=float(plan["start"]) if plan.get("start") is not None else None,
            last_stage_end=float(plan["end"]) if plan.get("end") is not None else None,
        )
        self.persist_candidate_planning(candidate)
        text = "\n".join(lines)[:3900]
        if notify:
            self.notify_all(text)
        return text

    def find_paid_public_plan(self, candidate: Candidate, now: float | None = None) -> dict[str, Any] | None:
        now = time.time() if now is None else now
        candidates = [
            p for p in candidate.stage_plans
            if p.get("is_public") and p.get("is_paid") and (p.get("end") is None or float(p["end"]) > now)
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda p: float(p.get("start") or now))

    def maybe_offer_paid_public(self, candidate: Candidate) -> None:
        """Register a paid Public stage silently.

        V4.5 intentionally does not push paid-mint prompts to Telegram. Paid
        projects are saved and shown only when the user opens the dedicated
        ``💳 المنتات المدفوعة`` section.
        """
        plan = self.find_paid_public_plan(candidate)
        if not plan:
            return
        stage_key = str(plan.get("key") or "")
        previous_stage_key = candidate.paid_stage_key
        candidate.paid_detected = True
        candidate.paid_stage_key = stage_key
        candidate.paid_stage_start = float(plan["start"]) if plan.get("start") is not None else None
        if previous_stage_key and previous_stage_key != stage_key:
            # Approval is stage-specific: never reuse an old paid approval on a
            # new price/time stage. The user must confirm the new stage again.
            candidate.paid_decision = "pending"
            candidate.paid_selection_confirmed = False
            candidate.paid_wallet_addresses.clear()
            candidate.paid_wallet_quantities.clear()
        elif not candidate.paid_decision:
            candidate.paid_decision = "pending"
        candidate.watch_kind = "auto_stage" if candidate.auto_discovered else candidate.watch_kind
        self.ensure_candidate_watch_persisted(candidate)
        self.store.set_watch_paid_plan(
            candidate.slug, candidate.paid_wallet_quantities,
            decision=candidate.paid_decision or "pending",
            stage_key=stage_key, stage_start=candidate.paid_stage_start, paid_detected=True,
        )

    def process_stage_schedule(self, candidate: Candidate) -> None:
        if not candidate.stage_plans:
            return
        now = time.time()
        if now - candidate.last_schedule_tick < 0.15:
            return
        candidate.last_schedule_tick = now
        current = self.current_plan_for_candidate(candidate, now)
        previous_key = candidate.processed_stage_key

        if current:
            self.set_current_stage(candidate, current)
            # New stage: reset resolved states for another cumulative-quantity
            # opportunity. Pending transactions remain untouched.
            if candidate.current_stage_key != previous_key:
                candidate.processed_stage_key = candidate.current_stage_key
                for state in candidate.wallets.values():
                    if not state.submitted:
                        state.final = False
                        state.confirmed = False
                        state.tx_hash = None
                        state.stage_key = candidate.current_stage_key
                        state.stage_label = candidate.current_stage_label
                        state.next_attempt = now
                self.stage_qualification_check(candidate, current, notify=False, force=True)
            elif not current.get("is_public") and now - candidate.last_qualification_check >= self.qualification_recheck_seconds:
                # Re-check qualification during an active private stage because
                # allowlists/signed eligibility can change while the stage is live.
                self.stage_qualification_check(candidate, current, notify=False, force=True)
        else:
            self.set_current_stage(candidate, None)
            future = self.next_plan_for_candidate(candidate, now)
            if future and future.get("start") is not None:
                candidate.next_stage_start = float(future["start"])
                lead = self.public_preopen_window_seconds if future.get("is_public") else self.preopen_probe_seconds
                probe_at = max(now, float(future["start"]) - lead)
                for state in candidate.wallets.values():
                    if not state.submitted:
                        state.next_attempt = min(state.next_attempt or probe_at, probe_at)

        self.maybe_offer_paid_public(candidate)
        self.persist_candidate_planning(candidate)

        # Archive only after all known stages have ended and no transaction is
        # waiting for a receipt. This keeps early allowlist mints alive for the
        # later public stage exactly as requested.
        if candidate.final_stage_end and now >= candidate.final_stage_end:
            pending = any(s.submitted and not s.confirmed and not s.final for s in candidate.wallets.values())
            if not pending:
                reason = "انتهت جميع مراحل المنت المعروفة"
                if self.store.get_watch(candidate.slug):
                    self.store.remove_watch(candidate.slug, reason=reason)
                if candidate.qualification_tracked:
                    self.store.archive_qualification_project(self.project_key_for_candidate(candidate), reason)
                candidate.done = True

    def prepare_state_after_receipt(self, candidate: Candidate, state: WalletState) -> None:
        state.submitted = False
        state.confirmed = False
        state.final = False
        state.tx_hash = None
        state.pending_quantity = 0
        state.mint_value_native = None
        state.confirmed_total = self.confirmed_total_for_wallet(candidate, state.wallet.address)
        # After one successful mint in a stage we do not immediately mint the
        # same stage again. The next known stage is the next opportunity; when
        # it opens, cumulative-target logic computes only the increase.
        future = self.next_plan_for_candidate(candidate)
        if future and future.get("start") is not None:
            lead = self.public_preopen_window_seconds if future.get("is_public") else self.preopen_probe_seconds
            state.next_attempt = max(time.time() + 0.5, float(future["start"]) - lead)
        else:
            state.next_attempt = time.time() + max(self.stage_refresh_seconds, 15.0)
            state.final = True

    # ---------- eligibility + mint execution ----------
    def eligibility_matrix(self, candidate: Candidate) -> str:
        active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
        states = [s for s in candidate.wallets.values() if s.wallet.address.lower() in active_addresses]
        if not states:
            return "لا توجد محافظ نشطة مناسبة لهذه الشبكة."
        pool = self.rpc_pools[candidate.chain]
        lines = [f"🧪 فحص الأهلية — {candidate.slug} ({chain_label(candidate.chain)})"]

        def one(state: WalletState):
            if candidate.mint_backend == "seadrop" and candidate.contract_address:
                result = check_seadrop_eligibility(pool, state.wallet, candidate.contract_address, state.quantity)
            else:
                result = check_eligibility(self.opensea, candidate.slug, state.wallet, state.quantity)
            balance = None
            try:
                balance = pool.balance_native(state.wallet.address)
            except Exception:
                pass
            return state, result, balance

        workers = min(self.max_parallel_wallets, len(states))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(one, state) for state in states]
            for future in as_completed(futures):
                state, result, balance = future.result()
                state.eligibility = result.status
                state.mint_value_native = result.mint_value_native
                icon = "✅" if result.eligible is True else "❌" if result.eligible is False else "⏳"
                price = "" if result.mint_value_native is None else f" | سعر المنت={result.mint_value_native} {native_symbol(candidate.chain)}"
                qty = "" if not result.quantity_used else f" | الكمية المتاحة={result.quantity_used}"
                bal = "" if balance is None else f" | الرصيد={balance:.6f}"
                lines.append(
                    f"{icon} {state.wallet.name} {short_address(state.wallet.address)}: "
                    f"{eligibility_label(result.status)}{qty}{price}{bal}"
                )
                if result.mint_value_native is not None and result.mint_value_native > 0:
                    candidate.paid_detected = True
        return "\n".join(lines)[:3900]

    def notify_state_change(self, candidate: Candidate, state: WalletState, status: str, detail: str | None = None) -> None:
        if status == state.last_notified_status:
            return
        state.last_notified_status = status
        if status in {"not_mintable_yet", "not_active_yet", "paid_wallet_selection_required"}:
            return
        # Auto discovery can evaluate many drops every scan. Routine skips are
        # intentionally silent to avoid flooding Telegram; successful submitted
        # and confirmed mints are still always announced.
        if candidate.auto_discovered and status in {
            "precondition_failed", "rate_limited", "gas_too_high", "gas_usd_too_high",
            "gas_price_unavailable", "mint_price_too_high", "total_spend_too_high",
            "paid_not_allowed", "target_not_allowed"
        }:
            return
        labels = {
            "precondition_failed": "❌ المحفظة غير مؤهلة للمرحلة الحالية",
            "rate_limited": "⏳ OpenSea قيّدت الطلب مؤقتًا",
            "insufficient_balance": "💸 رصيد المحفظة غير كافٍ",
            "gas_too_high": "⛽ الغاز أعلى من الحد المضبوط",
            "gas_usd_too_high": "💸 رسوم الشبكة أعلى من ميزانية الغاز بالدولار — سأنتظر انخفاضها",
            "gas_price_unavailable": "⏳ تعذر حساب سعر الغاز بالدولار مؤقتًا — لن أصرف بدون تحقق",
            "mint_price_too_high": "💰 سعر المنت أعلى من الحد المضبوط",
            "total_spend_too_high": "🛡 التكلفة الإجمالية أعلى من الحد المضبوط",
            "paid_not_allowed": "🔒 المنت المدفوع ممنوع حسب إعدادات البوت",
            "target_not_allowed": "🛡 عقد المنت غير مسموح به",
            "rpc_or_tx_error": "⚠️ خطأ في RPC أو المعاملة",
            "opensea_error": "⚠️ خطأ من OpenSea API",
            "bad_mint_payload": "⚠️ بيانات معاملة المنت غير صالحة",
        }
        if status in labels:
            self.notify_all(
                f"{labels[status]}\n"
                f"المشروع: {candidate.slug}\n"
                f"المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                f"الشبكة: {chain_label(candidate.chain)}\n"
                f"{(detail or '')[:600]}"
            )

    def notify_paid_selection(self, candidate: Candidate) -> None:
        """Compatibility registration for paid mints without push messages."""
        plan = self.find_paid_public_plan(candidate)
        if plan:
            self.maybe_offer_paid_public(candidate)
            return
        candidate.paid_detected = True
        candidate.paid_decision = candidate.paid_decision or "pending"
        self.ensure_candidate_watch_persisted(candidate)
        self.persist_paid_plan(candidate, candidate.paid_decision)

    def try_candidate(self, candidate: Candidate) -> None:
        if self.paused or candidate.done:
            return
        now = time.time()

        current = self.current_plan_for_candidate(candidate, now) if candidate.stage_plans else None
        plan = current
        if plan is None and candidate.stage_plans:
            future = self.next_plan_for_candidate(candidate, now)
            if not future or future.get("start") is None:
                return
            lead = self.public_preopen_window_seconds if future.get("is_public") else self.preopen_probe_seconds
            if float(future["start"]) - now > lead:
                return
            plan = future  # pre-open probing in the final seconds

        # Paid allowlist/private stages are recorded under qualification but are
        # never auto-purchased. Paid Public requires a saved explicit plan.
        if plan and plan.get("is_paid") and not plan.get("is_public"):
            for state in candidate.wallets.values():
                if not state.submitted:
                    state.next_attempt = max(now + self.qualification_recheck_seconds, float(plan.get("end") or (now + self.qualification_recheck_seconds)))
            return
        if plan and plan.get("is_public") and plan.get("is_paid"):
            candidate.paid_detected = True
            candidate.paid_stage_key = str(plan.get("key") or candidate.paid_stage_key)
            candidate.paid_stage_start = float(plan["start"]) if plan.get("start") is not None else candidate.paid_stage_start
            if candidate.paid_decision != "confirmed":
                self.maybe_offer_paid_public(candidate)
                return

        due = [state for state in candidate.wallets.values() if not state.submitted and not state.final and state.next_attempt <= now]
        if not due:
            return
        pool = self.rpc_pools[candidate.chain]

        work: list[tuple[WalletState, int]] = []
        for state in due:
            address = state.wallet.address.lower()
            if plan and plan.get("is_public") and plan.get("is_paid"):
                if address not in candidate.paid_wallet_addresses:
                    continue
                qty = int(candidate.paid_wallet_quantities.get(address) or 0)
                if qty <= 0:
                    continue
                limit = plan.get("wallet_limit")
                if limit:
                    qty = min(qty, int(limit))
                qty = max(1, min(qty, 100))
            else:
                target_total = self.stage_target_total(candidate, plan or {"wallet_limit": candidate.wallet_limit})
                confirmed_total = self.confirmed_total_for_wallet(candidate, state.wallet.address)
                state.confirmed_total = confirmed_total
                state.target_total = target_total
                qty = max(0, target_total - confirmed_total)
                if candidate.quantity_override:
                    qty = min(qty, max(1, int(candidate.quantity_override)))
                if candidate.remaining_supply is not None:
                    qty = min(qty, max(0, int(candidate.remaining_supply)))
                if qty <= 0:
                    future = self.next_plan_for_candidate(candidate, now)
                    if future and future.get("start") is not None:
                        lead = self.public_preopen_window_seconds if future.get("is_public") else self.preopen_probe_seconds
                        state.next_attempt = max(now + 0.5, float(future["start"]) - lead)
                    else:
                        state.next_attempt = now + self.stage_refresh_seconds
                    continue
            state.quantity = qty
            state.stage_key = str((plan or {}).get("key") or candidate.current_stage_key or "")
            state.stage_label = str((plan or {}).get("label") or candidate.current_stage_label or "Public Mint")
            work.append((state, qty))

        if not work:
            return
        workers = min(self.max_parallel_wallets, len(work))

        def execute(item: tuple[WalletState, int]):
            state, qty = item
            state.attempts += 1
            state.status = "attempting"
            paid_selected = (
                candidate.paid_decision == "confirmed"
                and state.wallet.address.lower() in candidate.paid_wallet_addresses
            )
            common = dict(
                rpc_pool=pool, wallet=state.wallet, quantity=qty,
                gas_strategy=self.gas_strategy, gas_limit_buffer=self.gas_limit_buffer,
                max_gas_native=self.max_gas_for_chain(candidate.chain),
                allow_paid=(candidate.allow_paid or candidate.paid_decision == "confirmed"),
                max_mint_price_native=candidate.max_mint_price_native, max_total_native=self.max_total_native,
                allowed_targets=self.allowed_targets, paid_wallet_allowed=paid_selected,
                max_gas_usd=self.max_gas_usd_for_chain(candidate.chain),
                native_usd_price=self.native_usd_price_for_chain(candidate.chain),
            )
            if candidate.mint_backend == "seadrop" and candidate.contract_address:
                return state, mint_seadrop_public(nft_contract=candidate.contract_address, **common)
            return state, mint_drop(opensea=self.opensea, slug=candidate.slug, **common)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(execute, item) for item in work]
            for future in as_completed(futures):
                state, result = future.result()
                state.status = result.status
                state.last_detail = result.detail
                if result.ok:
                    state.submitted = True
                    state.final = False
                    if result.quantity_used:
                        state.quantity = result.quantity_used
                    state.pending_quantity = int(result.quantity_used or state.quantity)
                    state.tx_hash = result.tx_hash
                    state.mint_value_native = result.mint_value_native
                    state.receipt_next_check = time.time() + self.receipt_check_seconds
                    self.store.record_mint(
                        slug=candidate.slug,
                        chain=candidate.chain,
                        wallet_name=state.wallet.name,
                        wallet_address=state.wallet.address,
                        status="submitted",
                        tx_hash=result.tx_hash,
                        mint_value_native=str(result.mint_value_native),
                        gas_max_native=str(result.gas_cost_native),
                        quantity=state.pending_quantity,
                        detail=result.detail,
                        contract_address=candidate.contract_address,
                        stage_key=state.stage_key or None,
                        stage_label=state.stage_label or None,
                        watch_kind=candidate.watch_kind,
                    )
                    url = explorer_tx_url(candidate.chain, result.tx_hash or "")
                    kind = "مجاني" if (result.mint_value_native or Decimal("0")) == 0 else "مدفوع"
                    self.notify_all(
                        f"🚀 تم إرسال معاملة Mint\n"
                        f"المشروع: {candidate.slug}\n"
                        f"المرحلة: {state.stage_label}\n"
                        f"النوع: {kind}\n"
                        f"الشبكة: {chain_label(candidate.chain)}\n"
                        f"المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                        f"الكمية: {state.pending_quantity}\n"
                        f"قيمة المنت: {result.mint_value_native} {native_symbol(candidate.chain)}\n"
                        f"أقصى تقدير للغاز: {result.gas_cost_native} {native_symbol(candidate.chain)}"
                        + (f" ≈ ${result.gas_cost_usd:.4f}" if result.gas_cost_usd is not None else "")
                        + "\n"
                        f"TX: {result.tx_hash}\n{url}"
                    )
                    continue

                if result.status == "paid_wallet_selection_required":
                    candidate.paid_detected = True
                    state.mint_value_native = result.mint_value_native
                    state.eligibility = "paid_waiting_selection"
                    state.next_attempt = time.time() + max(15.0, self.eligibility_retry_seconds)
                    if plan and plan.get("is_public"):
                        self.maybe_offer_paid_public(candidate)
                    continue

                if result.status == "not_mintable_yet":
                    stage_start_ts = float(plan["start"]) if plan and plan.get("start") is not None else (candidate.next_stage_start or candidate.public_start)
                    if stage_start_ts and stage_start_ts > time.time():
                        remaining = stage_start_ts - time.time()
                        retry = self.public_fast_retry_seconds if plan and plan.get("is_public") and remaining <= self.public_preopen_window_seconds else self.open_retry_interval
                        state.next_attempt = time.time() + retry
                    else:
                        state.next_attempt = time.time() + (self.public_fast_retry_seconds if plan and plan.get("is_public") else self.open_retry_interval)
                elif result.status == "precondition_failed":
                    state.eligibility = "not_eligible_now"
                    state.next_attempt = time.time() + self.qualification_recheck_seconds
                elif result.status == "rate_limited":
                    state.next_attempt = time.time() + self.rate_limit_retry_seconds
                elif result.status in {"gas_usd_too_high", "gas_price_unavailable"}:
                    state.next_attempt = time.time() + self.gas_over_budget_retry_seconds
                elif result.status in {"gas_too_high", "insufficient_balance", "mint_price_too_high", "total_spend_too_high"}:
                    state.next_attempt = time.time() + max(10, self.eligibility_retry_seconds)
                elif result.status in {"paid_not_allowed", "target_not_allowed"}:
                    state.next_attempt = time.time() + 30
                elif result.status in {"seadrop_not_configured", "no_fee_recipient"}:
                    state.next_attempt = time.time() + 15
                else:
                    state.next_attempt = time.time() + self.monitor_retry_interval
                self.notify_state_change(candidate, state, result.status, result.detail)

    def check_receipts(self, candidate: Candidate) -> None:
        pool = self.rpc_pools[candidate.chain]
        now = time.time()
        for state in candidate.wallets.values():
            if not state.submitted or state.confirmed or not state.tx_hash or state.receipt_next_check > now:
                continue
            status = pool.receipt_status(state.tx_hash)
            if status is None:
                state.receipt_next_check = now + self.receipt_check_seconds
                continue
            if status == 1:
                state.confirmed = True
                confirmed_qty = int(state.pending_quantity or state.quantity or 0)
                self.store.record_mint(
                    slug=candidate.slug,
                    chain=candidate.chain,
                    wallet_name=state.wallet.name,
                    wallet_address=state.wallet.address,
                    status="confirmed",
                    tx_hash=state.tx_hash,
                    mint_value_native=str(state.mint_value_native) if state.mint_value_native is not None else None,
                    quantity=confirmed_qty,
                    contract_address=candidate.contract_address,
                    stage_key=state.stage_key or None,
                    stage_label=state.stage_label or None,
                    watch_kind=candidate.watch_kind,
                )
                total = self.confirmed_total_for_wallet(candidate, state.wallet.address)
                self.notify_all(
                    f"✅ تم تأكيد الـMint بنجاح\n"
                    f"المشروع: {candidate.slug}\n"
                    f"المرحلة: {state.stage_label or 'Mint'}\n"
                    f"المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"الكمية المؤكدة الآن: {confirmed_qty}\n"
                    f"إجمالي ما أخذه البوت لهذه المحفظة من المشروع: {total}\n"
                    f"الشبكة: {chain_label(candidate.chain)}\n"
                    f"TX: {state.tx_hash}\n{explorer_tx_url(candidate.chain, state.tx_hash)}"
                )
                self.prepare_state_after_receipt(candidate, state)
            else:
                failed_hash = state.tx_hash
                self.store.record_mint(
                    slug=candidate.slug,
                    chain=candidate.chain,
                    wallet_name=state.wallet.name,
                    wallet_address=state.wallet.address,
                    status="reverted",
                    tx_hash=failed_hash,
                    quantity=int(state.pending_quantity or state.quantity or 0),
                    contract_address=candidate.contract_address,
                    stage_key=state.stage_key or None,
                    stage_label=state.stage_label or None,
                    watch_kind=candidate.watch_kind,
                )
                state.submitted = False
                state.confirmed = False
                state.tx_hash = None
                state.pending_quantity = 0
                # Do not hammer a reverting transaction in the same stage; a
                # new stage will reset final=False automatically.
                state.final = True
                self.notify_all(
                    f"❌ فشلت معاملة الـMint على الشبكة\n"
                    f"المشروع: {candidate.slug}\n"
                    f"المرحلة: {state.stage_label or 'Mint'}\n"
                    f"المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"TX: {failed_hash}"
                )

    # ---------- Telegram UI ----------
    def menu_buttons(self) -> list[list[tuple[str, str]]]:
        return [
            [("➕ إضافة محفظة", "add_wallet"), ("👛 المحافظ", "wallets")],
            [("🎟 التأهيل", "qualification_menu"), ("👀 المراقبة", "monitoring_menu")],
            [("🆓 المجانية المأخوذة", "free_mints"), ("💳 المنتات المدفوعة", "paid_watches")],
            [("🧪 فحص الأهلية", "eligibility_all"), ("📜 سجل العمليات", "history")],
            [("🌐 الشبكات", "chains"), ("⚙️ الإعدادات", "settings")],
            [("⏸ إيقاف التنفيذ" if not self.paused else "▶️ استئناف التنفيذ", "toggle_pause")],
        ]

    def qualification_menu_buttons(self) -> list[list[tuple[str, str]]]:
        return [
            [("📅 تأهيلات اليوم", "qualification_today")],
            [("🗄 التأهيلات القديمة", "qualification_old")],
            [("🔗 فحص رابط تأهيل", "qualification_add")],
            [("↩️ القائمة الرئيسية", "menu")],
        ]

    def monitoring_menu_buttons(self) -> list[list[tuple[str, str]]]:
        return [
            [("📡 المنتات تحت المراقبة", "monitoring_active")],
            [("🗄 المنتات القديمة", "monitoring_old")],
            [("➕ إضافة رابط منت", "monitoring_add")],
            [("↩️ القائمة الرئيسية", "menu")],
        ]

    def free_mints_buttons(self) -> list[list[tuple[str, str]]]:
        return [
            [("🔄 تحديث القائمة", "free_mints")],
            [("📜 سجل العمليات", "history")],
            [("↩️ القائمة الرئيسية", "menu")],
        ]

    def send_menu(self, chat_id: str) -> None:
        active = len(self.store.list_wallets(enabled_only=True))
        total = len(self.store.list_wallets(enabled_only=False))
        self.telegram.send(
            chat_id,
            "🤖 OpenSea Mint Guardian V4.5\n\n"
            f"🆓/🎟 الاكتشاف التلقائي: مفعّل كل {self.auto_free_scan_seconds:g} ثانية\n"
            f"⚡ الاستعداد للـPublic: آخر {self.public_preopen_window_seconds:g} ثوانٍ\n"
            f"📦 سياسة الكمية: حد المنت ≤100 يؤخذ كما هو، وإذا كان >100/غير محدود فالهدف {self.auto_stage_high_limit_quantity}\n"
            "• كل مرحلة تأهيل تُفحص للمحافظ النشطة وتُحفظ نتيجتها.\n"
            "• المرحلة المجانية تُنفذ تلقائيًا.\n"
            "• Public المدفوع يُحفظ بصمت ولا يظهر إلا عند فتح قسم «💳 المنتات المدفوعة».\n"
            "• لا يتم شراء المدفوع إلا بعد موافقتك + المحافظ + كمية كل محفظة.\n\n"
            f"المحافظ: {active} نشطة من أصل {total}\n"
            f"المراقبات في الذاكرة: {len(self.candidates)}",
            self.menu_buttons(),
        )

    def all_stored_wallets(self):
        return self.store.list_wallets(enabled_only=False)

    def wallets_text(self) -> str:
        wallets = self.all_stored_wallets()
        if not wallets:
            return "👛 لا توجد محافظ مضافة بعد.\nاضغط «➕ إضافة محفظة» لإضافة أول محفظة."
        active = sum(1 for w in wallets if w.enabled)
        lines = [f"👛 المحافظ — {active} نشطة / {len(wallets)} إجماليًا", ""]
        for wallet in wallets:
            status = "🟢 نشطة" if wallet.enabled else "🔴 متوقفة"
            lines.append(
                f"• {wallet.name}\n"
                f"  {status} | الكمية={wallet.quantity}\n"
                f"  {short_address(wallet.address)}"
            )
        lines.append("\nاضغط على اسم أي محفظة لإدارتها.")
        return "\n".join(lines)[:3900]

    def wallet_list_buttons(self) -> list[list[tuple[str, str]]]:
        rows: list[list[tuple[str, str]]] = []
        for wallet in self.all_stored_wallets()[:30]:
            icon = "🟢" if wallet.enabled else "🔴"
            rows.append([(f"{icon} {wallet.name}", f"wv:{wallet.id}")])
        rows.append([("➕ إضافة محفظة", "add_wallet"), ("↩️ القائمة الرئيسية", "menu")])
        return rows

    def wallet_detail_text(self, wallet_id: int) -> str:
        wallet = self.store.get_wallet_by_id(wallet_id)
        if not wallet:
            return "⚠️ لم يتم العثور على المحفظة."
        status = "🟢 نشطة وتشارك في الـMint" if wallet.enabled else "🔴 متوقفة ولا يتم تنفيذ Mint لها"
        lines = [
            f"👛 {wallet.name}",
            status,
            f"العنوان: {wallet.address}",
            f"كمية الـMint الافتراضية اليدوية: {wallet.quantity}",
            f"المنت التلقائي بالمراحل: يتبع حد المرحلة؛ وإذا كان الحد >{self.auto_stage_high_limit_threshold} أو غير محدود فالهدف {self.auto_stage_high_limit_quantity}",
            "",
            "الأرصدة:",
        ]
        for chain in self.enabled_chains:
            pool = self.rpc_pools.get(chain)
            if not pool or (wallet.chains and normalize_chain(chain) not in {normalize_chain(x) for x in wallet.chains}):
                continue
            try:
                balance = pool.balance_native(wallet.address)
                lines.append(f"• {chain_label(chain)}: {balance:.8f} {native_symbol(chain)}")
            except Exception:
                lines.append(f"• {chain_label(chain)}: تعذر قراءة الرصيد")
        return "\n".join(lines)[:3900]

    def wallet_detail_buttons(self, wallet_id: int) -> list[list[tuple[str, str]]]:
        wallet = self.store.get_wallet_by_id(wallet_id)
        if not wallet:
            return [[("↩️ المحافظ", "wallets")]]
        toggle = "⏸ إيقاف المحفظة" if wallet.enabled else "▶️ تشغيل المحفظة"
        return [
            [(toggle, f"wt:{wallet.id}")],
            [("✏️ تغيير الاسم", f"wr:{wallet.id}"), ("🔢 تغيير الكمية", f"wq:{wallet.id}")],
            [("🗑 حذف المحفظة", f"wd:{wallet.id}")],
            [("↩️ المحافظ", "wallets"), ("🏠 الرئيسية", "menu")],
        ]

    def chains_text(self) -> str:
        lines = ["🌐 حالة الشبكات"]
        for chain in self.enabled_chains:
            pool = self.rpc_pools.get(chain)
            if pool:
                lines.append(f"✅ {chain_label(chain)}: {len(pool.urls)} RPC متحقق")
            else:
                lines.append(f"❌ {chain_label(chain)}: لا يوجد RPC يعمل")
        return "\n".join(lines)[:3900]

    def status_text(self) -> str:
        if not self.candidates:
            return "👀 لا توجد مشاريع تحت المراقبة حاليًا."
        lines = ["👀 المراقبات النشطة"]
        now = time.time()
        for candidate in self.candidates.values():
            current = self.current_plan_for_candidate(candidate, now) if candidate.stage_plans else None
            future = self.next_plan_for_candidate(candidate, now) if candidate.stage_plans else None
            stage_text = (
                f"المرحلة الآن: {current.get('label')}" if current else
                f"المرحلة القادمة: {future.get('label')} — {format_ts(future.get('start'), self.display_tz)}" if future else
                "لا توجد مرحلة نشطة الآن"
            )
            paid = {
                "pending": "⏳ Public المدفوع بانتظار قرار/خطة",
                "confirmed": "✅ خطة شراء Public المدفوع مؤكدة",
                "declined": "🚫 شراء Public المدفوع مرفوض",
            }.get(candidate.paid_decision, "")
            source = {"manual": "يدوي", "auto_stage": "تلقائي/مراحل", "auto_free": "تلقائي/مجاني"}.get(candidate.watch_kind, candidate.watch_kind)
            backend = "SeaDrop مباشر" if candidate.mint_backend == "seadrop" else "OpenSea Drop"
            lines.append(
                f"\n• {candidate.slug} | {chain_label(candidate.chain)}\n"
                f"  المصدر: {source} / {backend}\n"
                f"  {stage_text}\n"
                f"  Public: {format_ts(candidate.public_start, self.display_tz)}"
                + (f"\n  {paid}" if paid else "")
            )
        return "\n".join(lines)[:3900]

    def settings_text(self) -> str:
        return (
            "⚙️ إعدادات التشغيل الحالية\n"
            f"تنفيذ الـMint: {'⏸ متوقف' if self.paused else '▶️ يعمل'}\n"
            f"Auto Discovery: {'مفعّل' if self.auto_free_enabled else 'متوقف'} — دورة شاملة كل {self.auto_free_scan_seconds:g} ثانية\n"
            f"OpenSea Stream: {'مفعّل' if self.auto_stream_enabled else 'متوقف'} | Events fallback: {'مفعّل' if self.auto_event_fallback_enabled else 'متوقف'}\n"
            f"إعادة فحص التأهيل أثناء المرحلة: كل {self.qualification_recheck_seconds:g} ثانية\n"
            f"استعداد Public قبل الفتح: {self.public_preopen_window_seconds:g} ثوانٍ | إعادة محاولة سريعة: {self.public_fast_retry_seconds:g} ثانية\n"
            f"سياسة الكمية التلقائية: حد ≤{self.auto_stage_high_limit_threshold} يؤخذ كهدف؛ أعلى منه/غير محدود = {self.auto_stage_high_limit_quantity}\n"
            f"Public المدفوع: يحتاج موافقة + محافظ + كمية كل محفظة\n"
            f"سقف الغاز بالدولار: ${self.max_gas_usd}\n"
            f"سقف الغاز Native: {'بدون حد' if self.max_gas_native <= 0 else self.max_gas_native}\n"
            f"أقصى سعر Mint: {'بدون حد' if self.max_mint_price_default <= 0 else self.max_mint_price_default}\n"
            f"استراتيجية الغاز: {self.gas_strategy} | Buffer={self.gas_limit_buffer}\n"
            f"المحافظ المتوازية: {self.max_parallel_wallets}"
        )

    def history_text(self) -> str:
        rows = self.store.recent_history(15)
        if not rows:
            return "📜 لا يوجد سجل Mint حتى الآن."
        status_labels = {
            "submitted": "🚀 أُرسلت",
            "confirmed": "✅ تأكدت",
            "reverted": "❌ فشلت",
        }
        lines = ["📜 آخر عمليات الـMint"]
        for row in rows:
            ts = format_ts(float(row["created_at"]), self.display_tz)
            label = status_labels.get(str(row["status"]), str(row["status"]))
            lines.append(
                f"• {label} | {row['slug']} | {row['wallet_name']}\n"
                f"  {ts}" + (f"\n  TX: {row['tx_hash']}" if row.get("tx_hash") else "")
            )
        return "\n".join(lines)[:3900]

    def free_mints_text(self) -> str:
        rows = self.store.free_mint_summary(30)
        if not rows:
            return (
                "🆓 لا توجد Free Mints مؤكدة أخذها البوت حتى الآن.\n\n"
                "عندما تنجح معاملة مجانية ستظهر هنا فقط بعد تأكيدها على الشبكة."
            )
        lines = ["🆓 المنتات المجانية التي تم أخذها", ""]
        for row in rows:
            names = [x.strip() for x in str(row.get("wallet_names") or "").split(",") if x.strip()]
            if len(names) > 4:
                wallet_text = "، ".join(names[:4]) + f" +{len(names)-4}"
            else:
                wallet_text = "، ".join(names) or "غير معروف"
            lines.append(
                f"• {row.get('slug')} | {chain_label(str(row.get('chain') or ''))}\n"
                f"  الكمية الإجمالية: {int(row.get('total_quantity') or 0)} | المحافظ: {int(row.get('wallet_count') or 0)}\n"
                f"  الأسماء: {wallet_text}\n"
                f"  آخر تأكيد: {format_ts(float(row.get('last_confirmed_at') or 0), self.display_tz)}"
            )
        lines.append("\nهذه القائمة تعرض الـMint المجاني المؤكد فقط، ولا تعرض المحاولات أو المعاملات الفاشلة.")
        return "\n".join(lines)[:3900]

    def _is_timestamp_today(self, ts: float | None) -> bool:
        if ts is None:
            return False
        return datetime.fromtimestamp(float(ts), self.display_tz).date() == datetime.now(self.display_tz).date()

    def qualification_today_text(self) -> str:
        rows = self.store.list_qualification_projects(active=None, limit=80)
        today_rows: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
        now = time.time()
        for row in rows:
            try:
                plans = json.loads(str(row.get("stages_json") or "[]"))
            except Exception:
                plans = []
            today_plans = [
                p for p in plans if self._is_timestamp_today(p.get("start")) or plan_is_active(p, now)
            ]
            if today_plans:
                today_rows.append((row, today_plans))
        if not today_rows:
            return "🎟 لا توجد منتات تأهيل مسجلة لتاريخ اليوم حتى الآن."
        lines = ["📅 تأهيلات اليوم"]
        for row, plans in today_rows[:15]:
            project_key = str(row["project_key"])
            lines.append(f"\n• {row['slug']} | {chain_label(str(row['chain']))}")
            for plan in plans[:6]:
                stage_key = str(plan.get("key") or "")
                wallet_rows = self.store.qualification_wallet_rows(project_key, stage_key)
                eligible_names: list[str] = []
                for wallet_row in wallet_rows:
                    if wallet_row.get("eligible") != 1:
                        continue
                    confirmed = self.store.confirmed_quantity_for_target(
                        str(row["slug"]), str(wallet_row["wallet_address"]),
                        chain=str(row["chain"]), contract_address=row.get("contract_address"),
                    )
                    target = int(wallet_row.get("target_total") or 0)
                    suffix = f" (مأخوذ {confirmed}/{target})" if target else (f" (مأخوذ {confirmed})" if confirmed else "")
                    eligible_names.append(str(wallet_row["wallet_name"]) + suffix)
                eligible_text = "، ".join(eligible_names) if eligible_names else "لا توجد محفظة مؤهلة حتى آخر فحص"
                lines.append(
                    f"  🎫 {plan.get('label')} | {format_ts(plan.get('start'), self.display_tz)}"
                    f" | {'عام' if plan.get('is_public') else 'تأهيل'}"
                )
                lines.append(f"     المؤهلة: {eligible_text}")
        return "\n".join(lines)[:3900]

    def qualification_old_text(self) -> str:
        rows = self.store.list_qualification_projects(active=None, limit=80)
        now = time.time()
        old: list[dict[str, Any]] = []
        for row in rows:
            try:
                plans = json.loads(str(row.get("stages_json") or "[]"))
            except Exception:
                plans = []
            if str(row.get("status")) != "active" or (plans and not any(self._is_timestamp_today(p.get("start")) or plan_is_active(p, now) for p in plans)):
                old.append(row)
        if not old:
            return "🗄 لا توجد تأهيلات قديمة محفوظة بعد."
        lines = ["🗄 التأهيلات القديمة"]
        for row in old[:25]:
            status = "منتهٍ" if str(row.get("status")) != "active" else "من تاريخ سابق"
            lines.append(
                f"• {row['slug']} | {chain_label(str(row['chain']))} | {status}\n"
                f"  آخر مرحلة: {row.get('last_stage_label') or 'غير معروف'}"
                + (f" | السبب: {row.get('archive_reason')}" if row.get("archive_reason") else "")
            )
        return "\n".join(lines)[:3900]

    def monitoring_active_text(self) -> str:
        rows = self.store.list_watches(active_only=True)
        if not rows:
            return "📡 لا توجد منتات محفوظة تحت المراقبة حاليًا."
        lines = ["📡 المنتات تحت المراقبة"]
        for row in rows[:30]:
            kind = {"manual": "يدوي", "auto_stage": "تلقائي/مراحل", "auto_free": "تلقائي/مجاني"}.get(str(row.get("watch_kind")), str(row.get("watch_kind") or ""))
            paid = str(row.get("paid_decision") or "")
            paid_text = {"pending": " | 💳 بانتظار قرار", "confirmed": " | 💳 شراء مؤكد", "declined": " | 🚫 المدفوع مرفوض"}.get(paid, "")
            lines.append(
                f"• {row['slug']} | {chain_label(str(row.get('chain') or ''))}\n"
                f"  {kind} | المرحلة القادمة: {format_ts(row.get('next_stage_start'), self.display_tz)}{paid_text}"
            )
        return "\n".join(lines)[:3900]

    def monitoring_old_text(self) -> str:
        rows = self.store.list_archived_watches(40)
        if not rows:
            return "🗄 لا توجد منتات مراقبة قديمة حتى الآن."
        lines = ["🗄 المنتات القديمة"]
        for row in rows:
            when = row.get("archived_at") or row.get("updated_at")
            lines.append(
                f"• {row['slug']} | {chain_label(str(row.get('chain') or ''))}\n"
                f"  انتهت: {format_ts(when, self.display_tz)} | السبب: {row.get('archive_reason') or 'انتهت المراقبة'}"
            )
        return "\n".join(lines)[:3900]

    def check_and_track_qualification_link(self, raw: str) -> str:
        ok, message, candidate = self.add_watch(raw, persist=True)
        if not ok or not candidate:
            return ("⚠️ " + message)[:3900]
        candidate.qualification_tracked = has_qualification_stages(candidate.stage_plans)
        self.persist_candidate_planning(candidate)
        current = self.current_plan_for_candidate(candidate)
        lines = [message, "", "🎟 نتيجة فحص التأهيل:"]
        if current:
            result_text = self.stage_qualification_check(candidate, current, notify=False, force=True)
            lines.append(result_text)
        else:
            future = self.next_plan_for_candidate(candidate)
            lines.append("لا توجد مرحلة نشطة الآن.")
            if future:
                lines.append(f"أقرب مرحلة: {future.get('label')} — {format_ts(future.get('start'), self.display_tz)}")
        future = self.next_plan_for_candidate(candidate)
        if future or candidate.qualification_tracked:
            lines.append("✅ تم حفظ المشروع في قسم المراقبة حتى المراحل القادمة.")
        return "\n".join(lines)[:3900]

    def all_eligibility_text(self) -> str:
        if not self.candidates:
            return "لا توجد مشاريع تحت المراقبة لفحصها."
        chunks = []
        for candidate in list(self.candidates.values())[:4]:
            chunks.append(self.eligibility_matrix(candidate))
        return "\n\n".join(chunks)[:3900]

    def candidate_token(self, candidate: Candidate) -> str:
        return hashlib.sha1(f"{candidate.chain}:{candidate.slug}".encode("utf-8")).hexdigest()[:10]

    def candidate_by_token(self, token: str) -> Candidate | None:
        for candidate in self.candidates.values():
            if self.candidate_token(candidate) == token:
                return candidate
        return None

    def paid_plan_for_candidate(self, candidate: Candidate) -> dict[str, Any] | None:
        if candidate.paid_stage_key:
            for plan in candidate.stage_plans:
                if str(plan.get("key") or "") == candidate.paid_stage_key:
                    return plan
        return self.find_paid_public_plan(candidate)

    def paid_price_native(self, candidate: Candidate) -> Decimal | None:
        """Best-effort per-token paid Public price in the chain native token."""
        # For a scheduled paid stage, prefer its own announced price. Reading
        # SeaDrop first could accidentally show the price of a currently active
        # different Public stage.
        plan = self.paid_plan_for_candidate(candidate)
        if plan:
            raw = plan.get("price")
            numeric = _numeric_value(raw)
            if numeric is not None and numeric >= 0:
                text = str(raw or "").lower()
                if "wei" in text or numeric >= Decimal("1000000000"):
                    return numeric / Decimal(10**18)
                return numeric

        # Fallback to exact SeaDrop on-chain configuration when metadata does
        # not expose a parseable price.
        if candidate.contract_address:
            pool = self.rpc_pools.get(candidate.chain)
            if pool:
                try:
                    public = read_seadrop_public_drop(pool.primary, candidate.contract_address)
                    if public and public.get("configured") and public.get("mint_price_wei") is not None:
                        wei = Decimal(str(public.get("mint_price_wei")))
                        if wei >= 0:
                            return wei / Decimal(10**18)
                except Exception:
                    pass
        return None

    @staticmethod
    def _fmt_decimal(value: Decimal, places: int = 8) -> str:
        text = f"{value:.{places}f}".rstrip("0").rstrip(".")
        return text or "0"

    def paid_price_parts(self, candidate: Candidate) -> tuple[str, str]:
        price = self.paid_price_native(candidate)
        symbol = native_symbol(candidate.chain)
        if price is None:
            return f"غير معروف {symbol}", "غير متاح USDT"
        native_text = f"{self._fmt_decimal(price, 10)} {symbol}"
        usd = self.native_usd_price_for_chain(candidate.chain)
        if usd is None:
            return native_text, "غير متاح USDT"
        # USDT display is an approximate USD-equivalent view only. The signed
        # transaction still uses the native-token amount from the mint contract.
        usdt_value = price * usd
        return native_text, f"≈ {self._fmt_decimal(usdt_value, 4)} USDT"

    def paid_list_line(self, candidate: Candidate) -> str:
        plan = self.paid_plan_for_candidate(candidate)
        native_text, usdt_text = self.paid_price_parts(candidate)
        decision = {
            "confirmed": "✅ شراء مؤكد",
            "declined": "🚫 لن يتم الشراء",
            "pending": "⏳ يحتاج إعداد/تأكيد",
        }.get(candidate.paid_decision or "pending", "⏳ يحتاج إعداد/تأكيد")
        open_text = format_ts(plan.get("start"), self.display_tz) if plan else format_ts(candidate.paid_stage_start, self.display_tz)
        return (
            f"• {candidate.slug} | {chain_label(candidate.chain)}\n"
            f"  السعر: {native_text} | {usdt_text}\n"
            f"  الفتح: {open_text} | {decision}"
        )

    def paid_quantity_cap(self, candidate: Candidate) -> int:
        plan = self.paid_plan_for_candidate(candidate)
        limit = plan.get("wallet_limit") if plan else None
        try:
            if limit is not None and int(limit) > 0:
                return max(1, min(int(limit), 100))
        except (TypeError, ValueError):
            pass
        return 100

    def persist_paid_plan(self, candidate: Candidate, decision: str | None = None) -> None:
        if decision is not None:
            candidate.paid_decision = decision
        quantities = {
            a: max(1, min(int(candidate.paid_wallet_quantities.get(a, 1)), self.paid_quantity_cap(candidate)))
            for a in candidate.paid_wallet_addresses
        }
        candidate.paid_wallet_quantities = quantities
        self.store.set_watch_paid_plan(
            candidate.slug,
            quantities,
            decision=candidate.paid_decision or "pending",
            stage_key=candidate.paid_stage_key or None,
            stage_start=candidate.paid_stage_start,
            paid_detected=True,
        )

    def paid_selector_text(self, candidate: Candidate) -> str:
        active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
        eligible_wallets = [s.wallet for s in candidate.wallets.values() if s.wallet.address.lower() in active_addresses]
        selected = candidate.paid_wallet_addresses
        selected_lines = []
        for wallet in eligible_wallets:
            address = wallet.address.lower()
            if address in selected:
                selected_lines.append(f"• {wallet.name}: {candidate.paid_wallet_quantities.get(address, 1)}")
        selected_text = "\n".join(selected_lines) if selected_lines else "لا توجد محافظ محددة بعد"
        plan = self.paid_plan_for_candidate(candidate)
        native_price, usdt_price = self.paid_price_parts(candidate)
        open_text = format_ts(plan.get("start"), self.display_tz) if plan else format_ts(candidate.paid_stage_start, self.display_tz)
        return (
            f"💳 خطة شراء Public Mint المدفوع\n\n"
            f"المشروع: {candidate.slug}\n"
            f"الشبكة: {chain_label(candidate.chain)}\n"
            f"وقت الفتح: {open_text}\n"
            f"السعر: {native_price}\n"
            f"القيمة التقريبية: {usdt_price}\n"
            f"أقصى كمية اختيارية/محفظة لهذه المرحلة: {self.paid_quantity_cap(candidate)}\n\n"
            "اختر المحافظ، ثم اضغط زر الكمية بجانب كل محفظة وأدخل الكمية المطلوبة.\n"
            "لن يتم توقيع أي Mint مدفوع قبل الضغط على «🚀 تأكيد خطة الشراء».\n"
            "أي Public مجاني سيبقى تلقائيًا لجميع المحافظ النشطة.\n\n"
            f"المحدد حاليًا:\n{selected_text}"
        )[:3900]

    def paid_selector_buttons(self, candidate: Candidate) -> list[list[tuple[str, str]]]:
        token = self.candidate_token(candidate)
        rows: list[list[tuple[str, str]]] = []
        active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
        shown = 0
        for state in candidate.wallets.values():
            wallet = state.wallet
            if wallet.address.lower() not in active_addresses:
                continue
            stored = self.store.get_wallet_by_address(wallet.address)
            if not stored:
                continue
            address = wallet.address.lower()
            selected = address in candidate.paid_wallet_addresses
            qty = candidate.paid_wallet_quantities.get(address, 1)
            rows.append([
                (("✅ " if selected else "⬜ ") + wallet.name, f"pwm:{token}:{stored.id}"),
                (f"🔢 {qty}", f"pqq:{token}:{stored.id}"),
            ])
            shown += 1
            if shown >= 25:
                break
        rows.append([("✅ تحديد الكل", f"pwa:{token}"), ("🧹 إلغاء التحديد", f"pac:{token}")])
        rows.append([("🚀 تأكيد خطة الشراء", f"pwc:{token}")])
        rows.append([("🚫 لا أريد شراء المدفوع", f"pwn:{token}")])
        rows.append([("↩️ المنتات المدفوعة", "paid_watches")])
        return rows

    def paid_watches_buttons(self) -> list[list[tuple[str, str]]]:
        rows: list[list[tuple[str, str]]] = []
        shown = 0
        for candidate in self.candidates.values():
            if candidate.paid_detected or candidate.has_paid_stage or candidate.paid_decision:
                icon = {"confirmed": "✅", "declined": "🚫", "pending": "⏳"}.get(candidate.paid_decision, "💳")
                rows.append([(f"{icon} {candidate.slug}", f"pwo:{self.candidate_token(candidate)}")])
                shown += 1
                if shown >= 40:
                    break
        rows.append([("↩️ المراقبة", "monitoring_menu"), ("🏠 الرئيسية", "menu")])
        return rows

    def edit_or_send(self, event: dict[str, Any], text: str, buttons=None) -> None:
        chat_id = event["chat_id"]
        message_id = int(event.get("message_id", 0) or 0)
        if message_id:
            self.telegram.edit(chat_id, message_id, text, buttons)
        else:
            self.telegram.send(chat_id, text, buttons)

    def _clear_pending(self, chat_id: str) -> None:
        self.pending_wallet_name.pop(chat_id, None)
        self.pending_wallet_import.pop(chat_id, None)
        self.pending_wallet_rename.pop(chat_id, None)
        self.pending_wallet_quantity.pop(chat_id, None)
        self.pending_paid_quantity.pop(chat_id, None)
        self.pending_link_action.pop(chat_id, None)

    def handle_callback(self, event: dict[str, Any]) -> None:
        chat_id = event["chat_id"]
        data = event.get("data", "")
        self.telegram.answer_callback(event.get("callback_id", ""))

        if data == "menu":
            self.edit_or_send(event, "🏠 القائمة الرئيسية", self.menu_buttons())
            return

        if data == "add_wallet":
            if event.get("chat_type") != "private":
                self.telegram.send(chat_id, "🔐 إضافة المحفظة متاحة فقط في محادثة خاصة مع البوت.")
                return
            self._clear_pending(chat_id)
            self.pending_wallet_name[chat_id] = time.time() + 180
            self.telegram.send(
                chat_id,
                "➕ إضافة محفظة جديدة\n\n"
                "أرسل الآن اسمًا للمحفظة حتى تميزها بسهولة.\n"
                "مثال: محفظة جواد 1 أو Ink-01\n\n"
                "أرسل /cancel للإلغاء.",
            )
            return

        if data == "wallets":
            self.edit_or_send(event, self.wallets_text(), self.wallet_list_buttons())
            return

        if data.startswith("wv:"):
            try:
                wallet_id = int(data.split(":", 1)[1])
            except ValueError:
                return
            self.edit_or_send(event, self.wallet_detail_text(wallet_id), self.wallet_detail_buttons(wallet_id))
            return

        if data.startswith("wt:"):
            try:
                wallet_id = int(data.split(":", 1)[1])
            except ValueError:
                return
            wallet = self.store.get_wallet_by_id(wallet_id)
            if not wallet:
                self.telegram.send(chat_id, "⚠️ لم يتم العثور على المحفظة.")
                return
            new_state = not wallet.enabled
            self.store.set_wallet_enabled_by_id(wallet_id, new_state)
            self.reload_wallets()
            updated = self.store.get_wallet_by_id(wallet_id)
            action = "تشغيل" if new_state else "إيقاف"
            self.notify_all(f"{'▶️' if new_state else '⏸'} تم {action} المحفظة «{wallet.name}».")
            self.edit_or_send(event, self.wallet_detail_text(wallet_id), self.wallet_detail_buttons(wallet_id))
            if new_state and self.candidates:
                for candidate in self.candidates.values():
                    state = candidate.wallets.get(updated.address.lower()) if updated else None
                    if state:
                        state.next_attempt = time.time()
            return

        if data.startswith("wr:"):
            try:
                wallet_id = int(data.split(":", 1)[1])
            except ValueError:
                return
            wallet = self.store.get_wallet_by_id(wallet_id)
            if not wallet:
                return
            self._clear_pending(chat_id)
            self.pending_wallet_rename[chat_id] = {"expiry": time.time() + 180, "wallet_id": wallet_id}
            self.telegram.send(chat_id, f"✏️ أرسل الاسم الجديد للمحفظة «{wallet.name}».\nأرسل /cancel للإلغاء.")
            return

        if data.startswith("wq:"):
            try:
                wallet_id = int(data.split(":", 1)[1])
            except ValueError:
                return
            wallet = self.store.get_wallet_by_id(wallet_id)
            if not wallet:
                return
            self._clear_pending(chat_id)
            self.pending_wallet_quantity[chat_id] = {"expiry": time.time() + 180, "wallet_id": wallet_id}
            self.telegram.send(
                chat_id,
                f"🔢 الكمية الحالية للمحفظة «{wallet.name}» هي {wallet.quantity}.\n"
                "أرسل كمية جديدة من 1 إلى 100.\nأرسل /cancel للإلغاء.",
            )
            return

        if data.startswith("wd:"):
            try:
                wallet_id = int(data.split(":", 1)[1])
            except ValueError:
                return
            wallet = self.store.get_wallet_by_id(wallet_id)
            if not wallet:
                return
            self.telegram.send(
                chat_id,
                f"⚠️ هل تريد حذف المحفظة «{wallet.name}» نهائيًا من قاعدة بيانات البوت؟",
                [[("🗑 نعم، احذف", f"wdc:{wallet_id}"), ("❌ إلغاء", f"wv:{wallet_id}")]],
            )
            return

        if data.startswith("wdc:"):
            try:
                wallet_id = int(data.split(":", 1)[1])
            except ValueError:
                return
            wallet = self.store.get_wallet_by_id(wallet_id)
            if wallet and self.store.delete_wallet_by_id(wallet_id):
                self.reload_wallets()
                self.telegram.send(chat_id, f"🗑 تم حذف المحفظة «{wallet.name}».", self.wallet_list_buttons())
            else:
                self.telegram.send(chat_id, "⚠️ لم يتم العثور على المحفظة.", self.wallet_list_buttons())
            return

        # ----- V4.4 qualification / monitoring menus -----
        if data == "qualification_menu":
            self.edit_or_send(
                event,
                "🎟 قسم التأهيل\n\n"
                "يعرض مراحل التأهيل ونتيجة كل محفظة نشطة، ويحفظ جدول المراحل حتى إعادة التشغيل.",
                self.qualification_menu_buttons(),
            )
            return
        if data == "qualification_today":
            self.edit_or_send(event, self.qualification_today_text(), self.qualification_menu_buttons())
            return
        if data == "qualification_old":
            self.edit_or_send(event, self.qualification_old_text(), self.qualification_menu_buttons())
            return
        if data == "qualification_add":
            self._clear_pending(chat_id)
            self.pending_link_action[chat_id] = {"expiry": time.time() + 300, "action": "qualification"}
            self.telegram.send(
                chat_id,
                "🎟 فحص تأهيل رابط\n\n"
                "أرسل رابط الـMint/المجموعة على OpenSea.\n"
                "سأقرأ جدول المراحل، أفحص جميع المحافظ النشطة للمرحلة الحالية، "
                "وأحفظ المشروع للمراحل القادمة إذا كان يحتاج مراقبة.\n\n"
                "أرسل /cancel للإلغاء.",
            )
            return

        if data == "monitoring_menu":
            self.edit_or_send(
                event,
                "👀 قسم المراقبة\n\n"
                "المشاريع المحفوظة هنا تستمر مراقبتها عبر Restart/Redeploy حتى انتهاء مراحلها.",
                self.monitoring_menu_buttons(),
            )
            return
        if data == "monitoring_active" or data == "watch_status":
            self.edit_or_send(event, self.monitoring_active_text(), self.monitoring_menu_buttons())
            return
        if data == "monitoring_old":
            self.edit_or_send(event, self.monitoring_old_text(), self.monitoring_menu_buttons())
            return
        if data in {"monitoring_add", "watches"}:
            self._clear_pending(chat_id)
            self.pending_link_action[chat_id] = {"expiry": time.time() + 300, "action": "watch"}
            self.telegram.send(
                chat_id,
                "👀 مراقبة منت\n\nأرسل الآن رابط الـMint/المجموعة على OpenSea.\n"
                "سأفحص جدول المراحل وأحفظه للمراقبة. في كل مرحلة سأعيد فحص المحافظ النشطة.\n"
                f"إذا كان الـPublic مجانيًا سأبدأ الاستعداد قبل الفتح بـ {self.public_preopen_window_seconds:g} ثوانٍ "
                "وأحاول التنفيذ لجميع المحافظ النشطة فور السماح به.\n"
                "أما Public المدفوع فلن يُشترى إلا بعد موافقتك وتحديد المحافظ والكميات.\n\n"
                "أرسل /cancel للإلغاء.",
            )
            return

        if data == "free_mints":
            self.edit_or_send(event, self.free_mints_text(), self.free_mints_buttons())
            return

        if data == "chains":
            self.edit_or_send(event, self.chains_text(), self.menu_buttons())
            return
        if data == "history":
            self.edit_or_send(event, self.history_text(), self.menu_buttons())
            return
        if data == "settings":
            self.edit_or_send(event, self.settings_text(), self.menu_buttons())
            return
        if data == "eligibility_all":
            self._clear_pending(chat_id)
            self.pending_link_action[chat_id] = {"expiry": time.time() + 300, "action": "eligibility"}
            self.telegram.send(
                chat_id,
                "🧪 فحص الأهلية\n\nأرسل رابط الـMint/المجموعة على OpenSea.\n"
                "سأفحص جميع المحافظ النشطة للمرحلة المتاحة الآن فقط، بدون إضافة المشروع إلى المراقبة "
                "وبدون توقيع أو إرسال أي معاملة.\n\nأرسل /cancel للإلغاء.",
            )
            return
        if data == "toggle_pause":
            self.paused = not self.paused
            self.edit_or_send(
                event,
                "⏸ تم إيقاف توقيع وإرسال معاملات الـMint. الاكتشاف والمراقبة مستمران."
                if self.paused else
                "▶️ تم استئناف توقيع وإرسال معاملات الـMint.",
                self.menu_buttons(),
            )
            return

        # ----- V4.4 scheduled paid-public planner -----
        if data == "paid_watches":
            paid = [
                c for c in self.candidates.values()
                if c.paid_detected or c.has_paid_stage or c.paid_decision
            ]
            if paid:
                lines = [
                    "💳 المنتات المدفوعة",
                    "",
                    "هذه القائمة لا تُرسل تلقائيًا؛ تظهر فقط عند فتح هذا القسم.",
                    "اختر مشروعًا لمراجعة السعر، وقت الفتح، المحافظ والكميات.",
                    "",
                ]
                for candidate in paid[:20]:
                    lines.append(self.paid_list_line(candidate))
                text = "\n".join(lines)[:3900]
            else:
                text = "💳 لا توجد Public Mints مدفوعة مكتشفة/مجدولة حاليًا."
            self.edit_or_send(event, text, self.paid_watches_buttons())
            return

        if data.startswith("ppo:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                self.telegram.send(chat_id, "⚠️ لم تعد هذه المراقبة موجودة.")
                return
            candidate.paid_decision = "pending"
            candidate.paid_detected = True
            candidate.paid_selection_confirmed = False
            self.persist_paid_plan(candidate, "pending")
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
            return

        if data.startswith("ppn:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            candidate.paid_wallet_addresses.clear()
            candidate.paid_wallet_quantities.clear()
            candidate.paid_decision = "declined"
            candidate.paid_selection_confirmed = False
            self.persist_paid_plan(candidate, "declined")
            self.edit_or_send(
                event,
                f"🚫 تم رفض شراء Public Mint المدفوع للمشروع {candidate.slug}.\n"
                "ستستمر مراقبة بقية المراحل، وأي Public مجاني سيبقى تلقائيًا لجميع المحافظ النشطة.",
                self.paid_watches_buttons(),
            )
            return

        if data.startswith("pwo:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                self.telegram.send(chat_id, "⚠️ لم تعد هذه المراقبة موجودة.")
                return
            if not candidate.paid_decision:
                candidate.paid_decision = "pending"
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
            return

        if data.startswith("pwm:"):
            parts = data.split(":")
            if len(parts) != 3:
                return
            candidate = self.candidate_by_token(parts[1])
            if not candidate:
                return
            try:
                wallet_id = int(parts[2])
            except ValueError:
                return
            wallet = self.store.get_wallet_by_id(wallet_id)
            if not wallet or not wallet.enabled or wallet.address.lower() not in candidate.wallets:
                self.telegram.answer_callback(event.get("callback_id", ""), "المحفظة غير نشطة أو لا تعمل على هذه الشبكة")
                return
            address = wallet.address.lower()
            if address in candidate.paid_wallet_addresses:
                candidate.paid_wallet_addresses.remove(address)
                candidate.paid_wallet_quantities.pop(address, None)
            else:
                candidate.paid_wallet_addresses.add(address)
                candidate.paid_wallet_quantities.setdefault(address, 1)
            candidate.paid_decision = "pending"
            candidate.paid_selection_confirmed = False
            self.persist_paid_plan(candidate, "pending")
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
            return

        if data.startswith("pqq:"):
            parts = data.split(":")
            if len(parts) != 3:
                return
            candidate = self.candidate_by_token(parts[1])
            if not candidate:
                return
            try:
                wallet_id = int(parts[2])
            except ValueError:
                return
            wallet = self.store.get_wallet_by_id(wallet_id)
            if not wallet or not wallet.enabled or wallet.address.lower() not in candidate.wallets:
                self.telegram.send(chat_id, "⚠️ المحفظة غير نشطة أو غير مناسبة لهذه الشبكة.")
                return
            self._clear_pending(chat_id)
            address = wallet.address.lower()
            candidate.paid_wallet_addresses.add(address)
            candidate.paid_wallet_quantities.setdefault(address, 1)
            candidate.paid_decision = "pending"
            self.persist_paid_plan(candidate, "pending")
            self.pending_paid_quantity[chat_id] = {
                "expiry": time.time() + 180,
                "token": parts[1],
                "wallet_id": wallet_id,
            }
            cap = self.paid_quantity_cap(candidate)
            self.telegram.send(
                chat_id,
                f"🔢 كمية المنت المدفوع للمحفظة «{wallet.name}»\n"
                f"أرسل الكمية المطلوبة من 1 إلى {cap}.\n"
                "لن يتم الشراء حتى تؤكد الخطة النهائية. أرسل /cancel للإلغاء.",
            )
            return

        if data.startswith("pwa:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
            candidate.paid_wallet_addresses = {
                state.wallet.address.lower() for state in candidate.wallets.values()
                if state.wallet.address.lower() in active_addresses
            }
            for address in candidate.paid_wallet_addresses:
                candidate.paid_wallet_quantities.setdefault(address, 1)
            candidate.paid_decision = "pending"
            candidate.paid_selection_confirmed = False
            self.persist_paid_plan(candidate, "pending")
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
            return

        if data.startswith("pac:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            candidate.paid_wallet_addresses.clear()
            candidate.paid_wallet_quantities.clear()
            candidate.paid_decision = "pending"
            candidate.paid_selection_confirmed = False
            self.persist_paid_plan(candidate, "pending")
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
            return

        if data.startswith("pwc:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            enabled_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
            candidate.paid_wallet_addresses &= enabled_addresses
            candidate.paid_wallet_addresses &= set(candidate.wallets.keys())
            if not candidate.paid_wallet_addresses:
                self.telegram.send(chat_id, "⚠️ حدد محفظة نشطة واحدة على الأقل قبل تأكيد الشراء.")
                return
            cap = self.paid_quantity_cap(candidate)
            for address in list(candidate.paid_wallet_addresses):
                candidate.paid_wallet_quantities[address] = max(1, min(int(candidate.paid_wallet_quantities.get(address, 1)), cap))
            candidate.paid_decision = "confirmed"
            candidate.paid_selection_confirmed = True
            candidate.paid_selection_notified = True
            self.persist_paid_plan(candidate, "confirmed")
            plan = self.paid_plan_for_candidate(candidate)
            probe_at = time.time()
            if plan and plan.get("start") is not None:
                probe_at = max(time.time(), float(plan["start"]) - self.public_preopen_window_seconds)
            for state in candidate.wallets.values():
                if state.wallet.address.lower() in candidate.paid_wallet_addresses and not state.submitted:
                    state.final = False
                    state.next_attempt = probe_at
            names = []
            for state in candidate.wallets.values():
                address = state.wallet.address.lower()
                if address in candidate.paid_wallet_addresses:
                    names.append(f"• {state.wallet.name}: {candidate.paid_wallet_quantities[address]}")
            self.edit_or_send(
                event,
                f"✅ تم تأكيد خطة شراء {candidate.slug}.\n"
                f"وقت الفتح: {format_ts(candidate.paid_stage_start, self.display_tz)}\n\n"
                + "\n".join(names)
                + "\n\nسيبدأ الاستعداد قبل الفتح مباشرة، مع تطبيق سقف الغاز والسعر قبل التوقيع.",
                self.paid_watches_buttons(),
            )
            return

        if data.startswith("pwn:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            candidate.paid_wallet_addresses.clear()
            candidate.paid_wallet_quantities.clear()
            candidate.paid_decision = "declined"
            candidate.paid_selection_confirmed = False
            candidate.paid_detected = True
            self.persist_paid_plan(candidate, "declined")
            self.edit_or_send(
                event,
                f"🚫 لن يتم شراء Public Mint المدفوع للمشروع {candidate.slug}.\n"
                "المراقبة ستستمر لبقية المراحل وأي Public مجاني لاحق.",
                self.paid_watches_buttons(),
            )
            return

    def handle_message(self, event: dict[str, Any]) -> None:
        chat_id = event["chat_id"]
        text = event.get("text", "").strip()

        if text.lower() == "/cancel":
            self._clear_pending(chat_id)
            self.telegram.send(chat_id, "تم إلغاء العملية الحالية.", self.menu_buttons())
            return

        pending_paid_qty = self.pending_paid_quantity.get(chat_id)
        if pending_paid_qty:
            if time.time() > float(pending_paid_qty.get("expiry", 0)):
                self.pending_paid_quantity.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة إدخال كمية المنت المدفوع.", self.menu_buttons())
                return
            candidate = self.candidate_by_token(str(pending_paid_qty.get("token") or ""))
            wallet = self.store.get_wallet_by_id(int(pending_paid_qty.get("wallet_id") or 0))
            if not candidate or not wallet or not wallet.enabled:
                self.pending_paid_quantity.pop(chat_id, None)
                self.telegram.send(chat_id, "⚠️ لم تعد خطة المنت أو المحفظة متاحة.", self.menu_buttons())
                return
            try:
                quantity = int(text)
            except ValueError:
                self.telegram.send(chat_id, "⚠️ أرسل رقمًا صحيحًا للكمية أو /cancel للإلغاء.")
                return
            cap = self.paid_quantity_cap(candidate)
            if quantity < 1 or quantity > cap:
                self.telegram.send(chat_id, f"⚠️ الكمية يجب أن تكون من 1 إلى {cap} لهذه المرحلة.")
                return
            address = wallet.address.lower()
            candidate.paid_wallet_addresses.add(address)
            candidate.paid_wallet_quantities[address] = quantity
            candidate.paid_decision = "pending"
            candidate.paid_selection_confirmed = False
            self.persist_paid_plan(candidate, "pending")
            self.pending_paid_quantity.pop(chat_id, None)
            self.telegram.send(
                chat_id,
                f"✅ تم ضبط «{wallet.name}» على كمية {quantity}.\n\n" + self.paid_selector_text(candidate),
                self.paid_selector_buttons(candidate),
            )
            return

        pending_link = self.pending_link_action.get(chat_id)
        if pending_link:
            if time.time() > float(pending_link.get("expiry", 0)):
                self.pending_link_action.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة إدخال الرابط. اضغط الزر وحاول مجددًا.", self.menu_buttons())
                return
            action = str(pending_link.get("action") or "")
            self.pending_link_action.pop(chat_id, None)
            if action == "eligibility":
                self.telegram.send(chat_id, "🧪 جارٍ فحص الرابط وأهلية المحافظ النشطة...")
                self.telegram.send(chat_id, self.check_link_eligibility(text), self.menu_buttons())
                return
            if action == "qualification":
                self.telegram.send(chat_id, "🎟 جارٍ قراءة المراحل وفحص تأهيل جميع المحافظ النشطة...")
                self.telegram.send(chat_id, self.check_and_track_qualification_link(text), self.qualification_menu_buttons())
                return
            if action == "watch":
                ok, message, candidate = self.add_watch(text, persist=True)
                self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.monitoring_menu_buttons())
                if ok and candidate:
                    self.process_stage_schedule(candidate)
                    current = self.current_plan_for_candidate(candidate)
                    if current:
                        result = self.stage_qualification_check(candidate, current, notify=False, force=True)
                        if result:
                            self.telegram.send(chat_id, result)
                    else:
                        future = self.next_plan_for_candidate(candidate)
                        if future:
                            self.telegram.send(
                                chat_id,
                                f"⏳ لا توجد مرحلة نشطة الآن. أقرب مرحلة: {future.get('label')} — "
                                f"{format_ts(future.get('start'), self.display_tz)}. تم ضبط إعادة الفحص تلقائيًا عند وقتها.",
                            )
                    self.maybe_offer_paid_public(candidate)
                return

        expiry = self.pending_wallet_name.get(chat_id)
        if expiry:
            if time.time() > expiry:
                self.pending_wallet_name.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة إضافة المحفظة. اضغط «➕ إضافة محفظة» وحاول مجددًا.", self.menu_buttons())
                return
            ok, clean_or_error = self.validate_wallet_name(text)
            if not ok:
                self.telegram.send(chat_id, f"⚠️ {clean_or_error}\nأرسل اسمًا آخر أو /cancel للإلغاء.")
                return
            self.pending_wallet_name.pop(chat_id, None)
            self.pending_wallet_import[chat_id] = {"expiry": time.time() + 180, "name": clean_or_error}
            self.telegram.send(
                chat_id,
                f"✅ الاسم: «{clean_or_error}»\n\n"
                "🔐 أرسل الآن PRIVATE KEY للمحفظة المخصصة للـMint.\n"
                "سيتم التحقق منه وتشفيره داخل قاعدة بيانات Railway ومحاولة حذف رسالتك مباشرة.\n"
                "استخدم محفظة Mint مخصصة ولا تستخدم محفظتك الرئيسية.\n\n"
                "أرسل /cancel للإلغاء.",
            )
            return

        pending_import = self.pending_wallet_import.get(chat_id)
        if pending_import:
            if time.time() > float(pending_import.get("expiry", 0)):
                self.pending_wallet_import.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة إدخال المفتاح. ابدأ إضافة المحفظة من جديد.", self.menu_buttons())
                return
            self.pending_wallet_import.pop(chat_id, None)
            self.telegram.delete_message(chat_id, int(event.get("message_id", 0)))
            ok, message = self.add_wallet_key(str(pending_import.get("name", "محفظة")), text)
            self.telegram.send(chat_id, message, self.wallet_list_buttons() if ok else self.menu_buttons())
            if ok and self.candidates:
                self.telegram.send(chat_id, self.all_eligibility_text())
                for candidate in self.candidates.values():
                    if candidate.paid_detected and not candidate.paid_selection_confirmed:
                        candidate.paid_selection_notified = False
                        self.notify_paid_selection(candidate)
            return

        pending_rename = self.pending_wallet_rename.get(chat_id)
        if pending_rename:
            if time.time() > float(pending_rename.get("expiry", 0)):
                self.pending_wallet_rename.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة تغيير الاسم.", self.menu_buttons())
                return
            wallet_id = int(pending_rename["wallet_id"])
            ok, clean_or_error = self.validate_wallet_name(text, exclude_id=wallet_id)
            if not ok:
                self.telegram.send(chat_id, f"⚠️ {clean_or_error}\nأرسل اسمًا آخر أو /cancel للإلغاء.")
                return
            self.pending_wallet_rename.pop(chat_id, None)
            self.store.set_wallet_name(wallet_id, clean_or_error)
            self.reload_wallets()
            self.telegram.send(chat_id, f"✅ تم تغيير اسم المحفظة إلى «{clean_or_error}».", self.wallet_detail_buttons(wallet_id))
            return

        pending_qty = self.pending_wallet_quantity.get(chat_id)
        if pending_qty:
            if time.time() > float(pending_qty.get("expiry", 0)):
                self.pending_wallet_quantity.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة تغيير الكمية.", self.menu_buttons())
                return
            try:
                quantity = int(text)
            except ValueError:
                self.telegram.send(chat_id, "⚠️ أرسل رقمًا صحيحًا من 1 إلى 100 أو /cancel للإلغاء.")
                return
            if quantity < 1 or quantity > 100:
                self.telegram.send(chat_id, "⚠️ الكمية يجب أن تكون من 1 إلى 100.")
                return
            wallet_id = int(pending_qty["wallet_id"])
            self.pending_wallet_quantity.pop(chat_id, None)
            self.store.set_wallet_quantity(wallet_id, quantity)
            self.reload_wallets()
            self.telegram.send(chat_id, f"✅ تم ضبط كمية الـMint الافتراضية على {quantity}.", self.wallet_detail_buttons(wallet_id))
            return

        parts = text.split()
        command = parts[0].lower().split("@", 1)[0] if parts else ""
        if command in {"/start", "/help", "/menu"} or text in {"القائمة", "الرئيسية"}:
            self.send_menu(chat_id)
            return
        if command == "/qualification":
            self.telegram.send(chat_id, self.qualification_today_text(), self.qualification_menu_buttons())
            return
        if command == "/status" or text == "المراقبات":
            self.telegram.send(chat_id, self.monitoring_active_text(), self.monitoring_menu_buttons())
            return
        if command == "/wallets" or text == "المحافظ":
            self.telegram.send(chat_id, self.wallets_text(), self.wallet_list_buttons())
            return
        if command == "/chains" or text == "الشبكات":
            self.telegram.send(chat_id, self.chains_text(), self.menu_buttons())
            return
        if command == "/free":
            self.telegram.send(chat_id, self.free_mints_text(), self.free_mints_buttons())
            return
        if command == "/paid":
            paid = [c for c in self.candidates.values() if c.paid_detected or c.has_paid_stage or c.paid_decision]
            if paid:
                lines = ["💳 المنتات المدفوعة", ""] + [self.paid_list_line(c) for c in paid[:20]]
                text_paid = "\n".join(lines)[:3900]
            else:
                text_paid = "💳 لا توجد Public Mints مدفوعة مكتشفة/مجدولة حاليًا."
            self.telegram.send(chat_id, text_paid, self.paid_watches_buttons())
            return
        if command == "/history" or text == "السجل":
            self.telegram.send(chat_id, self.history_text(), self.menu_buttons())
            return
        if command in {"/pause", "/panic"}:
            self.paused = True
            self.telegram.send(chat_id, "⏸ تم إيقاف توقيع وإرسال معاملات الـMint. المراقبة مستمرة.", self.menu_buttons())
            return
        if command == "/resume":
            self.paused = False
            self.telegram.send(chat_id, "▶️ تم استئناف توقيع وإرسال معاملات الـMint.", self.menu_buttons())
            return
        if command == "/eligibility":
            if len(parts) >= 2:
                self.telegram.send(chat_id, self.check_link_eligibility(" ".join(parts[1:])), self.menu_buttons())
            else:
                self._clear_pending(chat_id)
                self.pending_link_action[chat_id] = {"expiry": time.time() + 300, "action": "eligibility"}
                self.telegram.send(chat_id, "🧪 أرسل الآن رابط الـMint الذي تريد فحص أهلية المحافظ له.\nأرسل /cancel للإلغاء.")
            return
        if command == "/remove" and len(parts) >= 2:
            self.telegram.send(chat_id, self.remove_watch(parts[1]), self.menu_buttons())
            return
        if command == "/deletewallet" and len(parts) >= 2:
            address = parts[1].strip()
            if not Web3.is_address(address):
                self.telegram.send(chat_id, "عنوان المحفظة غير صالح.", self.menu_buttons())
                return
            wallet = self.store.get_wallet_by_address(Web3.to_checksum_address(address))
            deleted = self.store.delete_wallet(Web3.to_checksum_address(address))
            self.reload_wallets()
            self.telegram.send(
                chat_id,
                f"🗑 تم حذف المحفظة «{wallet.name}»." if deleted and wallet else "لم يتم العثور على المحفظة.",
                self.menu_buttons(),
            )
            return
        if command == "/watch":
            if len(parts) < 2:
                self._clear_pending(chat_id)
                self.pending_link_action[chat_id] = {"expiry": time.time() + 300, "action": "watch"}
                self.telegram.send(chat_id, "👀 أرسل الآن رابط الـMint الذي تريد مراقبته حتى فتح الـPublic.\nأرسل /cancel للإلغاء.")
                return
            forced_chain = None
            raw = " ".join(parts[1:])
            maybe = normalize_chain(parts[1])
            if maybe in CHAIN_CONFIGS and len(parts) >= 3:
                forced_chain = maybe
                raw = " ".join(parts[2:])
            ok, message, candidate = self.add_watch(raw, forced_chain=forced_chain, persist=True)
            self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.menu_buttons())
            if ok and candidate:
                self.process_stage_schedule(candidate)
                current = self.current_plan_for_candidate(candidate)
                if current:
                    result = self.stage_qualification_check(candidate, current, notify=False, force=True)
                    if result:
                        self.telegram.send(chat_id, result)
                self.maybe_offer_paid_public(candidate)
            return

        if "opensea.io" in text.lower() or re.fullmatch(r"[A-Za-z0-9._-]{2,200}", text):
            ok, message, candidate = self.add_watch(text, persist=True)
            self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.monitoring_menu_buttons())
            if ok and candidate:
                self.process_stage_schedule(candidate)
                current = self.current_plan_for_candidate(candidate)
                if current:
                    result = self.stage_qualification_check(candidate, current, notify=False, force=True)
                    if result:
                        self.telegram.send(chat_id, result)
                self.maybe_offer_paid_public(candidate)
            return

        self.telegram.send(chat_id, "أرسل رابط OpenSea أو استخدم أزرار القائمة الرئيسية.", self.menu_buttons())

    def drain_commands(self) -> None:
        while True:
            try:
                event = self.command_queue.get_nowait()
            except queue.Empty:
                return
            try:
                if event.get("type") == "callback":
                    self.handle_callback(event)
                else:
                    self.handle_message(event)
            except Exception as exc:
                log.exception("Telegram command failed")
                self.telegram.send(event.get("chat_id", ""), f"⚠️ تعذر تنفيذ الأمر: {exc}")

    def notify_all(self, text: str) -> None:
        if not self.telegram.enabled:
            return
        for chat_id in self.telegram.allowed_chat_ids:
            self.telegram.send(chat_id, text)

    # ---------- main loop ----------
    def run(self) -> None:
        start_health_server()
        log.info("Mint Guardian V4.5 starting")
        log.info("Chains: %s", ", ".join(self.enabled_chains))
        log.info("Wallets: %s | paid=%s | native gas cap=%s | USD gas cap=$%s | mint price cap=%s",
                 len(self.wallets), self.allow_paid_default, self.max_gas_native, self.max_gas_usd, self.max_mint_price_default)
        if self.telegram.enabled:
            self.telegram.start()
        self.bootstrap_watches()
        self.start_auto_free_discovery()
        log.info(
            "Stage planner ready | qualification recheck=%.1fs | public preopen=%.1fs | public retry=%.2fs | high/unknown qty=%s",
            self.qualification_recheck_seconds, self.public_preopen_window_seconds,
            self.public_fast_retry_seconds, self.auto_stage_high_limit_quantity,
        )
        self.notify_all(
            "🟢 OpenSea Mint Guardian V4.5 يعمل الآن على Railway.\n"
            f"الاكتشاف التلقائي: {'مفعّل كل ' + format(self.auto_free_scan_seconds, 'g') + ' ثانية' if self.auto_free_enabled else 'متوقف'}.\n"
            f"OpenSea Stream: {'مفعّل' if self.auto_stream_enabled else 'متوقف'} | REST Mint Events: {'مفعّل' if self.auto_event_fallback_enabled else 'متوقف'}.\n"
            f"التأهيل: إعادة فحص كل {self.qualification_recheck_seconds:g} ثانية أثناء المرحلة.\n"
            f"Public: يبدأ الاستعداد قبل الفتح بـ {self.public_preopen_window_seconds:g} ثوانٍ.\n"
            f"سياسة الكمية: الحد ≤{self.auto_stage_high_limit_threshold} كهدف؛ أعلى/غير محدود = {self.auto_stage_high_limit_quantity}.\n"
            "تمت استعادة المراقبات وخطط المدفوع والمحافظ النشطة بنجاح."
        )
        while not STOP:
            self.drain_commands()
            self.drain_auto_discovery()
            self.cleanup_auto_candidates()
            for key, candidate in list(self.candidates.items()):
                self.drain_commands()
                self.refresh_candidate(candidate)
                self.drain_commands()
                self.process_stage_schedule(candidate)
                self.drain_commands()
                self.try_candidate(candidate)
                self.drain_commands()
                self.check_receipts(candidate)
                if candidate.done:
                    pending = any(s.submitted and not s.confirmed for s in candidate.wallets.values())
                    if not pending:
                        self.candidates.pop(key, None)
            time.sleep(0.12)
        log.info("Stopped")


if __name__ == "__main__":
    try:
        Bot().run()
    except Exception as exc:
        log.exception("Fatal error: %s", exc)
        sys.exit(1)

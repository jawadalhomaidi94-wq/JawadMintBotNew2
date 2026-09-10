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
    build_fee_fields,
    default_rpcs,
    explorer_tx_url,
    mint_drop,
    mint_seadrop_public,
    prepare_seadrop_race_transactions,
    broadcast_seadrop_race_transactions,
    read_seadrop_public_fast,
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


class _SecretRedactionFilter(logging.Filter):
    """Redact configured service credentials from every emitted log line."""
    def __init__(self) -> None:
        super().__init__()
        names = ("ALCHEMY_API_KEY", "OPENSEA_API_KEY", "TELEGRAM_BOT_TOKEN", "WALLET_ENCRYPTION_KEY")
        self.secrets = [os.getenv(name, "").strip() for name in names if os.getenv(name, "").strip()]

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        for secret in self.secrets:
            if len(secret) >= 6:
                message = message.replace(secret, "***REDACTED***")
        # Telegram bot tokens have a distinctive form; protect even if the
        # environment list above changes later.
        message = re.sub(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b", "***REDACTED_BOT_TOKEN***", message)
        record.msg = message
        record.args = ()
        return True


_secret_filter = _SecretRedactionFilter()
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_secret_filter)

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


def safe_endpoint_for_log(url: str) -> str:
    """Return a useful endpoint label without API credentials/query secrets."""
    try:
        parsed = urlparse(str(url or ""))
        if not parsed.scheme or not parsed.netloc:
            return "<rpc>"
        path = parsed.path or ""
        path = re.sub(r"(/v2/)[^/]+$", r"\1***", path, flags=re.IGNORECASE)
        return f"{parsed.scheme}://{parsed.netloc}{path}"
    except Exception:
        return "<rpc>"


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


def _clean_social_url(value: Any, *, allow_handle: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if allow_handle and re.fullmatch(r"@?[A-Za-z0-9_]{1,30}", text):
        return f"https://x.com/{text.lstrip('@')}"
    if text.startswith("//"):
        text = "https:" + text
    if not re.match(r"^https?://", text, re.I):
        if "." in text and " " not in text:
            text = "https://" + text.lstrip("/")
        else:
            return None
    try:
        parsed = urlparse(text)
    except Exception:
        return None
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    return text


def extract_collection_social_identity(payload: Any) -> tuple[str | None, str | None, str | None]:
    """Return (twitter_username, twitter_url, website_url) from OpenSea collection metadata.

    The V4.11 shield only verifies that OpenSea exposes at least one project
    identity link. It deliberately does not inspect follower counts/account age;
    those require a future X API integration.
    """
    if not isinstance(payload, dict):
        return None, None, None
    roots: list[dict[str, Any]] = [payload]
    for key in ("collection", "data", "result"):
        child = payload.get(key)
        if isinstance(child, dict):
            roots.append(child)

    twitter_username: str | None = None
    twitter_url: str | None = None
    website_url: str | None = None

    for root in roots:
        if twitter_username is None:
            raw_user = root.get("twitter_username") or root.get("twitterUsername") or root.get("x_username") or root.get("xUsername")
            if isinstance(raw_user, str) and raw_user.strip():
                twitter_username = raw_user.strip().lstrip("@")
                twitter_url = _clean_social_url(twitter_username, allow_handle=True)
        if twitter_url is None:
            for key in ("twitter_url", "twitterUrl", "x_url", "xUrl", "twitter", "x"):
                url = _clean_social_url(root.get(key), allow_handle=True)
                if url:
                    twitter_url = url
                    if twitter_username is None:
                        try:
                            host = urlparse(url).netloc.lower()
                            if host.endswith("x.com") or host.endswith("twitter.com"):
                                path = urlparse(url).path.strip("/").split("/", 1)[0]
                                if path:
                                    twitter_username = path
                        except Exception:
                            pass
                    break
        if website_url is None:
            for key in ("external_url", "externalUrl", "external_link", "externalLink", "website_url", "websiteUrl", "website", "homepage"):
                url = _clean_social_url(root.get(key))
                if not url:
                    continue
                try:
                    host = urlparse(url).netloc.lower().split(":", 1)[0]
                except Exception:
                    host = ""
                # The OpenSea collection page itself is not an external project website.
                if host == "opensea.io" or host.endswith(".opensea.io"):
                    continue
                website_url = url
                break
        links = root.get("links")
        if isinstance(links, dict):
            if twitter_url is None:
                for key in ("twitter", "x"):
                    url = _clean_social_url(links.get(key), allow_handle=True)
                    if url:
                        twitter_url = url
                        break
            if website_url is None:
                for key in ("website", "external_url", "homepage"):
                    url = _clean_social_url(links.get(key))
                    if url:
                        website_url = url
                        break
    return twitter_username, twitter_url, website_url


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
        # Never hold the cache lock while doing HTTP. The V4.9 race lane calls
        # peek_usd(), and a background price refresh must not make that hot
        # read wait behind a multi-second network request.
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
                            with self._lock:
                                self._cache[symbol] = (time.time(), value)
                            return value
        except Exception as exc:
            log.debug("Alchemy price lookup failed for %s: %s", symbol, exc)
        # A recent stale value is safer than disabling execution entirely
        # because of one transient HTTP error. Limit stale use to 10 minutes.
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and now - cached[0] <= 600:
                return cached[1]
        return None

    def peek_usd(self, symbol: str, *, max_age_seconds: float = 600.0) -> Decimal | None:
        """Return an already-cached price without performing network I/O.

        The hot mint path must never pause on an HTTP price request. A separate
        background warmer keeps this cache fresh.
        """
        symbol = symbol.upper().strip()
        if not symbol:
            return None
        now = time.time()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and now - cached[0] <= max(10.0, float(max_age_seconds)):
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
    schedule_summary_notified: bool = False
    stage_open_notified_keys: set[str] = field(default_factory=set)
    # V4.9 project-specific gas policy. None means inherit the saved chain/global setting.
    gas_override_usd: Decimal | None = None
    ignore_gas_cap: bool = False
    # V4.9 race-lane state. These fields prevent the background candidate loop
    # from racing the dedicated public-mint engine for the same stage.
    race_stage_key: str = ""
    race_inflight: bool = False
    race_last_attempt: float = 0.0
    race_prepared: bool = False
    # V4.11 protected-free social identity gate. This state is project-level,
    # never wallet-level, so one metadata lookup protects every active wallet.
    social_trust_status: str = "unknown"  # unknown | pending | passed | rejected | error
    social_trust_checked_at: float = 0.0
    social_twitter_username: str | None = None
    social_twitter_url: str | None = None
    social_website_url: str | None = None
    social_trust_detail: str = ""

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
        self.session = requests.Session()

    @property
    def enabled(self) -> bool:
        return bool(self.token)

    def api(self, method: str, **data: Any) -> dict[str, Any]:
        response = self.session.post(f"https://api.telegram.org/bot{self.token}/{method}", data=data, timeout=35)
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
            log.debug("Telegram send failed: %s", exc)

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
        except Exception:
            # Editing can fail if Telegram sees no content change; this is harmless.
            pass

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
        commands = [
            {"command": "start", "description": "فتح القائمة الرئيسية"},
            {"command": "wallets", "description": "عرض وإدارة المحافظ"},
            {"command": "status", "description": "المنتات تحت المراقبة"},
            {"command": "qualification", "description": "قسم التأهيل ومراحل اليوم"},
            {"command": "watch", "description": "إضافة رابط منت للمراقبة"},
            {"command": "eligibility", "description": "فحص أهلية رابط للمحافظ النشطة"},
            {"command": "free", "description": "المنتات المجانية التي تم أخذها"},
            {"command": "paid", "description": "المنتات المدفوعة المحفوظة"},
            {"command": "chains", "description": "حالة الشبكات والـRPC"},
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
                payload = self.api(
                    "getUpdates",
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
                    # Never log raw Telegram input: wallet private keys and other
                    # secrets are entered through this same channel. Only slash
                    # commands are safe/useful to identify in operational logs.
                    command_label = text.split(None, 1)[0] if text.startswith("/") else "<private-input>"
                    log.info("Telegram message received | chat_id=%s | input=%s | len=%s", chat_id, command_label, len(text))
                    self.bot.command_queue.put({
                        "type": "message",
                        "chat_id": chat_id,
                        "chat_type": str(chat.get("type", "")),
                        "message_id": int(message.get("message_id", 0)),
                        "text": text,
                    })
            except Exception as exc:
                log.debug("Telegram polling error: %s", exc)
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
        # V4.9: gas budgets are persisted in SQLite and editable from Telegram.
        # Environment values are only first-run fallbacks; after that the saved
        # settings are authoritative across Railway restarts/redeploys.
        def _stored_decimal(key: str, fallback: Decimal) -> Decimal:
            raw = self.store.get_setting(key, str(fallback))
            try:
                value = Decimal(str(raw))
            except (InvalidOperation, TypeError, ValueError):
                value = fallback
            return max(Decimal("0"), value)

        env_gas_default = env_decimal("MAX_GAS_USD", "0.08")
        self.max_gas_usd = _stored_decimal("gas_usd_global", env_gas_default)
        self.chain_gas_caps_usd: dict[str, Decimal | None] = {}
        for _chain in self.enabled_chains:
            # Per-chain limits are now true UI overrides. If no value has been
            # saved from Telegram, the chain inherits the global saved limit.
            _raw_chain = self.store.get_setting(f"gas_usd_{_chain}", None)
            if _raw_chain is None:
                self.chain_gas_caps_usd[_chain] = None
            else:
                try:
                    self.chain_gas_caps_usd[_chain] = max(Decimal("0"), Decimal(str(_raw_chain)))
                except (InvalidOperation, TypeError, ValueError):
                    self.chain_gas_caps_usd[_chain] = None
        self.gas_strategy = self.store.get_setting("gas_strategy", os.getenv("GAS_STRATEGY", "smart").strip().lower()) or "smart"
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
        self.low_balance_retry_seconds = max(1.0, env_float("LOW_BALANCE_RETRY_SECONDS", 2.0))
        self.max_parallel_wallets = max(1, env_int("MAX_PARALLEL_WALLETS", 10))
        self.drop_limit = max(1, min(env_int("DROP_LIMIT", 25), 100))

        # V4.2 automatic free-mint discovery. The list scan itself runs every
        # 15 seconds by default; manual links remain persistent watches.
        self.auto_free_enabled = env_bool("AUTO_FREE_MINTS", True)
        self.auto_free_scan_seconds = max(5.0, env_float("AUTO_FREE_SCAN_SECONDS", 15.0))
        self.auto_free_drop_limit = max(1, min(env_int("AUTO_FREE_DROP_LIMIT", 100), 100))
        self.auto_free_initial_pages = max(1, min(env_int("AUTO_FREE_INITIAL_PAGES", 1), 10))
        self.auto_free_detail_workers = max(1, min(env_int("AUTO_FREE_DETAIL_WORKERS", 4), 20))
        self.auto_free_notify_discovery = env_bool("AUTO_FREE_NOTIFY_DISCOVERY", False)
        self.auto_free_candidate_ttl = max(30.0, env_float("AUTO_FREE_CANDIDATE_TTL", 90.0))
        configured_types = csv_values(os.getenv("AUTO_FREE_DROP_TYPES", "recently_minted,featured,upcoming"))
        self.auto_free_drop_types = [x for x in configured_types if x in {"recently_minted", "featured", "upcoming"}] or ["recently_minted", "featured", "upcoming"]
        self.auto_discovery_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        # Live Stream mint events use a separate priority queue so a catalog
        # backfill cannot delay a fresh mint signal.
        self.auto_priority_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.auto_catalog_seen: dict[str, float] = {}
        self.auto_catalog_detail_ttl = max(10.0, env_float("AUTO_CATALOG_DETAIL_TTL", 60.0))
        self.auto_stream_fast_path = env_bool("AUTO_STREAM_FAST_PATH", True)
        self.auto_discovery_thread: threading.Thread | None = None
        # V4.3: real-time mint discovery from OpenSea Stream plus REST mint-event
        # polling fallback. Drops scanning stays enabled as a third discovery path.
        self.auto_stream_enabled = env_bool("AUTO_FREE_STREAM", True)
        self.auto_event_fallback_enabled = env_bool("AUTO_FREE_EVENT_FALLBACK", True)
        self.auto_event_limit = max(1, min(env_int("AUTO_FREE_EVENT_LIMIT", 200), 200))
        self.auto_event_overlap_seconds = max(5, env_int("AUTO_FREE_EVENT_OVERLAP_SECONDS", 45))
        self.auto_event_initial_lookback_seconds = max(300, env_int("AUTO_FREE_EVENT_INITIAL_LOOKBACK_SECONDS", 86400))
        self.auto_event_initial_pages = max(1, min(env_int("AUTO_FREE_EVENT_INITIAL_PAGES", 1), 10))
        self.auto_event_last_after = int(time.time()) - max(60, self.auto_event_overlap_seconds)
        self.auto_event_seen: dict[str, float] = {}
        # REST is a safety/backfill lane, not the speed lane. Stream is instant
        # and does not consume OpenSea REST quota, so background REST is paced.
        self.auto_event_scan_seconds = max(15.0, env_float("AUTO_EVENT_SCAN_SECONDS", 30.0))
        # Upcoming is the useful pre-open lane for paid/public stage schedules,
        # so scan it more often than the general catalog. Live free mints still
        # come from Stream first.
        self.auto_upcoming_scan_seconds = max(10.0, env_float("AUTO_UPCOMING_SCAN_SECONDS", 15.0))
        self.auto_drop_scan_seconds = max(30.0, env_float("AUTO_DROP_SCAN_SECONDS", 60.0))
        self.auto_catalog_detail_budget = max(1, min(env_int("AUTO_CATALOG_DETAIL_BUDGET", 12), 100))
        self._last_auto_event_scan = 0.0
        self._last_auto_upcoming_scan = 0.0
        self._last_auto_drop_scan = 0.0
        self.auto_stream_thread: threading.Thread | None = None

        # V4.4 stage planner / qualification engine.
        self.auto_stage_default_quantity = max(1, min(env_int("AUTO_STAGE_UNKNOWN_LIMIT_QTY", 30), 100))
        self.auto_stage_high_limit_threshold = max(1, env_int("AUTO_STAGE_HIGH_LIMIT_THRESHOLD", 100))
        self.auto_stage_high_limit_quantity = max(1, min(env_int("AUTO_STAGE_HIGH_LIMIT_QTY", 30), 100))
        self.stage_discovery_horizon_seconds = max(3600, env_int("STAGE_DISCOVERY_HORIZON_SECONDS", 604800))
        self.public_preopen_window_seconds = max(1.0, env_float("PUBLIC_PREOPEN_WINDOW_SECONDS", 5.0))
        self.public_fast_retry_seconds = max(0.02, env_float("PUBLIC_FAST_RETRY_SECONDS", 0.05))

        # V4.11 STABLE RACE LANE. Scheduled Public mints are pre-built/signed shortly
        # before opening and broadcast by a dedicated scheduler at the opening
        # timestamp. Live Stream/SeaDrop signals bypass discovery queues and
        # enter a separate executor immediately.
        self.race_enabled = env_bool("RACE_LANE_ENABLED", True)
        self.race_prewarm_seconds = max(0.30, env_float("RACE_PREWARM_SECONDS", 2.5))
        self.race_scheduler_tick = max(0.002, env_float("RACE_SCHEDULER_TICK", 0.005))
        self.race_retry_seconds = max(0.02, env_float("RACE_RETRY_SECONDS", 0.04))
        self.race_launch_window_seconds = max(1.0, env_float("RACE_LAUNCH_WINDOW_SECONDS", 8.0))
        self.race_static_gas_limit = max(80_000, env_int("RACE_PUBLIC_GAS_LIMIT", 300_000))
        self.race_open_offset_seconds = max(0.0, env_float("RACE_OPEN_OFFSET_MS", 0.0) / 1000.0)
        self.race_stream_workers = max(4, min(env_int("RACE_STREAM_WORKERS", 24), 64))
        self.race_prep_workers = max(2, min(env_int("RACE_PREP_WORKERS", 8), 32))
        self.race_launch_workers = max(2, min(env_int("RACE_LAUNCH_WORKERS", 8), 32))
        # V4.11.3 signal coalescer: the first live signal still enters immediately,
        # while duplicate Stream/SeaDrop events for the same contract/stage are
        # merged in RAM instead of consuming more race workers and RPC reads.
        self.race_signal_stream_quiet_seconds = max(0.20, env_float("RACE_SIGNAL_STREAM_QUIET_SECONDS", 1.50))
        self.race_signal_seadrop_quiet_seconds = max(0.10, env_float("RACE_SIGNAL_SEADROP_QUIET_SECONDS", 0.40))
        self.race_signal_unknown_quiet_seconds = max(0.05, env_float("RACE_SIGNAL_UNKNOWN_QUIET_SECONDS", 0.20))
        self.race_gas_strategy = os.getenv("RACE_GAS_STRATEGY", "fast").strip().lower()
        if self.race_gas_strategy not in {"economy", "balanced", "smart", "fast", "turbo"}:
            self.race_gas_strategy = "fast"
        # Hot market snapshots are refreshed outside the launch path. This
        # avoids spending precious milliseconds on fee/price HTTP/RPC reads
        # when a small public mint opens.
        self.race_fee_refresh_seconds = max(0.15, env_float("RACE_FEE_REFRESH_SECONDS", 0.35))
        self.race_price_refresh_seconds = max(10.0, env_float("RACE_PRICE_REFRESH_SECONDS", 30.0))
        self.seadrop_wss_enabled = env_bool("SEADROP_WSS_DISCOVERY", True)
        self.qualification_recheck_seconds = max(5.0, env_float("QUALIFICATION_RECHECK_SECONDS", 15.0))
        # OpenSea's wallet-specific mint builder is quota-sensitive. Direct
        # SeaDrop checks stay fully parallel, while OpenSea qualification calls
        # use a small worker cap to avoid a many-wallet 429 burst.
        self.opensea_eligibility_workers = max(1, min(env_int("OPENSEA_ELIGIBILITY_WORKERS", 2), 6))
        # V4.9: qualification/monitoring are quiet dashboards by default.
        # Routine stage messages are shown only when the user opens the section;
        # transaction submitted/confirmed/reverted notifications remain active.
        self.routine_stage_notifications = (self.store.get_setting("routine_stage_notifications", "0") == "1")
        self.stage_summary_notifications = self.routine_stage_notifications
        self.stage_open_notifications = self.routine_stage_notifications
        self.silent_rate_limit_telegram = True

        # V4.11 Protected Free Mint Shield. SQLite is authoritative so the
        # Telegram toggle survives Railway restart/redeploy. It is ON by default.
        self.free_social_protection_enabled = (self.store.get_setting("free_social_protection_enabled", "1") == "1")
        self.social_trust_pass_ttl = max(300.0, env_float("SOCIAL_TRUST_PASS_TTL_SECONDS", 86400.0))
        self.social_trust_reject_ttl = max(60.0, env_float("SOCIAL_TRUST_REJECT_TTL_SECONDS", 120.0))
        self.social_trust_error_retry = max(2.0, env_float("SOCIAL_TRUST_ERROR_RETRY_SECONDS", 5.0))
        self.social_trust_workers = max(1, min(env_int("SOCIAL_TRUST_WORKERS", 2), 6))
        self.social_trust_lock = threading.RLock()
        self.social_trust_inflight: set[str] = set()

        self.paused = env_bool("START_PAUSED", False)

        self.wallets: list[WalletConfig] = []
        self.rpc_pools: dict[str, RpcPool] = {}
        self.candidates: dict[str, Candidate] = {}
        self.command_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        # Telegram commands are processed by a dedicated worker. V4.4 coupled
        # command execution to the mint/stage loop, so long eligibility/network
        # checks could starve /start and inline-button callbacks.
        self.command_worker_thread: threading.Thread | None = None

        # V4.11 stable independent hot-path state. None of these workers consumes the
        # catalog/discovery queue used by the slower metadata lane.
        self.candidates_lock = threading.RLock()
        self.race_state_lock = threading.RLock()
        # Keep transaction construction/broadcast exactly as V4.9, but isolate
        # live signals, prewarm, and scheduled launch so background work cannot
        # queue in front of a ready Public transaction.
        self.race_signal_executor = ThreadPoolExecutor(
            max_workers=self.race_stream_workers, thread_name_prefix="race-signal"
        )
        self.race_prep_executor = ThreadPoolExecutor(
            max_workers=self.race_prep_workers, thread_name_prefix="race-prep"
        )
        self.race_launch_executor = ThreadPoolExecutor(
            max_workers=self.race_launch_workers, thread_name_prefix="race-launch"
        )
        self.race_executor = self.race_signal_executor
        # Social metadata never shares a Race executor; a slow OpenSea lookup
        # therefore cannot occupy a signal/prewarm/launch worker.
        self.social_trust_executor = ThreadPoolExecutor(
            max_workers=self.social_trust_workers, thread_name_prefix="social-trust"
        )
        self.race_prepared: dict[str, dict[str, Any]] = {}
        self.race_preparing: set[str] = set()
        self.race_active: set[str] = set()
        self.race_queued: set[str] = set()
        # Only one live signal worker may resolve a contract at a time. Any
        # concurrent duplicate is merged into one pending hint. Once a stage is
        # resolved, a short source-specific RAM quiet window prevents item-mint
        # storms from re-reading the same SeaDrop config. Scheduled Race retries
        # remain independent and therefore keep their original 40ms timing.
        self.race_signal_inflight: set[str] = set()
        self.race_signal_pending: dict[str, dict[str, Any]] = {}
        self.race_signal_stage_cache: dict[str, dict[str, Any]] = {}
        self.race_signal_stage_seen: dict[str, float] = {}
        self.race_signal_coalesced = 0
        self.race_signal_stage_suppressed = 0
        self.race_scheduler_thread: threading.Thread | None = None
        self.seadrop_log_threads: list[threading.Thread] = []
        self.race_market_thread: threading.Thread | None = None
        self.race_market_lock = threading.RLock()
        self.race_fee_cache: dict[str, tuple[float, dict[str, int]]] = {}
        self.race_last_price_refresh = 0.0

        self.pending_wallet_name: dict[str, float] = {}
        self.pending_wallet_import: dict[str, dict[str, Any]] = {}
        self.pending_wallet_rename: dict[str, dict[str, Any]] = {}
        self.pending_wallet_quantity: dict[str, dict[str, Any]] = {}
        self.pending_link_action: dict[str, dict[str, Any]] = {}
        self.pending_paid_quantity: dict[str, dict[str, Any]] = {}
        self.pending_gas_setting: dict[str, dict[str, Any]] = {}
        self.pending_gas_project: dict[str, dict[str, Any]] = {}
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

    def mint_url(
        self,
        *,
        slug: str | None = None,
        chain: str | None = None,
        source: str | None = None,
        contract_address: str | None = None,
    ) -> str:
        """Return a copyable OpenSea URL for every project-facing message."""
        raw = str(source or "").strip()
        if raw.startswith(("https://", "http://")) and "opensea.io" in raw.lower():
            return raw
        clean_slug = str(slug or "").strip()
        if clean_slug and not clean_slug.startswith("contract-"):
            return f"https://opensea.io/collection/{quote(clean_slug, safe='-._~')}"
        address = str(contract_address or "").strip()
        if address and Web3.is_address(address):
            chain_name = opensea_chain_name(normalize_chain(chain or "ethereum"))
            return f"https://opensea.io/assets/{quote(chain_name, safe='-._~')}/{address}"
        return "https://opensea.io/"

    def candidate_mint_url(self, candidate: Candidate) -> str:
        return self.mint_url(
            slug=candidate.slug, chain=candidate.chain, source=candidate.source,
            contract_address=candidate.contract_address,
        )

    def row_mint_url(self, row: dict[str, Any]) -> str:
        return self.mint_url(
            slug=str(row.get("slug") or ""), chain=str(row.get("chain") or ""),
            source=str(row.get("source") or ""),
            contract_address=str(row.get("contract_address") or ""),
        )

    def mint_link_block(self, candidate: Candidate) -> str:
        return f"🔗 رابط المنت (للنسخ):\n{self.candidate_mint_url(candidate)}"

    # ---------- V4.11 protected-free social identity gate ----------
    def social_trust_required(self, candidate: Candidate, plan: dict[str, Any] | None = None) -> bool:
        """Whether this exact mint must pass the project-level X/website gate.

        Deliberate exemptions:
        - manual watches: the user explicitly chose the project;
        - qualification/allowlist projects: their final Public must not be delayed;
        - paid stages: they already require explicit user approval.
        """
        if not self.free_social_protection_enabled:
            return False
        if not candidate.auto_discovered or candidate.qualification_tracked:
            return False
        if plan is not None:
            if not bool(plan.get("is_public")):
                return False
            if bool(plan.get("is_paid")):
                return False
        return True

    def _social_status_ttl(self, status: str) -> float:
        if status == "passed":
            return self.social_trust_pass_ttl
        if status == "rejected":
            return self.social_trust_reject_ttl
        if status == "error":
            return self.social_trust_error_retry
        return 0.0

    def _apply_social_trust_record(self, candidate: Candidate, row: dict[str, Any]) -> bool:
        status = str(row.get("status") or "unknown")
        checked = float(row.get("checked_at") or 0.0)
        ttl = self._social_status_ttl(status)
        if status not in {"passed", "rejected", "error"} or checked <= 0 or (time.time() - checked) > ttl:
            return False
        candidate.social_trust_status = status
        candidate.social_trust_checked_at = checked
        candidate.social_twitter_username = str(row.get("twitter_username") or "") or None
        candidate.social_twitter_url = str(row.get("twitter_url") or "") or None
        candidate.social_website_url = str(row.get("website_url") or "") or None
        candidate.social_trust_detail = str(row.get("detail") or "")
        return True

    def social_trust_text(self, candidate: Candidate) -> str:
        if not candidate.auto_discovered or candidate.qualification_tracked:
            return "🛡 مستثنى من حماية Free Mint"
        if not self.free_social_protection_enabled:
            return "🛡 الحماية متوقفة — يسمح بجميع Free Mints"
        status = candidate.social_trust_status
        if status == "passed":
            bits = []
            if candidate.social_twitter_url:
                bits.append("𝕏")
            if candidate.social_website_url:
                bits.append("🌐")
            return "🟢 محمي: " + " + ".join(bits or ["هوية مشروع"])
        if status == "rejected":
            return "🔴 مرفوض: لا X ولا Website"
        if status == "pending":
            return "🟡 جارٍ فحص X / Website"
        if status == "error":
            return "🟠 تعذر التحقق مؤقتًا — لن يتم Mint حتى ينجح الفحص"
        return "⚪ لم يُفحص بعد"

    def _social_trust_worker(self, candidate: Candidate) -> None:
        project_key = self.project_key_for_candidate(candidate)
        try:
            slug = candidate.slug
            if (not slug or slug.startswith("contract-")) and candidate.contract_address:
                slug = self.resolve_slug_from_contract(candidate.chain, candidate.contract_address) or slug
            if not slug or slug.startswith("contract-"):
                raise RuntimeError("collection slug unavailable for social verification")

            collection = self.opensea.get_collection(slug)
            twitter_username, twitter_url, website_url = extract_collection_social_identity(collection)
            status = "passed" if (twitter_url or website_url) else "rejected"
            detail = "X or website found" if status == "passed" else "OpenSea collection has neither X nor external website"
            checked = time.time()
            self.store.upsert_social_trust(
                project_key=project_key, slug=slug, chain=candidate.chain,
                contract_address=candidate.contract_address, status=status,
                twitter_username=twitter_username, twitter_url=twitter_url,
                website_url=website_url, checked_at=checked, detail=detail,
            )
            candidate.social_trust_status = status
            candidate.social_trust_checked_at = checked
            candidate.social_twitter_username = twitter_username
            candidate.social_twitter_url = twitter_url
            candidate.social_website_url = website_url
            candidate.social_trust_detail = detail
            log.info(
                "Free social trust %s | %s | %s | X=%s | website=%s",
                status, candidate.slug, candidate.chain, bool(twitter_url), bool(website_url),
            )

            # A sudden Public free mint may have been waiting only on this gate.
            # PASS must wake the mint path even when OpenSea stage metadata and
            # the on-chain SeaDrop signal arrived in the opposite order.
            if status == "passed" and self.free_social_protection_enabled:
                now = time.time()
                self.sync_wallets_into_candidates()
                for state in candidate.wallets.values():
                    if not state.submitted and not state.final:
                        state.next_attempt = min(state.next_attempt or now, now)

                launched_from_plan = False
                plan = self.current_plan_for_candidate(candidate, now) if candidate.stage_plans else None
                if (
                    plan and plan.get("is_public") and not plan.get("is_paid")
                    and candidate.contract_address and self.race_enabled and not self.paused
                ):
                    race_key = self._race_key(candidate, str(plan.get("key") or ""))
                    with self.race_state_lock:
                        has_prepared = race_key in self.race_prepared
                    self.race_launch_executor.submit(
                        self._launch_candidate_race, candidate, plan, live=not has_prepared
                    )
                    launched_from_plan = True

                # Critical V4.11.3 fallback: a social PASS is also a fresh fast
                # contract signal. If stage_plans were not populated yet, read
                # SeaDrop on-chain immediately and launch the active free Public
                # instead of waiting for the next catalog/stream event.
                if candidate.contract_address and self.race_enabled and not launched_from_plan:
                    signal_slug = slug if slug and not slug.startswith("contract-") else candidate.slug
                    self.submit_fast_contract_signal(
                        candidate.chain, candidate.contract_address, signal_slug, "social-pass"
                    )
                    log.info(
                        "Social PASS fast wake | %s | %s | contract=%s",
                        candidate.slug, candidate.chain, short_address(candidate.contract_address),
                    )
        except Exception as exc:
            checked = time.time()
            detail = str(exc)[:800]
            candidate.social_trust_status = "error"
            candidate.social_trust_checked_at = checked
            candidate.social_trust_detail = detail
            try:
                self.store.upsert_social_trust(
                    project_key=project_key, slug=candidate.slug, chain=candidate.chain,
                    contract_address=candidate.contract_address, status="error",
                    checked_at=checked, detail=detail,
                )
            except Exception:
                pass
            log.info("Free social trust temporary error | %s | %s | %s", candidate.slug, candidate.chain, detail[:250])
        finally:
            with self.social_trust_lock:
                self.social_trust_inflight.discard(project_key)

    def ensure_social_trust_async(self, candidate: Candidate, *, force: bool = False) -> str:
        """Start at most one social lookup for a project and return current status."""
        if not self.free_social_protection_enabled or not candidate.auto_discovered or candidate.qualification_tracked:
            return "bypassed"
        # Do not consume REST quota on paid-only projects. Unknown-price Public
        # stages are treated as potential free stages so their trust result can
        # still be ready before the on-chain price resolves.
        if candidate.stage_plans and not any(
            p.get("is_public") and not p.get("is_paid") for p in candidate.stage_plans
        ):
            return "bypassed"
        project_key = self.project_key_for_candidate(candidate)
        now = time.time()

        # Hot scheduler path: while one worker is already resolving this project,
        # return from RAM only. Never hit SQLite every few milliseconds.
        if not force and candidate.social_trust_status == "pending":
            with self.social_trust_lock:
                if project_key in self.social_trust_inflight:
                    return "pending"

        if not force and candidate.social_trust_status in {"passed", "rejected", "error"}:
            ttl = self._social_status_ttl(candidate.social_trust_status)
            if candidate.social_trust_checked_at and now - candidate.social_trust_checked_at <= ttl:
                return candidate.social_trust_status

        if not force:
            try:
                cached = self.store.get_social_trust(project_key)
            except Exception:
                cached = None
            if cached and self._apply_social_trust_record(candidate, cached):
                return candidate.social_trust_status

        with self.social_trust_lock:
            if project_key in self.social_trust_inflight:
                candidate.social_trust_status = "pending"
                return "pending"
            self.social_trust_inflight.add(project_key)
            candidate.social_trust_status = "pending"
        self.social_trust_executor.submit(self._social_trust_worker, candidate)
        return "pending"

    def social_protection_allows(self, candidate: Candidate, plan: dict[str, Any] | None) -> bool:
        if not self.social_trust_required(candidate, plan):
            return True
        status = self.ensure_social_trust_async(candidate)
        return status == "passed"

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
                    log.info("Wallet %s linked to watched mint %s", wallet.name, candidate.slug)

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
                log.info("RPC %s ready | primary=%s | verified=%s", chain, safe_endpoint_for_log(pool.primary_url), len(pool.urls))
            except Exception as exc:
                log.warning("RPC pool disabled for %s: %s", chain, exc)

    def max_gas_for_chain(self, chain: str) -> Decimal:
        # Native cap stays as a compatibility safety valve; V4.9's primary
        # budget is the Telegram-managed USD cap below.
        raw = os.getenv(f"{chain.upper()}_MAX_GAS_NATIVE", "").strip()
        if raw:
            try:
                return Decimal(raw)
            except InvalidOperation:
                pass
        return self.max_gas_native

    def max_gas_usd_for_chain(self, chain: str, candidate: Candidate | None = None) -> Decimal:
        if candidate is not None:
            if candidate.ignore_gas_cap:
                return Decimal("0")  # 0 means unlimited in buyer.py
            if candidate.gas_override_usd is not None:
                return max(Decimal("0"), candidate.gas_override_usd)
        chain_cap = self.chain_gas_caps_usd.get(chain)
        return self.max_gas_usd if chain_cap is None else chain_cap

    def save_gas_cap(self, target: str, value: Decimal) -> None:
        value = max(Decimal("0"), value)
        if target == "global":
            self.max_gas_usd = value
            self.store.set_setting("gas_usd_global", str(value))
            return
        chain = normalize_chain(target)
        if chain not in self.enabled_chains:
            raise ValueError("Unsupported chain gas target")
        self.chain_gas_caps_usd[chain] = value
        self.store.set_setting(f"gas_usd_{chain}", str(value))

    def project_gas_policy_text(self, candidate: Candidate) -> str:
        if candidate.ignore_gas_cap:
            return "🔥 بدون حد لهذا المنت (تجاوز صريح)"
        if candidate.gas_override_usd is not None:
            return f"⛽ حد خاص: ${candidate.gas_override_usd}"
        return f"⛽ يرث حد الشبكة: ${self.max_gas_usd_for_chain(candidate.chain)}"

    def set_candidate_gas_policy(
        self, candidate: Candidate, *, ignore: bool = False, override_usd: Decimal | None = None
    ) -> None:
        candidate.ignore_gas_cap = bool(ignore)
        candidate.gas_override_usd = None if override_usd is None else max(Decimal("0"), override_usd)
        self.ensure_candidate_watch_persisted(candidate)
        self.store.set_watch_gas_policy(
            candidate.slug,
            gas_override_usd=None if candidate.gas_override_usd is None else str(candidate.gas_override_usd),
            ignore_gas_cap=candidate.ignore_gas_cap,
        )

    def native_usd_price_for_chain(self, chain: str) -> Decimal | None:
        return self.price_oracle.get_usd(native_symbol(chain))

    def race_native_usd_price(self, chain: str, *, allow_network: bool = False) -> Decimal | None:
        """Return a cached USD price for the race lane.

        Live/scheduled launch threads should not block on an HTTP price lookup.
        The market warmer continuously refreshes this cache. During prewarm we
        may allow a synchronous lookup as a fallback because it happens before
        the opening timestamp.
        """
        symbol = native_symbol(chain)
        cached = self.price_oracle.peek_usd(symbol, max_age_seconds=max(120.0, self.race_price_refresh_seconds * 4))
        if cached is not None:
            return cached
        return self.price_oracle.get_usd(symbol) if allow_network else None

    def race_fee_fields(self, chain: str, *, allow_network: bool = False) -> dict[str, int] | None:
        now = time.time()
        with self.race_market_lock:
            cached = self.race_fee_cache.get(chain)
            if cached and now - cached[0] <= max(2.0, self.race_fee_refresh_seconds * 6):
                return dict(cached[1])
        if not allow_network or chain not in self.rpc_pools:
            return None
        try:
            fields = build_fee_fields(self.rpc_pools[chain].primary, self.race_gas_strategy)
            with self.race_market_lock:
                self.race_fee_cache[chain] = (time.time(), dict(fields))
            return fields
        except Exception:
            return None

    def _race_market_warmer_loop(self) -> None:
        """Keep fee and native/USD snapshots hot outside the mint path."""
        log.info(
            "Race market warmer ready | fee=%.2fs | price=%.0fs",
            self.race_fee_refresh_seconds, self.race_price_refresh_seconds,
        )
        next_price = 0.0
        while not STOP:
            cycle = time.time()
            # Fee snapshots are chain-specific and cheap RPC reads. Refresh in
            # parallel so one slow endpoint cannot make every network stale.
            def fee_job(item: tuple[str, RpcPool]):
                chain, pool = item
                try:
                    return chain, build_fee_fields(pool.primary, self.race_gas_strategy)
                except Exception:
                    return chain, None

            items = list(self.rpc_pools.items())
            if items:
                with ThreadPoolExecutor(max_workers=min(len(items), 8)) as executor:
                    futures = [executor.submit(fee_job, item) for item in items]
                    for future in as_completed(futures):
                        try:
                            chain, fields = future.result()
                            if fields:
                                with self.race_market_lock:
                                    self.race_fee_cache[chain] = (time.time(), dict(fields))
                        except Exception:
                            pass

            if cycle >= next_price:
                # Current supported EVM networks use ETH as their native gas
                # asset, but dedupe by symbol so future chains are safe too.
                symbols = sorted({native_symbol(chain) for chain in self.rpc_pools})
                for symbol in symbols:
                    try:
                        self.price_oracle.get_usd(symbol)
                    except Exception:
                        pass
                self.race_last_price_refresh = time.time()
                next_price = cycle + self.race_price_refresh_seconds

            elapsed = time.time() - cycle
            time.sleep(max(0.05, self.race_fee_refresh_seconds - elapsed))

    def start_race_market_warmer(self) -> None:
        if not self.race_enabled or (self.race_market_thread and self.race_market_thread.is_alive()):
            return
        self.race_market_thread = threading.Thread(
            target=self._race_market_warmer_loop, name="race-market-warmer", daemon=True
        )
        self.race_market_thread.start()

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

        # V4.9: the race lane can create a contract-first candidate milliseconds
        # before the slower OpenSea metadata lane resolves the slug. Merge by
        # (chain, contract) so metadata enrichment can never create a second
        # candidate that races the same wallet/nonce for the same mint.
        if candidate is None and discovered_contract:
            contract_l = str(discovered_contract).lower()
            with self.candidates_lock:
                contract_match = next((
                    (existing_key, existing) for existing_key, existing in self.candidates.items()
                    if existing.chain == chain and existing.contract_address
                    and existing.contract_address.lower() == contract_l
                ), None)
                if contract_match is not None:
                    old_key, candidate = contract_match
                    if old_key != key:
                        self.candidates.pop(old_key, None)
                        candidate.slug = slug
                        self.candidates[key] = candidate

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
            with self.candidates_lock:
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
        if candidate.auto_discovered and not candidate.qualification_tracked:
            self.ensure_social_trust_async(
                candidate, force=(candidate.social_trust_status == "error" and not candidate.slug.startswith("contract-"))
            )
        paid_text = "مسموح فقط بعد تأكيدك واختيار المحافظ والكميات" if allow_paid else "مجاني فقط"
        kind_text = f"{len(plans)} مرحلة" + (" — يحتوي مراحل تأهيل" if has_qualification_stages(plans) else "")
        message = (
            "🎯 تم تفعيل مراقبة المنت\n\n"
            f"📦 المشروع: {slug}\n"
            f"🌐 الشبكة: {chain_label(chain)}\n"
            f"🎟 المراحل: {kind_text}\n"
            f"⏰ أقرب Public: {format_ts(candidate.public_start, self.display_tz)}\n"
            f"👛 المحافظ النشطة: {len(candidate.wallets)}\n"
            f"💳 المدفوع: {paid_text}\n\n"
            + "\n".join(candidate.stage_lines[:10])
            + "\n\n" + self.mint_link_block(candidate)
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
                gas_override_usd=None if candidate.gas_override_usd is None else str(candidate.gas_override_usd),
                ignore_gas_cap=candidate.ignore_gas_cap,
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
                    restored_stage_key = str(row.get("last_stage_key") or "")
                    if restored_stage_key:
                        candidate.processed_stage_key = restored_stage_key
                        candidate.stage_open_notified_keys.add(restored_stage_key)
                    candidate.schedule_summary_notified = True
                    candidate.ignore_gas_cap = bool(row.get("ignore_gas_cap", 0))
                    try:
                        _gas_override = str(row.get("gas_override_usd") or "").strip()
                        candidate.gas_override_usd = Decimal(_gas_override) if _gas_override else None
                    except (InvalidOperation, TypeError, ValueError):
                        candidate.gas_override_usd = None
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
        now = time.time()
        # Never put a REST refresh in front of a known Public free mint. In the
        # final seconds the transaction path has absolute priority.
        if candidate.stage_plans:
            current = self.current_plan_for_candidate(candidate, now)
            future = self.next_plan_for_candidate(candidate, now)
            if current and current.get("is_public") and not current.get("is_paid"):
                return
            if future and future.get("is_public") and not future.get("is_paid") and future.get("start") is not None:
                if float(future["start"]) - now <= self.public_preopen_window_seconds:
                    return
        if now < candidate.next_refresh:
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
        if candidate.auto_discovered and not candidate.qualification_tracked:
            self.ensure_social_trust_async(
                candidate, force=(candidate.social_trust_status == "error" and not candidate.slug.startswith("contract-"))
            )

        policy = "المدفوع لا يُنفذ إلا بعد تأكيدك" if allow_paid else "مجاني فقط"
        message = (
            "🎯 تم تفعيل مراقبة المنت\n\n"
            f"📦 المشروع: {slug}\n"
            f"🌐 الشبكة: {chain_label(chain)}\n"
            f"⚙️ المصدر: SeaDrop مباشر على السلسلة\n"
            f"📜 العقد: {contract_address}\n"
            f"⏰ موعد الـPublic: {format_ts(candidate.public_start, self.display_tz)}\n"
            f"🛡 السياسة: {policy}\n"
            f"🎟 الحالة: {candidate.stage_lines[0]}\n\n"
            f"{self.mint_link_block(candidate)}"
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
                lines.extend(["", f"🔗 رابط المنت (للنسخ):\n{self.mint_url(slug=slug, chain=chain, source=raw)}"])
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
        lines.extend(["", f"🔗 رابط المنت (للنسخ):\n{self.mint_url(slug=slug, chain=chain, source=raw, contract_address=contract)}"])
        return "\n".join(lines)[:3900]

    # ---------- V4.10.1 stable ultra-fast race lane ----------
    def _race_key(self, candidate: Candidate, stage_key: str | None = None) -> str:
        return f"{self.project_key_for_candidate(candidate)}:{stage_key or candidate.current_stage_key or 'public'}"

    def _fast_plan_from_public(self, public: dict[str, Any], contract: str) -> dict[str, Any]:
        start = float(public.get("start_time") or 0) or None
        end = float(public.get("end_time") or 0) or None
        price_wei = int(public.get("mint_price_wei") or 0)
        limit = int(public.get("max_per_wallet") or 0) or None
        return {
            "key": hashlib.sha1(f"race|{contract.lower()}|{start}|{end}|{price_wei}|{limit}".encode()).hexdigest()[:16],
            "index": 0,
            "label": "Public SeaDrop",
            "start": start,
            "end": end,
            "is_public": True,
            "is_free": price_wei == 0,
            "is_paid": price_wei > 0,
            "wallet_limit": limit,
            "price": str(Decimal(price_wei) / Decimal(10**18)),
        }

    def _ensure_fast_candidate(
        self, chain: str, contract: str, slug: str | None,
        public: dict[str, Any], source: str,
    ) -> Candidate:
        contract = Web3.to_checksum_address(contract)
        with self.candidates_lock:
            existing = next((
                c for c in self.candidates.values()
                if c.chain == chain and c.contract_address and c.contract_address.lower() == contract.lower()
            ), None)
            if existing is not None:
                candidate = existing
                if slug and candidate.slug.startswith("contract-"):
                    candidate.slug = slug
            else:
                safe_slug = slug or f"contract-{contract[-10:].lower()}"
                candidate = Candidate(
                    slug=safe_slug, chain=chain, source=source, allow_paid=True,
                    max_mint_price_native=self.max_mint_price_default,
                    auto_discovered=True, mint_backend="seadrop",
                    contract_address=contract, discovery_source=source,
                    watch_kind="auto_free",
                )
                for wallet in self.wallets:
                    if wallet.supports_chain(chain):
                        candidate.wallets[wallet.address.lower()] = WalletState(wallet=wallet, quantity=wallet.quantity)
                self.candidates[f"{chain}:{safe_slug}"] = candidate
            plan = self._fast_plan_from_public(public, contract)
            # Preserve richer OpenSea allowlist/signed-stage metadata if this
            # candidate already exists. The on-chain public plan is merged in
            # rather than replacing the qualification timeline.
            if candidate.stage_plans:
                same_public = next((
                    p for p in candidate.stage_plans
                    if p.get("is_public") and (
                        str(p.get("key") or "") == str(plan.get("key") or "")
                        or (p.get("start") is not None and plan.get("start") is not None and abs(float(p["start"]) - float(plan["start"])) < 2.0)
                    )
                ), None)
                if same_public is not None:
                    same_public.update(plan)
                else:
                    candidate.stage_plans.append(plan)
                    candidate.stage_plans.sort(key=lambda p: float(p.get("start") or 0))
            else:
                candidate.stage_plans = [plan]
            candidate.public_start = plan.get("start") or candidate.public_start
            candidate.stage_end = plan.get("end") or candidate.stage_end
            candidate.wallet_limit = plan.get("wallet_limit") or candidate.wallet_limit
            now = time.time()
            if plan.get("start") and float(plan["start"]) > now:
                candidate.next_stage_start = min(
                    [x for x in [candidate.next_stage_start, float(plan["start"])] if x is not None],
                    default=float(plan["start"]),
                )
            active_public = (not plan.get("start") or now >= float(plan["start"])) and (not plan.get("end") or now < float(plan["end"]))
            if active_public:
                candidate.current_stage_key = str(plan.get("key") or "")
                candidate.current_stage_label = str(plan.get("label") or "Public SeaDrop")
                candidate.current_stage_start = plan.get("start")
                candidate.current_stage_end = plan.get("end")
                candidate.current_stage_public = True
                candidate.current_stage_free = bool(plan.get("is_free"))
                candidate.current_stage_paid = bool(plan.get("is_paid"))
                candidate.current_stage_limit = plan.get("wallet_limit")
            candidate.last_seen_auto = now
            return candidate

    def _race_wallet_work(self, candidate: Candidate, plan: dict[str, Any]) -> list[tuple[WalletState, int]]:
        work: list[tuple[WalletState, int]] = []
        is_paid = bool(plan.get("is_paid"))
        target_total = self.stage_target_total(candidate, plan)
        now = time.time()
        for state in candidate.wallets.values():
            if state.submitted:
                continue
            # Prevent a wallet with no native gas balance from being hammered on
            # every 40ms Race retry. Healthy wallets still enter immediately.
            if state.next_attempt and state.next_attempt > now:
                continue
            address = state.wallet.address.lower()
            if is_paid:
                if candidate.paid_decision != "confirmed" or address not in candidate.paid_wallet_addresses:
                    continue
                qty = int(candidate.paid_wallet_quantities.get(address) or 0)
                if qty <= 0:
                    continue
                if plan.get("wallet_limit"):
                    qty = min(qty, int(plan["wallet_limit"]))
            else:
                confirmed = self.confirmed_total_for_wallet(candidate, state.wallet.address)
                state.confirmed_total = confirmed
                state.target_total = target_total
                qty = max(0, target_total - confirmed)
                if candidate.quantity_override:
                    qty = min(qty, max(1, int(candidate.quantity_override)))
            if qty <= 0:
                continue
            qty = max(1, min(int(qty), 100))
            state.quantity = qty
            state.stage_key = str(plan.get("key") or "")
            state.stage_label = str(plan.get("label") or "Public SeaDrop")
            work.append((state, qty))
        return work

    def notify_insufficient_balance(self, candidate: Candidate, state: WalletState, result: Any | None = None, detail: str | None = None) -> None:
        """Always alert when a mint cannot proceed because native funds are insufficient.

        This notification intentionally bypasses the routine monitoring/stage
        notification toggle. It is a transaction-safety alert, not dashboard noise.
        """
        alert_key = f"insufficient_balance:{state.stage_key or candidate.current_stage_key or 'mint'}"
        if state.last_notified_status == alert_key:
            return
        state.last_notified_status = alert_key
        symbol = native_symbol(candidate.chain)
        mint_value = getattr(result, "mint_value_native", None) if result is not None else None
        gas_value = getattr(result, "gas_cost_native", None) if result is not None else None
        gas_usd = getattr(result, "gas_cost_usd", None) if result is not None else None
        kind = "مدفوع" if (mint_value is not None and mint_value > 0) else "مجاني"
        lines = [
            "💸 فشل أخذ المنت — رصيد رسوم الشبكة غير كافٍ",
            "",
            f"📦 المشروع: {candidate.slug}",
            f"🏷 النوع: {kind}",
            f"🌐 الشبكة: {chain_label(candidate.chain)}",
            f"👛 المحفظة: {state.wallet.name} {short_address(state.wallet.address)}",
        ]
        if mint_value is not None:
            lines.append(f"💰 قيمة المنت: {mint_value} {symbol}")
        if gas_value is not None:
            gas_line = f"⛽ أقصى تقدير للغاز: {gas_value} {symbol}"
            if gas_usd is not None:
                gas_line += f" ≈ ${gas_usd:.4f}"
            lines.append(gas_line)
        lines.append(f"⚠️ أضف رصيد {symbol} كافيًا لهذه المحفظة ثم سيعيد البوت المحاولة تلقائيًا إذا كانت المرحلة ما زالت مفتوحة.")
        if detail:
            lines.append(f"📝 السبب: {str(detail)[:350]}")
        lines.extend(["", self.mint_link_block(candidate)])
        self.notify_all("\n".join(lines)[:3900])

    def _apply_race_results(self, candidate: Candidate, plan: dict[str, Any], results: dict[str, Any]) -> int:
        submitted = 0
        now = time.time()
        for addr_l, result in results.items():
            state = candidate.wallets.get(addr_l.lower())
            if state is None:
                continue
            state.status = result.status
            state.last_detail = result.detail
            if result.ok:
                submitted += 1
                state.submitted = True
                state.final = False
                state.pending_quantity = int(result.quantity_used or state.quantity or 1)
                state.quantity = state.pending_quantity
                state.tx_hash = result.tx_hash
                state.mint_value_native = result.mint_value_native
                state.receipt_next_check = now + self.receipt_check_seconds
                self.store.record_mint(
                    slug=candidate.slug, chain=candidate.chain,
                    wallet_name=state.wallet.name, wallet_address=state.wallet.address,
                    status="submitted", tx_hash=result.tx_hash,
                    mint_value_native=str(result.mint_value_native),
                    gas_max_native=str(result.gas_cost_native), quantity=state.pending_quantity,
                    detail=result.detail, contract_address=candidate.contract_address,
                    stage_key=state.stage_key or None, stage_label=state.stage_label or None,
                    watch_kind=candidate.watch_kind,
                )
                self.notify_all(
                    "⚡🚀 تم إرسال Mint عبر Race Lane\n\n"
                    f"📦 المشروع: {candidate.slug}\n"
                    f"🎟 المرحلة: {state.stage_label}\n"
                    f"🌐 الشبكة: {chain_label(candidate.chain)}\n"
                    f"👛 المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"🔢 الكمية: {state.pending_quantity}\n"
                    f"💰 القيمة: {result.mint_value_native} {native_symbol(candidate.chain)}\n"
                    + (f"⛽ أقصى تقدير: ${result.gas_cost_usd:.4f}\n" if result.gas_cost_usd is not None else "")
                    + "\n" + self.mint_link_block(candidate)
                    + "\n\n🔎 المعاملة:\n" + explorer_tx_url(candidate.chain, result.tx_hash or "")
                )
            else:
                # Keep hot failures retryable while the short launch window is
                # still open. Safety/policy failures use a slower retry so they
                # cannot consume every Race tick.
                if result.status == "insufficient_balance":
                    state.next_attempt = now + self.low_balance_retry_seconds
                    self.notify_insufficient_balance(candidate, state, result=result, detail=result.detail)
                elif result.status in {"gas_usd_too_high", "gas_price_unavailable", "gas_too_high"}:
                    state.next_attempt = now + self.gas_over_budget_retry_seconds
                elif result.status in {"paid_not_allowed", "paid_wallet_selection_required", "mint_price_too_high"}:
                    state.next_attempt = now + 15.0
                elif result.status == "precondition_failed" and any(
                    marker in str(result.detail or "").lower()
                    for marker in ("wallet already reached", "no on-chain supply remains", "sold out")
                ):
                    # Nothing useful can happen again in the same stage. A new
                    # stage automatically resets final=False in process_stage_schedule.
                    state.final = True
                    state.next_attempt = now + max(15.0, self.stage_refresh_seconds)
                else:
                    state.next_attempt = now + self.race_retry_seconds
        return submitted

    def _prepare_race_bundle(self, candidate: Candidate, plan: dict[str, Any], public: dict[str, Any]) -> dict[str, Any] | None:
        if not candidate.contract_address:
            return None
        work = self._race_wallet_work(candidate, plan)
        if not work:
            return None
        paid = bool(plan.get("is_paid"))
        return prepare_seadrop_race_transactions(
            rpc_pool=self.rpc_pools[candidate.chain],
            wallet_quantities=[(state.wallet, qty) for state, qty in work],
            nft_contract=candidate.contract_address,
            gas_strategy=self.race_gas_strategy,
            gas_limit_buffer=self.gas_limit_buffer,
            max_gas_native=self.max_gas_for_chain(candidate.chain),
            max_total_native=self.max_total_native,
            max_gas_usd=self.max_gas_usd_for_chain(candidate.chain, candidate),
            native_usd_price=self.race_native_usd_price(candidate.chain, allow_network=True),
            allowed_targets=self.allowed_targets,
            allow_paid=paid and candidate.paid_decision == "confirmed",
            paid_wallet_addresses=candidate.paid_wallet_addresses,
            max_mint_price_native=candidate.max_mint_price_native,
            fee_fields_override=self.race_fee_fields(candidate.chain, allow_network=True),
            public_override=public,
            allow_before_start=True,
            static_gas_limit=self.race_static_gas_limit,
            skip_balance_check=False,
            clamp_fees_to_gas_budget=True,
        )

    def _prewarm_candidate_race(self, candidate: Candidate, plan: dict[str, Any]) -> None:
        prep_perf = time.perf_counter()
        if self.paused or not self.race_enabled or not candidate.contract_address:
            return
        # Prewarm is intentionally allowed before the social decision: building
        # and signing locally spends no gas. Only broadcast is trust-gated.
        # This keeps the V4.10.1 race speed while the X/website lookup runs in parallel.
        key = self._race_key(candidate, str(plan.get("key") or ""))
        with self.race_state_lock:
            if key in self.race_preparing or key in self.race_prepared:
                return
            self.race_preparing.add(key)
        try:
            public = read_seadrop_public_fast(self.rpc_pools[candidate.chain].primary, candidate.contract_address)
            if not public or not public.get("configured"):
                return
            price_wei = int(public.get("mint_price_wei") or 0)
            if price_wei > 0 and candidate.paid_decision != "confirmed":
                return
            bundle = self._prepare_race_bundle(candidate, plan, public)
            if bundle:
                for addr_l, result in (bundle.get("results") or {}).items():
                    if getattr(result, "status", "") == "insufficient_balance":
                        state = candidate.wallets.get(str(addr_l).lower())
                        if state is not None:
                            state.next_attempt = time.time() + self.low_balance_retry_seconds
                            self.notify_insufficient_balance(candidate, state, result=result, detail=getattr(result, "detail", None))
            # Pause may have been pressed while prewarm was doing RPC work. Do
            # not retain a signed bundle in that case.
            if self.paused:
                return
            if bundle and bundle.get("entries"):
                with self.race_state_lock:
                    self.race_prepared[key] = bundle
                candidate.race_prepared = True
                log.info(
                    "Race prewarm ready | %s | %s | wallets=%s | opens=%.3f | prep=%.3fs",
                    candidate.slug, candidate.chain, len(bundle.get("entries") or []), float(plan.get("start") or 0),
                    time.perf_counter() - prep_perf,
                )
            elif bundle:
                statuses = sorted({getattr(r, "status", "unknown") for r in (bundle.get("results") or {}).values()})
                log.info(
                    "Race prewarm blocked | %s | %s | statuses=%s | prep=%.3fs",
                    candidate.slug, candidate.chain, ",".join(statuses) or "no-wallet-work", time.perf_counter() - prep_perf,
                )
        except Exception as exc:
            log.debug("Race prewarm failed %s: %s", candidate.slug, exc)
        finally:
            with self.race_state_lock:
                self.race_preparing.discard(key)

    def _launch_candidate_race(self, candidate: Candidate, plan: dict[str, Any], *, live: bool = False) -> None:
        launch_perf = time.perf_counter()
        if self.paused or not self.race_enabled or not candidate.contract_address:
            return
        if not self.social_protection_allows(candidate, plan):
            return
        stage_key = str(plan.get("key") or candidate.current_stage_key or "public")
        key = self._race_key(candidate, stage_key)
        with self.race_state_lock:
            if key in self.race_active:
                return
            self.race_active.add(key)
        candidate.race_inflight = True
        candidate.race_stage_key = stage_key
        candidate.race_last_attempt = time.time()
        try:
            bundle = None
            if not live:
                with self.race_state_lock:
                    bundle = self.race_prepared.pop(key, None)
            if bundle is None:
                public = read_seadrop_public_fast(self.rpc_pools[candidate.chain].primary, candidate.contract_address)
                if not public or not public.get("configured"):
                    return
                # A Stream event means the stage is already live; use one shared
                # estimate per quantity. A scheduled launch uses the static race
                # gas limit so it does not wait for a new block timestamp before
                # estimateGas starts succeeding.
                work = self._race_wallet_work(candidate, plan)
                if not work:
                    return
                paid = int(public.get("mint_price_wei") or 0) > 0
                bundle = prepare_seadrop_race_transactions(
                    rpc_pool=self.rpc_pools[candidate.chain],
                    wallet_quantities=[(state.wallet, qty) for state, qty in work],
                    nft_contract=candidate.contract_address,
                    gas_strategy=self.race_gas_strategy,
                    gas_limit_buffer=self.gas_limit_buffer,
                    max_gas_native=self.max_gas_for_chain(candidate.chain),
                    max_total_native=self.max_total_native,
                    max_gas_usd=self.max_gas_usd_for_chain(candidate.chain, candidate),
                    # Hot path: cached only. The market warmer prevents an HTTP
                    # price lookup from sitting in front of broadcast.
                    native_usd_price=self.race_native_usd_price(candidate.chain, allow_network=False),
                    allowed_targets=self.allowed_targets,
                    allow_paid=paid and candidate.paid_decision == "confirmed",
                    paid_wallet_addresses=candidate.paid_wallet_addresses,
                    max_mint_price_native=candidate.max_mint_price_native,
                    fee_fields_override=self.race_fee_fields(candidate.chain, allow_network=False),
                    public_override=public,
                    allow_before_start=not live,
                    static_gas_limit=None if live else self.race_static_gas_limit,
                    skip_balance_check=live,
                    clamp_fees_to_gas_budget=True,
                )
            if not bundle:
                log.info("RACE skipped | %s | %s | no bundle", candidate.slug, candidate.chain)
                return
            # Final zero-cost guards immediately before broadcast. Pause must
            # stop Race just like the normal path, and social protection remains
            # a RAM check when already resolved.
            if self.paused or not self.social_protection_allows(candidate, plan):
                return
            results = broadcast_seadrop_race_transactions(
                rpc_pool=self.rpc_pools[candidate.chain],
                bundle=bundle,
                max_parallel_wallets=self.max_parallel_wallets,
            )
            submitted = self._apply_race_results(candidate, plan, results)
            if submitted:
                log.info(
                    "RACE submitted | %s | %s | wallets=%s | launch-path=%.3fs",
                    candidate.slug, candidate.chain, submitted, time.perf_counter() - launch_perf,
                )
            else:
                statuses = sorted({getattr(r, "status", "unknown") for r in results.values()})
                first_detail = next((str(getattr(r, "detail", "") or "") for r in results.values() if getattr(r, "detail", None)), "")
                log.info(
                    "RACE no-submit | %s | %s | statuses=%s | launch-path=%.3fs%s",
                    candidate.slug, candidate.chain, ",".join(statuses) or "no-wallet-work",
                    time.perf_counter() - launch_perf,
                    (" | detail=" + first_detail[:500]) if first_detail else "",
                )
        finally:
            candidate.race_inflight = False
            with self.race_state_lock:
                self.race_active.discard(key)

    def _signal_contract_key(self, chain: str, contract: str) -> str:
        return f"{normalize_chain(chain)}:{str(contract).lower()}"

    @staticmethod
    def _signal_source_priority(source: str) -> int:
        # social-pass must never be lost: it is the event that unlocks the
        # Protected Free Mint Shield. SeaDrop WSS is next because it can reflect
        # an on-chain config change; OpenSea Stream is primarily mint activity.
        return {
            "social-pass": 100,
            "seadrop-wss": 80,
            "stream": 60,
            "events": 40,
        }.get(str(source or "").lower(), 20)

    def _signal_quiet_seconds(self, source: str, *, stage_known: bool) -> float:
        if not stage_known:
            return self.race_signal_unknown_quiet_seconds
        if source == "stream":
            return self.race_signal_stream_quiet_seconds
        if source == "seadrop-wss":
            return self.race_signal_seadrop_quiet_seconds
        return self.race_signal_unknown_quiet_seconds

    def _merge_pending_fast_signal(
        self,
        contract_key: str,
        *,
        slug: str | None,
        source: str,
        force: bool,
    ) -> None:
        """Keep only the most useful pending hint while one worker is in flight."""
        current = self.race_signal_pending.get(contract_key)
        incoming = {
            "slug": slug,
            "source": source,
            "force": bool(force),
            "queued_at": time.time(),
        }
        if current is None:
            self.race_signal_pending[contract_key] = incoming
            return
        # Prefer a real slug over an anonymous contract hint, and never let an
        # ordinary mint event overwrite a social-pass wake-up.
        cur_slug = str(current.get("slug") or "")
        new_slug = str(slug or "")
        if new_slug and (not cur_slug or cur_slug.startswith("contract-")):
            current["slug"] = slug
        if force or self._signal_source_priority(source) > self._signal_source_priority(str(current.get("source") or "")):
            current["source"] = source
        current["force"] = bool(current.get("force")) or bool(force)
        current["queued_at"] = incoming["queued_at"]

    def _apply_fast_slug_hint(self, chain: str, contract: str, slug: str | None) -> Candidate | None:
        """Enrich an existing contract candidate without another RPC call."""
        if not slug or str(slug).startswith("contract-"):
            return None
        contract_l = str(contract).lower()
        with self.candidates_lock:
            candidate = next((
                existing for existing in self.candidates.values()
                if existing.chain == chain and existing.contract_address
                and existing.contract_address.lower() == contract_l
            ), None)
            if candidate is None:
                return None
            # Match _ensure_fast_candidate's proven behavior: enrich the object
            # only. Do not re-key the candidates dict inside the signal coalescer.
            if candidate.slug.startswith("contract-"):
                candidate.slug = slug
            return candidate

    def _record_resolved_signal_stage(
        self,
        chain: str,
        contract: str,
        plan: dict[str, Any],
        *,
        source: str,
    ) -> None:
        contract_key = self._signal_contract_key(chain, contract)
        stage_key = str(plan.get("key") or "public")
        now = time.time()
        with self.race_state_lock:
            self.race_signal_stage_cache[contract_key] = {
                "stage_key": stage_key,
                "start": plan.get("start"),
                "end": plan.get("end"),
                "is_paid": bool(plan.get("is_paid")),
                "resolved_at": now,
            }
            self.race_signal_stage_seen[f"{contract_key}:{stage_key}:{source}"] = now
            self.race_signal_stage_seen[f"{contract_key}:{stage_key}:any"] = now
            # Long Railway uptimes can see many contracts. Bound both maps while
            # keeping recent stages hot in RAM.
            if len(self.race_signal_stage_cache) > 5000:
                cutoff = now - 1800.0
                self.race_signal_stage_cache = {
                    k: v for k, v in self.race_signal_stage_cache.items()
                    if float(v.get("resolved_at") or 0.0) >= cutoff
                }
            if len(self.race_signal_stage_seen) > 12000:
                cutoff = now - 1800.0
                self.race_signal_stage_seen = {
                    k: ts for k, ts in self.race_signal_stage_seen.items() if ts >= cutoff
                }

    def _fast_live_contract_signal(self, chain: str, contract: str, slug: str | None, source: str) -> bool:
        """Resolve one contract signal. Return True once a stable SeaDrop stage is known."""
        signal_perf = time.perf_counter()
        if not self.race_enabled or chain not in self.rpc_pools or not Web3.is_address(contract):
            return False
        contract = Web3.to_checksum_address(contract)
        now = time.time()
        try:
            public = read_seadrop_public_fast(self.rpc_pools[chain].primary, contract)
            if not public or not public.get("configured"):
                return False
            plan = self._fast_plan_from_public(public, contract)
            self._record_resolved_signal_stage(chain, contract, plan, source=source)
            candidate = self._ensure_fast_candidate(chain, contract, slug, public, source)
            start = float(plan.get("start") or 0)
            end = float(plan.get("end") or 0)
            active = (not start or now >= start) and (not end or now < end)
            if active:
                if plan.get("is_paid") and candidate.paid_decision != "confirmed":
                    candidate.paid_detected = True
                    self.maybe_offer_paid_public(candidate)
                    return True
                if self.paused:
                    # Discovery continues while paused, but no signing/broadcast.
                    return True
                if not self.social_protection_allows(candidate, plan):
                    log.debug("Fast free mint waiting for social trust | %s | %s", candidate.slug, chain)
                    return True
                self._launch_candidate_race(candidate, plan, live=True)
                log.info(
                    "Fast signal handled | source=%s | %s | %s | %.3fs",
                    source, candidate.slug, chain, time.perf_counter() - signal_perf,
                )
            elif start and start > now:
                candidate.watch_kind = "auto_stage"
                # Persist future on-chain public stages so a Railway restart does
                # not throw away the schedule discovered without OpenSea Drops.
                try:
                    self.ensure_candidate_watch_persisted(candidate)
                    self.persist_candidate_planning(candidate)
                except Exception:
                    pass
            return True
        except Exception as exc:
            log.debug("Fast live contract signal failed %s %s: %s", chain, contract, exc)
            return False

    def _run_coalesced_fast_signal(
        self,
        chain: str,
        contract: str,
        slug: str | None,
        source: str,
        contract_key: str,
    ) -> None:
        """Single-flight worker for one chain+contract.

        A dense burst can enqueue hundreds of item_transferred events. We run one
        RPC resolver only. If a high-priority social-pass arrives during that
        resolver it is replayed exactly once; ordinary duplicate mint events are
        folded into metadata hints and discarded once the stage is already known.
        """
        current_slug = slug
        current_source = source
        try:
            while not STOP:
                stable_stage = self._fast_live_contract_signal(chain, contract, current_slug, current_source)
                with self.race_state_lock:
                    pending = self.race_signal_pending.pop(contract_key, None)
                if pending:
                    hint_slug = pending.get("slug")
                    if hint_slug:
                        candidate = self._apply_fast_slug_hint(chain, contract, str(hint_slug))
                        # If the anonymous contract social lookup failed before the
                        # Stream supplied a slug, kick the gate again asynchronously.
                        if (
                            candidate is not None and candidate.auto_discovered and not candidate.qualification_tracked
                            and candidate.social_trust_status == "error" and self.free_social_protection_enabled
                        ):
                            self.ensure_social_trust_async(candidate, force=True)
                    # social-pass is an unlock event and must be replayed even if
                    # the first worker resolved the same stage. If the first read
                    # did not yet see a configured stage, replay one merged hint as
                    # well; this avoids losing a config transition during the RPC.
                    if bool(pending.get("force")) or not stable_stage:
                        current_slug = str(hint_slug or current_slug or "") or None
                        current_source = str(pending.get("source") or current_source)
                        continue
                    self.race_signal_coalesced += 1
                break
        finally:
            with self.race_state_lock:
                self.race_signal_inflight.discard(contract_key)
                # A signal may have arrived after our last pending pop but before
                # the inflight flag was cleared. Resubmit that one merged signal.
                late = self.race_signal_pending.pop(contract_key, None)
            if late and not STOP:
                self.submit_fast_contract_signal(
                    chain,
                    contract,
                    str(late.get("slug") or current_slug or "") or None,
                    str(late.get("source") or current_source),
                    force=bool(late.get("force")),
                )

    def submit_fast_contract_signal(
        self,
        chain: str | None,
        contract: str | None,
        slug: str | None,
        source: str,
        *,
        force: bool | None = None,
    ) -> None:
        """Submit a live signal with contract single-flight + stage-aware de-duplication."""
        chain_n = normalize_chain(chain or "")
        if not self.race_enabled or chain_n not in self.rpc_pools or not contract or not Web3.is_address(contract):
            return
        contract_c = Web3.to_checksum_address(contract)
        contract_key = self._signal_contract_key(chain_n, contract_c)
        source = str(source or "signal")
        force = (source == "social-pass") if force is None else bool(force)
        now = time.time()

        with self.race_state_lock:
            if contract_key in self.race_signal_inflight:
                self._merge_pending_fast_signal(contract_key, slug=slug, source=source, force=force)
                self.race_signal_coalesced += 1
                return

            # Once the stage fingerprint is known, raw mint-event storms do not
            # need another SeaDrop read every 100-200ms. This RAM-only gate never
            # delays the *first* signal and never suppresses social-pass. Scheduled
            # Race retry remains independent at RACE_RETRY_SECONDS (40ms).
            cache = self.race_signal_stage_cache.get(contract_key)
            if not force and cache:
                stage_key = str(cache.get("stage_key") or "public")
                seen_key = f"{contract_key}:{stage_key}:any"
                quiet = self._signal_quiet_seconds(source, stage_known=True)
                last = self.race_signal_stage_seen.get(seen_key, 0.0)
                if now - last < quiet:
                    self.race_signal_stage_suppressed += 1
                    return

            # Before the first stage fingerprint is known use only the old short
            # contract guard. It prevents an immediate duplicate executor submit
            # but keeps discovery responsiveness unchanged.
            if not force and not cache:
                seen_key = f"{contract_key}:unknown:{source}"
                last = self.race_signal_stage_seen.get(seen_key, 0.0)
                quiet = self._signal_quiet_seconds(source, stage_known=False)
                if now - last < quiet:
                    self.race_signal_stage_suppressed += 1
                    return
                self.race_signal_stage_seen[seen_key] = now

            self.race_signal_inflight.add(contract_key)

        try:
            self.race_signal_executor.submit(
                self._run_coalesced_fast_signal,
                chain_n,
                contract_c,
                slug,
                source,
                contract_key,
            )
        except Exception:
            with self.race_state_lock:
                self.race_signal_inflight.discard(contract_key)
            raise

    def _submit_scheduled_race(self, candidate: Candidate, plan: dict[str, Any]) -> None:
        """Queue at most one scheduled launch for a stage at a time.

        This prevents the scheduler tick from filling the executor with
        duplicate work while the first launch thread is still being scheduled.
        """
        if self.paused:
            return
        stage_key = str(plan.get("key") or candidate.current_stage_key or "public")
        key = self._race_key(candidate, stage_key)
        with self.race_state_lock:
            if key in self.race_active or key in self.race_queued:
                return
            self.race_queued.add(key)
        candidate.race_last_attempt = time.time()

        def runner():
            with self.race_state_lock:
                self.race_queued.discard(key)
            self._launch_candidate_race(candidate, plan, live=False)

        self.race_launch_executor.submit(runner)

    def _race_scheduler_loop(self) -> None:
        log.info(
            "Race scheduler ready | tick=%.3fs | prewarm=%.2fs | staticGas=%s",
            self.race_scheduler_tick, self.race_prewarm_seconds, self.race_static_gas_limit,
        )
        while not STOP:
            if self.paused:
                time.sleep(max(0.01, self.race_scheduler_tick))
                continue
            now = time.time()
            with self.candidates_lock:
                snapshot = list(self.candidates.values())
            for candidate in snapshot:
                if candidate.done or not candidate.contract_address or not candidate.stage_plans:
                    continue
                public_plans = [p for p in candidate.stage_plans if p.get("is_public") and p.get("start") is not None]
                if not public_plans:
                    continue
                for plan in public_plans:
                    start = float(plan.get("start") or 0)
                    launch_at = start + self.race_open_offset_seconds
                    end = float(plan.get("end") or 0) if plan.get("end") is not None else start + 3600
                    if now > end or now < start - self.race_prewarm_seconds:
                        continue
                    if plan.get("is_paid") and candidate.paid_decision != "confirmed":
                        continue
                    key = self._race_key(candidate, str(plan.get("key") or ""))
                    if now < launch_at:
                        with self.race_state_lock:
                            prepared = key in self.race_prepared or key in self.race_preparing
                        if not prepared:
                            self.race_prep_executor.submit(self._prewarm_candidate_race, candidate, plan)
                        continue
                    if now <= min(end, launch_at + self.race_launch_window_seconds):
                        # Do not churn launch workers while a protected automatic
                        # Free Mint is still awaiting/rejecting project identity.
                        # The social worker launches immediately on PASS.
                        if not self.social_protection_allows(candidate, plan):
                            continue
                        # Keep re-launching only while no transaction is pending;
                        # _race_wallet_work removes wallets already submitted.
                        if now - candidate.race_last_attempt >= self.race_retry_seconds:
                            self._submit_scheduled_race(candidate, plan)
            time.sleep(self.race_scheduler_tick)

    def start_race_scheduler(self) -> None:
        if not self.race_enabled or (self.race_scheduler_thread and self.race_scheduler_thread.is_alive()):
            return
        self.race_scheduler_thread = threading.Thread(target=self._race_scheduler_loop, name="public-race-scheduler", daemon=True)
        self.race_scheduler_thread.start()

    def _alchemy_wss_url(self, chain: str) -> str | None:
        api_key = os.getenv("ALCHEMY_API_KEY", "").strip()
        cfg = CHAIN_CONFIGS.get(chain) or {}
        slug = str(cfg.get("alchemy_slug") or "").strip()
        if not api_key or not slug:
            return None
        return f"wss://{slug}.g.alchemy.com/v2/{api_key}"

    async def _seadrop_log_loop(self, chain: str) -> None:
        url = self._alchemy_wss_url(chain)
        if not url:
            return
        while not STOP:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, open_timeout=10, close_timeout=3, max_size=2 * 1024 * 1024) as ws:
                    req = {
                        "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                        "params": ["logs", {"address": SEADROP_ADDRESS}],
                    }
                    await ws.send(json.dumps(req))
                    log.info("SeaDrop chain log stream connected | %s", chain)
                    while not STOP:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=45)
                        except asyncio.TimeoutError:
                            # websockets ping_interval already keeps the socket
                            # alive; an idle chain is not a reason to reconnect.
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        params = msg.get("params") if isinstance(msg, dict) else None
                        result = params.get("result") if isinstance(params, dict) else None
                        topics = result.get("topics") if isinstance(result, dict) else None
                        if not isinstance(topics, list) or len(topics) < 2:
                            continue
                        topic1 = str(topics[1])
                        if not topic1.startswith("0x") or len(topic1) != 66:
                            continue
                        contract = "0x" + topic1[-40:]
                        if Web3.is_address(contract):
                            self.submit_fast_contract_signal(chain, contract, None, "seadrop-wss")
            except Exception as exc:
                if not STOP:
                    log.debug("SeaDrop chain log stream %s reconnect: %s", chain, exc)
                    await asyncio.sleep(1.0)

    def _seadrop_log_worker(self, chain: str) -> None:
        try:
            asyncio.run(self._seadrop_log_loop(chain))
        except Exception as exc:
            if not STOP:
                log.debug("SeaDrop log worker stopped %s: %s", chain, exc)

    def start_seadrop_log_discovery(self) -> None:
        if not self.seadrop_wss_enabled:
            return
        if self.seadrop_log_threads:
            return
        for chain in self.enabled_chains:
            if not self._alchemy_wss_url(chain):
                continue
            t = threading.Thread(target=self._seadrop_log_worker, args=(chain,), name=f"seadrop-wss-{chain}", daemon=True)
            self.seadrop_log_threads.append(t)
            t.start()

    def _queue_mint_event(self, payload: dict[str, Any], source: str) -> None:
        chain = extract_event_chain(payload)
        if chain and chain not in self.enabled_chains:
            return
        contract = extract_contract_address(payload)
        slug = extract_event_slug(payload)
        if not contract and not slug:
            return
        # Stream is the speed signal. Submit it to the race lane *before* the
        # slower metadata de-duplication map; an Events-API backfill must never
        # suppress a newer live Stream signal for the same contract.
        if source == "stream" and chain and contract:
            self.submit_fast_contract_signal(chain, contract, slug, source)
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
        # V4.9: a live Stream signal no longer waits for the discovery queue or
        # the main candidate loop. Fire the independent race lane immediately.
        # The queue copy remains only for metadata enrichment/persistence.
        target_queue = self.auto_priority_queue if source == "stream" else self.auto_discovery_queue
        target_queue.put({
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

        # V4.9 FAST LANE: a live Stream mint event already gives us the most
        # useful real-time signal. When chain + contract are known, probe
        # SeaDrop directly before touching OpenSea REST. This bypasses REST
        # rate limits and can register an active free Public mint immediately.
        if self.auto_stream_fast_path and chain in self.rpc_pools and contract:
            try:
                public_fast = read_seadrop_public_drop(self.rpc_pools[chain].primary, contract)
            except Exception:
                public_fast = None
            if public_fast and public_fast.get("configured"):
                now_i = int(time.time())
                start_i = int(public_fast.get("start_time") or 0)
                end_i = int(public_fast.get("end_time") or 0)
                price_i = int(public_fast.get("mint_price_wei") or 0)
                remain_i = public_fast.get("remaining_supply")
                active_i = (not start_i or now_i >= start_i) and (not end_i or now_i < end_i)
                future_i = bool(start_i and now_i < start_i)
                if remain_i is None or int(remain_i) > 0:
                    fast_slug = slug or self.resolve_slug_from_contract(chain, contract) or f"contract-{contract[-10:].lower()}"
                    ok_fast, _msg_fast, fast_candidate = self.register_onchain_candidate(
                        fast_slug, chain, contract, str(item.get("source") or "stream"),
                        allow_paid=(price_i > 0),
                        max_mint_price_native=self.max_mint_price_default if price_i > 0 else Decimal("0"),
                        auto_discovered=True, discovery_source=str(item.get("source") or "stream"),
                    )
                    if ok_fast and fast_candidate:
                        fast_candidate.last_seen_auto = time.time()
                        if active_i and price_i == 0:
                            fast_candidate.watch_kind = "auto_free"
                            # Free Public discovered live: return immediately so
                            # try_candidate() can run in the same main-loop tick.
                            return
                        if future_i or price_i > 0:
                            fast_candidate.watch_kind = "auto_stage"
                            self.ensure_candidate_watch_persisted(fast_candidate)
                            self.maybe_offer_paid_public(fast_candidate)

        # Prefer Drop metadata next because it contains private/allowlist stage
        # schedules that getPublicDrop cannot expose. The fast lane above has
        # already protected live free mints from REST latency/429s.
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
                                self.notify_stage_schedule_once(candidate)
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
    def _background_detail_budget(self) -> int:
        status = self.opensea.rate_status()
        if float(status.get("cooldown") or 0) > 0:
            return 0
        remaining = status.get("remaining")
        reset_at = status.get("reset_at")
        if remaining is None or reset_at is None:
            # Before rate headers are known, start conservatively. The live
            # Stream fast lane does not need this REST budget.
            return min(4, self.auto_catalog_detail_budget)
        remaining = int(remaining)
        if remaining <= 10:
            return 0
        window = max(1.0, float(reset_at) - time.time())
        # Spend at most about half of the fair-share refill on detail backfill,
        # leaving quota for mint builders, manual checks, and stage refreshes.
        fair = int(max(0, remaining - 10) * (min(self.auto_upcoming_scan_seconds, self.auto_drop_scan_seconds) / window) * 0.5)
        return max(0, min(self.auto_catalog_detail_budget, fair))

    def _auto_free_scan_once(self, deep: bool = False) -> None:
        if not self.auto_free_enabled:
            return
        now_scan = time.time()

        # REST Mint Events is a fallback only. Live Stream remains continuous
        # and is processed immediately without consuming REST rate limit.
        if deep or now_scan - self._last_auto_event_scan >= self.auto_event_scan_seconds:
            if self.opensea.cooldown_remaining() <= 0:
                self._auto_mint_events_scan_once(deep=deep)
            self._last_auto_event_scan = now_scan

        # Catalog Drops is for backfill/schedules, not live free-mint speed.
        # Scan only `upcoming` every ~15s so scheduled Public/paid stages are
        # learned early; `featured`/`recently_minted` are slower backfill lanes.
        if self.opensea.cooldown_remaining() > 0:
            return
        if deep:
            types_to_scan = list(self.auto_free_drop_types)
            self._last_auto_upcoming_scan = now_scan
            self._last_auto_drop_scan = now_scan
        else:
            types_to_scan: list[str] = []
            if "upcoming" in self.auto_free_drop_types and now_scan - self._last_auto_upcoming_scan >= self.auto_upcoming_scan_seconds:
                types_to_scan.append("upcoming")
                self._last_auto_upcoming_scan = now_scan
            if now_scan - self._last_auto_drop_scan >= self.auto_drop_scan_seconds:
                types_to_scan.extend(t for t in self.auto_free_drop_types if t != "upcoming")
                self._last_auto_drop_scan = now_scan
            if not types_to_scan:
                return

        chain_query = ",".join(opensea_chain_name(c) for c in self.enabled_chains)
        slugs: dict[str, str | None] = {}
        pages = self.auto_free_initial_pages if deep else 1

        for drop_type in types_to_scan:
            cursor: str | None = None
            for _page in range(pages):
                if STOP or self.opensea.cooldown_remaining() > 0:
                    return
                try:
                    payload = self.opensea.get_drops(drop_type, chain_query, self.auto_free_drop_limit, cursor=cursor)
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

        now_seen = time.time()
        filtered_items: list[tuple[str, str | None]] = []
        for slug, hint in slugs.items():
            last_seen = self.auto_catalog_seen.get(slug.lower(), 0.0)
            if deep or now_seen - last_seen >= self.auto_catalog_detail_ttl:
                filtered_items.append((slug, hint))

        budget = self._background_detail_budget()
        if budget <= 0:
            return
        filtered_items = filtered_items[:budget]
        for slug, _hint in filtered_items:
            self.auto_catalog_seen[slug.lower()] = now_seen

        if len(self.auto_catalog_seen) > 10000:
            cutoff = now_seen - max(600.0, self.auto_catalog_detail_ttl * 5)
            self.auto_catalog_seen = {k: ts for k, ts in self.auto_catalog_seen.items() if ts >= cutoff}

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

        # A non-empty catalog can still produce an empty filtered list after
        # TTL/budget filtering. Never construct ThreadPoolExecutor(0).
        if not filtered_items:
            return
        workers = max(1, min(self.auto_free_detail_workers, len(filtered_items)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(fetch, item) for item in filtered_items]
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
        """Drain live mint signals first, then a bounded amount of catalog work.

        V4.6 drained the entire queue in one pass; a busy backfill could delay a
        brand-new Stream event. V4.9 gives Stream signals strict priority and
        limits slow catalog work per main-loop tick.
        """
        def process_item(item: dict[str, Any]) -> None:
            if item.get("kind") == "mint_event":
                self._process_auto_mint_event(item)
                return
            slug = str(item["slug"])
            drop = item["drop"]
            hint = item.get("chain_hint")
            active_free = bool(item.get("active_free", False))
            relevant_stage = bool(item.get("relevant_stage", False))
            has_qualification = bool(item.get("has_qualification", False))

            existing = next((c for c in self.candidates.values() if c.slug.lower() == slug.lower()), None)
            if existing and not existing.auto_discovered:
                self.register_drop(
                    slug, drop, existing.chain, existing.source,
                    allow_paid=existing.allow_paid,
                    max_mint_price_native=existing.max_mint_price_native,
                    quantity_override=existing.quantity_override,
                    auto_discovered=False,
                )
                return

            if not active_free and not relevant_stage:
                if existing and existing.auto_discovered and existing.watch_kind == "auto_free":
                    pending = any(st.submitted and not st.confirmed and not st.final for st in existing.wallets.values())
                    if not pending:
                        self.candidates.pop(f"{existing.chain}:{existing.slug}", None)
                return

            before = existing is not None
            allow_paid = relevant_stage or has_qualification
            ok, _message, candidate = self.register_drop(
                slug, drop, hint, "auto-stage" if relevant_stage else "auto-free",
                allow_paid=allow_paid,
                max_mint_price_native=self.max_mint_price_default if allow_paid else Decimal("0"),
                auto_discovered=True,
            )
            if not ok or not candidate:
                return
            candidate.last_seen_auto = time.time()
            if relevant_stage or has_qualification or candidate.has_paid_stage:
                candidate.watch_kind = "auto_stage"
                self.ensure_candidate_watch_persisted(candidate)
                self.persist_candidate_planning(candidate)
                if not before:
                    self.notify_stage_schedule_once(candidate)
            else:
                candidate.watch_kind = "auto_free"

            if not before and self.auto_free_notify_discovery and not candidate.qualification_tracked:
                self.notify_all(
                    "🆓 <b>تم اكتشاف Mint مجاني تلقائيًا</b>\n\n"
                    f"📦 المشروع: {candidate.slug}\n"
                    f"🌐 الشبكة: {chain_label(candidate.chain)}\n"
                    f"👛 المحافظ النشطة: {len(candidate.wallets)}\n\n"
                    f"{self.mint_link_block(candidate)}"
                )

        # Live Stream has strict priority.
        for _ in range(200):
            try:
                item = self.auto_priority_queue.get_nowait()
            except queue.Empty:
                break
            process_item(item)

        # Keep catalog/events fallback from monopolizing the mint loop.
        for _ in range(40):
            try:
                item = self.auto_discovery_queue.get_nowait()
            except queue.Empty:
                break
            process_item(item)

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
            gas_override_usd=None if candidate.gas_override_usd is None else str(candidate.gas_override_usd),
            ignore_gas_cap=candidate.ignore_gas_cap,
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
        is_recheck = stage_key in candidate.checked_stage_keys and not force
        if is_recheck:
            transient = {"unknown", "rate_limited", "preflight_error", "opensea_error", "seadrop_not_configured"}
            states = [s for s in states if s.eligibility in transient]
            if not states:
                return ""
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
                # Eligibility only needs to answer "can this wallet mint in the
                # active stage?". Probing the full target here could trigger a
                # binary quantity search (several POSTs per wallet) and exhaust
                # the OpenSea bucket before the actual mint. Probe one token;
                # the execution path still adapts to the highest valid quantity.
                result = check_eligibility(self.opensea, candidate.slug, state.wallet, 1)
                if result.eligible is True:
                    result.quantity_used = None
            return state, confirmed_total, additional, result

        if states:
            workers = min(
                self.max_parallel_wallets if candidate.mint_backend == "seadrop" else self.opensea_eligibility_workers,
                len(states),
            )
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
                        state.eligibility = status
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

                        # Do not immediately call OpenSea's mint builder again
                        # for wallets that were already found ineligible. That
                        # duplicate call pattern was a major source of 429s.
                        if result.eligible is False:
                            state.next_attempt = float(plan.get("end") or (now + max(60.0, self.qualification_recheck_seconds)))
                        elif result.eligible is None:
                            cooldown = self.opensea.cooldown_remaining()
                            state.next_attempt = now + max(self.qualification_recheck_seconds, cooldown)

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
        lines.extend(["", self.mint_link_block(candidate)])
        text = "\n".join(lines)[:3900]
        if notify:
            self.notify_all(text)
        return text

    def stage_schedule_summary_text(self, candidate: Candidate) -> str:
        # Paid stages are deliberately omitted from automatic Telegram pushes;
        # they remain visible only inside «💳 المنتات المدفوعة».
        plans = sorted(
            [p for p in candidate.stage_plans if not p.get("is_paid")],
            key=lambda p: float(p.get("start") or 0),
        )
        lines = [
            "🎟 تم اكتشاف منت متعدد المراحل",
            "",
            f"📦 المشروع: {candidate.slug}",
            f"🌐 الشبكة: {chain_label(candidate.chain)}",
            f"📋 عدد المراحل: {len(plans)}",
            "",
            "🗓 جدول المراحل:",
        ]
        for idx, plan in enumerate(plans[:10], 1):
            kind = "🌍 Public" if plan.get("is_public") else "🎫 تأهيل"
            price = "🆓 مجاني" if plan.get("is_free") else ("💳 مدفوع" if plan.get("is_paid") else "❔ السعر غير معروف")
            lines.append(
                f"{idx}. {kind} — {plan.get('label') or 'Mint'}\n"
                f"   ⏰ {format_ts(plan.get('start'), self.display_tz)}\n"
                f"   {price} | الحد/المحفظة: {plan.get('wallet_limit') or 'غير محدد'}"
            )
        lines.extend([
            "",
            "🔕 فحص المحافظ أثناء التأهيل يتم بصمت لتجنب كثرة الرسائل.",
            "🔔 سأرسل تنبيهًا واحدًا فقط عند بدء مرحلة جديدة.",
            "",
            self.mint_link_block(candidate),
        ])
        return "\n".join(lines)[:3900]

    def notify_stage_schedule_once(self, candidate: Candidate) -> None:
        if not self.stage_summary_notifications or candidate.schedule_summary_notified:
            return
        visible_plans = [p for p in candidate.stage_plans if not p.get("is_paid")]
        if not visible_plans:
            return
        if not candidate.qualification_tracked:
            return
        now = time.time()
        if not any(self._is_timestamp_today(p.get("start")) or plan_is_active(p, now) for p in visible_plans):
            return
        if candidate.discovery_source == "restored":
            candidate.schedule_summary_notified = True
            return
        candidate.schedule_summary_notified = True
        self.notify_all(self.stage_schedule_summary_text(candidate))

    def notify_stage_opened(self, candidate: Candidate, plan: dict[str, Any]) -> None:
        if not self.stage_open_notifications:
            return
        if plan.get("is_paid"):
            # Paid mints remain silent and are shown only in the dedicated paid section.
            return
        stage_key = str(plan.get("key") or "")
        if not stage_key or stage_key in candidate.stage_open_notified_keys:
            return
        candidate.stage_open_notified_keys.add(stage_key)
        target = self.stage_target_total(candidate, plan)
        kind = "🌍 Public" if plan.get("is_public") else "🎫 مرحلة تأهيل"
        price = "🆓 مجاني" if plan.get("is_free") else ("💳 مدفوع" if plan.get("is_paid") else "❔ السعر غير معروف")
        action = (
            "سيتم التنفيذ تلقائيًا للمحافظ المؤهلة." if plan.get("is_free")
            else "سيتم فحص الأهلية بصمت وحفظ النتائج في قسم التأهيل."
        )
        active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
        stage_states = [s for s in candidate.wallets.values() if s.wallet.address.lower() in active_addresses]
        eligible_count = sum(1 for s in stage_states if s.eligibility in {"eligible_now", "target_satisfied"})
        ineligible_count = sum(1 for s in stage_states if s.eligibility in {"not_eligible_now", "precondition_failed"})
        pending_count = max(0, len(stage_states) - eligible_count - ineligible_count)
        qualification_summary = (
            f"👛 المحافظ: ✅ {eligible_count} مؤهلة | ❌ {ineligible_count} غير مؤهلة"
            + (f" | ⏳ {pending_count} مؤقت" if pending_count else "")
        )
        self.notify_all(
            "🔔 بدأت مرحلة جديدة\n\n"
            f"📦 المشروع: {candidate.slug}\n"
            f"🌐 الشبكة: {chain_label(candidate.chain)}\n"
            f"🎟 المرحلة: {plan.get('label') or 'Mint'}\n"
            f"🏷 النوع: {kind} — {price}\n"
            f"⏰ البداية: {format_ts(plan.get('start'), self.display_tz)}\n"
            f"⏳ النهاية: {format_ts(plan.get('end'), self.display_tz)}\n"
            f"📦 الهدف/المحفظة: {target}\n"
            f"{qualification_summary}\n\n"
            f"⚙️ {action} التفاصيل الكاملة محفوظة داخل قسم «🎟 التأهيل».\n\n"
            f"{self.mint_link_block(candidate)}"
        )

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

        V4.6 intentionally does not push paid-mint prompts to Telegram. Paid
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
                if current.get("is_public") and not current.get("is_paid"):
                    # Critical V4.9 behavior: a free Public must never wait for
                    # an OpenSea eligibility preflight. Activate every enabled
                    # wallet immediately; the mint transaction/simulation is the
                    # final oracle. This avoids losing small-supply drops to 429s.
                    active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
                    target_total = self.stage_target_total(candidate, current)
                    for state in candidate.wallets.values():
                        if state.wallet.address.lower() not in active_addresses or state.submitted:
                            continue
                        state.final = False
                        state.eligibility = "public_open"
                        state.target_total = target_total
                        state.stage_key = candidate.current_stage_key
                        state.stage_label = candidate.current_stage_label
                        state.next_attempt = now
                    log.info(
                        "FREE PUBLIC OPEN | %s | chain=%s | active_wallets=%s | target=%s",
                        candidate.slug, candidate.chain, len(active_addresses), target_total,
                    )
                else:
                    self.stage_qualification_check(candidate, current, notify=False, force=True)
                self.notify_stage_opened(candidate, current)
            elif not current.get("is_public") and now - candidate.last_qualification_check >= self.qualification_recheck_seconds:
                # Only retry transient/unknown wallets inside the same stage.
                # Definitive eligible/ineligible results are checked again when
                # the next stage opens, exactly matching the stage schedule.
                self.stage_qualification_check(candidate, current, notify=False, force=False)
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
                # Manual eligibility is a yes/no check. Use one-token preflight
                # to avoid quantity-search API bursts; actual mint quantity is
                # resolved later by the execution engine.
                result = check_eligibility(self.opensea, candidate.slug, state.wallet, 1)
                if result.eligible is True:
                    result.quantity_used = None
            balance = None
            try:
                balance = pool.balance_native(state.wallet.address)
            except Exception:
                pass
            return state, result, balance

        workers = min(
            self.max_parallel_wallets if candidate.mint_backend == "seadrop" else self.opensea_eligibility_workers,
            len(states),
        )
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
        lines.extend(["", self.mint_link_block(candidate)])
        return "\n".join(lines)[:3900]

    def notify_state_change(self, candidate: Candidate, state: WalletState, status: str, detail: str | None = None) -> None:
        if status == "insufficient_balance":
            self.notify_insufficient_balance(candidate, state, detail=detail)
            return
        if status == state.last_notified_status:
            return
        state.last_notified_status = status
        if status in {"not_mintable_yet", "not_active_yet", "paid_wallet_selection_required"}:
            return
        if status == "rate_limited" and self.silent_rate_limit_telegram:
            # 429 is an API transport condition, not a wallet/mint failure. The
            # central REST limiter logs it once and retries after Retry-After.
            return
        if not self.routine_stage_notifications:
            # Monitoring/qualification/preflight state belongs in the dashboard,
            # not as unsolicited Telegram spam. Transaction lifecycle messages
            # are emitted elsewhere and remain enabled.
            log.info("Mint state | %s | %s | %s | %s", candidate.slug, state.wallet.name, status, (detail or "")[:160])
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
                f"{labels[status]}\n\n"
                f"📦 المشروع: {candidate.slug}\n"
                f"🌐 الشبكة: {chain_label(candidate.chain)}\n"
                f"👛 المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                + (f"📝 التفاصيل: {(detail or '')[:500]}\n\n" if detail else "\n")
                + self.mint_link_block(candidate)
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

        # Protected Free Mint Shield is project-level and runs once. Never let
        # the fallback path bypass it while Race Lane is waiting for metadata.
        if plan and plan.get("is_public") and not plan.get("is_paid"):
            if not self.social_protection_allows(candidate, plan):
                return

        # V4.9: do not let the normal REST/simulation path compete with the
        # dedicated race lane during the first seconds of a known SeaDrop Public
        # opening. The scheduler pre-signs and broadcasts independently. After
        # the race window, this method becomes the safety fallback again.
        if candidate.race_inflight:
            return
        if self.race_enabled and candidate.contract_address and plan and plan.get("is_public"):
            start_ts = float(plan.get("start") or now)
            if now >= start_ts - self.race_prewarm_seconds and now <= start_ts + self.race_launch_window_seconds:
                if not plan.get("is_paid") or candidate.paid_decision == "confirmed":
                    return

        due = [state for state in candidate.wallets.values() if not state.submitted and not state.final and state.next_attempt <= now]
        if not due:
            return
        pool = self.rpc_pools[candidate.chain]
        # Public stages prefer a direct SeaDrop transaction when the contract is
        # known/configured. This avoids OpenSea REST rate limits on the speed path.
        direct_public_seadrop = False
        if plan and plan.get("is_public") and candidate.contract_address:
            try:
                _public_cfg = read_seadrop_public_drop(pool.primary, candidate.contract_address)
                direct_public_seadrop = bool(_public_cfg and _public_cfg.get("configured"))
            except Exception:
                direct_public_seadrop = False

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
                max_gas_usd=self.max_gas_usd_for_chain(candidate.chain, candidate),
                native_usd_price=self.native_usd_price_for_chain(candidate.chain),
            )
            if (candidate.mint_backend == "seadrop" or direct_public_seadrop) and candidate.contract_address:
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
                        "🚀 تم إرسال معاملة Mint\n\n"
                        f"📦 المشروع: {candidate.slug}\n"
                        f"🎟 المرحلة: {state.stage_label or 'Mint'}\n"
                        f"🏷 النوع: {kind}\n"
                        f"🌐 الشبكة: {chain_label(candidate.chain)}\n"
                        f"👛 المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                        f"🔢 الكمية: {state.pending_quantity}\n"
                        f"💰 قيمة المنت: {result.mint_value_native} {native_symbol(candidate.chain)}\n"
                        f"⛽ أقصى تقدير للغاز: {result.gas_cost_native} {native_symbol(candidate.chain)}"
                        + (f" ≈ ${result.gas_cost_usd:.4f}" if result.gas_cost_usd is not None else "")
                        + "\n\n"
                        + self.mint_link_block(candidate)
                        + "\n\n🔎 المعاملة:\n" + str(url)
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
                    detail_l = str(result.detail or "").lower()
                    if any(marker in detail_l for marker in ("wallet already reached", "sold out", "no on-chain supply remains")):
                        state.final = True
                        state.next_attempt = time.time() + max(15.0, self.stage_refresh_seconds)
                    elif plan and plan.get("is_public") and not plan.get("is_paid"):
                        state.next_attempt = time.time() + self.public_fast_retry_seconds
                    else:
                        state.next_attempt = time.time() + self.qualification_recheck_seconds
                elif result.status == "rate_limited":
                    cooldown = self.opensea.cooldown_remaining()
                    state.next_attempt = time.time() + max(self.rate_limit_retry_seconds, cooldown)
                elif result.status in {"gas_usd_too_high", "gas_price_unavailable"}:
                    state.next_attempt = time.time() + self.gas_over_budget_retry_seconds
                elif result.status == "insufficient_balance":
                    state.next_attempt = time.time() + max(self.low_balance_retry_seconds, 2.0)
                elif result.status in {"gas_too_high", "mint_price_too_high", "total_spend_too_high"}:
                    state.next_attempt = time.time() + max(10, self.eligibility_retry_seconds)
                elif result.status in {"paid_not_allowed", "target_not_allowed"}:
                    state.next_attempt = time.time() + 30
                elif result.status in {"seadrop_not_configured", "no_fee_recipient"}:
                    state.next_attempt = time.time() + 15
                else:
                    state.next_attempt = time.time() + self.monitor_retry_interval
                if result.status == "insufficient_balance":
                    self.notify_insufficient_balance(candidate, state, result=result, detail=result.detail)
                else:
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
                    "✅ تم تأكيد الـMint بنجاح\n\n"
                    f"📦 المشروع: {candidate.slug}\n"
                    f"🎟 المرحلة: {state.stage_label or 'Mint'}\n"
                    f"🌐 الشبكة: {chain_label(candidate.chain)}\n"
                    f"👛 المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"🔢 الكمية المؤكدة الآن: {confirmed_qty}\n"
                    f"📊 إجمالي هذه المحفظة من المشروع: {total}\n\n"
                    + self.mint_link_block(candidate)
                    + "\n\n🔎 المعاملة:\n" + explorer_tx_url(candidate.chain, state.tx_hash)
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
                    "❌ فشلت معاملة الـMint على الشبكة\n\n"
                    f"📦 المشروع: {candidate.slug}\n"
                    f"🎟 المرحلة: {state.stage_label or 'Mint'}\n"
                    f"🌐 الشبكة: {chain_label(candidate.chain)}\n"
                    f"👛 المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n\n"
                    + self.mint_link_block(candidate)
                    + "\n\n🔎 TX:\n" + str(failed_hash)
                )

    # ---------- Telegram UI ----------
    def set_execution_paused(self, paused: bool) -> None:
        """Pause/resume every signing+broadcast path, including Race Lane."""
        self.paused = bool(paused)
        log.info("Execution pause state | paused=%s", self.paused)
        if self.paused:
            # Prepared transactions are intentionally discarded so Resume never
            # broadcasts a stale nonce/fee snapshot built before the pause.
            with self.race_state_lock:
                self.race_prepared.clear()
                self.race_queued.clear()

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

    def settings_buttons(self) -> list[list[tuple[str, str]]]:
        eth = self.max_gas_usd_for_chain("ethereum")
        ink = self.max_gas_usd_for_chain("ink")
        rh = self.max_gas_usd_for_chain("robinhood")
        return [
            [(f"⛽ عام ${self.max_gas_usd}", "gas_set:global")],
            [(f"Ξ Ethereum ${eth}", "gas_set:ethereum"), ("↩️ عام", "gas_inherit:ethereum")],
            [(f"🟣 Ink ${ink}", "gas_set:ink"), ("↩️ عام", "gas_inherit:ink")],
            [(f"🟢 Robinhood ${rh}", "gas_set:robinhood"), ("↩️ عام", "gas_inherit:robinhood")],
            [("🔥 استثناء منت من حد الغاز", "gas_project_add")],
            [("📋 استثناءات الغاز", "gas_project_list")],
            [("🛡 حماية Free Mint: مفعلة" if self.free_social_protection_enabled else "⚠️ حماية Free Mint: متوقفة", "free_social_toggle")],
            [("🔕 إيقاف إشعارات المراحل" if self.routine_stage_notifications else "🔔 تفعيل إشعارات المراحل", "stage_notify_toggle")],
            [("↩️ القائمة الرئيسية", "menu")],
        ]

    def gas_project_policy_buttons(self, candidate: Candidate) -> list[list[tuple[str, str]]]:
        token = self.candidate_token(candidate)
        return [
            [("🔥 بدون حد لهذا المنت", f"gpi:{token}")],
            [("⛽ حد خاص بالدولار", f"gpc:{token}")],
            [("🛡 استخدام حد الشبكة", f"gpr:{token}")],
            [("↩️ الإعدادات", "settings")],
        ]

    def gas_project_list_text(self) -> str:
        rows = []
        for c in self.candidates.values():
            if c.ignore_gas_cap or c.gas_override_usd is not None:
                rows.append(c)
        if not rows:
            return "🔥 لا توجد استثناءات غاز خاصة بالمشاريع حاليًا."
        lines = ["🔥 استثناءات الغاز للمشاريع", ""]
        for c in rows[:30]:
            lines.extend([
                f"📦 {c.slug}",
                f"🌐 {chain_label(c.chain)}",
                f"{self.project_gas_policy_text(c)}",
                f"🔗 {self.candidate_mint_url(c)}",
                "",
            ])
        return "\n".join(lines)[:3900]

    def send_menu(self, chat_id: str) -> None:
        active = len(self.store.list_wallets(enabled_only=True))
        total = len(self.store.list_wallets(enabled_only=False))
        self.telegram.send(
            chat_id,
            "🤖 OpenSea Mint Guardian V4.11.3\n\n"
            "🆓 الاكتشاف المجاني: Stream لحظي + SeaDrop مباشر + REST احتياطي\n"
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
                + f"\n  🔗 {self.candidate_mint_url(candidate)}"
            )
        return "\n".join(lines)[:3900]

    def settings_text(self) -> str:
        rate = self.opensea.rate_status()
        cooldown = float(rate.get("cooldown") or 0)
        rest_state = "جاهز" if cooldown <= 0 else f"تهدئة {cooldown:.1f}s"
        remaining = rate.get("remaining")
        limit = rate.get("limit")
        quota = "غير معروف" if remaining is None else f"{remaining}/{limit or '?'}"
        return (
            "⚙️ إعدادات التشغيل الحالية\n\n"
            f"▶️ التنفيذ: {'متوقف' if self.paused else 'يعمل'}\n"
            f"⚡ Stream اللحظي: {'مفعّل' if self.auto_stream_enabled else 'متوقف'} — أولوية قصوى ولا يستهلك REST quota\n"
            f"🛟 Mint Events احتياطي: كل {self.auto_event_scan_seconds:g} ثانية\n"
            f"⏰ Upcoming schedules: كل {self.auto_upcoming_scan_seconds:g} ثانية\n"
            f"📚 بقية Drops backfill: كل {self.auto_drop_scan_seconds:g} ثانية\n"
            f"🚦 OpenSea REST: {rest_state} | المتبقي: {quota}\n"
            f"🎟 التأهيل: يُفحص ويحفظ بصمت كل {self.qualification_recheck_seconds:g} ثانية عند الحاجة\n"
            f"🚀 Public مجاني: تنفيذ مباشر لكل المحافظ | استعداد {self.public_preopen_window_seconds:g}s | retry={self.public_fast_retry_seconds:g}s\n"
            f"📦 سياسة الكمية: حد ≤{self.auto_stage_high_limit_threshold} كهدف؛ أعلى/غير محدود = {self.auto_stage_high_limit_quantity}\n"
            f"💳 Public المدفوع: موافقة + محافظ + كمية لكل محفظة\n"
            f"🛡 حماية Free Mint: {'مفعلة — يشترط X أو Website للمنت التلقائي بدون تأهيل' if self.free_social_protection_enabled else 'متوقفة — يأخذ جميع Free Mints كما في V4.10.1'}\n"
            f"⛽ الحد العام: ${self.max_gas_usd} | Ethereum=${self.max_gas_usd_for_chain('ethereum')} | Ink=${self.max_gas_usd_for_chain('ink')} | Robinhood=${self.max_gas_usd_for_chain('robinhood')}\n"
            f"💸 تنبيه نقص رصيد الغاز: مفعّل دائمًا | إعادة المحاولة كل {self.low_balance_retry_seconds:g}s\n"
            f"🔕 إشعارات المراقبة/التأهيل التلقائية: {'مفعلة' if self.routine_stage_notifications else 'متوقفة'}\n"
            f"🧠 Gas strategy: {self.gas_strategy} | Buffer={self.gas_limit_buffer}\n"
            f"👛 المحافظ المتوازية: {self.max_parallel_wallets}"
        )

    def history_text(self) -> str:
        rows = self.store.recent_history(12)
        if not rows:
            return "📜 لا يوجد سجل Mint حتى الآن."
        status_labels = {"submitted": "🚀 أُرسلت", "confirmed": "✅ تأكدت", "reverted": "❌ فشلت"}
        lines = ["📜 سجل عمليات الـMint", ""]
        for row in rows:
            ts = format_ts(float(row["created_at"]), self.display_tz)
            label = status_labels.get(str(row["status"]), str(row["status"]))
            url = self.row_mint_url(row)
            lines.extend([
                f"{label} — {row['slug']}",
                f"🌐 {chain_label(str(row.get('chain') or ''))} | 👛 {row['wallet_name']}",
                f"🕒 {ts}",
                f"🔗 {url}" + (f"\n🔎 TX: {row['tx_hash']}" if row.get("tx_hash") else ""),
                "",
            ])
        return "\n".join(lines)[:3900]

    def free_mints_text(self) -> str:
        rows = self.store.free_mint_summary(25)
        if not rows:
            return (
                "🆓 لا توجد Free Mints مؤكدة أخذها البوت حتى الآن.\n\n"
                "عندما تنجح معاملة مجانية ستظهر هنا بعد تأكيدها على الشبكة."
            )
        lines = ["🆓 المنتات المجانية التي تم أخذها", ""]
        for row in rows:
            names = [x.strip() for x in str(row.get("wallet_names") or "").split(",") if x.strip()]
            wallet_text = "، ".join(names[:4]) + (f" +{len(names)-4}" if len(names) > 4 else "")
            lines.extend([
                f"✅ {row.get('slug')}",
                f"🌐 الشبكة: {chain_label(str(row.get('chain') or ''))}",
                f"🔢 الكمية الإجمالية: {int(row.get('total_quantity') or 0)}",
                f"👛 المحافظ: {int(row.get('wallet_count') or 0)} — {wallet_text or 'غير معروف'}",
                f"🕒 آخر تأكيد: {format_ts(float(row.get('last_confirmed_at') or 0), self.display_tz)}",
                f"🔗 رابط المنت: {self.row_mint_url(row)}",
                "",
            ])
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
            today_plans = [p for p in plans if self._is_timestamp_today(p.get("start")) or plan_is_active(p, now)]
            if today_plans:
                today_rows.append((row, today_plans))
        if not today_rows:
            return "🎟 لا توجد منتات تأهيل مسجلة لتاريخ اليوم حتى الآن."
        lines = ["📅 تأهيلات اليوم", ""]
        for row, plans in today_rows[:12]:
            project_key = str(row["project_key"])
            lines.extend([
                f"🎟 {row['slug']}",
                f"🌐 الشبكة: {chain_label(str(row['chain']))}",
                f"🔗 رابط المنت: {self.row_mint_url(row)}",
            ])
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
                    suffix = f" ({confirmed}/{target})" if target else (f" ({confirmed})" if confirmed else "")
                    eligible_names.append(str(wallet_row["wallet_name"]) + suffix)
                eligible_text = "، ".join(eligible_names) if eligible_names else "لا توجد محفظة مؤهلة حتى آخر فحص"
                mode = "🌍 Public" if plan.get("is_public") else "🎫 تأهيل"
                lines.extend([
                    f"  {mode} — {plan.get('label')}",
                    f"  ⏰ {format_ts(plan.get('start'), self.display_tz)}",
                    f"  👛 المؤهلة: {eligible_text}",
                ])
            lines.append("")
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
        lines = ["🗄 التأهيلات القديمة", ""]
        for row in old[:20]:
            status = "منتهٍ" if str(row.get("status")) != "active" else "من تاريخ سابق"
            lines.extend([
                f"🎟 {row['slug']} — {status}",
                f"🌐 {chain_label(str(row['chain']))}",
                f"🏁 آخر مرحلة: {row.get('last_stage_label') or 'غير معروف'}" + (f" | {row.get('archive_reason')}" if row.get("archive_reason") else ""),
                f"🔗 {self.row_mint_url(row)}",
                "",
            ])
        return "\n".join(lines)[:3900]

    def monitoring_active_text(self) -> str:
        rows = self.store.list_watches(active_only=True)
        if not rows:
            return "📡 لا توجد منتات محفوظة تحت المراقبة حاليًا."
        lines = ["📡 المنتات تحت المراقبة", ""]
        for row in rows[:25]:
            kind = {"manual": "🖐 يدوي", "auto_stage": "🤖 تلقائي/مراحل", "auto_free": "🆓 تلقائي/مجاني"}.get(str(row.get("watch_kind")), str(row.get("watch_kind") or ""))
            paid = str(row.get("paid_decision") or "")
            paid_text = {"pending": "💳 بانتظار قرار", "confirmed": "✅ شراء مدفوع مؤكد", "declined": "🚫 المدفوع مرفوض"}.get(paid, "")
            lines.extend([
                f"📦 {row['slug']}",
                f"🌐 {chain_label(str(row.get('chain') or ''))} | {kind}" + (f" | {paid_text}" if paid_text else ""),
                f"⏰ المرحلة القادمة: {format_ts(row.get('next_stage_start'), self.display_tz)}",
                f"🔗 {self.row_mint_url(row)}",
                "",
            ])
        return "\n".join(lines)[:3900]

    def monitoring_old_text(self) -> str:
        rows = self.store.list_archived_watches(30)
        if not rows:
            return "🗄 لا توجد منتات مراقبة قديمة حتى الآن."
        lines = ["🗄 المنتات القديمة", ""]
        for row in rows[:25]:
            when = row.get("archived_at") or row.get("updated_at")
            lines.extend([
                f"📦 {row['slug']}",
                f"🌐 {chain_label(str(row.get('chain') or ''))}",
                f"🏁 انتهت: {format_ts(when, self.display_tz)}",
                f"📝 السبب: {row.get('archive_reason') or 'انتهت المراقبة'}",
                f"🔗 {self.row_mint_url(row)}",
                "",
            ])
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
            f"💳 {candidate.slug}\n"
            f"🌐 {chain_label(candidate.chain)}\n"
            f"💰 السعر: {native_text}\n"
            f"💵 التقريبي: {usdt_text}\n"
            f"⏰ الفتح: {open_text}\n"
            f"📌 الحالة: {decision}\n"
            f"🔗 {self.candidate_mint_url(candidate)}"
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
            f"أقصى كمية اختيارية/محفظة لهذه المرحلة: {self.paid_quantity_cap(candidate)}\n"
            f"سياسة الغاز: {self.project_gas_policy_text(candidate)}\n\n"
            "اختر المحافظ، ثم اضغط زر الكمية بجانب كل محفظة وأدخل الكمية المطلوبة.\n"
            "لن يتم توقيع أي Mint مدفوع قبل الضغط على «🚀 تأكيد خطة الشراء».\n"
            "أي Public مجاني سيبقى تلقائيًا لجميع المحافظ النشطة.\n\n"
            f"المحدد حاليًا:\n{selected_text}\n\n"
            f"{self.mint_link_block(candidate)}"
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
        gas_label = "🛡 إعادة حد الغاز" if candidate.ignore_gas_cap else "🔥 تجاوز حد الغاز لهذا المنت"
        rows.append([(gas_label, f"pgo:{token}"), ("⛽ حد خاص", f"gpc:{token}")])
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
        rows.insert(0, [("➕ إضافة رابط منت مدفوع", "paid_add")])
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
        self.pending_gas_setting.pop(chat_id, None)
        self.pending_gas_project.pop(chat_id, None)
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
            self.edit_or_send(event, self.settings_text(), self.settings_buttons())
            return

        if data.startswith("gpi:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                self.telegram.send(chat_id, "⚠️ لم يعد المشروع موجودًا في المراقبة.")
                return
            self.set_candidate_gas_policy(candidate, ignore=True, override_usd=None)
            self.edit_or_send(
                event,
                "🔥 تم تفعيل تجاوز حد الغاز لهذا المنت فقط.\n\n"
                f"📦 {candidate.slug}\n🌐 {chain_label(candidate.chain)}\n"
                "⚠️ قد تُرسل المعاملة برسوم أعلى من الحدود العامة إذا توفر الرصيد.\n\n"
                + self.mint_link_block(candidate),
                self.gas_project_policy_buttons(candidate),
            )
            return

        if data.startswith("gpr:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            self.set_candidate_gas_policy(candidate, ignore=False, override_usd=None)
            self.edit_or_send(
                event,
                "🛡 عاد هذا المنت لاستخدام حد الغاز الخاص بالشبكة.\n\n"
                f"📦 {candidate.slug}\n{self.project_gas_policy_text(candidate)}\n\n" + self.mint_link_block(candidate),
                self.gas_project_policy_buttons(candidate),
            )
            return

        if data.startswith("gpc:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            self._clear_pending(chat_id)
            self.pending_gas_setting[chat_id] = {
                "expiry": time.time() + 180, "target": "project",
                "candidate_token": self.candidate_token(candidate),
            }
            self.telegram.send(
                chat_id,
                f"⛽ حد غاز خاص — {candidate.slug}\n\n"
                "أرسل الحد بالدولار مثل 0.15 أو 0.50.\n"
                "هذا الحد سيطبق على هذا المنت فقط ويُحفظ بعد Restart/Redeploy.\n\nأرسل /cancel للإلغاء.",
            )
            return

        if data.startswith("gas_inherit:"):
            chain = normalize_chain(data.split(":", 1)[1])
            if chain not in self.enabled_chains:
                return
            self.chain_gas_caps_usd[chain] = None
            self.store.delete_setting(f"gas_usd_{chain}")
            self.edit_or_send(
                event,
                f"✅ {chain_label(chain)} عاد لاستخدام الحد العام: ${self.max_gas_usd}\n\n" + self.settings_text(),
                self.settings_buttons(),
            )
            return

        if data.startswith("gas_set:"):
            target = data.split(":", 1)[1]
            if target != "global" and normalize_chain(target) not in self.enabled_chains:
                return
            self._clear_pending(chat_id)
            self.pending_gas_setting[chat_id] = {"expiry": time.time() + 180, "target": target}
            current = self.max_gas_usd if target == "global" else self.max_gas_usd_for_chain(normalize_chain(target))
            self.telegram.send(
                chat_id,
                f"⛽ تعديل حد الغاز — {target}\n\nالقيمة الحالية: ${current}\n"
                "أرسل الحد الجديد بالدولار مثل 0.08 أو 0.25.\n"
                "القيمة 0 تعني بدون حد، لذلك استخدمها فقط إذا كنت تقبل أي رسوم.\n\nأرسل /cancel للإلغاء.",
            )
            return

        if data == "gas_project_add":
            self._clear_pending(chat_id)
            self.pending_gas_project[chat_id] = {"expiry": time.time() + 300}
            self.telegram.send(
                chat_id,
                "🔥 استثناء منت من حد الغاز\n\nأرسل رابط المنت. سأضيفه للمراقبة إن لم يكن موجودًا، "
                "ثم أجعله يتجاوز حد الغاز العالمي/الشبكة حتى تلغي الاستثناء.\n\n⚠️ هذا قد يدفع رسومًا مرتفعة. أرسل /cancel للإلغاء.",
            )
            return

        if data == "gas_project_list":
            self.edit_or_send(event, self.gas_project_list_text(), self.settings_buttons())
            return

        if data == "free_social_toggle":
            self.free_social_protection_enabled = not self.free_social_protection_enabled
            self.store.set_setting(
                "free_social_protection_enabled", "1" if self.free_social_protection_enabled else "0"
            )
            now = time.time()
            with self.candidates_lock:
                candidates = list(self.candidates.values())
            if self.free_social_protection_enabled:
                for candidate in candidates:
                    if candidate.auto_discovered and not candidate.qualification_tracked:
                        self.ensure_social_trust_async(candidate)
            else:
                # Restore the exact pre-shield behavior immediately: all due
                # automatic free projects may proceed without another metadata check.
                for candidate in candidates:
                    for state in candidate.wallets.values():
                        if not state.submitted and not state.final:
                            state.next_attempt = min(state.next_attempt or now, now)
                    plan = self.current_plan_for_candidate(candidate, now) if candidate.stage_plans else None
                    if (
                        candidate.auto_discovered and plan and plan.get("is_public") and not plan.get("is_paid")
                        and candidate.contract_address and self.race_enabled
                    ):
                        self.race_launch_executor.submit(self._launch_candidate_race, candidate, plan, live=True)
            self.edit_or_send(event, self.settings_text(), self.settings_buttons())
            return

        if data == "stage_notify_toggle":
            self.routine_stage_notifications = not self.routine_stage_notifications
            self.stage_summary_notifications = self.routine_stage_notifications
            self.stage_open_notifications = self.routine_stage_notifications
            self.store.set_setting("routine_stage_notifications", "1" if self.routine_stage_notifications else "0")
            self.edit_or_send(event, self.settings_text(), self.settings_buttons())
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
            self.set_execution_paused(not self.paused)
            self.edit_or_send(
                event,
                "⏸ تم إيقاف توقيع وإرسال معاملات الـMint. الاكتشاف والمراقبة مستمران."
                if self.paused else
                "▶️ تم استئناف توقيع وإرسال معاملات الـMint.",
                self.menu_buttons(),
            )
            return

        # ----- V4.9 paid-public planner -----
        if data == "paid_add":
            self._clear_pending(chat_id)
            self.pending_link_action[chat_id] = {"expiry": time.time() + 300, "action": "paid"}
            self.telegram.send(
                chat_id,
                "💳 إضافة رابط منت مدفوع\n\nأرسل رابط المنت. سأقرأ السعر وموعد الـPublic، ثم "
                "أعرض المحافظ النشطة لتحدد المحافظ والكمية لكل واحدة. لن يتم أي شراء قبل تأكيدك النهائي.\n\n"
                "أرسل /cancel للإلغاء.",
            )
            return

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
                "🚫 تم رفض شراء Public Mint المدفوع\n\n"
                f"📦 المشروع: {candidate.slug}\n"
                "⚙️ ستستمر مراقبة بقية المراحل، وأي Public مجاني سيبقى تلقائيًا لجميع المحافظ النشطة.\n\n"
                f"{self.mint_link_block(candidate)}",
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

        if data.startswith("pgo:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            self.set_candidate_gas_policy(candidate, ignore=not candidate.ignore_gas_cap, override_usd=None)
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
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
                "✅ تم تأكيد خطة الشراء\n\n"
                f"📦 المشروع: {candidate.slug}\n"
                f"⏰ وقت الفتح: {format_ts(candidate.paid_stage_start, self.display_tz)}\n\n"
                + "👛 المحافظ والكميات:\n" + "\n".join(names)
                + "\n\n⚙️ سيبدأ الاستعداد قبل الفتح مباشرة، مع تطبيق سقف الغاز والسعر قبل التوقيع.\n\n"
                + self.mint_link_block(candidate),
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
                "🚫 تم إلغاء شراء Public Mint المدفوع\n\n"
                f"📦 المشروع: {candidate.slug}\n"
                "⚙️ المراقبة ستستمر لبقية المراحل وأي Public مجاني لاحق.\n\n"
                f"{self.mint_link_block(candidate)}",
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

        pending_gas = self.pending_gas_setting.get(chat_id)
        if pending_gas:
            if time.time() > float(pending_gas.get("expiry", 0)):
                self.pending_gas_setting.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة تعديل حد الغاز.", self.settings_buttons())
                return
            raw = text.replace("$", "").strip()
            try:
                value = Decimal(raw)
            except (InvalidOperation, ValueError):
                self.telegram.send(chat_id, "⚠️ أرسل رقمًا صحيحًا مثل 0.08 أو 0.25، أو /cancel.")
                return
            if value < 0 or value > Decimal("1000"):
                self.telegram.send(chat_id, "⚠️ الحد يجب أن يكون بين 0 و1000 دولار.")
                return
            target = str(pending_gas.get("target") or "global")
            if target == "project":
                candidate = self.candidate_by_token(str(pending_gas.get("candidate_token") or ""))
                if not candidate:
                    self.pending_gas_setting.pop(chat_id, None)
                    self.telegram.send(chat_id, "⚠️ لم يعد المشروع موجودًا في المراقبة.", self.settings_buttons())
                    return
                self.set_candidate_gas_policy(candidate, ignore=False, override_usd=value)
                self.pending_gas_setting.pop(chat_id, None)
                self.telegram.send(
                    chat_id,
                    f"✅ تم حفظ حد غاز خاص لـ {candidate.slug}: ${value}\n\n" + self.mint_link_block(candidate),
                    self.gas_project_policy_buttons(candidate),
                )
                return
            self.save_gas_cap(target, value)
            self.pending_gas_setting.pop(chat_id, None)
            self.telegram.send(chat_id, f"✅ تم حفظ حد الغاز لـ {target}: ${value}", self.settings_buttons())
            return

        pending_gas_project = self.pending_gas_project.get(chat_id)
        if pending_gas_project:
            if time.time() > float(pending_gas_project.get("expiry", 0)):
                self.pending_gas_project.pop(chat_id, None)
                self.telegram.send(chat_id, "انتهت مهلة إضافة استثناء الغاز.", self.settings_buttons())
                return
            self.pending_gas_project.pop(chat_id, None)
            ok, message, candidate = self.add_watch(text, persist=True)
            if not ok or not candidate:
                self.telegram.send(chat_id, "⚠️ " + message, self.settings_buttons())
                return
            self.telegram.send(
                chat_id,
                "🔥 إعداد غاز خاص بالمنت\n\n"
                f"📦 المشروع: {candidate.slug}\n🌐 الشبكة: {chain_label(candidate.chain)}\n"
                f"الحالي: {self.project_gas_policy_text(candidate)}\n\n"
                "اختر السياسة المطلوبة لهذا المنت فقط:\n"
                "• بدون حد: للفرص التي تريدها حتى مع رسوم مرتفعة.\n"
                "• حد خاص: رقم بالدولار لهذا المنت.\n"
                "• حد الشبكة: العودة للإعداد العام.\n\n"
                + self.mint_link_block(candidate),
                self.gas_project_policy_buttons(candidate),
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
            if action == "paid":
                ok, message, candidate = self.add_watch(text, persist=True)
                if not ok or not candidate:
                    self.telegram.send(chat_id, "⚠️ " + message, self.paid_watches_buttons())
                    return
                self.process_stage_schedule(candidate)
                self.maybe_offer_paid_public(candidate)
                plan = self.find_paid_public_plan(candidate)
                if plan or candidate.paid_detected or candidate.has_paid_stage:
                    candidate.paid_detected = True
                    candidate.paid_decision = candidate.paid_decision or "pending"
                    self.persist_paid_plan(candidate, candidate.paid_decision)
                    self.telegram.send(chat_id, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
                else:
                    self.telegram.send(
                        chat_id,
                        "⏳ لم أجد Public مدفوعًا مؤكدًا في البيانات الحالية، لكن تم حفظ الرابط للمراقبة. "
                        "إذا ظهر Public مدفوع سيظهر داخل قسم «💳 المنتات المدفوعة».\n\n" + self.mint_link_block(candidate),
                        self.paid_watches_buttons(),
                    )
                return
            if action == "watch":
                ok, message, candidate = self.add_watch(text, persist=True)
                self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.monitoring_menu_buttons())
                if ok and candidate:
                    self.process_stage_schedule(candidate)
                    current = self.current_plan_for_candidate(candidate)
                    if current and not (current.get("is_public") and not current.get("is_paid")):
                        result = self.stage_qualification_check(candidate, current, notify=False, force=True)
                        if result:
                            self.telegram.send(chat_id, result)
                    elif current and current.get("is_public") and not current.get("is_paid"):
                        self.telegram.send(
                            chat_id,
                            "🚀 الـPublic المجاني مفتوح الآن؛ تم إعطاء التنفيذ أولوية قصوى لكل المحافظ النشطة.\n\n"
                            + self.mint_link_block(candidate),
                        )
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
            self.set_execution_paused(True)
            self.telegram.send(chat_id, "⏸ تم إيقاف توقيع وإرسال جميع معاملات الـMint بما فيها Race Lane. المراقبة مستمرة.", self.menu_buttons())
            return
        if command == "/resume":
            self.set_execution_paused(False)
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
                if current and not (current.get("is_public") and not current.get("is_paid")):
                    result = self.stage_qualification_check(candidate, current, notify=False, force=True)
                    if result:
                        self.telegram.send(chat_id, result)
                elif current and current.get("is_public") and not current.get("is_paid"):
                    self.telegram.send(chat_id, "🚀 Public غير مدفوع/مجاني قيد التنفيذ الفوري لكل المحافظ النشطة.\n\n" + self.mint_link_block(candidate))
                self.maybe_offer_paid_public(candidate)
            return

        if "opensea.io" in text.lower() or re.fullmatch(r"[A-Za-z0-9._-]{2,200}", text):
            ok, message, candidate = self.add_watch(text, persist=True)
            self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.monitoring_menu_buttons())
            if ok and candidate:
                self.process_stage_schedule(candidate)
                current = self.current_plan_for_candidate(candidate)
                if current and not (current.get("is_public") and not current.get("is_paid")):
                    result = self.stage_qualification_check(candidate, current, notify=False, force=True)
                    if result:
                        self.telegram.send(chat_id, result)
                elif current and current.get("is_public") and not current.get("is_paid"):
                    self.telegram.send(chat_id, "🚀 Public غير مدفوع/مجاني قيد التنفيذ الفوري لكل المحافظ النشطة.\n\n" + self.mint_link_block(candidate))
                self.maybe_offer_paid_public(candidate)
            return

        self.telegram.send(chat_id, "أرسل رابط OpenSea أو استخدم أزرار القائمة الرئيسية.", self.menu_buttons())

    def _process_command_event(self, event: dict[str, Any]) -> None:
        try:
            if event.get("type") == "callback":
                self.handle_callback(event)
            else:
                self.handle_message(event)
        except Exception as exc:
            log.exception("Telegram command failed")
            self.telegram.send(event.get("chat_id", ""), f"⚠️ تعذر تنفيذ الأمر: {exc}")

    def command_worker_loop(self) -> None:
        """Process Telegram updates independently from mint/stage scanning."""
        log.info("Telegram command worker ready")
        while not STOP:
            try:
                event = self.command_queue.get(timeout=0.50)
            except queue.Empty:
                continue
            try:
                self._process_command_event(event)
            finally:
                try:
                    self.command_queue.task_done()
                except ValueError:
                    pass

    def start_command_worker(self) -> None:
        if self.command_worker_thread and self.command_worker_thread.is_alive():
            return
        self.command_worker_thread = threading.Thread(
            target=self.command_worker_loop, name="telegram-command-worker", daemon=True
        )
        self.command_worker_thread.start()

    def drain_commands(self) -> None:
        # Kept only for compatibility with older code paths. The dedicated
        # worker is the sole consumer from V4.6 onward to avoid races/double handling.
        return

    def candidate_loop_priority(self, candidate: Candidate) -> tuple[int, float]:
        """Prioritize live/free/public work ahead of background stage bookkeeping."""
        now = time.time()
        if any(st.submitted and not st.confirmed for st in candidate.wallets.values()):
            return (0, 0.0)
        current = self.current_plan_for_candidate(candidate, now) if candidate.stage_plans else None
        if current and current.get("is_public") and not current.get("is_paid"):
            return (1, float(current.get("start") or 0))
        if current and current.get("is_free"):
            return (2, float(current.get("start") or 0))
        future = self.next_plan_for_candidate(candidate, now) if candidate.stage_plans else None
        if future and future.get("is_public") and not future.get("is_paid"):
            remaining = float(future.get("start") or (now + 999999)) - now
            if remaining <= self.public_preopen_window_seconds:
                return (3, remaining)
        if candidate.watch_kind == "auto_free":
            return (4, candidate.next_refresh)
        if current:
            return (5, float(current.get("start") or 0))
        return (6, float(candidate.next_stage_start or 9e18))

    def notify_all(self, text: str) -> None:
        if not self.telegram.enabled:
            return
        for chat_id in self.telegram.allowed_chat_ids:
            self.telegram.send(chat_id, text)

    # ---------- main loop ----------
    def run(self) -> None:
        start_health_server()
        log.info("Mint Guardian V4.11.3 starting")
        log.info("Chains: %s", ", ".join(self.enabled_chains))
        log.info("Wallets: %s | paid=%s | native gas cap=%s | USD gas cap=$%s | mint price cap=%s",
                 len(self.wallets), self.allow_paid_default, self.max_gas_native, self.max_gas_usd, self.max_mint_price_default)
        if self.telegram.enabled:
            self.start_command_worker()
            self.telegram.start()
        self.bootstrap_watches()
        # Start market snapshots before discovery. It runs independently and
        # keeps price/fee data hot for the race lane.
        self.start_race_market_warmer()
        self.start_race_scheduler()
        self.start_seadrop_log_discovery()
        self.start_auto_free_discovery()
        log.info(
            "Discovery priority ready | stream-fast=%s | upcoming=%.1fs | mint-events=%.1fs | catalog-backfill=%.1fs | REST concurrency=%s",
            self.auto_stream_fast_path, self.auto_upcoming_scan_seconds, self.auto_event_scan_seconds,
            self.auto_drop_scan_seconds, getattr(self.opensea, "rest_concurrency", "?"),
        )
        log.info(
            "Stage planner ready | qualification recheck=%.1fs | public preopen=%.1fs | public retry=%.2fs | high/unknown qty=%s",
            self.qualification_recheck_seconds, self.public_preopen_window_seconds,
            self.public_fast_retry_seconds, self.auto_stage_high_limit_quantity,
        )
        log.info(
            "RACE LANE V4.11.3 STABLE ready | enabled=%s | prewarm=%.2fs | scheduler=%.3fs | retry=%.3fs | staticGas=%s | signal/prep/launch=%s/%s/%s | SeaDrop-WSS=%s | fee-cache=%.2fs | race-gas=%s",
            self.race_enabled, self.race_prewarm_seconds, self.race_scheduler_tick,
            self.race_retry_seconds, self.race_static_gas_limit,
            self.race_stream_workers, self.race_prep_workers, self.race_launch_workers,
            self.seadrop_wss_enabled, self.race_fee_refresh_seconds, self.race_gas_strategy,
        )
        log.info(
            "Race signal coalescer ready | single-flight=True | stream-quiet=%.2fs | seadrop-quiet=%.2fs | unknown-quiet=%.2fs",
            self.race_signal_stream_quiet_seconds, self.race_signal_seadrop_quiet_seconds,
            self.race_signal_unknown_quiet_seconds,
        )
        log.info(
            "Protected Free Mint Shield ready | enabled=%s | rule=X-or-website | workers=%s | passTTL=%.0fs | rejectTTL=%.0fs",
            self.free_social_protection_enabled, self.social_trust_workers,
            self.social_trust_pass_ttl, self.social_trust_reject_ttl,
        )
        self.notify_all(
            "🟢 OpenSea Mint Guardian V4.11.3 يعمل الآن على Railway.\n"
            f"الاكتشاف التلقائي: {'مفعّل كل ' + format(self.auto_free_scan_seconds, 'g') + ' ثانية' if self.auto_free_enabled else 'متوقف'}.\n"
            f"OpenSea Stream: {'مفعّل' if self.auto_stream_enabled else 'متوقف'} | REST Mint Events: {'مفعّل' if self.auto_event_fallback_enabled else 'متوقف'}.\n"
            f"التأهيل/المراقبة: تعمل بصمت وتظهر تفاصيلها عند فتح الأقسام.\n"
            f"Public المجاني: تنفيذ مباشر لكل المحافظ، والاستعداد قبل الفتح بـ {self.public_preopen_window_seconds:g} ثوانٍ.\n"
            f"حماية Free Mint: {'مفعلة — التلقائي بدون تأهيل يحتاج X أو Website' if self.free_social_protection_enabled else 'متوقفة — جميع Free Mints مسموحة'}.\n"
            f"سياسة الكمية: الحد ≤{self.auto_stage_high_limit_threshold} كهدف؛ أعلى/غير محدود = {self.auto_stage_high_limit_quantity}.\n"
            "تمت استعادة المراقبات وخطط المدفوع والمحافظ النشطة بنجاح."
        )
        while not STOP:
            self.drain_auto_discovery()
            self.cleanup_auto_candidates()
            with self.candidates_lock:
                ordered = sorted(list(self.candidates.items()), key=lambda kv: self.candidate_loop_priority(kv[1]))
            for key, candidate in ordered:
                self.refresh_candidate(candidate)
                self.process_stage_schedule(candidate)
                self.try_candidate(candidate)
                self.check_receipts(candidate)
                if candidate.done:
                    pending = any(s.submitted and not s.confirmed for s in candidate.wallets.values())
                    if not pending:
                        with self.candidates_lock:
                            self.candidates.pop(key, None)
                # Pull in any live Stream mint that arrived while this candidate
                # was being processed; it will be first on the next sorted pass.
                self.drain_auto_discovery()
            time.sleep(0.02)
        log.info("Stopped")


if __name__ == "__main__":
    try:
        Bot().run()
    except Exception as exc:
        log.exception("Fatal error: %s", exc)
        sys.exit(1)

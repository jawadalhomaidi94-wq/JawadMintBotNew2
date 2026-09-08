from __future__ import annotations

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
from urllib.parse import parse_qs, unquote, urlparse
from zoneinfo import ZoneInfo

import requests
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
    default_rpcs,
    explorer_tx_url,
    mint_drop,
    native_symbol,
    normalize_chain,
    opensea_chain_name,
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
    labels: list[str] = []
    for key in ("label", "name", "stageName", "stage_name", "type", "kind"):
        if isinstance(stage.get(key), str):
            labels.append(str(stage[key]).lower())
    if any("public" in x for x in labels):
        return True
    allow_values = [stage.get("allowlist"), stage.get("allow_list"), stage.get("merkleRoot"), stage.get("merkle_root")]
    return not labels and not any(v not in (None, "", False, [], {}) for v in allow_values)


def max_per_wallet(stage: dict[str, Any]) -> int | None:
    for key in ("maxPerWallet", "max_per_wallet", "walletLimit", "wallet_limit"):
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
                if key_l in {"chain", "chain_name", "chainname", "blockchain", "network"} and isinstance(child, str):
                    chain = normalize_chain(child)
                    if chain in CHAIN_CONFIGS and chain not in found:
                        found.append(chain)
                walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value[:50]:
                walk(child, depth + 1)

    walk(payload)
    return found


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
            {"command": "status", "description": "عرض مشاريع الـMint تحت المراقبة"},
            {"command": "eligibility", "description": "فحص أهلية المحافظ"},
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
        self.paused = env_bool("START_PAUSED", False)

        self.wallets: list[WalletConfig] = []
        self.rpc_pools: dict[str, RpcPool] = {}
        self.candidates: dict[str, Candidate] = {}
        self.command_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.pending_wallet_name: dict[str, float] = {}
        self.pending_wallet_import: dict[str, dict[str, Any]] = {}
        self.pending_wallet_rename: dict[str, dict[str, Any]] = {}
        self.pending_wallet_quantity: dict[str, dict[str, Any]] = {}
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

    def sync_wallets_into_candidates(self) -> None:
        now = time.time()
        current = {w.address.lower(): w for w in self.wallets}
        for candidate in self.candidates.values():
            for address in list(candidate.wallets):
                if address not in current:
                    state = candidate.wallets[address]
                    # If a transaction was already submitted, keep the state only
                    # until its receipt is resolved. Disabling a wallet cannot undo
                    # a transaction that is already on-chain.
                    if state.submitted and not state.confirmed and not state.final:
                        continue
                    candidate.wallets.pop(address, None)
            for wallet in self.wallets:
                key = wallet.address.lower()
                if not wallet.supports_chain(candidate.chain):
                    continue
                qty = candidate.quantity_override or wallet.quantity or self.quantity_default
                if candidate.wallet_limit:
                    qty = min(qty, candidate.wallet_limit)
                if candidate.remaining_supply is not None and candidate.remaining_supply > 0:
                    qty = min(qty, candidate.remaining_supply)
                if candidate.auto_discovered and key not in candidate.wallets:
                    latest = self.store.latest_mint_status(candidate.slug, wallet.address)
                    if latest in {"submitted", "confirmed"}:
                        # Never auto-mint the same drop twice after a restart.
                        continue
                if key in candidate.wallets:
                    # Keep candidate state in sync after rename/quantity changes.
                    state = candidate.wallets[key]
                    state.wallet = wallet
                    if not state.submitted:
                        state.quantity = max(1, qty)
                    continue
                probe_at = max(now, ((candidate.next_stage_start or candidate.public_start or now) - self.preopen_probe_seconds))
                candidate.wallets[key] = WalletState(wallet=wallet, quantity=max(1, qty), next_attempt=probe_at)
                self.notify_all(
                    f"➕ تمت إضافة المحفظة «{wallet.name}» إلى مراقبة منت نشط\n"
                    f"العنوان: {short_address(wallet.address)}\n"
                    f"المشروع: {candidate.slug}\nالشبكة: {chain_label(candidate.chain)}\n"
                    "سيتم فحص أهليتها تلقائيًا، والمنت المجاني سيُنفذ لها تلقائيًا إذا كانت مؤهلة."
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
            f"كمية المنت الافتراضية: {self.quantity_default}\n"
            "ستدخل تلقائيًا في جميع المنتات المجانية التي تراقبها."
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
        lines, public_start, next_start, wallet_limit, has_paid_stage = self.summarize_stages(drop)
        if not lines:
            return False, f"لم تُرجع OpenSea أي مراحل Mint للمشروع {slug}.", None

        key = f"{chain}:{slug}"
        candidate = self.candidates.get(key)
        if candidate is None:
            candidate = Candidate(
                slug=slug, chain=chain, source=source, allow_paid=allow_paid,
                max_mint_price_native=max_mint_price_native, quantity_override=quantity_override,
                auto_discovered=auto_discovered,
                last_seen_auto=time.time() if auto_discovered else 0.0,
            )
            self.candidates[key] = candidate
        elif auto_discovered and not candidate.auto_discovered:
            # A manually watched project keeps its manual policy/source.
            candidate.last_seen_auto = time.time()
        else:
            candidate.source = source
            candidate.allow_paid = allow_paid
            candidate.max_mint_price_native = max_mint_price_native
            candidate.quantity_override = quantity_override
            if not auto_discovered:
                # Sending a link manually promotes a previously auto-discovered
                # candidate into a persistent/manual watch.
                candidate.auto_discovered = False
        if auto_discovered and candidate.auto_discovered:
            candidate.last_seen_auto = time.time()
        candidate.stage_lines = lines
        candidate.public_start = public_start
        candidate.next_stage_start = next_start
        # For auto-free we can safely use the active free stage limit. Manual
        # watches may have several stages with different limits, so do not cap
        # them using a limit from the wrong stage; buyer.py will probe OpenSea
        # and find the highest quantity actually accepted for that wallet.
        candidate.wallet_limit = active_free_wallet_limit(drop) if candidate.auto_discovered else None
        candidate.remaining_supply = remaining_supply(drop)
        candidate.has_paid_stage = has_paid_stage
        if not candidate.auto_discovered:
            candidate.paid_detected = candidate.paid_detected or has_paid_stage
        candidate.next_refresh = time.time() + self._refresh_interval(candidate)
        self.sync_wallets_into_candidates()

        open_for_probe = max(time.time(), ((next_start or public_start or time.time()) - self.preopen_probe_seconds))
        for state in candidate.wallets.values():
            if not state.submitted and not state.final:
                # Do not hammer OpenSea for hours before the published stage.
                # If the state is already due because a new wallet was added to an active stage, keep it due.
                if state.next_attempt <= time.time() and (next_start is None or next_start <= time.time() + self.preopen_probe_seconds):
                    state.next_attempt = time.time()
                else:
                    state.next_attempt = open_for_probe

        paid_text = "مسموح بعد اختيار المحافظ" if allow_paid else "مجاني فقط"
        cap_text = "بدون حد" if max_mint_price_native <= 0 else f"{max_mint_price_native} {native_symbol(chain)}"
        kind_text = "يحتوي مرحلة مدفوعة" if has_paid_stage else "لم تُكتشف مرحلة مدفوعة حاليًا"
        message = (
            f"🎯 بدأت مراقبة: {slug}\n"
            f"الشبكة: {chain_label(chain)}\n"
            f"موعد الـPublic: {format_ts(public_start, self.display_tz)}\n"
            f"المحافظ النشطة المناسبة للشبكة: {len(candidate.wallets)}\n"
            f"نوع المراحل: {kind_text}\n"
            f"المنت المدفوع: {paid_text}\n"
            f"حد سعر المنت: {cap_text}\n\n"
            + "\n".join(lines[:8])
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
        except Exception as exc:
            return False, f"فشل جلب معلومات {slug} من OpenSea: {exc}", None
        ok, message, candidate = self.register_drop(
            slug, drop, chain_hint, raw,
            allow_paid=self.allow_paid_default,
            max_mint_price_native=self.max_mint_price_default,
        )
        if ok and persist and candidate:
            self.store.upsert_watch(
                slug=slug, chain=candidate.chain, source=raw,
                allow_paid=candidate.allow_paid,
                max_mint_price_native=str(candidate.max_mint_price_native),
                quantity=candidate.quantity_override,
            )
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
                drop = self.opensea.get_drop(slug)
                ok, _message, candidate = self.register_drop(
                    slug, drop, str(row.get("chain") or "") or None, str(row.get("source") or slug),
                    allow_paid=bool(row.get("allow_paid", 1)),
                    max_mint_price_native=Decimal(str(row.get("max_mint_price_native") or "0")),
                    quantity_override=row.get("quantity"),
                )
                if ok and candidate:
                    candidate.paid_detected = bool(row.get("paid_detected", 0)) or candidate.has_paid_stage
                    candidate.paid_selection_confirmed = bool(row.get("paid_selection_confirmed", 0))
                    try:
                        saved = json.loads(str(row.get("paid_wallets_json") or "[]"))
                        candidate.paid_wallet_addresses = {str(a).lower() for a in saved if isinstance(a, str)}
                    except Exception:
                        candidate.paid_wallet_addresses = set()
            except Exception as exc:
                log.warning("Could not restore watch %s: %s", slug, exc)

    def refresh_candidate(self, candidate: Candidate) -> None:
        # Auto-free candidates are refreshed by the dedicated 15-second
        # discovery worker. Avoid extra 3-second GET /drops/{slug} calls.
        if candidate.auto_discovered:
            return
        if time.time() < candidate.next_refresh:
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

    # ---------- automatic free-mint discovery ----------
    def _auto_free_scan_once(self, deep: bool = False) -> None:
        if not self.auto_free_enabled:
            return
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
                is_free = has_active_free_stage(drop) and not (remain is not None and remain <= 0)
                hints = [c for c in extract_known_chains(drop) if c in self.enabled_chains]
                chain_hint = hints[0] if hints else hint
                return {"slug": slug, "drop": drop, "chain_hint": chain_hint, "active_free": is_free}
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
        log.info("Auto Free Mint enabled | scan every %.1fs | types=%s | limit=%s",
                 self.auto_free_scan_seconds, ",".join(self.auto_free_drop_types), self.auto_free_drop_limit)

    def drain_auto_discovery(self) -> None:
        while True:
            try:
                item = self.auto_discovery_queue.get_nowait()
            except queue.Empty:
                break
            slug = str(item["slug"])
            drop = item["drop"]
            hint = item.get("chain_hint")
            active_free = bool(item.get("active_free", True))
            # If this is already a manual watch, the manual candidate already
            # attempts any free stage automatically; do not change its policy.
            existing = next((c for c in self.candidates.values() if c.slug.lower() == slug.lower()), None)
            if existing and not existing.auto_discovered:
                existing.last_seen_auto = time.time()
                continue
            if not active_free:
                if existing and existing.auto_discovered:
                    has_pending = any(st.submitted and not st.confirmed and not st.final for st in existing.wallets.values())
                    if not has_pending:
                        self.candidates.pop(f"{existing.chain}:{existing.slug}", None)
                continue

            before = existing is not None
            ok, _message, candidate = self.register_drop(
                slug, drop, hint, "auto-free",
                allow_paid=False,
                max_mint_price_native=Decimal("0"),
                auto_discovered=True,
            )
            if not ok or not candidate:
                continue
            candidate.last_seen_auto = time.time()
            if not before and self.auto_free_notify_discovery:
                self.notify_all(
                    f"🆓 تم اكتشاف Free Mint تلقائيًا\n"
                    f"المشروع: {candidate.slug}\n"
                    f"الشبكة: {chain_label(candidate.chain)}\n"
                    f"المحافظ النشطة: {len(candidate.wallets)}\n"
                    f"الحد المكتشف/المحفظة: {candidate.wallet_limit or 'غير محدد'}\n"
                    f"المتبقي من المعروض: {candidate.remaining_supply if candidate.remaining_supply is not None else 'غير معروف'}\n"
                    "سيحاول البوت التنفيذ تلقائيًا ضمن حد رسوم الغاز المضبوط."
                )

    def cleanup_auto_candidates(self) -> None:
        now = time.time()
        for key, candidate in list(self.candidates.items()):
            if not candidate.auto_discovered:
                continue
            has_pending_receipt = any(s.submitted and not s.confirmed and not s.final for s in candidate.wallets.values())
            if has_pending_receipt:
                continue
            if candidate.last_seen_auto and now - candidate.last_seen_auto > self.auto_free_candidate_ttl:
                self.candidates.pop(key, None)

    # ---------- eligibility + mint execution ----------
    def eligibility_matrix(self, candidate: Candidate) -> str:
        active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
        states = [s for s in candidate.wallets.values() if s.wallet.address.lower() in active_addresses]
        if not states:
            return "لا توجد محافظ نشطة مناسبة لهذه الشبكة."
        pool = self.rpc_pools[candidate.chain]
        lines = [f"🧪 فحص الأهلية — {candidate.slug} ({chain_label(candidate.chain)})"]

        def one(state: WalletState):
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
                bal = "" if balance is None else f" | الرصيد={balance:.6f}"
                lines.append(
                    f"{icon} {state.wallet.name} {short_address(state.wallet.address)}: "
                    f"{eligibility_label(result.status)}{price}{bal}"
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
        if candidate.auto_discovered:
            return
        if candidate.paid_selection_confirmed or candidate.paid_selection_notified:
            return
        candidate.paid_selection_notified = True
        self.store.set_watch_paid_selection(
            candidate.slug,
            candidate.paid_wallet_addresses,
            confirmed=False,
            paid_detected=True,
        )
        for chat_id in self.telegram.allowed_chat_ids:
            self.telegram.send(chat_id, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))

    def try_candidate(self, candidate: Candidate) -> None:
        if self.paused:
            return
        now = time.time()
        due = [state for state in candidate.wallets.values() if not state.submitted and not state.final and state.next_attempt <= now]
        if not due:
            return
        pool = self.rpc_pools[candidate.chain]
        workers = min(self.max_parallel_wallets, len(due))

        def execute(state: WalletState):
            state.attempts += 1
            state.status = "attempting"
            paid_selected = (
                candidate.paid_selection_confirmed
                and state.wallet.address.lower() in candidate.paid_wallet_addresses
            )
            return state, mint_drop(
                rpc_pool=pool,
                wallet=state.wallet,
                opensea=self.opensea,
                slug=candidate.slug,
                quantity=state.quantity,
                gas_strategy=self.gas_strategy,
                gas_limit_buffer=self.gas_limit_buffer,
                max_gas_native=self.max_gas_for_chain(candidate.chain),
                allow_paid=candidate.allow_paid,
                max_mint_price_native=candidate.max_mint_price_native,
                max_total_native=self.max_total_native,
                allowed_targets=self.allowed_targets,
                paid_wallet_allowed=paid_selected,
                max_gas_usd=self.max_gas_usd_for_chain(candidate.chain),
                native_usd_price=self.native_usd_price_for_chain(candidate.chain),
            )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(execute, state) for state in due]
            for future in as_completed(futures):
                state, result = future.result()
                state.status = result.status
                state.last_detail = result.detail
                if result.ok:
                    state.submitted = True
                    if result.quantity_used:
                        state.quantity = result.quantity_used
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
                        quantity=state.quantity,
                        detail=result.detail,
                    )
                    url = explorer_tx_url(candidate.chain, result.tx_hash or "")
                    kind = "مجاني" if (result.mint_value_native or Decimal("0")) == 0 else "مدفوع"
                    self.notify_all(
                        f"🚀 تم إرسال معاملة Mint\n"
                        f"المشروع: {candidate.slug}\n"
                        f"النوع: {kind}\n"
                        f"الشبكة: {chain_label(candidate.chain)}\n"
                        f"المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                        f"الكمية: {state.quantity}\n"
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
                    if not candidate.paid_selection_confirmed:
                        self.notify_paid_selection(candidate)
                    continue

                if result.status == "not_mintable_yet":
                    future_open = candidate.next_stage_start or candidate.public_start
                    if future_open and future_open > time.time() + 2:
                        state.next_attempt = max(time.time() + self.open_retry_interval, future_open - self.preopen_probe_seconds)
                    else:
                        state.next_attempt = time.time() + self.open_retry_interval
                elif result.status == "precondition_failed":
                    state.eligibility = "not_eligible_now"
                    state.next_attempt = time.time() + self.eligibility_retry_seconds
                elif result.status == "rate_limited":
                    state.next_attempt = time.time() + self.rate_limit_retry_seconds
                elif result.status in {"gas_usd_too_high", "gas_price_unavailable"}:
                    # Cost-first mode: keep watching and retry quickly instead of
                    # overpaying. This is intentionally not a final failure.
                    state.next_attempt = time.time() + self.gas_over_budget_retry_seconds
                elif result.status in {"gas_too_high", "insufficient_balance", "mint_price_too_high", "total_spend_too_high"}:
                    state.next_attempt = time.time() + max(10, self.eligibility_retry_seconds)
                elif result.status in {"paid_not_allowed", "target_not_allowed"}:
                    state.next_attempt = time.time() + 30
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
                self.store.record_mint(
                    slug=candidate.slug,
                    chain=candidate.chain,
                    wallet_name=state.wallet.name,
                    wallet_address=state.wallet.address,
                    status="confirmed",
                    tx_hash=state.tx_hash,
                    mint_value_native=str(state.mint_value_native) if state.mint_value_native is not None else None,
                    quantity=state.quantity,
                )
                self.notify_all(
                    f"✅ تم تأكيد الـMint بنجاح\n"
                    f"المشروع: {candidate.slug}\n"
                    f"المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"الشبكة: {chain_label(candidate.chain)}\n"
                    f"TX: {state.tx_hash}\n{explorer_tx_url(candidate.chain, state.tx_hash)}"
                )
            else:
                state.final = True
                self.store.record_mint(
                    slug=candidate.slug,
                    chain=candidate.chain,
                    wallet_name=state.wallet.name,
                    wallet_address=state.wallet.address,
                    status="reverted",
                    tx_hash=state.tx_hash,
                )
                self.notify_all(
                    f"❌ فشلت معاملة الـMint على الشبكة\n"
                    f"المشروع: {candidate.slug}\n"
                    f"المحفظة: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"TX: {state.tx_hash}"
                )

    # ---------- Telegram UI ----------
    def menu_buttons(self) -> list[list[tuple[str, str]]]:
        return [
            [("➕ إضافة محفظة", "add_wallet"), ("👛 المحافظ", "wallets")],
            [("🎯 المراقبات", "watches"), ("🧪 فحص الأهلية", "eligibility_all")],
            [("💳 محافظ المنت المدفوع", "paid_watches"), ("📜 سجل العمليات", "history")],
            [("🌐 الشبكات", "chains"), ("⚙️ الإعدادات", "settings")],
            [("⏸ إيقاف التنفيذ" if not self.paused else "▶️ استئناف التنفيذ", "toggle_pause")],
        ]

    def send_menu(self, chat_id: str) -> None:
        active = len(self.store.list_wallets(enabled_only=True))
        total = len(self.store.list_wallets(enabled_only=False))
        self.telegram.send(
            chat_id,
            "🤖 OpenSea Mint Guardian V4.2\n\n"
            f"🆓 البحث التلقائي عن Free Mint: {'مفعّل' if self.auto_free_enabled else 'متوقف'} — كل {self.auto_free_scan_seconds:g} ثانية\n"
            "أرسل رابط OpenSea فقط عندما تريد مراقبة مشروع محدد وموعد فتح الـPublic.\n"
            "• المنتات المجانية المكتشفة: لجميع المحافظ النشطة تلقائيًا.\n"
            "• المنت المدفوع: لن يُنفذ إلا للمحافظ التي تختارها أنت.\n\n"
            f"المحافظ: {active} نشطة من أصل {total}\n"
            f"المراقبات الحالية: {len(self.candidates)}",
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
            f"كمية الـMint الافتراضية: {wallet.quantity}",
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
            return "🎯 لا توجد مشاريع تحت المراقبة حاليًا."
        lines = ["🎯 المراقبات النشطة"]
        for candidate in self.candidates.values():
            if candidate.paid_detected:
                if not candidate.paid_selection_confirmed:
                    paid = "⏳ المدفوع بانتظار اختيار المحافظ"
                elif candidate.paid_wallet_addresses:
                    paid = f"💳 المدفوع محدد لـ {len(candidate.paid_wallet_addresses)} محفظة"
                else:
                    paid = "🚫 تم اختيار تجاهل المراحل المدفوعة"
            else:
                paid = "🆓 لا توجد مرحلة مدفوعة مكتشفة"
            source_icon = "🤖🆓" if candidate.auto_discovered else "🎯"
            lines.append(
                f"\n{source_icon} {candidate.slug} | {chain_label(candidate.chain)}\n"
                f"Public: {format_ts(candidate.public_start, self.display_tz)}\n"
                f"تم الإرسال: {candidate.submitted_count()}/{len(candidate.wallets)} | "
                f"تم التأكيد: {candidate.confirmed_count()}\n"
                f"{paid}"
            )
        return "\n".join(lines)[:3900]

    def settings_text(self) -> str:
        return (
            "⚙️ إعدادات التشغيل الحالية\n"
            f"تنفيذ الـMint: {'⏸ متوقف' if self.paused else '▶️ يعمل'}\n"
            f"السماح بالمدفوع: {'نعم، بعد اختيار المحافظ' if self.allow_paid_default else 'لا'}\n"
            f"أقصى سعر Mint: {'بدون حد' if self.max_mint_price_default <= 0 else self.max_mint_price_default}\n"
            f"أقصى Gas: {'بدون حد' if self.max_gas_native <= 0 else self.max_gas_native}\n"
            f"أقصى إجمالي للعملية: {'بدون حد' if self.max_total_native <= 0 else self.max_total_native}\n"
            f"استراتيجية الغاز: {self.gas_strategy}\n"
            f"سقف الغاز بالدولار: ${self.max_gas_usd}\n"
            f"Auto Free Mint: {'مفعّل' if self.auto_free_enabled else 'متوقف'} — كل {self.auto_free_scan_seconds:g} ثانية\n"
            f"تنبيه اكتشاف كل Free Mint: {'مفعّل' if self.auto_free_notify_discovery else 'صامت حتى التنفيذ'}\n"
            f"المحافظ المتوازية: {self.max_parallel_wallets}\n"
            f"الاستعداد قبل الفتح: {self.preopen_probe_seconds} ثانية"
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

    def paid_selector_text(self, candidate: Candidate) -> str:
        active_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
        eligible_wallets = [s.wallet for s in candidate.wallets.values() if s.wallet.address.lower() in active_addresses]
        selected = candidate.paid_wallet_addresses
        names = [w.name for w in eligible_wallets if w.address.lower() in selected]
        selected_text = "، ".join(names) if names else "لا توجد محافظ محددة بعد"
        observed = [s.mint_value_native for s in candidate.wallets.values() if s.mint_value_native is not None and s.mint_value_native > 0]
        price = min(observed) if observed else None
        price_text = f"{price} {native_symbol(candidate.chain)}" if price is not None else "سيتم التحقق عند فتح المرحلة"
        return (
            f"💳 اختيار محافظ المنت المدفوع\n\n"
            f"المشروع: {candidate.slug}\n"
            f"الشبكة: {chain_label(candidate.chain)}\n"
            f"السعر المكتشف: {price_text}\n\n"
            "اختر المحافظ التي تسمح لها بشراء الـMint المدفوع.\n"
            "🆓 أي Mint مجاني سيبقى تلقائيًا لجميع المحافظ النشطة، بغض النظر عن هذا الاختيار.\n\n"
            f"المحدد حاليًا: {selected_text}"
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
            selected = wallet.address.lower() in candidate.paid_wallet_addresses
            rows.append([(("✅ " if selected else "⬜ ") + wallet.name, f"pwm:{token}:{stored.id}")])
            shown += 1
            if shown >= 25:
                break
        rows.append([("✅ تحديد الكل", f"pwa:{token}"), ("🧹 إلغاء التحديد", f"pac:{token}")])
        rows.append([("🚀 تأكيد الاختيار", f"pwc:{token}")])
        rows.append([("🚫 تجاهل المدفوع فقط", f"pwn:{token}")])
        return rows

    def paid_watches_buttons(self) -> list[list[tuple[str, str]]]:
        rows: list[list[tuple[str, str]]] = []
        for candidate in self.candidates.values():
            if candidate.paid_detected or candidate.has_paid_stage:
                rows.append([(f"💳 {candidate.slug}", f"pwo:{self.candidate_token(candidate)}")])
        rows.append([("↩️ القائمة الرئيسية", "menu")])
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

        if data == "watches":
            self.edit_or_send(event, self.status_text(), self.menu_buttons())
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
            self.telegram.send(chat_id, "🧪 جارٍ فحص أهلية جميع المحافظ النشطة للمراقبات الحالية...")
            text = self.all_eligibility_text()
            self.telegram.send(chat_id, text, self.menu_buttons())
            for candidate in self.candidates.values():
                if candidate.paid_detected and not candidate.paid_selection_confirmed:
                    self.notify_paid_selection(candidate)
            return
        if data == "toggle_pause":
            self.paused = not self.paused
            self.edit_or_send(
                event,
                "⏸ تم إيقاف توقيع وإرسال معاملات الـMint. المراقبة مستمرة."
                if self.paused else
                "▶️ تم استئناف توقيع وإرسال معاملات الـMint.",
                self.menu_buttons(),
            )
            return

        if data == "paid_watches":
            paid = [c for c in self.candidates.values() if c.paid_detected or c.has_paid_stage]
            text = "💳 اختر المشروع لتحديد محافظ المنت المدفوع." if paid else "لا توجد مراحل مدفوعة مكتشفة في المراقبات الحالية."
            self.edit_or_send(event, text, self.paid_watches_buttons())
            return

        if data.startswith("pwo:"):
            token = data.split(":", 1)[1]
            candidate = self.candidate_by_token(token)
            if not candidate:
                self.telegram.send(chat_id, "⚠️ لم تعد هذه المراقبة موجودة.")
                return
            candidate.paid_selection_confirmed = False
            candidate.paid_selection_notified = True
            self.store.set_watch_paid_selection(
                candidate.slug, candidate.paid_wallet_addresses, confirmed=False, paid_detected=True
            )
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
                self.telegram.answer_callback(event.get("callback_id", ""), "المحفظة غير نشطة أو غير مناسبة لهذه الشبكة")
                return
            address = wallet.address.lower()
            if address in candidate.paid_wallet_addresses:
                candidate.paid_wallet_addresses.remove(address)
            else:
                candidate.paid_wallet_addresses.add(address)
            candidate.paid_selection_confirmed = False
            candidate.paid_selection_notified = True
            self.store.set_watch_paid_selection(
                candidate.slug, candidate.paid_wallet_addresses, confirmed=False, paid_detected=True
            )
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
            candidate.paid_selection_confirmed = False
            self.store.set_watch_paid_selection(candidate.slug, candidate.paid_wallet_addresses, confirmed=False, paid_detected=True)
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
            return

        if data.startswith("pac:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            candidate.paid_wallet_addresses.clear()
            candidate.paid_selection_confirmed = False
            self.store.set_watch_paid_selection(candidate.slug, set(), confirmed=False, paid_detected=True)
            self.edit_or_send(event, self.paid_selector_text(candidate), self.paid_selector_buttons(candidate))
            return

        if data.startswith("pwc:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            if not candidate.paid_wallet_addresses:
                self.telegram.send(chat_id, "⚠️ لم تحدد أي محفظة. اختر محفظة واحدة على الأقل، أو استخدم «🚫 تجاهل المدفوع فقط».")
                return
            enabled_addresses = {w.address.lower() for w in self.store.list_wallets(enabled_only=True)}
            active_addresses = {
                state.wallet.address.lower() for state in candidate.wallets.values()
                if state.wallet.address.lower() in enabled_addresses
            }
            candidate.paid_wallet_addresses &= active_addresses
            if not candidate.paid_wallet_addresses:
                self.telegram.send(chat_id, "⚠️ المحافظ المحددة لم تعد نشطة. اختر من جديد.")
                return
            candidate.paid_selection_confirmed = True
            candidate.paid_detected = True
            candidate.paid_selection_notified = True
            self.store.set_watch_paid_selection(candidate.slug, candidate.paid_wallet_addresses, confirmed=True, paid_detected=True)
            now = time.time()
            for state in candidate.wallets.values():
                if state.wallet.address.lower() in candidate.paid_wallet_addresses and not state.submitted:
                    state.next_attempt = now
            names = [state.wallet.name for state in candidate.wallets.values() if state.wallet.address.lower() in candidate.paid_wallet_addresses]
            self.edit_or_send(
                event,
                f"✅ تم اعتماد محافظ المنت المدفوع للمشروع {candidate.slug}:\n" + "، ".join(names) +
                "\n\nسيستمر المنت المجاني لجميع المحافظ النشطة.",
                self.menu_buttons(),
            )
            return

        if data.startswith("pwn:"):
            candidate = self.candidate_by_token(data.split(":", 1)[1])
            if not candidate:
                return
            candidate.paid_wallet_addresses.clear()
            candidate.paid_selection_confirmed = True
            candidate.paid_detected = True
            candidate.paid_selection_notified = True
            self.store.set_watch_paid_selection(candidate.slug, set(), confirmed=True, paid_detected=True)
            self.edit_or_send(
                event,
                f"🚫 لن يتم شراء أي Mint مدفوع للمشروع {candidate.slug}.\n"
                "ستبقى المراقبة فعالة، وإذا ظهر Mint مجاني فسيتم تنفيذه لجميع المحافظ النشطة.",
                self.menu_buttons(),
            )
            return

    def handle_message(self, event: dict[str, Any]) -> None:
        chat_id = event["chat_id"]
        text = event.get("text", "").strip()

        if text.lower() == "/cancel":
            self._clear_pending(chat_id)
            self.telegram.send(chat_id, "تم إلغاء العملية الحالية.", self.menu_buttons())
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
        command = parts[0].lower() if parts else ""
        if command in {"/start", "/help", "/menu"} or text in {"القائمة", "الرئيسية"}:
            self.send_menu(chat_id)
            return
        if command == "/status" or text == "المراقبات":
            self.telegram.send(chat_id, self.status_text(), self.menu_buttons())
            return
        if command == "/wallets" or text == "المحافظ":
            self.telegram.send(chat_id, self.wallets_text(), self.wallet_list_buttons())
            return
        if command == "/chains" or text == "الشبكات":
            self.telegram.send(chat_id, self.chains_text(), self.menu_buttons())
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
            self.telegram.send(chat_id, self.all_eligibility_text(), self.menu_buttons())
            for candidate in self.candidates.values():
                if candidate.paid_detected and not candidate.paid_selection_confirmed:
                    self.notify_paid_selection(candidate)
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
                self.telegram.send(chat_id, "الاستخدام: /watch [الشبكة] <رابط OpenSea أو اسم المجموعة>")
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
                eligibility = self.eligibility_matrix(candidate)
                self.telegram.send(chat_id, eligibility)
                if candidate.paid_detected and not candidate.paid_selection_confirmed:
                    candidate.paid_selection_notified = False
                    self.notify_paid_selection(candidate)
            return

        if "opensea.io" in text.lower() or re.fullmatch(r"[A-Za-z0-9._-]{2,200}", text):
            ok, message, candidate = self.add_watch(text, persist=True)
            self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.menu_buttons())
            if ok and candidate:
                eligibility = self.eligibility_matrix(candidate)
                self.telegram.send(chat_id, eligibility)
                if candidate.paid_detected and not candidate.paid_selection_confirmed:
                    candidate.paid_selection_notified = False
                    self.notify_paid_selection(candidate)
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
        log.info("Mint Guardian V4.2 starting")
        log.info("Chains: %s", ", ".join(self.enabled_chains))
        log.info("Wallets: %s | paid=%s | native gas cap=%s | USD gas cap=$%s | mint price cap=%s",
                 len(self.wallets), self.allow_paid_default, self.max_gas_native, self.max_gas_usd, self.max_mint_price_default)
        if self.telegram.enabled:
            self.telegram.start()
        self.bootstrap_watches()
        self.start_auto_free_discovery()
        self.notify_all(
            "🟢 OpenSea Mint Guardian V4.2 يعمل الآن على Railway.\n"
            f"البحث التلقائي عن الـFree Mint: {'مفعّل كل ' + format(self.auto_free_scan_seconds, 'g') + ' ثانية' if self.auto_free_enabled else 'متوقف'}.\n"
            "تمت استعادة المراقبات والمحافظ النشطة بنجاح."
        )
        for candidate in self.candidates.values():
            if candidate.paid_detected and not candidate.paid_selection_confirmed:
                self.notify_paid_selection(candidate)

        while not STOP:
            self.drain_commands()
            self.drain_auto_discovery()
            self.cleanup_auto_candidates()
            for candidate in list(self.candidates.values()):
                self.refresh_candidate(candidate)
                self.try_candidate(candidate)
                self.check_receipts(candidate)
            time.sleep(0.12)
        log.info("Stopped")


if __name__ == "__main__":
    try:
        Bot().run()
    except Exception as exc:
        log.exception("Fatal error: %s", exc)
        sys.exit(1)

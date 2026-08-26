"""
Advanced Telegram controller for the existing mint bot.

This file intentionally leaves the old main.py and buyer.py functions intact.
It imports and reuses them, then adds:
  - encrypted multi-wallet support
  - Telegram inline buttons
  - social-link gate before minting
  - lower-gas execution windows
  - manual mint watch requests
  - allowlist/public-stage mint attempts per active wallet
  - portfolio/floor-price scan helpers
"""

import asyncio
import json
import logging
import os
import re
import time
import hashlib
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
import websockets
from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

from buyer import (
    GAS_LIMIT_SAFETY_MARGIN,
    LIMITED_BUY_QTY,
    decide_quantity,
    get_web3,
    get_onchain_public_price_wei,
)
from state_store import JsonState
from wallet_store import WalletRecord, WalletStore, short_address


load_dotenv()

log = logging.getLogger("advanced-bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"
WALLET_PASSWORD = os.environ.get("ADVANCED_WALLET_PASSWORD", "")
STATE_DIR = Path(os.environ.get("ADVANCED_STATE_DIR", "."))
WALLET_STORE_PATH = STATE_DIR / "wallets.json"
RUNTIME_STATE_PATH = STATE_DIR / "advanced_state.json"

AUTO_BUY_ENABLED = os.environ.get("ADVANCED_AUTO_BUY", "true").lower() == "true"
AUTO_BUY_PAID_DROPS = os.environ.get("ADVANCED_AUTO_BUY_PAID_DROPS", "false").lower() == "true"
FREE_PRICE_THRESHOLD_USD = float(os.environ.get("ADVANCED_FREE_PRICE_THRESHOLD_USD", "0.01"))
GAS_LOW_FACTOR = float(os.environ.get("ADVANCED_GAS_LOW_FACTOR", "0.80"))
MANUAL_DEADLINE_URGENCY_SECONDS = int(os.environ.get("ADVANCED_DEADLINE_URGENCY_SECONDS", "90"))
WATCH_POLL_INTERVAL_SECONDS = float(os.environ.get("ADVANCED_WATCH_POLL_SECONDS", "3"))
PORTFOLIO_SCAN_INTERVAL_SECONDS = int(os.environ.get("ADVANCED_PORTFOLIO_SCAN_SECONDS", "180"))
OPENSEA_SCOPED_TOKEN = os.environ.get("OPENSEA_SCOPED_TOKEN", "")
OPENSEA_LISTING_ENABLED = os.environ.get("OPENSEA_LISTING_ENABLED", "false").lower() == "true"
DROPS_DISCOVERY_INTERVAL_SECONDS = int(os.environ.get("ADVANCED_DROPS_DISCOVERY_SECONDS", "45"))
HEARTBEAT_STATUS_INTERVAL_SECONDS = int(os.environ.get("ADVANCED_HEARTBEAT_STATUS_SECONDS", "3600"))
DROP_DETAIL_CACHE_SECONDS = float(os.environ.get("ADVANCED_DROP_DETAIL_CACHE_SECONDS", "2"))
FLOOR_CACHE_SECONDS = float(os.environ.get("ADVANCED_FLOOR_CACHE_SECONDS", "300"))
PORT = int(os.environ.get("PORT", "8000"))
ENABLE_HEALTH_SERVER = os.environ.get("ADVANCED_ENABLE_HEALTH_SERVER", "true").lower() == "true"

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))
HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": float(os.environ.get("ADVANCED_MAX_GAS_USD_ROBINHOOD", "0.05")),
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": float(os.environ.get("ADVANCED_MAX_GAS_USD_ETHEREUM", "0.50")),
    },
}
W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

ALLOWED_CHAT_ID = str(TELEGRAM_CHAT_ID)

wallet_store = WalletStore(WALLET_STORE_PATH)
runtime_state = JsonState(RUNTIME_STATE_PATH, {"bought_keys": []})
runtime_settings = runtime_state.data.setdefault("settings", {})
if "auto_buy_enabled" not in runtime_settings:
    runtime_settings["auto_buy_enabled"] = AUTO_BUY_ENABLED
    runtime_state.save()
telegram_send_queue: "asyncio.Queue[tuple[str, dict | None]]" = asyncio.Queue()
telegram_state: dict[str, dict[str, Any]] = {}
manual_watchlist: dict[str, dict[str, Any]] = {}
auto_watchlist: dict[str, dict[str, Any]] = {}
in_flight: set[str] = set()
bought_keys: set[str] = runtime_state.set_values("bought_keys")
sale_proposals: dict[str, dict[str, Any]] = {}
watch_callback_keys: dict[str, str] = {}
wallet_tx_locks: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)
_eth_price_cache = {"value": None, "ts": 0}


class TTLCache:
    def __init__(self, ttl_seconds: float):
        self.ttl_seconds = ttl_seconds
        self.items: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        entry = self.items.get(key)
        if not entry:
            return None
        ts, value = entry
        if time.time() - ts > self.ttl_seconds:
            self.items.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self.items[key] = (time.time(), value)


drop_detail_cache = TTLCache(DROP_DETAIL_CACHE_SECONDS)
floor_cache = TTLCache(FLOOR_CACHE_SECONDS)


def auto_buy_enabled() -> bool:
    return bool(runtime_settings.get("auto_buy_enabled", AUTO_BUY_ENABLED))


def set_auto_buy_enabled(enabled: bool) -> None:
    runtime_settings["auto_buy_enabled"] = enabled
    runtime_state.save()


def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = float(response.json()["ethereum"]["usd"])
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as exc:
        log.warning("ETH price fallback after error: %s", exc)
        return _eth_price_cache["value"] or 3000.0


def fetch_drop_detail(slug: str) -> tuple[bool | None, dict[str, Any] | None]:
    cached = drop_detail_cache.get(slug)
    if cached is not None:
        return cached
    try:
        response = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if response.status_code == 200:
            result = (True, response.json())
            drop_detail_cache.set(slug, result)
            return result
        if response.status_code == 404:
            result = (False, None)
            drop_detail_cache.set(slug, result)
            return result
        result = (None, None)
        drop_detail_cache.set(slug, result)
        return result
    except Exception as exc:
        log.warning("drop detail error for %s: %s", slug, exc)
        return None, None


def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def stage_has_ended(stage: dict[str, Any]) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end


@dataclass
class GasSample:
    ema_usd: float | None = None
    best_usd: float | None = None
    last_usd: float | None = None
    updated_at: float = 0


class GasOptimizer:
    def __init__(self) -> None:
        self.samples: dict[str, GasSample] = {chain: GasSample() for chain in CHAIN_CONFIGS}

    def update(self, chain_key: str, gas_fee_usd: float) -> GasSample:
        sample = self.samples.setdefault(chain_key, GasSample())
        sample.last_usd = gas_fee_usd
        sample.best_usd = gas_fee_usd if sample.best_usd is None else min(sample.best_usd, gas_fee_usd)
        sample.ema_usd = gas_fee_usd if sample.ema_usd is None else (sample.ema_usd * 0.75) + (gas_fee_usd * 0.25)
        sample.updated_at = time.time()
        return sample

    def should_execute(
        self,
        chain_key: str,
        gas_fee_usd: float,
        max_gas_fee_usd: float,
        deadline_ts: float | None = None,
    ) -> tuple[bool, str]:
        previous_sample = self.samples.setdefault(chain_key, GasSample())
        had_history = previous_sample.ema_usd is not None
        sample = self.update(chain_key, gas_fee_usd)
        if gas_fee_usd > max_gas_fee_usd:
            return False, f"الغاز ${gas_fee_usd:.4f} أعلى من الحد ${max_gas_fee_usd:.4f}"

        now = time.time()
        if deadline_ts and deadline_ts - now <= MANUAL_DEADLINE_URGENCY_SECONDS:
            return True, "قرب انتهاء/فتح المرحلة، تم قبول الغاز ضمن الحد"

        if not had_history:
            if gas_fee_usd <= max_gas_fee_usd * GAS_LOW_FACTOR:
                return True, "أول قراءة منخفضة بما يكفي مقارنة بحد الغاز"
            return False, f"نجمع عينات غاز أكثر، الحالي ${gas_fee_usd:.4f}"

        if sample.best_usd is not None and gas_fee_usd <= sample.best_usd * 1.08:
            return True, "الغاز قريب من أقل قراءة مسجلة"

        if sample.ema_usd is not None and gas_fee_usd <= sample.ema_usd * GAS_LOW_FACTOR:
            return True, "الغاز أقل من المتوسط المتحرك"

        return False, f"ننتظر غاز أقل، الحالي ${gas_fee_usd:.4f}"


gas_optimizer = GasOptimizer()


def telegram_api(method: str, data: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(f"{TELEGRAM_API}/{method}", data=data, timeout=15)
    try:
        return response.json()
    except Exception:
        return {"ok": False, "description": response.text}


def enqueue_telegram(text: str, reply_markup: dict | None = None) -> None:
    telegram_send_queue.put_nowait((text, reply_markup))


async def telegram_sender() -> None:
    while True:
        text, reply_markup = await telegram_send_queue.get()
        data: dict[str, Any] = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
        await asyncio.to_thread(telegram_api, "sendMessage", data)
        telegram_send_queue.task_done()
        await asyncio.sleep(0.35)


def answer_callback(callback_query_id: str, text: str = "") -> None:
    telegram_api("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})


def delete_message(chat_id: str | int, message_id: int) -> None:
    telegram_api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})


def main_keyboard() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "👛 المحافظ", "callback_data": "wallets"},
                {"text": "🎯 مراقبة مينت", "callback_data": "manual_mint"},
            ],
            [
                {"text": "🖼 فحص المحافظ", "callback_data": "scan_portfolio"},
                {"text": "👀 قائمة المراقبة", "callback_data": "watchlist"},
            ],
            [
                {
                    "text": "⏸ إيقاف الالتقاط" if auto_buy_enabled() else "▶️ تشغيل الالتقاط",
                    "callback_data": "toggle_auto_buy",
                },
                {"text": "⚙️ الحالة", "callback_data": "status"},
            ],
        ]
    }


def wallets_keyboard() -> dict[str, Any]:
    rows = [[{"text": "➕ إضافة محفظة", "callback_data": "add_wallet"}]]
    for wallet in wallet_store.all_wallets():
        state = "🟢" if wallet.active else "🔴"
        rows.append([
            {
                "text": f"{state} {wallet.label} {short_address(wallet.address)}",
                "callback_data": f"toggle_wallet:{wallet.address}",
            }
        ])
    rows.append([{"text": "⬅️ رجوع", "callback_data": "menu"}])
    return {"inline_keyboard": rows}


def watchlist_keyboard() -> dict[str, Any]:
    rows = []
    for watch_key in sorted(set(auto_watchlist) | set(manual_watchlist)):
        entry = auto_watchlist.get(watch_key) or manual_watchlist.get(watch_key) or {}
        label = f"{entry.get('chain_key', '?')}:{entry.get('slug', watch_key)}"
        callback_id = hashlib.sha1(watch_key.encode("utf-8")).hexdigest()[:16]
        watch_callback_keys[callback_id] = watch_key
        rows.append([{"text": f"إلغاء {label}", "callback_data": f"drop_watch:{callback_id}"}])
    rows.append([{"text": "⬅️ رجوع", "callback_data": "menu"}])
    return {"inline_keyboard": rows}


def sale_keyboard(proposals: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for proposal in proposals[:8]:
        rows.append([
            {
                "text": f"عرض {proposal['name'][:22]}",
                "callback_data": f"sell_prompt:{proposal['id']}",
            }
        ])
    rows.append([{"text": "⬅️ رجوع", "callback_data": "menu"}])
    return {"inline_keyboard": rows}


def only_allowed_chat(update: dict[str, Any]) -> bool:
    message = update.get("message") or update.get("callback_query", {}).get("message") or {}
    chat_id = str((message.get("chat") or {}).get("id", ""))
    return chat_id == ALLOWED_CHAT_ID


def extract_slug(text: str) -> str | None:
    text = text.strip()
    if not text:
        return None

    if "opensea.io" in text:
        parsed = urlparse(text.split()[0])
        parts = [part for part in parsed.path.split("/") if part]
        if "collection" in parts:
            idx = parts.index("collection")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        if parts:
            return parts[-1]

    match = re.search(r"([a-zA-Z0-9][a-zA-Z0-9_-]{1,120})", text)
    return match.group(1) if match else None


def extract_quantity(text: str, default: int = 1) -> int:
    numbers = [int(item) for item in re.findall(r"\b([1-9][0-9]?)\b", text)]
    if not numbers:
        return default
    return max(1, min(numbers[-1], 100))


def flatten_values(value: Any) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            for child_key, child_value in flatten_values(child):
                results.append((f"{key}.{child_key}" if child_key else key, child_value))
    elif isinstance(value, list):
        for item in value:
            results.extend(flatten_values(item))
    elif isinstance(value, str):
        results.append(("", value.strip()))
    return results


def collection_has_site_or_x(detail: dict[str, Any]) -> tuple[bool, str]:
    site_keys = ("website", "external_url", "project_url", "homepage", "site_url", "url")
    x_keys = ("twitter", "twitter_username", "twitter_url", "x_url", "x")
    found_site = None
    found_x = None

    for key, value in flatten_values(detail):
        key_lower = key.lower()
        value_lower = value.lower()
        if not value:
            continue
        if any(item in key_lower for item in site_keys) and "opensea.io" not in value_lower:
            found_site = found_site or value
        if any(item in key_lower for item in x_keys) or "twitter.com/" in value_lower or "x.com/" in value_lower:
            found_x = found_x or value

    if found_site and found_x:
        return True, f"موقع + X ({found_site}, {found_x})"
    if found_site:
        return True, f"موقع ({found_site})"
    if found_x:
        return True, f"X ({found_x})"
    return False, "لا يوجد موقع أو X في بيانات المجموعة"


def started_today_or_future(stage: dict[str, Any]) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    local_start = start.astimezone(LOCAL_TZ).date()
    return local_start >= time_now_local_date()


def time_now_local_date():
    from datetime import datetime

    return datetime.now(LOCAL_TZ).date()


def stage_deadline_ts(detail: dict[str, Any]) -> float | None:
    stage = detail.get("active_stage") or detail.get("next_stage") or {}
    end = parse_iso(stage.get("end_time", ""))
    start = parse_iso(stage.get("start_time", ""))
    target = end or start
    return target.timestamp() if target else None


def remaining_supply(detail: dict[str, Any]) -> int:
    try:
        return max(0, int(detail.get("max_supply") or 0) - int(detail.get("total_supply") or 0))
    except Exception:
        return 0


def price_is_free_or_allowed(price_wei: int, eth_price_usd: float, allow_paid: bool) -> bool:
    if allow_paid:
        return True
    return (price_wei / 1e18) * eth_price_usd < FREE_PRICE_THRESHOLD_USD


def build_drop_mint_transaction(slug: str, minter: str, quantity: int) -> tuple[bool, dict[str, Any]]:
    try:
        response = requests.post(
            f"{DROPS_API_BASE}/{slug}/mint",
            headers={"x-api-key": OPENSEA_API_KEY, "content-type": "application/json"},
            json={"minter": minter, "quantity": quantity},
            timeout=8,
        )
        if response.status_code == 200:
            return True, response.json()
        try:
            payload = response.json()
        except Exception:
            payload = {"error": response.text}
        return False, {"status_code": response.status_code, "payload": payload}
    except Exception as exc:
        return False, {"error": str(exc)}


def extract_tx_fields(payload: dict[str, Any]) -> dict[str, Any] | None:
    tx = payload.get("transaction") if isinstance(payload.get("transaction"), dict) else payload
    to_address = tx.get("to") or tx.get("target")
    data = tx.get("data") or tx.get("calldata") or tx.get("input")
    value = tx.get("value", 0)
    if not to_address or not data:
        return None
    if isinstance(value, str):
        value = int(value, 16) if value.startswith("0x") else int(value)
    return {"to": Web3.to_checksum_address(to_address), "data": data, "value": int(value)}


def sign_and_send_mint_tx(
    chain_key: str,
    wallet: WalletRecord,
    private_key: str,
    tx_fields: dict[str, Any],
    max_gas_fee_usd: float,
    deadline_ts: float | None,
) -> dict[str, Any]:
    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()
    sender = Web3.to_checksum_address(wallet.address)

    with wallet_tx_locks[sender.lower()]:
        tx = {
            "from": sender,
            "to": tx_fields["to"],
            "data": tx_fields["data"],
            "value": tx_fields["value"],
            "nonce": w3.eth.get_transaction_count(sender, "pending"),
            "chainId": w3.eth.chain_id,
            "gasPrice": w3.eth.gas_price,
        }

        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as exc:
            return {"success": False, "reason": "simulation_failed", "error": str(exc)}

        gas_fee_usd = (tx["gas"] * tx["gasPrice"] / 1e18) * eth_price_usd
        should_execute, gas_reason = gas_optimizer.should_execute(
            chain_key, gas_fee_usd, max_gas_fee_usd, deadline_ts
        )
        if not should_execute:
            return {"success": False, "reason": "waiting_for_lower_gas", "gas_fee_usd": gas_fee_usd, "details": gas_reason}

        total_cost_wei = tx_fields["value"] + (tx["gas"] * tx["gasPrice"])
        if w3.eth.get_balance(sender) < total_cost_wei:
            return {"success": False, "reason": "insufficient_funds_for_total_cost"}

        signed = Account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        return {"success": True, "tx_hash": tx_hash.hex(), "gas_fee_usd": gas_fee_usd}


async def attempt_drop_for_wallet(
    slug: str,
    chain_key: str,
    wallet: WalletRecord,
    quantity: int,
    allow_paid: bool,
    max_gas_fee_usd: float,
    deadline_ts: float | None,
) -> dict[str, Any]:
    if not WALLET_PASSWORD:
        return {"success": False, "reason": "missing_wallet_password"}

    ok, detail = await asyncio.to_thread(fetch_drop_detail, slug)
    if not ok or not detail:
        return {"success": False, "reason": "drop_not_found"}

    social_ok, social_reason = collection_has_site_or_x(detail)
    if not social_ok:
        return {"success": False, "reason": "missing_socials", "details": social_reason}

    if not detail.get("is_minting"):
        return {"success": False, "reason": "not_minting_now"}

    if remaining_supply(detail) <= 0:
        return {"success": False, "reason": "sold_out"}

    stage = detail.get("active_stage")
    if not stage:
        return {"success": False, "reason": "no_active_stage"}
    if stage_has_ended(stage):
        return {"success": False, "reason": "stage_ended"}

    eth_price_usd = get_eth_price_usd()
    stage_price_wei = int(stage.get("price", "0") or 0)
    price_wei = stage_price_wei
    if price_wei == 0 and detail.get("contract_address"):
        onchain_price = await asyncio.to_thread(
            get_onchain_public_price_wei,
            W3_INSTANCES[chain_key],
            detail["contract_address"],
        )
        price_wei = onchain_price if onchain_price is not None else 0

    if not price_is_free_or_allowed(price_wei, eth_price_usd, allow_paid):
        return {"success": False, "reason": "paid_drop_blocked"}

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    final_quantity = min(quantity, decide_quantity(max_per_wallet, remaining_supply(detail)))

    ok, tx_payload = await asyncio.to_thread(build_drop_mint_transaction, slug, wallet.address, final_quantity)
    if not ok:
        return {"success": False, "reason": "not_eligible_or_builder_failed", "details": tx_payload}

    tx_fields = extract_tx_fields(tx_payload)
    if not tx_fields:
        return {"success": False, "reason": "unsupported_mint_payload", "details": tx_payload}
    tx_price_per_token = tx_fields["value"] // max(1, final_quantity)
    if not price_is_free_or_allowed(tx_price_per_token, eth_price_usd, allow_paid):
        return {"success": False, "reason": "paid_drop_blocked_after_builder"}

    private_key = await asyncio.to_thread(wallet_store.decrypt_private_key, wallet.address, WALLET_PASSWORD)
    result = await asyncio.to_thread(
        sign_and_send_mint_tx,
        chain_key,
        wallet,
        private_key,
        tx_fields,
        max_gas_fee_usd,
        deadline_ts,
    )
    result["quantity"] = final_quantity
    result["wallet"] = wallet.address
    result["socials"] = social_reason
    return result


async def evaluate_drop_for_active_wallets(
    slug: str,
    chain_key: str,
    quantity: int,
    allow_paid: bool,
    source: str,
) -> None:
    key = f"{chain_key}:{slug}:{source}"
    watch_key = f"{chain_key}:{slug}"
    if key in in_flight:
        return
    in_flight.add(key)
    try:
        wallets = wallet_store.active_wallets()
        if not wallets:
            if source == "manual":
                enqueue_telegram("لا توجد محافظ نشطة. أضف محفظة من زر المحافظ أولًا.", main_keyboard())
            return

        ok, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not ok or not detail:
            return

        social_ok, social_reason = collection_has_site_or_x(detail)
        if not social_ok:
            auto_watchlist.pop(watch_key, None)
            manual_watchlist.pop(watch_key, None)
            enqueue_telegram(f"تم تجاهل <b>{slug}</b>: {social_reason}")
            return

        if not detail.get("is_minting"):
            if source == "manual":
                manual_watchlist[watch_key] = {
                    "slug": slug,
                    "chain_key": chain_key,
                    "quantity": quantity,
                    "allow_paid": allow_paid,
                    "created_at": time.time(),
                }
            return

        if remaining_supply(detail) <= 0:
            auto_watchlist.pop(watch_key, None)
            manual_watchlist.pop(watch_key, None)
            enqueue_telegram(f"انتهت فرصة <b>{slug}</b>: الكمية نفدت.")
            return

        deadline_ts = stage_deadline_ts(detail)
        wallet_attempts = [
            (
                wallet,
                attempt_drop_for_wallet(
                    slug,
                    chain_key,
                    wallet,
                    quantity,
                    allow_paid,
                    CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"],
                    deadline_ts,
                ),
            )
            for wallet in wallets
            if f"{chain_key}:{slug}:{wallet.address}" not in bought_keys
        ]
        if not wallet_attempts:
            return

        results = await asyncio.gather(
            *(attempt for _wallet, attempt in wallet_attempts),
            return_exceptions=True,
        )
        transient = False
        for (wallet, _attempt), result in zip(wallet_attempts, results):
            if isinstance(result, Exception):
                log.warning("wallet attempt failed for %s: %s", wallet.address, result)
                transient = True
                continue
            if result.get("success"):
                bought_key = f"{chain_key}:{slug}:{wallet.address}"
                bought_keys.add(bought_key)
                runtime_state.add_unique("bought_keys", bought_key)
                enqueue_telegram(
                    "✅ <b>تم الشراء</b>\n"
                    f"المجموعة: <b>{slug}</b>\n"
                    f"المحفظة: <code>{short_address(result['wallet'])}</code>\n"
                    f"الكمية: {result['quantity']}\n"
                    f"الغاز: ${result['gas_fee_usd']:.4f}\n"
                    f"المصدر: {result['socials']}\n"
                    f"TX: <code>{result['tx_hash']}</code>",
                    main_keyboard(),
                )
            elif result.get("reason") in {"waiting_for_lower_gas", "not_minting_now", "not_eligible_or_builder_failed"}:
                transient = True

        if transient:
            target = manual_watchlist if source == "manual" else auto_watchlist
            target[watch_key] = {
                "slug": slug,
                "chain_key": chain_key,
                "quantity": quantity,
                "allow_paid": allow_paid,
                "created_at": time.time(),
            }
        else:
            auto_watchlist.pop(watch_key, None)
            manual_watchlist.pop(watch_key, None)
    finally:
        in_flight.discard(key)


async def watch_loop() -> None:
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        for _watch_key, entry in list(auto_watchlist.items()):
            await evaluate_drop_for_active_wallets(
                entry["slug"],
                entry["chain_key"],
                entry.get("quantity", LIMITED_BUY_QTY),
                entry.get("allow_paid", False),
                "auto",
            )
        for _watch_key, entry in list(manual_watchlist.items()):
            await evaluate_drop_for_active_wallets(
                entry["slug"],
                entry["chain_key"],
                entry.get("quantity", 1),
                entry.get("allow_paid", True),
                "manual",
            )


async def listen_opensea_stream() -> None:
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info("Advanced stream connected")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not (isinstance(parsed, list) and len(parsed) == 5):
                        continue

                    _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    chain_name = (item.get("chain", {}) or {}).get("name", "")
                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(chain_name)
                    if not chain_key:
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if slug and auto_buy_enabled():
                        watch_key = f"{chain_key}:{slug}"
                        auto_watchlist.setdefault(
                            watch_key,
                            {
                                "slug": slug,
                                "chain_key": chain_key,
                                "quantity": LIMITED_BUY_QTY,
                                "allow_paid": AUTO_BUY_PAID_DROPS,
                                "created_at": time.time(),
                            },
                        )
        except Exception as exc:
            log.warning("stream reconnect after error: %s", exc)
            await asyncio.sleep(3)


def opensea_get(path: str, params: dict[str, Any] | None = None) -> tuple[bool, dict[str, Any]]:
    try:
        response = requests.get(
            f"https://api.opensea.io/api/v2/{path.lstrip('/')}",
            headers={"x-api-key": OPENSEA_API_KEY},
            params=params or {},
            timeout=12,
        )
        if response.status_code == 200:
            return True, response.json()
        return False, {"status_code": response.status_code, "body": response.text[:500]}
    except Exception as exc:
        return False, {"error": str(exc)}


def opensea_post(path: str, payload: dict[str, Any], scoped: bool = False) -> tuple[bool, dict[str, Any]]:
    headers = {"x-api-key": OPENSEA_API_KEY, "content-type": "application/json"}
    if scoped and OPENSEA_SCOPED_TOKEN:
        headers["authorization"] = f"Bearer {OPENSEA_SCOPED_TOKEN}"
    try:
        response = requests.post(
            f"https://api.opensea.io/api/v2/{path.lstrip('/')}",
            headers=headers,
            json=payload,
            timeout=15,
        )
        if response.status_code in {200, 201}:
            return True, response.json()
        return False, {"status_code": response.status_code, "body": response.text[:1200]}
    except Exception as exc:
        return False, {"error": str(exc)}


def collection_floor_price(slug: str) -> str:
    cached = floor_cache.get(slug)
    if cached is not None:
        return cached
    ok, stats = opensea_get(f"collections/{slug}/stats")
    if not ok:
        return "غير متوفر"
    total = stats.get("total") or stats.get("stats") or stats
    floor = total.get("floor_price") or total.get("floorPrice")
    result = str(floor) if floor is not None else "غير متوفر"
    floor_cache.set(slug, result)
    return result


def create_listing_actions(proposal: dict[str, Any], price_eth: float) -> tuple[bool, dict[str, Any]]:
    if not OPENSEA_LISTING_ENABLED:
        return False, {"error": "OPENSEA_LISTING_ENABLED=false"}
    if not OPENSEA_SCOPED_TOKEN:
        return False, {"error": "OPENSEA_SCOPED_TOKEN is missing"}

    value_wei = str(int(price_eth * 1e18))
    payload = {
        "items": [
            {
                "chain": proposal["chain"],
                "token_address": proposal["contract"],
                "token_id": proposal["identifier"],
                "quantity": 1,
                "price": {
                    "currency": "ETH",
                    "value": value_wei,
                    "decimals": 18,
                },
                "expiration_time": int(time.time()) + 7 * 24 * 60 * 60,
            }
        ],
        "maker": proposal["wallet"],
        "use_creator_fee": True,
    }
    return opensea_post("listings/actions", payload, scoped=True)


def nft_contract_address(nft: dict[str, Any]) -> str | None:
    contract = nft.get("contract")
    if isinstance(contract, str):
        return contract
    if isinstance(contract, dict):
        return contract.get("address")
    return nft.get("contract_address") or nft.get("token_address")


def parse_wei(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


def dict_has_tx_fields(value: Any) -> bool:
    return isinstance(value, dict) and bool(value.get("to") or value.get("target")) and bool(
        value.get("data") or value.get("calldata") or value.get("input")
    )


def extract_transaction_action(action: dict[str, Any]) -> dict[str, Any] | None:
    candidates = [
        action,
        action.get("transaction"),
        action.get("transaction_data"),
        action.get("transactionData"),
        action.get("tx"),
        action.get("data"),
    ]
    for candidate in candidates:
        if dict_has_tx_fields(candidate):
            return {
                "to": Web3.to_checksum_address(candidate.get("to") or candidate.get("target")),
                "data": candidate.get("data") or candidate.get("calldata") or candidate.get("input"),
                "value": parse_wei(candidate.get("value"), 0),
                "gas": parse_wei(candidate.get("gas"), 0) or None,
            }
    return None


def extract_typed_data(action: dict[str, Any]) -> dict[str, Any] | None:
    candidates = [
        action.get("typed_data"),
        action.get("typedData"),
        action.get("signing_payload"),
        action.get("signingPayload"),
        action.get("payload"),
        action.get("data"),
        action,
    ]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if {"domain", "types", "message"}.issubset(candidate.keys()):
            return candidate
        nested = candidate.get("typed_data") or candidate.get("typedData")
        if isinstance(nested, dict) and {"domain", "types", "message"}.issubset(nested.keys()):
            return nested
    return None


def action_list(actions_payload: dict[str, Any]) -> list[dict[str, Any]]:
    actions = actions_payload.get("actions") or actions_payload.get("steps") or []
    return [action for action in actions if isinstance(action, dict)]


def is_signature_action(action: dict[str, Any]) -> bool:
    action_type = str(action.get("type") or action.get("kind") or action.get("action_type") or "").lower()
    method = str(action.get("method") or "").lower()
    return "sign" in action_type or "sig" in action_type or "signtypeddata" in method or extract_typed_data(action) is not None


def execute_transaction_action(
    chain_key: str,
    wallet: WalletRecord,
    private_key: str,
    tx_fields: dict[str, Any],
) -> dict[str, Any]:
    w3 = W3_INSTANCES[chain_key]
    sender = Web3.to_checksum_address(wallet.address)
    with wallet_tx_locks[sender.lower()]:
        tx = {
            "from": sender,
            "to": tx_fields["to"],
            "data": tx_fields["data"],
            "value": tx_fields["value"],
            "nonce": w3.eth.get_transaction_count(sender, "pending"),
            "chainId": w3.eth.chain_id,
            "gasPrice": w3.eth.gas_price,
        }
        if tx_fields.get("gas"):
            tx["gas"] = tx_fields["gas"]
        else:
            tx["gas"] = int(w3.eth.estimate_gas(tx) * GAS_LIMIT_SAFETY_MARGIN)

        signed = Account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt.get("status") != 1:
            raise RuntimeError(f"approval transaction failed: {tx_hash.hex()}")
        return {"success": True, "tx_hash": tx_hash.hex()}


def extract_listing_parameters(actions_payload: dict[str, Any], typed_data: dict[str, Any] | None) -> dict[str, Any] | None:
    candidates = [
        actions_payload.get("parameters"),
        (actions_payload.get("protocol_data") or {}).get("parameters"),
        (actions_payload.get("protocolData") or {}).get("parameters"),
        (actions_payload.get("order") or {}).get("parameters"),
    ]
    orders = actions_payload.get("orders")
    if isinstance(orders, list) and orders:
        candidates.append((orders[0].get("protocol_data") or {}).get("parameters"))
        candidates.append((orders[0].get("protocolData") or {}).get("parameters"))
        candidates.append(orders[0].get("parameters"))
    if typed_data:
        candidates.append(typed_data.get("message"))

    for candidate in candidates:
        if isinstance(candidate, dict) and candidate.get("offerer") and candidate.get("offer"):
            return candidate
    return None


def extract_protocol_address(actions_payload: dict[str, Any], typed_data: dict[str, Any] | None) -> str | None:
    candidates = [
        actions_payload.get("protocol_address"),
        actions_payload.get("protocolAddress"),
        (actions_payload.get("protocol_data") or {}).get("protocol_address"),
        (actions_payload.get("protocolData") or {}).get("protocolAddress"),
    ]
    orders = actions_payload.get("orders")
    if isinstance(orders, list) and orders:
        candidates.append(orders[0].get("protocol_address"))
        candidates.append(orders[0].get("protocolAddress"))
        candidates.append((orders[0].get("protocol_data") or {}).get("protocol_address"))
    if typed_data:
        candidates.append((typed_data.get("domain") or {}).get("verifyingContract"))

    for candidate in candidates:
        if candidate:
            return Web3.to_checksum_address(candidate)
    return None


def submit_listing_order(
    chain: str,
    parameters: dict[str, Any],
    protocol_address: str,
    signature: str,
) -> tuple[bool, dict[str, Any]]:
    body = {
        "parameters": parameters,
        "protocol_address": protocol_address,
        "signature": signature,
    }
    return opensea_post(f"orders/{chain}/seaport/listings", body, scoped=True)


def execute_listing(proposal: dict[str, Any], price_eth: float) -> tuple[bool, dict[str, Any]]:
    if not WALLET_PASSWORD:
        return False, {"error": "ADVANCED_WALLET_PASSWORD is missing"}
    wallet = wallet_store.wallets.get(proposal["wallet"].lower())
    if not wallet:
        return False, {"error": "wallet is not in local store"}

    private_key = wallet_store.decrypt_private_key(wallet.address, WALLET_PASSWORD)
    ok, actions_payload = create_listing_actions(proposal, price_eth)
    if not ok:
        return False, actions_payload

    approval_hashes = []
    typed_data = None
    for action in action_list(actions_payload):
        tx_fields = extract_transaction_action(action)
        if tx_fields and not is_signature_action(action):
            sent = execute_transaction_action(proposal["chain_key"], wallet, private_key, tx_fields)
            approval_hashes.append(sent["tx_hash"])
            continue
        if is_signature_action(action):
            typed_data = extract_typed_data(action) or typed_data

    if typed_data is None:
        typed_data = extract_typed_data(actions_payload)
    if typed_data is None:
        return False, {"error": "OpenSea did not return a typed-data signing payload", "payload": actions_payload}

    signed_message = Account.sign_typed_data(private_key, full_message=typed_data)
    signature = signed_message.signature.hex()
    parameters = extract_listing_parameters(actions_payload, typed_data)
    protocol_address = extract_protocol_address(actions_payload, typed_data)
    if not parameters or not protocol_address:
        return False, {"error": "missing listing parameters or protocol address", "payload": actions_payload}

    ok, posted = submit_listing_order(proposal["chain"], parameters, protocol_address, signature)
    if ok:
        posted["approval_tx_hashes"] = approval_hashes
    return ok, posted


async def scan_portfolios() -> None:
    wallets = wallet_store.active_wallets()
    if not wallets:
        enqueue_telegram("لا توجد محافظ نشطة للفحص.", main_keyboard())
        return

    for wallet in wallets:
        lines = [f"🖼 <b>فحص محفظة</b> <code>{short_address(wallet.address)}</code>"]
        proposals: list[dict[str, Any]] = []
        for chain_key in CHAIN_CONFIGS:
            ok, payload = await asyncio.to_thread(
                opensea_get,
                f"chain/{CHAIN_CONFIGS[chain_key]['stream_chain_name']}/account/{wallet.address}/nfts",
                {"limit": 20},
            )
            if not ok:
                lines.append(f"{chain_key}: تعذر جلب NFTs")
                continue
            nfts = payload.get("nfts") or payload.get("assets") or []
            if not nfts:
                lines.append(f"{chain_key}: لا توجد NFTs ظاهرة")
                continue
            lines.append(f"{chain_key}: {len(nfts)} عنصر/عناصر")
            for nft in nfts[:8]:
                collection = nft.get("collection") or {}
                slug = collection.get("slug") or nft.get("collection_slug") or ""
                name = nft.get("name") or nft.get("identifier") or "NFT"
                floor = collection_floor_price(slug) if slug else "غير متوفر"
                lines.append(f"• {name} | {slug or 'no-slug'} | floor: {floor}")
                contract = nft_contract_address(nft)
                identifier = nft.get("identifier") or nft.get("token_id") or nft.get("tokenId")
                if contract and identifier:
                    proposal_source = f"{chain_key}:{wallet.address}:{contract}:{identifier}"
                    proposal_id = hashlib.sha1(proposal_source.encode("utf-8")).hexdigest()[:16]
                    proposal = {
                        "id": proposal_id,
                        "wallet": wallet.address,
                        "chain": CHAIN_CONFIGS[chain_key]["stream_chain_name"],
                        "chain_key": chain_key,
                        "contract": contract,
                        "identifier": str(identifier),
                        "name": str(name),
                        "slug": slug,
                        "floor": floor,
                    }
                    sale_proposals[proposal_id] = proposal
                    proposals.append(proposal)

            offers_ok, offers_payload = await asyncio.to_thread(
                opensea_get,
                f"account/{wallet.address}/offers_received",
                {"limit": 10, "chains": CHAIN_CONFIGS[chain_key]["stream_chain_name"]},
            )
            offers = offers_payload.get("offers") or offers_payload.get("orders") or [] if offers_ok else []
            if offers:
                lines.append(f"{chain_key}: يوجد {len(offers)} عرض/عروض واردة تحتاج مراجعة.")

        enqueue_telegram("\n".join(lines), sale_keyboard(proposals) if proposals else main_keyboard())


async def handle_callback(callback: dict[str, Any]) -> None:
    data = callback.get("data", "")
    message = callback.get("message") or {}
    chat_id = str((message.get("chat") or {}).get("id", ""))
    answer_callback(callback["id"])

    if data == "menu":
        enqueue_telegram("لوحة التحكم:", main_keyboard())
    elif data == "status":
        enqueue_telegram(
            "⚙️ <b>الحالة</b>\n"
            f"BOT_ENABLED: <code>{BOT_ENABLED}</code>\n"
            f"الالتقاط التلقائي: <code>{auto_buy_enabled()}</code>\n"
            f"محافظ نشطة: <b>{len(wallet_store.active_wallets())}</b>\n"
            f"مراقبة تلقائية: <b>{len(auto_watchlist)}</b>\n"
            f"مراقبة يدوية: <b>{len(manual_watchlist)}</b>",
            main_keyboard(),
        )
    elif data == "toggle_auto_buy":
        set_auto_buy_enabled(not auto_buy_enabled())
        state = "مفعل" if auto_buy_enabled() else "متوقف"
        enqueue_telegram(f"تم تغيير الالتقاط التلقائي إلى: <b>{state}</b>", main_keyboard())
    elif data == "wallets":
        enqueue_telegram("إدارة المحافظ:", wallets_keyboard())
    elif data == "add_wallet":
        telegram_state[chat_id] = {"awaiting": "private_key"}
        enqueue_telegram(
            "أرسل المفتاح الخاص للمحفظة في الرسالة التالية.\n"
            "سيتم تخزينه مشفرًا محليًا باستخدام <code>ADVANCED_WALLET_PASSWORD</code>، وسأحاول حذف رسالة المفتاح من تيليجرام بعد استلامها."
        )
    elif data.startswith("toggle_wallet:"):
        address = data.split(":", 1)[1]
        record = wallet_store.toggle_active(address)
        if record:
            state = "مفعلة" if record.active else "متوقفة"
            enqueue_telegram(f"تم تغيير حالة {short_address(record.address)} إلى: {state}", wallets_keyboard())
    elif data == "manual_mint":
        telegram_state[chat_id] = {"awaiting": "manual_mint"}
        enqueue_telegram("أرسل رابط OpenSea للمينت أو slug، ثم الكمية. مثال:\n<code>my-drop-slug 2</code>")
    elif data.startswith("sell_prompt:"):
        proposal_id = data.split(":", 1)[1]
        proposal = sale_proposals.get(proposal_id)
        if not proposal:
            enqueue_telegram("هذا العنصر لم يعد موجودًا في ذاكرة الفحص. أعد فحص المحافظ.", main_keyboard())
            return
        telegram_state[chat_id] = {"awaiting": "listing_price", "proposal_id": proposal_id}
        enqueue_telegram(
            "أرسل سعر العرض بالـ ETH لهذا العنصر:\n"
            f"<b>{proposal['name']}</b>\n"
            f"Floor الحالي: <code>{proposal.get('floor', 'غير متوفر')}</code>\n"
            f"العنصر: <code>{proposal['contract']} #{proposal['identifier']}</code>"
        )
    elif data == "scan_portfolio":
        enqueue_telegram("بدأت فحص المحافظ النشطة، سأرسل النتائج هنا.")
        asyncio.create_task(scan_portfolios())
    elif data == "watchlist":
        enqueue_telegram(
            f"المراقبة الحالية: تلقائي {len(auto_watchlist)}، يدوي {len(manual_watchlist)}",
            watchlist_keyboard(),
        )
    elif data.startswith("drop_watch:"):
        callback_id = data.split(":", 1)[1]
        watch_key = watch_callback_keys.get(callback_id, callback_id)
        auto_watchlist.pop(watch_key, None)
        manual_watchlist.pop(watch_key, None)
        enqueue_telegram(f"تم إلغاء مراقبة <b>{watch_key}</b>.", watchlist_keyboard())


async def handle_message(message: dict[str, Any]) -> None:
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id", ""))
    text = (message.get("text") or "").strip()

    if text in {"/start", "/menu"}:
        enqueue_telegram("لوحة التحكم:", main_keyboard())
        return

    state = telegram_state.get(chat_id) or {}
    awaiting = state.get("awaiting")

    if awaiting == "private_key":
        telegram_state.pop(chat_id, None)
        if message.get("message_id"):
            await asyncio.to_thread(delete_message, chat_id, message["message_id"])
        if not WALLET_PASSWORD:
            enqueue_telegram("لا يمكن إضافة محفظة قبل ضبط <code>ADVANCED_WALLET_PASSWORD</code> في ملف البيئة.", wallets_keyboard())
            return
        try:
            record = await asyncio.to_thread(wallet_store.add_wallet, text, WALLET_PASSWORD)
            enqueue_telegram(f"تمت إضافة المحفظة وتفعيلها: <code>{short_address(record.address)}</code>", wallets_keyboard())
        except Exception as exc:
            enqueue_telegram(f"تعذر إضافة المحفظة: <code>{str(exc)}</code>", wallets_keyboard())
        return

    if awaiting == "manual_mint":
        telegram_state.pop(chat_id, None)
        slug = extract_slug(text)
        quantity = extract_quantity(text)
        if not slug:
            enqueue_telegram("لم أستطع استخراج slug من الرسالة. أرسل رابط OpenSea أو slug واضح.", main_keyboard())
            return
        for chain_key in CHAIN_CONFIGS:
            watch_key = f"{chain_key}:{slug}"
            manual_watchlist[watch_key] = {
                "slug": slug,
                "chain_key": chain_key,
                "quantity": quantity,
                "allow_paid": True,
                "created_at": time.time(),
            }
        enqueue_telegram(
            f"تمت إضافة <b>{slug}</b> للمراقبة اليدوية بكمية <b>{quantity}</b>.\n"
            "سأحاول الشراء للمحافظ النشطة عند فتح مرحلة مؤهلة أو عامة، مدفوعة أو مجانية، مع انتظار أقل غاز ممكن ضمن وقت المرحلة.",
            main_keyboard(),
        )
        return

    if awaiting == "listing_price":
        telegram_state.pop(chat_id, None)
        proposal = sale_proposals.get(state.get("proposal_id"))
        if not proposal:
            enqueue_telegram("هذا العنصر لم يعد موجودًا في ذاكرة الفحص. أعد فحص المحافظ.", main_keyboard())
            return
        try:
            price_eth = float(text.replace(",", "."))
        except ValueError:
            enqueue_telegram("السعر غير واضح. أرسل رقمًا مثل <code>0.025</code>.", main_keyboard())
            return
        if price_eth <= 0:
            enqueue_telegram("السعر يجب أن يكون أكبر من صفر.", main_keyboard())
            return

        ok, payload = await asyncio.to_thread(execute_listing, proposal, price_eth)
        if ok:
            enqueue_telegram(
                "✅ تم نشر العرض على OpenSea.\n"
                f"العنصر: <b>{proposal['name']}</b>\n"
                f"السعر: <code>{price_eth}</code> ETH\n"
                f"معاملات approval: <code>{payload.get('approval_tx_hashes', [])}</code>",
                main_keyboard(),
            )
            log.info("listing actions for %s: %s", proposal["id"], payload)
        else:
            enqueue_telegram(
                "لم يتم نشر العرض.\n"
                f"السبب: <code>{payload.get('error') or payload.get('body') or payload}</code>\n"
                "لتفعيل هذا المسار اضبط <code>OPENSEA_LISTING_ENABLED=true</code> و<code>OPENSEA_SCOPED_TOKEN</code>.",
                main_keyboard(),
            )
        return

    enqueue_telegram("استخدم الأزرار للتحكم في البوت.", main_keyboard())


async def telegram_updates_loop() -> None:
    offset = None
    while True:
        try:
            params: dict[str, Any] = {"timeout": 25, "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset is not None:
                params["offset"] = offset
            response = await asyncio.to_thread(
                requests.get,
                f"{TELEGRAM_API}/getUpdates",
                params=params,
                timeout=35,
            )
            payload = response.json()
            for update in payload.get("result", []):
                offset = update["update_id"] + 1
                if not only_allowed_chat(update):
                    continue
                if "callback_query" in update:
                    await handle_callback(update["callback_query"])
                elif "message" in update:
                    await handle_message(update["message"])
        except Exception as exc:
            log.warning("telegram polling error: %s", exc)
            await asyncio.sleep(3)


async def periodic_portfolio_scan() -> None:
    while True:
        await asyncio.sleep(PORTFOLIO_SCAN_INTERVAL_SECONDS)
        await scan_portfolios()


async def heartbeat_status_loop() -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_STATUS_INTERVAL_SECONDS)
        gas_lines = []
        for chain_key, sample in gas_optimizer.samples.items():
            if sample.last_usd is not None:
                gas_lines.append(f"{chain_key}: last ${sample.last_usd:.4f}, best ${sample.best_usd:.4f}")
        enqueue_telegram(
            "نبض الحالة:\n"
            f"الالتقاط التلقائي: <code>{auto_buy_enabled()}</code>\n"
            f"محافظ نشطة: <b>{len(wallet_store.active_wallets())}</b>\n"
            f"مراقبة تلقائية: <b>{len(auto_watchlist)}</b>\n"
            f"مراقبة يدوية: <b>{len(manual_watchlist)}</b>\n"
            + ("\n".join(gas_lines) if gas_lines else "لا توجد عينات غاز بعد."),
            main_keyboard(),
        )


async def health_server_loop() -> None:
    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.read(2048)
            body = json.dumps(
                {
                    "ok": True,
                    "auto_buy_enabled": auto_buy_enabled(),
                    "active_wallets": len(wallet_store.active_wallets()),
                    "auto_watchlist": len(auto_watchlist),
                    "manual_watchlist": len(manual_watchlist),
                }
            ).encode("utf-8")
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                + b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n".encode("ascii")
                + b"Connection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    log.info("health server listening on port %s", PORT)
    async with server:
        await server.serve_forever()


def extract_drop_slug(drop: dict[str, Any]) -> str | None:
    collection = drop.get("collection") if isinstance(drop.get("collection"), dict) else {}
    return (
        drop.get("slug")
        or drop.get("collection_slug")
        or drop.get("collectionSlug")
        or collection.get("slug")
    )


async def discover_drops_loop() -> None:
    while True:
        try:
            if not auto_buy_enabled():
                await asyncio.sleep(DROPS_DISCOVERY_INTERVAL_SECONDS)
                continue
            for chain_key, cfg in CHAIN_CONFIGS.items():
                for drop_type in ("upcoming", "active", "featured"):
                    ok, payload = await asyncio.to_thread(
                        opensea_get,
                        "drops",
                        {
                            "chain": cfg["stream_chain_name"],
                            "chains": cfg["stream_chain_name"],
                            "type": drop_type,
                            "limit": 50,
                        },
                    )
                    if not ok:
                        continue
                    drops = payload.get("drops") or payload.get("results") or []
                    for drop in drops:
                        if not isinstance(drop, dict):
                            continue
                        slug = extract_drop_slug(drop)
                        if not slug:
                            continue
                        watch_key = f"{chain_key}:{slug}"
                        auto_watchlist.setdefault(
                            watch_key,
                            {
                                "slug": slug,
                                "chain_key": chain_key,
                                "quantity": LIMITED_BUY_QTY,
                                "allow_paid": AUTO_BUY_PAID_DROPS,
                                "created_at": time.time(),
                            },
                        )
        except Exception as exc:
            log.warning("drop discovery error: %s", exc)
        await asyncio.sleep(DROPS_DISCOVERY_INTERVAL_SECONDS)


async def run() -> None:
    if not BOT_ENABLED:
        enqueue_telegram("🔴 البوت المتقدم يعمل بوضع الإيقاف لأن <code>BOT_ENABLED=false</code>.", main_keyboard())
        await telegram_sender()
        return

    enqueue_telegram("✅ البوت المتقدم اشتغل. استخدم لوحة التحكم:", main_keyboard())
    tasks = [
        telegram_sender(),
        telegram_updates_loop(),
        listen_opensea_stream(),
        watch_loop(),
        discover_drops_loop(),
        periodic_portfolio_scan(),
        heartbeat_status_loop(),
    ]
    if ENABLE_HEALTH_SERVER:
        tasks.append(health_server_loop())
    await asyncio.gather(*tasks)


def main() -> None:
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("stopped manually")
            break
        except Exception as exc:
            log.exception("advanced bot crashed: %s", exc)
            time.sleep(3)


if __name__ == "__main__":
    main()

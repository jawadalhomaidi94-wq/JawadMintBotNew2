from __future__ import annotations

import json
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
    return "Mint stage"


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
        return "unknown"
    if isinstance(value, dict):
        for path in ("value", "raw", "wei", "amount"):
            v = value.get(path)
            if v is not None:
                if isinstance(v, dict):
                    v = v.get("value") or v.get("raw") or v.get("wei")
                if v is not None:
                    return str(v)
    return str(value)


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
        return "unknown"
    return datetime.fromtimestamp(ts, tz).strftime("%Y-%m-%d %H:%M:%S %Z")


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

    def run(self) -> None:
        log.info("Telegram listener enabled")
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
                        self.send(chat_id, f"⛔ This chat is not authorized. Chat ID: {chat_id}")
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
        self.gas_strategy = os.getenv("GAS_STRATEGY", "fast").strip().lower()
        self.gas_limit_buffer = max(1.0, env_float("GAS_LIMIT_BUFFER", 1.15))
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
        self.paused = env_bool("START_PAUSED", False)

        self.wallets: list[WalletConfig] = []
        self.rpc_pools: dict[str, RpcPool] = {}
        self.candidates: dict[str, Candidate] = {}
        self.command_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.pending_wallet_import: dict[str, float] = {}
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
                    candidate.wallets.pop(address, None)
            for wallet in self.wallets:
                key = wallet.address.lower()
                if not wallet.supports_chain(candidate.chain) or key in candidate.wallets:
                    continue
                qty = candidate.quantity_override or wallet.quantity or self.quantity_default
                if candidate.wallet_limit:
                    qty = min(qty, candidate.wallet_limit)
                probe_at = max(now, ((candidate.next_stage_start or candidate.public_start or now) - self.preopen_probe_seconds))
                candidate.wallets[key] = WalletState(wallet=wallet, quantity=max(1, qty), next_attempt=probe_at)
                self.notify_all(
                    f"➕ New wallet joined active watch\n{wallet.name} {short_address(wallet.address)}\n"
                    f"Drop: {candidate.slug}\nChain: {candidate.chain}\nIt will be checked/minted automatically."
                )

    def add_wallet_key(self, private_key: str) -> tuple[bool, str]:
        try:
            account = Account.from_key(private_key.strip())
            address = Web3.to_checksum_address(account.address)
        except Exception:
            return False, "Invalid EVM private key."
        if any(w.address.lower() == address.lower() for w in self.wallets):
            return False, f"Wallet already exists: {short_address(address)}"
        name = f"wallet-{len(self.store.list_wallets(enabled_only=False)) + 1}"
        self.store.add_wallet(
            name=name, address=address, private_key=private_key.strip(),
            quantity=self.quantity_default, chains=tuple(self.enabled_chains),
        )
        self.reload_wallets()
        return True, f"✅ {name} added securely\nAddress: {address}\nChains: {', '.join(self.enabled_chains)}"

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

    def summarize_stages(self, drop: dict[str, Any]) -> tuple[list[str], float | None, float | None, int | None]:
        now = time.time()
        lines: list[str] = []
        public_start = None
        next_start = None
        wallet_limit = None
        for stage in get_stages(drop):
            start = stage_start(stage)
            end = stage_end(stage)
            label = stage_label(stage)
            price = extract_price_hint(stage)
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
                f"• {label}: {format_ts(start, self.display_tz)} | price={price}"
                + (f" | max/wallet={limit}" if limit else "")
            )
        return lines, public_start, next_start, wallet_limit

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
    ) -> tuple[bool, str, Candidate | None]:
        chain = self.detect_chain(slug, drop, chain_hint)
        if not chain:
            return False, f"{slug}: could not detect chain. Use /watch <chain> <url-or-slug>", None
        if chain not in self.rpc_pools:
            return False, f"{slug}: {chain} has no working RPC", None
        lines, public_start, next_start, wallet_limit = self.summarize_stages(drop)
        if not lines:
            return False, f"{slug}: OpenSea returned no mint stages", None

        key = f"{chain}:{slug}"
        candidate = self.candidates.get(key)
        if candidate is None:
            candidate = Candidate(
                slug=slug, chain=chain, source=source, allow_paid=allow_paid,
                max_mint_price_native=max_mint_price_native, quantity_override=quantity_override,
            )
            self.candidates[key] = candidate
        candidate.source = source
        candidate.allow_paid = allow_paid
        candidate.max_mint_price_native = max_mint_price_native
        candidate.quantity_override = quantity_override
        candidate.stage_lines = lines
        candidate.public_start = public_start
        candidate.next_stage_start = next_start
        candidate.wallet_limit = wallet_limit
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

        paid_text = "ON (paid + free)" if allow_paid else "FREE ONLY"
        cap_text = "unlimited" if max_mint_price_native <= 0 else f"{max_mint_price_native} {native_symbol(chain)}"
        message = (
            f"🎯 Watching {slug}\n"
            f"Chain: {chain}\n"
            f"Public opens: {format_ts(public_start, self.display_tz)}\n"
            f"Wallets: {len(candidate.wallets)}\n"
            f"Auto paid mint: {paid_text}\n"
            f"Mint-price cap: {cap_text}\n\n"
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
            return False, "Could not extract an OpenSea collection/drop slug.", None
        chain_hint = normalize_chain(forced_chain) if forced_chain else chain_from_input
        try:
            drop = self.opensea.get_drop(slug)
        except Exception as exc:
            return False, f"OpenSea drop lookup failed for {slug}: {exc}", None
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
            return "Invalid slug"
        self.store.remove_watch(slug)
        removed = 0
        for key in list(self.candidates):
            if self.candidates[key].slug.lower() == slug.lower():
                self.candidates.pop(key, None); removed += 1
        return f"🗑 Removed {slug} ({removed} active watch)."

    def bootstrap_watches(self) -> None:
        for row in self.store.list_watches():
            if STOP:
                break
            slug = str(row["slug"])
            try:
                drop = self.opensea.get_drop(slug)
                self.register_drop(
                    slug, drop, str(row.get("chain") or "") or None, str(row.get("source") or slug),
                    allow_paid=bool(row.get("allow_paid", 1)),
                    max_mint_price_native=Decimal(str(row.get("max_mint_price_native") or "0")),
                    quantity_override=row.get("quantity"),
                )
            except Exception as exc:
                log.warning("Could not restore watch %s: %s", slug, exc)

    def refresh_candidate(self, candidate: Candidate) -> None:
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

    # ---------- eligibility + mint execution ----------
    def eligibility_matrix(self, candidate: Candidate) -> str:
        states = list(candidate.wallets.values())
        if not states:
            return "No wallets."
        pool = self.rpc_pools[candidate.chain]
        lines = [f"🧪 Eligibility — {candidate.slug} ({candidate.chain})"]

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
            futures = [executor.submit(one, s) for s in states]
            for future in as_completed(futures):
                state, result, balance = future.result()
                state.eligibility = result.status
                state.mint_value_native = result.mint_value_native
                icon = "✅" if result.eligible is True else "❌" if result.eligible is False else "⏳"
                price = "" if result.mint_value_native is None else f" | mint={result.mint_value_native} {native_symbol(candidate.chain)}"
                bal = "" if balance is None else f" | bal={balance:.6f}"
                lines.append(f"{icon} {state.wallet.name} {short_address(state.wallet.address)}: {result.status}{price}{bal}")
        return "\n".join(lines)[:3900]

    def notify_state_change(self, candidate: Candidate, state: WalletState, status: str, detail: str | None = None) -> None:
        if status == state.last_notified_status:
            return
        state.last_notified_status = status
        if status in {"not_mintable_yet", "not_active_yet"}:
            return
        labels = {
            "precondition_failed": "❌ Not eligible in current stage",
            "rate_limited": "⏳ OpenSea rate-limited request",
            "insufficient_balance": "💸 Insufficient wallet balance",
            "gas_too_high": "⛽ Gas above configured cap",
            "mint_price_too_high": "💰 Mint price above configured cap",
            "total_spend_too_high": "🛡 Total spend above configured cap",
            "paid_not_allowed": "🔒 Paid mint blocked by policy",
            "rpc_or_tx_error": "⚠️ RPC/transaction error",
            "opensea_error": "⚠️ OpenSea API error",
        }
        if status in labels:
            self.notify_all(
                f"{labels[status]}\nDrop: {candidate.slug}\nWallet: {state.wallet.name} {short_address(state.wallet.address)}\n"
                f"Chain: {candidate.chain}\n{(detail or '')[:600]}"
            )

    def try_candidate(self, candidate: Candidate) -> None:
        if self.paused:
            return
        now = time.time()
        due = [s for s in candidate.wallets.values() if not s.submitted and not s.final and s.next_attempt <= now]
        if not due:
            return
        pool = self.rpc_pools[candidate.chain]
        workers = min(self.max_parallel_wallets, len(due))

        def execute(state: WalletState):
            state.attempts += 1
            state.status = "attempting"
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
            )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(execute, state) for state in due]
            for future in as_completed(futures):
                state, result = future.result()
                state.status = result.status
                state.last_detail = result.detail
                if result.ok:
                    state.submitted = True
                    state.tx_hash = result.tx_hash
                    state.mint_value_native = result.mint_value_native
                    state.receipt_next_check = time.time() + self.receipt_check_seconds
                    self.store.record_mint(
                        slug=candidate.slug, chain=candidate.chain,
                        wallet_name=state.wallet.name, wallet_address=state.wallet.address,
                        status="submitted", tx_hash=result.tx_hash,
                        mint_value_native=str(result.mint_value_native),
                        gas_max_native=str(result.gas_cost_native), detail=result.detail,
                    )
                    url = explorer_tx_url(candidate.chain, result.tx_hash or "")
                    self.notify_all(
                        f"🚀 MINT SUBMITTED\n"
                        f"Drop: {candidate.slug}\nChain: {candidate.chain}\n"
                        f"Wallet: {state.wallet.name} {short_address(state.wallet.address)}\n"
                        f"Quantity: {state.quantity}\nMint value: {result.mint_value_native} {native_symbol(candidate.chain)}\n"
                        f"Max gas estimate: {result.gas_cost_native} {native_symbol(candidate.chain)}\n"
                        f"TX: {result.tx_hash}\n{url}"
                    )
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
                    slug=candidate.slug, chain=candidate.chain,
                    wallet_name=state.wallet.name, wallet_address=state.wallet.address,
                    status="confirmed", tx_hash=state.tx_hash,
                    mint_value_native=str(state.mint_value_native) if state.mint_value_native is not None else None,
                )
                self.notify_all(
                    f"✅ MINT CONFIRMED\nDrop: {candidate.slug}\nWallet: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"Chain: {candidate.chain}\nTX: {state.tx_hash}\n{explorer_tx_url(candidate.chain, state.tx_hash)}"
                )
            else:
                state.final = True
                self.store.record_mint(
                    slug=candidate.slug, chain=candidate.chain,
                    wallet_name=state.wallet.name, wallet_address=state.wallet.address,
                    status="reverted", tx_hash=state.tx_hash,
                )
                self.notify_all(
                    f"❌ MINT REVERTED\nDrop: {candidate.slug}\nWallet: {state.wallet.name} {short_address(state.wallet.address)}\n"
                    f"TX: {state.tx_hash}"
                )

    # ---------- Telegram UI ----------
    def menu_buttons(self) -> list[list[tuple[str, str]]]:
        return [
            [("➕ Add Wallet", "add_wallet"), ("👛 Wallets", "wallets")],
            [("🎯 Watches", "watches"), ("🧪 Check Eligibility", "eligibility_all")],
            [("🌐 Networks", "chains"), ("📜 History", "history")],
            [("⏸ Pause" if not self.paused else "▶️ Resume", "toggle_pause"), ("⚙️ Settings", "settings")],
        ]

    def send_menu(self, chat_id: str) -> None:
        self.telegram.send(
            chat_id,
            "🤖 OpenSea Mint Guardian V3\nSend an OpenSea drop/collection link and I will keep watching it for all wallets.",
            self.menu_buttons(),
        )

    def wallets_text(self) -> str:
        if not self.wallets:
            return "No wallets yet. Use ➕ Add Wallet."
        lines = ["👛 Wallets"]
        for wallet in self.wallets:
            balance_bits = []
            for chain in self.enabled_chains:
                pool = self.rpc_pools.get(chain)
                if not pool or not wallet.supports_chain(chain):
                    continue
                try:
                    balance_bits.append(f"{chain}={pool.balance_native(wallet.address):.6f}")
                except Exception:
                    pass
            lines.append(f"• {wallet.name} {short_address(wallet.address)} | qty={wallet.quantity}\n  " + " | ".join(balance_bits))
        return "\n".join(lines)[:3900]

    def chains_text(self) -> str:
        lines = ["🌐 Network status"]
        for chain in self.enabled_chains:
            pool = self.rpc_pools.get(chain)
            lines.append(f"{'✅' if pool else '❌'} {chain}: {len(pool.urls) if pool else 0} verified RPC(s)" + (f" | primary={pool.primary_url}" if pool else ""))
        return "\n".join(lines)[:3900]

    def status_text(self) -> str:
        if not self.candidates:
            return "No active watches."
        lines = ["🎯 Active watches"]
        for c in self.candidates.values():
            lines.append(
                f"\n• {c.slug} | {c.chain}\nPublic: {format_ts(c.public_start, self.display_tz)}\n"
                f"Submitted: {c.submitted_count()}/{len(c.wallets)} | Confirmed: {c.confirmed_count()}\n"
                f"Paid: {'ON' if c.allow_paid else 'OFF'} | Price cap: {'∞' if c.max_mint_price_native <= 0 else c.max_mint_price_native}"
            )
        return "\n".join(lines)[:3900]

    def settings_text(self) -> str:
        return (
            f"⚙️ Settings\n"
            f"Paused: {self.paused}\n"
            f"Auto paid mints: {self.allow_paid_default}\n"
            f"Max mint price: {'unlimited' if self.max_mint_price_default <= 0 else self.max_mint_price_default}\n"
            f"Max gas: {'unlimited' if self.max_gas_native <= 0 else self.max_gas_native}\n"
            f"Max total/mint: {'unlimited' if self.max_total_native <= 0 else self.max_total_native}\n"
            f"Gas strategy: {self.gas_strategy}\n"
            f"Parallel wallets: {self.max_parallel_wallets}\n"
            f"Pre-open probe: {self.preopen_probe_seconds}s"
        )

    def history_text(self) -> str:
        rows = self.store.recent_history(12)
        if not rows:
            return "No mint history yet."
        lines = ["📜 Recent mint history"]
        for row in rows:
            ts = format_ts(float(row["created_at"]), self.display_tz)
            lines.append(f"• {row['status']} | {row['slug']} | {row['wallet_name']} | {ts}" + (f"\n  {row['tx_hash']}" if row.get("tx_hash") else ""))
        return "\n".join(lines)[:3900]

    def all_eligibility_text(self) -> str:
        if not self.candidates:
            return "No active watches."
        chunks = []
        for candidate in list(self.candidates.values())[:4]:
            chunks.append(self.eligibility_matrix(candidate))
        return "\n\n".join(chunks)[:3900]

    def handle_callback(self, event: dict[str, Any]) -> None:
        chat_id = event["chat_id"]
        data = event.get("data", "")
        self.telegram.answer_callback(event.get("callback_id", ""))
        if data == "add_wallet":
            if event.get("chat_type") != "private":
                self.telegram.send(chat_id, "🔐 Add-wallet works only in a private chat with the bot.")
                return
            self.pending_wallet_import[chat_id] = time.time() + 180
            self.telegram.send(
                chat_id,
                "🔐 Send the PRIVATE KEY for the dedicated mint wallet now.\n"
                "I will validate it, encrypt it immediately in the Railway database, and try to delete your Telegram message.\n"
                "Use a dedicated mint wallet, never your main wallet.\n\nSend /cancel to abort.",
            )
            return
        if data == "wallets":
            self.telegram.send(chat_id, self.wallets_text(), self.menu_buttons()); return
        if data == "watches":
            self.telegram.send(chat_id, self.status_text(), self.menu_buttons()); return
        if data == "chains":
            self.telegram.send(chat_id, self.chains_text(), self.menu_buttons()); return
        if data == "history":
            self.telegram.send(chat_id, self.history_text(), self.menu_buttons()); return
        if data == "settings":
            self.telegram.send(chat_id, self.settings_text(), self.menu_buttons()); return
        if data == "eligibility_all":
            self.telegram.send(chat_id, "🧪 Checking every active watch against every wallet…")
            self.telegram.send(chat_id, self.all_eligibility_text(), self.menu_buttons()); return
        if data == "toggle_pause":
            self.paused = not self.paused
            self.telegram.send(chat_id, "⏸ Mint signing PAUSED. Monitoring continues." if self.paused else "▶️ Mint signing RESUMED.", self.menu_buttons())
            return

    def handle_message(self, event: dict[str, Any]) -> None:
        chat_id = event["chat_id"]
        text = event.get("text", "").strip()

        expiry = self.pending_wallet_import.get(chat_id)
        if expiry:
            if text.lower() == "/cancel":
                self.pending_wallet_import.pop(chat_id, None)
                self.telegram.send(chat_id, "Cancelled.", self.menu_buttons())
                return
            if time.time() > expiry:
                self.pending_wallet_import.pop(chat_id, None)
                self.telegram.send(chat_id, "Wallet import timed out. Tap Add Wallet again.", self.menu_buttons())
                return
            self.pending_wallet_import.pop(chat_id, None)
            self.telegram.delete_message(chat_id, int(event.get("message_id", 0)))
            ok, message = self.add_wallet_key(text)
            self.telegram.send(chat_id, message, self.menu_buttons())
            if ok and self.candidates:
                self.telegram.send(chat_id, self.all_eligibility_text())
            return

        parts = text.split()
        command = parts[0].lower() if parts else ""
        if command in {"/start", "/help", "/menu"}:
            self.send_menu(chat_id); return
        if command == "/status":
            self.telegram.send(chat_id, self.status_text(), self.menu_buttons()); return
        if command == "/wallets":
            self.telegram.send(chat_id, self.wallets_text(), self.menu_buttons()); return
        if command == "/chains":
            self.telegram.send(chat_id, self.chains_text(), self.menu_buttons()); return
        if command == "/history":
            self.telegram.send(chat_id, self.history_text(), self.menu_buttons()); return
        if command in {"/pause", "/panic"}:
            self.paused = True; self.telegram.send(chat_id, "⏸ Mint signing PAUSED. Monitoring continues.", self.menu_buttons()); return
        if command == "/resume":
            self.paused = False; self.telegram.send(chat_id, "▶️ Mint signing RESUMED.", self.menu_buttons()); return
        if command == "/eligibility":
            self.telegram.send(chat_id, self.all_eligibility_text(), self.menu_buttons()); return
        if command == "/remove" and len(parts) >= 2:
            self.telegram.send(chat_id, self.remove_watch(parts[1]), self.menu_buttons()); return
        if command == "/deletewallet" and len(parts) >= 2:
            address = parts[1].strip()
            if not Web3.is_address(address):
                self.telegram.send(chat_id, "Invalid wallet address.", self.menu_buttons()); return
            deleted = self.store.delete_wallet(Web3.to_checksum_address(address))
            self.reload_wallets()
            self.telegram.send(chat_id, "🗑 Wallet deleted." if deleted else "Wallet not found.", self.menu_buttons()); return
        if command == "/watch":
            if len(parts) < 2:
                self.telegram.send(chat_id, "Usage: /watch [chain] <OpenSea URL or slug>"); return
            forced_chain = None
            raw = " ".join(parts[1:])
            maybe = normalize_chain(parts[1])
            if maybe in CHAIN_CONFIGS and len(parts) >= 3:
                forced_chain = maybe; raw = " ".join(parts[2:])
            ok, message, candidate = self.add_watch(raw, forced_chain=forced_chain, persist=True)
            self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.menu_buttons())
            if ok and candidate:
                self.telegram.send(chat_id, self.eligibility_matrix(candidate))
            return

        if "opensea.io" in text.lower() or re.fullmatch(r"[A-Za-z0-9._-]{2,200}", text):
            ok, message, candidate = self.add_watch(text, persist=True)
            self.telegram.send(chat_id, ("✅ " if ok else "⚠️ ") + message, self.menu_buttons())
            if ok and candidate:
                self.telegram.send(chat_id, self.eligibility_matrix(candidate))
            return
        self.telegram.send(chat_id, "Send an OpenSea link or use /menu.", self.menu_buttons())

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
                self.telegram.send(event.get("chat_id", ""), f"⚠️ Command failed: {exc}")

    def notify_all(self, text: str) -> None:
        if not self.telegram.enabled:
            return
        for chat_id in self.telegram.allowed_chat_ids:
            self.telegram.send(chat_id, text)

    # ---------- main loop ----------
    def run(self) -> None:
        start_health_server()
        log.info("Mint Guardian V3 starting")
        log.info("Chains: %s", ", ".join(self.enabled_chains))
        log.info("Wallets: %s | paid=%s | gas cap=%s | mint price cap=%s", len(self.wallets), self.allow_paid_default, self.max_gas_native, self.max_mint_price_default)
        if self.telegram.enabled:
            self.telegram.start()
        self.bootstrap_watches()
        self.notify_all("🟢 Mint Guardian V3 is online on Railway.\nMonitoring restored watches and all enabled wallets.")

        while not STOP:
            self.drain_commands()
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

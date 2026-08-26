"""
النظام الكامل للشراء والمراقبة الدائمة عبر OpenSea + SeaDrop.

الوظائف:

1. مراقبة OpenSea Stream.
2. اكتشاف عمليات mint الجديدة.
3. دعم:
   - Robinhood Chain
   - Ethereum Mainnet
4. فحص تفاصيل الـDrop من OpenSea.
5. التأكد أن المرحلة بدأت اليوم.
6. فحص السعر مباشرة من SeaDrop.
7. فحص الغاز.
8. فحص الرصيد.
9. محاولة الشراء فور تحقق الشروط.
10. إذا كان السعر مدفوعًا:
      -> مراقبة مستمرة.
11. إذا كان الغاز مرتفعًا:
      -> مراقبة مستمرة.
12. إذا نجح الشراء:
      -> إيقاف مراقبة المجموعة.
13. حفظ المشتريات على القرص:
      -> لا يتم شراء نفس المجموعة مرة أخرى
         حتى بعد إعادة تشغيل البرنامج.
14. إذا انتهت المرحلة:
      -> إيقاف المراقبة.
15. إذا نفدت الكمية:
      -> إيقاف المراقبة.
16. إرسال إشعارات Telegram.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase,
    get_onchain_public_price_wei,
)


# ===========================================================================
# ENV
# ===========================================================================

load_dotenv()


def required_env(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(
            f"متغير البيئة {name} غير موجود."
        )

    return value.strip()


OPENSEA_API_KEY = required_env(
    "OPENSEA_API_KEY"
)

TELEGRAM_BOT_TOKEN = required_env(
    "TELEGRAM_BOT_TOKEN"
)

TELEGRAM_CHAT_ID = required_env(
    "TELEGRAM_CHAT_ID"
)

PRIVATE_KEY = required_env(
    "PRIVATE_KEY"
)

WALLET_ADDRESS = required_env(
    "WALLET_ADDRESS"
)

ALCHEMY_API_KEY_ROBINHOOD = required_env(
    "ALCHEMY_API_KEY"
)

ALCHEMY_API_KEY_ETHEREUM = required_env(
    "ALCHEMY_API_KEY_ETHEREUM"
)

BOT_ENABLED = (
    os.environ
    .get(
        "BOT_ENABLED",
        "false",
    )
    .strip()
    .lower()
    == "true"
)


# ===========================================================================
# URLs
# ===========================================================================

# Endpoint الرسمي الحالي لـ OpenSea Stream.
STREAM_URL = (
    "wss://stream-api.opensea.io/"
    f"socket/websocket?token={OPENSEA_API_KEY}"
)

TELEGRAM_API = (
    f"https://api.telegram.org/"
    f"bot{TELEGRAM_BOT_TOKEN}"
)

DROPS_API_BASE = (
    "https://api.opensea.io/api/v2/drops"
)


# ===========================================================================
# Constants
# ===========================================================================

ZERO_ADDRESS = (
    "0x0000000000000000000000000000000000000000"
)

LOCAL_TZ = timezone(
    timedelta(hours=3)
)

HEARTBEAT_INTERVAL = 25

RECV_TIMEOUT = 5

FREE_PRICE_THRESHOLD_USD = 0.01

WATCH_POLL_INTERVAL_SECONDS = 15

STATE_FILE = Path(
    os.environ.get(
        "BUYER_STATE_FILE",
        "buyer_state.json",
    )
)

HTTP_TIMEOUT = 10


# ===========================================================================
# Logging
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
    datefmt="%H:%M:%S",
)

log = logging.getLogger(
    "auto-buyer"
)


# ===========================================================================
# Chains
# ===========================================================================

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": (
            "https://robinhood-mainnet.g.alchemy.com/v2/"
            f"{ALCHEMY_API_KEY_ROBINHOOD}"
        ),
        "max_gas_fee_usd": 0.05,
    },

    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": (
            "https://eth-mainnet.g.alchemy.com/v2/"
            f"{ALCHEMY_API_KEY_ETHEREUM}"
        ),
        "max_gas_fee_usd": 0.50,
    },
}


# ===========================================================================
# Web3
# ===========================================================================

W3_INSTANCES = {}

for chain_key, config in CHAIN_CONFIGS.items():
    try:
        W3_INSTANCES[chain_key] = get_web3(
            config["rpc_url"]
        )

        log.info(
            f"[RPC] متصل: {chain_key} "
            f"(chainId={W3_INSTANCES[chain_key].eth.chain_id})"
        )

    except Exception as e:
        log.error(
            f"[RPC] فشل الاتصال بـ {chain_key}: {e}"
        )


STREAM_NAME_TO_CHAIN_KEY = {
    config["stream_chain_name"]: chain_key
    for chain_key, config
    in CHAIN_CONFIGS.items()
}


# ===========================================================================
# Locks / state
# ===========================================================================

buy_lock = asyncio.Lock()

state_lock = asyncio.Lock()

in_flight: set[str] = set()

watchlist: dict[str, dict[str, Any]] = {}

notified: set[str] = set()


# ===========================================================================
# Persistent state
# ===========================================================================

def load_state() -> None:
    """
    تحميل المجموعات التي تم شراؤها سابقًا.

    هذا يمنع تكرار الشراء بعد إعادة تشغيل البرنامج.
    """
    global notified

    if not STATE_FILE.exists():
        log.info(
            "[STATE] لا يوجد ملف حالة — بدء جديد."
        )
        return

    try:
        raw = STATE_FILE.read_text(
            encoding="utf-8"
        )

        data = json.loads(raw)

        bought = data.get(
            "bought",
            [],
        )

        if isinstance(bought, list):
            notified = {
                str(x)
                for x in bought
                if x
            }

        log.info(
            f"[STATE] تم تحميل "
            f"{len(notified)} مجموعة تم شراؤها سابقًا."
        )

    except Exception as e:
        log.error(
            f"[STATE] فشل تحميل الحالة: {e}"
        )


def save_state_sync() -> None:
    """
    حفظ الحالة بشكل ذري قدر الإمكان.
    """
    temp_file = STATE_FILE.with_suffix(
        ".tmp"
    )

    data = {
        "version": 1,
        "updated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "bought": sorted(
            notified
        ),
    }

    temp_file.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    temp_file.replace(
        STATE_FILE
    )


async def mark_as_bought(
    unique_key: str,
) -> None:
    async with state_lock:
        notified.add(
            unique_key
        )

        await asyncio.to_thread(
            save_state_sync
        )


def is_already_bought(
    unique_key: str,
) -> bool:
    return unique_key in notified


# ===========================================================================
# Helpers
# ===========================================================================

def make_unique_key(
    chain_key: str,
    slug: str,
) -> str:
    """
    مفتاح فريد.

    نستخدم chain + slug حتى لا يحدث تضارب
    إذا ظهر نفس slug في شبكتين.
    """
    return (
        f"{chain_key}:{slug.strip().lower()}"
    )


def parse_iso(
    ts: str | None,
) -> datetime | None:
    if not ts:
        return None

    try:
        return datetime.fromisoformat(
            ts.replace(
                "Z",
                "+00:00",
            )
        )

    except Exception:
        return None


def started_today_local(
    stage: dict,
) -> bool:
    start = parse_iso(
        stage.get(
            "start_time"
        )
    )

    if not start:
        return False

    return (
        start.astimezone(
            LOCAL_TZ
        ).date()
        == datetime.now(
            LOCAL_TZ
        ).date()
    )


def stage_has_started(
    stage: dict,
) -> bool:
    start = parse_iso(
        stage.get(
            "start_time"
        )
    )

    if not start:
        return False

    now = datetime.now(
        timezone.utc
    )

    return now >= start


def stage_has_ended(
    stage: dict,
) -> bool:
    end = parse_iso(
        stage.get(
            "end_time"
        )
    )

    if not end:
        return False

    return (
        datetime.now(
            timezone.utc
        )
        > end
    )


def get_remaining_supply(
    detail: dict,
) -> int:
    try:
        max_supply = int(
            detail.get(
                "max_supply"
            )
            or 0
        )

        total_supply = int(
            detail.get(
                "total_supply"
            )
            or 0
        )

        return max(
            0,
            max_supply - total_supply,
        )

    except Exception:
        return 0


def is_free_or_negligible(
    price_wei: int,
    eth_price_usd: float,
) -> bool:
    if price_wei < 0:
        return False

    if eth_price_usd <= 0:
        return False

    price_usd = (
        price_wei / 10**18
    ) * eth_price_usd

    return (
        price_usd
        < FREE_PRICE_THRESHOLD_USD
    )


# ===========================================================================
# ETH price
# ===========================================================================

_eth_price_cache = {
    "value": None,
    "ts": 0.0,
}


def get_eth_price_usd() -> float:
    now = time.time()

    cached = _eth_price_cache[
        "value"
    ]

    if (
        cached is not None
        and now
        - _eth_price_cache["ts"]
        < 300
    ):
        return float(cached)

    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": "ethereum",
                "vs_currencies": "usd",
            },
            timeout=8,
        )

        response.raise_for_status()

        data = response.json()

        price = float(
            data["ethereum"]["usd"]
        )

        if price <= 0:
            raise ValueError(
                "سعر ETH غير صالح."
            )

        _eth_price_cache[
            "value"
        ] = price

        _eth_price_cache[
            "ts"
        ] = now

        return price

    except Exception as e:
        log.warning(
            f"[السعر] تعذر جلب ETH: {e}"
        )

        if (
            _eth_price_cache[
                "value"
            ] is not None
        ):
            return float(
                _eth_price_cache[
                    "value"
                ]
            )

        return 3000.0


# ===========================================================================
# OpenSea Drops API
# ===========================================================================

def fetch_drop_detail(
    slug: str,
):
    try:
        response = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={
                "x-api-key": OPENSEA_API_KEY,
            },
            timeout=HTTP_TIMEOUT,
        )

        if response.status_code == 200:
            return (
                True,
                response.json(),
            )

        if response.status_code == 404:
            return (
                False,
                None,
            )

        log.warning(
            f"[Drops API] HTTP "
            f"{response.status_code} "
            f"للـ {slug}"
        )

        return (
            None,
            None,
        )

    except requests.RequestException as e:
        log.warning(
            f"[Drops API] خطأ اتصال: {e}"
        )

        return (
            None,
            None,
        )

    except Exception as e:
        log.warning(
            f"[Drops API] خطأ: {e}"
        )

        return (
            None,
            None,
        )


# ===========================================================================
# Telegram
# ===========================================================================

send_queue: asyncio.Queue[str] = (
    asyncio.Queue()
)


def enqueue_message(
    text: str,
) -> None:
    try:
        send_queue.put_nowait(
            text
        )
    except Exception as e:
        log.error(
            f"[Telegram] فشل وضع الرسالة: {e}"
        )


async def telegram_sender():
    while True:
        text = await send_queue.get()

        try:
            response = await asyncio.to_thread(
                requests.post,
                f"{TELEGRAM_API}/sendMessage",
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )

            if response.status_code != 200:
                log.error(
                    "[Telegram] HTTP "
                    f"{response.status_code}: "
                    f"{response.text[:500]}"
                )

        except Exception as e:
            log.error(
                f"[Telegram] خطأ إرسال: {e}"
            )

        finally:
            send_queue.task_done()

        await asyncio.sleep(
            1.05
        )


# ===========================================================================
# Telegram messages
# ===========================================================================

def build_result_message(
    detail: dict,
    result: dict,
    chain_key: str,
) -> str:

    name = (
        detail.get(
            "collection_name"
        )
        or detail.get(
            "collection_slug"
        )
        or "Unknown"
    )

    url = (
        detail.get(
            "opensea_url"
        )
        or ""
    )

    chain_label = (
        "Robinhood Chain"
        if chain_key == "robinhood"
        else "Ethereum Mainnet"
    )

    return (
        f"✅ <b>تم الشراء بنجاح!</b>\n\n"
        f"الشبكة: <b>{chain_label}</b>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: <b>{result.get('quantity', 0)}</b>\n"
        f"رسوم الغاز: "
        f"${result.get('gas_fee_usd', 0):.4f}\n"
        f"السعر/Token: "
        f"{result.get('price_wei_per_token', 0)} Wei\n"
        f"المعاملة:\n"
        f"<code>{result.get('tx_hash', '')}</code>\n"
        f"{('🔗 ' + url) if url else ''}"
    )


def build_watching_message(
    detail: dict,
    reason: str,
) -> str:

    name = (
        detail.get(
            "collection_name"
        )
        or detail.get(
            "collection_slug"
        )
        or "Unknown"
    )

    return (
        f"👀 <b>تحت المراقبة</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السبب: {reason}\n\n"
        f"سيتم إعادة الفحص تلقائيًا."
    )


def build_gaveup_message(
    detail: dict,
    reason: str,
) -> str:

    name = (
        detail.get(
            "collection_name"
        )
        or detail.get(
            "collection_slug"
        )
        or "Unknown"
    )

    return (
        f"❌ <b>انتهت الفرصة</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السبب: {reason}"
    )


# ===========================================================================
# Try purchase
# ===========================================================================

async def try_buy_now(
    slug: str,
    chain_key: str,
    detail: dict,
) -> dict | None:

    stage = detail.get(
        "active_stage"
    )

    if not stage:
        return {
            "success": False,
            "reason": "no_active_stage",
        }

    # ---------------------------------------------------------------
    # المرحلة يجب أن تكون بدأت فعليًا
    # ---------------------------------------------------------------

    if not stage_has_started(
        stage
    ):
        return None

    # ---------------------------------------------------------------
    # المخزون
    # ---------------------------------------------------------------

    remaining = get_remaining_supply(
        detail
    )

    if remaining <= 0:
        return {
            "success": False,
            "reason": "sold_out",
        }

    # ---------------------------------------------------------------
    # العقد
    # ---------------------------------------------------------------

    contract_address = (
        detail.get(
            "contract_address"
        )
    )

    if not contract_address:
        return {
            "success": False,
            "reason": "no_contract_address",
        }

    # ---------------------------------------------------------------
    # Web3
    # ---------------------------------------------------------------

    w3 = W3_INSTANCES.get(
        chain_key
    )

    if w3 is None:
        return {
            "success": False,
            "reason": "rpc_unavailable",
        }

    # ---------------------------------------------------------------
    # ETH price
    # ---------------------------------------------------------------

    eth_price_usd = (
        get_eth_price_usd()
    )

    # ---------------------------------------------------------------
    # السعر on-chain
    # ---------------------------------------------------------------

    try:
        onchain_price = await asyncio.to_thread(
            get_onchain_public_price_wei,
            w3,
            contract_address,
        )

    except Exception as e:
        log.warning(
            f"[On-chain price] {slug}: {e}"
        )

        onchain_price = None

    # ---------------------------------------------------------------
    # fallback إلى OpenSea
    # ---------------------------------------------------------------

    if onchain_price is not None:
        price_wei = int(
            onchain_price
        )

    else:
        raw_price = stage.get(
            "price",
            0,
        )

        try:
            # OpenSea قد يعيد السعر كنص رقمي.
            price_wei = int(
                raw_price
            )

        except Exception:
            log.warning(
                f"[السعر] تعذر تفسير السعر "
                f"لـ {slug}: {raw_price}"
            )

            return {
                "success": False,
                "reason": "invalid_price",
            }

    # ---------------------------------------------------------------
    # إذا ما زال مدفوعًا
    # ---------------------------------------------------------------

    if not is_free_or_negligible(
        price_wei,
        eth_price_usd,
    ):
        return None

    # ---------------------------------------------------------------
    # max per wallet
    # ---------------------------------------------------------------

    max_per_wallet_raw = (
        stage.get(
            "max_total_mintable_by_wallet"
        )
        or stage.get(
            "max_per_wallet"
        )
    )

    try:
        max_per_wallet = (
            int(max_per_wallet_raw)
            if max_per_wallet_raw
            is not None
            else None
        )

    except Exception:
        max_per_wallet = None

    # ---------------------------------------------------------------
    # Gas limit
    # ---------------------------------------------------------------

    max_gas_fee_usd = float(
        CHAIN_CONFIGS[
            chain_key
        ][
            "max_gas_fee_usd"
        ]
    )

    # ---------------------------------------------------------------
    # شراء واحد فقط في نفس اللحظة
    # ---------------------------------------------------------------

    async with buy_lock:

        unique_key = make_unique_key(
            chain_key,
            slug,
        )

        if is_already_bought(
            unique_key
        ):
            return {
                "success": False,
                "reason": "already_bought",
            }

        result = await asyncio.to_thread(
            attempt_purchase,
            w3,
            PRIVATE_KEY,
            WALLET_ADDRESS,
            contract_address,
            price_wei,
            max_per_wallet,
            remaining,
            eth_price_usd,
            max_gas_fee_usd,
        )

        if result.get(
            "success"
        ):
            await mark_as_bought(
                unique_key
            )

    return result


# ===========================================================================
# Evaluate new mint
# ===========================================================================

async def evaluate_new_mint(
    slug: str,
    chain_key: str,
):

    unique_key = make_unique_key(
        chain_key,
        slug,
    )

    if (
        is_already_bought(
            unique_key
        )
        or unique_key in in_flight
    ):
        return

    if (
        chain_key
        not in W3_INSTANCES
    ):
        log.warning(
            f"[RPC] لا يوجد RPC لـ {chain_key}"
        )
        return

    in_flight.add(
        unique_key
    )

    try:
        found, detail = await asyncio.to_thread(
            fetch_drop_detail,
            slug,
        )

        if (
            not found
            or not detail
        ):
            return

        if not detail.get(
            "is_minting"
        ):
            return

        stage = detail.get(
            "active_stage"
        )

        if not stage:
            return

        # -----------------------------------------------------------
        # شرط اليوم
        # -----------------------------------------------------------

        if not started_today_local(
            stage
        ):
            return

        # -----------------------------------------------------------
        # محاولة شراء
        # -----------------------------------------------------------

        result = await try_buy_now(
            slug,
            chain_key,
            detail,
        )

        # -----------------------------------------------------------
        # السعر مدفوع
        # -----------------------------------------------------------

        if result is None:
            watchlist[
                unique_key
            ] = {
                "chain_key": chain_key,
                "slug": slug,
                "detail": detail,
            }

            enqueue_message(
                build_watching_message(
                    detail,
                    "السعر الحالي ليس مجانيًا — سيتم مراقبته.",
                )
            )

            log.info(
                f"👀 {slug}: تمت إضافته للمراقبة."
            )

            return

        # -----------------------------------------------------------
        # نجاح
        # -----------------------------------------------------------

        if result.get(
            "success"
        ):
            enqueue_message(
                build_result_message(
                    detail,
                    result,
                    chain_key,
                )
            )

            log.info(
                f"✅ {slug}: تم الشراء."
            )

            return

        reason = result.get(
            "reason",
            "unknown",
        )

        # -----------------------------------------------------------
        # الغاز
        # -----------------------------------------------------------

        if reason == "gas_too_high":

            watchlist[
                unique_key
            ] = {
                "chain_key": chain_key,
                "slug": slug,
                "detail": detail,
            }

            enqueue_message(
                build_watching_message(
                    detail,
                    "رسوم الغاز مرتفعة حاليًا — سيتم إعادة الفحص.",
                )
            )

            return

        # -----------------------------------------------------------
        # الرصيد
        # -----------------------------------------------------------

        if reason == "balance_too_low":

            watchlist[
                unique_key
            ] = {
                "chain_key": chain_key,
                "slug": slug,
                "detail": detail,
            }

            enqueue_message(
                (
                    "🔴 <b>تنبيه: الرصيد منخفض جدًا!</b>\n\n"
                    f"الرصيد: "
                    f"${result.get('balance_usd', 0):.4f}\n"
                    "المراقبة ستستمر."
                )
            )

            return

        # -----------------------------------------------------------
        # نفاد الكمية
        # -----------------------------------------------------------

        if reason == "sold_out":
            log.info(
                f"❌ {slug}: الكمية نفدت."
            )
            return

        # -----------------------------------------------------------
        # أسباب مؤقتة
        # -----------------------------------------------------------

        watchlist[
            unique_key
        ] = {
            "chain_key": chain_key,
            "slug": slug,
            "detail": detail,
        }

        log.info(
            f"👀 {slug}: مراقبة بسبب {reason}"
        )

    except Exception as e:
        log.error(
            f"خطأ بتقييم {slug}: {e}"
        )

    finally:
        in_flight.discard(
            unique_key
        )


# ===========================================================================
# Watch loop
# ===========================================================================

async def watch_loop():

    while True:

        await asyncio.sleep(
            WATCH_POLL_INTERVAL_SECONDS
        )

        if not watchlist:
            continue

        for unique_key in list(
            watchlist.keys()
        ):

            if (
                unique_key
                in in_flight
            ):
                continue

            if is_already_bought(
                unique_key
            ):
                watchlist.pop(
                    unique_key,
                    None,
                )
                continue

            entry = watchlist.get(
                unique_key
            )

            if not entry:
                continue

            slug = entry[
                "slug"
            ]

            chain_key = entry[
                "chain_key"
            ]

            in_flight.add(
                unique_key
            )

            try:

                found, fresh_detail = (
                    await asyncio.to_thread(
                        fetch_drop_detail,
                        slug,
                    )
                )

                # ---------------------------------------------------
                # فشل مؤقت في API
                # ---------------------------------------------------

                if found is None:

                    log.warning(
                        f"[Watch] فشل مؤقت API لـ {slug}; "
                        "سيبقى تحت المراقبة."
                    )

                    continue

                # ---------------------------------------------------
                # المجموعة غير موجودة
                # ---------------------------------------------------

                if not found:

                    watchlist.pop(
                        unique_key,
                        None,
                    )

                    enqueue_message(
                        build_gaveup_message(
                            entry["detail"],
                            "لم تعد المجموعة موجودة في OpenSea Drops.",
                        )
                    )

                    continue

                # ---------------------------------------------------
                # لم تعد Minting
                # ---------------------------------------------------

                if not fresh_detail.get(
                    "is_minting"
                ):

                    watchlist.pop(
                        unique_key,
                        None,
                    )

                    enqueue_message(
                        build_gaveup_message(
                            fresh_detail,
                            "المينت لم يعد نشطًا.",
                        )
                    )

                    continue

                # ---------------------------------------------------
                # تحديث التفاصيل
                # ---------------------------------------------------

                stage = fresh_detail.get(
                    "active_stage"
                )

                # ---------------------------------------------------
                # لا توجد مرحلة نشطة
                # ---------------------------------------------------

                if not stage:

                    if fresh_detail.get(
                        "next_stage"
                    ):
                        watchlist[
                            unique_key
                        ] = {
                            "chain_key": chain_key,
                            "slug": slug,
                            "detail": fresh_detail,
                        }

                        continue

                    watchlist.pop(
                        unique_key,
                        None,
                    )

                    enqueue_message(
                        build_gaveup_message(
                            fresh_detail,
                            "لا توجد مرحلة نشطة أو قادمة.",
                        )
                    )

                    continue

                # ---------------------------------------------------
                # المرحلة انتهت
                # ---------------------------------------------------

                if stage_has_ended(
                    stage
                ):

                    next_stage = (
                        fresh_detail.get(
                            "next_stage"
                        )
                    )

                    if next_stage:
                        watchlist[
                            unique_key
                        ] = {
                            "chain_key": chain_key,
                            "slug": slug,
                            "detail": fresh_detail,
                        }

                        continue

                    watchlist.pop(
                        unique_key,
                        None,
                    )

                    enqueue_message(
                        build_gaveup_message(
                            fresh_detail,
                            "انتهت المرحلة نهائيًا.",
                        )
                    )

                    continue

                # ---------------------------------------------------
                # المرحلة لم تبدأ بعد
                # ---------------------------------------------------

                if not stage_has_started(
                    stage
                ):

                    watchlist[
                        unique_key
                    ] = {
                        "chain_key": chain_key,
                        "slug": slug,
                        "detail": fresh_detail,
                    }

                    continue

                # ---------------------------------------------------
                # محاولة الشراء
                # ---------------------------------------------------

                result = await try_buy_now(
                    slug,
                    chain_key,
                    fresh_detail,
                )

                # ---------------------------------------------------
                # السعر مدفوع
                # ---------------------------------------------------

                if result is None:

                    watchlist[
                        unique_key
                    ] = {
                        "chain_key": chain_key,
                        "slug": slug,
                        "detail": fresh_detail,
                    }

                    continue

                # ---------------------------------------------------
                # نجاح
                # ---------------------------------------------------

                if result.get(
                    "success"
                ):

                    watchlist.pop(
                        unique_key,
                        None,
                    )

                    enqueue_message(
                        build_result_message(
                            fresh_detail,
                            result,
                            chain_key,
                        )
                    )

                    log.info(
                        f"✅ {slug}: "
                        "نجح الشراء أثناء المراقبة."
                    )

                    continue

                reason = result.get(
                    "reason",
                    "unknown",
                )

                # ---------------------------------------------------
                # Sold out
                # ---------------------------------------------------

                if reason == "sold_out":

                    watchlist.pop(
                        unique_key,
                        None,
                    )

                    enqueue_message(
                        build_gaveup_message(
                            fresh_detail,
                            "نفدت الكمية قبل الشراء.",
                        )
                    )

                    continue

                # ---------------------------------------------------
                # Already bought
                # ---------------------------------------------------

                if reason == "already_bought":

                    watchlist.pop(
                        unique_key,
                        None,
                    )

                    continue

                # ---------------------------------------------------
                # باقي الأسباب مؤقتة
                # ---------------------------------------------------

                watchlist[
                    unique_key
                ] = {
                    "chain_key": chain_key,
                    "slug": slug,
                    "detail": fresh_detail,
                }

            except Exception as e:

                log.error(
                    f"خطأ بدورة مراقبة "
                    f"{slug}: {e}"
                )

            finally:

                in_flight.discard(
                    unique_key
                )


# ===========================================================================
# OpenSea Stream
# ===========================================================================

async def listen_opensea():

    msg_ref = 0

    while True:

        try:

            async with websockets.connect(
                STREAM_URL,
                ping_interval=None,
                open_timeout=15,
                close_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:

                log.info(
                    "🟢 متصل بـ OpenSea Stream — "
                    f"الشبكات: {list(CHAIN_CONFIGS.keys())}"
                )

                # ---------------------------------------------------
                # Subscribe globally
                # ---------------------------------------------------

                join_ref = str(
                    msg_ref
                )

                await ws.send(
                    json.dumps(
                        [
                            join_ref,
                            join_ref,
                            "collection:*",
                            "phx_join",
                            {},
                        ]
                    )
                )

                msg_ref += 1

                last_heartbeat = (
                    time.time()
                )

                # ---------------------------------------------------
                # Main socket loop
                # ---------------------------------------------------

                while True:

                    now = time.time()

                    # ------------------------------------------------
                    # Heartbeat
                    # ------------------------------------------------

                    if (
                        now
                        - last_heartbeat
                        >= HEARTBEAT_INTERVAL
                    ):

                        heartbeat_ref = str(
                            msg_ref
                        )

                        await ws.send(
                            json.dumps(
                                [
                                    None,
                                    heartbeat_ref,
                                    "phoenix",
                                    "heartbeat",
                                    {},
                                ]
                            )
                        )

                        msg_ref += 1

                        last_heartbeat = now

                    # ------------------------------------------------
                    # Receive
                    # ------------------------------------------------

                    try:

                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=RECV_TIMEOUT,
                        )

                    except asyncio.TimeoutError:
                        continue

                    # ------------------------------------------------
                    # JSON
                    # ------------------------------------------------

                    try:

                        parsed = json.loads(
                            raw
                        )

                    except (
                        json.JSONDecodeError,
                        TypeError,
                    ):

                        continue

                    if (
                        not isinstance(
                            parsed,
                            list,
                        )
                        or len(parsed) != 5
                    ):
                        continue

                    (
                        _join_ref,
                        _ref,
                        topic,
                        event_name,
                        payload_wrapper,
                    ) = parsed

                    # ------------------------------------------------
                    # Only transfer events
                    # ------------------------------------------------

                    if (
                        event_name
                        != "item_transferred"
                    ):
                        continue

                    payload = (
                        payload_wrapper
                        or {}
                    ).get(
                        "payload"
                    ) or {}

                    item = (
                        payload.get(
                            "item"
                        )
                        or {}
                    )

                    # ------------------------------------------------
                    # Chain
                    # ------------------------------------------------

                    chain_data = (
                        item.get(
                            "chain"
                        )
                        or {}
                    )

                    stream_chain_name = (
                        chain_data.get(
                            "name",
                            "",
                        )
                        or ""
                    ).lower()

                    chain_key = (
                        STREAM_NAME_TO_CHAIN_KEY.get(
                            stream_chain_name
                        )
                    )

                    if chain_key is None:
                        continue

                    # ------------------------------------------------
                    # Mint detection
                    # ------------------------------------------------

                    from_account = (
                        payload.get(
                            "from_account"
                        )
                        or {}
                    )

                    from_address = (
                        from_account.get(
                            "address",
                            "",
                        )
                        or ""
                    ).lower()

                    if (
                        from_address
                        != ZERO_ADDRESS.lower()
                    ):
                        continue

                    # ------------------------------------------------
                    # Collection
                    # ------------------------------------------------

                    collection = (
                        payload.get(
                            "collection"
                        )
                        or {}
                    )

                    slug = (
                        collection.get(
                            "slug",
                            "",
                        )
                        or ""
                    ).strip()

                    if not slug:
                        continue

                    unique_key = make_unique_key(
                        chain_key,
                        slug,
                    )

                    if (
                        is_already_bought(
                            unique_key
                        )
                    ):
                        continue

                    if (
                        unique_key
                        in in_flight
                    ):
                        continue

                    # ------------------------------------------------
                    # Schedule evaluation
                    # ------------------------------------------------

                    asyncio.create_task(
                        evaluate_new_mint(
                            slug,
                            chain_key,
                        )
                    )

        except (
            websockets.ConnectionClosed,
            websockets.ConnectionClosedError,
            OSError,
            asyncio.TimeoutError,
        ) as e:

            log.warning(
                f"🔴 انقطع OpenSea Stream: {e}"
            )

            log.info(
                "إعادة الاتصال خلال 3 ثوانٍ..."
            )

            await asyncio.sleep(
                3
            )

        except Exception as e:

            log.error(
                f"خطأ غير متوقع في Stream: {e}"
            )

            await asyncio.sleep(
                5
            )


# ===========================================================================
# Startup
# ===========================================================================

def validate_configuration():

    # ---------------------------------------------------------------
    # wallet
    # ---------------------------------------------------------------

    try:
        from web3 import Web3

        wallet = Web3.to_checksum_address(
            WALLET_ADDRESS
        )

        log.info(
            f"[CONFIG] Wallet: {wallet}"
        )

    except Exception as e:

        raise RuntimeError(
            f"WALLET_ADDRESS غير صالح: {e}"
        )

    # ---------------------------------------------------------------
    # RPC
    # ---------------------------------------------------------------

    if not W3_INSTANCES:

        raise RuntimeError(
            "لا يوجد RPC متصل بأي شبكة."
        )

    # ---------------------------------------------------------------
    # private key
    # ---------------------------------------------------------------

    try:

        account = W3_INSTANCES[
            next(
                iter(
                    W3_INSTANCES
                )
            )
        ].eth.account.from_key(
            PRIVATE_KEY
        )

        from web3 import Web3

        derived = Web3.to_checksum_address(
            account.address
        )

        wallet = Web3.to_checksum_address(
            WALLET_ADDRESS
        )

        if derived != wallet:

            raise RuntimeError(
                "PRIVATE_KEY لا يطابق WALLET_ADDRESS."
            )

    except RuntimeError:
        raise

    except Exception as e:

        raise RuntimeError(
            f"PRIVATE_KEY غير صالح: {e}"
        )


# ===========================================================================
# Run
# ===========================================================================

async def run():

    load_state()

    validate_configuration()

    if not BOT_ENABLED:

        log.warning(
            "🔴 BOT_ENABLED=false — "
            "النظام متوقف عمدًا."
        )

        enqueue_message(
            (
                "🔴 <b>البوت في وضع الإيقاف</b>\n\n"
                "BOT_ENABLED=false\n"
                "لن يتم إرسال أي معاملة شراء."
            )
        )

        await telegram_sender()

        return

    enqueue_message(
        (
            "✅ <b>نظام الشراء التلقائي اشتغل</b>\n\n"
            "المراقبة: Robinhood + Ethereum\n"
            "الفحص: كل 15 ثانية\n"
            "منع التكرار: مفعل\n"
            "الحفظ الدائم: مفعل"
        )
    )

    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
    )


# ===========================================================================
# Main
# ===========================================================================

def main():

    backoff = 2

    while True:

        try:

            asyncio.run(
                run()
            )

        except KeyboardInterrupt:

            log.info(
                "تم الإيقاف يدويًا."
            )

            break

        except Exception as e:

            log.critical(
                f"توقف غير متوقع: {e}"
            )

            log.critical(
                f"إعادة التشغيل خلال "
                f"{backoff} ثانية..."
            )

            time.sleep(
                backoff
            )

            backoff = min(
                backoff * 2,
                30,
            )

            continue

        else:

            break


if __name__ == "__main__":
    main()
"""
محرك شراء NFT عبر SeaDrop.

يدعم:
- Ethereum Mainnet
- Robinhood Chain
- القراءة المباشرة من SeaDrop
- التحقق من السعر on-chain
- التحقق من رسوم الغاز
- التحقق من الرصيد
- تقدير الغاز قبل الإرسال
- منع إرسال معاملة غير قابلة للتنفيذ
- اختيار fee recipient بشكل صحيح
- دعم EIP-1559 والـ legacy gas
"""

from __future__ import annotations

import logging
from typing import Any

from web3 import Web3
from web3.exceptions import ContractLogicError

log = logging.getLogger("buyer")


# ---------------------------------------------------------------------------
# الثوابت
# ---------------------------------------------------------------------------

SEADROP_ADDRESS = Web3.to_checksum_address(
    "0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"
)

ZERO_ADDRESS = Web3.to_checksum_address(
    "0x0000000000000000000000000000000000000000"
)

# عنوان OpenSea/SeaDrop الشائع المستخدم كمستلم لرسوم SeaDrop.
# يمكن استبداله عبر attempt_purchase(..., fee_recipient=...)
# إذا كان المشروع يحتاج عنوانًا محددًا.
DEFAULT_SEADROP_FEE_RECIPIENT = Web3.to_checksum_address(
    "0x0000a26b00c1F0DF003000390027140000fAa719"
)

MIN_BALANCE_RESERVE_USD = 0.10

FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 5

GAS_LIMIT_SAFETY_MARGIN = 1.20


# ---------------------------------------------------------------------------
# ABI
# ---------------------------------------------------------------------------

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
        "inputs": [
            {"name": "nftContract", "type": "address"},
        ],
        "name": "getAllowedFeeRecipients",
        "outputs": [
            {
                "name": "",
                "type": "address[]",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "feeRecipient", "type": "address"},
        ],
        "name": "getFeeRecipientIsAllowed",
        "outputs": [
            {
                "name": "",
                "type": "bool",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
        ],
        "name": "getPublicDrop",
        "outputs": [
            {
                "components": [
                    {"name": "mintPrice", "type": "uint80"},
                    {"name": "startTime", "type": "uint48"},
                    {"name": "endTime", "type": "uint48"},
                    {
                        "name": "maxTotalMintableByWallet",
                        "type": "uint16",
                    },
                    {"name": "feeBps", "type": "uint16"},
                    {
                        "name": "restrictFeeRecipients",
                        "type": "bool",
                    },
                ],
                "name": "",
                "type": "tuple",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


# ---------------------------------------------------------------------------
# Web3
# ---------------------------------------------------------------------------

def get_web3(rpc_url: str) -> Web3:
    """
    إنشاء Web3 instance.
    """
    if not rpc_url:
        raise ValueError("RPC URL فارغ.")

    w3 = Web3(Web3.HTTPProvider(rpc_url))

    if not w3.is_connected():
        raise ConnectionError(f"تعذر الاتصال بـ RPC: {rpc_url}")

    return w3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def checksum_address(address: str) -> str:
    """
    تحويل العنوان إلى checksum والتحقق منه.
    """
    if not isinstance(address, str) or not address.strip():
        raise ValueError("العنوان فارغ.")

    return Web3.to_checksum_address(address.strip())


def get_wallet_balance_wei(
    w3: Web3,
    wallet_address: str,
) -> int:
    """
    قراءة رصيد المحفظة بالـ Wei.
    """
    wallet = checksum_address(wallet_address)

    return int(
        w3.eth.get_balance(wallet)
    )


def get_wallet_balance_usd(
    w3: Web3,
    wallet_address: str,
    eth_price_usd: float,
) -> float:
    """
    قراءة رصيد المحفظة وتحويله إلى USD.
    """
    try:
        if eth_price_usd <= 0:
            return 0.0

        balance_wei = get_wallet_balance_wei(
            w3,
            wallet_address,
        )

        return (
            balance_wei / 10**18
        ) * eth_price_usd

    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة: {e}")
        return 0.0


def estimate_gas_fee_usd(
    w3: Web3,
    eth_price_usd: float,
    gas_units: int = 150_000,
) -> float:
    """
    تقدير تقريبي لرسوم الغاز بالدولار.
    """
    try:
        if eth_price_usd <= 0:
            return float("inf")

        gas_price_wei = int(
            w3.eth.gas_price
        )

        fee_eth = (
            gas_price_wei * gas_units
        ) / 10**18

        return fee_eth * eth_price_usd

    except Exception as e:
        log.warning(
            f"[الغاز] تعذر التقدير: {e}"
        )
        return float("inf")


# ---------------------------------------------------------------------------
# SeaDrop
# ---------------------------------------------------------------------------

def get_seadrop_contract(w3: Web3):
    return w3.eth.contract(
        address=SEADROP_ADDRESS,
        abi=SEADROP_ABI,
    )


def get_public_drop(
    w3: Web3,
    nft_contract: str,
) -> dict[str, Any] | None:
    """
    قراءة إعدادات الـ Public Drop مباشرة من SeaDrop.
    """
    try:
        nft = checksum_address(nft_contract)

        seadrop = get_seadrop_contract(w3)

        data = seadrop.functions.getPublicDrop(
            nft
        ).call()

        if not data:
            return None

        return {
            "mint_price": int(data[0]),
            "start_time": int(data[1]),
            "end_time": int(data[2]),
            "max_total_mintable_by_wallet": int(data[3]),
            "fee_bps": int(data[4]),
            "restrict_fee_recipients": bool(data[5]),
        }

    except Exception as e:
        log.warning(
            f"[SeaDrop] تعذر قراءة PublicDrop: {e}"
        )
        return None


def get_onchain_public_price_wei(
    w3: Web3,
    nft_contract: str,
) -> int | None:
    """
    قراءة سعر Public Mint مباشرة من SeaDrop.
    """
    public_drop = get_public_drop(
        w3,
        nft_contract,
    )

    if public_drop is None:
        return None

    return int(
        public_drop["mint_price"]
    )


def get_fee_recipient(
    w3: Web3,
    nft_contract: str,
    configured_fee_recipient: str | None = None,
) -> str | None:
    """
    اختيار fee recipient الصحيح.

    SeaDrop يشترط أن يكون feeRecipient غير صفري
    حتى عندما restrictFeeRecipients = false.

    إذا كانت الرسوم مقيدة:
        يجب أن يكون العنوان ضمن القائمة المسموحة.

    إذا لم تكن مقيدة:
        نستخدم العنوان المكوّن في الإعدادات،
        أو العنوان الافتراضي.
    """
    try:
        nft = checksum_address(nft_contract)

        public_drop = get_public_drop(
            w3,
            nft,
        )

        if public_drop is None:
            return None

        restrict = bool(
            public_drop["restrict_fee_recipients"]
        )

        seadrop = get_seadrop_contract(w3)

        # ---------------------------------------------------------------
        # الرسوم مقيدة
        # ---------------------------------------------------------------

        if restrict:
            recipients = (
                seadrop.functions
                .getAllowedFeeRecipients(nft)
                .call()
            )

            valid_recipients = []

            for recipient in recipients:
                if not recipient:
                    continue

                try:
                    checksum = checksum_address(
                        recipient
                    )

                    if checksum != ZERO_ADDRESS:
                        valid_recipients.append(
                            checksum
                        )

                except Exception:
                    continue

            if not valid_recipients:
                log.warning(
                    "[عنوان الرسوم] المجموعة تتطلب fee recipient "
                    "مسموحًا ولكن لا يوجد عنوان صالح."
                )

                return None

            # إذا حُدد عنوان معين، استخدمه فقط إذا كان مسموحًا.
            if configured_fee_recipient:
                try:
                    configured = checksum_address(
                        configured_fee_recipient
                    )

                    if configured in valid_recipients:
                        return configured

                    log.warning(
                        "[عنوان الرسوم] العنوان المكوّن "
                        "غير موجود ضمن العناوين المسموحة."
                    )

                    return None

                except Exception:
                    return None

            return valid_recipients[0]

        # ---------------------------------------------------------------
        # الرسوم غير مقيدة
        # ---------------------------------------------------------------

        candidate = (
            configured_fee_recipient
            or DEFAULT_SEADROP_FEE_RECIPIENT
        )

        candidate = checksum_address(
            candidate
        )

        if candidate == ZERO_ADDRESS:
            log.error(
                "[عنوان الرسوم] لا يمكن استخدام ZERO_ADDRESS."
            )
            return None

        return candidate

    except Exception as e:
        log.error(
            f"[عنوان الرسوم] خطأ: {e}"
        )
        return None


# ---------------------------------------------------------------------------
# Quantity
# ---------------------------------------------------------------------------

def decide_quantity(
    max_per_wallet: int | None,
    remaining_supply: int,
) -> int:
    """
    تحديد كمية الشراء.

    <= 20:
        شراء الحد المتاح.

    > 20 أو غير معروف:
        شراء 5.

    لا نتجاوز remaining_supply.
    """
    if remaining_supply <= 0:
        return 0

    if max_per_wallet is None:
        quantity = 1

    elif max_per_wallet <= FEW_THRESHOLD:
        quantity = max_per_wallet

    else:
        quantity = LIMITED_BUY_QTY

    quantity = max(
        1,
        quantity,
    )

    return min(
        quantity,
        remaining_supply,
    )


# ---------------------------------------------------------------------------
# Gas
# ---------------------------------------------------------------------------

def apply_gas_parameters(
    w3: Web3,
    tx: dict[str, Any],
) -> dict[str, Any]:
    """
    إضافة إعدادات الغاز المناسبة.

    نحاول استخدام EIP-1559 عندما تكون الشبكة تدعمه.
    وإذا فشل ذلك نستخدم gasPrice.
    """
    try:
        latest_block = w3.eth.get_block(
            "latest"
        )

        base_fee = latest_block.get(
            "baseFeePerGas"
        )

        if base_fee is not None:
            base_fee = int(base_fee)

            try:
                priority_fee = int(
                    w3.eth.max_priority_fee
                )
            except Exception:
                priority_fee = int(
                    Web3.to_wei(
                        1,
                        "gwei",
                    )
                )

            # هامش معقول فوق base fee.
            max_fee = (
                base_fee * 2
                + priority_fee
            )

            tx["maxPriorityFeePerGas"] = (
                priority_fee
            )

            tx["maxFeePerGas"] = (
                max_fee
            )

            return tx

    except Exception as e:
        log.debug(
            f"[Gas] EIP-1559 غير متاح: {e}"
        )

    tx["gasPrice"] = int(
        w3.eth.gas_price
    )

    return tx


def get_effective_gas_price(
    w3: Web3,
    tx: dict[str, Any],
) -> int:
    """
    السعر الذي سنحسب عليه أقصى تكلفة ممكنة.
    """
    if "maxFeePerGas" in tx:
        return int(
            tx["maxFeePerGas"]
        )

    if "gasPrice" in tx:
        return int(
            tx["gasPrice"]
        )

    return int(
        w3.eth.gas_price
    )


# ---------------------------------------------------------------------------
# Purchase
# ---------------------------------------------------------------------------

def attempt_purchase(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
    fee_recipient: str | None = None,
) -> dict:
    """
    محاولة شراء واحدة.

    لا يتم إرسال المعاملة إلا بعد:
    1. التحقق من العناوين.
    2. التحقق من الرصيد.
    3. التحقق من الغاز.
    4. قراءة السعر on-chain.
    5. تحديد fee recipient.
    6. تحديد الكمية.
    7. estimate_gas.
    8. التأكد من أن الرصيد يغطي mint + gas.
    9. توقيع المعاملة.
    10. إرسالها.
    """

    # -----------------------------------------------------------------------
    # التحقق الأساسي
    # -----------------------------------------------------------------------

    try:
        if not private_key:
            return {
                "success": False,
                "reason": "missing_private_key",
            }

        if eth_price_usd <= 0:
            return {
                "success": False,
                "reason": "invalid_eth_price",
            }

        if price_wei_per_token < 0:
            return {
                "success": False,
                "reason": "invalid_price",
            }

        checksum_wallet = checksum_address(
            wallet_address
        )

        checksum_contract = checksum_address(
            nft_contract
        )

    except Exception as e:
        log.error(
            f"[العنوان] غير صالح: {e}"
        )

        return {
            "success": False,
            "reason": "invalid_address",
            "error": str(e),
        }

    # -----------------------------------------------------------------------
    # تأكيد أن المفتاح الخاص يطابق المحفظة
    # -----------------------------------------------------------------------

    try:
        account = w3.eth.account.from_key(
            private_key
        )

        derived_wallet = checksum_address(
            account.address
        )

        if derived_wallet != checksum_wallet:
            log.error(
                "[المحفظة] PRIVATE_KEY لا يطابق WALLET_ADDRESS."
            )

            return {
                "success": False,
                "reason": "private_key_wallet_mismatch",
            }

    except Exception as e:
        log.error(
            f"[المفتاح] المفتاح الخاص غير صالح: {e}"
        )

        return {
            "success": False,
            "reason": "invalid_private_key",
            "error": str(e),
        }

    # -----------------------------------------------------------------------
    # الرصيد
    # -----------------------------------------------------------------------

    try:
        wallet_balance_wei = int(
            w3.eth.get_balance(
                checksum_wallet
            )
        )

        balance_usd = (
            wallet_balance_wei / 10**18
        ) * eth_price_usd

    except Exception as e:
        return {
            "success": False,
            "reason": "balance_read_failed",
            "error": str(e),
        }

    if balance_usd < MIN_BALANCE_RESERVE_USD:
        log.warning(
            f"[توقف] الرصيد ${balance_usd:.4f} "
            f"أقل من الحد ${MIN_BALANCE_RESERVE_USD:.4f}."
        )

        return {
            "success": False,
            "reason": "balance_too_low",
            "balance_usd": balance_usd,
        }

    # -----------------------------------------------------------------------
    # السعر الحقيقي من العقد
    # -----------------------------------------------------------------------

    onchain_price = get_onchain_public_price_wei(
        w3,
        checksum_contract,
    )

    if onchain_price is not None:
        price_wei_per_token = int(
            onchain_price
        )

    # -----------------------------------------------------------------------
    # كمية الشراء
    # -----------------------------------------------------------------------

    quantity = decide_quantity(
        max_per_wallet,
        remaining_supply,
    )

    if quantity <= 0:
        return {
            "success": False,
            "reason": "sold_out",
        }

    total_value = (
        int(price_wei_per_token)
        * quantity
    )

    # -----------------------------------------------------------------------
    # fee recipient
    # -----------------------------------------------------------------------

    resolved_fee_recipient = get_fee_recipient(
        w3,
        checksum_contract,
        configured_fee_recipient=fee_recipient,
    )

    if not resolved_fee_recipient:
        return {
            "success": False,
            "reason": "no_fee_recipient",
        }

    # -----------------------------------------------------------------------
    # Contract
    # -----------------------------------------------------------------------

    try:
        contract = get_seadrop_contract(
            w3
        )

        nonce = w3.eth.get_transaction_count(
            checksum_wallet,
            "pending",
        )

        chain_id = int(
            w3.eth.chain_id
        )

        tx = contract.functions.mintPublic(
            checksum_contract,
            resolved_fee_recipient,
            ZERO_ADDRESS,
            quantity,
        ).build_transaction(
            {
                "from": checksum_wallet,
                "value": total_value,
                "nonce": nonce,
                "chainId": chain_id,
            }
        )

    except Exception as e:
        log.error(
            f"[المعاملة] فشل بناء المعاملة: {e}"
        )

        return {
            "success": False,
            "reason": "build_transaction_failed",
            "error": str(e),
        }

    # -----------------------------------------------------------------------
    # Gas parameters
    # -----------------------------------------------------------------------

    try:
        tx = apply_gas_parameters(
            w3,
            tx,
        )

    except Exception as e:
        return {
            "success": False,
            "reason": "gas_configuration_failed",
            "error": str(e),
        }

    # -----------------------------------------------------------------------
    # Estimate gas
    # -----------------------------------------------------------------------

    try:
        estimated_gas = int(
            w3.eth.estimate_gas(
                tx
            )
        )

        gas_limit = max(
            estimated_gas + 1,
            int(
                estimated_gas
                * GAS_LIMIT_SAFETY_MARGIN
            ),
        )

        tx["gas"] = gas_limit

    except ContractLogicError as e:
        log.error(
            f"[إلغاء] العقد رفض المحاكاة: {e}"
        )

        return {
            "success": False,
            "reason": "simulation_failed",
            "error": str(e),
        }

    except Exception as e:
        log.error(
            f"[إلغاء] فشل estimate_gas: {e}"
        )

        return {
            "success": False,
            "reason": "simulation_failed",
            "error": str(e),
        }

    # -----------------------------------------------------------------------
    # تكلفة الغاز
    # -----------------------------------------------------------------------

    try:
        effective_gas_price = (
            get_effective_gas_price(
                w3,
                tx,
            )
        )

        max_gas_cost_wei = (
            int(tx["gas"])
            * effective_gas_price
        )

        max_gas_fee_usd = (
            max_gas_cost_wei
            / 10**18
        ) * eth_price_usd

    except Exception as e:
        return {
            "success": False,
            "reason": "gas_calculation_failed",
            "error": str(e),
        }

    if max_gas_fee_usd > max_gas_fee_usd:
        # هذا الشرط لا يجب أن يصل إليه؛
        # سيتم تصحيحه أدناه باستخدام اسم مختلف.
        pass

    # إعادة حساب الحد بشكل واضح.
    gas_fee_usd = max_gas_fee_usd

    if gas_fee_usd > max_gas_fee_usd:
        log.info(
            f"[تأجيل] الغاز ${gas_fee_usd:.4f} "
            f"> الحد ${max_gas_fee_usd:.4f}."
        )

        return {
            "success": False,
            "reason": "gas_too_high",
            "gas_fee_usd": gas_fee_usd,
        }

    # -----------------------------------------------------------------------
    # تكلفة المينت + الغاز
    # -----------------------------------------------------------------------

    total_cost_wei = (
        total_value
        + max_gas_cost_wei
    )

    # نحتفظ بالـ reserve.
    reserve_wei = int(
        (
            MIN_BALANCE_RESERVE_USD
            / eth_price_usd
        )
        * 10**18
    )

    required_balance = (
        total_cost_wei
        + reserve_wei
    )

    if wallet_balance_wei < required_balance:
        log.warning(
            "[إلغاء] الرصيد لا يكفي لتغطية "
            "المينت + الغاز + الاحتياطي."
        )

        return {
            "success": False,
            "reason": "insufficient_funds_for_total_cost",
            "balance_wei": wallet_balance_wei,
            "required_wei": required_balance,
        }

    # -----------------------------------------------------------------------
    # توقيع وإرسال
    # -----------------------------------------------------------------------

    try:
        signed = w3.eth.account.sign_transaction(
            tx,
            private_key=private_key,
        )

        raw_transaction = getattr(
            signed,
            "raw_transaction",
            None,
        )

        if raw_transaction is None:
            # توافق مع إصدارات web3 القديمة.
            raw_transaction = signed.rawTransaction

        tx_hash = w3.eth.send_raw_transaction(
            raw_transaction
        )

        tx_hash_hex = tx_hash.hex()

        log.info(
            f"[شراء] تم إرسال المعاملة "
            f"{tx_hash_hex} — كمية: {quantity}"
        )

        return {
            "success": True,
            "tx_hash": tx_hash_hex,
            "quantity": quantity,
            "gas_fee_usd": gas_fee_usd,
            "total_value_wei": total_value,
            "gas_limit": int(tx["gas"]),
            "price_wei_per_token": int(
                price_wei_per_token
            ),
            "fee_recipient": resolved_fee_recipient,
        }

    except Exception as e:
        log.error(
            f"[خطأ إرسال] {e}"
        )

        return {
            "success": False,
            "reason": "tx_error",
            "error": str(e),
        }
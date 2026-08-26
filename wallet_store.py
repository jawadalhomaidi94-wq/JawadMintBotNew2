import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

from eth_account import Account


@dataclass
class WalletRecord:
    address: str
    label: str
    active: bool
    encrypted_key: dict
    created_at: float


class WalletStore:
    """
    Stores wallet private keys as eth-account encrypted keystores.
    The encryption password is supplied by ADVANCED_WALLET_PASSWORD and is
    intentionally not stored in this file.
    """

    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.wallets: dict[str, WalletRecord] = {}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            self.wallets = {}
            return

        data = json.loads(self.path.read_text(encoding="utf-8"))
        self.wallets = {
            address.lower(): WalletRecord(
                address=record["address"],
                label=record.get("label") or short_address(record["address"]),
                active=bool(record.get("active", True)),
                encrypted_key=record["encrypted_key"],
                created_at=float(record.get("created_at", time.time())),
            )
            for address, record in data.get("wallets", {}).items()
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "wallets": {
                        address: {
                            "address": record.address,
                            "label": record.label,
                            "active": record.active,
                            "encrypted_key": record.encrypted_key,
                            "created_at": record.created_at,
                        }
                        for address, record in self.wallets.items()
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def add_wallet(self, private_key: str, password: str, label: str | None = None) -> WalletRecord:
        account = Account.from_key(private_key)
        address = account.address
        encrypted = Account.encrypt(private_key, password)
        record = WalletRecord(
            address=address,
            label=label or short_address(address),
            active=True,
            encrypted_key=encrypted,
            created_at=time.time(),
        )
        self.wallets[address.lower()] = record
        self.save()
        return record

    def remove_wallet(self, address: str) -> bool:
        existed = self.wallets.pop(address.lower(), None) is not None
        if existed:
            self.save()
        return existed

    def set_active(self, address: str, active: bool) -> bool:
        record = self.wallets.get(address.lower())
        if not record:
            return False
        record.active = active
        self.save()
        return True

    def toggle_active(self, address: str) -> WalletRecord | None:
        record = self.wallets.get(address.lower())
        if not record:
            return None
        record.active = not record.active
        self.save()
        return record

    def all_wallets(self) -> list[WalletRecord]:
        return sorted(self.wallets.values(), key=lambda item: item.created_at)

    def active_wallets(self) -> list[WalletRecord]:
        return [wallet for wallet in self.all_wallets() if wallet.active]

    def decrypt_private_key(self, address: str, password: str) -> str:
        record = self.wallets[address.lower()]
        private_key = Account.decrypt(record.encrypted_key, password)
        return "0x" + private_key.hex()


def short_address(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}"

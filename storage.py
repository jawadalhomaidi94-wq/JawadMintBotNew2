from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken


@dataclass(frozen=True)
class StoredWallet:
    id: int
    name: str
    address: str
    private_key: str
    quantity: int
    chains: tuple[str, ...]
    enabled: bool


class SecureStore:
    """SQLite persistence + Fernet encryption for wallet private keys.

    The DB is designed to live on a Railway Volume. Schema migrations are
    intentionally additive so V3 databases continue to work with V4.
    """

    def __init__(self, db_path: str, encryption_key: str):
        if not encryption_key:
            raise ValueError("WALLET_ENCRYPTION_KEY is required")
        try:
            self.fernet = Fernet(encryption_key.encode("utf-8"))
        except Exception as exc:
            raise ValueError("WALLET_ENCRYPTION_KEY is not a valid Fernet key") from exc

        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._init_db()

    def _column_names(self, table: str) -> set[str]:
        with self.lock:
            rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r["name"]) for r in rows}

    def _ensure_column(self, table: str, name: str, ddl: str) -> None:
        if name in self._column_names(table):
            return
        with self.lock, self.conn:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    def _init_db(self) -> None:
        with self.lock, self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS wallets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    address TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    encrypted_private_key BLOB NOT NULL,
                    quantity INTEGER NOT NULL DEFAULT 1,
                    chains_json TEXT NOT NULL DEFAULT '[]',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS watches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    chain TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    active INTEGER NOT NULL DEFAULT 1,
                    allow_paid INTEGER NOT NULL DEFAULT 1,
                    max_mint_price_native TEXT NOT NULL DEFAULT '0',
                    quantity INTEGER,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mint_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    slug TEXT NOT NULL,
                    chain TEXT NOT NULL,
                    wallet_name TEXT NOT NULL,
                    wallet_address TEXT NOT NULL,
                    status TEXT NOT NULL,
                    tx_hash TEXT,
                    mint_value_native TEXT,
                    gas_max_native TEXT,
                    quantity INTEGER,
                    detail TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_mint_history_created ON mint_history(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_mint_history_slug ON mint_history(slug);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_name_nocase ON wallets(name COLLATE NOCASE);
                """
            )

        # V4 additive watch fields. Existing V3 databases are upgraded in-place.
        self._ensure_column("watches", "paid_detected", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("watches", "paid_selection_confirmed", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("watches", "paid_wallets_json", "TEXT NOT NULL DEFAULT '[]'")
        # V4.2: record the actual quantity accepted by OpenSea.
        self._ensure_column("mint_history", "quantity", "INTEGER")

    def encrypt(self, private_key: str) -> bytes:
        return self.fernet.encrypt(private_key.encode("utf-8"))

    def decrypt(self, token: bytes) -> str:
        try:
            return self.fernet.decrypt(token).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("Cannot decrypt a stored wallet. WALLET_ENCRYPTION_KEY may have changed.") from exc

    def _row_to_wallet(self, row: sqlite3.Row) -> StoredWallet:
        chains_raw = json.loads(row["chains_json"] or "[]")
        chains = tuple(str(x) for x in chains_raw if isinstance(x, str))
        return StoredWallet(
            id=int(row["id"]),
            name=str(row["name"]),
            address=str(row["address"]),
            private_key=self.decrypt(row["encrypted_private_key"]),
            quantity=max(1, int(row["quantity"])),
            chains=chains,
            enabled=bool(row["enabled"]),
        )

    def add_wallet(
        self,
        *,
        name: str,
        address: str,
        private_key: str,
        quantity: int = 1,
        chains: tuple[str, ...] = (),
    ) -> int:
        name = name.strip()
        if not name:
            raise ValueError("Wallet name is required")
        now = time.time()
        encrypted = self.encrypt(private_key)
        with self.lock, self.conn:
            existing_name = self.conn.execute(
                "SELECT id,address FROM wallets WHERE name=? COLLATE NOCASE", (name,)
            ).fetchone()
            if existing_name and str(existing_name["address"]).lower() != address.lower():
                raise ValueError("Wallet name already exists")

            self.conn.execute(
                """
                INSERT INTO wallets(name,address,encrypted_private_key,quantity,chains_json,enabled,created_at,updated_at)
                VALUES(?,?,?,?,?,1,?,?)
                ON CONFLICT(address) DO UPDATE SET
                    name=excluded.name,
                    encrypted_private_key=excluded.encrypted_private_key,
                    quantity=excluded.quantity,
                    chains_json=excluded.chains_json,
                    enabled=1,
                    updated_at=excluded.updated_at
                """,
                (name, address, encrypted, max(1, int(quantity)), json.dumps(list(chains)), now, now),
            )
            row = self.conn.execute("SELECT id FROM wallets WHERE address=? COLLATE NOCASE", (address,)).fetchone()
            return int(row["id"])

    def list_wallets(self, enabled_only: bool = True) -> list[StoredWallet]:
        sql = "SELECT * FROM wallets"
        if enabled_only:
            sql += " WHERE enabled=1"
        sql += " ORDER BY id"
        with self.lock:
            rows = self.conn.execute(sql).fetchall()
        return [self._row_to_wallet(r) for r in rows]

    def get_wallet_by_id(self, wallet_id: int) -> StoredWallet | None:
        with self.lock:
            row = self.conn.execute("SELECT * FROM wallets WHERE id=?", (int(wallet_id),)).fetchone()
        return self._row_to_wallet(row) if row else None

    def get_wallet_by_address(self, address: str) -> StoredWallet | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM wallets WHERE address=? COLLATE NOCASE", (address,)
            ).fetchone()
        return self._row_to_wallet(row) if row else None

    def wallet_name_exists(self, name: str, exclude_id: int | None = None) -> bool:
        name = name.strip()
        with self.lock:
            if exclude_id is None:
                row = self.conn.execute(
                    "SELECT 1 FROM wallets WHERE name=? COLLATE NOCASE LIMIT 1", (name,)
                ).fetchone()
            else:
                row = self.conn.execute(
                    "SELECT 1 FROM wallets WHERE name=? COLLATE NOCASE AND id<>? LIMIT 1",
                    (name, int(exclude_id)),
                ).fetchone()
        return row is not None

    def set_wallet_enabled(self, address: str, enabled: bool) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE wallets SET enabled=?, updated_at=? WHERE address=? COLLATE NOCASE",
                (1 if enabled else 0, time.time(), address),
            )
            return cur.rowcount > 0

    def set_wallet_enabled_by_id(self, wallet_id: int, enabled: bool) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE wallets SET enabled=?, updated_at=? WHERE id=?",
                (1 if enabled else 0, time.time(), int(wallet_id)),
            )
            return cur.rowcount > 0

    def set_wallet_name(self, wallet_id: int, name: str) -> bool:
        name = name.strip()
        if not name:
            return False
        if self.wallet_name_exists(name, exclude_id=wallet_id):
            raise ValueError("Wallet name already exists")
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE wallets SET name=?, updated_at=? WHERE id=?",
                (name, time.time(), int(wallet_id)),
            )
            return cur.rowcount > 0

    def set_wallet_quantity(self, wallet_id: int, quantity: int) -> bool:
        quantity = max(1, min(int(quantity), 100))
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE wallets SET quantity=?, updated_at=? WHERE id=?",
                (quantity, time.time(), int(wallet_id)),
            )
            return cur.rowcount > 0

    def delete_wallet(self, address: str) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute("DELETE FROM wallets WHERE address=? COLLATE NOCASE", (address,))
            return cur.rowcount > 0

    def delete_wallet_by_id(self, wallet_id: int) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute("DELETE FROM wallets WHERE id=?", (int(wallet_id),))
            return cur.rowcount > 0

    def upsert_watch(
        self,
        *,
        slug: str,
        chain: str,
        source: str,
        allow_paid: bool,
        max_mint_price_native: str = "0",
        quantity: int | None = None,
    ) -> None:
        now = time.time()
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO watches(slug,chain,source,active,allow_paid,max_mint_price_native,quantity,created_at,updated_at)
                VALUES(?,?,?,1,?,?,?,?,?)
                ON CONFLICT(slug) DO UPDATE SET
                    chain=excluded.chain,
                    source=excluded.source,
                    active=1,
                    allow_paid=excluded.allow_paid,
                    max_mint_price_native=excluded.max_mint_price_native,
                    quantity=excluded.quantity,
                    updated_at=excluded.updated_at
                """,
                (slug, chain, source, 1 if allow_paid else 0, str(max_mint_price_native), quantity, now, now),
            )

    def set_watch_paid_selection(
        self,
        slug: str,
        addresses: set[str] | list[str] | tuple[str, ...],
        *,
        confirmed: bool,
        paid_detected: bool = True,
    ) -> bool:
        normalized = sorted({str(a).lower() for a in addresses if str(a).strip()})
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE watches
                SET paid_detected=?, paid_selection_confirmed=?, paid_wallets_json=?, updated_at=?
                WHERE slug=? COLLATE NOCASE
                """,
                (1 if paid_detected else 0, 1 if confirmed else 0, json.dumps(normalized), time.time(), slug),
            )
            return cur.rowcount > 0

    def clear_watch_paid_selection(self, slug: str) -> bool:
        return self.set_watch_paid_selection(slug, set(), confirmed=False, paid_detected=False)

    def list_watches(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM watches WHERE active=1 ORDER BY created_at"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_watch(self, slug: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM watches WHERE slug=? COLLATE NOCASE", (slug,)
            ).fetchone()
        return dict(row) if row else None

    def remove_watch(self, slug: str) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE watches SET active=0, updated_at=? WHERE slug=? COLLATE NOCASE",
                (time.time(), slug),
            )
            return cur.rowcount > 0

    def record_mint(
        self,
        *,
        slug: str,
        chain: str,
        wallet_name: str,
        wallet_address: str,
        status: str,
        tx_hash: str | None = None,
        mint_value_native: str | None = None,
        gas_max_native: str | None = None,
        quantity: int | None = None,
        detail: str | None = None,
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO mint_history(
                    created_at,slug,chain,wallet_name,wallet_address,status,tx_hash,
                    mint_value_native,gas_max_native,quantity,detail
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    time.time(), slug, chain, wallet_name, wallet_address, status, tx_hash,
                    mint_value_native, gas_max_native, (int(quantity) if quantity is not None else None),
                    (detail or "")[:1500],
                ),
            )

    def latest_mint_status(self, slug: str, wallet_address: str) -> str | None:
        """Return the latest persisted status for this drop + wallet.

        Used by auto-discovery to avoid minting the same drop twice after a
        Railway restart. A later reverted row correctly overrides an older
        submitted row.
        """
        with self.lock:
            row = self.conn.execute(
                """
                SELECT status FROM mint_history
                WHERE slug=? COLLATE NOCASE AND wallet_address=? COLLATE NOCASE
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (slug, wallet_address),
            ).fetchone()
        return str(row["status"]) if row else None

    def recent_history(self, limit: int = 15) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM mint_history ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

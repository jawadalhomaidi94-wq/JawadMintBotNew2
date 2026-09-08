from __future__ import annotations

import json
import os
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
    """SQLite persistence + Fernet encryption for private keys.

    Put the database on a Railway Volume. Keep WALLET_ENCRYPTION_KEY only in
    Railway Variables. The database never stores a plaintext private key.
    """

    def __init__(self, db_path: str, encryption_key: str):
        if not encryption_key:
            raise ValueError(
                "WALLET_ENCRYPTION_KEY is required. Generate one with: "
                "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            )
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
                    detail TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_mint_history_created ON mint_history(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_mint_history_slug ON mint_history(slug);
                """
            )

    def encrypt(self, private_key: str) -> bytes:
        return self.fernet.encrypt(private_key.encode("utf-8"))

    def decrypt(self, token: bytes) -> str:
        try:
            return self.fernet.decrypt(token).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError(
                "Cannot decrypt a stored wallet. WALLET_ENCRYPTION_KEY may have changed."
            ) from exc

    def add_wallet(
        self,
        *,
        name: str,
        address: str,
        private_key: str,
        quantity: int = 1,
        chains: tuple[str, ...] = (),
    ) -> int:
        now = time.time()
        encrypted = self.encrypt(private_key)
        with self.lock, self.conn:
            cur = self.conn.execute(
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
            row = self.conn.execute("SELECT id FROM wallets WHERE address=?", (address,)).fetchone()
            return int(row["id"] if row else cur.lastrowid)

    def list_wallets(self, enabled_only: bool = True) -> list[StoredWallet]:
        sql = "SELECT * FROM wallets"
        if enabled_only:
            sql += " WHERE enabled=1"
        sql += " ORDER BY id"
        with self.lock:
            rows = self.conn.execute(sql).fetchall()
        output: list[StoredWallet] = []
        for row in rows:
            try:
                chains_raw = json.loads(row["chains_json"] or "[]")
                chains = tuple(str(x) for x in chains_raw if isinstance(x, str))
                output.append(
                    StoredWallet(
                        id=int(row["id"]),
                        name=str(row["name"]),
                        address=str(row["address"]),
                        private_key=self.decrypt(row["encrypted_private_key"]),
                        quantity=max(1, int(row["quantity"])),
                        chains=chains,
                        enabled=bool(row["enabled"]),
                    )
                )
            except Exception:
                raise
        return output

    def set_wallet_enabled(self, address: str, enabled: bool) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE wallets SET enabled=?, updated_at=? WHERE address=? COLLATE NOCASE",
                (1 if enabled else 0, time.time(), address),
            )
            return cur.rowcount > 0

    def delete_wallet(self, address: str) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute("DELETE FROM wallets WHERE address=? COLLATE NOCASE", (address,))
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

    def list_watches(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM watches WHERE active=1 ORDER BY created_at"
            ).fetchall()
        return [dict(row) for row in rows]

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
        detail: str | None = None,
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO mint_history(
                    created_at,slug,chain,wallet_name,wallet_address,status,tx_hash,
                    mint_value_native,gas_max_native,detail
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    time.time(), slug, chain, wallet_name, wallet_address, status, tx_hash,
                    mint_value_native, gas_max_native, (detail or "")[:1500],
                ),
            )

    def recent_history(self, limit: int = 15) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM mint_history ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

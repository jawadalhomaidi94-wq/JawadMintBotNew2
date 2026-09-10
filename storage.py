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
    intentionally additive so V3/V4 databases continue to work with V4.9.
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

                -- V4.13.1: rows older than 24h are compacted here before being
                -- deleted from mint_history. This preserves cumulative mint
                -- accounting/anti-duplicate state without keeping an unbounded
                -- visible history table. target_key is contract-first when the
                -- NFT contract is known, otherwise it falls back to the slug.
                CREATE TABLE IF NOT EXISTS mint_totals (
                    chain TEXT NOT NULL,
                    wallet_address TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    contract_address TEXT,
                    confirmed_quantity INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(chain, wallet_address, target_key)
                );

                CREATE INDEX IF NOT EXISTS idx_mint_totals_lookup
                    ON mint_totals(chain,wallet_address,target_key);

                CREATE TABLE IF NOT EXISTS qualification_projects (
                    project_key TEXT PRIMARY KEY,
                    slug TEXT NOT NULL,
                    chain TEXT NOT NULL,
                    contract_address TEXT,
                    source TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active',
                    discovered_at REAL NOT NULL,
                    last_checked_at REAL NOT NULL,
                    next_stage_start REAL,
                    public_start REAL,
                    final_stage_end REAL,
                    stages_json TEXT NOT NULL DEFAULT '[]',
                    last_stage_key TEXT,
                    last_stage_label TEXT,
                    last_stage_start REAL,
                    last_stage_end REAL,
                    archived_at REAL,
                    archive_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS qualification_wallets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_key TEXT NOT NULL,
                    stage_key TEXT NOT NULL,
                    stage_label TEXT NOT NULL,
                    stage_start REAL,
                    stage_end REAL,
                    wallet_name TEXT NOT NULL,
                    wallet_address TEXT NOT NULL,
                    eligibility_status TEXT NOT NULL,
                    eligible INTEGER,
                    stage_limit INTEGER,
                    target_total INTEGER,
                    additional_needed INTEGER,
                    quantity_available INTEGER,
                    checked_at REAL NOT NULL,
                    UNIQUE(project_key, stage_key, wallet_address COLLATE NOCASE)
                );

                CREATE INDEX IF NOT EXISTS idx_qualification_projects_status ON qualification_projects(status,last_checked_at DESC);
                CREATE INDEX IF NOT EXISTS idx_qualification_projects_discovered ON qualification_projects(discovered_at DESC);
                CREATE INDEX IF NOT EXISTS idx_qualification_wallets_project ON qualification_wallets(project_key,stage_key);

                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS social_trust_cache (
                    project_key TEXT PRIMARY KEY,
                    slug TEXT NOT NULL,
                    chain TEXT NOT NULL,
                    contract_address TEXT,
                    status TEXT NOT NULL,
                    twitter_username TEXT,
                    twitter_url TEXT,
                    website_url TEXT,
                    checked_at REAL NOT NULL,
                    detail TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_social_trust_slug
                    ON social_trust_cache(slug COLLATE NOCASE, checked_at DESC);

                -- V4.12.0: Collection Offers are deliberately stored separately
                -- from mint_history/watches so Offer bookkeeping cannot alter
                -- Race Lane state or cumulative Mint accounting.
                CREATE TABLE IF NOT EXISTS offer_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    slug TEXT NOT NULL,
                    chain TEXT NOT NULL,
                    wallet_name TEXT NOT NULL,
                    wallet_address TEXT NOT NULL,
                    order_hash TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    protocol_address TEXT NOT NULL,
                    currency_address TEXT,
                    currency_symbol TEXT NOT NULL DEFAULT 'WETH',
                    unit_price_usdt TEXT,
                    unit_price_weth TEXT,
                    quantity INTEGER NOT NULL DEFAULT 1,
                    total_weth TEXT,
                    start_time REAL,
                    end_time REAL,
                    status TEXT NOT NULL DEFAULT 'active',
                    protocol_data_json TEXT,
                    response_json TEXT,
                    detail TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_offer_history_wallet_status
                    ON offer_history(wallet_address COLLATE NOCASE,status,created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_offer_history_slug_status
                    ON offer_history(slug COLLATE NOCASE,status,created_at DESC);
                """
            )

        # V4 additive watch fields. Existing V3 databases are upgraded in-place.
        self._ensure_column("watches", "paid_detected", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("watches", "paid_selection_confirmed", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("watches", "paid_wallets_json", "TEXT NOT NULL DEFAULT '[]'")
        # V4.2: record the actual quantity accepted by OpenSea.
        self._ensure_column("mint_history", "quantity", "INTEGER")
        # V4.3: persist direct SeaDrop backend metadata and contract-level
        # history so the same NFT contract is never auto-minted twice even if
        # OpenSea temporarily exposes it under a different/missing slug.
        self._ensure_column("watches", "mint_backend", "TEXT NOT NULL DEFAULT 'opensea'")
        self._ensure_column("watches", "contract_address", "TEXT")
        self._ensure_column("mint_history", "contract_address", "TEXT")
        # V4.4 stage/qualification/watch planning fields.
        self._ensure_column("watches", "watch_kind", "TEXT NOT NULL DEFAULT 'manual'")
        self._ensure_column("watches", "archived_at", "REAL")
        self._ensure_column("watches", "archive_reason", "TEXT")
        self._ensure_column("watches", "stages_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column("watches", "next_stage_start", "REAL")
        self._ensure_column("watches", "public_start", "REAL")
        self._ensure_column("watches", "last_stage_key", "TEXT")
        self._ensure_column("watches", "last_stage_label", "TEXT")
        self._ensure_column("watches", "paid_decision", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("watches", "paid_stage_key", "TEXT")
        self._ensure_column("watches", "paid_stage_start", "REAL")
        self._ensure_column("watches", "paid_wallet_quantities_json", "TEXT NOT NULL DEFAULT '{}'")
        # V4.9: project-specific gas policy. Global/per-chain defaults live in bot_settings.
        self._ensure_column("watches", "gas_override_usd", "TEXT")
        self._ensure_column("watches", "ignore_gas_cap", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("mint_history", "stage_key", "TEXT")
        self._ensure_column("mint_history", "stage_label", "TEXT")
        self._ensure_column("mint_history", "watch_kind", "TEXT")

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
        mint_backend: str = "opensea",
        contract_address: str | None = None,
        watch_kind: str = "manual",
        stages_json: str = "[]",
        next_stage_start: float | None = None,
        public_start: float | None = None,
        gas_override_usd: str | None = None,
        ignore_gas_cap: bool = False,
    ) -> None:
        now = time.time()
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO watches(
                    slug,chain,source,active,allow_paid,max_mint_price_native,quantity,
                    mint_backend,contract_address,watch_kind,stages_json,next_stage_start,public_start,
                    gas_override_usd,ignore_gas_cap,archived_at,archive_reason,created_at,updated_at
                ) VALUES(?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,?,?)
                ON CONFLICT(slug) DO UPDATE SET
                    chain=excluded.chain,
                    source=excluded.source,
                    active=1,
                    allow_paid=excluded.allow_paid,
                    max_mint_price_native=excluded.max_mint_price_native,
                    quantity=excluded.quantity,
                    mint_backend=excluded.mint_backend,
                    contract_address=excluded.contract_address,
                    watch_kind=excluded.watch_kind,
                    stages_json=excluded.stages_json,
                    next_stage_start=excluded.next_stage_start,
                    public_start=excluded.public_start,
                    gas_override_usd=COALESCE(excluded.gas_override_usd,watches.gas_override_usd),
                    ignore_gas_cap=watches.ignore_gas_cap,
                    archived_at=NULL,
                    archive_reason=NULL,
                    updated_at=excluded.updated_at
                """,
                (
                    slug, chain, source, 1 if allow_paid else 0, str(max_mint_price_native), quantity,
                    str(mint_backend or "opensea"), contract_address, str(watch_kind or "manual"),
                    stages_json or "[]", next_stage_start, public_start, gas_override_usd,
                    1 if ignore_gas_cap else 0, now, now,
                ),
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

    def list_watches(self, active_only: bool = True) -> list[dict[str, Any]]:
        sql = "SELECT * FROM watches"
        if active_only:
            sql += " WHERE active=1"
        sql += " ORDER BY created_at"
        with self.lock:
            rows = self.conn.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def list_archived_watches(self, limit: int = 30) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM watches WHERE active=0 ORDER BY COALESCE(archived_at,updated_at) DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_watch(self, slug: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM watches WHERE slug=? COLLATE NOCASE", (slug,)
            ).fetchone()
        return dict(row) if row else None

    def remove_watch(self, slug: str, reason: str = "manual") -> bool:
        now = time.time()
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE watches SET active=0, archived_at=?, archive_reason=?, updated_at=? WHERE slug=? COLLATE NOCASE",
                (now, (reason or "manual")[:500], now, slug),
            )
            return cur.rowcount > 0

    def update_watch_stage_metadata(
        self, slug: str, *, stages_json: str, next_stage_start: float | None, public_start: float | None,
        last_stage_key: str | None = None, last_stage_label: str | None = None,
    ) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE watches SET stages_json=?, next_stage_start=?, public_start=?,
                    last_stage_key=?, last_stage_label=?, updated_at=?
                WHERE slug=? COLLATE NOCASE
                """,
                (stages_json or "[]", next_stage_start, public_start, last_stage_key, last_stage_label, time.time(), slug),
            )
            return cur.rowcount > 0

    def set_watch_paid_plan(
        self, slug: str, quantities: dict[str, int], *, decision: str,
        stage_key: str | None = None, stage_start: float | None = None, paid_detected: bool = True,
    ) -> bool:
        normalized = {str(a).lower(): max(1, min(int(q), 100)) for a, q in quantities.items() if str(a).strip()}
        addresses = sorted(normalized)
        confirmed = decision == "confirmed"
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE watches SET paid_detected=?, paid_selection_confirmed=?, paid_wallets_json=?,
                    paid_wallet_quantities_json=?, paid_decision=?, paid_stage_key=?, paid_stage_start=?, updated_at=?
                WHERE slug=? COLLATE NOCASE
                """,
                (1 if paid_detected else 0, 1 if confirmed else 0, json.dumps(addresses),
                 json.dumps(normalized, ensure_ascii=False), decision, stage_key, stage_start, time.time(), slug),
            )
            return cur.rowcount > 0

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self.lock:
            row = self.conn.execute("SELECT value FROM bot_settings WHERE key=?", (str(key),)).fetchone()
        return str(row["value"]) if row else default

    def set_setting(self, key: str, value: str) -> None:
        now = time.time()
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO bot_settings(key,value,updated_at) VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at
                """,
                (str(key), str(value), now),
            )

    def delete_setting(self, key: str) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute("DELETE FROM bot_settings WHERE key=?", (str(key),))
            return cur.rowcount > 0

    def get_social_trust(self, project_key: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM social_trust_cache WHERE project_key=?", (str(project_key),)
            ).fetchone()
        return dict(row) if row else None

    def upsert_social_trust(
        self, *, project_key: str, slug: str, chain: str, contract_address: str | None,
        status: str, twitter_username: str | None = None, twitter_url: str | None = None,
        website_url: str | None = None, checked_at: float | None = None, detail: str | None = None,
    ) -> None:
        checked = float(checked_at if checked_at is not None else time.time())
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO social_trust_cache(
                    project_key,slug,chain,contract_address,status,twitter_username,twitter_url,website_url,checked_at,detail
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(project_key) DO UPDATE SET
                    slug=excluded.slug,chain=excluded.chain,contract_address=excluded.contract_address,
                    status=excluded.status,twitter_username=excluded.twitter_username,twitter_url=excluded.twitter_url,
                    website_url=excluded.website_url,checked_at=excluded.checked_at,detail=excluded.detail
                """,
                (str(project_key), str(slug), str(chain), contract_address, str(status), twitter_username,
                 twitter_url, website_url, checked, detail),
            )

    def set_watch_gas_policy(
        self, slug: str, *, gas_override_usd: str | None = None, ignore_gas_cap: bool = False
    ) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE watches SET gas_override_usd=?,ignore_gas_cap=?,updated_at=?
                WHERE slug=? COLLATE NOCASE
                """,
                (gas_override_usd, 1 if ignore_gas_cap else 0, time.time(), slug),
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
        contract_address: str | None = None,
        stage_key: str | None = None,
        stage_label: str | None = None,
        watch_kind: str | None = None,
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO mint_history(
                    created_at,slug,chain,wallet_name,wallet_address,status,tx_hash,
                    mint_value_native,gas_max_native,quantity,detail,contract_address,stage_key,stage_label,watch_kind
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    time.time(), slug, chain, wallet_name, wallet_address, status, tx_hash,
                    mint_value_native, gas_max_native, (int(quantity) if quantity is not None else None),
                    (detail or "")[:1500], contract_address, stage_key, stage_label, watch_kind,
                ),
            )

    def latest_mint_status(
        self,
        slug: str,
        wallet_address: str,
        *,
        chain: str | None = None,
        contract_address: str | None = None,
    ) -> str | None:
        """Return latest status for a logical mint target + wallet.

        V4.3 matches either slug or the exact contract address on the same chain,
        preventing duplicate auto-mints when Stream/Events temporarily resolve
        the same collection under different identifiers.
        """
        with self.lock:
            if chain and contract_address:
                row = self.conn.execute(
                    """
                    SELECT status FROM mint_history
                    WHERE wallet_address=? COLLATE NOCASE AND chain=? COLLATE NOCASE
                      AND (slug=? COLLATE NOCASE OR contract_address=? COLLATE NOCASE)
                    ORDER BY created_at DESC, id DESC LIMIT 1
                    """,
                    (wallet_address, chain, slug, contract_address),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT status FROM mint_history
                    WHERE slug=? COLLATE NOCASE AND wallet_address=? COLLATE NOCASE
                    ORDER BY created_at DESC, id DESC LIMIT 1
                    """,
                    (slug, wallet_address),
                ).fetchone()
        return str(row["status"]) if row else None

    def latest_mint_record(
        self,
        slug: str,
        wallet_address: str,
        *,
        chain: str | None = None,
        contract_address: str | None = None,
    ) -> dict[str, Any] | None:
        with self.lock:
            if chain and contract_address:
                row = self.conn.execute(
                    """
                    SELECT * FROM mint_history
                    WHERE wallet_address=? COLLATE NOCASE AND chain=? COLLATE NOCASE
                      AND (slug=? COLLATE NOCASE OR contract_address=? COLLATE NOCASE)
                    ORDER BY created_at DESC, id DESC LIMIT 1
                    """,
                    (wallet_address, chain, slug, contract_address),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT * FROM mint_history
                    WHERE slug=? COLLATE NOCASE AND wallet_address=? COLLATE NOCASE
                    ORDER BY created_at DESC, id DESC LIMIT 1
                    """,
                    (slug, wallet_address),
                ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _mint_target_key(slug: str, contract_address: str | None = None) -> str:
        contract = str(contract_address or "").strip().lower()
        return contract if contract else str(slug or "").strip().lower()

    def confirmed_quantity_for_target(
        self,
        slug: str,
        wallet_address: str,
        *,
        chain: str,
        contract_address: str | None = None,
    ) -> int:
        """Return cumulative confirmed quantity from recent rows + compacted totals.

        Visible mint history is intentionally kept to 24h in V4.13.1, but the
        bot must still remember older confirmed quantity to avoid duplicate
        mints and to calculate cumulative stage targets correctly.
        """
        target_key = self._mint_target_key(slug, contract_address)
        with self.lock:
            total_row = self.conn.execute(
                """
                SELECT COALESCE(confirmed_quantity,0) AS qty FROM mint_totals
                WHERE chain=? COLLATE NOCASE AND wallet_address=? COLLATE NOCASE AND target_key=? COLLATE NOCASE
                """,
                (chain, wallet_address, target_key),
            ).fetchone()
            if contract_address:
                row = self.conn.execute(
                    """
                    SELECT COALESCE(SUM(COALESCE(quantity,0)),0) AS qty FROM mint_history
                    WHERE status='confirmed' AND wallet_address=? COLLATE NOCASE AND chain=? COLLATE NOCASE
                      AND (slug=? COLLATE NOCASE OR contract_address=? COLLATE NOCASE)
                    """,
                    (wallet_address, chain, slug, contract_address),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT COALESCE(SUM(COALESCE(quantity,0)),0) AS qty FROM mint_history
                    WHERE status='confirmed' AND wallet_address=? COLLATE NOCASE AND chain=? COLLATE NOCASE
                      AND slug=? COLLATE NOCASE
                    """,
                    (wallet_address, chain, slug),
                ).fetchone()
        return int((total_row["qty"] if total_row else 0) or 0) + int((row["qty"] if row else 0) or 0)

    def compact_mint_history(self, *, max_age_seconds: float = 86400.0) -> dict[str, int]:
        """Compact confirmed mint rows then delete visible history older than max_age_seconds.

        This operation is transactionally safe: confirmed quantities are merged
        into mint_totals before any old rows are removed. It is designed for a
        low-priority maintenance thread, never the Race Lane.
        """
        cutoff = time.time() - max(60.0, float(max_age_seconds))
        compacted = 0
        deleted = 0
        with self.lock, self.conn:
            rows = self.conn.execute(
                """
                SELECT slug,chain,wallet_address,contract_address,
                       COALESCE(SUM(COALESCE(quantity,0)),0) AS qty
                FROM mint_history
                WHERE status='confirmed' AND created_at < ?
                GROUP BY slug,chain,wallet_address,contract_address
                """,
                (cutoff,),
            ).fetchall()
            for row in rows:
                qty = int(row["qty"] or 0)
                if qty <= 0:
                    continue
                slug = str(row["slug"] or "")
                contract = str(row["contract_address"] or "") or None
                target_key = self._mint_target_key(slug, contract)
                self.conn.execute(
                    """
                    INSERT INTO mint_totals(chain,wallet_address,target_key,slug,contract_address,confirmed_quantity,updated_at)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(chain,wallet_address,target_key) DO UPDATE SET
                        confirmed_quantity=mint_totals.confirmed_quantity + excluded.confirmed_quantity,
                        slug=excluded.slug,
                        contract_address=COALESCE(excluded.contract_address,mint_totals.contract_address),
                        updated_at=excluded.updated_at
                    """,
                    (str(row["chain"] or ""), str(row["wallet_address"] or ""), target_key,
                     slug, contract, qty, time.time()),
                )
                compacted += qty
            cur = self.conn.execute("DELETE FROM mint_history WHERE created_at < ?", (cutoff,))
            deleted = int(cur.rowcount or 0)
        return {"compacted_quantity": compacted, "deleted_rows": deleted}

    def cleanup_transient_cache(self, *, social_max_age_seconds: float = 172800.0) -> dict[str, int]:
        """Delete stale non-authoritative cache rows without touching watches/wallets/offers."""
        cutoff = time.time() - max(3600.0, float(social_max_age_seconds))
        with self.lock, self.conn:
            cur = self.conn.execute("DELETE FROM social_trust_cache WHERE checked_at < ?", (cutoff,))
            social_deleted = int(cur.rowcount or 0)
        return {"social_cache_deleted": social_deleted}

    def upsert_qualification_project(
        self,
        *,
        project_key: str,
        slug: str,
        chain: str,
        contract_address: str | None,
        source: str,
        stages: list[dict[str, Any]],
        next_stage_start: float | None,
        public_start: float | None,
        final_stage_end: float | None,
        last_stage_key: str | None = None,
        last_stage_label: str | None = None,
        last_stage_start: float | None = None,
        last_stage_end: float | None = None,
    ) -> None:
        now = time.time()
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO qualification_projects(
                    project_key,slug,chain,contract_address,source,status,discovered_at,last_checked_at,
                    next_stage_start,public_start,final_stage_end,stages_json,last_stage_key,last_stage_label,
                    last_stage_start,last_stage_end,archived_at,archive_reason
                ) VALUES(?,?,?,?,?,'active',?,?,?,?,?,?,?,?,?,?,NULL,NULL)
                ON CONFLICT(project_key) DO UPDATE SET
                    slug=excluded.slug, chain=excluded.chain, contract_address=excluded.contract_address,
                    source=excluded.source, status='active', last_checked_at=excluded.last_checked_at,
                    next_stage_start=excluded.next_stage_start, public_start=excluded.public_start,
                    final_stage_end=excluded.final_stage_end, stages_json=excluded.stages_json,
                    last_stage_key=COALESCE(excluded.last_stage_key,qualification_projects.last_stage_key),
                    last_stage_label=COALESCE(excluded.last_stage_label,qualification_projects.last_stage_label),
                    last_stage_start=COALESCE(excluded.last_stage_start,qualification_projects.last_stage_start),
                    last_stage_end=COALESCE(excluded.last_stage_end,qualification_projects.last_stage_end),
                    archived_at=NULL, archive_reason=NULL
                """,
                (project_key, slug, chain, contract_address, source, now, now, next_stage_start, public_start,
                 final_stage_end, json.dumps(stages, ensure_ascii=False), last_stage_key, last_stage_label,
                 last_stage_start, last_stage_end),
            )

    def record_qualification_wallet(
        self,
        *,
        project_key: str,
        stage_key: str,
        stage_label: str,
        stage_start: float | None,
        stage_end: float | None,
        wallet_name: str,
        wallet_address: str,
        eligibility_status: str,
        eligible: bool | None,
        stage_limit: int | None,
        target_total: int | None,
        additional_needed: int | None,
        quantity_available: int | None,
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO qualification_wallets(
                    project_key,stage_key,stage_label,stage_start,stage_end,wallet_name,wallet_address,
                    eligibility_status,eligible,stage_limit,target_total,additional_needed,quantity_available,checked_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(project_key,stage_key,wallet_address) DO UPDATE SET
                    wallet_name=excluded.wallet_name, eligibility_status=excluded.eligibility_status,
                    eligible=excluded.eligible, stage_limit=excluded.stage_limit, target_total=excluded.target_total,
                    additional_needed=excluded.additional_needed, quantity_available=excluded.quantity_available,
                    checked_at=excluded.checked_at
                """,
                (project_key, stage_key, stage_label, stage_start, stage_end, wallet_name, wallet_address,
                 eligibility_status, None if eligible is None else (1 if eligible else 0), stage_limit,
                 target_total, additional_needed, quantity_available, time.time()),
            )

    def list_qualification_projects(self, *, active: bool | None = None, limit: int = 40) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        sql = "SELECT * FROM qualification_projects"
        args: list[Any] = []
        if active is True:
            sql += " WHERE status='active'"
        elif active is False:
            sql += " WHERE status<>'active'"
        sql += " ORDER BY discovered_at DESC LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.conn.execute(sql, tuple(args)).fetchall()
        return [dict(r) for r in rows]

    def qualification_wallet_rows(self, project_key: str, stage_key: str | None = None) -> list[dict[str, Any]]:
        with self.lock:
            if stage_key:
                rows = self.conn.execute(
                    "SELECT * FROM qualification_wallets WHERE project_key=? AND stage_key=? ORDER BY wallet_name COLLATE NOCASE",
                    (project_key, stage_key),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM qualification_wallets WHERE project_key=? ORDER BY checked_at DESC,wallet_name COLLATE NOCASE",
                    (project_key,),
                ).fetchall()
        return [dict(r) for r in rows]

    def archive_qualification_project(self, project_key: str, reason: str) -> bool:
        now = time.time()
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE qualification_projects SET status='archived', archived_at=?, archive_reason=?, last_checked_at=? WHERE project_key=?",
                (now, (reason or "finished")[:500], now, project_key),
            )
            return cur.rowcount > 0

    def free_mint_summary(self, limit: int = 30) -> list[dict[str, Any]]:
        """Aggregate confirmed zero-cost mints by logical project."""
        limit = max(1, min(int(limit), 100))
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    slug, chain, contract_address,
                    MAX(created_at) AS last_confirmed_at,
                    COALESCE(SUM(COALESCE(quantity,0)),0) AS total_quantity,
                    COUNT(DISTINCT wallet_address) AS wallet_count,
                    GROUP_CONCAT(DISTINCT wallet_name) AS wallet_names
                FROM mint_history
                WHERE status='confirmed'
                  AND mint_value_native IS NOT NULL
                  AND ABS(CAST(mint_value_native AS REAL)) < 0.000000000000000001
                GROUP BY slug COLLATE NOCASE, chain COLLATE NOCASE, COALESCE(contract_address,'') COLLATE NOCASE
                ORDER BY last_confirmed_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def recent_history(self, limit: int = 15) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM mint_history ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    # ---------- V4.12.0 Collection Offer persistence ----------
    def record_offer(
        self,
        *,
        slug: str,
        chain: str,
        wallet_name: str,
        wallet_address: str,
        order_hash: str,
        protocol_address: str,
        currency_address: str | None = None,
        currency_symbol: str = "WETH",
        unit_price_usdt: str | None = None,
        unit_price_weth: str | None = None,
        quantity: int = 1,
        total_weth: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        status: str = "active",
        protocol_data_json: str | None = None,
        response_json: str | None = None,
        detail: str | None = None,
    ) -> int:
        now = time.time()
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO offer_history(
                    created_at,updated_at,slug,chain,wallet_name,wallet_address,order_hash,
                    protocol_address,currency_address,currency_symbol,unit_price_usdt,unit_price_weth,
                    quantity,total_weth,start_time,end_time,status,protocol_data_json,response_json,detail
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(order_hash) DO UPDATE SET
                    updated_at=excluded.updated_at,slug=excluded.slug,chain=excluded.chain,
                    wallet_name=excluded.wallet_name,wallet_address=excluded.wallet_address,
                    protocol_address=excluded.protocol_address,currency_address=COALESCE(excluded.currency_address,offer_history.currency_address),
                    currency_symbol=excluded.currency_symbol,
                    unit_price_usdt=COALESCE(excluded.unit_price_usdt,offer_history.unit_price_usdt),
                    unit_price_weth=COALESCE(excluded.unit_price_weth,offer_history.unit_price_weth),
                    quantity=excluded.quantity,total_weth=COALESCE(excluded.total_weth,offer_history.total_weth),
                    start_time=COALESCE(excluded.start_time,offer_history.start_time),
                    end_time=COALESCE(excluded.end_time,offer_history.end_time),
                    status=excluded.status,
                    protocol_data_json=COALESCE(excluded.protocol_data_json,offer_history.protocol_data_json),
                    response_json=COALESCE(excluded.response_json,offer_history.response_json),
                    detail=COALESCE(excluded.detail,offer_history.detail)
                """,
                (
                    now, now, slug, chain, wallet_name, wallet_address, order_hash, protocol_address,
                    currency_address, currency_symbol, unit_price_usdt, unit_price_weth,
                    max(1, min(int(quantity), 100)), total_weth, start_time, end_time, status,
                    protocol_data_json, response_json, (detail or "")[:2000],
                ),
            )
            row = self.conn.execute(
                "SELECT id FROM offer_history WHERE order_hash=? COLLATE NOCASE", (order_hash,)
            ).fetchone()
            return int(row["id"])

    def upsert_remote_offer(
        self,
        *,
        slug: str,
        chain: str,
        wallet_name: str,
        wallet_address: str,
        order_hash: str,
        protocol_address: str,
        currency_symbol: str = "WETH",
        unit_price_weth: str | None = None,
        quantity: int = 1,
        total_weth: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        status: str = "active",
        response_json: str | None = None,
        currency_address: str | None = None,
        unit_price_usdt: str | None = None,
        protocol_data_json: str | None = None,
        detail: str | None = None,
        **_ignored: Any,
    ) -> int:
        """Upsert an OpenSea profile-offer row without erasing richer local data."""
        return self.record_offer(
            slug=slug, chain=chain, wallet_name=wallet_name, wallet_address=wallet_address,
            order_hash=order_hash, protocol_address=protocol_address, currency_address=currency_address,
            currency_symbol=currency_symbol, unit_price_usdt=unit_price_usdt,
            unit_price_weth=unit_price_weth, quantity=quantity, total_weth=total_weth,
            start_time=start_time, end_time=end_time, status=status,
            protocol_data_json=protocol_data_json, response_json=response_json, detail=detail,
        )

    def get_offer(self, offer_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute("SELECT * FROM offer_history WHERE id=?", (int(offer_id),)).fetchone()
        return dict(row) if row else None

    def get_offer_by_hash(self, order_hash: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM offer_history WHERE order_hash=? COLLATE NOCASE", (order_hash,)
            ).fetchone()
        return dict(row) if row else None

    def list_offers(
        self,
        *,
        status: str | None = None,
        wallet_address: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 200))
        clauses: list[str] = []
        args: list[Any] = []
        if status:
            clauses.append("status=?")
            args.append(status)
        if wallet_address:
            clauses.append("wallet_address=? COLLATE NOCASE")
            args.append(wallet_address)
        sql = "SELECT * FROM offer_history"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.conn.execute(sql, tuple(args)).fetchall()
        return [dict(row) for row in rows]

    def set_offer_status(self, offer_id: int, status: str, detail: str | None = None) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE offer_history SET status=?,detail=?,updated_at=? WHERE id=?",
                (str(status), (detail or "")[:2000], time.time(), int(offer_id)),
            )
            return cur.rowcount > 0


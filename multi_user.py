from __future__ import annotations
import json, sqlite3, threading, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from cryptography.fernet import Fernet

DEFAULT_PERMISSIONS = {
    'wallets.view','wallets.create','wallets.delete','wallets.edit','wallets.balance',
    'free_mints.view','free_mints.auto','qualification.view','qualification.watch','qualification.mint',
    'monitoring.view','monitoring.create','history.view','chains.view','settings.view','settings.update',
    'bot.pause','offers.view','offers.create','offers.cancel','paid_mints.use','notifications.receive'
}

@dataclass(frozen=True)
class Tenant:
    id:int; username:str; telegram_id:str; bot_token:str; wallet_limit:int|None; active:bool; permissions:frozenset[str]; priority:int=100

class UserRegistry:
    def __init__(self, path:str, encryption_key:str):
        self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True)
        self.conn=sqlite3.connect(str(self.path),check_same_thread=False,timeout=30); self.conn.row_factory=sqlite3.Row
        self.lock=threading.RLock(); self.fernet=Fernet(encryption_key.encode())
        with self.lock,self.conn:
            self.conn.executescript('''
            PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE COLLATE NOCASE,
              telegram_id TEXT NOT NULL UNIQUE, bot_token_enc BLOB NOT NULL, wallet_limit INTEGER, active INTEGER NOT NULL DEFAULT 1,
              permissions_json TEXT NOT NULL DEFAULT '[]', priority INTEGER NOT NULL DEFAULT 100, created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS wallet_claims(address TEXT PRIMARY KEY COLLATE NOCASE, user_id INTEGER NOT NULL, claimed_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, created_at REAL NOT NULL, actor TEXT NOT NULL,
              action TEXT NOT NULL, target_user_id INTEGER, detail TEXT);
            ''')
    def _enc(self,s:str)->bytes:return self.fernet.encrypt(s.encode())
    def _dec(self,b:bytes)->str:return self.fernet.decrypt(b).decode()
    def add(self,username:str,telegram_id:str,bot_token:str,wallet_limit:int,permissions:set[str]|None=None)->int:
        username=username.strip(); telegram_id=str(telegram_id).strip(); bot_token=bot_token.strip()
        if not username or not telegram_id or ':' not in bot_token: raise ValueError('بيانات المستخدم أو Bot Token غير صحيحة.')
        for existing in self.list(False):
            if existing.bot_token == bot_token: raise ValueError('Bot Token مستخدم لمستخدم آخر.')
        if wallet_limit<0: raise ValueError('حد المحافظ يجب أن يكون 0 أو أكثر.')
        now=time.time(); perms=sorted(permissions or DEFAULT_PERMISSIONS)
        with self.lock,self.conn:
            cur=self.conn.execute('INSERT INTO users(username,telegram_id,bot_token_enc,wallet_limit,active,permissions_json,priority,created_at,updated_at) VALUES(?,?,?,?,1,?,100,?,?)',
              (username,telegram_id,self._enc(bot_token),wallet_limit,json.dumps(perms),now,now))
            uid=int(cur.lastrowid); self.audit('admin','user.add',uid,f'wallet_limit={wallet_limit}')
            return uid
    def audit(self,actor,action,target=None,detail=''):
        with self.lock,self.conn:self.conn.execute('INSERT INTO audit_log(created_at,actor,action,target_user_id,detail) VALUES(?,?,?,?,?)',(time.time(),actor,action,target,detail))
    def get(self,uid:int)->Tenant|None:
        with self.lock:r=self.conn.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone()
        return self._row(r) if r else None
    def list(self,active_only=False)->list[Tenant]:
        q='SELECT * FROM users'+(' WHERE active=1' if active_only else '')+' ORDER BY priority,id'
        with self.lock:rows=self.conn.execute(q).fetchall()
        return [self._row(r) for r in rows]
    def _row(self,r)->Tenant:
        return Tenant(int(r['id']),r['username'],r['telegram_id'],self._dec(r['bot_token_enc']),r['wallet_limit'],bool(r['active']),frozenset(json.loads(r['permissions_json'] or '[]')),int(r['priority']))
    def claim_wallet(self,address:str,user_id:int)->tuple[bool,int|None]:
        address=address.lower()
        with self.lock,self.conn:
            r=self.conn.execute('SELECT user_id FROM wallet_claims WHERE address=? COLLATE NOCASE',(address,)).fetchone()
            if r:return int(r['user_id'])==int(user_id),int(r['user_id'])
            self.conn.execute('INSERT INTO wallet_claims(address,user_id,claimed_at) VALUES(?,?,?)',(address,int(user_id),time.time())); return True,None
    def release_wallet(self,address:str,user_id:int):
        with self.lock,self.conn:self.conn.execute('DELETE FROM wallet_claims WHERE address=? COLLATE NOCASE AND user_id=?',(address.lower(),int(user_id)))

    def set_active(self,uid:int,active:bool):
        with self.lock,self.conn:self.conn.execute('UPDATE users SET active=?,updated_at=? WHERE id=?',(1 if active else 0,time.time(),uid)); self.audit('admin','user.active',uid,str(active))
    def set_limit(self,uid:int,limit:int):
        if limit<0: raise ValueError('invalid limit')
        with self.lock,self.conn:self.conn.execute('UPDATE users SET wallet_limit=?,updated_at=? WHERE id=?',(limit,time.time(),uid)); self.audit('admin','user.wallet_limit',uid,str(limit))
    def set_permissions(self,uid:int,perms:set[str]):
        with self.lock,self.conn:self.conn.execute('UPDATE users SET permissions_json=?,updated_at=? WHERE id=?',(json.dumps(sorted(perms)),time.time(),uid)); self.audit('admin','user.permissions',uid,','.join(sorted(perms)))
    def delete(self,uid:int):
        with self.lock,self.conn:self.conn.execute('DELETE FROM users WHERE id=?',(uid,)); self.audit('admin','user.delete',uid,'')

class TenantRuntime:
    def __init__(self, tenant:Tenant|None, *, is_admin=False):
        self.tenant=tenant; self.is_admin=is_admin
    @property
    def user_id(self): return 0 if self.is_admin else int(self.tenant.id)
    @property
    def name(self): return 'Admin' if self.is_admin else self.tenant.username
    @property
    def active(self): return True if self.is_admin else bool(self.tenant.active)
    @property
    def wallet_limit(self): return None if self.is_admin else self.tenant.wallet_limit
    def can(self,p): return self.is_admin or p in self.tenant.permissions

class TenantSupervisor:
    def __init__(self, admin_bot, registry:UserRegistry, bot_factory, data_dir:Path):
        self.admin_bot=admin_bot; self.registry=registry; self.bot_factory=bot_factory; self.data_dir=Path(data_dir)
        self.lock=threading.RLock(); self.bots:dict[int,Any]={}; self.threads:dict[int,threading.Thread]={}
    def start_all(self):
        for t in self.registry.list(active_only=True): self.start_user(t.id)
    def start_user(self,uid:int):
        with self.lock:
            if uid in self.bots:return self.bots[uid]
            t=self.registry.get(uid)
            if not t or not t.active:return None
            rt=TenantRuntime(t)
            db=str(self.data_dir/f'tenant_{uid}.db')
            bot=self.bot_factory(tenant_runtime=rt,telegram_token=t.bot_token,telegram_chat_id=t.telegram_id,db_path_override=db,discovery_source=self.admin_bot,user_registry=self.registry,tenant_supervisor=self)
            self.bots[uid]=bot
            th=threading.Thread(target=bot.run,name=f'tenant-{uid}-{t.username}',daemon=True); self.threads[uid]=th; th.start()
            self.registry.audit('system','tenant.start',uid,'')
            return bot
    def stop_user(self,uid:int):
        # Bot loops observe tenant active state through refresh_tenant_status; pause immediately here too.
        with self.lock:
            bot=self.bots.get(uid)
            if bot: bot.set_execution_paused(True); bot.tenant_disabled=True
            self.registry.set_active(uid,False)
    def resume_user(self,uid:int):
        self.registry.set_active(uid,True)
        with self.lock:
            bot=self.bots.get(uid)
            if bot:
                bot.tenant_disabled=False; bot.set_execution_paused(False); return bot
        return self.start_user(uid)
    def refresh_user(self,uid:int):
        with self.lock:
            bot=self.bots.get(uid); t=self.registry.get(uid)
            if bot and t: bot.tenant_runtime=TenantRuntime(t)

    def fanout_resolved_signal(self, source_candidate, plan, public, source:str, emitted_perf:float, admin_headstart_seconds:float=0.0):
        """Immediately enqueue one Admin-resolved event into every active tenant lane.

        This is RAM-only and non-blocking for Admin. No tenant discovery RPC is
        performed; each tenant still applies its own permissions, pause, Safe
        Protection, gas caps, wallets and notification policy.
        """
        with self.lock:
            targets=list(self.bots.items())
        submitted=0
        for uid,bot in targets:
            try:
                t=self.registry.get(uid)
                if not t or not t.active or getattr(bot,'tenant_disabled',False):
                    continue
                # Permission prefilter avoids even executor work for tenants that
                # cannot participate. The Bot repeats this check server-side.
                if getattr(source_candidate,'qualification_tracked',False):
                    if 'qualification.mint' not in t.permissions:
                        continue
                elif 'free_mints.auto' not in t.permissions:
                    continue
                bot.race_signal_executor.submit(
                    bot.receive_direct_resolved_signal, source_candidate, plan, public, source,
                    emitted_perf, admin_headstart_seconds
                )
                submitted += 1
            except Exception:
                continue
        return submitted

#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import closing
import json
import hashlib
import re
import uuid
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from economics import Fill, optional_decimal, taker_economics, xemm_journal, calculate, fill_fee, event_time, venue_name

from combined_pnl import (DEFAULT_SINCE, dec, decimal_str, default_since, fmt_money, fmt_pct, iso,
    iter_jsonl, json_default, latest_capital_from_state, parse_dt, print_table, projection, report_runs, utc_now)


TAKER_BOT = "LIGHTER_ASTER_TAKER_ARB"
XEMM_BOT = "XEMM_LIGHTER_ASTER"
# Every row's `mode` column: built from local artifacts only, never from an exchange API.
MODE = "lan"

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS strategy_trades (
    trade_key TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    strategy TEXT NOT NULL,
    bot TEXT NOT NULL,
    market TEXT NOT NULL,
    timestamp TEXT,
    timestamp_us INTEGER,
    direction TEXT,
    qty TEXT NOT NULL,
    gross_pnl_usdc TEXT,
    policy_fees_usdc TEXT,
    net_pnl_usdc TEXT,
    aster_fee_usdc TEXT,
    lighter_fee_usdc TEXT,
    aster_fee_rate TEXT,
    lighter_fee_rate TEXT,
    aster_order_id TEXT,
    lighter_client_order_index TEXT,
    cloid TEXT,
    aster_px TEXT,
    lighter_px TEXT,
    confirmation_status TEXT NOT NULL DEFAULT 'local_only',
    confirmed_at TEXT,
    source TEXT NOT NULL,
    source_path TEXT,
    source_line INTEGER,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    economic_status TEXT NOT NULL DEFAULT 'legacy_unverified',
    matched_qty TEXT,
    residual_qty TEXT,
    aster_qty TEXT,
    lighter_qty TEXT
);

CREATE INDEX IF NOT EXISTS idx_strategy_trades_market_ts
    ON strategy_trades(market, timestamp);

CREATE INDEX IF NOT EXISTS idx_strategy_trades_strategy_ts
    ON strategy_trades(strategy, timestamp);

CREATE TABLE IF NOT EXISTS venue_fills (
    fill_key TEXT PRIMARY KEY,
    trade_key TEXT NOT NULL,
    mode TEXT NOT NULL,
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    timestamp TEXT,
    timestamp_us INTEGER,
    side TEXT,
    qty TEXT NOT NULL,
    price TEXT,
    notional_usdc TEXT,
    liquidity TEXT NOT NULL,
    fee_rate TEXT,
    policy_fee_usdc TEXT,
    confirmation_status TEXT NOT NULL DEFAULT 'local_only',
    confirmed_at TEXT,
    external_trade_id TEXT,
    order_id TEXT,
    client_order_id TEXT,
    source TEXT NOT NULL,
    source_path TEXT,
    source_line INTEGER,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    economic_status TEXT NOT NULL DEFAULT 'legacy_unverified',
    fee_provenance TEXT NOT NULL DEFAULT 'unknown',
    FOREIGN KEY(trade_key) REFERENCES strategy_trades(trade_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_venue_fills_trade_key
    ON venue_fills(trade_key);

CREATE INDEX IF NOT EXISTS idx_venue_fills_venue_market_ts
    ON venue_fills(venue, market, timestamp);

CREATE TABLE IF NOT EXISTS sync_state (
    source TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    market TEXT NOT NULL,
    path TEXT NOT NULL,
    last_refresh_at TEXT NOT NULL,
    last_line_count INTEGER NOT NULL,
    last_mtime_ns INTEGER,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS reconciliation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    mode TEXT NOT NULL,
    market TEXT NOT NULL,
    severity TEXT NOT NULL,
    subject TEXT NOT NULL,
    detail TEXT NOT NULL
);
"""


@dataclass
class IngestStats:
    source: str
    path: Path
    read: int = 0
    upserted_trades: int = 0
    upserted_fills: int = 0
    skipped: int = 0
    missing: bool = False
    error: str | None = None


def raw_json(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def timestamp_us(dt: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt.astimezone(timezone.utc) - epoch
    return ((delta.days * 86400 + delta.seconds) * 1_000_000) + delta.microseconds


def parse_timestamp_us(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    return timestamp_us(parse_dt(str(raw)))


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    migrate_schema(conn)


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) for row in rows}


def migrate_schema(conn: sqlite3.Connection) -> None:
    additions = {
        "strategy_trades": {"schema_version":"INTEGER NOT NULL DEFAULT 1", "economic_status":"TEXT NOT NULL DEFAULT 'legacy_unverified'",
            "matched_qty":"TEXT", "residual_qty":"TEXT", "aster_qty":"TEXT", "lighter_qty":"TEXT"},
        "venue_fills": {"schema_version":"INTEGER NOT NULL DEFAULT 1", "economic_status":"TEXT NOT NULL DEFAULT 'legacy_unverified'",
            "fee_provenance":"TEXT NOT NULL DEFAULT 'unknown'"},
    }
    for table, fields in additions.items():
        existing = table_columns(conn,table)
        for name, declaration in fields.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
    for table, key_column in [("strategy_trades", "trade_key"), ("venue_fills", "fill_key")]:
        if "timestamp_us" not in table_columns(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN timestamp_us INTEGER")
        rows = conn.execute(
            f"SELECT {key_column}, timestamp FROM {table} WHERE timestamp IS NOT NULL AND timestamp_us IS NULL"
        ).fetchall()
        for row in rows:
            try:
                ts_us = parse_timestamp_us(row["timestamp"])
            except (ValueError, TypeError):
                continue
            conn.execute(
                f"UPDATE {table} SET timestamp_us = ? WHERE {key_column} = ?",
                (ts_us, row[key_column]),
            )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_trades_market_ts_us ON strategy_trades(market, timestamp_us)")
    migrate_nullable_economics(conn)


def migrate_nullable_economics(conn: sqlite3.Connection) -> None:
    """Preserve historical values and extensions while allowing unknown economics."""
    fields = {
        "strategy_trades": {"gross_pnl_usdc", "policy_fees_usdc", "net_pnl_usdc", "aster_fee_usdc",
            "lighter_fee_usdc", "aster_fee_rate", "lighter_fee_rate"},
        "venue_fills": {"price", "notional_usdc", "fee_rate", "policy_fee_usdc"},
    }
    pending = []
    for table, names in fields.items():
        if any(row[1] in names and row[3] for row in conn.execute(f"PRAGMA table_info({table})")):
            pending.append((table, names))
    if not pending:
        return
    conn.commit()
    foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    legacy_alter = conn.execute("PRAGMA legacy_alter_table").fetchone()[0]
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("PRAGMA legacy_alter_table=ON")
    try:
        conn.execute("BEGIN IMMEDIATE")
        for table, names in pending:
            sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0]
            auxiliaries = [row[0] for row in conn.execute(
                "SELECT sql FROM sqlite_master WHERE tbl_name=? AND type IN ('index','trigger') AND sql IS NOT NULL", (table,))]
            for name in names:
                sql = re.sub(rf'(\b{re.escape(name)}\s+\w+\s+)NOT\s+NULL\b', r'\1', sql, flags=re.IGNORECASE)
            replacement = table + "_nullable_migration"
            sql = re.sub(r'(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)["`\[]?' + table + r'["`\]]?',
                lambda match: match[1] + replacement, sql, count=1, flags=re.IGNORECASE)
            conn.execute(sql)
            columns = ','.join('"' + row[1].replace('"', '""') + '"' for row in conn.execute(f"PRAGMA table_info({table})"))
            conn.execute(f"INSERT INTO {replacement} ({columns}) SELECT {columns} FROM {table}")
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f"ALTER TABLE {replacement} RENAME TO {table}")
            for statement in auxiliaries:
                conn.execute(statement)
        if conn.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("foreign key violation during nullable economics migration")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.execute(f"PRAGMA legacy_alter_table={legacy_alter}")
        conn.execute(f"PRAGMA foreign_keys={foreign_keys}")


def database_records(n: dict[str, Any], *, path: Path, line_no: int, source: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    now = iso(utc_now())
    at = n.get("timestamp")
    timestamp = iso(at) if at is not None else None
    at_us = timestamp_us(at) if at is not None else None
    strategy = "TAKER" if n["key"].startswith("taker:") else "XEMM"
    status = n.get("economic_status", "legacy_unverified")
    raw = raw_json(n.get("raw") or {})
    a_fee, h_fee = n.get("aster_fee_usdc"), n.get("lighter_fee_usdc")
    trade = {
        "trade_key":n["key"], "mode":MODE, "strategy":strategy,
        "bot":TAKER_BOT if strategy == "TAKER" else XEMM_BOT,
        "market":n["market"], "timestamp":timestamp, "timestamp_us":at_us,
        "direction":n.get("direction") or f"ASTER_MAKER_HEDGE_{str(n.get('hedge_side','')).upper()}",
        "qty":decimal_str(dec(n.get("qty"))),
        "gross_pnl_usdc":decimal_str(n.get("gross_pnl_usdc")),
        "policy_fees_usdc":decimal_str(n.get("fees_usdc")),
        "net_pnl_usdc":decimal_str(n.get("net_pnl_usdc")),
        "aster_fee_usdc":decimal_str(a_fee), "lighter_fee_usdc":decimal_str(h_fee),
        "aster_fee_rate":None, "lighter_fee_rate":None,
        "aster_order_id":n.get("aster_order_id"), "lighter_client_order_index":n.get("lighter_client_order_index"),
        "cloid":n.get("cloid"),
        "aster_px":None if n.get("aster_px") is None else decimal_str(n["aster_px"]),
        "lighter_px":None if n.get("lighter_px") is None else decimal_str(n["lighter_px"]),
        "confirmation_status":"local_only", "confirmed_at":None,
        "source":source,"source_path":str(path),"source_line":line_no,"raw_json":raw,
        "created_at":now,"updated_at":now,"schema_version":2,"economic_status":status,
        **{key:None if n.get(key) is None else decimal_str(n[key]) for key in ("matched_qty","residual_qty","aster_qty","lighter_qty")},
    }
    fills = []
    for fill in n.get("fills", []):
        detail = fill.source.get("detail", fill.source)
        known = fill.quote is not None and fill.fee is not None
        price = fill.quote/fill.qty if fill.quote is not None and fill.qty else None
        ft = iso(fill.timestamp) if fill.timestamp is not None else timestamp
        fu = timestamp_us(fill.timestamp) if fill.timestamp is not None else at_us
        fills.append({
            "fill_key":fill.stored_key or f"local:{n['key']}:{fill.identity}","trade_key":n["key"],"mode":MODE,"venue":fill.venue,
            "market":n["market"],"timestamp":ft,"timestamp_us":fu,"side":fill.side,"qty":decimal_str(fill.qty),
            "price":decimal_str(price),"notional_usdc":decimal_str(fill.quote),
            "liquidity":"maker" if detail.get("maker") is True else ("taker" if detail.get("maker") is False else "unknown"),
            "fee_rate":decimal_str(fill.fee/fill.quote) if fill.fee is not None and fill.quote else None,
            "policy_fee_usdc":decimal_str(fill.fee),"confirmation_status":"local_only","confirmed_at":None,
            "external_trade_id":detail.get("trade_id"),"order_id":detail.get("order_id"),
            "client_order_id":fill.attempt_id or None,"source":source,"source_path":str(path),"source_line":line_no,
            "raw_json":raw_json(fill.source),"created_at":now,"updated_at":now,
            "schema_version":2,"economic_status":"confirmed" if known else status,
            "fee_provenance":"venue" if fill.fee is not None else "unknown",
        })
    return trade, fills


def taker_trade_from_row(row: dict[str, Any], *, path: Path, line_no: int) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    n = taker_economics(row)
    return database_records(n,path=path,line_no=line_no,source="taker_local_ledger") if n else None


def upsert_row(conn: sqlite3.Connection, table: str, key_column: str, row: dict[str, Any], preserve: set[str]) -> None:
    columns = list(row.keys())
    placeholders = ", ".join("?" for _ in columns)
    assignments = ", ".join(
        f"{column}=excluded.{column}" for column in columns if column != key_column and column not in preserve
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT({key_column}) DO UPDATE SET {assignments}"
    )
    conn.execute(sql, [row[column] for column in columns])


def upsert_trade(conn: sqlite3.Connection, trade: dict[str, Any]) -> bool:
    previous=conn.execute("SELECT confirmation_status,source,schema_version,economic_status,aster_qty,lighter_qty FROM strategy_trades WHERE trade_key=?",(trade["trade_key"],)).fetchone()
    if previous is not None:
        if previous["confirmation_status"]=="exchange_confirmed":
            return False
        if previous["source"]=="raw_execution_fills" and previous["economic_status"]=="confirmed" and (
            trade["source"]!="raw_execution_fills" or trade["economic_status"]!="confirmed"
        ):
            grew=any(dec(trade.get(k))>dec(previous[k]) for k in ("aster_qty","lighter_qty"))
            if not grew:
                return False
    upsert_row(conn,"strategy_trades","trade_key",trade,preserve={"confirmation_status","confirmed_at","created_at"})
    return True


def upsert_fill(conn: sqlite3.Connection, fill: dict[str, Any]) -> None:
    if fill.get("external_trade_id") is not None and conn.execute(
        "SELECT 1 FROM venue_fills WHERE trade_key=? AND venue=? AND external_trade_id=? "
        "AND order_id IS ? AND confirmation_status='exchange_confirmed'",
        (fill["trade_key"],fill["venue"],fill["external_trade_id"],fill.get("order_id")),
    ).fetchone():
        return
    existing=conn.execute("SELECT confirmation_status FROM venue_fills WHERE fill_key=?",(fill["fill_key"],)).fetchone()
    if existing and existing["confirmation_status"]=="exchange_confirmed":
        return
    upsert_row(conn,"venue_fills","fill_key",fill,preserve={"confirmation_status","confirmed_at","external_trade_id","created_at"})


def replace_trade_records(conn: sqlite3.Connection, trade: dict[str, Any], fills: list[dict[str, Any]]) -> bool:
    if not upsert_trade(conn,trade):
        return False
    conn.execute("DELETE FROM venue_fills WHERE trade_key=? AND confirmation_status!='exchange_confirmed'",(trade["trade_key"],))
    for fill in fills:
        upsert_fill(conn,fill)
    return True


def update_sync_state(conn: sqlite3.Connection, stats: IngestStats, *, market: str) -> None:
    mtime_ns = None
    if stats.path.exists():
        mtime_ns = stats.path.stat().st_mtime_ns
    conn.execute(
        """
        INSERT INTO sync_state (source, mode, market, path, last_refresh_at, last_line_count, last_mtime_ns, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source) DO UPDATE SET
            mode=excluded.mode,
            market=excluded.market,
            path=excluded.path,
            last_refresh_at=excluded.last_refresh_at,
            last_line_count=excluded.last_line_count,
            last_mtime_ns=excluded.last_mtime_ns,
            last_error=excluded.last_error
        """,
        (
            stats.source,
            MODE,
            market,
            str(stats.path),
            iso(utc_now()),
            stats.read,
            mtime_ns,
            stats.error,
        ),
    )


def ingest_taker_trades(conn: sqlite3.Connection, path: Path, *, market: str) -> IngestStats:
    stats = IngestStats("taker_local_ledger", path)
    if not path.exists():
        stats.missing = True
        update_sync_state(conn, stats, market=market)
        return stats

    errors: list[str] = []
    for line_no, row in iter_jsonl(path, errors):
        stats.read += 1
        if row.get("market") != market:
            stats.skipped += 1
            continue
        try:
            parsed = taker_trade_from_row(row, path=path, line_no=line_no)
        except Exception as exc:  # keep one malformed local line from blocking history refresh
            stats.skipped += 1
            stats.error = str(exc)
            continue
        if parsed is None:
            stats.skipped += 1
            continue
        trade, fills = parsed
        if replace_trade_records(conn,trade,fills):
            stats.upserted_trades += 1
            stats.upserted_fills += len(fills)
    if errors:
        stats.error = "; ".join(errors[:3])
        stats.skipped += len(errors)
    update_sync_state(conn, stats, market=market)
    return stats


def ingest_xemm_journal(conn: sqlite3.Connection, path: Path, *, market: str) -> IngestStats:
    stats = IngestStats("xemm_journal",path)
    if not path.exists():
        stats.missing=True
    else:
        parsed=xemm_journal(path,market)
        stats.skipped=parsed["malformed_rows"]
        for index,n in enumerate(parsed["trades"],1):
            stats.read+=1
            trade,fills=database_records(n,path=path,line_no=n.get("source_line",index),source="xemm_journal")
            if replace_trade_records(conn,trade,fills):
                stats.upserted_trades+=1
                stats.upserted_fills+=len(fills)
        if parsed["malformed_rows"]:
            stats.error=f"{parsed['malformed_rows']} malformed economic rows; source preserved"
    update_sync_state(conn,stats,market=market)
    return stats


def refresh_lan(
    conn: sqlite3.Connection,
    *,
    market: str,
    taker_trades: Path,
    xemm_journals: Sequence[Path] = (),
) -> list[IngestStats]:
    stats = [ingest_taker_trades(conn, taker_trades, market=market)]
    for journal in xemm_journals:
        # Last so the journal's real trade times + actual hedge fees win on shared keys.
        stats.append(ingest_xemm_journal(conn, journal, market=market))
    conn.commit()
    return stats



OWNED_TABLES = ("strategy_trades", "venue_fills", "sync_state", "reconciliation_events")

def database_fingerprint(conn: sqlite3.Connection, schema: str = "main") -> str:
    """Concurrency guard for reviewed replacements, not a scientific validity test."""
    digest=hashlib.sha256()
    for table in OWNED_TABLES:
        info=conn.execute(f"PRAGMA {schema}.table_info({table})").fetchall()
        if not info:
            continue
        names=sorted(str(r[1]) for r in info)
        quoted=",".join('"'+name.replace('"','""')+'"' for name in names)
        key={"strategy_trades":"trade_key","venue_fills":"fill_key","sync_state":"source","reconciliation_events":"id"}[table]
        digest.update(json.dumps([table,names]).encode())
        for row in conn.execute(f"SELECT type,name,sql FROM {schema}.sqlite_master WHERE tbl_name=? ORDER BY type,name", (table,)):
            digest.update(json.dumps(list(row)).encode())
        for row in conn.execute(f"SELECT {quoted} FROM {schema}.{table} ORDER BY {key}"):
            digest.update(json.dumps(list(row),default=str,separators=(",",":")).encode())
    return digest.hexdigest()

def database_overview(conn: sqlite3.Connection) -> dict[str, Any]:
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='strategy_trades'").fetchone():
        return {"trades":0,"known_net_pnl_usdc":"0","incomplete_trades":0}
    rows=conn.execute("SELECT * FROM strategy_trades").fetchall()
    known=Decimal(0)
    incomplete=0
    for row in rows:
        values=confirmed_economics(row)
        if values is not None:
            known+=values[2]
        else:
            incomplete+=1
    return {"trades":len(rows),"known_net_pnl_usdc":decimal_str(known),"incomplete_trades":incomplete}

def confirmed_economics(row) -> list[Decimal] | None:
    columns=set(row.keys())
    confirmed=row["confirmation_status"]=="exchange_confirmed" or (
        "economic_status" in columns and row["economic_status"]=="confirmed" and row["schema_version"]>=2)
    values=[optional_decimal(row[c]) for c in ("gross_pnl_usdc","policy_fees_usdc","net_pnl_usdc","aster_fee_usdc","lighter_fee_usdc")]
    if not confirmed or any(v is None for v in values):
        return None
    gross,fees,net,aster,lighter=values
    if abs(gross-fees-net)>Decimal("0.00000001") or abs(fees-aster-lighter)>Decimal("0.00000001"):
        return None
    return values

def repair_raw_fees(conn: sqlite3.Connection, paths: list[Path], market: str) -> dict[str, int]:
    """Use own-account, order-identified raw fills only when quantity coverage agrees."""
    trades=conn.execute("SELECT * FROM strategy_trades WHERE market=?",(market,)).fetchall()
    evidence: dict[tuple[str,str], dict[str,Fill]]={}
    malformed=[]
    for path in paths:
        for line,row in iter_jsonl(path,malformed):
            d=row.get("detail",row)
            if not isinstance(d,dict) or row.get("market",d.get("market"))!=market:
                continue
            venue=venue_name(d.get("venue"))
            qty=optional_decimal(d.get("qty",d.get("size")))
            px=optional_decimal(d.get("px",d.get("price")))
            quote=optional_decimal(d.get("notional_usd",d.get("usd_amount")))
            if quote is None and qty is not None and px is not None:
                quote=qty*px
            side=str(d.get("side","")).lower()
            trade_id=d.get("trade_id")
            if venue not in {"aster","lighter"} or qty is None or qty<=0 or quote is None or quote<=0 or trade_id is None or side not in {"buy","sell"}:
                malformed.append(f"{path}:{line}: raw fill lacks own-order evidence")
                continue
            fee=fill_fee(d,trusted=row.get("schema_version",1)>=2 and row.get("economic_status")=="confirmed")
            if venue=="aster" and d.get("commission_asset") in {"USD","USDT","USDC"}:
                fee=optional_decimal(d.get("commission"))
            identity=f"{venue}:{d.get('order_id')}:{trade_id}"
            for trade in trades:
                logical=d.get("logical_id")
                matches=(logical is not None and trade["trade_key"]==f"xemm:{logical}")
                if venue=="aster" and trade["aster_order_id"] is not None:
                    matches |= str(d.get("order_id"))==trade["aster_order_id"]
                if venue=="lighter" and trade["lighter_client_order_index"] is not None:
                    matches |= str(d.get("client_order_index"))==trade["lighter_client_order_index"]
                if matches:
                    evidence.setdefault((trade["trade_key"],venue),{})[identity]=Fill(
                        venue,side,qty,quote,fee,event_time(row),identity,str(d.get("attempt_id","")),row)
    repaired=0
    for trade in trades:
        if trade["confirmation_status"]=="exchange_confirmed":
            continue
        replacement=[]
        changed=False
        for venue in ("aster","lighter"):
            old=conn.execute("SELECT * FROM venue_fills WHERE trade_key=? AND venue=?",(trade["trade_key"],venue)).fetchall()
            raw=list(evidence.get((trade["trade_key"],venue),{}).values())
            expected=optional_decimal(trade[f"{venue}_qty"])
            if expected is None:
                expected=sum((dec(r["qty"]) for r in old),Decimal(0)) if old else None
            if raw and expected is not None and sum((f.qty for f in raw),Decimal(0))==expected:
                replacement.extend(raw)
                changed=True
            else:
                for r in old:
                    quantity=dec(r["qty"])
                    quote=optional_decimal(r["notional_usdc"])
                    if quote==0 and quantity>0: quote=None
                    replacement.append(Fill(venue,r["side"],quantity,quote,
                        optional_decimal(r["policy_fee_usdc"]) if r["fee_provenance"]=="venue" or r["confirmation_status"]=="exchange_confirmed" else None,
                        parse_dt(r["timestamp"]) if r["timestamp"] else None,r["fill_key"],r["client_order_id"] or "",
                        json.loads(r["raw_json"]),stored_key=r["fill_key"]))
        if not changed:
            continue
        # Never upgrade a legacy aggregate whose other executed leg is unrepresented.
        if any(optional_decimal(trade[f"{venue}_qty"]) is None for venue in ("aster","lighter")):
            continue
        n=calculate(replacement)
        if trade["strategy"]=="XEMM":
            n["qty"]=dec(trade["qty"])  # fee repair must not relabel corrective taker volume as maker volume
        n.update(key=trade["trade_key"],market=trade["market"],direction=trade["direction"],
            cloid=trade["cloid"],aster_order_id=trade["aster_order_id"],lighter_client_order_index=trade["lighter_client_order_index"],
            timestamp=max((f.timestamp for f in replacement if f.timestamp is not None),
                default=parse_dt(trade["timestamp"]) if trade["timestamp"] else None),
            raw={"repair":"individual own-account fills","previous_source":trade["source"]})
        records,fills=database_records(n,path=paths[0],line_no=0,source="raw_execution_fills")
        repaired+=int(replace_trade_records(conn,records,fills))
    return {"repaired_trades":repaired,"unusable_raw_rows":len(malformed)}

def build_repaired_database(args: argparse.Namespace) -> dict[str, Any]:
    target=args.rebuild_out or args.db.with_name(args.db.stem+".rebuilt.sqlite")
    if target.resolve()==args.db.resolve():
        raise ValueError("--rebuild-out must differ from the original database")
    target.parent.mkdir(parents=True,exist_ok=True)
    tmp=target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    conn=open_db(tmp)
    before={"trades":0,"known_net_pnl_usdc":"0","incomplete_trades":0}
    original_hash=None
    try:
        if args.db.exists():
            with closing(sqlite3.connect(args.db.resolve().as_uri()+"?mode=ro",uri=True)) as original:
                original.row_factory=sqlite3.Row
                original.backup(conn)
                original_hash=database_fingerprint(conn)
                before=database_overview(conn)
        init_db(conn)
        stats=refresh_lan(conn,market=args.market,taker_trades=args.taker_trades,xemm_journals=args.xemm_journal)
        raw=repair_raw_fees(conn,args.raw_fills,args.market) if args.raw_fills else {"repaired_trades":0,"unusable_raw_rows":0}
        conn.commit()
        if conn.execute("PRAGMA integrity_check").fetchone()[0]!="ok" or conn.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("rebuilt database failed SQLite consistency checks")
        result={"original":str(args.db.resolve()),"candidate":str(target.resolve()),
            "original_fingerprint":original_hash,"candidate_fingerprint":database_fingerprint(conn),
            "before":before,"after":database_overview(conn),"raw_repair":raw,
            "refresh":[asdict(st) for st in stats]}
    finally:
        conn.close()
    tmp.replace(target)
    target.with_suffix(".summary.json").write_text(json.dumps(result,default=json_default,indent=2)+"\n",encoding="utf-8")
    return result

def replace_reviewed_database(args: argparse.Namespace) -> dict[str, Any]:
    target=args.rebuild_out or args.db.with_name(args.db.stem+".rebuilt.sqlite")
    if target.resolve()==args.db.resolve():
        raise ValueError("candidate must differ from original database")
    summary_path=target.with_suffix(".summary.json")
    reviewed=json.loads(summary_path.read_text(encoding="utf-8"))
    if reviewed["original"]!=str(args.db.resolve()) or reviewed["candidate"]!=str(target.resolve()):
        raise ValueError("reviewed rebuild paths do not match")
    backup=None
    conn=open_db(args.db)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("ATTACH DATABASE ? AS reviewed",(str(target.resolve()),))
        conn.execute("BEGIN IMMEDIATE")
        expected=reviewed["original_fingerprint"]
        if expected is None and conn.execute("SELECT 1 FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone():
            raise ValueError("original database appeared since review; rebuild before replacing")
        if (database_fingerprint(conn) if expected is not None else None)!=expected:
            raise ValueError("original database changed since review; rebuild before replacing")
        if database_fingerprint(conn,"reviewed")!=reviewed["candidate_fingerprint"]:
            raise ValueError("candidate changed since review; rebuild before replacing")
        if expected is not None:
            backup=args.db.with_name(args.db.stem+"-before-repair-"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+".sqlite")
            source=sqlite3.connect(args.db.resolve().as_uri()+"?mode=ro",uri=True)
            destination=sqlite3.connect(backup)
            try: source.backup(destination)
            finally:
                destination.close()
                source.close()
        # Replace only this tool's tables in one transaction; other database content survives.
        for table in reversed(OWNED_TABLES):
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        for table in OWNED_TABLES:
            ddl=conn.execute("SELECT sql FROM reviewed.sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()[0]
            conn.execute(ddl)
            names=[r[1] for r in conn.execute(f"PRAGMA reviewed.table_info({table})")]
            quoted=",".join('"'+n.replace('"','""')+'"' for n in names)
            conn.execute(f"INSERT INTO main.{table} ({quoted}) SELECT {quoted} FROM reviewed.{table}")
        for table in OWNED_TABLES:
            for (ddl,) in conn.execute("SELECT sql FROM reviewed.sqlite_master WHERE tbl_name=? AND type IN ('index','trigger') AND sql IS NOT NULL",(table,)):
                conn.execute(ddl)
        if conn.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("candidate contains broken fill references")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"replaced":str(args.db),"backup":str(backup) if backup else None,"comparison":reviewed}


def empty_bucket(strategy: str) -> dict[str, Any]:
    return {"strategy":strategy,"trades":0,"incomplete_trades":0,"local_only_trades":0,"exchange_confirmed_trades":0,
        "gross_pnl_usdc":Decimal(0),"policy_fees_usdc":Decimal(0),"net_pnl_usdc":Decimal(0),
        "aster_fees_usdc":Decimal(0),"lighter_fees_usdc":Decimal(0),
        "known_net_pnl_usdc":Decimal(0),"estimated_net_pnl_usdc":Decimal(0)}


def report_from_db(conn: sqlite3.Connection, *, market: str, since: datetime, now: datetime, db_path: Path,
    capital_usdc: Decimal | None = None, bot_state: Path | None = None) -> dict[str, Any]:
    buckets={"TAKER":empty_bucket("TAKER"),"XEMM":empty_bucket("XEMM")}
    confirmation_counts={}
    rows=conn.execute(
        "SELECT * FROM strategy_trades WHERE market=? AND timestamp_us>=? AND timestamp_us<=? ORDER BY timestamp_us,trade_key",
        (market,timestamp_us(since),timestamp_us(now))).fetchall()
    totals=("gross_pnl_usdc","policy_fees_usdc","net_pnl_usdc","aster_fees_usdc","lighter_fees_usdc")
    for row in rows:
        b=buckets.setdefault(str(row["strategy"]),empty_bucket(str(row["strategy"])))
        status=str(row["confirmation_status"])
        confirmation_counts[status]=confirmation_counts.get(status,0)+1
        b["trades"]+=1
        b["exchange_confirmed_trades" if status=="exchange_confirmed" else "local_only_trades"]+=1
        values=confirmed_economics(row)
        if values is not None:
            for dest,value in zip(totals,values):
                b[dest]+=value
            b["known_net_pnl_usdc"]+=values[2]
        else:
            b["incomplete_trades"]+=1
            if row["economic_status"]=="estimated":
                b["estimated_net_pnl_usdc"]+=dec(row["net_pnl_usdc"])
    total=empty_bucket("TOTAL")
    for b in buckets.values():
        for name,value in b.items():
            if name!="strategy": total[name]+=value
    # Untimestamped rows cannot be placed inside or outside any window: they keep every
    # window incomplete instead of silently vanishing from it.
    total["untimestamped_trades"]=conn.execute(
        "SELECT COUNT(*) FROM strategy_trades WHERE market=? AND timestamp_us IS NULL",(market,)).fetchone()[0]
    total["incomplete_trades"]+=total["untimestamped_trades"]
    source_errors=[r[0] for r in conn.execute("SELECT last_error FROM sync_state WHERE market=? AND last_error IS NOT NULL",(market,))]
    for b in [*buckets.values(),total]:
        if b["incomplete_trades"] or (b is total and source_errors):
            for name in totals: b[name]=None
    capital_source="cli"
    capital=capital_usdc
    if capital is None and bot_state is not None:
        capital,capital_source=latest_capital_from_state(bot_state)
    return {"db":db_path,"market":market,"since":since,"now":now,
        "by_strategy":[buckets[k] for k in sorted(buckets)],"total":total,
        "confirmation_counts":confirmation_counts,"source_errors":source_errors,
        "projection":projection(total["net_pnl_usdc"],capital,since,now),"capital_source":capital_source,
        "notes":["Local execution economics include matched spread and explicit recovery closes; portfolio marks and funding are excluded.",
            "Fees require venue evidence. Legacy, estimated and incomplete rows suppress full net totals; their original evidence is preserved.",
            "Known net subtotals exclude unresolved rows; estimated recovery losses are shown separately."]}


def print_human(stats: list[IngestStats], report: dict[str, Any] | None) -> None:
    if stats:
        print_table(
            "Refresh",
            ["Source", "Rows", "Trades", "Fills", "Skipped", "Missing"],
            [
                [s.source, s.read, s.upserted_trades, s.upserted_fills, s.skipped, "yes" if s.missing else "no"]
                for s in stats
            ],
            right_align={1, 2, 3, 4},
        )
        print()
    if report is None:
        return
    p = report["projection"]
    print_table(
        "Trade History PnL",
        ["Source", "Trades", "Gross USDC", "Policy Fees", "Net USDC", "Local Only", "Confirmed"],
        [
            [
                bucket["strategy"],
                bucket["trades"],
                fmt_money(bucket["gross_pnl_usdc"]),
                fmt_money(bucket["policy_fees_usdc"], signed=False),
                fmt_money(bucket["net_pnl_usdc"]),
                bucket["local_only_trades"],
                bucket["exchange_confirmed_trades"],
            ]
            for bucket in report["by_strategy"]
        ]
        + [
            [
                "TOTAL",
                report["total"]["trades"],
                fmt_money(report["total"]["gross_pnl_usdc"]),
                fmt_money(report["total"]["policy_fees_usdc"], signed=False),
                fmt_money(report["total"]["net_pnl_usdc"]),
                report["total"]["local_only_trades"],
                report["total"]["exchange_confirmed_trades"],
            ]
        ],
        right_align={1, 2, 3, 4, 5, 6},
    )
    print()
    print_table(
        "Projection",
        ["Metric", "Value"],
        [
            ["DB", report["db"]],
            ["Market", report["market"]],
            ["Since UTC", iso(report["since"])],
            ["Now UTC", iso(report["now"])],
            ["Elapsed Days", f"{p['elapsed_days']:.6f}"],
            ["Capital USDC", p["capital_usdc"] if p["capital_usdc"] is not None else "n/a"],
            ["Window Return", fmt_pct(p["window_return_pct"], 8)],
            ["Simple Annualized", fmt_pct(p["simple_annualized_return_pct"], 4)],
            ["Projected CAGR", fmt_pct(p["projected_cagr_pct"], 4)],
        ],
    )
    print()
    print("notes:")
    for note in report["notes"]:
        print(f"- {note}")


def parse_args() -> argparse.Namespace:
    stack_root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Canonical local trade-history DB and PnL report.")
    parser.add_argument("--dry-run", action="store_true", help="The dry run's files and its own DB (the VPS's, copied to LIGHTER_ASTER_BOT/runs/tokyo/dry-run/) instead of live.")
    parser.add_argument("--market", default="HYPE")
    parser.add_argument("--since", default=None, help=f"UTC/RFC3339 start time. Default: {DEFAULT_SINCE}; with --dry-run, the dry run's first start.")
    parser.add_argument("--now", default=None, help="Override report end time. Defaults to current UTC time.")
    parser.add_argument("--db", type=Path, default=None, help="Default: runs/trade_history.sqlite in the repository root (with --dry-run, in LIGHTER_ASTER_BOT/runs/tokyo/dry-run/).")
    parser.add_argument("--taker-trades", type=Path, default=None)
    parser.add_argument("--xemm-journal", type=Path, action="append", default=None, help="XEMM raw journal with logical/attempt execution evidence and economic timestamps; repeatable. Default: `run`'s.")
    parser.add_argument("--bot-state", type=Path, default=None, help="Controller state file (capital for projections). Default: `run`'s.")
    parser.add_argument("--capital-usdc", type=Decimal, default=None)
    parser.add_argument("--no-refresh", action="store_true", help="Report existing DB contents without reading local ledgers first.")
    parser.add_argument("--refresh-only", action="store_true", help="Refresh the DB and skip the PnL report.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    repair = parser.add_mutually_exclusive_group()
    repair.add_argument("--rebuild", action="store_true", help="Build and compare a separate repaired database; preserve the original.")
    repair.add_argument("--replace-rebuilt", action="store_true", help="Apply the previously reviewed candidate, retaining an original backup.")
    parser.add_argument("--rebuild-out", type=Path, help="Candidate path (default: <db-stem>.rebuilt.sqlite).")
    parser.add_argument("--raw-fills", type=Path, action="append", default=[], help="Own-account execution_trade JSONL with venue/order identities, notional and fee evidence; repeatable.")
    args = parser.parse_args()
    bot_runs = report_runs(stack_root, args.dry_run)
    args.since = args.since or default_since(bot_runs, args.market, args.dry_run)
    if args.db is None:
        args.db = (bot_runs if args.dry_run else stack_root / "runs") / "trade_history.sqlite"
    if args.taker_trades is None:
        args.taker_trades = bot_runs / f"trades_{args.market}.jsonl"
    if args.xemm_journal is None:
        args.xemm_journal = [bot_runs / f"bot-{args.market}-journal.jsonl"]
    if args.bot_state is None:
        args.bot_state = bot_runs / f"bot-{args.market}.state.json"
    return args


def main() -> int:
    args = parse_args()
    if args.rebuild or args.replace_rebuilt:
        result = build_repaired_database(args) if args.rebuild else replace_reviewed_database(args)
        print(json.dumps(result,default=json_default,indent=2))
        return 0
    since = parse_dt(args.since)
    now = parse_dt(args.now) if args.now else utc_now()
    with closing(open_db(args.db)) as conn:
        init_db(conn)
        conn.commit()
        stats: list[IngestStats] = []
        if not args.no_refresh:
            stats = refresh_lan(
                conn,
                market=args.market,
                taker_trades=args.taker_trades,
                xemm_journals=args.xemm_journal,
            )
        report = None
        if not args.refresh_only:
            report = report_from_db(
                conn,
                market=args.market,
                since=since,
                now=now,
                db_path=args.db,
                capital_usdc=args.capital_usdc,
                bot_state=args.bot_state,
            )
    if args.json:
        print(json.dumps({"refresh": [asdict(s) for s in stats], "report": report}, default=json_default, indent=2))
    else:
        print_human(stats, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

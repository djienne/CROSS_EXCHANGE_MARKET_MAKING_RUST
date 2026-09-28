"""Execution economics shared by local reports and historical rebuilding.

For each logical obligation, realized closes on each venue plus the spread on
matched opposite remaining positions are execution economics, not portfolio
mark-to-market. Fees belong to actual fills. Unknown values never become zero.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ZERO = Decimal(0)

def optional_decimal(raw: Any) -> Decimal | None:
    try:
        value = Decimal(str(raw)) if raw is not None else None
        return value if value is not None and value.is_finite() else None
    except (InvalidOperation, ValueError):
        return None

def parse_timestamp(value: Any) -> datetime | None:
    """ISO-8601 text as aware UTC. Rust writes nanosecond fractions, which Python < 3.11
    rejects, so they are trimmed to microseconds; naive times are UTC."""
    if not isinstance(value, str):
        return None
    try:
        at = datetime.fromisoformat(re.sub(r"(\.\d{6})\d+(?=[+-]\d\d:\d\d$)", r"\1", value.strip().replace("Z", "+00:00")))
    except ValueError:
        return None
    return at.replace(tzinfo=timezone.utc) if at.tzinfo is None else at.astimezone(timezone.utc)

def event_time(row: dict[str, Any]) -> datetime | None:
    detail = row.get("detail")
    for obj in ([detail, row] if isinstance(detail, dict) else [row]):
        for name in ("event_time_ms", "timestamp_ms", "ts_ms"):
            value = optional_decimal(obj.get(name))
            if value is not None and value > 0:
                try:
                    sec, ms = divmod(int(value), 1000)
                    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=sec, milliseconds=ms)
                except (OverflowError, ValueError):
                    pass
        for name in ("timestamp", "ts", "time", "created_at"):
            at = parse_timestamp(obj.get(name))
            if at is not None:
                return at
    return None

def venue_name(raw: Any) -> str:
    name = str(raw).lower()
    return "lighter" if name in {"lighter", "hyperliquid", "hl"} else name

def fill_fee(detail: dict[str, Any], *, trusted: bool) -> Decimal | None:
    notional = optional_decimal(detail.get("notional_usd", detail.get("usd_amount")))
    ticks = optional_decimal(detail.get("fee_ticks"))
    if detail.get("fee_ticks") is not None:
        # An explicit null fee_usd next to a rate is the bot marking contradictory evidence.
        if (notional is None or ticks is None or not isinstance(detail.get("maker"), bool)
                or detail.get("fee_complete") is False or ("fee_usd" in detail and detail["fee_usd"] is None)):
            return None
        # Native selected rate/notional can repair an older derived fee_usd field.
        return abs(notional) * ticks / Decimal(1_000_000)
    if trusted and detail.get("fee_complete", True):
        return optional_decimal(detail.get("fee_usd"))
    return None

@dataclass
class Fill:
    venue: str
    side: str
    qty: Decimal
    quote: Decimal | None
    fee: Decimal | None
    timestamp: datetime | None
    identity: str
    attempt_id: str
    source: dict[str, Any]
    stored_key: str | None = None

def _fill(detail: dict[str, Any], row: dict[str, Any], identity: str, *, venue: str,
          side: str, qty: Any, quote: Any = None, price: Any = None,
          fee: Decimal | None = None, attempt_id: str = "") -> Fill:
    q = optional_decimal(qty)
    if q is None or q < 0 or side.lower() not in {"buy", "sell"} or venue not in {"aster", "lighter"}:
        raise ValueError("invalid fill quantity, side or venue")
    n = optional_decimal(quote)
    px = optional_decimal(price)
    if n is None and px is not None and px > 0:
        n = q * px
    if n is not None and (n < 0 or (q > 0 and n == 0)):
        raise ValueError("invalid fill notional")
    return Fill(venue, side.lower(), q, n, fee, event_time(row), identity, attempt_id, row)

def calculate(fills: list[Fill], *, legacy: bool = False) -> dict[str, Any]:
    states = {v: {"qty": ZERO, "avg": ZERO, "realized": ZERO, "volume": ZERO, "quote": ZERO,
                  "fee": ZERO, "prices_complete": True, "fees_complete": True} for v in ("aster", "lighter")}
    for f in sorted(fills, key=lambda f: f.timestamp or datetime.min.replace(tzinfo=timezone.utc)):
        st = states[f.venue]
        st["volume"] += f.qty
        st["quote"] += f.quote or ZERO
        if f.fee is None:
            st["fees_complete"] = False
        else:
            st["fee"] += f.fee
        delta = f.qty if f.side == "buy" else -f.qty
        old = st["qty"]
        st["qty"] += delta
        if f.qty == 0:
            continue
        if f.quote is None:
            st["prices_complete"] = False
            continue
        px = f.quote / f.qty
        if old == 0 or (old > 0) == (delta > 0):
            st["avg"] = (abs(old) * st["avg"] + f.quote) / (abs(old) + f.qty)
        else:
            closed = min(abs(old), f.qty)
            st["realized"] += closed * (px - st["avg"]) * (1 if old > 0 else -1)
            if st["qty"] == 0:
                st["avg"] = ZERO
            elif (st["qty"] > 0) != (old > 0):
                st["avg"] = px
    a, h = states["aster"], states["lighter"]
    opposite = a["qty"] * h["qty"] < 0
    matched = min(abs(a["qty"]), abs(h["qty"])) if opposite else ZERO
    prices_known = a["prices_complete"] and h["prices_complete"]
    realized = a["realized"] + h["realized"]
    spread = matched * (h["avg"] - a["avg"]) * (1 if a["qty"] > 0 else -1) if opposite else ZERO
    gross = realized + spread if prices_known else None
    known_fees = a["fee"] + h["fee"]
    fees_known = a["fees_complete"] and h["fees_complete"]
    net = gross - known_fees if gross is not None and fees_known else None
    return {
        "schema_version": 2,
        "economic_status": "confirmed" if net is not None else ("legacy_unverified" if legacy else "incomplete"),
        "qty": a["volume"], "aster_qty": a["volume"], "lighter_qty": h["volume"],
        "matched_qty": matched, "residual_qty": a["qty"] + h["qty"],
        "aster_px": a["quote"] / a["volume"] if a["volume"] and a["prices_complete"] else None,
        "lighter_px": h["quote"] / h["volume"] if h["volume"] and h["prices_complete"] else None,
        "gross_pnl_usdc": gross, "fees_usdc": known_fees if fees_known else None,
        "venue_realized_pnl_usdc": realized if prices_known else None,
        "execution_spread_usdc": spread if prices_known else None,
        "known_fees_usdc": known_fees, "aster_fee_usdc": a["fee"] if a["fees_complete"] else None,
        "lighter_fee_usdc": h["fee"] if h["fees_complete"] else None,
        "net_pnl_usdc": net, "fills": fills,
    }

def taker_economics(row: dict[str, Any]) -> dict[str, Any] | None:
    direction = str(row.get("direction", "")).upper()
    # Rows name the hedge leg `hedge_*` since it may be Hyperliquid; older ones `lighter_*`.
    key_a, key_h = row.get("aster_order_id"), row.get("hedge_client_order_index", row.get("lighter_client_order_index"))
    if key_a is None or key_h is None or not row.get("market"):
        return None
    if direction == "RECOVERY":
        result = calculate([])
        result.update(gross_pnl_usdc=optional_decimal(row.get("actual_gross_usd")),
            fees_usdc=optional_decimal(row.get("actual_fees_usd")),
            net_pnl_usdc=optional_decimal(row.get("actual_net_usd")),
            economic_status="estimated", key=f"taker:recovery:{key_a}:{key_h}:{row.get('timestamp')}")
    else:
        # SELL_<X>_BUY_<Y>. The `aster_*` slot is the first leg, named by `first_venue` (Aster if absent).
        first = str(row.get("first_venue", "aster")).upper()
        sold, _, bought = direction.removeprefix("SELL_").partition("_BUY_")
        if not direction.startswith("SELL_") or sold == bought or first not in (sold, bought):
            return None
        fills = []
        for venue, side, identity in zip(("aster", "lighter"), ("sell", "buy") if sold == first else ("buy", "sell"), (key_a, key_h)):
            d = row.get("aster_fill") if venue == "aster" else row.get("hedge_fill", row.get("lighter_fill"))
            if not isinstance(d, dict):
                return None
            trusted = row.get("schema_version", 1) >= 2 and d.get("fee_provenance") == "venue"
            fee = optional_decimal(d.get("fee_usd")) if trusted else None
            fills.append(_fill(d, row, f"{venue}:{identity}", venue=venue, side=side,
                qty=d.get("qty", row.get("qty")), quote=d.get("notional"), price=d.get("vwap"), fee=fee,
                attempt_id=str(identity)))
        result = calculate(fills, legacy=row.get("schema_version", 1) < 2)
        result["key"] = f"taker:{key_a}:{key_h}"
        if row.get("economic_status") == "estimated":
            result["economic_status"] = "estimated"
        if row.get("schema_version", 1) >= 2 and row.get("economic_status") == "confirmed":
            for producer, computed in [("actual_gross_usd", "gross_pnl_usdc"), ("actual_fees_usd", "fees_usdc"), ("actual_net_usd", "net_pnl_usdc")]:
                value = optional_decimal(row.get(producer))
                if value is None or result[computed] is None or abs(value-result[computed]) > Decimal("0.00000001"):
                    result["economic_status"] = "incomplete"
                    result["net_pnl_usdc"] = None
                    break
    result.update(market=str(row["market"]), direction=direction, timestamp=event_time(row), raw=row,
        aster_order_id=str(key_a), lighter_client_order_index=str(key_h), cloid=None)
    return result

def xemm_journal(path: Path, market: str, *, now: datetime | None = None) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    malformed = 0
    def group(logical: str) -> dict[str, Any]:
        return groups.setdefault(logical, {"fills": {}, "attempts": {}, "attempt_times": {}, "unidentified": {}, "legacy": False, "timestamp": None, "raw": None})
    if not path.exists():
        return {"trades": [], "malformed_rows": 0}
    with path.open(encoding="utf-8") as stream:
        for line_no, text in enumerate(stream, 1):
            if not text.strip():
                continue
            try:
                row = json.loads(text)
                if not isinstance(row, dict) or row.get("market") != market:
                    continue
                kind = row.get("kind")
                if kind not in {"maker_fill", "execution_trade", "execution_progress", "maker_order_progress", "fill", "hedge_fill"}:
                    continue
                d = row.get("detail")
                if not isinstance(d, dict):
                    raise ValueError("missing economic detail")
                at = event_time(row)
                if now is not None and at is not None and at > now:
                    continue
                logical = str(d.get("logical_id", d.get("cloid", "")))
                if not logical:
                    raise ValueError("missing logical obligation id")
                g = group(logical)
                if at is not None and (g["timestamp"] is None or at > g["timestamp"]):
                    g["timestamp"] = at
                g["raw"] = row
                g["source_line"] = line_no
                trusted = row.get("schema_version", 1) >= 2 and row.get("economic_status") == "confirmed"
                g["legacy"] |= row.get("schema_version", 1) < 2
                if kind in {"execution_progress", "maker_order_progress"}:
                    attempt = str(d.get("attempt_id", d.get("client_id", "")))
                    qty = optional_decimal(d.get("cumulative_qty"))
                    if not attempt or qty is None or qty < 0:
                        raise ValueError("invalid cumulative execution")
                    old = g["attempts"].get(attempt)
                    if old is None or qty >= optional_decimal(old["detail"]["cumulative_qty"]):
                        if old is None or qty > optional_decimal(old["detail"]["cumulative_qty"]):
                            g["attempt_times"][attempt]=at
                        g["attempts"][attempt] = row
                    continue
                if kind == "maker_fill":
                    identity = f"aster:{d.get('order_id')}:{d.get('trade_id')}"
                    f = _fill(d,row,identity,venue="aster",side=str(d.get("maker_side","")),qty=d.get("qty"),
                        quote=d.get("notional_usd"),price=d.get("px"),fee=fill_fee(d,trusted=trusted),
                        attempt_id=str(d.get("client_id",d.get("order_id",""))))
                elif kind == "execution_trade":
                    if d.get("identity_complete") is False:
                        attempt=str(d.get("attempt_id", ""))
                        lower=optional_decimal(d.get("qty"))
                        previous=g["unidentified"].get(attempt, ZERO)
                        g["unidentified"][attempt]=max(previous,lower) if previous is not None and lower is not None and lower>=0 else None
                        continue
                    venue = venue_name(d.get("venue"))
                    identity = f"{venue}:{d.get('order_id')}:{d.get('trade_id')}"
                    f = _fill(d,row,identity,venue=venue,side=str(d.get("side","")),qty=d.get("qty"),
                        quote=d.get("notional_usd"),price=d.get("px"),fee=fill_fee(d,trusted=trusted),
                        attempt_id=str(d.get("attempt_id","")))
                elif kind == "fill":
                    side = str(d.get("side","")).lower()
                    if side not in {"buy", "sell"}:
                        raise ValueError("invalid maker side")
                    identity = f"legacy-maker:{logical}"
                    f = _fill(d,row,identity,venue="aster",side="sell" if side == "buy" else "buy",qty=d.get("qty"),
                        price=d.get("avg_aster_px"),fee=fill_fee(d,trusted=trusted),attempt_id=logical)
                else:
                    identity = f"legacy-hedge:{logical}:{d.get('trade_id',row.get('mono_ns',line_no))}"
                    f = _fill(d,row,identity,venue="lighter",side=str(d.get("side","")),qty=d.get("qty"),
                        price=d.get("px"),fee=fill_fee(d,trusted=trusted),attempt_id=logical)
                g["fills"][identity] = f
            except (ValueError, TypeError, InvalidOperation, OverflowError):
                malformed += 1
    trades = []
    for logical, g in groups.items():
        fills = list(g["fills"].values())
        for attempt, row in g["attempts"].items():
            d = row["detail"]
            venue = venue_name(d.get("venue", "aster"))
            def belongs(f: Fill) -> bool:
                raw = f.source.get("detail", {})
                return f.venue == venue and (f.attempt_id == attempt
                    or (d.get("client_id") is not None and raw.get("client_id") == d["client_id"])
                    or (d.get("venue_order_id") is not None and str(raw.get("order_id")) == str(d["venue_order_id"])))
            covered = [f for f in fills if belongs(f)]
            qty = optional_decimal(d.get("cumulative_qty")) or ZERO
            raw_qty = sum((f.qty for f in covered), ZERO)
            if raw_qty >= qty:
                continue
            total_quote = optional_decimal(d.get("cumulative_quote_usd"))
            total_fee = optional_decimal(d.get("cumulative_fee_usd")) if d.get("fee_complete") else None
            quote = total_quote - sum((f.quote for f in covered), ZERO) if total_quote is not None and all(f.quote is not None for f in covered) else None
            fee = total_fee - sum((f.fee for f in covered), ZERO) if total_fee is not None and all(f.fee is not None for f in covered) else None
            try:
                coverage=_fill(d,row,f"coverage:{attempt}",venue=venue,side=str(d.get("side","")),qty=qty-raw_qty,
                    quote=quote,fee=fee,attempt_id=attempt)
                coverage.timestamp=g["attempt_times"].get(attempt)
                fills.append(coverage)
            except ValueError:
                malformed += 1
        if not any(f.qty > 0 for f in fills):
            continue
        result = calculate(fills, legacy=g["legacy"])
        result["qty"] = sum((f.qty for f in fills if f.source.get("kind") in {"maker_fill", "maker_order_progress", "fill"}
            and not f.source.get("detail",{}).get("reduce_only",False)), ZERO)
        for attempt,lower in g["unidentified"].items():
            proof=g["attempts"].get(attempt,{}).get("detail",{})
            cumulative=optional_decimal(proof.get("cumulative_qty"))
            if lower is None or cumulative is None or cumulative<lower or not proof.get("terminal"):
                result["economic_status"]="incomplete"
                result["net_pnl_usdc"]=None
        first_maker = next((f for f in fills if f.venue == "aster"), None)
        result.update(key=f"xemm:{logical}", cloid=logical, logical_id=logical, market=market,
            timestamp=max((f.timestamp for f in fills if f.timestamp is not None), default=None), raw=g["raw"], source_line=g.get("source_line"),
            hedge_side=("sell" if first_maker.side == "buy" else "buy") if first_maker else None,
            terminal=bool(g["attempts"]) and all(r["detail"].get("terminal") for r in g["attempts"].values()))
        trades.append(result)
    return {"trades": trades, "malformed_rows": malformed}

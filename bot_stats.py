#!/usr/bin/env python3
"""Execution quality and health of `run`: python3 bot_stats.py --market HYPE [--dry-run] [--json].

combined_pnl.py says how much was made; this says why. It covers five things:
- how much of the expected edge each taker trade kept, and which leg lost the rest;
- how often the entry gate fired;
- what XEMM trades earned and how fast they were hedged;
- what the controller did, and how XEMM handed the rights to the taker;
- for a dry run, whether the simulator stayed faithful to its latency model.
The model's targets are in bot.toml [dry_run].
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from combined_pnl import default_since, iso, iter_jsonl, parse_dt, report_roots, utc_now
from economics import parse_timestamp, xemm_journal


def pct(xs: list[float], q: float) -> float | None:
    """Nearest-rank quantile, as the simulator's diagnostics compute it."""
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def dist(xs: list[float]) -> dict[str, Any]:
    return {"n": len(xs), "mean": round(sum(xs) / len(xs), 3), "p50": round(pct(xs, 0.5), 3),
            "p90": round(pct(xs, 0.9), 3)} if xs else {"n": 0}


def rows(path: Path, since: datetime, now: datetime) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [r for _, r in iter_jsonl(path) if (at := parse_timestamp(r.get("timestamp"))) is not None and since <= at <= now]


def ms(at: str) -> float:
    return parse_timestamp(at).timestamp() * 1000


def taker(runs: Path, market: str, since: datetime, now: datetime) -> dict[str, Any]:
    opps = rows(runs / f"opportunities_{market}.jsonl", since, now)
    final: dict[str, dict[str, Any]] = {}
    for r in rows(runs / f"executions_{market}.jsonl", since, now):
        final[r["execution_id"]] = r  # append-only; the last row of an execution is its outcome
    executed = sorted((r for r in opps if r.get("decision") == "would_execute"), key=lambda r: ms(r["timestamp"]))
    trades = collections.defaultdict(list)
    for r in final.values():
        econ, a, lf = r.get("actual_economics"), r.get("aster_fill"), r.get("lighter_fill")
        if r.get("outcome") != "success" or not econ:
            continue
        # The gate records every executed opportunity just before its orders go out, with the
        # depth-weighted prices the expected edge was computed from.
        start = ms(r["started_at"])
        opp = next((o for o in reversed(executed) if o["direction"] == r["direction"]
                    and 0 <= start - ms(o["timestamp"]) < 1000), None)
        notional = float(a["notional"])
        trades["expected_gross_bps"].append(float(r["gross_edge_bps"]))
        trades["realized_gross_bps"].append(float(econ["gross_usd"]) / notional * 1e4)
        trades["fees_bps"].append(float(econ["fees_usd"]) / notional * 1e4)
        trades["net_bps"].append(float(econ["net_bps"]))
        trades["net_usd"].append(float(econ["net_usd"]))
        if opp:
            buy_aster = r["direction"] == "SELL_LIGHTER_BUY_ASTER"
            aster_px, lighter_px = float(a["vwap"]), float(lf["vwap"])
            buy, sell = float(opp["buy_px"]), float(opp["sell_px"])
            # Positive = filled worse than the decision price; the two legs sum to the edge lost.
            trades["aster_slippage_bps"].append((aster_px - buy) / buy * 1e4 if buy_aster else (sell - aster_px) / sell * 1e4)
            trades["lighter_slippage_bps"].append((sell - lighter_px) / sell * 1e4 if buy_aster else (lighter_px - buy) / buy * 1e4)
            trades["aster_book_age_ms"].append(float(opp["aster_book_age_ms"]))
            trades["lighter_book_age_ms"].append(float(opp["lighter_book_age_ms"]))
        if (m := re.search(r'\\"updateTime\\":(\d+)', r.get("aster_submit", ""))):
            trades["aster_fill_ms"].append(int(m[1]) - start)
        times = [e["event_time_ms"] for e in r.get("lighter_fee_evidence") or [] if e.get("event_time_ms")]
        if times:
            trades["lighter_fill_ms"].append(max(times) - start)
    expected, realized = sum(trades["expected_gross_bps"]), sum(trades["realized_gross_bps"])
    last = opps[-1] if opps else {}
    hours = (now - since).total_seconds() / 3600
    return {
        "trades": len(trades["net_usd"]), "wins": sum(x > 0 for x in trades["net_usd"]),
        "net_usd": round(sum(trades["net_usd"]), 6),
        "edge_kept": round(realized / expected, 3) if expected else None,
        **{k: dist(v) for k, v in trades.items() if k != "net_usd"},
        "outcomes": dict(collections.Counter(r.get("outcome") for r in final.values())),
        "gate": {"decisions": dict(collections.Counter(r.get("decision") for r in opps)),
                 "samples_per_hour": round(len(opps) / hours, 2) if hours > 0 else None,
                 "threshold_bps": last.get("gate_threshold_bps"), "history_samples": last.get("history_sample_count")},
    }


def xemm(runs: Path, market: str, since: datetime, now: datetime) -> dict[str, Any]:
    path = runs / f"bot-{market}-journal.jsonl"
    if not path.exists():
        return {"trades": 0}
    stats = collections.defaultdict(list)
    trades = [t for t in xemm_journal(path, market, now=now)["trades"] if t["timestamp"] and since <= t["timestamp"] <= now]
    for t in trades:
        notional = t["aster_px"] * t["aster_qty"] if t["aster_px"] and t["aster_qty"] else None
        if notional and t["gross_pnl_usdc"] is not None:
            stats["gross_bps"].append(float(t["gross_pnl_usdc"] / notional * 10_000))
        if notional and t["net_pnl_usdc"] is not None:
            stats["net_bps"].append(float(t["net_pnl_usdc"] / notional * 10_000))
        firsts = {v: min((f.timestamp for f in t["fills"] if f.venue == v and f.timestamp), default=None) for v in ("aster", "lighter")}
        if all(firsts.values()):
            stats["hedge_delay_ms"].append((firsts["lighter"] - firsts["aster"]).total_seconds() * 1000)
    return {"trades": len(trades), "incomplete": sum(t["net_pnl_usdc"] is None for t in trades),
            "residual": sum(t["residual_qty"] != 0 for t in trades), **{k: dist(v) for k, v in stats.items()}}


def controller(runs: Path, market: str, since: datetime, now: datetime) -> dict[str, Any]:
    events = rows(runs / f"bot-{market}.events.jsonl", since, now)
    state_path = runs / f"bot-{market}.state.json"
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    # XEMM's hand-overs (strategy.rs `Yield`): its cancels until the grant, the taker's hold,
    # and the re-read of the positions the taker left.
    handover: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    journal = runs / f"bot-{market}-journal.jsonl"
    lo, hi = since.timestamp() * 1000, now.timestamp() * 1000
    for _, r in iter_jsonl(journal) if journal.exists() else ():
        if r.get("kind") in ("yield", "yield_withdrawn", "rights_returned", "resumed") and lo <= r.get("ts_ms", 0) <= hi:
            handover[r["kind"]].append(r["detail"])
    return {
        "events": dict(collections.Counter(e["kind"] for e in events)),
        "handovers": {"granted": len(handover["yield"]), "withdrawn": len(handover["yield_withdrawn"]),
                      "fills_while_yielding": sum(d["fills"] for d in handover["yield"]),
                      **{key: dist([d[key] for d in handover[kind]]) for kind, key in
                         (("yield", "cancel_to_grant_ms"), ("rights_returned", "held_ms"), ("resumed", "resume_ms"))}},
        "halts": [f'{e["timestamp"][:19]} {e.get("reason")}' for e in events if e["kind"] == "safe_halt"],
        "network_paused_s": sum(e.get("paused_secs", 0) for e in events if e["kind"] == "network_resume"),
        "rights": state.get("rights"), "equity_pnl_usd": state.get("pnl", {}).get("equity_pnl_usdc"),
    }


def simulator(runs: Path, market: str, since: datetime, now: datetime) -> dict[str, Any] | None:
    path = runs / f"sim-{market}.diag.jsonl"
    if not path.exists():
        return None
    diag = [r for _, r in iter_jsonl(path) if since.timestamp() * 1000 <= r["ts_ms"] <= now.timestamp() * 1000]
    if not diag:
        return None
    minutes = sum(r["window_s"] for r in diag) / 60
    # A window whose scheduler ran >500 ms late is a host freeze (sleep, VM pause), not the model.
    frozen = {r["ts_ms"] for r in diag if (r["lateness_ms"].get("max") or 0) > 500}
    out: dict[str, Any] = {"minutes": round(minutes, 1), "host_frozen_windows": len(frozen),
                           "scheduler_lateness_p99_ms": pct([r["lateness_ms"]["p99"] for r in diag if r["lateness_ms"].get("n")], 0.5)}
    for venue in ("aster", "lighter"):
        v = [r[venue] for r in diag]
        frames, late = sum(x["frames"] for x in v), sum(x["late_frames"] for x in v)
        clean = [r[venue] for r in diag if r["ts_ms"] not in frozen]
        late_clean = sum(x["late_frames"] for x in clean) / max(sum(x["frames"] for x in clean), 1)
        per_window = lambda key, q: pct([x[key][q] for x in v if x.get(key, {}).get("n")], 0.5)  # median window
        rejects = collections.Counter()
        for x in v:
            rejects.update(x["rejects"])
        out[venue] = {
            "late_frames_pct": round(100 * late / max(frames, 1), 3), "late_frames_pct_unfrozen": round(100 * late_clean, 3),
            "gaps": sum(x["gaps"] for x in v),
            "book_lag_p99_ms": pct([x["lag_ms"]["book"]["p99"] for x in v if x["lag_ms"].get("book", {}).get("n")], 0.5),
            "rtt_p50_ms": per_window("rtt_ms", "p50"), "rtt_p99_ms": per_window("rtt_ms", "p99"),
            "requests_per_min": round(sum(x["requests"] for x in v) / minutes, 1),
            "orders": sum(x["orders"] for x in v), "rejects": dict(rejects),
            "maker_fills": sum(x["maker_fills"] for x in v), "taker_fills": sum(x["taker_fills"] for x in v),
            "maker_wait_p50_ms": per_window("maker_wait_ms", "p50"), "queue_ahead_p50": per_window("queue_ahead", "p50"),
            "prints_inside_spread_pct": round(100 * sum(x["prints_inside_spread"] for x in v) / max(sum(x["prints"] for x in v), 1), 2),
            "prints_over_visible_pct": round(100 * sum(x["prints_over_visible"] for x in v) / max(sum(x["prints"] for x in v), 1), 2),
            "account": {k: v[-1]["account"][k] for k in ("equity", "fees", "funding")},
        }
    return out


def report(runs: Path, market: str, since: datetime, now: datetime) -> dict[str, Any]:
    return {"runs": str(runs), "since": iso(since), "now": iso(now), "hours": round((now - since).total_seconds() / 3600, 2),
            "taker": taker(runs, market, since, now), "xemm": xemm(runs, market, since, now),
            "controller": controller(runs, market, since, now), "simulator": simulator(runs, market, since, now)}


def print_human(value: Any, indent: int = 0) -> None:
    for key, item in value.items():
        if isinstance(item, dict) and item and not all(isinstance(x, (int, float, str, type(None))) for x in item.values()):
            print(" " * indent + f"{key}:")
            print_human(item, indent + 2)
        else:
            text = ", ".join(f"{k} {x}" for k, x in item.items()) if isinstance(item, dict) else item
            print(" " * indent + f"{key}: {text}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--market", default="HYPE")
    parser.add_argument("--dry-run", action="store_true", help="Report the dry run (LIGHTER_ASTER_BOT/runs/dry-run/).")
    parser.add_argument("--since", default=None, help="UTC/RFC3339 start. Default: as combined_pnl.py.")
    parser.add_argument("--now", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    runs = report_roots(Path(__file__).resolve().parent, args.dry_run)[1]
    since = parse_dt(args.since or default_since(runs, args.market, args.dry_run))
    result = report(runs, args.market, since, parse_dt(args.now) if args.now else utc_now())
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_human(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

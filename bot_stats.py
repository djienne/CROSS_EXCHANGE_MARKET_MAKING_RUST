#!/usr/bin/env python3
"""Execution quality and health of `run`: python3 bot_stats.py --market HYPE [--dry-run] [--runs DIR] [--json].

combined_pnl.py says how much was made; this says why. It covers five things:
- how much of the expected edge each taker trade kept, and which leg lost the rest;
- how often the entry gate fired;
- what XEMM trades earned and how fast they were hedged, and how its quotes were refreshed;
- what the controller did, and how XEMM handed the rights to the taker;
- for a dry run, whether the simulator stayed faithful to its latency model.
The model's targets are in bot.toml [dry_run].
Taker fields named aster/lighter describe the first/hedge legs; the venue lists identify them.
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
    """Sorted sample at min(N-1, floor(q*N)); p50 is the upper median for even N.

    Simulator diagnostics instead use nearest rank (ceil(q*N)-1).
    """
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
            buy_aster = r["direction"].endswith("_BUY_" + str(r.get("first_venue", "aster")).upper())
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
        "first_venues": sorted({r.get("first_venue") or "aster" for r in final.values()}),
        "hedge_venues": sorted({r.get("hedge_venue") or "lighter" for r in final.values()}),
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
        for f in t["fills"]:
            # The Aster user stream also calls a reduce-only recovery MARKET a maker_fill.
            if f.venue == "aster" and (f.source.get("kind") != "maker_fill" or f.source.get("detail", {}).get("reduce_only")):
                continue
            if f.timestamp and since <= f.timestamp <= now and f.quote and f.fee is not None:
                stats["maker_fees_bps" if f.venue == "aster" else "hedge_fees_bps"].append(float(f.fee / f.quote * 10_000))
    makers = [f for t in trades for f in t["fills"] if f.source.get("kind") == "maker_fill"
              and not f.source.get("detail", {}).get("reduce_only")
              and f.qty > 0 and f.timestamp and since <= f.timestamp <= now]
    # Venue clocks can differ. This delay uses the host's monotonic clock, from the hedge
    # obligation's creation to its first observed fill; later fee/backfill notices do not add samples.
    attempts = {f.attempt_id for t in trades for f in t["fills"] if f.venue != "aster"}
    observed = {}
    for _, r in iter_jsonl(path):
        d = r.get("detail", {})
        if (not isinstance(d, dict) or r.get("market") != market or r.get("kind") != "execution_progress" or d.get("purpose") != "hedge"
                or d.get("attempt_id") not in attempts
                or not since.timestamp() * 1000 <= r.get("ts_ms", 0) <= now.timestamp() * 1000
                or float(d.get("cumulative_qty") or 0) <= 0):
            continue
        created, seen = d.get("created_ns"), d.get("observed_ns")
        if created and seen and seen >= created:
            key = d["attempt_id"]
            delay = (seen - created) / 1_000_000
            observed[key] = min(observed.get(key, delay), delay)
    stats["hedge_first_fill_observed_ms"] = list(observed.values())
    hours = (now - since).total_seconds() / 3600
    return {"trades": len(trades), "incomplete": sum(t["net_pnl_usdc"] is None for t in trades),
            "residual": sum(t["residual_qty"] != 0 for t in trades),
            "maker_fills": len(makers), "maker_sides": dict(collections.Counter(f.side for f in makers)),
            "maker_fills_per_hour": round(len(makers) / hours, 2) if hours > 0 else None,
            "known_net_usd": float(sum(t["net_pnl_usdc"] for t in trades if t["net_pnl_usdc"] is not None)),
            **{k: dist(v) for k, v in stats.items()}}


def quotes(runs: Path, market: str, since: datetime, now: datetime) -> dict[str, Any]:
    """XEMM's quoting: records per minute, each refresh's round trip (sent to the venue's
    answer), and the share of the time each side had an order resting."""
    path = runs / f"bot-{market}-journal.jsonl"
    lo, hi = since.timestamp() * 1000, now.timestamp() * 1000
    kinds, answers, side_of = collections.Counter(), collections.Counter(), {}
    asked: dict[str, tuple[str, int, str]] = {}  # side -> client, refresh time, kind
    resting: dict[str, tuple[str, int]] = {}  # side -> (client id, resting since)
    round_trip, amend_trip, up = [], [], collections.Counter()
    for _, r in iter_jsonl(path) if path.exists() else ():
        ts, kind, d = r.get("ts_ms", 0), r.get("kind"), r.get("detail")
        if not lo <= ts <= hi or not isinstance(d, dict):
            continue
        cid, side = d.get("client_id"), d.get("side")
        if kind in ("place", "replace", "amend", "cancel"):
            kinds[kind] += 1
            if cid and side:
                side_of[cid] = side
            if kind in ("replace", "amend"):
                asked[side] = (cid, ts, kind)
        elif kind == "order_update" and (side := side_of.get(cid)):
            state, held = d.get("state"), resting.get(side)
            answers[state] += 1
            request = asked.get(side)
            if request and ((request[2] == "replace" and state == "accepted")
                    or (request[0] == cid and request[2] == "amend" and state in ("amended", "amend_rejected"))):
                elapsed = ts - asked.pop(side)[1]
                round_trip.append(elapsed)
                if request[2] == "amend":
                    amend_trip.append(elapsed)
            elif request and request[0] == cid and request[2] == "amend" and state in ("cancelled", "filled_or_expired"):
                asked.pop(side)
            if state in ("accepted", "amended") and (not held or held[0] != cid):
                # An order that ended unjournaled (a sweep, a restart) counts as resting until
                # the side's next one, so the uptime is an upper bound.
                if held:
                    up[side] += ts - held[1]
                resting[side] = (cid, ts)
            elif state in ("cancelled", "filled_or_expired") and held and held[0] == cid:
                up[side] += ts - resting.pop(side)[1]
    for side, (_, t) in resting.items():
        up[side] += hi - t
    minutes = max(hi - lo, 1) / 60_000
    return {"per_min": {k: round(n / minutes, 2) for k, n in sorted(kinds.items())}, "answers": dict(answers),
            "refresh_round_trip_ms": dist(round_trip),
            "amend_round_trip_ms": dist(amend_trip),
            "uptime_pct": {s: round(100 * t / max(hi - lo, 1), 1) for s, t in sorted(up.items())}}


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
    for venue in (k for k in ("aster", "lighter", "hyperliquid") if k in diag[-1]):
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
            "quotes": quotes(runs, market, since, now),
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
    parser.add_argument("--runs", type=Path, help="Read this runs directory instead of the default live or dry-run directory.")
    parser.add_argument("--since", default=None, help="UTC/RFC3339 start. Default: as combined_pnl.py.")
    parser.add_argument("--now", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    runs = args.runs.resolve() if args.runs else report_roots(Path(__file__).resolve().parent, args.dry_run)[1]
    if args.runs and not runs.is_dir():
        parser.error(f"runs directory does not exist: {runs}")
    since = parse_dt(args.since or default_since(runs, args.market, args.dry_run))
    result = report(runs, args.market, since, parse_dt(args.now) if args.now else utc_now())
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_human(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

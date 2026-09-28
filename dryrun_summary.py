#!/usr/bin/env python3
"""One screen per dry-run market: python3 dryrun_summary.py [--since RFC3339].

It condenses combined_pnl.py (what was made) and bot_stats.py (why) for every market with a
dry-run state file (LIGHTER_ASTER_BOT/runs/dry-run/bot-<market>.state.json). Run those two with
`--dry-run --market <M>` for the full detail.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNS = ROOT / "LIGHTER_ASTER_BOT" / "runs" / "dry-run"


def report(script: str, market: str, since: str | None) -> dict:
    cmd = [sys.executable, str(ROOT / script), "--dry-run", "--market", market, "--json"]
    return json.loads(subprocess.run(cmd + (["--since", since] if since else []), check=True, capture_output=True, text=True).stdout)


def last_trade(market: str) -> str:
    path = RUNS / f"trades_{market}.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    return json.loads(lines[-1])["timestamp"][:19] + "Z" if lines else "never"


def dist(d: dict | None, key: str = "p50") -> str:
    return f"{d[key]}" if d and d.get("n") else "-"


def summary(market: str, since: str | None) -> None:
    pnl, stats = report("combined_pnl.py", market, since), report("bot_stats.py", market, since)
    t, x, c, sim = stats["taker"], stats["xemm"], stats["controller"], stats["simulator"] or {}
    usd = lambda v: f"{float(v):+.4f}" if v is not None else "?"
    print(f"== {market}: {stats['since'][:16]}Z -> {stats['now'][:16]}Z ({stats['hours']} h)")
    print(f"  net $: taker {usd(pnl['taker']['net_pnl_usdc'])} ({pnl['taker']['trades']} trades), "
          f"xemm {usd(pnl['xemm']['net_pnl_usdc'])} ({pnl['xemm']['trades']} trades), total {usd(pnl['total']['net_pnl_usdc'])}")
    gate = t["gate"]
    print(f"  taker: last trade {last_trade(market)}, edge kept {t['edge_kept']}, "
          f"expected/realized gross bps {dist(t['expected_gross_bps'], 'mean')}/{dist(t['realized_gross_bps'], 'mean')}, "
          f"gate {gate['decisions']} at {gate['threshold_bps']} bps")
    h = c["handovers"]
    print(f"  hand-offs to the taker: {h['granted']} granted, cancel->grant p50 {dist(h['cancel_to_grant_ms'])} ms, "
          f"held p50 {dist(h['held_ms'])} ms, resume p50 {dist(h['resume_ms'])} ms, fills while yielding {h['fills_while_yielding']}")
    q = stats["quotes"]
    print(f"  xemm quotes/min {q['per_min']}, refresh round trip p50 {dist(q['refresh_round_trip_ms'])} ms, "
          f"resting {q['uptime_pct']}%")
    print(f"  halts: {len(c['halts'])}{' (last ' + c['halts'][-1] + ')' if c['halts'] else ''}, network paused {c['network_paused_s']} s")
    for venue in ("aster", "lighter", "hyperliquid"):
        if venue in sim:
            v = sim[venue]
            print(f"  sim {venue}: {v['orders']} orders, maker/taker fills {v['maker_fills']}/{v['taker_fills']}, "
                  f"rejects {v['rejects'] or 0}, book lag p99 {v['book_lag_p99_ms']} ms, late frames {v['late_frames_pct_unfrozen']}%")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since", default=None, help="UTC/RFC3339 start. Default: each market's dry-run start.")
    args = parser.parse_args()
    markets = sorted(p.name[len("bot-"):-len(".state.json")] for p in RUNS.glob("bot-*.state.json"))
    if not markets:
        print(f"no dry-run state in {RUNS}")
        return 1
    for market in markets:
        summary(market, args.since)
    return 0


if __name__ == "__main__":
    sys.exit(main())

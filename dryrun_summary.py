#!/usr/bin/env python3
"""One screen per dry-run market: python3 dryrun_summary.py [--since RFC3339] [--runs <replay>].

It condenses combined_pnl.py (what was made) and bot_stats.py (why) for every market with a
dry-run state file (LIGHTER_ASTER_BOT/runs/dry-run/bot-<market>.state.json). Run those two with
`--dry-run --market <M>` for the full detail. `--runs LIGHTER_ASTER_BOT/runs/replay/<run>` reports
a replay instead, over its tape window (`replay.json`); its times are the tape's, shifted.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNS = ROOT / "LIGHTER_ASTER_BOT" / "runs" / "dry-run"


def report(script: str, runs: Path, market: str, since: str | None, now: str | None) -> dict:
    cmd = [sys.executable, str(ROOT / script), "--dry-run", "--runs", str(runs), "--market", market, "--json"]
    cmd += (["--since", since] if since else []) + (["--now", now] if now else [])
    return json.loads(subprocess.run(cmd, check=True, capture_output=True, text=True).stdout)


def last_trade(runs: Path, market: str) -> str:
    path = runs / f"trades_{market}.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    return json.loads(lines[-1])["timestamp"][:19] + "Z" if lines else "never"


def dist(d: dict | None, key: str = "p50") -> str:
    return f"{d[key]}" if d and d.get("n") else "-"


def summary(runs: Path, market: str, since: str | None, now: str | None) -> None:
    pnl, stats = report("combined_pnl.py", runs, market, since, now), report("bot_stats.py", runs, market, since, now)
    t, x, c, sim = stats["taker"], stats["xemm"], stats["controller"], stats["simulator"] or {}
    usd = lambda v: f"{float(v):+.4f}" if v is not None else "?"
    print(f"== {market}: {stats['since'][:16]}Z -> {stats['now'][:16]}Z ({stats['hours']} h)")
    print(f"  net $: taker {usd(pnl['taker']['net_pnl_usdc'])} ({pnl['taker']['trades']} trades), "
          f"xemm {usd(pnl['xemm']['net_pnl_usdc'])} ({pnl['xemm']['trades']} trades), total {usd(pnl['total']['net_pnl_usdc'])}")
    gate = t["gate"]
    print(f"  taker: last trade {last_trade(runs, market)}, edge kept {t['edge_kept']}, "
          f"expected/realized gross bps {dist(t['expected_gross_bps'], 'mean')}/{dist(t['realized_gross_bps'], 'mean')}, "
          f"gate {gate['decisions']} at {gate['threshold_bps']} bps")
    h = c["handovers"]
    print(f"  hand-offs to the taker: {h['granted']} granted, cancel->grant p50 {dist(h['cancel_to_grant_ms'])} ms, "
          f"held p50 {dist(h['held_ms'])} ms, resume p50 {dist(h['resume_ms'])} ms, fills while yielding {h['fills_while_yielding']}")
    q = stats["quotes"]
    print(f"  xemm quotes/min {q['per_min']}, refresh round trip p50 {dist(q['refresh_round_trip_ms'])} ms, "
          f"resting {q['uptime_pct']}%")
    print(f"  halts: {len(c['halts'])}{' (last ' + c['halts'][-1] + ')' if c['halts'] else ''}, network paused {c['network_paused_s']} s")
    if sim:  # a busy host makes the simulated venues late too: a replay needs these near zero
        print(f"  scheduler: lateness p99 {sim['scheduler_lateness_p99_ms']} ms (median minute), "
              f"{sim['host_frozen_windows']} of {sim['minutes']:.0f} minutes over 500 ms late")
    for venue in ("aster", "lighter", "hyperliquid"):
        if venue in sim:
            v = sim[venue]
            print(f"  sim {venue}: {v['orders']} orders, maker/taker fills {v['maker_fills']}/{v['taker_fills']}, "
                  f"rejects {v['rejects'] or 0}, book lag p99 {v['book_lag_p99_ms']} ms, late frames {v['late_frames_pct_unfrozen']}%")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since", default=None, help="UTC/RFC3339 start. Default: each market's dry-run start.")
    parser.add_argument("--runs", type=Path, default=None, help="A replay's directory (LIGHTER_ASTER_BOT/runs/replay/<run>).")
    args = parser.parse_args()
    runs = RUNS
    if args.runs:  # the reports default to its window
        about = json.loads((args.runs / "replay.json").read_text(encoding="utf-8"))
        runs = args.runs / "dry-run"
        print(f"replay of {about['from']} -> {about['until']} (latency x{about.get('latency_k', 1)}, Lighter {about.get('lighter', 'standard')}, "
              f"seed {about.get('seed', '?')}, commit {about.get('commit', '?')}), shifted {about['shift_ms'] / 86_400_000:.3f} days; "
              f"gate seeded with {about['gate_samples']} samples")
    markets = sorted(p.name[len("bot-"):-len(".state.json")] for p in runs.glob("bot-*.state.json"))
    if not markets:
        print(f"no dry-run state in {runs}")
        return 1
    for market in markets:
        summary(runs, market, args.since, None)
    return 0


if __name__ == "__main__":
    sys.exit(main())

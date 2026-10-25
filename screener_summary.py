#!/usr/bin/env python3
"""The screener's best routes so far: python3 screener_summary.py [--top N] [--min-days D] [--local] [report options].

It scores the Tokyo VPS's recording (SCREENER/data-tokyo, pulled daily by `python vps.py pull`;
clean feeds, from 2026-10-10 12:18Z) unless --local picks this PC's (SCREENER/data, whose feeds
stall for tens of seconds). It runs SCREENER's report in Docker (`docker compose run --rm report
--json`, 8 GB cap) and prints the best routes by the report's own `best` fixed strategy. The report
holds the days in memory, ~12x their files' size, so this splits the pairs into enough
`--pair-groups` passes for ~4 GB on average, under the cap. Report options (--since, --until, --lighter
premium, --latency 2, --pair-groups) pass through; SCREENER/README.md explains them and the
columns. $/day use the configured clip ($13 by default).
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path

SCREENER = Path(__file__).resolve().parent / "SCREENER"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--min-days", type=float, default=1.0, help="Hide routes with less data (default 1 day).")
    parser.add_argument("--local", action="store_true", help="Score this PC's recording instead of the Tokyo VPS's.")
    args, report_args = parser.parse_known_args()
    data = "data" if args.local else "data-tokyo"
    if not any(a.startswith("--pair-groups") for a in report_args):
        # ~12x the files in memory (2026-10-12..18: 1.57 GB of files took over 16 GB). The passes are
        # uneven (2026-10-25, 8 of them: 3.9 to 7.0 GB), hence ~4 GB on average. Ponytail: counts every
        # file, so a --since/--until window gets more passes than it needs (slower, not wrong).
        gb = sum(p.stat().st_size for p in (SCREENER / data).glob("*.screen.zst")) / 1e9
        report_args += ["--pair-groups", str(max(1, math.ceil(12 * gb / 4)))]
    print(f"data: SCREENER/{data} ({'this PC' if args.local else 'Tokyo VPS'})")
    out = subprocess.run(["docker", "compose", "run", "--rm", "-T", "report", "--json", "--data", data, *report_args],
                         cwd=SCREENER, check=True, capture_output=True, text=True).stdout
    report = json.loads(out)
    pairs = sorted(report["pairs"], key=lambda p: p["best_usd_day"], reverse=True)
    shown = [p for p in pairs if p["days"] >= args.min_days]
    most_days = max((p["days"] for p in pairs), default=0)
    print(f"{len(pairs)} routes, up to {most_days:.1f} days; lighter {report['lighter_tier']}, latency x{report['latency']}, "
          f"day-to-day rank correlation {report['day_to_day_rank_correlation']}; {len(pairs) - len(shown)} routes under {args.min_days} day hidden")
    if most_days < 7:
        print("Under the initial 7-day review window; rank stability is unproven (SCREENER/README.md, Comparison).")
    print(f"{'route':<18}{'days':>6}{'best':>8}{'$/day':>8}{'+days':>6}{'tk/day':>8}{'kept':>6}{'tk$/d':>8}{'xf0$/d':>8}{'xf1$/d':>8}{'spread0/1 bps':>15}{'thin%':>7}")
    for p in shown[:args.top]:
        tk, d = p["taker_gated"], max(p["days"], 1e-9)
        kept = f"{tk['realized_bps'] / tk['expected_bps']:.2f}" if tk["trades"] and tk["expected_bps"] else "-"
        spreads = "/".join(f"{s:.0f}" if s is not None else "-" for s in (p["spread_a_bps"], p["spread_l_bps"]))
        print(f"{p['pair']:<18}{d:>6.2f}{p['best']:>8}{p['best_usd_day']:>8.2f}{p['best_days_positive']:>6}{tk['trades'] / d:>8.1f}{kept:>6}"
              f"{tk['pnl_usd'] / d:>8.2f}{p['xemm_fixed']['pnl_usd'] / d:>8.2f}{p['xemm_reverse_fixed']['pnl_usd'] / d:>8.2f}"
              f"{spreads:>15}{100 * (p['thin'] or 0):>7.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

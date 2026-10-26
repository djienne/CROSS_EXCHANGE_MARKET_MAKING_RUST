#!/usr/bin/env python3
"""Reset the XEMM circuit-breaker trip latch.

When the cumulative-loss circuit breaker fires, the bot writes a trip-latch file
(`runs/bot-<MARKET>.trip.json`), halts, and then REFUSES to restart while that file exists. This
script clears the latch so the next run can start. It is a pure host-side file operation — it works
through the Docker bind mount (`./runs:/app/runs`), so there is no need to `docker exec` into the
container, and it works whether or not the container is running.

Without --runs-dir it looks in <crate>/runs (`run`'s).

Usage (from LIGHTER_ASTER_BOT/, the deploy dir; the default runs dir does not depend on the cwd):
    python scripts/reset_breaker.py                 # clear ALL *.trip.json latches
    python scripts/reset_breaker.py --coin HYPE     # clear only runs/bot-HYPE.trip.json (`run`)
    python scripts/reset_breaker.py --archive       # rename instead of delete (keeps an audit copy)
    python scripts/reset_breaker.py --runs-dir /path/to/runs    # override the runs directory

Exit code 0 on success (including "nothing to clear"); 1 only on an unexpected error.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def _runs_dirs_default() -> list[Path]:
    return [Path(__file__).resolve().parent.parent / "runs"]  # robust to the cwd


def _describe(path: Path) -> None:
    try:
        rec = json.loads(path.read_text())
        print(f"  - {path.name}")
        print(f"      reason : {rec.get('reason', 'unknown')}")
        print(f"      at     : {rec.get('ts_utc', 'unknown')}")
        print(f"      market : {rec.get('market', 'unknown')}")
        print(f"      loss   : {rec.get('loss_usd', '?')} USD "
              f"(equity {rec.get('equity_usd', '?')} vs baseline {rec.get('baseline_usd', '?')}, "
              f"limit {rec.get('limit_usd', '?')})")
    except Exception as e:  # noqa: BLE001 - best-effort context only
        print(f"  - {path.name}  (could not read trip record: {e})")


def main() -> int:
    ap = argparse.ArgumentParser(description="Reset the XEMM circuit-breaker trip latch")
    ap.add_argument("--runs-dir", type=Path, default=None,
                    help="runs directory holding the *.trip.json latch(es) "
                         "(default: <crate>/runs and the stack-level runs/)")
    ap.add_argument("--coin", help="clear only this market's latch, e.g. HYPE -> runs/bot-HYPE.trip.json")
    ap.add_argument("--archive", action="store_true",
                    help="rename the latch to <name>.cleared.<ts> instead of deleting it")
    args = ap.parse_args()

    runs_dirs = [args.runs_dir] if args.runs_dir else _runs_dirs_default()
    runs_dirs = [d for d in runs_dirs if d.exists()]
    if not runs_dirs:
        print(f"runs directory not found: {args.runs_dir or 'no <crate>/runs'}")
        return 0

    targets = []
    for runs_dir in runs_dirs:
        if args.coin:
            # Mirror `run`: its XEMM stem runs/bot-<MARKET> -> bot-<MARKET>.trip.json
            targets.append(runs_dir / f"bot-{args.coin.upper()}.trip.json")
        else:
            targets.extend(sorted(runs_dir.glob("*.trip.json")))

    searched = ", ".join(str(d) for d in runs_dirs)
    targets = [t for t in targets if t.exists()]
    if not targets:
        print(f"No trip latch to clear in {searched}. The breaker is not tripped.")
        return 0

    print(f"Found {len(targets)} trip latch(es) in {searched}:")
    for t in targets:
        _describe(t)

    cleared = 0
    for t in targets:
        try:
            if args.archive:
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                dest = t.with_name(f"{t.name}.cleared.{ts}")
                t.rename(dest)
                print(f"Archived {t.name} -> {dest.name}")
            else:
                t.unlink()
                print(f"Deleted  {t.name}")
            cleared += 1
        except Exception as e:  # noqa: BLE001
            print(f"ERROR clearing {t}: {e}", file=sys.stderr)
            return 1

    print(f"\nCircuit breaker reset ({cleared} latch(es) cleared). The next run may proceed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

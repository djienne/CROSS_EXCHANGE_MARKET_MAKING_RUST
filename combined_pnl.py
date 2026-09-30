#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from economics import event_time, optional_decimal, parse_timestamp, xemm_journal, taker_economics


SECONDS_PER_YEAR = Decimal(365 * 24 * 60 * 60)
DEFAULT_SINCE = "2026-06-28T00:00:00Z"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(raw: str) -> datetime:
    if raw.strip().lower() == "now":
        return utc_now()
    parsed = parse_timestamp(raw)
    if parsed is None:
        raise ValueError(f"invalid timestamp: {raw!r}")
    return parsed


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def dec(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    parsed = optional_decimal(value)
    return parsed if parsed is not None else default


def sum_optional(rows, key: str) -> Decimal | None:
    values = [optional_decimal(row.get(key)) for row in rows]
    return sum(values, Decimal(0)) if all(v is not None for v in values) else None


def decimal_str(value: Decimal | None) -> str | None:
    return None if value is None else format(value.normalize(), "f")


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_str(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"{type(value)!r} is not JSON serializable")


def iter_jsonl(path: Path, errors: list[str] | None = None):
    """(line number, object) per JSON-object line; other lines are skipped with a warning."""
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("expected a JSON object")
                yield line_no, row
            except ValueError as exc:  # json.JSONDecodeError is a ValueError
                if errors is not None:
                    errors.append(f"line {line_no}: {exc}")
                print(f"warn: skipping invalid JSON in {path}:{line_no}: {exc}", file=sys.stderr)


def summarize_taker(path: Path, since: datetime, now: datetime, market: str) -> dict[str, Any]:
    rows = []
    if path.exists():
        for _, row in iter_jsonl(path):
            if row.get("market") != market:
                continue
            ts = event_time(row)
            if ts is not None and since <= ts <= now:
                rows.append(row)
    normalized = []
    for row in rows:
        try:
            normalized.append(taker_economics(row))
        except (ValueError, TypeError, InvalidOperation):
            normalized.append(None)
    confirmed = [r for r in normalized if r is not None and r["economic_status"] == "confirmed"]
    unknown = len(rows)-len(confirmed)
    net = sum((r["net_pnl_usdc"] for r in confirmed),Decimal(0))
    # Recovery rows are conservative loss estimates: excluded from known net, shown on their own.
    estimated = sum((r["net_pnl_usdc"] for r in normalized if r is not None and r["economic_status"] == "estimated"
                     and r["net_pnl_usdc"] is not None), Decimal(0))
    return {"trades":len(rows),"path":path,
        "gross_pnl_usdc":sum_optional(confirmed,"gross_pnl_usdc") if not unknown else None,
        "fees_usdc":sum_optional(confirmed,"fees_usdc") if not unknown else None,
        "net_pnl_usdc":net if not unknown else None,
        "known_net_pnl_usdc":net,"estimated_net_pnl_usdc":estimated,"incomplete_trades":unknown}




def summarize_xemm_journal(
    path: Path, market: str, since: datetime, now: datetime, include_untimestamped: bool,
) -> dict[str, Any]:
    parsed = xemm_journal(path, market, now=now)
    trades = []
    missing_time = skipped_time = filtered = 0
    for trade in parsed["trades"]:
        ts = trade["timestamp"]
        if ts is None:
            if not include_untimestamped:
                skipped_time += 1
                continue
            missing_time += 1
        elif not since <= ts <= now:
            filtered += 1
            continue
        trades.append(trade)
    incomplete = sum(t["net_pnl_usdc"] is None for t in trades)
    uncertain = incomplete > 0 or parsed["malformed_rows"] > 0
    known_net = sum((t["net_pnl_usdc"] for t in trades if t["net_pnl_usdc"] is not None), Decimal(0))
    return {"path": path, "trades": len(trades),
        "gross_pnl_usdc": sum_optional(trades,"gross_pnl_usdc"),
        "aster_fees_usdc": sum_optional(trades,"aster_fee_usdc"),
        "lighter_fees_usdc": sum_optional(trades, "lighter_fee_usdc"),
        "fees_usdc": sum_optional(trades,"fees_usdc"),
        "net_pnl_usdc": None if uncertain else known_net,
        "known_net_pnl_usdc": known_net, "incomplete_trades": incomplete,
        "unmatched_fills": sum(t["residual_qty"] != 0 and t["lighter_qty"] == 0 for t in trades),
        "unmatched_hedges": sum(t["residual_qty"] != 0 and t["aster_qty"] == 0 for t in trades),
        "qty_mismatches": sum(t["residual_qty"] != 0 for t in trades),
        "malformed_rows": parsed["malformed_rows"],
        "untimestamped_trades": missing_time, "skipped_untimestamped_trades": skipped_time,
        "time_filtered_trades": filtered}


def selected_xemm_journals(
    paths: list[Path],
    runs_dirs: list[Path],
    since: datetime,
    require_paths: bool = False,
) -> tuple[list[Path], list[Path]]:
    if paths:
        included = []
        skipped = []
        seen: set[Path] = set()
        for raw_path in paths:
            path = raw_path.expanduser().resolve()
            if path in seen:
                continue
            seen.add(path)
            if require_paths and not path.is_file():
                raise SystemExit(f"error: --xemm-journal file not found: {path}")
            if path.is_file() and (require_paths or path.stat().st_size > 0):
                included.append(path)
            else:
                skipped.append(path)
        return included, skipped
    included: list[Path] = []
    skipped: list[Path] = []
    seen: set[Path] = set()
    for runs_dir in runs_dirs:
        if not runs_dir.exists():
            continue
        for path in sorted(runs_dir.glob("*journal*.jsonl")):
            path = path.resolve()
            if path in seen:
                continue
            seen.add(path)
            if not path.is_file() or path.stat().st_size == 0:
                continue
            mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
            if mtime >= since:
                included.append(path)
            else:
                skipped.append(path)
    return included, skipped


def report_roots(stack_root: Path, dry_run: bool = False) -> tuple[Path, Path]:
    """The `runs/` directories the reports read by default: the retired stacks' and `run`'s.
    A dry run keeps every file in `LIGHTER_ASTER_BOT/runs/dry-run/`, which live never shares,
    so both resolve there."""
    bot_runs = stack_root / "LIGHTER_ASTER_BOT" / "runs"
    return (bot_runs / "dry-run", bot_runs / "dry-run") if dry_run else (stack_root / "runs", bot_runs)


def default_since(bot_runs: Path, market: str, dry_run: bool) -> str:
    """Live reports from DEFAULT_SINCE; a dry run from its first recorded start.
    Projections use elapsed wall time, including downtime."""
    if dry_run:
        try:
            with (bot_runs / f"bot-{market}.events.jsonl").open(encoding="utf-8") as f:
                return json.loads(f.readline())["timestamp"]
        except (OSError, ValueError, KeyError, TypeError):
            pass
    return DEFAULT_SINCE


def default_state_path(roots: tuple[Path, Path], market: str) -> Path:
    """`run`'s controller state, else the retired orchestrator's (same keys)."""
    legacy_runs, bot_runs = roots
    current = bot_runs / f"bot-{market}.state.json"
    legacy = legacy_runs / f"orchestrator_state_{market}.json"
    return current if current.exists() or not legacy.exists() else legacy


def latest_capital_from_state(path: Path) -> tuple[Decimal | None, str | None]:
    """The taker's equity in the controller state, else XEMM's: both read the same accounts."""
    if not path.exists():
        return None, None
    state = json.loads(path.read_text(encoding="utf-8"))
    accounts = state.get("accounts") or {}
    for key in ("taker", "xemm"):
        equity = dec((accounts.get(key) or {}).get("total_equity_usd"), Decimal("-1"))
        if equity > 0:
            return equity, f"{path}:{key}.total_equity_usd"
    return None, None


def projection(net: Decimal | None, capital: Decimal | None, since: datetime, now: datetime) -> dict[str, Any]:
    elapsed = Decimal(max(1, int((now - since).total_seconds())))
    out: dict[str, Any] = {
        "elapsed_seconds": elapsed,
        "elapsed_days": elapsed / Decimal(86400),
        "capital_usdc": capital,
        "window_return_pct": None,
        "simple_annualized_return_pct": None,
        "projected_cagr_pct": None,
    }
    if net is None or capital is None or capital <= 0:
        return out
    window_return = net / capital
    annual_factor = SECONDS_PER_YEAR / elapsed
    out["window_return_pct"] = window_return * Decimal(100)
    out["simple_annualized_return_pct"] = window_return * annual_factor * Decimal(100)
    base = 1.0 + float(window_return)
    if base > 0:
        try:
            projected = (math.pow(base, float(annual_factor)) - 1.0) * 100.0
            if math.isfinite(projected):
                out["projected_cagr_pct"] = Decimal(str(projected))
        except OverflowError:
            pass
    return out


def combine(args: argparse.Namespace) -> dict[str, Any]:
    since = parse_dt(args.since)
    now = parse_dt(args.now) if args.now else utc_now()
    taker = summarize_taker(args.taker_trades, since, now, args.market)
    journal_paths, skipped_journals = selected_xemm_journals(
        args.xemm_journal,
        args.xemm_runs_dir,
        since,
        args.require_xemm_journal_paths,
    )
    xemm_journals = [
        summarize_xemm_journal(
            path,
            args.market,
            since,
            now,
            args.include_untimestamped_xemm,
        )
        for path in journal_paths
    ]
    xemm = {
        "trades": sum(j["trades"] for j in xemm_journals),
        "incomplete_trades": sum(j["incomplete_trades"] for j in xemm_journals),
        "known_net_pnl_usdc": sum((j["known_net_pnl_usdc"] for j in xemm_journals),Decimal(0)),
        "gross_pnl_usdc": sum_optional(xemm_journals, "gross_pnl_usdc"),
        "aster_fees_usdc": sum_optional(xemm_journals, "aster_fees_usdc"),
        "lighter_fees_usdc": sum_optional(xemm_journals, "lighter_fees_usdc"),
        "fees_usdc": sum_optional(xemm_journals, "fees_usdc"),
        "net_pnl_usdc": sum_optional(xemm_journals, "net_pnl_usdc"),
        "unmatched_fills": sum(j["unmatched_fills"] for j in xemm_journals),
        "unmatched_hedges": sum(j["unmatched_hedges"] for j in xemm_journals),
        "qty_mismatches": sum(j["qty_mismatches"] for j in xemm_journals),
        "malformed_rows": sum(j["malformed_rows"] for j in xemm_journals),
        "untimestamped_trades": sum(j["untimestamped_trades"] for j in xemm_journals),
        "skipped_untimestamped_trades": sum(j["skipped_untimestamped_trades"] for j in xemm_journals),
        "time_filtered_trades": sum(j["time_filtered_trades"] for j in xemm_journals),
        "journals": xemm_journals,
        "skipped_journals": skipped_journals,
        "journal_selection": args.xemm_journal_selection,
        "untimestamped_policy": args.xemm_untimestamped,
    }
    total = {
        "trades": taker["trades"] + xemm["trades"],
        "incomplete_trades": taker["incomplete_trades"] + xemm["incomplete_trades"],
        "known_net_pnl_usdc": taker["known_net_pnl_usdc"] + xemm["known_net_pnl_usdc"],
        "estimated_net_pnl_usdc": taker["estimated_net_pnl_usdc"],
        "gross_pnl_usdc": sum_optional([taker,xemm], "gross_pnl_usdc"),
        "fees_usdc": sum_optional([taker,xemm], "fees_usdc"),
        "net_pnl_usdc": sum_optional([taker,xemm], "net_pnl_usdc"),
    }
    capital = args.capital_usdc
    capital_source = "cli"
    if capital is None:
        capital, capital_source = latest_capital_from_state(args.orchestrator_state)
    proj = projection(total["net_pnl_usdc"], capital, since, now)
    notes = [
        "Execution economics include matched spread and explicit recovery closes; portfolio marks and funding are excluded.",
        "Unknown/legacy fee evidence makes full net totals and return projections unavailable; known subtotals remain visible.",
        "Known net subtotals exclude unresolved rows; estimated recovery losses are shown separately.",
        "By default XEMM includes only the production run/orchestrator journals. Pass --xemm-journal for exact files or --xemm-runs-dir for an mtime-based scan.",
    ]
    if xemm["untimestamped_trades"]:
        notes.append(
            f"Included {xemm['untimestamped_trades']} XEMM trade(s) without wall-clock timestamps "
            "(journal rows written before the bot stamped ts_ms); these cannot be windowed inside the journal file."
        )
    if xemm["skipped_untimestamped_trades"]:
        notes.append(
            f"Skipped {xemm['skipped_untimestamped_trades']} XEMM trade(s) without wall-clock timestamps "
            "(journal rows written before the bot stamped ts_ms); "
            "use --xemm-untimestamped include only for deliberate historical backfills."
        )
    if xemm["time_filtered_trades"]:
        notes.append(f"Filtered {xemm['time_filtered_trades']} timestamped XEMM trade(s) outside the requested window.")
    return {
        "market": args.market,
        "since": since,
        "now": now,
        "taker": taker,
        "xemm": xemm,
        "total": total,
        "projection": proj,
        "capital_source": capital_source,
        "notes": notes,
    }


def fmt_money(value: Decimal | None, signed: bool = True, places: int = 8) -> str:
    if value is None:
        return "unavailable"
    sign = "+" if signed else ""
    return f"{value:{sign}.{places}f}"


def fmt_pct(value: Decimal | None, places: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}%"


def capital_label(result: dict[str, Any]) -> str:
    capital = result["projection"]["capital_usdc"]
    if capital is None:
        return "n/a (pass --capital-usdc)"
    return str(capital)


def print_table(title: str, headers: list[str], rows: list[list[Any]], right_align: set[int] | None = None) -> None:
    right_align = right_align or set()
    text_rows = [[str(cell) for cell in row] for row in rows]
    widths = [
        max(len(headers[idx]), *(len(row[idx]) for row in text_rows))
        for idx in range(len(headers))
    ]

    def render_row(row: list[str]) -> str:
        cells = []
        for idx, cell in enumerate(row):
            if idx in right_align:
                cells.append(cell.rjust(widths[idx]))
            else:
                cells.append(cell.ljust(widths[idx]))
        return " | ".join(cells)

    print(title)
    print(render_row(headers))
    print("-+-".join("-" * width for width in widths))
    for row in text_rows:
        print(render_row(row))


def print_human(result: dict[str, Any]) -> None:
    p = result["projection"]
    xemm = result["xemm"]

    print_table(
        "Execution economics (marks and funding excluded)",
        ["Source", "Trades", "Gross USDC", "Fees USDC", "Net USDC", "Lighter Journal Fees"],
        [
            [
                "TAKER",
                result["taker"]["trades"],
                fmt_money(result["taker"]["gross_pnl_usdc"]),
                fmt_money(result["taker"]["fees_usdc"], signed=False),
                fmt_money(result["taker"]["net_pnl_usdc"]),
                "-",
            ],
            [
                "XEMM",
                xemm["trades"],
                fmt_money(xemm["gross_pnl_usdc"]),
                fmt_money(xemm["fees_usdc"], signed=False),
                fmt_money(xemm["net_pnl_usdc"]),
                fmt_money(xemm["lighter_fees_usdc"], signed=False),
            ],
            [
                "TOTAL",
                result["total"]["trades"],
                fmt_money(result["total"]["gross_pnl_usdc"]),
                fmt_money(result["total"]["fees_usdc"], signed=False),
                fmt_money(result["total"]["net_pnl_usdc"]),
                "-",
            ],
        ],
        right_align={1, 2, 3, 4, 5},
    )
    print()
    print(f"Known net subtotal: {fmt_money(result['total']['known_net_pnl_usdc'])}; "
          f"estimated recovery net: {fmt_money(result['total']['estimated_net_pnl_usdc'])}; "
          f"incomplete trades: {result['total']['incomplete_trades']}")
    print_table(
        "Projection",
        ["Metric", "Value"],
        [
            ["Market", result["market"]],
            ["Since UTC", iso(result["since"])],
            ["Now UTC", iso(result["now"])],
            ["Elapsed Days", f"{p['elapsed_days']:.6f}"],
            ["Capital USDC", capital_label(result)],
            ["Window Return", fmt_pct(p["window_return_pct"], 8)],
            ["Simple Annualized", fmt_pct(p["simple_annualized_return_pct"], 4)],
            ["Projected CAGR", fmt_pct(p["projected_cagr_pct"], 4)],
        ],
    )
    print()
    if p["capital_usdc"] is not None:
        print(f"capital source: {result['capital_source']}")
    if xemm["journals"]:
        print("XEMM journals included:")
        for journal in xemm["journals"]:
            print(f"- {journal['path']}")
    if xemm["skipped_journals"]:
        print("XEMM journals skipped:")
        for path in xemm["skipped_journals"]:
            print(f"- {path}")
    if (
        result["xemm"]["unmatched_fills"]
        or result["xemm"]["unmatched_hedges"]
        or result["xemm"]["qty_mismatches"]
        or result["xemm"].get("malformed_rows")
        or result["xemm"].get("skipped_untimestamped_trades")
    ):
        print(
            "XEMM warnings: "
            f"unmatched_fills={result['xemm']['unmatched_fills']} "
            f"unmatched_hedges={result['xemm']['unmatched_hedges']} "
            f"qty_mismatches={result['xemm']['qty_mismatches']} "
            f"malformed_rows={result['xemm'].get('malformed_rows', 0)} "
            f"skipped_untimestamped_trades={result['xemm'].get('skipped_untimestamped_trades', 0)}"
        )
    print()
    print("notes:")
    for note in result["notes"]:
        print(f"- {note}")


def parse_args() -> argparse.Namespace:
    stack_root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Combined execution economics report for taker arb + XEMM.")
    parser.add_argument("--dry-run", action="store_true", help="Report the dry run (LIGHTER_ASTER_BOT/runs/dry-run/) instead of live.")
    parser.add_argument("--since", default=None, help=f"UTC/RFC3339 start time. Default: {DEFAULT_SINCE}; with --dry-run, the dry run's first start.")
    parser.add_argument("--now", default=None, help="Override report end time. Defaults to current UTC time.")
    parser.add_argument("--market", default="HYPE")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--capital-usdc", type=Decimal, default=None, help="Capital denominator for projected CAGR. Defaults to the latest active bot equity in the controller state file.")
    parser.add_argument("--taker-trades", type=Path, default=None)
    parser.add_argument("--xemm-runs-dir", type=Path, action="append", default=None, help="Advanced: scan a directory of XEMM journal files by mtime. Repeatable.")
    parser.add_argument("--xemm-journal", type=Path, action="append", default=[], help="Explicit XEMM journal to include. Repeatable. Overrides default journal selection.")
    parser.add_argument(
        "--xemm-untimestamped",
        choices=["include", "exclude"],
        default="exclude",
        help="How to handle legacy XEMM fill/hedge rows without wall-clock timestamps.",
    )
    parser.add_argument("--bot-state", "--orchestrator-state", dest="orchestrator_state", type=Path, default=None, help="Controller state file. Default: `run`'s, else the retired orchestrator's.")
    args = parser.parse_args()
    roots = report_roots(stack_root, args.dry_run)
    legacy_runs, bot_runs = roots
    args.since = args.since or default_since(bot_runs, args.market, args.dry_run)
    explicit_xemm_journals = bool(args.xemm_journal)
    explicit_xemm_runs_dirs = args.xemm_runs_dir is not None
    if args.taker_trades is None:
        args.taker_trades = bot_runs / f"trades_{args.market}.jsonl"
    if args.xemm_runs_dir is None:
        args.xemm_runs_dir = []
    if not explicit_xemm_journals and not explicit_xemm_runs_dirs:
        args.xemm_journal = [
            legacy_runs / f"orchestrator-xemm-{args.market}-journal.jsonl",
            bot_runs / f"bot-{args.market}-journal.jsonl",
        ]
        args.xemm_journal_selection = "default_production_journals"
        args.require_xemm_journal_paths = False
    elif explicit_xemm_journals:
        args.xemm_journal_selection = "explicit_journals"
        args.require_xemm_journal_paths = True
    else:
        args.xemm_journal_selection = "runs_dir_mtime_since"
        args.require_xemm_journal_paths = False
    if args.orchestrator_state is None:
        args.orchestrator_state = default_state_path(roots, args.market)
    args.include_untimestamped_xemm = args.xemm_untimestamped == "include"
    return args


def main() -> int:
    args = parse_args()
    result = combine(args)
    if result["projection"]["projected_cagr_pct"] is None and result["projection"]["capital_usdc"] is None:
        print("warn: projected CAGR unavailable because capital could not be inferred", file=sys.stderr)
    if args.json:
        print(json.dumps(result, default=json_default, indent=2))
    else:
        print_human(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

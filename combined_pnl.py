#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timedelta, timezone
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


def by_day(trades: list[tuple[datetime | None, Decimal | None]], shift_ms: int) -> dict[str, dict[str, Any]]:
    """Trades and known net per UTC day of the market (a replay's clock less its shift); a trade
    without a known net counts as incomplete."""
    days: dict[str, dict[str, Any]] = {}
    for ts, net in trades:
        if ts is not None:
            day = days.setdefault((ts - timedelta(milliseconds=shift_ms)).date().isoformat(), {"trades": 0, "known_net_pnl_usdc": Decimal(0), "incomplete": 0})
            day["trades"] += 1
            if net is None:
                day["incomplete"] += 1
            else:
                day["known_net_pnl_usdc"] += net
    return dict(sorted(days.items()))


def replay_json(runs: Path) -> Path | None:
    """A replay's replay.json, beside its `dry-run/` (`runs`) or in `runs`."""
    return next((p for p in (runs.parent / "replay.json", runs / "replay.json") if p.exists()), None)


def replay_window(runs: Path) -> tuple[str, str, int] | None:
    """A replay's window on its own clock (`from`/`until` + `shift_ms`) and its shift. One still
    running, or stopped early, ends at its simulator's last minute (diag `ts_ms` closes a window)."""
    path = replay_json(runs)
    if path is None:
        return None
    about = json.loads(path.read_text(encoding="utf-8"))
    shift = timedelta(milliseconds=about["shift_ms"])
    until = parse_dt(about["until"]) + shift
    ends = [row["ts_ms"] for diag in (path.parent / "dry-run").glob("sim-*.diag.jsonl") for _, row in iter_jsonl(diag)]
    if ends:
        until = min(until, datetime.fromtimestamp(max(ends) / 1000, timezone.utc))
    return iso(parse_dt(about["from"]) + shift), iso(until), about["shift_ms"]


def summarize_taker(path: Path, since: datetime, now: datetime, market: str, shift_ms: int = 0) -> dict[str, Any]:
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
        "known_net_pnl_usdc":net,"estimated_net_pnl_usdc":estimated,"incomplete_trades":unknown,
        "by_day":by_day([(event_time(row), r["net_pnl_usdc"] if r is not None and r["economic_status"] == "confirmed" else None)
                         for row, r in zip(rows, normalized)], shift_ms)}




def summarize_xemm_journal(
    path: Path, market: str, since: datetime, now: datetime, include_untimestamped: bool, shift_ms: int = 0,
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
        "by_day": by_day([(t["timestamp"], t["net_pnl_usdc"]) for t in trades], shift_ms),
        "unmatched_fills": sum(t["residual_qty"] != 0 and t["lighter_qty"] == 0 for t in trades),
        "unmatched_hedges": sum(t["residual_qty"] != 0 and t["aster_qty"] == 0 for t in trades),
        "qty_mismatches": sum(t["residual_qty"] != 0 for t in trades),
        "malformed_rows": parsed["malformed_rows"],
        "untimestamped_trades": missing_time, "skipped_untimestamped_trades": skipped_time,
        "time_filtered_trades": filtered}


def selected_xemm_journals(paths: list[Path], require_paths: bool = False) -> tuple[list[Path], list[Path]]:
    included: list[Path] = []
    skipped: list[Path] = []
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


def report_runs(stack_root: Path, dry_run: bool = False) -> Path:
    """The `runs/` directory the reports read by default: `run`'s. A dry run keeps every file in
    its `runs/dry-run/`, which live never shares: the Tokyo VPS's, which `vps.py pull` copies to
    `LIGHTER_ASTER_BOT/runs/tokyo/` daily (this PC's own, until 2026-10-10, stay in
    `LIGHTER_ASTER_BOT/runs/dry-run/`: `--runs`)."""
    bot_runs = stack_root / "LIGHTER_ASTER_BOT" / "runs"
    return bot_runs / "tokyo" / "dry-run" if dry_run else bot_runs


def copy_end(runs: Path) -> str | None:
    """When `runs` is the daily copy of the VPS's runs (`runs/tokyo/` for the live bot, its
    `dry-run/` for the dry runs), when it was taken: its records end there, so a report's window
    does too."""
    path = next((p for p in (runs / "pulled_at.txt", runs.parent / "pulled_at.txt") if p.exists()), None)
    return path.read_text(encoding="utf-8").strip() if path else None


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


def equity_change(path: Path, since: datetime, now: datetime) -> dict[str, Any] | None:
    """The accounts' total equity at the window's first and last sample (bot-<M>.equity.jsonl, every
    15 s): what execution economics leave out. A position built one way books its spread per trade,
    but earns it only if the spread closes; meanwhile its two venues' marks swing this by ~$0.1 a
    sample (HYPE-LH, 10-11). Live, deposits and withdrawals show here too."""
    if not path.exists():
        return None
    rows = [(at, dec(row.get("total_equity_usd"))) for _, row in iter_jsonl(path)
            if (at := parse_timestamp(row.get("timestamp"))) and since <= at <= now]
    if not rows:
        return None
    (start, first), (end, last) = rows[0], rows[-1]
    return {"from": start, "until": end, "from_usd": first, "until_usd": last, "change_usdc": last - first}


def combine(args: argparse.Namespace) -> dict[str, Any]:
    since = parse_dt(args.since)
    now = parse_dt(args.now) if args.now else utc_now()
    taker = summarize_taker(args.taker_trades, since, now, args.market, args.shift_ms)
    journal_paths, skipped_journals = selected_xemm_journals(args.xemm_journal, args.require_xemm_journal_paths)
    xemm_journals = [
        summarize_xemm_journal(
            path,
            args.market,
            since,
            now,
            args.include_untimestamped_xemm,
            args.shift_ms,
        )
        for path in journal_paths
    ]
    xemm_days: dict[str, dict[str, Any]] = {}
    for day, d in ((day, d) for j in xemm_journals for day, d in j["by_day"].items()):
        acc = xemm_days.setdefault(day, dict.fromkeys(d, 0))
        for key in d:
            acc[key] += d[key]
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
        "by_day": dict(sorted(xemm_days.items())),
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
        capital, capital_source = latest_capital_from_state(args.bot_state)
    proj = projection(total["net_pnl_usdc"], capital, since, now)
    notes = [
        "Execution economics include matched spread and explicit recovery closes; portfolio marks and funding are excluded "
        "(the account equity line includes them).",
        "Unknown/legacy fee evidence makes full net totals and return projections unavailable; known subtotals remain visible.",
        "Known net subtotals exclude unresolved rows; estimated recovery losses are shown separately.",
        "By default XEMM reads `run`'s journal. Pass --xemm-journal for exact files.",
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
        "shift_ms": args.shift_ms,
        "taker": taker,
        "xemm": xemm,
        "total": total,
        "projection": proj,
        "equity": equity_change(args.equity, since, now),
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
    if eq := result["equity"]:
        print(f"Account equity (marks and funding included): {eq['from_usd']:.4f} -> {eq['until_usd']:.4f} USDC "
              f"({fmt_money(eq['change_usdc'], places=4)}), {iso(eq['from'])} -> {iso(eq['until'])}")
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
    parser.add_argument("--dry-run", action="store_true", help="Report the dry run (the VPS's, copied to LIGHTER_ASTER_BOT/runs/tokyo/dry-run/) instead of live.")
    parser.add_argument("--runs", type=Path, default=None, help="Read this runs directory (a replay's: runs/replay/<run>/dry-run, with --dry-run) instead.")
    parser.add_argument("--since", default=None, help=f"UTC/RFC3339 start time. Default: {DEFAULT_SINCE}; with --dry-run, the dry run's first start.")
    parser.add_argument("--now", default=None, help="Override report end time. Default: a replay's window end, else the Tokyo copy's pull time, else now.")
    parser.add_argument("--market", default="HYPE")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--capital-usdc", type=Decimal, default=None, help="Capital denominator for projected CAGR. Defaults to the latest active bot equity in the controller state file.")
    parser.add_argument("--taker-trades", type=Path, default=None)
    parser.add_argument("--xemm-journal", type=Path, action="append", default=[], help="Explicit XEMM journal to include. Repeatable. Overrides default journal selection.")
    parser.add_argument(
        "--xemm-untimestamped",
        choices=["include", "exclude"],
        default="exclude",
        help="How to handle legacy XEMM fill/hedge rows without wall-clock timestamps.",
    )
    parser.add_argument("--bot-state", type=Path, default=None, help="Controller state file. Default: `run`'s.")
    args = parser.parse_args()
    bot_runs = args.runs or report_runs(stack_root, args.dry_run)
    window = replay_window(args.runs) if args.runs else None
    args.shift_ms = window[2] if window else 0
    args.since = args.since or (window[0] if window else default_since(bot_runs, args.market, args.dry_run))
    args.now = args.now or (window[1] if window else copy_end(bot_runs))
    if args.taker_trades is None:
        args.taker_trades = bot_runs / f"trades_{args.market}.jsonl"
    args.equity = bot_runs / f"bot-{args.market}.equity.jsonl"
    if not args.xemm_journal:
        args.xemm_journal = [bot_runs / f"bot-{args.market}-journal.jsonl"]
        args.xemm_journal_selection = "default_production_journals"
        args.require_xemm_journal_paths = False
    else:
        args.xemm_journal_selection = "explicit_journals"
        args.require_xemm_journal_paths = True
    if args.bot_state is None:
        args.bot_state = bot_runs / f"bot-{args.market}.state.json"
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

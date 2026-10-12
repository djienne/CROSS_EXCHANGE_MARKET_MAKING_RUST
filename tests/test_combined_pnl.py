#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import combined_pnl  # noqa: E402


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")


class XemmSummaryTests(unittest.TestCase):
    def summarize(
        self,
        rows: list[dict],
        *,
        since: str = "2026-01-01T00:00:00Z",
        now: str = "2026-01-03T00:00:00Z",
        include_untimestamped: bool = False,
        current_schema: bool = True,
    ) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "journal.jsonl"
            if current_schema:
                converted=[]
                for index,row in enumerate(rows):
                    row=dict(row)
                    d=dict(row.get("detail",{}))
                    logical=d.get("cloid","")
                    if row.get("kind")=="fill":
                        row["kind"]="maker_fill"
                        d={"logical_id":logical,"maker_side":"Sell" if d.get("side")=="BUY" else "Buy",
                            "qty":d.get("qty"),"px":d.get("avg_aster_px"),"fee_usd":"0","fee_complete":True,
                            "order_id":f"A-{logical}","trade_id":f"M-{index}","client_id":f"maker-{logical}"}
                    elif row.get("kind")=="hedge_fill":
                        row["kind"]="execution_trade"
                        d={"logical_id":logical,"attempt_id":logical,"venue":"lighter","side":d.get("side"),
                            "qty":d.get("qty"),"px":d.get("px"),"fee_usd":d.get("fee_usd"),
                            "order_id":f"L-{logical}","trade_id":f"H-{index}"}
                    row.update(schema_version=2,economic_status="confirmed",detail=d)
                    converted.append(row)
                rows=converted
            write_jsonl(path, rows)
            return combined_pnl.summarize_xemm_journal(
                path,
                "HYPE",
                combined_pnl.parse_dt(since),
                combined_pnl.parse_dt(now),
                include_untimestamped,
            )

    def test_xemm_side_math_matches_hedge_side(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0.1"}},
            {"timestamp": "2026-01-02T00:00:02Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "b", "side": "SELL", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:03Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "b", "side": "SELL", "qty": "1", "px": "101", "fee_usd": "0.2"}},
        ]
        out = self.summarize(rows)
        self.assertEqual(out["trades"], 2)
        self.assertEqual(out["gross_pnl_usdc"], Decimal("2"))
        self.assertEqual(out["lighter_fees_usdc"], Decimal("0.3"))
        self.assertEqual(out["net_pnl_usdc"], Decimal("1.7"))

    def test_missing_venue_fee_remains_unknown(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "px": "99"}},
        ]
        out = self.summarize(rows)
        self.assertIsNone(out["lighter_fees_usdc"])
        self.assertIsNone(out["net_pnl_usdc"])

    def test_mismatched_pair_keeps_matched_economics_and_exposes_residual(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "2", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows)
        self.assertEqual(out["trades"], 1)
        self.assertEqual(out["qty_mismatches"], 1)
        self.assertEqual(out["net_pnl_usdc"], Decimal("1"))

    def test_timestamped_xemm_rows_are_filtered_by_since_now(self) -> None:
        rows = [
            {"timestamp": "2026-01-01T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-01T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows, since="2026-01-02T00:00:00Z")
        self.assertEqual(out["trades"], 1)
        self.assertEqual(out["time_filtered_trades"], 1)

    def test_ts_ms_rows_are_windowed_not_policy_excluded(self) -> None:
        # Current-format journal rows carry ts_ms (epoch millis, stamped by JournalRecord).
        # They must window like any timestamped row — never fall into the untimestamped
        # policy that hides them from the default report.
        def ms(iso_ts: str) -> int:
            return int(combined_pnl.parse_dt(iso_ts).timestamp() * 1000)

        rows = [
            {"ts_ms": ms("2026-01-01T00:00:00Z"), "kind": "fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"ts_ms": ms("2026-01-01T00:00:01Z"), "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
            {"ts_ms": ms("2026-01-02T00:00:00Z"), "kind": "fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"ts_ms": ms("2026-01-02T00:00:01Z"), "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows, since="2026-01-02T00:00:00Z")
        self.assertEqual(out["trades"], 1)
        self.assertEqual(out["time_filtered_trades"], 1)
        self.assertEqual(out["skipped_untimestamped_trades"], 0)
        self.assertEqual(out["untimestamped_trades"], 0)

    def test_legacy_untimestamped_xemm_rows_are_policy_controlled(self) -> None:
        rows = [
            {"kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        strict = self.summarize(rows, include_untimestamped=False, current_schema=False)
        legacy = self.summarize(rows, include_untimestamped=True, current_schema=False)
        self.assertEqual(strict["trades"], 0)
        self.assertEqual(strict["skipped_untimestamped_trades"], 1)
        self.assertEqual(legacy["trades"], 1)
        self.assertEqual(legacy["untimestamped_trades"], 1)

    def test_malformed_xemm_row_is_skipped_not_fatal(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "bad", "side": "BUY", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "good", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "good", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows)
        self.assertEqual(out["malformed_rows"], 1)
        self.assertEqual(out["trades"], 1)

    def test_taker_summary_respects_upper_now_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "taker.jsonl"
            write_jsonl(
                path,
                [
                    {"timestamp": "2026-01-02T00:00:00Z", "market": "HYPE", "actual_gross_usd": "1", "actual_fees_usd": "0", "actual_net_usd": "1"},
                    {"timestamp": "2026-01-04T00:00:00Z", "market": "HYPE", "actual_gross_usd": "1", "actual_fees_usd": "0", "actual_net_usd": "1"},
                ],
            )
            out = combined_pnl.summarize_taker(
                path,
                combined_pnl.parse_dt("2026-01-01T00:00:00Z"),
                combined_pnl.parse_dt("2026-01-03T00:00:00Z"),
                "HYPE",
            )
        self.assertEqual(out["trades"], 1)

    def test_estimated_recovery_losses_are_reported_not_hidden(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "taker.jsonl"
            write_jsonl(path, [{"schema_version": 2, "economic_status": "estimated", "timestamp": "2026-01-02T00:00:00Z",
                                "market": "HYPE", "direction": "RECOVERY", "aster_order_id": -1, "lighter_client_order_index": 0,
                                "actual_gross_usd": "-0.40", "actual_fees_usd": "0", "actual_net_usd": "-0.40"}])
            out = combined_pnl.summarize_taker(path, combined_pnl.parse_dt("2026-01-01T00:00:00Z"),
                                               combined_pnl.parse_dt("2026-01-03T00:00:00Z"), "HYPE")
        self.assertEqual((out["known_net_pnl_usdc"], out["estimated_net_pnl_usdc"], out["incomplete_trades"]),
                         (Decimal("0"), Decimal("-0.40"), 1))
        self.assertIsNone(out["net_pnl_usdc"])

    def test_dry_run_reports_from_its_first_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp)
            self.assertEqual(combined_pnl.default_since(runs, "HYPE", True), combined_pnl.DEFAULT_SINCE)
            write_jsonl(runs / "bot-HYPE.events.jsonl", [
                {"kind": "bot_started", "timestamp": "2026-09-24T14:14:54Z"},
                {"kind": "bot_started", "timestamp": "2026-09-25T00:00:00Z"},
            ])
            self.assertEqual(combined_pnl.default_since(runs, "HYPE", True), "2026-09-24T14:14:54Z")
            self.assertEqual(combined_pnl.default_since(runs, "HYPE", False), combined_pnl.DEFAULT_SINCE)

    def test_a_replay_reports_its_window_and_net_per_market_day(self) -> None:
        def trade(at: str, net: str = "1") -> dict:  # sold 1 at 101, bought 1 at 100; another net disagrees: incomplete
            leg = lambda px: {"qty": "1", "vwap": px, "fee_usd": "0", "fee_provenance": "venue"}
            return {"schema_version": 2, "economic_status": "confirmed", "timestamp": at, "market": "HYPE",
                    "direction": "SELL_ASTER_BUY_LIGHTER", "aster_order_id": at, "hedge_client_order_index": 1,
                    "aster_fill": leg("101"), "hedge_fill": leg("100"), "actual_gross_usd": "1", "actual_fees_usd": "0", "actual_net_usd": net}
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "dry-run"
            runs.mkdir()
            # Shifted 1 h: the run's 00:30 is the market's 23:30 the day before; its window ends at 03:00.
            (runs.parent / "replay.json").write_text(json.dumps({"from": "2026-01-02T23:00:00Z", "until": "2026-01-03T02:00:00Z",
                                                                 "shift_ms": 3_600_000}), encoding="utf-8")
            write_jsonl(runs / "trades_HYPE.jsonl", [trade("2026-01-03T00:30:00Z"), trade("2026-01-03T01:30:00Z"),
                                                     trade("2026-01-03T02:00:00Z", net="2"), trade("2026-01-03T04:00:00Z")])
            # The account equity change counts only samples in the window.
            write_jsonl(runs / "bot-HYPE.equity.jsonl", [{"timestamp": at, "total_equity_usd": eq} for at, eq in [
                ("2026-01-02T23:59:50Z", "300"), ("2026-01-03T00:00:05Z", "400"), ("2026-01-03T02:59:55.123456789Z", "399.9"),
                ("2026-01-03T03:00:10Z", "300")]])
            out = json.loads(subprocess.run([sys.executable, combined_pnl.__file__, "--runs", str(runs), "--market", "HYPE", "--json"],
                                            check=True, capture_output=True, text=True).stdout)
        self.assertEqual((out["since"][:19], out["now"][:19], out["shift_ms"]), ("2026-01-03T00:00:00", "2026-01-03T03:00:00", 3_600_000))
        days = {day: (d["trades"], Decimal(d["known_net_pnl_usdc"]), d["incomplete"]) for day, d in out["taker"]["by_day"].items()}
        self.assertEqual(days, {"2026-01-02": (1, Decimal(1), 0), "2026-01-03": (2, Decimal(1), 1)})
        self.assertEqual((Decimal(out["equity"]["from_usd"]), Decimal(out["equity"]["change_usdc"])), (Decimal(400), Decimal("-0.1")))


if __name__ == "__main__":
    unittest.main()

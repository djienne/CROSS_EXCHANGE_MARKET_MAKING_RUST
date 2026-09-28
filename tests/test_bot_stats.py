#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bot_stats  # noqa: E402


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


class BotStatsTests(unittest.TestCase):
    def test_edge_lost_splits_into_leg_slippage_and_host_freezes_are_separated(self) -> None:
        start_ms = 1_790_000_000_000
        started = datetime.fromtimestamp(start_ms / 1000, timezone.utc).isoformat().replace("+00:00", "Z")
        opp = {"timestamp": started, "direction": "SELL_LIGHTER_BUY_ASTER", "decision": "would_execute",
               "buy_px": "100", "sell_px": "100.10", "aster_book_age_ms": 40, "lighter_book_age_ms": 0,
               "gate_threshold_bps": "9", "history_sample_count": 60}
        done = {"timestamp": started, "started_at": started, "execution_id": "e1", "outcome": "success",
                "direction": "SELL_LIGHTER_BUY_ASTER", "gross_edge_bps": "10",
                "aster_fill": {"vwap": "100.05", "notional": "100.05"}, "lighter_fill": {"vwap": "100.08"},
                "actual_economics": {"gross_usd": "0.03", "fees_usd": "0.04", "net_usd": "-0.01", "net_bps": "-1"},
                "aster_submit": 'Accepted { raw: "{\\"updateTime\\":%d}" }' % (start_ms + 90),
                "lighter_fee_evidence": [{"event_time_ms": start_ms + 300}]}
        window = lambda ts, lateness, late, venues=("aster", "lighter"): {"ts_ms": ts, "window_s": 60, "lateness_ms": {"n": 1, "p99": lateness, "max": lateness},
            **{v: {"frames": 100, "late_frames": late, "gaps": 0, "lag_ms": {}, "requests": 6, "orders": 0, "rejects": {},
                   "maker_fills": 0, "taker_fills": 0, "prints": 0, "prints_inside_spread": 0, "prints_over_visible": 0,
                   "account": {"equity": "200", "fees": "0", "funding": "0"}} for v in venues}}
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp)
            write_jsonl(runs / "opportunities_HYPE.jsonl", [opp])
            write_jsonl(runs / "executions_HYPE.jsonl", [{**done, "outcome": "submitting", "actual_economics": None}, done])
            write_jsonl(runs / "sim-HYPE.diag.jsonl", [window(start_ms, 2000, 50), window(start_ms + 60_000, 2, 1)])
            write_jsonl(runs / "sim-HYPE-HL.diag.jsonl", [window(start_ms, 2, 1, ("aster", "hyperliquid"))])
            since, until = (datetime.fromtimestamp(start_ms / 1000 + h * 3600, timezone.utc) for h in (-1, 1))
            result = bot_stats.report(runs, "HYPE", since, until)
            hl = bot_stats.simulator(runs, "HYPE-HL", since, until)
        taker, sim = result["taker"], result["simulator"]
        self.assertEqual((taker["trades"], taker["outcomes"]), (1, {"success": 1}))
        self.assertAlmostEqual(taker["aster_slippage_bps"]["mean"], 5.0, places=2)
        self.assertAlmostEqual(taker["lighter_slippage_bps"]["mean"], 1.998, places=2)
        # Expected edge less both legs' slippage is the realized edge (to the bps basis difference).
        kept = taker["realized_gross_bps"]["mean"]
        self.assertAlmostEqual(10 - 5.0 - 1.998, kept, places=1)
        self.assertEqual((taker["aster_fill_ms"]["p50"], taker["lighter_fill_ms"]["p50"]), (90, 300))
        self.assertEqual((sim["host_frozen_windows"], sim["aster"]["late_frames_pct"], sim["aster"]["late_frames_pct_unfrozen"]),
                         (1, 25.5, 1.0))
        # A Hyperliquid-hedged market's simulator reports its own venues.
        self.assertEqual([v for v in ("aster", "lighter", "hyperliquid") if v in hl], ["aster", "hyperliquid"])

    def test_quote_refreshes_are_timed_and_a_side_rests_until_its_order_ends(self) -> None:
        t = 1_790_000_000_000
        rec = lambda ms, kind, **detail: {"ts_ms": t + ms, "kind": kind, "market": "HYPE", "detail": detail}
        answer = lambda ms, cid, state: rec(ms, "order_update", client_id=cid, state=state)
        rows = [
            rec(0, "place", side="Buy", client_id="b1"), answer(100, "b1", "accepted"),
            # Cancel-then-place: down from the cancel to the new order's ack.
            rec(1_000, "replace", side="Buy", client_id="b1"), answer(1_100, "b1", "cancelled"),
            rec(1_100, "place", side="Buy", client_id="b2"), answer(1_200, "b2", "accepted"),
            # An amend keeps the order resting.
            rec(2_000, "amend", side="Buy", client_id="b2"), answer(2_100, "b2", "amended"),
            rec(3_000, "amend", side="Buy", client_id="b2"), answer(3_050, "b2", "amend_rejected"),
            answer(5_000, "b2", "cancelled"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp)
            write_jsonl(runs / "bot-HYPE-journal.jsonl", rows)
            since, until = (datetime.fromtimestamp((t + ms) / 1000, timezone.utc) for ms in (0, 10_000))
            q = bot_stats.quotes(runs, "HYPE", since, until)
        self.assertEqual(q["refresh_round_trip_ms"]["n"], 3)
        self.assertEqual((q["refresh_round_trip_ms"]["p50"], q["refresh_round_trip_ms"]["p90"]), (100, 200))
        self.assertEqual(q["uptime_pct"], {"Buy": 48.0})  # 100..1100 and 1200..5000 of 10 s
        self.assertEqual(q["answers"]["amend_rejected"], 1)


if __name__ == "__main__":
    unittest.main()

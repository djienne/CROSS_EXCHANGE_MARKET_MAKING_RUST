#!/usr/bin/env python3
from __future__ import annotations

import json
from contextlib import contextmanager
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import combined_pnl  # noqa: E402
import trade_history  # noqa: E402


def write_jsonl(path: Path, rows: list[dict]) -> None:
    # Current producer fixtures explicitly carry venue fee evidence.
    for row in rows:
        legs=[row.get("aster_fill"),row.get("lighter_fill")]
        if all(isinstance(f,dict) and "fee_usd" in f for f in legs):
            row.update(schema_version=2,economic_status="confirmed")
            for f in legs: f["fee_provenance"]="venue"
    path.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")


class TradeHistoryTests(unittest.TestCase):
    @contextmanager
    def open_db(self, path: Path):
        conn = trade_history.open_db(path)
        trade_history.init_db(conn)
        try:
            yield conn
        finally:
            conn.close()

    def test_taker_unequal_legs_price_the_matched_qty_and_must_agree_with_the_producer(self) -> None:
        # The fills are repriced: only the matched 0.9 earns the spread, 0.9 x (100 - 99) = 0.9,
        # not 1 x (100 - 99) = 1 from the larger leg. A producer's actual_* that disagrees with
        # the fills leaves its trade incomplete instead of certifying either number.
        row = {
            "timestamp": "2026-01-02T00:00:00.123456789Z",
            "market": "HYPE",
            "direction": "SELL_ASTER_BUY_LIGHTER",
            "qty": "0.9",
            "actual_gross_usd": "0.9",
            "actual_fees_usd": "0.04",
            "actual_net_usd": "0.86",
            "aster_fill": {"qty": "1", "vwap": "100", "notional": "100", "fee_usd": "0.04"},
            "lighter_fill": {"qty": "0.9", "vwap": "99", "notional": "89.1", "fee_usd": "0"},
            "aster_order_id": 1,
            "lighter_client_order_index": 2,
        }
        disagreeing = json.loads(json.dumps(row))
        disagreeing.update(aster_order_id=3, actual_gross_usd="1", actual_net_usd="0.96")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            write_jsonl(orch_path, [])
            since = combined_pnl.parse_dt("2026-01-01T00:00:00Z")
            now = combined_pnl.parse_dt("2026-01-03T00:00:00Z")
            with self.open_db(db_path) as conn:
                write_jsonl(taker_path, [row])
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(conn, market="HYPE", since=since, now=now, db_path=db_path)
                self.assertEqual(report["total"]["trades"], 1)
                self.assertEqual(report["total"]["gross_pnl_usdc"], Decimal("0.9"))
                self.assertEqual(report["total"]["policy_fees_usdc"], Decimal("0.04"))
                self.assertEqual(report["total"]["net_pnl_usdc"], Decimal("0.86"))
                write_jsonl(taker_path, [row, disagreeing])
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(conn, market="HYPE", since=since, now=now, db_path=db_path)
            self.assertEqual((report["total"]["trades"], report["total"]["incomplete_trades"]), (2, 1))
            self.assertEqual(report["total"]["known_net_pnl_usdc"], Decimal("0.86"))
            self.assertIsNone(report["total"]["net_pnl_usdc"])

    def test_taker_ingests_sell_lighter_buy_aster_direction(self) -> None:
        # This direction was silently dropped before 2026-07-18 (the matcher
        # expected a never-emitted BUY_ASTER_SELL_LIGHTER string).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            write_jsonl(
                taker_path,
                [
                    {
                        "timestamp": "2026-01-02T00:00:00Z",
                        "market": "HYPE",
                        "direction": "SELL_LIGHTER_BUY_ASTER",
                        "qty": "1",
                        "actual_gross_usd": "1",
                        "actual_fees_usd": "0.04",
                        "actual_net_usd": "0.96",
                        "aster_fill": {"qty": "1", "vwap": "99", "notional": "99", "fee_usd": "0.04"},
                        "lighter_fill": {"qty": "1", "vwap": "100", "notional": "100", "fee_usd": "0"},
                        "aster_order_id": 5,
                        "lighter_client_order_index": 6,
                    }
                ],
            )
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                rows = conn.execute(
                    "SELECT direction, net_pnl_usdc FROM strategy_trades"
                ).fetchall()
            self.assertEqual(1, len(rows))
            self.assertEqual("SELL_LIGHTER_BUY_ASTER", rows[0][0])
            self.assertEqual("0.96", rows[0][1])

    def test_taker_recovery_rows_ingested_as_losses_with_distinct_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"

            def recovery(ts: str) -> dict:
                return {
                    "timestamp": ts,
                    "market": "HYPE",
                    "direction": "RECOVERY",
                    "qty": "0",
                    "actual_gross_usd": "-1.25",
                    "actual_fees_usd": "0",
                    "actual_net_usd": "-1.25",
                    "aster_fill": {"qty": "0", "vwap": "0", "notional": "0"},
                    "lighter_fill": {"qty": "0", "vwap": "0", "notional": "0"},
                    "aster_order_id": 0,
                    "lighter_client_order_index": 0,
                }

            write_jsonl(
                taker_path,
                [recovery("2026-01-02T00:00:00.111111111Z"), recovery("2026-01-02T01:00:00.222222222Z")],
            )
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                rows = conn.execute(
                    "SELECT trade_key, net_pnl_usdc FROM strategy_trades ORDER BY trade_key"
                ).fetchall()
            self.assertEqual(2, len(rows))
            self.assertNotEqual(rows[0][0], rows[1][0])
            for key, net in rows:
                self.assertTrue(key.startswith("taker:recovery:0:0:"))
                self.assertEqual("-1.25", net)

    def test_refresh_self_heals_previously_miscomputed_rows(self) -> None:
        # The same trade_key re-ingested overwrites non-preserved columns, so one
        # ordinary refresh corrects rows written by the old formula.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            row = {
                "timestamp": "2026-01-02T00:00:00Z",
                "market": "HYPE",
                "direction": "SELL_ASTER_BUY_LIGHTER",
                "qty": "1",
                "actual_gross_usd": "0.5",
                "actual_fees_usd": "0.04",
                "actual_net_usd": "0.46",
                "aster_fill": {"qty": "1", "vwap": "100", "notional": "100", "fee_usd": "0.04"},
                "lighter_fill": {"qty": "1", "vwap": "99.5", "notional": "99.5", "fee_usd": "0"},
                "aster_order_id": 9,
                "lighter_client_order_index": 9,
            }
            write_jsonl(taker_path, [row])
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                # Seed the DB with a wrong historical value under the same key.
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                conn.execute("UPDATE strategy_trades SET net_pnl_usdc = '999' WHERE trade_key = 'taker:9:9'")
                conn.commit()
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                net = conn.execute(
                    "SELECT net_pnl_usdc FROM strategy_trades WHERE trade_key = 'taker:9:9'"
                ).fetchone()[0]
            self.assertEqual("0.46", net)

    def test_refresh_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            write_jsonl(
                taker_path,
                [
                    {
                        "timestamp": "2026-01-02T00:00:00Z",
                        "market": "HYPE",
                        "direction": "SELL_LIGHTER_BUY_ASTER",
                        "qty": "2",
                        "aster_fill": {"qty": "2", "vwap": "10", "notional": "20"},
                        "lighter_fill": {"qty": "2", "vwap": "11", "notional": "22"},
                        "aster_order_id": 10,
                        "lighter_client_order_index": 20,
                    }
                ],
            )
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                trade_count = conn.execute("SELECT COUNT(*) FROM strategy_trades").fetchone()[0]
                fill_count = conn.execute("SELECT COUNT(*) FROM venue_fills").fetchone()[0]
            self.assertEqual(trade_count, 1)
            self.assertEqual(fill_count, 2)

    def test_xemm_orchestrator_preserves_matched_economics_and_folds_corrections(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            row={"timestamp":"2026-01-02T00:00:00Z","key":"xemm:abc","cloid":"abc","bot":"XEMM_LIGHTER_ASTER",
                "market":"HYPE","direction":"ASTER_MAKER_HEDGE_SELL","qty":"0.20","aster_qty":"0.20","lighter_qty":"0.12",
                "matched_qty":"0.12","residual_qty":"0.08","aster_px":"100","lighter_px":"101",
                "gross_pnl_usdc":"0.12","fees_usdc":"0.01","net_pnl_usdc":"0.11","aster_fee_usdc":"0","lighter_fee_usdc":"0.01",
                "schema_version":2,"economic_status":"confirmed"}
            final=dict(row,lighter_qty="0.20",matched_qty="0.20",residual_qty="0",lighter_px="100.5",
                gross_pnl_usdc="0.10",fees_usdc="0.02",net_pnl_usdc="0.08",lighter_fee_usdc="0.02")
            correction={"timestamp":"2026-01-02T00:00:01Z","key":"xemm:abc#rev1","cloid":"abc","bot":"XEMM_LIGHTER_ASTER",
                "market":"HYPE","direction":"XEMM_CORRECTION","corrected_trade":final}
            path=root/"orchestrator.jsonl"
            write_jsonl(path,[row,correction])
            with self.open_db(root/"history.sqlite") as conn:
                trade_history.ingest_orchestrator_xemm(conn,path,market="HYPE")
                values=conn.execute("SELECT net_pnl_usdc,matched_qty FROM strategy_trades").fetchall()
                self.assertEqual(len(values),1)
                self.assertEqual(tuple(values[0]),("0.08","0.2"))
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM venue_fills").fetchone()[0],0,
                    "aggregate reports must not invent native fill identities")

    def test_report_filters_by_time_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            base = {
                "market": "HYPE",
                "direction": "SELL_ASTER_BUY_LIGHTER",
                "qty": "1",
                "aster_fill": {"qty": "1", "vwap": "100", "notional": "100"},
                "lighter_fill": {"qty": "1", "vwap": "99", "notional": "99"},
            }
            old = dict(base, timestamp="2026-01-01T00:00:00Z", aster_order_id=1, lighter_client_order_index=1)
            new = dict(base, timestamp="2026-01-02T00:00:00Z", aster_order_id=2, lighter_client_order_index=2)
            write_jsonl(taker_path, [old, new])
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(
                    conn,
                    market="HYPE",
                    since=combined_pnl.parse_dt("2026-01-02T00:00:00Z"),
                    now=combined_pnl.parse_dt("2026-01-03T00:00:00Z"),
                    db_path=db_path,
                )
            self.assertEqual(report["total"]["trades"], 1)

    def test_untimestamped_rows_keep_every_window_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path, orch_path, db_path = root / "taker.jsonl", root / "orchestrator.jsonl", root / "history.sqlite"
            row = {"market": "HYPE", "direction": "SELL_ASTER_BUY_LIGHTER", "qty": "1", "timestamp": "2026-01-02T00:00:00Z",
                   "aster_order_id": 1, "lighter_client_order_index": 1,
                   "aster_fill": {"qty": "1", "vwap": "100", "notional": "100", "fee_usd": "0"},
                   "lighter_fill": {"qty": "1", "vwap": "99", "notional": "99", "fee_usd": "0"}}
            write_jsonl(taker_path, [row])
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                conn.execute("UPDATE strategy_trades SET timestamp_us=NULL")  # a legacy row without a time
                report = trade_history.report_from_db(
                    conn, market="HYPE", since=combined_pnl.parse_dt("2026-01-01T00:00:00Z"),
                    now=combined_pnl.parse_dt("2026-01-03T00:00:00Z"), db_path=db_path)
            self.assertEqual((report["total"]["trades"], report["total"]["untimestamped_trades"],
                              report["total"]["incomplete_trades"]), (0, 1, 1))
            self.assertIsNone(report["total"]["net_pnl_usdc"])

    def test_report_time_window_uses_normalized_timestamp_not_text_sort(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            base = {
                "market": "HYPE",
                "direction": "SELL_ASTER_BUY_LIGHTER",
                "qty": "1",
                "aster_fill": {"qty": "1", "vwap": "100", "notional": "100"},
                "lighter_fill": {"qty": "1", "vwap": "99", "notional": "99"},
            }
            boundary = dict(base, timestamp="2026-06-28T00:00:00.500000Z", aster_order_id=1, lighter_client_order_index=1)
            write_jsonl(taker_path, [boundary])
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(
                    conn,
                    market="HYPE",
                    since=combined_pnl.parse_dt("2026-06-28T00:00:00Z"),
                    now=combined_pnl.parse_dt("2026-06-28T00:00:01Z"),
                    db_path=db_path,
                )
            self.assertEqual(report["total"]["trades"], 1)

    def test_xemm_journal_ingest_captures_downtime_trades_with_real_times(self) -> None:
        # A fill/hedge_fill pair that only exists in the bot journal (orchestrator was
        # down) must land in the DB with the journal's ts_ms as the trade time and the
        # actual hedge fee from the journal.
        def ms(iso_ts: str) -> int:
            return int(combined_pnl.parse_dt(iso_ts).timestamp() * 1000)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            journal_path = root / "journal.jsonl"
            db_path = root / "history.sqlite"
            write_jsonl(taker_path, [])
            write_jsonl(orch_path, [])
            write_jsonl(
                journal_path,
                [
                    {"schema_version":2,"economic_status":"confirmed","ts_ms":ms("2026-01-02T00:00:00Z"),"kind":"maker_fill","market":"HYPE","detail":{"logical_id":"abc","maker_side":"Sell","qty":"1","px":"100","fee_usd":"0","fee_complete":True,"order_id":"A1","trade_id":"M1","client_id":"m1"}},
                    {"schema_version":2,"economic_status":"confirmed","ts_ms":ms("2026-01-02T00:00:01Z"),"kind":"execution_trade","market":"HYPE","detail":{"logical_id":"abc","attempt_id":"h1","venue":"lighter","side":"Buy","qty":"1","px":"99","fee_usd":"0.01","order_id":"H1","trade_id":"T1"}},
                ],
            )
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(
                    conn,
                    market="HYPE",
                    taker_trades=taker_path,
                    orchestrator_trades=orch_path,
                    xemm_journals=[journal_path],
                )
                # Idempotent on re-run.
                trade_history.refresh_lan(
                    conn,
                    market="HYPE",
                    taker_trades=taker_path,
                    orchestrator_trades=orch_path,
                    xemm_journals=[journal_path],
                )
                row = conn.execute(
                    "SELECT timestamp, gross_pnl_usdc, net_pnl_usdc, lighter_fee_usdc, source FROM strategy_trades WHERE trade_key = ?",
                    ("xemm:abc",),
                ).fetchone()
                trade_count = conn.execute("SELECT COUNT(*) FROM strategy_trades").fetchone()[0]
            self.assertEqual(trade_count, 1)
            self.assertIsNotNone(row)
            timestamp, gross, net, lighter_fee, source = row
            self.assertTrue(str(timestamp).startswith("2026-01-02T00:00:01"), timestamp)
            # Hedge BUY => maker leg sold on Aster at 100, bought back at 99 => +1 gross.
            self.assertEqual(Decimal(str(gross)), Decimal("1"))
            self.assertEqual(Decimal(str(lighter_fee)), Decimal("0.01"))
            self.assertEqual(Decimal(str(net)), Decimal("0.99"))
            self.assertEqual(source, "xemm_journal")

    def test_dry_run_reports_read_and_write_only_the_dry_run_directory(self) -> None:
        dry = Path(trade_history.__file__).resolve().parent / "LIGHTER_ASTER_BOT" / "runs" / "tokyo" / "dry-run"
        for module in (combined_pnl, trade_history):
            for flags in ([], ["--dry-run"]):
                with mock.patch.object(sys, "argv", [module.__name__, *flags]):
                    args = module.parse_args()
                keys = ("taker_trades", "orchestrator_state", "db", "orchestrator_trades")
                paths = [getattr(args, key) for key in keys if hasattr(args, key)] + args.xemm_journal
                self.assertEqual({dry in path.parents for path in paths}, {bool(flags)}, (module.__name__, flags))


if __name__ == "__main__":
    unittest.main()

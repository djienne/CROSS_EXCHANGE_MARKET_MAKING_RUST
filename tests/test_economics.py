from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import economics
import combined_pnl
import trade_history

CASES = json.loads((Path(__file__).parent / "fixtures/execution_economics.json").read_text())


def write_rows(path, rows):
    path.write_text("".join(json.dumps(r)+"\n" for r in rows), encoding="utf-8")


class EconomicContractTests(unittest.TestCase):
    def test_shared_cash_flow_cases_across_parser_summary_and_database(self):
        for case in CASES:
            with self.subTest(case=case["name"]), tempfile.TemporaryDirectory() as td:
                root=Path(td)
                journal=root/"journal.jsonl"
                write_rows(journal,case["rows"])
                parsed=economics.xemm_journal(journal,"HYPE")
                self.assertEqual(parsed["malformed_rows"],0)
                self.assertEqual(len(parsed["trades"]),1)
                trade=parsed["trades"][0]
                for key,value in case["expected"].items():
                    expected=value if key=="economic_status" or value is None else Decimal(value)
                    self.assertEqual(trade[key],expected,key)
                since=combined_pnl.parse_dt("2026-01-01T00:00:00Z")
                now=combined_pnl.parse_dt("2026-01-03T00:00:00Z")
                summary=combined_pnl.summarize_xemm_journal(journal,"HYPE",since,now,False)
                expected=case["expected"]["net_pnl_usdc"]
                expected=None if expected is None else Decimal(expected)
                self.assertEqual(summary["net_pnl_usdc"],expected)
                conn=trade_history.open_db(root/"history.sqlite")
                try:
                    trade_history.init_db(conn)
                    for _ in range(2):
                        trade_history.ingest_xemm_journal(conn,journal,market="HYPE")
                    report=trade_history.report_from_db(conn,market="HYPE",since=since,now=now,db_path=root/"history.sqlite")
                    self.assertEqual(report["total"]["trades"],1)
                    self.assertEqual(report["total"]["net_pnl_usdc"],expected)
                    quantities={r["venue"]:Decimal(r["qty"]) for r in conn.execute("SELECT venue,SUM(CAST(qty AS REAL)) AS qty FROM venue_fills GROUP BY venue")}
                    if case["name"]=="partial":
                        self.assertAlmostEqual(quantities["lighter"],Decimal("0.12"),places=12)
                finally:
                    conn.close()

    def test_a_taker_row_hedged_on_hyperliquid_reads_like_a_lighter_one(self):
        fill = lambda px, fee: {"qty": "0.1", "vwap": px, "notional": str(Decimal(px) / 10), "fee_usd": fee, "fee_provenance": "venue"}
        old = {"schema_version": 2, "economic_status": "confirmed", "market": "HYPE", "timestamp": "2026-01-02T00:00:00Z",
               "direction": "SELL_LIGHTER_BUY_ASTER", "aster_order_id": 1, "lighter_client_order_index": 2,
               "aster_fill": fill("98", "0.00392"), "lighter_fill": fill("99", "0.004455"),
               "actual_gross_usd": "0.1", "actual_fees_usd": "0.008375", "actual_net_usd": "0.091625"}
        new = {("hedge" + k[len("lighter"):] if k.startswith("lighter") else k): v for k, v in old.items()}
        new.update(direction="SELL_HYPERLIQUID_BUY_ASTER", hedge_venue="hyperliquid")
        lh = dict(new, direction="SELL_HYPERLIQUID_BUY_LIGHTER", first_venue="lighter")
        for row in (old, new, lh):
            result = economics.taker_economics(row)
            self.assertEqual((result["economic_status"], result["net_pnl_usdc"], result["lighter_client_order_index"]), ("confirmed", Decimal("0.091625"), "2"))

    def test_pairing_precedes_window_filter_and_no_future_fill_is_used(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/"events.jsonl"
            rows=CASES[0]["rows"]
            write_rows(path,rows)
            boundary=economics.event_time(rows[1])
            summary=combined_pnl.summarize_xemm_journal(path,"HYPE",boundary,boundary,False)
            self.assertEqual(summary["net_pnl_usdc"],Decimal("0.11"))
            self.assertEqual(summary["trades"],1)
            before=combined_pnl.summarize_xemm_journal(path,"HYPE",economics.event_time(rows[0]),economics.event_time(rows[0]),False)
            self.assertEqual(before["unmatched_fills"],1)
            self.assertEqual(before["gross_pnl_usdc"],Decimal(0))

    def test_missing_fees_never_enable_projection_and_nonfinite_values_are_rejected(self):
        start=combined_pnl.parse_dt("2026-01-01")
        end=combined_pnl.parse_dt("2026-01-02")
        self.assertIsNone(combined_pnl.projection(None,Decimal(100),start,end)["window_return_pct"])
        for value in ["NaN","Infinity","-Infinity","bad"]:
            self.assertIsNone(economics.optional_decimal(value))

    def test_nullable_schema_preserves_legacy_rows_and_unknown_sql_values(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            conn=trade_history.open_db(root/"history.sqlite")
            try:
                legacy=trade_history.SCHEMA_SQL
                for name in ("gross_pnl_usdc","policy_fees_usdc","net_pnl_usdc","aster_fee_usdc",
                             "lighter_fee_usdc","aster_fee_rate","lighter_fee_rate","price","notional_usdc","fee_rate","policy_fee_usdc"):
                    legacy=legacy.replace(f"    {name} TEXT,",f"    {name} TEXT NOT NULL,")
                conn.executescript(legacy)
                conn.execute("ALTER TABLE strategy_trades ADD COLUMN user_annotation TEXT DEFAULT 'keep'")
                conn.execute("CREATE VIEW saved_trade_view AS SELECT trade_key,user_annotation FROM strategy_trades")
                trade_history.init_db(conn)
                journal=root/"journal.jsonl"
                case=next(c for c in CASES if c["name"]=="unknown_fee")
                write_rows(journal,case["rows"])
                trade_history.ingest_xemm_journal(conn,journal,market="HYPE")
                values=conn.execute("SELECT net_pnl_usdc,policy_fees_usdc,lighter_fee_usdc,lighter_fee_rate,user_annotation FROM strategy_trades").fetchone()
                self.assertEqual(tuple(values),(None,None,None,None,"keep"))
                self.assertIsNone(conn.execute("SELECT policy_fee_usdc FROM venue_fills WHERE venue='lighter'").fetchone()[0])
                self.assertEqual(conn.execute("SELECT user_annotation FROM saved_trade_view").fetchone()[0],"keep")
                trade_history.init_db(conn)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM strategy_trades").fetchone()[0],1)
                self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(),[])
                self.assertEqual(conn.execute("PRAGMA foreign_keys").fetchone()[0],1)
            finally:
                conn.close()

    def test_unchanged_cumulative_observation_does_not_move_trade_time(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/"journal.jsonl"
            case=next(c for c in CASES if c["name"]=="cumulative_only")
            rows=json.loads(json.dumps(case["rows"]))
            expected=economics.event_time(rows[-1])
            later=json.loads(json.dumps(rows[-1]))
            later["ts_ms"]+=86400000
            rows.append(later)
            write_rows(path,rows)
            self.assertEqual(economics.xemm_journal(path,"HYPE")["trades"][0]["timestamp"],expected)

    def test_rebuild_preserves_original_repairs_values_and_replacement_keeps_backup(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            journal=root/"journal.jsonl"
            write_rows(journal,CASES[0]["rows"])
            args=argparse.Namespace(db=root/"history.sqlite",rebuild_out=None,market="HYPE",raw_fills=[],
                taker_trades=root/"missing-taker.jsonl",orchestrator_trades=root/"missing-orch.jsonl",xemm_journal=[journal])
            conn=trade_history.open_db(args.db)
            try:
                trade_history.init_db(conn)
                trade_history.ingest_xemm_journal(conn,journal,market="HYPE")
                conn.execute("ALTER TABLE strategy_trades ADD COLUMN user_annotation TEXT DEFAULT 'keep'")
                conn.execute("CREATE INDEX user_trade_index ON strategy_trades(user_annotation)")
                conn.execute("CREATE TABLE user_audit (trade_key TEXT)")
                conn.execute("CREATE TRIGGER user_trade_trigger AFTER UPDATE ON strategy_trades BEGIN INSERT INTO user_audit VALUES (new.trade_key); END")
                conn.execute("UPDATE strategy_trades SET net_pnl_usdc='999'")
                conn.commit()
            finally:
                conn.close()
            original=args.db.read_bytes()
            raw=journal.read_bytes()
            first=trade_history.build_repaired_database(args)
            second=trade_history.build_repaired_database(args)
            self.assertEqual(first["after"],second["after"])
            self.assertEqual(first["after"]["known_net_pnl_usdc"],"0.11")
            self.assertEqual(args.db.read_bytes(),original)
            self.assertEqual(journal.read_bytes(),raw)
            applied=trade_history.replace_reviewed_database(args)
            backup=sqlite3.connect(applied["backup"])
            fixed=sqlite3.connect(args.db)
            try:
                self.assertEqual(backup.execute("SELECT net_pnl_usdc FROM strategy_trades").fetchone()[0],"999")
                self.assertEqual(fixed.execute("SELECT net_pnl_usdc FROM strategy_trades").fetchone()[0],"0.11")
                self.assertEqual(fixed.execute("SELECT user_annotation FROM strategy_trades").fetchone()[0],"keep")
                self.assertEqual(fixed.execute("SELECT COUNT(*) FROM sqlite_master WHERE name IN ('user_trade_index','user_trade_trigger')").fetchone()[0],2)
                self.assertEqual(fixed.execute("SELECT COUNT(*) FROM user_audit").fetchone()[0],1)
            finally:
                backup.close()
                fixed.close()

    def test_reviewed_replacement_rejects_concurrent_original_changes(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            journal=root/"journal.jsonl"
            write_rows(journal,CASES[0]["rows"])
            args=argparse.Namespace(db=root/"history.sqlite",rebuild_out=None,market="HYPE",raw_fills=[],
                taker_trades=root/"missing-taker",orchestrator_trades=root/"missing-orch",xemm_journal=[journal])
            conn=trade_history.open_db(args.db)
            try:
                trade_history.init_db(conn)
                conn.commit()
            finally:
                conn.close()
            trade_history.build_repaired_database(args)
            conn=sqlite3.connect(args.db)
            try:
                conn.execute("INSERT INTO reconciliation_events(created_at,mode,market,severity,subject,detail) VALUES ('now','lan','HYPE','info','concurrent','keep')")
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(ValueError,"changed since review"):
                trade_history.replace_reviewed_database(args)

    def test_raw_per_fill_fees_repair_legacy_results_and_survive_refresh(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            ledger=root/"taker.jsonl"
            old={"timestamp":"2026-01-02T00:00:00Z","market":"HYPE","direction":"SELL_ASTER_BUY_LIGHTER",
                "qty":"1","aster_order_id":11,"lighter_client_order_index":22,
                "actual_gross_usd":"1","actual_fees_usd":"0.04028","actual_net_usd":"0.95972",
                "aster_fill":{"qty":"1","vwap":"100","notional":"100","fee_usd":"0.04"},
                "lighter_fill":{"qty":"1","vwap":"99","notional":"99","fee_usd":"0.00028"}}
            write_rows(ledger,[old])
            raw=root/"own-trades.jsonl"
            write_rows(raw,[
                {"market":"HYPE","venue":"aster","side":"sell","order_id":"11","trade_id":"A1","qty":"1","px":"100",
                    "commission":"0.04","commission_asset":"USDT","timestamp":"2026-01-02T00:00:00Z"},
                {"market":"HYPE","venue":"lighter","side":"buy","client_order_index":22,"order_id":"L1","trade_id":"H1",
                    "qty":"1","px":"99","notional_usd":"99","maker":False,"fee_ticks":"280","timestamp":"2026-01-02T00:00:00Z"}])
            conn=trade_history.open_db(root/"history.sqlite")
            try:
                trade_history.init_db(conn)
                trade_history.ingest_taker_trades(conn,ledger,market="HYPE")
                outcome=trade_history.repair_raw_fees(conn,[raw],"HYPE")
                self.assertEqual(outcome["repaired_trades"],1)
                trade_history.ingest_taker_trades(conn,ledger,market="HYPE")
                r=conn.execute("SELECT net_pnl_usdc,economic_status,source FROM strategy_trades").fetchone()
                self.assertEqual(tuple(r),("0.93228","confirmed","raw_execution_fills"))
                incomplete=[]
                for row in map(json.loads,raw.read_text().splitlines()):
                    row.pop("commission",None)
                    row.pop("fee_ticks",None)
                    incomplete.append(row)
                write_rows(raw,incomplete)
                self.assertEqual(trade_history.repair_raw_fees(conn,[raw],"HYPE")["repaired_trades"],0)
                self.assertEqual(conn.execute("SELECT net_pnl_usdc FROM strategy_trades").fetchone()[0],"0.93228")
            finally:
                conn.close()

    def test_rust_nanosecond_and_naive_timestamps_parse_as_utc(self):
        from datetime import datetime, timezone
        expected=datetime(2026,9,23,12,0,0,123456,tzinfo=timezone.utc)
        for raw in ["2026-09-23T12:00:00.123456789Z","2026-09-23T14:00:00.123456789+02:00","2026-09-23T12:00:00.123456"]:
            with self.subTest(raw=raw):
                self.assertEqual(economics.parse_timestamp(raw),expected)
        self.assertIsNone(economics.parse_timestamp("not a time"))
        self.assertIsNone(economics.parse_timestamp(None))
        with self.assertRaises(ValueError):
            combined_pnl.parse_dt("not a time")

    def test_malformed_rate_or_role_evidence_remains_unknown(self):
        base={"notional_usd":"100","maker":False,"fee_ticks":"280","fee_usd":"0.028"}
        self.assertEqual(economics.fill_fee(base,trusted=True),Decimal("0.028"))
        for changed in [{"fee_ticks":"bad"},{"maker":"false"},{"fee_complete":False},{"fee_usd":None}]:
            with self.subTest(changed=changed):
                self.assertIsNone(economics.fill_fee({**base,**changed},trusted=True))

    def test_external_confirmation_is_preserved_and_inconsistent_numbers_are_not_certified(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            path=root/"journal.jsonl"
            write_rows(path,CASES[0]["rows"])
            conn=trade_history.open_db(root/"history.sqlite")
            try:
                trade_history.init_db(conn)
                trade_history.ingest_xemm_journal(conn,path,market="HYPE")
                conn.execute("UPDATE strategy_trades SET confirmation_status='exchange_confirmed',policy_fees_usdc='0.02',lighter_fee_usdc='0.02',net_pnl_usdc='0.10'")
                trade_history.ingest_xemm_journal(conn,path,market="HYPE")
                self.assertEqual(trade_history.database_overview(conn)["known_net_pnl_usdc"],"0.1")
                conn.execute("UPDATE strategy_trades SET net_pnl_usdc='999'")
                self.assertEqual(trade_history.database_overview(conn)["incomplete_trades"],1)
            finally:
                conn.close()

    def test_fee_repair_does_not_duplicate_an_exchange_confirmed_fill(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            journal=root/"journal.jsonl"
            write_rows(journal,CASES[0]["rows"])
            raw=root/"own-fills.jsonl"
            rows=[{"schema_version":2,"economic_status":"confirmed","market":"HYPE","detail":{
                "logical_id":"a","venue":"lighter","side":"sell","qty":"0.12","px":"101",
                "fee_usd":"0.01","order_id":"L1","trade_id":"T1"}}]
            conn=trade_history.open_db(root/"history.sqlite")
            try:
                trade_history.init_db(conn)
                trade_history.ingest_xemm_journal(conn,journal,market="HYPE")
                conn.execute("UPDATE venue_fills SET confirmation_status='exchange_confirmed' WHERE venue='aster'")
                for include_aster in (False,True):
                    if include_aster:
                        rows.append({"schema_version":2,"economic_status":"confirmed","market":"HYPE","detail":{
                            "logical_id":"a","venue":"aster","side":"buy","qty":"0.2","px":"100",
                            "fee_usd":"0","order_id":"A1","trade_id":"M1"}})
                    write_rows(raw,rows)
                    self.assertEqual(trade_history.repair_raw_fees(conn,[raw],"HYPE")["repaired_trades"],1)
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM venue_fills").fetchone()[0],2)
                    self.assertEqual(conn.execute("SELECT qty FROM venue_fills WHERE venue='aster'").fetchone()[0],"0.2")
            finally:
                conn.close()

    def test_fee_repair_preserves_maker_volume_when_a_correction_uses_aster(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            journal=root/"journal.jsonl"
            case=next(c for c in CASES if c["name"]=="closing_recovery")
            write_rows(journal,case["rows"])
            raw=root/"own-fills.jsonl"
            write_rows(raw,[
                {"schema_version":2,"economic_status":"confirmed","market":"HYPE","detail":{
                    "logical_id":"a","venue":"aster","side":side,"order_id":oid,"trade_id":tid,
                    "qty":"0.05","px":px,"fee_usd":fee,"timestamp":"2026-01-02T00:00:00Z"}}
                for side,oid,tid,px,fee in [("buy","A1","M1","100","0.0005"),("sell","A2","T1","99","0.00198")]])
            conn=trade_history.open_db(root/"history.sqlite")
            try:
                trade_history.init_db(conn)
                trade_history.ingest_xemm_journal(conn,journal,market="HYPE")
                self.assertEqual(trade_history.repair_raw_fees(conn,[raw],"HYPE")["repaired_trades"],1)
                result=conn.execute("SELECT qty,aster_qty,net_pnl_usdc FROM strategy_trades").fetchone()
                self.assertEqual(tuple(result),("0.05","0.1","-0.05248"))
            finally:
                conn.close()


if __name__=="__main__":
    unittest.main()

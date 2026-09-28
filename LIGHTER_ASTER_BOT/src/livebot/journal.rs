//! Bounded nonblocking domain-event enqueue; JSON and disk work belong to the writer thread.

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::types::Side;
use super::account::Venue;
use super::exec::command::ExecutionTrade;
use super::fills::{AsterFill, HedgeIntent};

const JOURNAL_CAP: usize = 65_536;

#[derive(Debug, Clone, Serialize)]
pub struct MakerFillRecord {
    pub logical_id: String,
    pub maker_side: Side,
    pub qty: Decimal,
    pub px: Decimal,
    pub notional_usd: Decimal,
    pub fee_usd: Option<Decimal>,
    pub fee_complete: bool,
    pub commission: Option<Decimal>,
    pub commission_asset: Option<String>,
    pub order_id: String,
    pub trade_id: String,
    pub client_id: String,
    pub reduce_only: bool,
    pub event_time_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProgressRecord {
    pub attempt_id: String,
    pub logical_id: String,
    pub venue: Venue,
    pub side: Side,
    pub purpose: &'static str,
    pub cumulative_qty: Decimal,
    pub cumulative_quote_usd: Option<Decimal>,
    pub cumulative_fee_usd: Option<Decimal>,
    pub fee_complete: bool,
    pub terminal: bool,
    pub client_id: Option<String>,
    pub venue_order_id: Option<String>,
    pub tx_hash: Option<String>,
    pub nonce: Option<i64>,
    pub client_order_index: Option<i64>,
    pub created_ns: i64,
    pub wire_start_ns: Option<i64>,
    pub observed_ns: i64,
    pub event_time_ms: Option<i64>,
    pub book_source: Option<&'static str>,
    pub book_age_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuoteRecord {
    pub side: Side,
    pub price: Option<Decimal>,
    pub qty: Option<Decimal>,
    pub reason: Option<&'static str>,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticRecord {
    pub gate: &'static str,
    pub aster_qty: Decimal,
    pub lighter_qty: Decimal,
    pub pending_qty: Decimal,
    pub outstanding_attempts: usize,
    pub aster_age_ms: i64,
    pub lighter_age_ms: i64,
    pub bid_decision: &'static str,
    pub ask_decision: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum JournalDetail {
    MakerFill(MakerFillRecord),
    Progress(ProgressRecord),
    Trade(ExecutionTrade),
    Quote(QuoteRecord),
    Diagnostic(DiagnosticRecord),
    Order { client_id: String, venue_order_id: Option<String>, state: &'static str },
    Reason { reason: &'static str },
    Net { closed_qty: Decimal, realized_pnl: Decimal },
    Legacy(serde_json::Value), // cold/control callers and readable historical-compatible details
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalRecord {
    pub schema_version: u32,
    pub mono_ns: i64,
    pub ts_ms: i64,
    pub kind: &'static str,
    pub economic_status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market: Option<String>,
    pub detail: JournalDetail,
}

#[derive(Default)]
struct JournalState {
    unhealthy: AtomicBool,
    trip: OnceLock<super::breaker::TripSnapshot>,
    trip_path: OnceLock<PathBuf>,
    trip_wake: Notify,
    trip_persisted: AtomicBool,
}

pub struct JournalReceiver {
    rx: Receiver<JournalRecord>,
    state: Arc<JournalState>,
}

#[derive(Clone)]
pub struct Journal {
    tx: Option<Sender<JournalRecord>>,
    state: Arc<JournalState>,
}

impl Journal {
    #[cfg(test)]
    pub fn null() -> Self { Self { tx: None, state: Arc::new(JournalState::default()) } }

    pub fn channel() -> (Self, JournalReceiver) {
        let (tx, rx) = channel(JOURNAL_CAP);
        let state = Arc::new(JournalState::default());
        (Self { tx: Some(tx), state: state.clone() }, JournalReceiver { rx, state })
    }

    pub fn healthy(&self) -> bool { !self.state.unhealthy.load(Ordering::Acquire) }
    pub fn configure_trip_path(&self, path: PathBuf) { let _ = self.state.trip_path.set(path); }

    /// Called after the strategy has latched, frozen and dispatched cancellation.
    pub fn trip(&self, snapshot: super::breaker::TripSnapshot) {
        let _ = self.state.trip.set(snapshot);
        self.state.trip_wake.notify_one();
    }

    pub fn persist_trip_cold(&self) -> Result<()> { persist_trip(&self.state) }
    pub fn detached_control(&self) -> Self { Self { tx: None, state: self.state.clone() } }

    pub fn typed(&self, mono_ns: i64, kind: &'static str, market: Option<String>, detail: JournalDetail, economic_status: &'static str) {
        let Some(tx) = &self.tx else { return };
        let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
        if tx.try_send(JournalRecord { schema_version: 2, mono_ns, ts_ms, kind, market, detail, economic_status }).is_err() {
            self.state.unhealthy.store(true, Ordering::Release);
        }
    }

    /// For cold/control records. Latency-critical callers use typed domain events.
    pub fn record(&self, mono_ns: i64, kind: &'static str, market: Option<String>, detail: serde_json::Value) {
        self.typed(mono_ns, kind, market, JournalDetail::Legacy(detail), "legacy_unverified");
    }

    pub fn reason(&self, mono_ns: i64, kind: &'static str, market: Option<String>, reason: &'static str) {
        self.typed(mono_ns, kind, market, JournalDetail::Reason { reason }, "confirmed");
    }

    pub fn maker_fill(&self, mono_ns: i64, logical_id: String, fill: &AsterFill) {
        let fee = fill.usd_fee();
        self.typed(mono_ns, "maker_fill", Some(fill.market.0.clone()), JournalDetail::MakerFill(MakerFillRecord {
            logical_id, maker_side: fill.aster_side, qty: fill.last_fill_qty, px: fill.last_fill_px,
            notional_usd: fill.last_fill_qty * fill.last_fill_px, fee_usd: fee, fee_complete: fee.is_some(),
            commission: fill.commission, commission_asset: fill.commission_asset.clone(),
            order_id: fill.order_id.clone(), trade_id: fill.trade_id.clone(), client_id: fill.client_id.clone(),
            reduce_only: fill.reduce_only, event_time_ms: fill.event_time_ms,
        }), if fee.is_some() { "confirmed" } else { "incomplete" });
    }

    pub fn progress(&self, mono_ns: i64, intent: &HedgeIntent) {
        let wire = intent.wire.as_ref();
        let complete = intent.fee_usd.is_some() && intent.filled_quote_usd.is_some();
        self.typed(mono_ns, "execution_progress", Some(intent.market.0.clone()), JournalDetail::Progress(ProgressRecord {
            attempt_id: intent.cloid.to_hex(), logical_id: intent.logical_id.to_hex(), venue: intent.venue,
            side: intent.hedge_side, purpose: match intent.purpose { super::fills::IntentPurpose::Hedge => "hedge", super::fills::IntentPurpose::ReduceDelta => "reduce_delta" },
            cumulative_qty: intent.filled_qty, cumulative_quote_usd: intent.filled_quote_usd,
            cumulative_fee_usd: intent.fee_usd, fee_complete: intent.fee_usd.is_some(), terminal: intent.terminal,
            client_id: intent.client_id.clone(), venue_order_id: intent.hl_oid.clone(),
            tx_hash: wire.and_then(|w| w.tx_hash.clone()), nonce: wire.and_then(|w| w.nonce),
            client_order_index: wire.and_then(|w| w.client_order_index), created_ns: intent.created_ns,
            wire_start_ns: wire.map(|w| w.sent_ns), observed_ns: mono_ns, event_time_ms: intent.event_time_ms, book_source: intent.book_source, book_age_ms: intent.book_age_ms,
        }), if complete { "confirmed" } else { "incomplete" });
    }

    pub fn execution_trade(&self, mono_ns: i64, trade: ExecutionTrade) {
        let complete = trade.fee_usd.is_some() && trade.identity_complete;
        self.typed(mono_ns, "execution_trade", Some(trade.market.clone()), JournalDetail::Trade(trade),
            if complete { "confirmed" } else { "incomplete" });
    }

    pub fn maker_progress(&self, mono_ns: i64, logical_id: super::ids::Cloid, market: &crate::types::MarketId,
        side: Side, client_id: &str, order_id: &str, qty: Decimal, quote: Option<Decimal>, terminal: bool, event_time_ms: Option<i64>) {
        self.typed(mono_ns, "maker_order_progress", Some(market.0.clone()), JournalDetail::Progress(ProgressRecord {
            attempt_id: client_id.to_owned(), logical_id: logical_id.to_hex(), venue: Venue::Aster,
            side, purpose: "maker", cumulative_qty: qty, cumulative_quote_usd: quote,
            cumulative_fee_usd: (qty == Decimal::ZERO).then_some(Decimal::ZERO), fee_complete: qty == Decimal::ZERO,
            terminal, client_id: Some(client_id.to_owned()), venue_order_id: Some(order_id.to_owned()),
            tx_hash: None, nonce: None, client_order_index: None, created_ns: mono_ns,
            wire_start_ns: None, observed_ns: mono_ns, event_time_ms, book_source: None, book_age_ms: None,
        }), if qty == Decimal::ZERO { "confirmed" } else { "incomplete" });
    }
}

fn persist_trip(state: &JournalState) -> Result<()> {
    if state.trip_persisted.load(Ordering::Acquire) { return Ok(()); }
    if let (Some(snapshot), Some(path)) = (state.trip.get(), state.trip_path.get()) {
        super::breaker::write_trip(path, &snapshot.record())?;
        state.trip_persisted.store(true, Ordering::Release);
    }
    Ok(())
}

fn flush_immediately(kind: &str) -> bool {
    !matches!(kind, "place" | "amend" | "cancel" | "quote_diagnostic")
}

/// Must run on its own OS thread: Write/flush are intentionally synchronous here.
pub async fn run_journal_writer<W: Write>(mut receiver: JournalReceiver, mut writer: W) -> Result<()> {
    let mut since_flush = 0;
    loop {
        tokio::select! {
            biased;
            _ = receiver.state.trip_wake.notified() => {
                if let Err(e) = persist_trip(&receiver.state) {
                    receiver.state.unhealthy.store(true, Ordering::Release);
                    return Err(e);
                }
            }
            rec = receiver.rx.recv() => {
                let Some(rec) = rec else { break };
                let result = (|| -> Result<()> {
                    serde_json::to_writer(&mut writer, &rec)?;
                    writer.write_all(b"\n")?;
                    since_flush += 1;
                    if flush_immediately(rec.kind) || since_flush >= 64 {
                        writer.flush()?;
                        since_flush = 0;
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    receiver.state.unhealthy.store(true, Ordering::Release);
                    return Err(e);
                }
            }
        }
    }
    persist_trip(&receiver.state)?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn records_are_written_as_jsonl() {
        let (j, rx) = Journal::channel();
        j.record(1, "place", Some("BTC".into()), json!({"px": 100}));
        j.record(2, "cancel", None, json!({"reason": "stale"}));
        drop(j); // close the channel so the writer loop ends
        let mut buf: Vec<u8> = Vec::new();
        run_journal_writer(rx, &mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["kind"], "place");
        assert_eq!(first["market"], "BTC");
        assert_eq!(first["detail"]["px"], 100);
        // Every record must carry a wall-clock stamp for PnL/history windowing.
        assert!(first["ts_ms"].as_i64().unwrap() > 1_500_000_000_000);
        // the second record omits market (skip_serializing_if None)
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert!(second.get("market").is_none());
    }

    #[tokio::test]
    async fn writer_flushes_risk_event_before_channel_closes() {
        use std::sync::atomic::AtomicUsize;
        struct Spy { bytes: Arc<std::sync::Mutex<Vec<u8>>>, flushes: Arc<AtomicUsize> }
        impl Write for Spy {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> { self.bytes.lock().unwrap().extend_from_slice(buf); Ok(buf.len()) }
            fn flush(&mut self) -> std::io::Result<()> { self.flushes.fetch_add(1, Ordering::Release); Ok(()) }
        }
        let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let flushes = Arc::new(AtomicUsize::new(0));
        let (journal, rx) = Journal::channel();
        let writer = tokio::spawn(run_journal_writer(rx, Spy { bytes: bytes.clone(), flushes: flushes.clone() }));
        journal.reason(1, "place", None, "quote");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while bytes.lock().unwrap().is_empty() { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(flushes.load(Ordering::Acquire), 0);
        journal.reason(2, "freeze", None, "risk");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while flushes.load(Ordering::Acquire)==0 { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert!(!writer.is_finished(), "flush must occur while sender is still live");
        drop(journal);
        writer.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_writer_marks_journal_unhealthy() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> { Err(std::io::Error::other("disk unavailable")) }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let (journal, rx) = Journal::channel();
        journal.reason(1,"freeze",None,"risk");
        assert!(run_journal_writer(rx, Broken).await.is_err());
        assert!(!journal.healthy());
    }
}

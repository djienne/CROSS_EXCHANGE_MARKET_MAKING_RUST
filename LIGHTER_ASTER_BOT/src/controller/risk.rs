//! Cross-engine loss stops, ported from orchestrator.py (`PnlTracker`, `TradeTracker`,
//! `breaker_reason`). One limit (`max_loss_usdc`), two independent tests, both inclusive and
//! evaluated every tick:
//! * **equity drawdown** `last_equity - baseline <= -limit`. The baseline is one sample,
//!   armed on the first sample and persisted, so a loss keeps counting across restarts; a
//!   baseline not refreshed for `baseline_max_gap_hours` is discarded.
//! * **realized trade PnL** since startup: unverified gains never count, unverified losses
//!   count in full.
//!
//! The engines' own loss breakers (10 USDC each) normally trip first; these are the backstop
//! across both engines and across restarts.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::SystemTime;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{iso, EventLog};
use crate::live_report::TradeSummary;
use crate::taker::pnl::{append_json_line, write_json_atomic};

/// The persisted equity baseline (`bot-<M>.baseline.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaselineFile {
    market: String,
    baseline_equity_usd: Decimal,
    baseline_ts: DateTime<Utc>,
    /// Refreshed at most hourly while running; the stale-gap rule measures from here.
    #[serde(default)]
    last_seen_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    source_bot: Option<String>,
}

pub struct EquityTracker {
    market: String,
    baseline_path: PathBuf,
    samples_path: PathBuf,
    max_loss: Decimal,
    baseline: Option<BaselineFile>,
    last_equity: Option<Decimal>,
    last_persist: Option<DateTime<Utc>>,
    /// Set by a reload: the first sample is compared with the reloaded baseline once.
    pending_divergence_check: bool,
}

impl EquityTracker {
    /// Reloads the persisted baseline unless it went unrefreshed for more than
    /// `max_gap_hours` (`0` keeps any). A corrupt file is reported and ignored.
    pub fn load(
        market: &str,
        baseline_path: PathBuf,
        samples_path: PathBuf,
        max_loss: Decimal,
        max_gap_hours: u64,
        now: DateTime<Utc>,
        events: &mut EventLog,
    ) -> Self {
        let mut tracker = Self {
            market: market.to_owned(),
            baseline_path,
            samples_path,
            max_loss,
            baseline: None,
            last_equity: None,
            last_persist: None,
            pending_divergence_check: false,
        };
        let path = tracker.baseline_path.display().to_string();
        let file = match std::fs::read_to_string(&tracker.baseline_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return tracker,
            Err(error) => Err(error.to_string()),
            Ok(text) => serde_json::from_str::<BaselineFile>(&text).map_err(|error| error.to_string()),
        };
        let file = match file {
            Ok(file) => file,
            Err(error) => {
                events.emit("baseline_load_failed", json!({"path": path, "error": error}));
                return tracker;
            }
        };
        let last_seen = file.last_seen_ts.unwrap_or(file.baseline_ts);
        let gap_hours = (now - last_seen).num_seconds() as f64 / 3600.0;
        if max_gap_hours > 0 && gap_hours > max_gap_hours as f64 {
            events.emit("baseline_stale_discarded", json!({
                "baseline_equity_usd": file.baseline_equity_usd, "baseline_ts": iso(file.baseline_ts),
                "last_seen_ts": iso(last_seen), "gap_hours": (gap_hours * 10.0).round() / 10.0, "max_gap_hours": max_gap_hours,
            }));
            let _ = std::fs::remove_file(&tracker.baseline_path);
            return tracker;
        }
        tracker.baseline = Some(file);
        tracker.pending_divergence_check = true;
        tracker
    }

    /// Records one equity sample computed by `source_bot`'s status. Arms the baseline on the
    /// first sample, refreshes the persisted file hourly, and appends the sample to the equity log.
    pub fn record(
        &mut self,
        equity: Decimal,
        source_bot: &str,
        accounts: &Value,
        now: DateTime<Utc>,
        events: &mut EventLog,
    ) -> Value {
        match &self.baseline {
            None => {
                self.baseline = Some(BaselineFile {
                    market: self.market.clone(),
                    baseline_equity_usd: equity,
                    baseline_ts: now,
                    last_seen_ts: Some(now),
                    source_bot: Some(source_bot.to_owned()),
                });
                self.persist(now, events);
            }
            Some(baseline) if self.pending_divergence_check => {
                let gap = equity - baseline.baseline_equity_usd;
                if gap.abs() >= self.max_loss * dec!(0.2) {
                    // Usually the equity definition changed while down. Warn, but do NOT
                    // re-anchor: a real loss during downtime must keep counting.
                    events.emit("baseline_restart_gap", json!({
                        "baseline_equity_usd": baseline.baseline_equity_usd, "baseline_ts": iso(baseline.baseline_ts),
                        "first_sample_equity_usd": equity, "gap_usdc": gap,
                        "baseline_source_bot": baseline.source_bot, "sample_source_bot": source_bot,
                    }));
                }
            }
            _ => {}
        }
        self.pending_divergence_check = false;
        if self.baseline.is_some() && self.last_persist.is_none_or(|at| now - at >= chrono::Duration::hours(1)) {
            self.persist(now, events);
        }
        self.last_equity = Some(equity);
        let baseline = self.baseline.as_ref().map(|b| b.baseline_equity_usd);
        let sample = json!({
            "timestamp": iso(now), "market": self.market, "source_bot": source_bot,
            "total_equity_usd": equity.normalize(), "baseline_equity_usd": baseline.map(|b| b.normalize()),
            "equity_pnl_usdc": baseline.map(|b| (equity - b).normalize()),
            "aster_equity_usd": accounts.get("aster_equity_usd"), "lighter_equity_usd": accounts.get("lighter_equity_usd"),
            "total_available_usd": accounts.get("total_available_usd"),
        });
        if let Err(error) = append_json_line(&self.samples_path, &sample, false) {
            events.emit("equity_sample_write_failed", json!({"error": format!("{error:#}")}));
        }
        sample
    }

    fn persist(&mut self, now: DateTime<Utc>, events: &mut EventLog) {
        let Some(baseline) = self.baseline.as_mut() else { return };
        baseline.last_seen_ts = Some(now);
        match write_json_atomic(&self.baseline_path, baseline, true) {
            Ok(()) => self.last_persist = Some(now),
            Err(error) => events.emit("baseline_persist_failed", json!({"error": format!("{error:#}")})),
        }
    }

    pub fn breach(&self) -> Option<String> {
        let pnl = self.last_equity? - self.baseline.as_ref()?.baseline_equity_usd;
        (pnl <= -self.max_loss).then(|| format!("equity_drawdown {pnl} <= -{}", self.max_loss))
    }

    pub fn summary(&self) -> Value {
        let baseline = self.baseline.as_ref();
        json!({
            "baseline_ts": baseline.map(|b| iso(b.baseline_ts)),
            "baseline_equity_usd": baseline.map(|b| b.baseline_equity_usd.normalize()),
            "baseline_source_bot": baseline.and_then(|b| b.source_bot.clone()),
            "last_equity_usd": self.last_equity.map(|e| e.normalize()),
            "equity_pnl_usdc": self.last_equity.zip(baseline).map(|(e, b)| (e - b.baseline_equity_usd).normalize()),
        })
    }
}

/// One trade's contribution to the totals (`TradeTracker.impact`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Impact {
    /// `schema_version >= 2`, `economic_status == "confirmed"`, and gross, fees and net all
    /// present with `|gross - fees - net| <= 1e-8`.
    known: bool,
    /// What the stop counts: `net` when known, else `min(0, net or 0)` — an unverified gain
    /// cannot offset a verified loss, while a conservative loss estimate can stop risk.
    risk: Decimal,
    net: Decimal,
    gross: Decimal,
    fees: Decimal,
}

impl Impact {
    fn new(schema_version: u64, economic_status: Option<&str>, gross: Option<Decimal>, fees: Option<Decimal>, net: Option<Decimal>) -> Self {
        let consistent = match (gross, fees, net) {
            (Some(gross), Some(fees), Some(net)) => (gross - fees - net).abs() <= dec!(0.00000001),
            _ => false,
        };
        let known = schema_version >= 2 && economic_status == Some("confirmed") && consistent;
        if known {
            let (gross, fees, net) = (gross.unwrap_or_default(), fees.unwrap_or_default(), net.unwrap_or_default());
            Self { known, risk: net, net, gross, fees }
        } else {
            Self { risk: net.unwrap_or_default().min(Decimal::ZERO), ..Self::default() }
        }
    }
}

#[derive(Debug, Default)]
struct Totals {
    trades: i64,
    incomplete: i64,
    wins: i64,
    risk_net: Decimal,
    known_net: Decimal,
    known_gross: Decimal,
    known_fees: Decimal,
}

impl Totals {
    fn adjust(&mut self, impact: Impact, sign: i64) {
        let signed = Decimal::from(sign);
        self.trades += sign;
        if !impact.known {
            self.incomplete += sign;
        } else if impact.net > Decimal::ZERO {
            self.wins += sign;
        }
        self.risk_net += impact.risk * signed;
        self.known_net += impact.net * signed;
        self.known_gross += impact.gross * signed;
        self.known_fees += impact.fees * signed;
    }
}

/// Trades both engines booked since startup: the taker's ledger (`trades_<M>.jsonl`, tailed)
/// and XEMM's journal (summarized by `live_report` when it changes). One current record per
/// trade: a re-summarized XEMM trade replaces its previous impact.
pub struct RealizedTrades {
    market: String,
    since: DateTime<Utc>,
    taker_ledger: PathBuf,
    offset: u64,
    xemm_journal: PathBuf,
    /// Size and mtime of the journal at the last successful summary; unchanged = skip.
    journal_seen: Option<(u64, SystemTime)>,
    rows: HashMap<String, Impact>,
    totals: Totals,
    xemm_report_failures: u32,
    malformed_rows: usize,
}

impl RealizedTrades {
    pub fn new(market: &str, since: DateTime<Utc>, taker_ledger: PathBuf, xemm_journal: PathBuf) -> Self {
        Self {
            market: market.to_owned(),
            since,
            taker_ledger,
            offset: 0,
            xemm_journal,
            journal_seen: None,
            rows: HashMap::new(),
            totals: Totals::default(),
            xemm_report_failures: 0,
            malformed_rows: 0,
        }
    }

    /// Skips the taker ledger rows already present at startup.
    pub fn prime(&mut self) {
        self.offset = std::fs::metadata(&self.taker_ledger).map(|m| m.len()).unwrap_or(0);
    }

    /// Books new trades; returns how many new taker ledger rows arrived (in reduce mode each
    /// one extends the lease).
    pub fn poll(&mut self, events: &mut EventLog) -> usize {
        let new_taker_rows = match self.read_taker_rows() {
            Ok(rows) => rows.into_iter().filter(|row| self.book_taker(row)).count(),
            Err(error) => {
                events.emit("taker_ledger_read_failed", json!({"path": self.taker_ledger.display().to_string(), "error": format!("{error:#}")}));
                0
            }
        };
        self.poll_xemm(events);
        new_taker_rows
    }

    /// Complete lines appended since the last read. A shrunken file was rotated or truncated:
    /// it is re-read from the start, and the per-trade keys drop rows already booked.
    fn read_taker_rows(&mut self) -> Result<Vec<Value>> {
        let len = match std::fs::metadata(&self.taker_ledger) {
            Ok(meta) => meta.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.offset = 0;
                return Ok(Vec::new());
            }
            Err(error) => return Err(error.into()),
        };
        if len < self.offset {
            self.offset = 0;
        }
        let mut reader = BufReader::new(File::open(&self.taker_ledger)?);
        reader.seek(SeekFrom::Start(self.offset))?;
        let mut rows = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 || !line.ends_with('\n') {
                break; // a partial trailing line waits for the writer to finish it
            }
            self.offset += read as u64;
            if let Ok(row @ Value::Object(_)) = serde_json::from_str::<Value>(line.trim()) {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn book_taker(&mut self, raw: &Value) -> bool {
        let text = |key: &str| raw.get(key).map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_owned));
        if raw.get("market").and_then(Value::as_str).is_some_and(|market| market != self.market) {
            return false;
        }
        let (aster, lighter) = (text("aster_order_id"), text("lighter_client_order_index"));
        let key = if text("direction").is_some_and(|d| d.eq_ignore_ascii_case("RECOVERY")) {
            format!("taker:recovery:{aster:?}:{lighter:?}:{:?}", text("timestamp"))
        } else if aster.is_some() || lighter.is_some() {
            format!("taker:{aster:?}:{lighter:?}")
        } else {
            format!("taker:fallback:{raw}")
        };
        if self.rows.contains_key(&key) {
            return false;
        }
        let timestamp = raw.get("timestamp").and_then(Value::as_str).and_then(|ts| ts.parse::<DateTime<Utc>>().ok());
        if timestamp.is_some_and(|ts| ts < self.since) {
            return false;
        }
        let impact = Impact::new(
            raw.get("schema_version").and_then(Value::as_u64).unwrap_or(1),
            Some(raw.get("economic_status").and_then(Value::as_str).unwrap_or("legacy_unverified")),
            py_decimal(raw.get("actual_gross_usd")),
            py_decimal(raw.get("actual_fees_usd")),
            py_decimal(raw.get("actual_net_usd")),
        );
        self.book(key, impact);
        true
    }

    /// Ponytail: re-summarizes the whole journal whenever it changed (it grows for the life
    /// of the XEMM stem). Fine at hours-to-days of XEMM activity; past that, summarize
    /// incrementally from an offset or rotate the journal per session.
    fn poll_xemm(&mut self, events: &mut EventLog) {
        let Ok(meta) = std::fs::metadata(&self.xemm_journal) else { return };
        let seen = (meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH));
        if self.journal_seen == Some(seen) {
            return;
        }
        match crate::live_report::summarize_path(&self.xemm_journal, Some(&self.market), Some(self.since.timestamp_millis())) {
            Ok(summary) => {
                self.journal_seen = Some(seen);
                self.xemm_report_failures = 0;
                self.malformed_rows = summary.malformed_rows;
                for trade in &summary.trades {
                    self.book(format!("xemm:{}", trade.cloid), xemm_impact(trade));
                }
            }
            Err(error) => {
                self.xemm_report_failures += 1;
                events.emit("xemm_live_report_failed", json!({
                    "error": format!("{error:#}"), "journal": self.xemm_journal.display().to_string(),
                    "consecutive_failures": self.xemm_report_failures,
                }));
            }
        }
    }

    fn book(&mut self, key: String, impact: Impact) {
        if let Some(previous) = self.rows.insert(key, impact) {
            self.totals.adjust(previous, -1);
        }
        self.totals.adjust(impact, 1);
    }

    pub fn breach(&self, max_loss: Decimal) -> Option<String> {
        let realized = self.totals.risk_net;
        (realized <= -max_loss).then(|| format!("realized_trade_pnl {realized} <= -{max_loss}"))
    }

    pub fn summary(&self) -> Value {
        let t = &self.totals;
        let complete = t.incomplete == 0 && self.malformed_rows == 0;
        json!({
            "since": iso(self.since), "trades": t.trades, "incomplete_trades": t.incomplete, "wins": t.wins,
            "risk_net_pnl_usdc": t.risk_net.normalize(), "known_net_pnl_usdc": t.known_net.normalize(),
            "net_pnl_usdc": complete.then(|| t.known_net.normalize()),
            "gross_pnl_usdc": complete.then(|| t.known_gross.normalize()),
            "fees_usdc": complete.then(|| t.known_fees.normalize()),
            "complete": complete, "xemm_malformed_rows": self.malformed_rows,
            "xemm_report_failures": self.xemm_report_failures,
        })
    }
}

fn xemm_impact(trade: &TradeSummary) -> Impact {
    let fees = trade.aster_fee.zip(trade.lighter_fee).map(|(aster, lighter)| aster + lighter);
    Impact::new(trade.schema_version.into(), Some(trade.economic_status), trade.gross_pnl, fees, trade.net_pnl)
}

/// Startup gate for the bot breaker (`bot-<M>.breaker.json`, written by every safe halt).
/// Without `ack` a latched breaker refuses the start; with it the file is archived as
/// `<name>.acked.<stamp>`. An equity-drawdown breaker also needs `reset_baseline`, or the
/// reloaded baseline would trip again on the first tick.
pub fn check_breaker(path: &Path, ack: bool, reset_baseline: bool, events: &mut EventLog) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let body: Value = std::fs::read_to_string(path).ok().and_then(|text| serde_json::from_str(&text).ok()).unwrap_or(Value::Null);
    let reason = body.get("reason").and_then(Value::as_str).unwrap_or("?");
    let detail = body.pointer("/details/breaker_reason").and_then(Value::as_str).unwrap_or("");
    if !ack {
        events.emit("breaker_present_abort", json!({"path": path.display().to_string(), "reason": reason}));
        bail!(
            "bot breaker latched: {} (reason={reason} {detail}). Review what happened, then restart with --ack-breaker",
            path.display()
        );
    }
    if detail.starts_with("equity_drawdown") && !reset_baseline {
        bail!("{} is an equity-drawdown halt: also pass --reset-breaker-baseline, or the persisted baseline trips again at once", path.display());
    }
    let archived = PathBuf::from(format!("{}.acked.{}", path.display(), Utc::now().format("%Y%m%dT%H%M%SZ")));
    std::fs::rename(path, &archived)?;
    events.emit("breaker_acknowledged", json!({"path": path.display().to_string(), "archived": archived.display().to_string()}));
    Ok(())
}

/// `Decimal(str(raw))` when finite, else `None` (economics.optional_decimal): how the retired
/// orchestrator.py read engine reports, kept for the ledger's JSON numbers.
pub fn py_decimal(raw: Option<&Value>) -> Option<Decimal> {
    let text = match raw? {
        Value::String(text) => text.trim().to_owned(),
        Value::Number(number) => number.to_string(),
        // str(True) / str([..]) / str({..}) are not decimals; None is unknown.
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => return None,
    };
    // Plain and exponent forms; Infinity/NaN are not finite, so unknown as in the original.
    Decimal::from_str(&text).ok().or_else(|| text.contains(['e', 'E']).then(|| Decimal::from_scientific(&text).ok()).flatten())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dryrun::tests::temp_dir;

    fn tracker(dir: &Path, now: DateTime<Utc>, events: &mut EventLog) -> EquityTracker {
        EquityTracker::load("HYPE", dir.join("b.json"), dir.join("s.jsonl"), dec!(15), 48, now, events)
    }

    #[test]
    fn drawdown_stop_is_inclusive_and_survives_a_restart() {
        let dir = temp_dir("bot-risk");
        let mut events = EventLog::new(dir.join("events.jsonl"));
        let now = Utc::now();
        let mut equity = tracker(&dir, now, &mut events);
        equity.record(dec!(100), "T", &Value::Null, now, &mut events);
        assert!(dir.join("b.json").exists(), "the first sample arms the baseline");
        equity.record(dec!(85.01), "T", &Value::Null, now, &mut events);
        assert_eq!(equity.breach(), None);
        equity.record(dec!(85), "T", &Value::Null, now, &mut events);
        assert!(equity.breach().unwrap().starts_with("equity_drawdown"));
        // The baseline survives a restart, so the loss keeps counting.
        let mut reloaded = tracker(&dir, now + chrono::Duration::hours(1), &mut events);
        reloaded.record(dec!(84), "X", &Value::Null, now, &mut events);
        assert!(reloaded.breach().is_some());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn stale_baseline_is_discarded() {
        let dir = temp_dir("bot-risk");
        let mut events = EventLog::new(dir.join("events.jsonl"));
        let armed = Utc::now() - chrono::Duration::days(5);
        let mut equity = tracker(&dir, armed, &mut events);
        equity.record(dec!(100), "T", &Value::Null, armed, &mut events);
        assert!(tracker(&dir, Utc::now(), &mut events).baseline.is_none());
        assert!(!dir.join("b.json").exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn realized_stop_counts_unverified_losses_but_never_unverified_gains() {
        let confirmed = |net: Decimal| Impact::new(2, Some("confirmed"), Some(net + dec!(1)), Some(dec!(1)), Some(net));
        assert!(confirmed(dec!(2)).known);
        assert_eq!(Impact::new(2, Some("estimated"), None, None, Some(dec!(5))).risk, Decimal::ZERO);
        assert_eq!(Impact::new(2, Some("estimated"), None, None, Some(dec!(-1.5))).risk, dec!(-1.5));
        assert_eq!(Impact::new(1, Some("confirmed"), Some(dec!(3)), Some(dec!(1)), Some(dec!(2))).risk, Decimal::ZERO);
        // Inconsistent economics are not "known" even when confirmed.
        assert!(!Impact::new(2, Some("confirmed"), Some(dec!(3)), Some(dec!(1)), Some(dec!(1))).known);

        let dir = temp_dir("bot-risk");
        let ledger = dir.join("trades_HYPE.jsonl");
        std::fs::write(&ledger, "{\"aster_order_id\":1,\"lighter_client_order_index\":1,\"actual_net_usd\":\"-50\"}\n").unwrap();
        let mut trades = RealizedTrades::new("HYPE", Utc::now() - chrono::Duration::hours(1), ledger.clone(), dir.join("none.jsonl"));
        trades.prime();
        let mut events = EventLog::new(dir.join("events.jsonl"));
        let row = |id: i64, status: &str, net: &str| {
            format!("{{\"schema_version\":2,\"economic_status\":\"{status}\",\"market\":\"HYPE\",\"aster_order_id\":{id},\"lighter_client_order_index\":{id},\"actual_gross_usd\":\"{net}\",\"actual_fees_usd\":\"0\",\"actual_net_usd\":\"{net}\"}}\n")
        };
        let append = |text: String| {
            use std::io::Write;
            std::fs::OpenOptions::new().append(true).open(&ledger).unwrap().write_all(text.as_bytes()).unwrap();
        };
        append(row(2, "confirmed", "-10") + &row(3, "estimated", "-4.99") + &row(4, "estimated", "40") + "{\"partial");
        assert_eq!(trades.poll(&mut events), 3, "the pre-startup row and the partial line are not booked");
        assert_eq!(trades.breach(dec!(15)), None);
        append("\n".to_string() + &row(3, "estimated", "-4.99") + &row(5, "confirmed", "-0.01"));
        assert_eq!(trades.poll(&mut events), 1, "a duplicate key is booked once");
        assert!(trades.breach(dec!(15)).unwrap().starts_with("realized_trade_pnl -15"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn breaker_needs_ack_and_a_drawdown_breaker_also_a_baseline_reset() {
        let dir = temp_dir("bot-risk");
        let mut events = EventLog::new(dir.join("events.jsonl"));
        let path = dir.join("bot-HYPE.breaker.json");
        check_breaker(&path, false, false, &mut events).unwrap();
        std::fs::write(&path, r#"{"reason":"pnl_breaker","details":{"breaker_reason":"equity_drawdown -15 <= -15"}}"#).unwrap();
        assert!(check_breaker(&path, false, false, &mut events).is_err());
        assert!(check_breaker(&path, true, false, &mut events).unwrap_err().to_string().contains("--reset-breaker-baseline"));
        check_breaker(&path, true, true, &mut events).unwrap();
        assert!(!path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

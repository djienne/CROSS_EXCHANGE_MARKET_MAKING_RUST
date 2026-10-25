use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::{mpsc, oneshot, Notify};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::taker::config::{EntryGateCfg, EntryGateMode};
use crate::taker::pnl::market_component;
use crate::taker::types::MarketId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunitySample {
    pub timestamp: DateTime<Utc>,
    pub market: String,
    pub direction: String,
    pub gross_edge_bps: Decimal,
    pub expected_net_margin_bps: Decimal,
    pub expected_net_usd: Decimal,
    pub qty: Decimal,
    pub sell_px: Decimal,
    pub buy_px: Decimal,
    pub ref_px: Decimal,
    pub top_depth_qty: Decimal,
    #[serde(default)]
    pub depth_guard_enabled: bool,
    #[serde(default)]
    pub liquidity_multiple: Decimal,
    #[serde(default)]
    pub depth_supported_qty: Decimal,
    #[serde(default)]
    pub sell_depth_target_qty: Decimal,
    #[serde(default)]
    pub buy_depth_target_qty: Decimal,
    #[serde(default)]
    pub sell_depth_available_qty: Decimal,
    #[serde(default)]
    pub buy_depth_available_qty: Decimal,
    #[serde(default)]
    pub sell_depth_worst_px: Decimal,
    #[serde(default)]
    pub buy_depth_worst_px: Decimal,
    #[serde(default)]
    pub sell_depth_levels_used: usize,
    #[serde(default)]
    pub buy_depth_levels_used: usize,
    #[serde(default)]
    pub sell_best_px: Decimal,
    #[serde(default)]
    pub buy_best_px: Decimal,
    #[serde(default)]
    pub sell_best_qty: Decimal,
    #[serde(default)]
    pub buy_best_qty: Decimal,
    pub aster_book_age_ms: i64,
    pub lighter_book_age_ms: i64,
    pub decision: String,
    pub gate_threshold_bps: Option<Decimal>,
    pub history_sample_count: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct OpportunityGateInput<'a> {
    pub timestamp: DateTime<Utc>,
    pub direction: &'a str,
    pub gross_edge_bps: Decimal,
    pub expected_net_margin_bps: Decimal,
    pub expected_net_usd: Decimal,
    pub qty: Decimal,
    pub sell_px: Decimal,
    pub buy_px: Decimal,
    pub ref_px: Decimal,
    pub top_depth_qty: Decimal,
    pub depth_guard_enabled: bool,
    pub liquidity_multiple: Decimal,
    pub depth_supported_qty: Decimal,
    pub sell_depth_target_qty: Decimal,
    pub buy_depth_target_qty: Decimal,
    pub sell_depth_available_qty: Decimal,
    pub buy_depth_available_qty: Decimal,
    pub sell_depth_worst_px: Decimal,
    pub buy_depth_worst_px: Decimal,
    pub sell_depth_levels_used: usize,
    pub buy_depth_levels_used: usize,
    pub sell_best_px: Decimal,
    pub buy_best_px: Decimal,
    pub sell_best_qty: Decimal,
    pub buy_best_qty: Decimal,
    pub aster_book_age_ms: i64,
    pub lighter_book_age_ms: i64,
    pub force_record: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct GateEvaluation {
    pub allow_execution: bool,
    pub would_allow: bool,
    pub threshold_bps: Option<Decimal>,
    pub sample_count: usize,
    pub decision: &'static str,
    pub recorded: bool,
}

/// Exact nearest-rank order statistic. The lower partition contains ceil(p*N/100)
/// observations; its largest member is the percentile. Counts preserve duplicate edges.
struct RankedWindow {
    percentile: Decimal,
    hours: u64,
    lower: BTreeMap<Decimal, usize>,
    upper: BTreeMap<Decimal, usize>,
    lower_count: usize,
    expiry: BTreeMap<(DateTime<Utc>, u64), Decimal>,
    serial: u64,
}

impl RankedWindow {
    fn new(cfg: &EntryGateCfg) -> Self {
        Self { percentile: cfg.entry_percentile, hours: cfg.history_window_hours,
            lower: BTreeMap::new(), upper: BTreeMap::new(), lower_count: 0,
            expiry: BTreeMap::new(), serial: 0 }
    }

    fn take(map: &mut BTreeMap<Decimal, usize>, key: Decimal) {
        let count = map.get_mut(&key).expect("partition key exists");
        *count -= 1;
        if *count == 0 { map.remove(&key); }
    }

    fn rebalance(&mut self) {
        let target = (self.percentile * Decimal::from(self.expiry.len()) / Decimal::from(100))
            .ceil().to_usize().expect("validated rank fits usize");
        while self.lower_count > target {
            let value = *self.lower.last_key_value().expect("lower nonempty").0;
            Self::take(&mut self.lower, value);
            *self.upper.entry(value).or_default() += 1;
            self.lower_count -= 1;
        }
        while self.lower_count < target {
            let value = *self.upper.first_key_value().expect("upper nonempty").0;
            Self::take(&mut self.upper, value);
            *self.lower.entry(value).or_default() += 1;
            self.lower_count += 1;
        }
    }

    fn insert(&mut self, timestamp: DateTime<Utc>, edge: Decimal) {
        self.serial += 1;
        self.expiry.insert((timestamp, self.serial), edge);
        if self.lower.last_key_value().is_some_and(|(last, _)| edge <= *last) {
            *self.lower.entry(edge).or_default() += 1;
            self.lower_count += 1;
        } else {
            *self.upper.entry(edge).or_default() += 1;
        }
        self.rebalance();
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        let cutoff = cutoff(now, self.hours);
        while self.expiry.first_key_value().is_some_and(|((timestamp, _), _)| *timestamp < cutoff) {
            let (_, edge) = self.expiry.pop_first().expect("expiry nonempty");
            if self.lower.contains_key(&edge) {
                Self::take(&mut self.lower, edge);
                self.lower_count -= 1;
            } else { Self::take(&mut self.upper, edge); }
        }
        self.rebalance();
    }

    fn snapshot(&self, sequence: u64) -> ThresholdSnapshot {
        ThresholdSnapshot { sequence, count: self.expiry.len(),
            percentile: self.lower.last_key_value().map(|(edge, _)| *edge),
            valid_through: self.expiry.first_key_value().map(|((timestamp, _), _)|
                *timestamp + Duration::hours(self.hours as i64)) }
    }
}

#[derive(Clone)]
struct ThresholdSnapshot {
    sequence: u64,
    count: usize,
    percentile: Option<Decimal>,
    valid_through: Option<DateTime<Utc>>,
}

enum HistoryCommand {
    Sample(u64, OpportunitySample),
    Stop(oneshot::Sender<()>),
}

pub struct OpportunityGate {
    cfg: EntryGateCfg,
    market: MarketId,
    path: PathBuf,
    snapshot: Arc<ArcSwap<ThresholdSnapshot>>,
    tx: mpsc::Sender<HistoryCommand>,
    healthy: Arc<AtomicBool>,
    changed: Arc<Notify>,
    sequence: u64,
    last_sample_at: Option<DateTime<Utc>>,
}

impl OpportunityGate {
    pub fn new(cfg: &EntryGateCfg, market: &MarketId, persist_dir: &str, now: DateTime<Utc>) -> Result<Self> {
        let path = PathBuf::from(persist_dir).join(format!("opportunities_{}.jsonl", market_component(market)));
        let mut window = RankedWindow::new(cfg);
        if cfg.active() {
            fs::create_dir_all(persist_dir)?;
            for sample in load_recent_samples(&path, market, cutoff(now, cfg.history_window_hours))? {
                if sample.timestamp <= now { window.insert(sample.timestamp, sample.gross_edge_bps); }
            }
        }
        let snapshot = Arc::new(ArcSwap::from_pointee(window.snapshot(0)));
        let healthy = Arc::new(AtomicBool::new(true));
        let changed = Arc::new(Notify::new());
        let (tx, mut rx) = mpsc::channel(1024);
        let worker_snapshot = snapshot.clone();
        let worker_health = healthy.clone();
        let worker_changed = changed.clone();
        let mut writer = if cfg.active() { Some(crate::taker::pnl::LockedJournal::open(&path)?) } else { None };
        std::thread::Builder::new().name("taker-history".to_string()).spawn(move || {
            while let Some(command) = rx.blocking_recv() {
                match command {
                    HistoryCommand::Sample(sequence, sample) => {
                        if let Err(error) = writer.as_mut().expect("active history writer").append(&sample, false) {
                            if worker_health.swap(false, Ordering::AcqRel) {
                                warn!("opportunity journal failed; entries blocked until a write succeeds: {error:#}");
                            }
                        } else {
                            // A write that succeeds again ends the failure, without a restart.
                            worker_health.store(true, Ordering::Release);
                            window.insert(sample.timestamp, sample.gross_edge_bps);
                            window.prune(sample.timestamp);
                        }
                        worker_snapshot.store(Arc::new(window.snapshot(sequence)));
                        worker_changed.notify_one();
                    }
                    HistoryCommand::Stop(ack) => { let _ = ack.send(()); break; }
                }
            }
        })?;
        Ok(Self { cfg: cfg.clone(), market: market.clone(), path, snapshot, tx, healthy, changed,
            sequence: 0, last_sample_at: None })
    }

    pub fn path(&self) -> &Path { &self.path }
    pub fn loaded_samples(&self) -> usize { self.snapshot.load().count }
    pub fn changed(&self) -> Arc<Notify> { self.changed.clone() }
    pub fn healthy(&self) -> bool { self.healthy.load(Ordering::Acquire) && !self.tx.is_closed() }

    pub async fn shutdown(&self) -> Result<()> {
        let (ack, done) = oneshot::channel();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            self.tx.send(HistoryCommand::Stop(ack)).await.context("history worker stopped")?;
            done.await.context("history drain acknowledgement missing")
        }).await.context("history drain exceeded five seconds")??;
        if !self.healthy.load(Ordering::Acquire) { anyhow::bail!("history writer failed"); }
        Ok(())
    }

    pub fn evaluate(&mut self, input: OpportunityGateInput<'_>, required: Decimal) -> GateEvaluation {
        let snapshot = self.snapshot.load_full();
        if !self.cfg.active() {
            return GateEvaluation { allow_execution: true, would_allow: true, threshold_bps: None,
                sample_count: snapshot.count, decision: "off", recorded: false };
        }
        let expired = snapshot.valid_through.is_some_and(|through| input.timestamp > through);
        if !self.healthy() || snapshot.sequence != self.sequence || expired {
            // No decision, but sampling never waits for one: the worker prunes the window at each
            // sample and a successful write ends a journal failure, so a gate that stopped sampling
            // here would stay pending for good (2026-10-02: a week of `history_pending`).
            let decision = if self.healthy() { "history_pending" } else { "history_unavailable" };
            let recorded = self.record_if_due(input, decision, None, snapshot.count, false);
            return GateEvaluation { allow_execution: false, would_allow: false, threshold_bps: None,
                sample_count: snapshot.count, decision, recorded };
        }
        let threshold = (required + self.cfg.min_extra_bps).max(snapshot.percentile.unwrap_or(required));
        let ready = snapshot.count >= self.cfg.min_history_samples;
        let would_allow = ready && input.gross_edge_bps >= threshold;
        let allow = ready && (would_allow || self.cfg.mode == EntryGateMode::Shadow);
        let decision = if !ready { "warmup_block" } else if would_allow { "would_execute" }
            else if self.cfg.mode == EntryGateMode::Shadow { "shadow_block" } else { "gated_out" };
        let threshold_bps = ready.then_some(threshold);
        let force = allow && input.force_record;
        let recorded = self.record_if_due(input, decision, threshold_bps, snapshot.count, force);
        GateEvaluation { allow_execution: allow && self.healthy(), would_allow, threshold_bps,
            sample_count: snapshot.count, decision, recorded }
    }

    fn record_if_due(&mut self, input: OpportunityGateInput<'_>, decision: &str,
        threshold_bps: Option<Decimal>, sample_count: usize, force: bool) -> bool {
        // A clock stepped back past the last sample does not hold sampling until it catches up.
        let interval = Duration::milliseconds(self.cfg.sample_interval_ms as i64);
        if !force && self.last_sample_at.is_some_and(|last| (Duration::zero()..interval).contains(&(input.timestamp - last))) {
            return false;
        }
        let sample = OpportunitySample {
            timestamp: input.timestamp, market: self.market.0.clone(), direction: input.direction.to_string(),
            gross_edge_bps: input.gross_edge_bps, expected_net_margin_bps: input.expected_net_margin_bps,
            expected_net_usd: input.expected_net_usd, qty: input.qty, sell_px: input.sell_px,
            buy_px: input.buy_px, ref_px: input.ref_px, top_depth_qty: input.top_depth_qty,
            depth_guard_enabled: input.depth_guard_enabled, liquidity_multiple: input.liquidity_multiple,
            depth_supported_qty: input.depth_supported_qty, sell_depth_target_qty: input.sell_depth_target_qty,
            buy_depth_target_qty: input.buy_depth_target_qty, sell_depth_available_qty: input.sell_depth_available_qty,
            buy_depth_available_qty: input.buy_depth_available_qty, sell_depth_worst_px: input.sell_depth_worst_px,
            buy_depth_worst_px: input.buy_depth_worst_px, sell_depth_levels_used: input.sell_depth_levels_used,
            buy_depth_levels_used: input.buy_depth_levels_used, sell_best_px: input.sell_best_px,
            buy_best_px: input.buy_best_px, sell_best_qty: input.sell_best_qty, buy_best_qty: input.buy_best_qty,
            aster_book_age_ms: input.aster_book_age_ms, lighter_book_age_ms: input.lighter_book_age_ms,
            decision: decision.to_string(), gate_threshold_bps: threshold_bps, history_sample_count: sample_count,
        };
        let next = self.sequence + 1;
        if self.tx.try_send(HistoryCommand::Sample(next, sample)).is_err() {
            if self.healthy.swap(false, Ordering::AcqRel) {
                warn!("opportunity history queue unavailable; entries blocked");
            }
            return false;
        }
        self.sequence = next;
        self.last_sample_at = Some(input.timestamp);
        true
    }
}

fn load_recent_samples(
    path: &Path,
    market: &MarketId,
    cutoff: DateTime<Utc>,
) -> Result<VecDeque<OpportunitySample>> {
    if !path.exists() {
        return Ok(VecDeque::new());
    }
    let file =
        File::open(path).with_context(|| format!("open opportunity history {}", path.display()))?;
    let mut samples = VecDeque::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("read opportunity history line {}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: OpportunitySample = match serde_json::from_str(&line) {
            Ok(row) => row,
            Err(e) => {
                warn!(
                    "skipping malformed opportunity history row {} line {}: {e:#}",
                    path.display(),
                    idx + 1
                );
                continue;
            }
        };
        if row.market == market.0 && row.timestamp >= cutoff {
            samples.push_back(row);
        }
    }
    Ok(samples)
}

#[cfg(test)]
fn percentile<I>(values: I, percentile: Decimal) -> Option<Decimal>
where
    I: IntoIterator<Item = Decimal>,
{
    let mut values: Vec<Decimal> = values.into_iter().collect();
    if values.is_empty() {
        return None;
    }
    values.sort();
    let p = percentile.to_f64().unwrap_or(90.0).clamp(0.0, 100.0);
    let rank = ((p / 100.0) * values.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(values.len() - 1);
    Some(values[idx])
}

fn cutoff(now: DateTime<Utc>, window_hours: u64) -> DateTime<Utc> {
    now - Duration::hours(window_hours as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn tmp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lighter_aster_taker_arb_gate_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }

    fn cfg(mode: EntryGateMode) -> EntryGateCfg {
        EntryGateCfg {
            enabled: true,
            mode,
            history_window_hours: 72,
            sample_interval_ms: 1000,
            min_history_samples: 3,
            entry_percentile: dec!(90),
            min_extra_bps: dec!(0.5),
        }
    }

    fn input(ts: DateTime<Utc>, gross_edge_bps: Decimal) -> OpportunityGateInput<'static> {
        OpportunityGateInput {
            timestamp: ts,
            direction: "SELL_ASTER_BUY_LIGHTER",
            gross_edge_bps,
            expected_net_margin_bps: gross_edge_bps - dec!(6),
            expected_net_usd: dec!(0.01),
            qty: dec!(0.20),
            sell_px: dec!(62.20),
            buy_px: dec!(62.14),
            ref_px: dec!(62.17),
            top_depth_qty: dec!(1.0),
            depth_guard_enabled: true,
            liquidity_multiple: dec!(10),
            depth_supported_qty: dec!(0.20),
            sell_depth_target_qty: dec!(2.00),
            buy_depth_target_qty: dec!(2.00),
            sell_depth_available_qty: dec!(2.00),
            buy_depth_available_qty: dec!(2.00),
            sell_depth_worst_px: dec!(62.20),
            buy_depth_worst_px: dec!(62.14),
            sell_depth_levels_used: 1,
            buy_depth_levels_used: 1,
            sell_best_px: dec!(62.20),
            buy_best_px: dec!(62.14),
            sell_best_qty: dec!(2.00),
            buy_best_qty: dec!(2.00),
            aster_book_age_ms: 10,
            lighter_book_age_ms: 20,
            force_record: true,
        }
    }

    fn sampled_input(ts: DateTime<Utc>, gross_edge_bps: Decimal) -> OpportunityGateInput<'static> {
        OpportunityGateInput {
            force_record: false,
            ..input(ts, gross_edge_bps)
        }
    }

    fn row(ts: DateTime<Utc>, market: &str, edge: Decimal) -> OpportunitySample {
        OpportunitySample {
            timestamp: ts,
            market: market.to_string(),
            direction: "SELL_ASTER_BUY_LIGHTER".to_string(),
            gross_edge_bps: edge,
            expected_net_margin_bps: edge - dec!(6),
            expected_net_usd: dec!(0.01),
            qty: dec!(0.20),
            sell_px: dec!(62.20),
            buy_px: dec!(62.14),
            ref_px: dec!(62.17),
            top_depth_qty: dec!(1.0),
            depth_guard_enabled: true,
            liquidity_multiple: dec!(10),
            depth_supported_qty: dec!(0.20),
            sell_depth_target_qty: dec!(2.00),
            buy_depth_target_qty: dec!(2.00),
            sell_depth_available_qty: dec!(2.00),
            buy_depth_available_qty: dec!(2.00),
            sell_depth_worst_px: dec!(62.20),
            buy_depth_worst_px: dec!(62.14),
            sell_depth_levels_used: 1,
            buy_depth_levels_used: 1,
            sell_best_px: dec!(62.20),
            buy_best_px: dec!(62.14),
            sell_best_qty: dec!(2.00),
            buy_best_qty: dec!(2.00),
            aster_book_age_ms: 10,
            lighter_book_age_ms: 20,
            decision: "would_execute".to_string(),
            gate_threshold_bps: None,
            history_sample_count: 0,
        }
    }

    fn append_test_row(path: &Path, sample: &OpportunitySample) {
        crate::taker::pnl::append_json_line(path, sample, false).unwrap();
    }

    fn settle(gate: &OpportunityGate) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while gate.snapshot.load().sequence != gate.sequence {
            assert!(std::time::Instant::now() < deadline, "history worker did not publish");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[test]
    fn rank_matches_independent_sorted_oracle_after_insertions_and_expiry() {
        let now = Utc::now();
        for p in [dec!(25), dec!(50), dec!(90), dec!(100)] {
            let mut config = cfg(EntryGateMode::Enforce);
            config.entry_percentile = p;
            let mut window = RankedWindow::new(&config);
            let values = [9, 1, 11, 4, 4, 3, 8, 10, 2, 5, 6, 7];
            let mut reference = Vec::new();
            for (i, value) in values.into_iter().enumerate() {
                let edge = Decimal::from(value);
                let timestamp = now - Duration::hours(73) + Duration::minutes(i as i64 * 10);
                window.insert(timestamp, edge);
                reference.push((timestamp, edge));
                assert_eq!(window.snapshot(0).percentile,
                    percentile(reference.iter().map(|(_, edge)| *edge), p));
            }
            window.prune(now);
            reference.retain(|(timestamp, _)| *timestamp >= now - Duration::hours(72));
            assert_eq!(window.snapshot(0).percentile,
                percentile(reference.iter().map(|(_, edge)| *edge), p));
            assert_eq!(window.snapshot(0).count, 6);
        }
    }

    #[test]
    fn nearest_rank_uses_interior_order_statistics_and_duplicate_counts() {
        // Nearest rank = the ceil(p*N/100)-th smallest. 1..=11: p50 -> 6th = 6, p90 -> 10th = 10.
        // [5,1,9,5]: p75 -> 3rd of [1,5,5,9] = 5 (9 if duplicate counts were lost), p100 -> 9.
        let now = Utc::now();
        let rank = |values: &[i64], p: Decimal| {
            let mut config = cfg(EntryGateMode::Enforce);
            config.entry_percentile = p;
            let mut window = RankedWindow::new(&config);
            for &value in values { window.insert(now, Decimal::from(value)); }
            window.snapshot(0).percentile
        };
        let interior = [11, 1, 10, 2, 9, 3, 8, 4, 7, 5, 6];
        assert_eq!(rank(&interior, dec!(50)), Some(dec!(6)));
        assert_eq!(rank(&interior, dec!(90)), Some(dec!(10)));
        assert_eq!(rank(&[5, 1, 9, 5], dec!(75)), Some(dec!(5)));
        assert_eq!(rank(&[5, 1, 9, 5], dec!(100)), Some(dec!(9)));
    }

    #[test]
    fn warmup_blocks_both_modes_and_history_writes_are_readable() {
        for mode in [EntryGateMode::Enforce, EntryGateMode::Shadow] {
            let dir = tmp_dir("warmup");
            let now = Utc::now();
            let mut gate = OpportunityGate::new(&cfg(mode), &MarketId::from("HYPE"), dir.to_str().unwrap(), now).unwrap();
            let decision = gate.evaluate(input(now, dec!(9)), dec!(6));
            assert!(!decision.allow_execution);
            assert!(decision.recorded);
            settle(&gate);
            assert_eq!(gate.loaded_samples(), 1);
            let lines = fs::read_to_string(gate.path()).unwrap();
            let persisted: OpportunitySample = serde_json::from_str(lines.trim()).unwrap();
            assert_eq!(persisted.gross_edge_bps, dec!(9));
            drop(gate);
            let _ = fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn modes_apply_known_threshold_only_after_warmup() {
        for mode in [EntryGateMode::Enforce, EntryGateMode::Shadow] {
            let dir = tmp_dir("modes");
            let now = Utc::now();
            let path = dir.join("opportunities_HYPE.jsonl");
            for edge in [dec!(6), dec!(11), dec!(12)] {
                append_test_row(&path, &row(now - Duration::minutes(1), "HYPE", edge));
            }
            let mut gate = OpportunityGate::new(&cfg(mode), &MarketId::from("HYPE"), dir.to_str().unwrap(), now).unwrap();
            let decision = gate.evaluate(sampled_input(now, dec!(7)), dec!(6));
            assert_eq!(decision.threshold_bps, Some(dec!(12)));
            assert_eq!(decision.allow_execution, mode == EntryGateMode::Shadow);
            settle(&gate);
            let second = gate.evaluate(sampled_input(now + Duration::milliseconds(250), dec!(13)), dec!(6));
            assert!(second.allow_execution);
            assert!(!second.recorded);
            drop(gate);
            let _ = fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn off_does_not_write_history() {
        let dir = tmp_dir("off");
        let now = Utc::now();
        let mut gate = OpportunityGate::new(&cfg(EntryGateMode::Off), &MarketId::from("HYPE"), dir.to_str().unwrap(), now).unwrap();
        let decision = gate.evaluate(input(now, dec!(6)), dec!(6));
        assert!(decision.allow_execution);
        assert!(!decision.recorded);
        assert!(!gate.path().exists());
    }

    #[test]
    fn timestamp_index_prunes_unsorted_history_without_rewriting_source() {
        let dir = tmp_dir("unsorted");
        let now = Utc::now();
        let path = dir.join("opportunities_HYPE.jsonl");
        append_test_row(&path, &row(now, "HYPE", dec!(6)));
        append_test_row(&path, &row(now - Duration::hours(71), "HYPE", dec!(100)));
        append_test_row(&path, &row(now, "HYPE", dec!(7)));
        let original = fs::read(&path).unwrap();
        let mut gate = OpportunityGate::new(&cfg(EntryGateMode::Enforce), &MarketId::from("HYPE"), dir.to_str().unwrap(), now).unwrap();
        // A sample two hours on prunes the 71 h old row, wherever it sits in the file.
        assert!(gate.evaluate(sampled_input(now + Duration::hours(2), dec!(5)), dec!(6)).recorded);
        settle(&gate);
        assert_eq!(gate.loaded_samples(), 3);
        assert_eq!(gate.snapshot.load().percentile, Some(dec!(7)));
        assert!(fs::read(&path).unwrap().starts_with(&original), "pruning never rewrites the history");
        drop(gate);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn sampling_never_waits_for_a_decision() {
        // 2026-10-02 dry runs: an expired window waited for a Prune handshake and recorded nothing, so
        // it stayed `history_pending` for a week. Every undecided state still samples, and that sample
        // is what lets the gate decide again.
        let dir = tmp_dir("keeps_sampling");
        let t0 = Utc::now() - Duration::hours(80);
        let path = dir.join("opportunities_HYPE.jsonl");
        append_test_row(&path, &row(t0, "HYPE", dec!(8)));
        append_test_row(&path, &row(t0 + Duration::minutes(30), "HYPE", dec!(9)));
        let start = t0 + Duration::hours(72) - Duration::seconds(1);
        let mut gate = OpportunityGate::new(&cfg(EntryGateMode::Enforce), &MarketId::from("HYPE"), dir.to_str().unwrap(), start).unwrap();
        let at = |seconds: i64| t0 + Duration::hours(72) + Duration::seconds(seconds);
        let expired = gate.evaluate(sampled_input(at(1), dec!(9)), dec!(6));
        assert_eq!((expired.decision, expired.recorded), ("history_pending", true));
        settle(&gate);
        assert_eq!(gate.evaluate(sampled_input(at(2), dec!(9)), dec!(6)).decision, "warmup_block");
        // Expired again when the second row ages out: sampled again, and deciding (and trading) again.
        assert!(gate.evaluate(sampled_input(at(31 * 60), dec!(9)), dec!(6)).recorded);
        settle(&gate);
        assert_eq!(gate.evaluate(sampled_input(at(31 * 60 + 1), dec!(9)), dec!(6)).decision, "would_execute");
        // A failed journal write: the next write that succeeds makes the gate healthy again.
        gate.healthy.store(false, Ordering::Release);
        let failed = gate.evaluate(sampled_input(at(40 * 60), dec!(9)), dec!(6));
        assert_eq!((failed.decision, failed.recorded), ("history_unavailable", true));
        settle(&gate);
        assert!(gate.healthy());
        // A clock stepped back an hour still samples.
        assert!(gate.evaluate(sampled_input(at(40 * 60) - Duration::hours(1), dec!(9)), dec!(6)).recorded);
        drop(gate);
        let _ = fs::remove_dir_all(dir);
    }
    #[tokio::test]
    #[ignore = "explicit local CPU benchmark; no network or live orders"]
    async fn benchmark_cached_hot_gate_and_exact_cold_rank_updates() {
        fn percentile_ns(values: &mut [u128], percentile: usize) -> u128 {
            values.sort_unstable();
            values[(values.len()-1)*percentile/100]
        }
        for count in [1000usize,10000,259200] {
            let now = Utc::now();
            let dir = tmp_dir("rank_bench");
            let config = cfg(EntryGateMode::Enforce);
            let mut window = RankedWindow::new(&config);
            for index in 0..count {
                window.insert(now-Duration::seconds((count-index) as i64),
                    Decimal::from((index*7919)%10000)/Decimal::from(1000));
            }
            let mut gate = OpportunityGate::new(&config,&MarketId::from("HYPE"),dir.to_str().unwrap(),now).unwrap();
            gate.snapshot.store(Arc::new(window.snapshot(0)));
            gate.last_sample_at = Some(now);
            let sample = sampled_input(now,dec!(20));
            for _ in 0..1000 { std::hint::black_box(gate.evaluate(sample,dec!(6))); }
            let mut hot = Vec::with_capacity(10000);
            for _ in 0..10000 {
                let start = std::time::Instant::now();
                std::hint::black_box(gate.evaluate(std::hint::black_box(sample),dec!(6)));
                hot.push(start.elapsed().as_nanos());
            }
            let mut cold = Vec::new();
            for index in 1..=128 {
                let timestamp = now+Duration::seconds(index);
                let start = std::time::Instant::now();
                window.insert(timestamp,Decimal::from(index%13));
                window.prune(timestamp);
                std::hint::black_box(window.snapshot(index as u64));
                cold.push(start.elapsed().as_nanos());
            }
            println!("rank_benchmark samples={} hot_p50_ns={} hot_p95_ns={} hot_p99_ns={} cold_update_p50_ns={} cold_update_p99_ns={} scope=synthetic_cpu_excludes_disk_network",
                count,percentile_ns(&mut hot,50),percentile_ns(&mut hot,95),percentile_ns(&mut hot,99),
                percentile_ns(&mut cold,50),percentile_ns(&mut cold,99));
            gate.shutdown().await.unwrap();
            let _ = fs::remove_dir_all(dir);
        }
    }

}

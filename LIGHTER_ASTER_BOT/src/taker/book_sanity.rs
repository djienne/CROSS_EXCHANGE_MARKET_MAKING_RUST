use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use arc_swap::ArcSwap;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::taker::aster::ws::AsterBookFeed;
use crate::taker::book::OrderBook;
use crate::taker::config::{BookSanityCfg, Config};
use crate::taker::connectors::rest_book;
use crate::taker::markets::MarketSpec;
use crate::taker::pnl::market_component;
use crate::taker::types::{MarketId, Side};
use crate::taker::venues::OtherLeg;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSanitySnapshot {
    pub enabled: bool,
    pub blocked: bool,
    pub blocked_until: Option<DateTime<Utc>>,
    pub failure_streak: u64,
    pub success_streak: u64,
    pub last_reason: Option<Arc<str>>,
    pub last_action: String,
    pub last_checked_at: Option<DateTime<Utc>>,
}

impl BookSanitySnapshot {
    pub fn configured(enabled: bool) -> Self {
        Self {
            enabled,
            blocked: false,
            blocked_until: None,
            failure_streak: 0,
            success_streak: 0,
            last_reason: None,
            last_action: if enabled { "starting" } else { "disabled" }.to_string(),
            last_checked_at: None,
        }
    }
}

#[derive(Clone)]
pub struct BookSanityHandle {
    inner: Arc<ArcSwap<BookSanitySnapshot>>,
}

impl BookSanityHandle {
    fn new(enabled: bool) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(BookSanitySnapshot::configured(enabled))),
        }
    }

    pub fn snapshot(&self) -> BookSanitySnapshot { self.inner.load().as_ref().clone() }

    pub fn entry_block(&self) -> Option<BookSanitySnapshot> {
        let snapshot = self.inner.load();
        (snapshot.enabled && snapshot.blocked).then(|| snapshot.as_ref().clone())
    }

    fn update_error(&self, now: DateTime<Utc>, reason: String) -> BookSanitySnapshot {
        let mut snapshot = self.snapshot();
        snapshot.last_checked_at = Some(now);
        snapshot.last_reason = Some(Arc::from(reason));
        snapshot.last_action = "check_error".to_string();
        self.inner.store(Arc::new(snapshot.clone()));
        snapshot
    }

    fn update_failure(
        &self,
        cfg: &BookSanityCfg,
        now: DateTime<Utc>,
        reason: String,
    ) -> BookSanitySnapshot {
        let mut snapshot = self.snapshot();
        let was_blocked = snapshot.blocked;
        snapshot.last_checked_at = Some(now);
        snapshot.last_reason = Some(Arc::from(reason));
        snapshot.failure_streak += 1;
        snapshot.success_streak = 0;
        if snapshot.failure_streak >= cfg.required_failures {
            snapshot.blocked = true;
            if !was_blocked {
                snapshot.blocked_until =
                    Some(now + chrono::Duration::milliseconds(cfg.block_cooldown_ms as i64));
                snapshot.last_action = "blocked".to_string();
            } else {
                snapshot.last_action = "still_blocked".to_string();
            }
        } else {
            snapshot.last_action = "failure_observed".to_string();
        }
        self.inner.store(Arc::new(snapshot.clone()));
        snapshot
    }

    fn update_success(&self, cfg: &BookSanityCfg, now: DateTime<Utc>) -> BookSanitySnapshot {
        let mut snapshot = self.snapshot();
        snapshot.last_checked_at = Some(now);
        snapshot.last_reason = None;
        snapshot.failure_streak = 0;
        snapshot.success_streak += 1;
        if snapshot.blocked {
            let cooldown_done = snapshot.blocked_until.is_none_or(|until| now >= until);
            if cooldown_done && snapshot.success_streak >= cfg.required_successes {
                snapshot.blocked = false;
                snapshot.blocked_until = None;
                snapshot.last_action = "unblocked".to_string();
            } else {
                snapshot.last_action = "blocked_clean_wait".to_string();
            }
        } else {
            snapshot.last_action = "ok".to_string();
        }
        self.inner.store(Arc::new(snapshot.clone()));
        snapshot
    }
}

#[derive(Debug, Clone, Serialize)]
struct SanityEvent {
    timestamp: DateTime<Utc>,
    market: String,
    action: String,
    blocked: bool,
    blocked_until: Option<DateTime<Utc>>,
    failure_streak: u64,
    success_streak: u64,
    reason: Option<String>,
    venues: Vec<VenueSanityReport>,
}

#[derive(Debug, Clone, Serialize)]
struct VenueSanityReport {
    venue: &'static str,
    ok: bool,
    reason: Option<String>,
    rest_bid: Option<Decimal>,
    ws_bid: Option<Decimal>,
    bid_bps: Option<Decimal>,
    rest_ask: Option<Decimal>,
    ws_ask: Option<Decimal>,
    ask_bps: Option<Decimal>,
    sell_vwap_bps: Option<Decimal>,
    buy_vwap_bps: Option<Decimal>,
    target_qty: Decimal,
    sell_rest_levels_used: Option<usize>,
    sell_ws_levels_used: Option<usize>,
    buy_rest_levels_used: Option<usize>,
    buy_ws_levels_used: Option<usize>,
}

pub fn start(
    cfg: Config,
    spec: MarketSpec,
    aster_books: Option<AsterBookFeed>,
    lighter: Arc<OtherLeg>,
    http: reqwest::Client,
) -> BookSanityHandle {
    let handle = BookSanityHandle::new(cfg.arb.book_sanity.enabled);
    let state_path = state_path(&cfg.pnl.persist_dir, &spec.market_id);
    let events_path = events_path(&cfg.pnl.persist_dir, &spec.market_id);
    if let Err(e) = persist_snapshot(&state_path, &handle.snapshot()) {
        warn!("failed to initialize book sanity state {}: {e:#}", state_path.display());
    }
    let Some(aster_books) = aster_books.filter(|_| cfg.arb.book_sanity.enabled) else {
        return handle;
    };

    info!(
        "book_sanity enabled market={} interval_ms={} top_threshold={}bps vwap_threshold={}bps required_failures={} required_successes={} block_cooldown_ms={} rest_depth_levels={} liquidity_multiple={}",
        spec.market_id,
        cfg.arb.book_sanity.interval_ms,
        cfg.arb.book_sanity.max_top_bps,
        cfg.arb.book_sanity.max_vwap_bps,
        cfg.arb.book_sanity.required_failures,
        cfg.arb.book_sanity.required_successes,
        cfg.arb.book_sanity.block_cooldown_ms,
        cfg.arb.book_sanity.rest_depth_levels,
        cfg.arb.book_sanity.liquidity_multiple
    );

    let task_handle = handle.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(cfg.arb.book_sanity.interval_ms));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let event = run_check(&cfg, &spec, &aster_books, lighter.as_ref(), &http, &task_handle)
                .await;
            // File I/O off the runtime worker: the box runs a single worker thread, so
            // a slow disk write here would stall the scan loop, WS ingest, and any
            // in-flight execution for the syscall duration (same hot/cold rule as
            // append_execution_log in arb.rs).
            let events_path_task = events_path.clone();
            let state_path_task = state_path.clone();
            let snapshot = task_handle.snapshot();
            let io = tokio::task::spawn_blocking(move || {
                let appended = append_event(&events_path_task, &event);
                let persisted = persist_snapshot(&state_path_task, &snapshot);
                (appended, persisted)
            })
            .await;
            match io {
                Ok((appended, persisted)) => {
                    if let Err(e) = appended {
                        warn!("failed to append book sanity event {}: {e:#}", events_path.display());
                    }
                    if let Err(e) = persisted {
                        warn!("failed to persist book sanity state {}: {e:#}", state_path.display());
                    }
                }
                Err(e) => warn!("book sanity persistence task failed: {e}"),
            }
        }
    });
    handle
}

async fn run_check(
    cfg: &Config,
    spec: &MarketSpec,
    aster_books: &AsterBookFeed,
    lighter: &OtherLeg,
    http: &reqwest::Client,
    handle: &BookSanityHandle,
) -> SanityEvent {
    let now = Utc::now();
    let ws_aster = aster_books.order_book();
    let ws_lighter = lighter.order_book(&spec.market_id);
    let (rest_aster, rest_lighter) = tokio::join!(
        rest_book::fetch_aster_book(
            http,
            &cfg.venues.aster_base_url,
            &spec.aster_symbol,
            cfg.arb.book_sanity.rest_depth_levels as u32,
        ),
        rest_book::fetch_lighter_book(
            http,
            &cfg.venues.lighter_base_url,
            spec.lighter_market_id,
            cfg.arb.book_sanity.rest_depth_levels as u32,
        ),
    );

    let (rest_aster, rest_lighter, ws_aster, ws_lighter) =
        match (rest_aster, rest_lighter, ws_aster, ws_lighter) {
            (Ok(ra), Ok(rl), Ok(wa), Ok(wl)) => (ra, rl, wa, wl),
            (ra, rl, wa, wl) => {
                let reason = format!(
                    "book_sanity_fetch_error aster_rest={} lighter_rest={} aster_ws={} lighter_ws={}",
                    result_state(&ra),
                    result_state(&rl),
                    result_state(&wa),
                    result_state(&wl)
                );
                let snapshot = handle.update_error(now, reason.clone());
                return SanityEvent {
                    timestamp: now,
                    market: spec.market_id.0.clone(),
                    action: snapshot.last_action,
                    blocked: snapshot.blocked,
                    blocked_until: snapshot.blocked_until,
                    failure_streak: snapshot.failure_streak,
                    success_streak: snapshot.success_streak,
                    reason: Some(reason),
                    venues: Vec::new(),
                };
            }
        };

    let aster_report = compare_venue("aster", &rest_aster, &ws_aster, cfg, spec);
    let lighter_report = compare_venue("lighter", &rest_lighter, &ws_lighter, cfg, spec);
    let failed_venues: Vec<&'static str> = [&aster_report, &lighter_report]
        .iter()
        .filter(|report| !report.ok)
        .map(|report| report.venue)
        .collect();
    let reason = if failed_venues.is_empty() {
        None
    } else {
        Some(format!(
            "book_sanity_divergence venues={} details={}",
            failed_venues.join(","),
            [&aster_report, &lighter_report]
                .iter()
                .filter_map(|report| report.reason.as_ref().map(|r| format!("{}:{r}", report.venue)))
                .collect::<Vec<_>>()
                .join(";")
        ))
    };

    let snapshot = if let Some(reason) = reason.clone() {
        handle.update_failure(&cfg.arb.book_sanity, now, reason)
    } else {
        handle.update_success(&cfg.arb.book_sanity, now)
    };

    if snapshot.last_action == "blocked" {
        if !aster_report.ok {
            warn!("book_sanity requesting Aster reconnect: {:?}", aster_report.reason);
            aster_books.request_reconnect();
        }
        if !lighter_report.ok {
            warn!("book_sanity requesting Lighter reconnect: {:?}", lighter_report.reason);
            if let Err(e) = lighter.request_order_book_reconnect(&spec.market_id) {
                warn!("book_sanity failed to request Lighter reconnect: {e:#}");
            }
        }
    }
    if snapshot.last_action == "unblocked" {
        info!("book_sanity unblocked market={}", spec.market_id);
    } else if snapshot.last_action == "blocked" {
        warn!("book_sanity blocked market={} reason={:?}", spec.market_id, snapshot.last_reason);
    } else {
        debug!("book_sanity check market={} action={}", spec.market_id, snapshot.last_action);
    }

    SanityEvent {
        timestamp: now,
        market: spec.market_id.0.clone(),
        action: snapshot.last_action,
        blocked: snapshot.blocked,
        blocked_until: snapshot.blocked_until,
        failure_streak: snapshot.failure_streak,
        success_streak: snapshot.success_streak,
        reason,
        venues: vec![aster_report, lighter_report],
    }
}

fn result_state<T, E>(result: &std::result::Result<T, E>) -> &'static str {
    if result.is_ok() {
        "ok"
    } else {
        "err"
    }
}

fn compare_venue(
    venue: &'static str,
    rest: &OrderBook,
    ws: &OrderBook,
    cfg: &Config,
    spec: &MarketSpec,
) -> VenueSanityReport {
    let target_qty = sanity_target_qty(rest, ws, cfg, spec);
    let rest_bid = rest.best_bid().map(|l| l.px);
    let ws_bid = ws.best_bid().map(|l| l.px);
    let rest_ask = rest.best_ask().map(|l| l.px);
    let ws_ask = ws.best_ask().map(|l| l.px);
    let bid_bps = rest_bid.zip(ws_bid).and_then(|(a, b)| price_bps(a, b));
    let ask_bps = rest_ask.zip(ws_ask).and_then(|(a, b)| price_bps(a, b));

    let sell_rest = rest.depth_vwap(
        Side::Sell,
        target_qty,
        cfg.arb.book_sanity.rest_depth_levels,
    );
    let sell_ws = ws.depth_vwap(
        Side::Sell,
        target_qty,
        cfg.arb.book_sanity.rest_depth_levels,
    );
    let buy_rest = rest.depth_vwap(
        Side::Buy,
        target_qty,
        cfg.arb.book_sanity.rest_depth_levels,
    );
    let buy_ws = ws.depth_vwap(
        Side::Buy,
        target_qty,
        cfg.arb.book_sanity.rest_depth_levels,
    );
    let sell_vwap_bps = sell_rest
        .zip(sell_ws)
        .and_then(|(rest_quote, ws_quote)| price_bps(rest_quote.vwap_px, ws_quote.vwap_px));
    let buy_vwap_bps = buy_rest
        .zip(buy_ws)
        .and_then(|(rest_quote, ws_quote)| price_bps(rest_quote.vwap_px, ws_quote.vwap_px));

    let mut failures = Vec::new();
    if rest_bid.is_none() || ws_bid.is_none() || rest_ask.is_none() || ws_ask.is_none() {
        failures.push("missing_top".to_string());
    }
    for (name, value, threshold) in [
        ("bid_top", bid_bps, cfg.arb.book_sanity.max_top_bps),
        ("ask_top", ask_bps, cfg.arb.book_sanity.max_top_bps),
        ("sell_vwap", sell_vwap_bps, cfg.arb.book_sanity.max_vwap_bps),
        ("buy_vwap", buy_vwap_bps, cfg.arb.book_sanity.max_vwap_bps),
    ] {
        match value {
            Some(v) if v > threshold => failures.push(format!("{name}_{}bps", v)),
            Some(_) => {}
            None if name.ends_with("vwap") => failures.push(format!("{name}_unavailable")),
            None => {}
        }
    }
    let reason = (!failures.is_empty()).then(|| failures.join(","));

    VenueSanityReport {
        venue,
        ok: reason.is_none(),
        reason,
        rest_bid,
        ws_bid,
        bid_bps,
        rest_ask,
        ws_ask,
        ask_bps,
        sell_vwap_bps,
        buy_vwap_bps,
        target_qty,
        sell_rest_levels_used: sell_rest.map(|q| q.levels_used),
        sell_ws_levels_used: sell_ws.map(|q| q.levels_used),
        buy_rest_levels_used: buy_rest.map(|q| q.levels_used),
        buy_ws_levels_used: buy_ws.map(|q| q.levels_used),
    }
}

fn sanity_target_qty(rest: &OrderBook, ws: &OrderBook, cfg: &Config, spec: &MarketSpec) -> Decimal {
    let ref_px = rest.mid().or_else(|| ws.mid()).unwrap_or(Decimal::ZERO);
    if ref_px <= Decimal::ZERO {
        return spec.step.max(spec.lighter_qty_step);
    }
    let raw = cfg.arb.desired_notional / ref_px * cfg.arb.book_sanity.liquidity_multiple;
    raw.max(spec.step.max(spec.lighter_qty_step))
}

fn price_bps(a: Decimal, b: Decimal) -> Option<Decimal> {
    let reference = (a + b) / Decimal::from(2);
    (reference > Decimal::ZERO).then_some((a - b).abs() / reference * Decimal::from(10_000))
}

pub fn events_path(persist_dir: &str, market: &MarketId) -> PathBuf {
    PathBuf::from(persist_dir).join(format!("book_sanity_{}.jsonl", market_component(market)))
}

pub fn state_path(persist_dir: &str, market: &MarketId) -> PathBuf {
    PathBuf::from(persist_dir).join(format!("book_sanity_state_{}.json", market_component(market)))
}

pub fn load_snapshot(persist_dir: &str, market: &MarketId) -> Option<BookSanitySnapshot> {
    let path = state_path(persist_dir, market);
    let file = File::open(path).ok()?;
    serde_json::from_reader(file).ok()
}

fn append_event(path: &Path, event: &SanityEvent) -> Result<()> {
    crate::taker::pnl::append_json_line(path,event,false)
}

fn persist_snapshot(path: &Path, snapshot: &BookSanitySnapshot) -> Result<()> {
    crate::taker::pnl::write_json_atomic(path,snapshot,false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn test_book(bid: Decimal, ask: Decimal) -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels(
            [(bid, dec!(10)), (bid - dec!(0.1), dec!(10))],
            [(ask, dec!(10)), (ask + dec!(0.1), dec!(10))],
            now,
            now,
        )
    }

    fn test_cfg() -> Config {
        let mut cfg = Config {
            arb: crate::taker::config::ArbCfg::default(),
            pnl: crate::taker::config::PnlCfg::default(),
            live: crate::taker::config::LiveCfg::default(),
            venues: crate::taker::config::VenueCfg::default(),
            risk: crate::taker::config::RiskCfg::default(),
            markets: Vec::new(),
        };
        cfg.arb.book_sanity.enabled = true;
        cfg.arb.book_sanity.max_top_bps = dec!(5);
        cfg.arb.book_sanity.max_vwap_bps = dec!(5);
        cfg.arb.book_sanity.liquidity_multiple = dec!(1);
        cfg.arb.desired_notional = dec!(10);
        cfg
    }

    fn test_spec() -> MarketSpec {
        MarketSpec {
            market_id: MarketId("HYPE".to_string()),
            aster_symbol: "HYPEUSDT".to_string(),
            lighter_symbol: "HYPE".to_string(),
            lighter_market_id: 24,
            lighter_price_decimals: 4,
            lighter_size_decimals: 2,
            lighter_price_tick: dec!(0.0001), hedge: Default::default(), first: Default::default(), first_market_index: 0,
            step: dec!(0.01),
            tick: dec!(0.001),
            aster_min_qty: dec!(0.01),
            aster_min_notional: dec!(5),
            lighter_qty_step: dec!(0.01),
            lighter_min_notional: dec!(10),
        }
    }

    #[test]
    fn price_bps_uses_mid_reference() {
        assert_eq!(price_bps(dec!(100), dec!(101)).unwrap().round_dp(4), dec!(99.5025));
    }

    #[test]
    fn compare_venue_flags_large_top_divergence() {
        let report = compare_venue(
            "aster",
            &test_book(dec!(100), dec!(101)),
            &test_book(dec!(99), dec!(102)),
            &test_cfg(),
            &test_spec(),
        );
        assert!(!report.ok);
        assert!(report.reason.unwrap().contains("bid_top"));
    }

    #[test]
    fn state_blocks_after_required_failures_and_unblocks_after_successes() {
        let cfg = BookSanityCfg {
            enabled: true,
            required_failures: 2,
            required_successes: 2,
            block_cooldown_ms: 1,
            ..BookSanityCfg::default()
        };
        let handle = BookSanityHandle::new(true);
        let now = Utc::now();
        assert!(!handle.update_failure(&cfg, now, "one".to_string()).blocked);
        assert!(handle.update_failure(&cfg, now, "two".to_string()).blocked);
        let later = now + chrono::Duration::milliseconds(2);
        assert!(handle.update_success(&cfg, later).blocked);
        assert!(!handle.update_success(&cfg, later).blocked);
    }
}

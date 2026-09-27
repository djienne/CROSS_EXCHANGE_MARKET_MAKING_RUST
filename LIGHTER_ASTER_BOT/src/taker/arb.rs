//! Taker-arb engine: scan both books for cross-venue edge, then fire simultaneous IOCs.
//!
//! # Hot/cold separation rules (hold these when changing the scan loop)
//!
//! The scan iteration (fetch books → f64 edge scan → gate checks) is the hot path:
//! * **Book reads are lock-free** — Aster via `ArcSwapOption` load, Lighter via the
//!   per-market cells resolved at venue construction (never the feed writer's mutex).
//! * **Sizing and edge prefilters use cached f64 math**; qualifying opportunities use
//!   Decimal for exact gate thresholds, exchange quantities and accounting.
//! * **No inline file I/O** — entry-gate samples and execution logs go to dedicated writer
//!   threads (`taker-history`, `taker-journal`) or `spawn_blocking`; the lease and the
//!   request for it are `watch` values shared with XEMM under `run`; account state arrives
//!   via a `watch` channel from the background refresher.
//! * **No inline REST on the iteration** — the lease nonce refresh runs as a spawned task
//!   with execution gated until it lands; account snapshots refresh on their own task.
//! Execution itself (sign + submit both legs concurrently, confirm, reconcile, rescue) is
//! deliberately synchronous within the loop: nothing may scan for new entries while legs
//! are unconfirmed.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Level};

use crate::taker::aster::creds::{AsterCreds, HyperliquidCreds, LighterCreds};
use crate::taker::aster::rest::{
    immediate_fill_from_order_response, order_response_is_terminal, AsterRest,
    SubmitOutcome as AsterOutcome,
};
use crate::taker::aster::sign::{AsterSigner, EvmAsterSigner};
use crate::taker::aster::ws::AsterBookFeed;
use crate::taker::book::OrderBook;
use crate::taker::config::{Config, MarketCfg};
use crate::taker::connectors::{rest_book, rest_specs};
use crate::config::HedgeVenue;
use crate::decimal::bps_to_rate;
use crate::taker::decimal::{common_qty_step, floor_to_step};
use crate::taker::entry_gate::{OpportunityGate, OpportunityGateInput};
use crate::taker::markets::MarketSpec;
use crate::taker::pnl::{format_ts, market_component, ActiveSession, ColdJournal, EconomicStatus, PnlTracker, PnlUpdate, TradeLedgerRow};
use crate::taker::types::{FeeEvidence, FeeProvenance, FillSummary, MarketId, Side};
use crate::taker::venues::hyperliquid::HyperliquidVenue;
use crate::taker::venues::lighter::{
    LighterFillConfirmation, LighterVenue, PendingFill, SubmitOutcome as LighterOutcome,
};
use crate::taker::venues::{OtherLeg, TerminalOrders};

static EXECUTION_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Direction {
    SellAsterBuyLighter,
    SellLighterBuyAster,
}

impl Direction {
    /// The ledger's name, naming the second leg's real venue.
    pub(super) fn as_str(self, hedge: HedgeVenue) -> &'static str {
        match (self, hedge) {
            (Direction::SellAsterBuyLighter, HedgeVenue::Lighter) => "SELL_ASTER_BUY_LIGHTER",
            (Direction::SellLighterBuyAster, HedgeVenue::Lighter) => "SELL_LIGHTER_BUY_ASTER",
            (Direction::SellAsterBuyLighter, HedgeVenue::Hyperliquid) => "SELL_ASTER_BUY_HYPERLIQUID",
            (Direction::SellLighterBuyAster, HedgeVenue::Hyperliquid) => "SELL_HYPERLIQUID_BUY_ASTER",
        }
    }

    pub(super) fn aster_side(self) -> Side {
        match self {
            Direction::SellAsterBuyLighter => Side::Sell,
            Direction::SellLighterBuyAster => Side::Buy,
        }
    }

    pub(super) fn lighter_side(self) -> Side {
        self.aster_side().opposite()
    }
}

#[derive(Debug, Clone)]
struct Opportunity {
    direction: Direction,
    qty: Decimal,
    qty_f64: f64,
    gross_edge_bps: Decimal,
    expected_net_margin_bps: Decimal,
    sell_px: Decimal,
    buy_px: Decimal,
    ref_px: Decimal,
    top_depth_qty: Decimal,
    depth_guard_enabled: bool,
    liquidity_multiple: Decimal,
    depth_supported_qty: Decimal,
    sell_depth_target_qty: Decimal,
    buy_depth_target_qty: Decimal,
    sell_depth_available_qty: Decimal,
    buy_depth_available_qty: Decimal,
    sell_depth_worst_px: Decimal,
    buy_depth_worst_px: Decimal,
    sell_depth_levels_used: usize,
    buy_depth_levels_used: usize,
    sell_best_px: Decimal,
    buy_best_px: Decimal,
    sell_best_qty: Decimal,
    buy_best_qty: Decimal,
    desired_qty: Decimal,
    min_qty: Decimal,
    headroom_qty: Decimal,
    margin_room_qty: Decimal,
    expected_gross_usd: Decimal,
    expected_fee_usd: Decimal,
    expected_net_usd: Decimal,
    required_margin_usd: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExposureEffect {
    Reduce,
    Increase,
    Flat,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PositionSnapshot {
    pub(super) aster_qty: Decimal,
    pub(super) lighter_qty: Decimal,
}

impl PositionSnapshot {
    pub(super) fn net_qty(self) -> Decimal {
        self.aster_qty + self.lighter_qty
    }
}

#[derive(Debug, Clone, Copy)]
struct MarginSnapshot {
    aster_available_usd: Decimal,
    lighter_available_usd: Decimal,
    /// Per-venue marked equity (balance + uPnL) when the venue payload allows computing
    /// it. Recovery loss estimation prefers equity deltas: closing a position RELEASES
    /// available margin, so an available-only delta can fully mask a realized loss.
    aster_equity_usd: Option<Decimal>,
    lighter_equity_usd: Option<Decimal>,
}

impl MarginSnapshot {
    fn total_equity_usd(self) -> Option<Decimal> {
        match (self.aster_equity_usd, self.lighter_equity_usd) {
            (Some(a), Some(l)) => Some(a + l),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PositionF64 {
    aster_qty: f64,
    lighter_qty: f64,
}

impl PositionF64 {
    fn from_snapshot(pos: PositionSnapshot) -> Option<Self> {
        Some(Self {
            aster_qty: decimal_to_f64(pos.aster_qty)?,
            lighter_qty: decimal_to_f64(pos.lighter_qty)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct MarginF64 {
    aster_available_usd: f64,
    lighter_available_usd: f64,
}

impl MarginF64 {
    fn from_snapshot(margins: MarginSnapshot) -> Option<Self> {
        Some(Self {
            aster_available_usd: decimal_to_f64(margins.aster_available_usd)?,
            lighter_available_usd: decimal_to_f64(margins.lighter_available_usd)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct MarketMathF64 {
    common_qty_step: f64,
    qty_decimal_places: u32,
    qty_tol_cap: f64,
    aster_min_qty: f64,
    aster_min_notional: f64,
    lighter_min_notional: f64,
    desired_notional: f64,
    aster_taker_fee_rate: f64,
    lighter_taker_fee_rate: f64,
    margin_rate: f64,
    max_abs_position_notional_usd: f64,
    margin_buffer_usd: f64,
    depth_liquidity_multiple: f64,
}

impl MarketMathF64 {
    fn from_config_spec(cfg: &Config, spec: &MarketSpec) -> Result<Self> {
        let common_step_dec = common_qty_step(spec.step, spec.lighter_qty_step)?;
        let common_qty_step = positive_decimal_to_f64(common_step_dec)
            .context("common quantity step is not representable as finite f64")?;
        let qty_decimal_places = common_step_dec.normalize().scale();
        Ok(Self {
            common_qty_step,
            qty_decimal_places,
            qty_tol_cap: (common_qty_step * 1e-6).max(f64::EPSILON * 16.0),
            aster_min_qty: positive_decimal_to_f64(spec.aster_min_qty)
                .context("Aster min qty is not representable as finite positive f64")?,
            aster_min_notional: positive_decimal_to_f64(spec.aster_min_notional)
                .context("Aster min notional is not representable as finite positive f64")?,
            lighter_min_notional: positive_decimal_to_f64(spec.lighter_min_notional)
                .context("Lighter min notional is not representable as finite positive f64")?,
            desired_notional: positive_decimal_to_f64(cfg.arb.desired_notional)
                .context("desired notional is not representable as finite positive f64")?,
            aster_taker_fee_rate: non_negative_decimal_to_f64(cfg.arb.aster_taker_fee_bps)
                .context("Aster taker fee is not representable as finite f64")?
                / 10_000.0,
            lighter_taker_fee_rate: non_negative_decimal_to_f64(cfg.arb.lighter_taker_fee_bps)
                .context("Lighter taker fee is not representable as finite f64")?
                / 10_000.0,
            margin_rate: non_negative_decimal_to_f64(cfg.arb.margin_bps)
                .context("margin bps is not representable as finite f64")?
                / 10_000.0,
            max_abs_position_notional_usd: positive_decimal_to_f64(
                cfg.risk.max_abs_position_notional_usd,
            )
            .context("max abs position notional is not representable as finite positive f64")?,
            margin_buffer_usd: non_negative_decimal_to_f64(cfg.risk.margin_buffer_usd)
                .context("margin buffer is not representable as finite f64")?,
            depth_liquidity_multiple: positive_decimal_to_f64(cfg.arb.depth_guard.liquidity_multiple)
                .context("depth liquidity multiple is not representable as finite positive f64")?,
        })
    }

    fn liquidity_multiple(self, depth_guard_enabled: bool) -> f64 {
        if depth_guard_enabled {
            self.depth_liquidity_multiple
        } else {
            1.0
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AccountSnapshot {
    execution_epoch: u64,
    aster_open_orders: usize,
    lighter_open_orders: usize,
    position: PositionSnapshot,
    lighter_ws_qty: Option<Decimal>,
    lighter_ws_rest_divergence_qty: Option<Decimal>,
    margins: MarginSnapshot,
    refreshed_at: tokio::time::Instant,
}

impl AccountSnapshot {
    fn is_stale(self, max_age: Duration) -> bool {
        self.refreshed_at.elapsed() >= max_age
    }

    fn age_ms(self) -> u128 {
        self.refreshed_at.elapsed().as_millis()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum ExposureFilter {
    #[default]
    Any,
    Reduce,
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub secs: Option<u64>,
    pub max_trades: Option<u64>,
    pub min_size: bool,
    pub observe_only: bool,
    pub exposure_filter: ExposureFilter,
    /// Under `run`: execution is allowed only while this holds a valid lease, which XEMM
    /// grants once its quotes are gone. `None` = full rights.
    pub lease: Option<watch::Receiver<Option<ExecutionLease>>>,
    /// Under `run`: `Some(id)` asks XEMM for the lease when an opportunity passes the entry
    /// gate; `None` hands it back after the attempt.
    pub want: Option<watch::Sender<Option<u64>>>,
    pub reduce_cooldown_ms: u64,
    /// The controller's network pause: while set, no new entry starts (in-flight executions,
    /// recovery closes and shutdown carry on).
    pub pause: Option<Arc<AtomicBool>>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            secs: None,
            max_trades: None,
            min_size: false,
            observe_only: false,
            exposure_filter: ExposureFilter::Any,
            lease: None,
            want: None,
            reduce_cooldown_ms: 5_000,
            pause: None,
        }
    }
}

/// The taker's outstanding request for the lease under `run`: handed back at the next iteration
/// after one pass under the lease, or after `WANT_TIMEOUT` without a grant.
struct Want {
    id: u64,
    at: tokio::time::Instant,
    /// A pass ran under the lease.
    granted: bool,
    /// An execution started (a trade or an auto-flatten).
    attempted: bool,
}

const WANT_TIMEOUT: Duration = Duration::from_secs(5);

/// Execution rights for one market until `expires_at`, granted by XEMM under `run`. A new
/// `lease_id` re-arms the session (nonce refresh + fresh account snapshot) before any order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLease {
    pub market: String,
    pub lease_id: String,
    pub expires_at: DateTime<Utc>,
}

impl ExecutionLease {
    fn valid_for(&self, market: &MarketId, now: DateTime<Utc>) -> bool {
        self.market == market.0 && self.expires_at > now && !self.lease_id.is_empty()
    }
}

const LEASE_REREAD_INTERVAL: Duration = Duration::from_millis(250);
const CONTROL_MAX_AGE: Duration = Duration::from_millis(500);

#[derive(Clone)]
struct ControlSnapshot {
    lease: Option<ExecutionLease>,
    observed_at: tokio::time::Instant,
    validated_epoch: Option<u64>,
}

struct LeaseCache {
    rx: watch::Receiver<ControlSnapshot>,
    execution_epoch: Arc<AtomicU64>,
    task: Option<tokio::task::JoinHandle<()>>,
}

fn begin_execution(epoch: &AtomicU64) -> u64 {
    let previous = epoch.fetch_update(Ordering::AcqRel, Ordering::Acquire,
        |value| (value % 2 == 0).then_some(value + 1)).unwrap_or_else(|value| value);
    if previous % 2 == 0 { previous + 1 } else { previous }
}

fn finish_execution(epoch: &AtomicU64) -> u64 {
    let previous = epoch.fetch_update(Ordering::AcqRel, Ordering::Acquire,
        |value| (value % 2 == 1).then_some(value + 1)).unwrap_or_else(|value| value);
    if previous % 2 == 1 { previous + 1 } else { previous }
}

fn publish_account(tx: &watch::Sender<AccountSnapshot>, snapshot: AccountSnapshot) {
    tx.send_if_modified(|current| {
        if snapshot.execution_epoch < current.execution_epoch
            || (snapshot.execution_epoch == current.execution_epoch && snapshot.refreshed_at < current.refreshed_at) {
            return false;
        }
        *current = snapshot;
        true
    });
}

fn spawn_control_refresher(
    lease_rx: Option<watch::Receiver<Option<ExecutionLease>>>, spec: MarketSpec, cfg: Config, aster: Arc<AsterRest>,
    lighter: Arc<OtherLeg>, execution_epoch: Arc<AtomicU64>,
    account_tx: watch::Sender<AccountSnapshot>, wake: Arc<Notify>, session: ActiveSession,
) -> LeaseCache {
    let (tx, rx) = watch::channel(ControlSnapshot {
        lease: None, observed_at: tokio::time::Instant::now(), validated_epoch: None,
    });
    let mut result = LeaseCache { rx, execution_epoch: execution_epoch.clone(), task: None };
    let Some(mut lease_rx) = lease_rx else { return result; };
    result.task = Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(LEASE_REREAD_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut validated_lease: Option<String> = None;
        let mut grants_open = true;
        loop {
            // A grant or revoke is seen at once; the tick re-validates expiry, account
            // freshness and open orders while a lease stands.
            tokio::select! {
                _ = tick.tick() => {}
                changed = lease_rx.changed(), if grants_open => { grants_open = changed.is_ok(); }
            }
            let observed_at = tokio::time::Instant::now();
            // A closed channel (controller gone) reads as no lease.
            let lease = (if grants_open { lease_rx.borrow_and_update().clone() } else { None })
                .filter(|lease| lease.valid_for(&spec.market_id, Utc::now()));
            let epoch = execution_epoch.load(Ordering::Acquire);
            let mut state = ControlSnapshot { lease: lease.clone(), observed_at, validated_epoch: None };
            if let Some(lease) = lease {
                if epoch % 2 == 0 {
                    let new_lease = validated_lease.as_deref() != Some(lease.lease_id.as_str());
                    if new_lease {
                        let _ = tx.send(state.clone());
                        wake.notify_one();
                        let (nonce, snapshot) = tokio::join!(lighter.refresh_nonce(),
                            refresh_account_snapshot(&spec.market_id, &aster, &lighter, &execution_epoch));
                        if nonce.is_ok() {
                            if let Ok(snapshot) = snapshot {
                                let armed = if cfg.pnl.enabled {
                                    if let Some(equity) = snapshot.margins.total_equity_usd() { session.arm_with_equity(equity).await }
                                    else { Err(anyhow::anyhow!("marked equity unavailable for session arming")) }
                                } else { session.arm().await };
                                // XEMM's cancels may not show yet: retry on the next tick.
                                let clear = snapshot.aster_open_orders == 0 && snapshot.lighter_open_orders == 0;
                                if armed.is_ok() {
                                    publish_account(&account_tx, snapshot);
                                    if clear { validated_lease = Some(lease.lease_id.clone()); }
                                }
                            }
                        }
                    }
                    let snapshot = *account_tx.borrow();
                    if validated_lease.as_deref() == Some(lease.lease_id.as_str())
                        && snapshot.execution_epoch == epoch
                        && execution_epoch.load(Ordering::Acquire) == epoch
                        && !snapshot.is_stale(Duration::from_millis(cfg.live.max_account_snapshot_age_ms as u64))
                        && snapshot.aster_open_orders == 0 && snapshot.lighter_open_orders == 0
                        && !session.unresolved() {
                        state.validated_epoch = Some(epoch);
                    }
                }
            } else { validated_lease = None; }
            if tx.send(state).is_err() { break; }
            wake.notify_one();
        }
    }));
    result
}

/// A missing book (`fetch_books` failing, e.g. while a websocket reconnects). The scan loop
/// retries every `poll_interval_ms`, so a warning per attempt floods the log for as long as
/// the outage lasts (~90 lines/s seen during a feed stall). This warns when the outage starts
/// and every `LOG_EVERY` while it lasts, and gives its length once the books are back.
#[derive(Default)]
struct BookOutage {
    /// When the fetch started failing, and when that was last logged.
    since: Option<(tokio::time::Instant, tokio::time::Instant)>,
}

impl BookOutage {
    const LOG_EVERY: Duration = Duration::from_secs(5);

    /// A failed fetch at `now`: the outage's length so far, if this failure is to be logged.
    fn failed(&mut self, now: tokio::time::Instant) -> Option<Duration> {
        match &mut self.since {
            None => {
                self.since = Some((now, now));
                Some(Duration::ZERO)
            }
            Some((start, logged)) if now - *logged >= Self::LOG_EVERY => {
                *logged = now;
                Some(now - *start)
            }
            Some(_) => None,
        }
    }

    /// A successful fetch at `now`: the length of the outage it ends, if any.
    fn recovered(&mut self, now: tokio::time::Instant) -> Option<Duration> {
        self.since.take().map(|(start, _)| now - start)
    }
}

async fn wait_for_scan(wake: &Notify, interval_ms: u64) {
    tokio::select! {
        _ = wake.notified() => {}
        _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
    }
}

fn valid_execution_lease(
    cache: &mut LeaseCache, options: &RunOptions, spec: &MarketSpec, now: DateTime<Utc>,
) -> Option<ExecutionLease> {
    if options.observe_only || options.lease.is_none() { return None; }
    let state = cache.rx.borrow();
    let epoch = cache.execution_epoch.load(Ordering::Acquire);
    if epoch % 2 != 0 || state.observed_at.elapsed() > CONTROL_MAX_AGE
        || state.validated_epoch != Some(epoch) { return None; }
    state.lease.as_ref().filter(|lease| lease.valid_for(&spec.market_id, now)).cloned()
}

/// `wanted`: under `run`, a lease counts only while this taker's own request is outstanding
/// (XEMM may take a moment to revoke it after the hand-back).
fn execution_lease_enabled(
    cache: &mut LeaseCache,
    options: &RunOptions,
    spec: &MarketSpec,
    now: DateTime<Utc>,
    wanted: bool,
) -> (bool, Option<ExecutionLease>) {
    if options.observe_only || (options.want.is_some() && !wanted) {
        return (false, None);
    }
    if options.lease.is_none() {
        return (true, None);
    }
    let lease = valid_execution_lease(cache, options, spec, now);
    (lease.is_some(), lease)
}

#[derive(Debug, Clone, Copy)]
struct SizingDecisionF64 {
    qty: f64,
    desired_qty: f64,
    min_qty: f64,
    top_depth_qty: f64,
    depth_guard_enabled: bool,
    liquidity_multiple: f64,
    depth_supported_qty: f64,
    sell_depth_target_qty: f64,
    buy_depth_target_qty: f64,
    sell_depth_available_qty: f64,
    buy_depth_available_qty: f64,
    sell_depth_worst_px: f64,
    buy_depth_worst_px: f64,
    sell_depth_levels_used: usize,
    buy_depth_levels_used: usize,
    sell_best_px: f64,
    buy_best_px: f64,
    sell_best_qty: f64,
    buy_best_qty: f64,
    headroom_qty: f64,
    margin_room_qty: f64,
}

#[inline]
fn f64_to_dec(v: f64) -> Decimal {
    Decimal::from_f64_retain(v)
        .unwrap_or(Decimal::ZERO)
        .round_dp(12)
}

/// Skip the Decimal opportunity build only when the f64 edge is CLEARLY below the
/// exact threshold. 1e-9 bps dwarfs every error term between the f64 edge and its
/// round_dp(12) Decimal (<= 5e-13 bps) plus the Decimal->f64 threshold conversion
/// (<= ~2e-12 bps for realistic thresholds), while being economically zero; anything
/// closer than this band still takes the exact Decimal comparison, so borderline
/// accept/reject behavior is unchanged.
const EDGE_PREFILTER_EPS_BPS: f64 = 1e-9;

fn qty_f64_to_dec(math: &MarketMathF64, v: f64) -> Decimal {
    Decimal::from_f64_retain(round_qty_to_scale_f64(v, math))
        .unwrap_or(Decimal::ZERO)
        .round_dp(math.qty_decimal_places)
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    let out = value.to_f64()?;
    out.is_finite().then_some(out)
}

fn positive_decimal_to_f64(value: Decimal) -> Option<f64> {
    let out = decimal_to_f64(value)?;
    (out > 0.0).then_some(out)
}

fn non_negative_decimal_to_f64(value: Decimal) -> Option<f64> {
    let out = decimal_to_f64(value)?;
    (out >= 0.0).then_some(out)
}


fn unit_snap_tol(units: f64) -> f64 {
    (units.abs() * f64::EPSILON * 64.0).max(1e-12)
}

fn snap_step_units(units: f64) -> f64 {
    let nearest = units.round();
    if (units - nearest).abs() <= unit_snap_tol(units) {
        nearest
    } else {
        units
    }
}

fn round_qty_to_scale_f64(qty: f64, math: &MarketMathF64) -> f64 {
    if !qty.is_finite() || qty <= 0.0 {
        return 0.0;
    }
    let scale = 10f64.powi(math.qty_decimal_places as i32);
    if !scale.is_finite() || scale <= 0.0 {
        return qty;
    }
    (qty * scale).round() / scale
}

fn qty_cmp_tol(math: &MarketMathF64, a: f64, b: f64) -> f64 {
    let raw = a.abs().max(b.abs()) * f64::EPSILON * 64.0;
    raw.max(f64::EPSILON).min(math.qty_tol_cap)
}

fn qty_le(a: f64, b: f64, math: &MarketMathF64) -> bool {
    a <= b + qty_cmp_tol(math, a, b)
}

fn qty_ge(a: f64, b: f64, math: &MarketMathF64) -> bool {
    a + qty_cmp_tol(math, a, b) >= b
}

fn qty_gt(a: f64, b: f64, math: &MarketMathF64) -> bool {
    a > b + qty_cmp_tol(math, a, b)
}

#[derive(Debug, Clone)]
struct TradeReport {
    execution_id: String,
    lighter_fee_evidence: Vec<FeeEvidence>,
    position: PositionSnapshot,
    lighter_ws_qty: Option<Decimal>,
    lighter_ws_rest_divergence_qty: Option<Decimal>,
    margin_before: MarginSnapshot,
    margin_after: MarginSnapshot,
    economics: ActualEconomics,
    aster_order_id: i64,
    lighter_client_order_index: i64,
    hedge_retry_action_taken: bool,
}

impl TradeReport {
    fn available_margin_delta_usd(&self) -> Decimal {
        (self.margin_after.aster_available_usd + self.margin_after.lighter_available_usd)
            - (self.margin_before.aster_available_usd + self.margin_before.lighter_available_usd)
    }
}

#[derive(Debug, Clone, Copy)]
struct ActualEconomics {
    aster_fill: FillSummary,
    lighter_fill: FillSummary,
    gross_usd: Decimal,
    fees_usd: Decimal,
    net_usd: Decimal,
    net_bps: Decimal,
    fill_qty_mismatch: Decimal,
    /// Signed unmatched leg qty (sell − buy): NOT PnL, open exposure. Booked separately so
    /// the loss breaker sees real matched PnL instead of up to ~$3/trade of phantom profit.
    residual_qty: Decimal,
    /// Absolute residual valued at the opportunity reference price.
    residual_notional_usd: Decimal,
}

#[derive(Debug, Clone)]
struct RecoveryReport {
    execution_id: String,
    action_taken: bool,
    position: PositionSnapshot,
    lighter_ws_qty: Option<Decimal>,
    margin_after: MarginSnapshot,
    estimated_loss_usdc: Decimal,
    aster_open_orders: usize,
    lighter_open_orders: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HedgeRetryVenue {
    Aster,
    Lighter,
}

impl HedgeRetryVenue {
    fn as_str(self) -> &'static str {
        match self {
            HedgeRetryVenue::Aster => "Aster",
            HedgeRetryVenue::Lighter => "Lighter",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HedgeRetryPlan {
    venue: HedgeRetryVenue,
    side: Side,
    qty: Decimal,
    price_bound: Decimal,
}

#[derive(Debug, Clone)]
struct HedgeRetryAttempt {
    fee_evidence: Vec<FeeEvidence>,
    identity: serde_json::Value,
    attempt: u64,
    venue: HedgeRetryVenue,
    side: Side,
    qty: Decimal,
    price_bound: Decimal,
    reduce_only: bool,
    submit_result: Option<String>,
    fill: Option<FillSummary>,
    fill_status: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct HedgeRetryReport {
    attempted: bool,
    succeeded: bool,
    slippage_bps: Decimal,
    attempts: Vec<HedgeRetryAttempt>,
    final_position: Option<PositionSnapshot>,
    net_notional: Option<Decimal>,
    aster_open_orders: Option<usize>,
    lighter_open_orders: Option<usize>,
    error: Option<String>,
}

impl HedgeRetryReport {
    fn empty(slippage_bps: Decimal, error: Option<String>) -> Self {
        Self {
            attempted: false,
            succeeded: false,
            slippage_bps,
            attempts: Vec::new(),
            final_position: None,
            net_notional: None,
            aster_open_orders: None,
            lighter_open_orders: None,
            error,
        }
    }
}

#[derive(Debug, Default)]
struct RecoveredFailureTracker {
    events: VecDeque<(tokio::time::Instant, Decimal)>,
}

impl RecoveredFailureTracker {
    fn record(&mut self, loss_usdc: Decimal, cfg: &Config) -> Option<String> {
        let now = tokio::time::Instant::now();
        self.events
            .retain(|(ts, _)| now.duration_since(*ts) <= Duration::from_secs(3600));
        self.events.push_back((now, loss_usdc.max(Decimal::ZERO)));
        let count = self.events.len() as u64;
        let loss_sum = self
            .events
            .iter()
            .fold(Decimal::ZERO, |acc, (_, loss)| acc + *loss);
        if count > cfg.arb.max_recovered_failures_per_hour {
            return Some(format!(
                "recovered failure count {count} exceeds hourly limit {}",
                cfg.arb.max_recovered_failures_per_hour
            ));
        }
        if loss_sum > cfg.arb.max_recovered_loss_usdc_per_hour {
            return Some(format!(
                "recovered failure loss ${loss_sum} exceeds hourly limit ${}",
                cfg.arb.max_recovered_loss_usdc_per_hour
            ));
        }
        None
    }

    #[cfg(test)]
    fn event_count(&self) -> usize {
        self.events.len()
    }
}

#[derive(Debug, thiserror::Error)]
enum ExecutionError {
    #[error("{details}")]
    Skipped { details: String },
    #[error("{details}")]
    Unreconciled { details: String },
    #[error("{details}")]
    AccountingUnavailable { details: String },
    #[error("{details}")]
    OutcomeUnresolved { details: String },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl ExecutionError {
    fn needs_recovery(&self) -> bool {
        matches!(
            self,
            ExecutionError::Unreconciled { .. }
                | ExecutionError::AccountingUnavailable { .. }
        )
    }

    fn is_skip(&self) -> bool {
        matches!(self, ExecutionError::Skipped { .. })
    }
}

/// Run the taker engine until `stop` is cancelled, the duration/trade limit is reached or a
/// safety stop fires. Stopping never interrupts an execution: the loop checks `stop` only
/// between iterations, then verifies flat orders/positions before clearing the session.
pub async fn run(mut cfg: Config, markets: Vec<MarketCfg>, options: RunOptions, stop: CancellationToken) -> Result<()> {
    let hedge_venue = markets.first().map(|m| m.hedge_venue).unwrap_or_default();
    if hedge_venue == HedgeVenue::Hyperliquid {
        // The second leg pays its own venue's fee. Hyperliquid pushes whole books, so there is
        // no incremental state for the book sanity check to catch.
        cfg.arb.lighter_taker_fee_bps = cfg.arb.hyperliquid_taker_fee_bps
            .context("a market hedged on Hyperliquid needs [taker.arb] hyperliquid_taker_fee_bps")?;
        cfg.arb.book_sanity.enabled = false;
    }
    if !cfg.live.enabled || !cfg.live.mode.eq_ignore_ascii_case("live") {
        bail!("refusing to run: set [live] enabled = true and mode = \"live\"");
    }
    if markets.len() != 1 {
        bail!(
            "live taker arb is single-market only; selected {} markets",
            markets.len()
        );
    }

    let specs = rest_specs::build_market_specs(
        &markets,
        &cfg.venues.aster_base_url,
        &cfg.venues.lighter_base_url,
        Some(&cfg.venues.hyperliquid_base_url),
    )
    .await?;
    let spec = specs.first().context("no resolved market spec")?.clone();
    let math = MarketMathF64::from_config_spec(&cfg, &spec)?;
    info!(
        "resolved market {}: Aster {} step={} min_notional={} | Lighter {} market_id={} qty_step={} min_notional={} common_qty_step={}",
        spec.market_id,
        spec.aster_symbol,
        spec.step,
        spec.aster_min_notional,
        spec.lighter_symbol,
        spec.lighter_market_id,
        spec.lighter_qty_step,
        spec.lighter_min_notional,
        math.common_qty_step
    );

    let bot_start = Utc::now();
    let mut pnl = if cfg.pnl.enabled {
        let tracker = PnlTracker::new(&cfg.pnl, &spec.market_id, bot_start)?;
        let snapshot = tracker.snapshot();
        info!(
            "pnl tracker enabled: market={} since={} loaded_trades={} cumulative_pnl=${} max_loss=${} ledger={} breaker={}",
            spec.market_id,
            format_ts(snapshot.since),
            snapshot.loaded_trades,
            snapshot.cumulative_pnl_usdc,
            snapshot.max_loss_usdc,
            snapshot.ledger_path.display(),
            snapshot.breaker_path.display()
        );
        // A dry run's breaker lives in runs/dry-run: without the flag the reset hits live's.
        let reset = if cfg.venues.dry_run { "reset-circuit-breaker --dry-run" } else { "reset-circuit-breaker" };
        if let Some(breaker) = tracker.active_breaker()? {
            bail!(
                "circuit breaker active: market={} since={} cumulative_pnl=${} max_loss=${}; reset with {reset}",
                breaker.market,
                format_ts(breaker.pnl_since),
                breaker.cumulative_pnl_usdc,
                breaker.max_loss_usdc
            );
        }
        if let Some(breaker) = tracker.trip_from_loaded_pnl_if_needed()? {
            bail!(
                "circuit breaker triggered from persisted PnL: market={} since={} cumulative_pnl=${} max_loss=${}; reset with {reset} after changing pnl.since or limit",
                breaker.market,
                format_ts(breaker.pnl_since),
                breaker.cumulative_pnl_usdc,
                breaker.max_loss_usdc
            );
        }
        Some(tracker)
    } else {
        info!("pnl tracker disabled");
        None
    };
    let mut entry_gate = OpportunityGate::new(
        &cfg.arb.entry_gate,
        &spec.market_id,
        &cfg.pnl.persist_dir,
        bot_start,
    )?;
    if cfg.arb.entry_gate.enabled {
        info!(
            "entry gate configured: market={} mode={} loaded_samples={} history_window_hours={} min_history_samples={} entry_percentile={} min_extra_bps={} sample_interval_ms={} history={}",
            spec.market_id,
            cfg.arb.entry_gate.mode.as_str(),
            entry_gate.loaded_samples(),
            cfg.arb.entry_gate.history_window_hours,
            cfg.arb.entry_gate.min_history_samples,
            cfg.arb.entry_gate.entry_percentile,
            cfg.arb.entry_gate.min_extra_bps,
            cfg.arb.entry_gate.sample_interval_ms,
            entry_gate.path().display()
        );
    } else {
        info!("entry gate disabled");
    }

    let dry = cfg.venues.dry_run;
    let acreds = if dry { AsterCreds::dry_run() } else { AsterCreds::from_env()? };
    let aster_account_id = acreds.user.clone();
    let aster_signer: Arc<dyn AsterSigner> =
        Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
    let aster = Arc::new(AsterRest::new(
        cfg.venues.aster_base_url.clone(),
        aster_signer,
        &specs,
    )?);
    let lighter = Arc::new(match hedge_venue {
        HedgeVenue::Lighter => {
            let lcreds = if dry { LighterCreds::dry_run() } else { LighterCreds::from_env()? };
            OtherLeg::Lighter(LighterVenue::new(&cfg.venues.lighter_base_url, Path::new(&cfg.venues.signers_dir), lcreds, &specs).await?)
        }
        HedgeVenue::Hyperliquid => {
            let hcreds = if dry { HyperliquidCreds::dry_run() } else { HyperliquidCreds::from_env()? };
            OtherLeg::Hyperliquid(HyperliquidVenue::new(&cfg.venues.hyperliquid_base_url, hcreds, &spec).await?)
        }
    });
    let session = ActiveSession::new(crate::taker::pnl::session_path(&cfg.pnl, &spec.market_id), serde_json::json!({
        "schema_version": 2, "session_id": next_execution_id(), "process_id": std::process::id(), "started_at": Utc::now(), "status": "active",
        "market": spec.market_id.to_string(), "aster_account": aster_account_id,
        "hedge_venue": hedge_venue, "lighter_market_index": spec.lighter_market_id,
        "lighter_account_index": match &*lighter { OtherLeg::Lighter(l) => Some(l.account_index()), OtherLeg::Hyperliquid(_) => None },
    }));
    let execution_journal = ColdJournal::new(execution_log_path(&cfg, &spec.market_id), true)?;
    let scan_wake = Arc::new(Notify::new());
    let aster_books =
        AsterBookFeed::spawn_from_rest_base(&cfg.venues.aster_base_url, &spec.aster_symbol);
    aster_books.set_scan_notify(scan_wake.clone());
    lighter.set_scan_notify(scan_wake.clone());

    aster_books.wait_ready(Duration::from_secs(20)).await?;
    info!("Aster websocket book ready: market={}", spec.market_id);
    lighter
        .wait_ready(&spec.market_id, Duration::from_secs(20))
        .await?;
    info!("{hedge_venue:?} websocket state ready: market={}", spec.market_id);
    let standby_until_lease = options.lease.is_some();
    ensure_clean_start(
        &cfg,
        &spec,
        &aster_books,
        &aster,
        &lighter,
        options.observe_only || standby_until_lease,
    )
    .await?;
    let account_snapshot_max_age =
        Duration::from_millis(cfg.live.max_account_snapshot_age_ms as u64);
    let execution_epoch = Arc::new(AtomicU64::new(0));
    let mut account = refresh_account_snapshot(&spec.market_id, &aster, &lighter, &execution_epoch).await?;
    if options.lease.is_none() && !options.observe_only {
        if cfg.pnl.enabled {
            session.arm_with_equity(account.margins.total_equity_usd()
                .context("marked equity unavailable for session arming")?).await?;
        } else { session.arm().await?; }
    }
    let (account_tx, mut account_rx) = watch::channel(account);
    let account_refresh_now = Arc::new(Notify::new());
    let _account_refresh_task = spawn_account_snapshot_refresher(
        &cfg,
        spec.market_id.clone(),
        aster.clone(),
        lighter.clone(),
        account_tx.clone(),
        execution_epoch.clone(),
        account_refresh_now.clone(),
    );

    let http = rest_book::client()?;
    let book_sanity = crate::taker::book_sanity::start(
        cfg.clone(),
        spec.clone(),
        aster_books.clone(),
        lighter.clone(),
        http.clone(),
    );
    let deadline = options
        .secs
        .map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    let mut cooldown_until =
        tokio::time::Instant::now() + Duration::from_millis(cfg.arb.startup_warmup_ms);
    let mut trades_executed = 0u64;
    let mut want: Option<Want> = None;
    let mut next_want_id = 0u64;
    let mut last_stale_account_log_at: Option<tokio::time::Instant> = None;
    let mut last_book_sanity_block_log_at: Option<tokio::time::Instant> = None;
    let mut book_outage = BookOutage::default();
    // Gated/standby decisions can repeat every 10ms scan while an edge stays visible;
    // log only on decision change or every 5s (recorded samples stay unthrottled).
    let mut last_gate_log: Option<(&'static str, tokio::time::Instant)> = None;
    let mut last_standby_log_at: Option<tokio::time::Instant> = None;
    let mut recovered_failures = RecoveredFailureTracker::default();
    // Consecutive main-loop position-mismatch detections (see the guard below): after
    // `risk.mismatch_flatten_after_checks` in a row the residual is actively flattened
    // instead of pausing forever on a naked position.
    let mut mismatch_consecutive: u32 = 0;
    // Change-detection state: books that fed the last FULL evaluation + the account
    // generation it saw. Stored only after the mismatch/divergence guards pass, so a
    // mismatched pair is re-entered every iteration.
    const FULL_EVAL_FLOOR: Duration = Duration::from_millis(250);
    let mut last_sized: Option<(Arc<OrderBook>, Arc<OrderBook>, u64)> = None;
    let mut account_gen: u64 = 0;
    let mut last_full_eval = tokio::time::Instant::now();
    let mut last_flatten_denied_log_at: Option<tokio::time::Instant> = None;
    let mut last_fill_stats_log = tokio::time::Instant::now();
    let mut lease_cache = spawn_control_refresher(options.lease.clone(), spec.clone(), cfg.clone(),
        aster.clone(), lighter.clone(), execution_epoch.clone(), account_tx.clone(), scan_wake.clone(), session.clone());
    let history_changed = entry_gate.changed();
    // Set when the cooldown select below woke on account_rx.changed(): changed()
    // consumes the watch seen-marker, so the freshness check later in the iteration
    // must OR this in or fresh-snapshot mismatch counting breaks. Persists across
    // `continue`s (e.g. a book-fetch failure) until a freshness check consumes it.
    let mut woke_for_account_update = false;

    info!(
        "taker arb running: market={} required_gross_edge={}bps desired_notional=${} min_size={} max_trades={:?} observe_only={} exposure_filter={:?} lease_standby={} startup_warmup_ms={} cooldown_ms={} reduce_cooldown_ms={} fees_bps=aster:{} lighter:{} margin_bps={} slippage_bps=aster:{} lighter:{} depth_guard_enabled={} liquidity_multiple={} depth_max_levels={} rescue_breaker=count_per_hour:{} loss_per_hour:${} risk_max_abs_notional=${} risk_mismatch=${} margin_buffer=${}",
        spec.market_id,
        cfg.arb.required_gross_edge_bps(),
        cfg.arb.desired_notional,
        options.min_size,
        options.max_trades,
        options.observe_only,
        options.exposure_filter,
        options.lease.is_some(),
        cfg.arb.startup_warmup_ms,
        cfg.arb.cooldown_ms,
        options.reduce_cooldown_ms,
        cfg.arb.aster_taker_fee_bps,
        cfg.arb.lighter_taker_fee_bps,
        cfg.arb.margin_bps,
        cfg.arb.max_aster_slippage_bps,
        cfg.arb.max_lighter_slippage_bps,
        cfg.arb.depth_guard.enabled,
        cfg.arb.depth_guard.liquidity_multiple,
        cfg.arb.depth_guard.max_levels,
        cfg.arb.max_recovered_failures_per_hour,
        cfg.arb.max_recovered_loss_usdc_per_hour,
        cfg.risk.max_abs_position_notional_usd,
        cfg.risk.max_position_mismatch_usd,
        cfg.risk.margin_buffer_usd
    );
    if cfg.arb.startup_warmup_ms > 0 {
        info!(
            "startup warmup active: waiting {}ms before first scan",
            cfg.arb.startup_warmup_ms
        );
    }

    // `stop` is a level, not an edge: a stop requested during any of the loop's plain
    // poll-interval sleeps (or during an execution) is seen at the next select. (A fresh
    // signal::ctrl_c() per iteration once swallowed a SIGINT landing in those sleeps —
    // observed live 2026-07-02.)
    let run_result: Result<()> = async {
    loop {
        if let (Some(tx), Some(w)) = (&options.want, &want) {
            // After one pass under the lease (an execution, or a skip when the edge is gone), or
            // without a grant in time.
            if w.granted || w.attempted || w.at.elapsed() >= WANT_TIMEOUT {
                let outcome = if w.attempted { "executed" } else if w.granted { "skipped" } else { "no_grant" };
                info!("handing the execution rights back want_id={} outcome={outcome} held_ms={}", w.id, w.at.elapsed().as_millis());
                if !w.attempted {
                    cooldown_until = tokio::time::Instant::now() + Duration::from_millis(cfg.arb.cooldown_ms.max(1000));
                }
                tx.send_replace(None);
                want = None;
            }
        }
        if let Some(deadline) = deadline {
            if tokio::time::Instant::now() >= deadline {
                info!("duration elapsed; stopping");
                break;
            }
        }
        tokio::select! {
            // biased: cooldown_until is usually already elapsed, and an unbiased
            // select could keep picking the ready timer over a pending stop.
            biased;
            _ = stop.cancelled() => {
                info!("stop requested; stopping");
                break;
            }
            _ = tokio::time::sleep_until(cooldown_until) => {}
            _ = scan_wake.notified() => {}
            _ = history_changed.notified() => {}
            // A fresh account snapshot wakes the loop DURING cooldown so the mismatch
            // guard reacts within seconds instead of after a long trade cooldown; the
            // cooldown gate before the sizing path keeps entries shut on early wakes.
            changed = account_rx.changed() => {
                match changed {
                    Ok(()) => woke_for_account_update = true,
                    // Unreachable while the refresher task holds the sender; bounded
                    // sleep so a closed channel cannot spin this loop hot.
                    Err(_) => {
                        wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
                    }
                }
            }
        }
        if let Some(deadline) = deadline {
            if tokio::time::Instant::now() >= deadline {
                info!("duration elapsed; stopping");
                break;
            }
        }

        let (aster_book, lighter_book) = match fetch_books(&spec, &aster_books, &lighter) {
            Ok(v) => {
                if let Some(lasted) = book_outage.recovered(tokio::time::Instant::now()) {
                    info!("books back after {:.1}s without one", lasted.as_secs_f64());
                }
                v
            }
            Err(e) => {
                if let Some(lasted) = book_outage.failed(tokio::time::Instant::now()) {
                    warn!(
                        "book fetch failed ({:.1}s so far, repeated every {}s): {e:#}",
                        lasted.as_secs_f64(),
                        BookOutage::LOG_EVERY.as_secs()
                    );
                }
                wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
                continue;
            }
        };
        let now = Utc::now();
        if !book_ok(&aster_book, now, cfg.arb.max_book_staleness_ms)
            || !book_ok(&lighter_book, now, cfg.arb.max_book_staleness_ms)
        {
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }

        // "Fresh" = a NEW snapshot (a new REST read) arrived since the last iteration —
        // either still pending on the watch, or already consumed by the cooldown select's
        // changed() wake. The mismatch counter below only counts fresh generations — 4
        // reads of the same 15s snapshot are one observation, not four "consecutive
        // checks".
        let account_fresh = account_rx.has_changed().unwrap_or(false) || woke_for_account_update;
        if account_fresh {
            account_gen = account_gen.wrapping_add(1);
        }
        woke_for_account_update = false;
        let candidate_account = *account_rx.borrow_and_update();
        if candidate_account.execution_epoch == execution_epoch.load(Ordering::Acquire)
            && candidate_account.execution_epoch % 2 == 0 {
            account = candidate_account;
        }
        if account.execution_epoch != execution_epoch.load(Ordering::Acquire) {
            account_refresh_now.notify_one();
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }
        if account.is_stale(account_snapshot_max_age) {
            let log_now = match last_stale_account_log_at {
                Some(ts) => ts.elapsed() >= Duration::from_secs(5),
                None => true,
            };
            if log_now {
                warn!(
                    "cold account snapshot stale; skipping scan iteration: age_ms={} max_age_ms={}",
                    account.age_ms(),
                    account_snapshot_max_age.as_millis()
                );
                last_stale_account_log_at = Some(tokio::time::Instant::now());
            }
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }
        last_stale_account_log_at = None;

        if scan_inputs_unchanged(
            &last_sized,
            &aster_book,
            &lighter_book,
            account_gen,
            last_full_eval.elapsed(),
            FULL_EVAL_FLOOR,
        ) {
            // Nothing an evaluation depends on has changed since the last full pass —
            // skip the sizing body this iteration (the staleness gates above already ran).
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }

        // Throttled fill-matching health log (cold: one Instant compare per iteration).
        if last_fill_stats_log.elapsed() >= Duration::from_secs(60) {
            last_fill_stats_log = tokio::time::Instant::now();
            if let OtherLeg::Lighter(l) = &*lighter {
                let s = l.fill_tracker_stats();
                info!(
                    "lighter fill-tracker stats: registered={} trades_seen={} matched={} unmatched={} duplicates={} timeouts={}",
                    s.registered, s.trades_seen, s.matched_trades, s.unmatched_trades, s.duplicate_trades, s.timeouts
                );
            }
        }

        let pos = account.position;
        let Some(mismatch_notional) = net_mismatch_notional(pos, &aster_book, &lighter_book) else {
            warn!(
                "position mismatch check unavailable due to f64 conversion failure: aster={} lighter={}",
                pos.aster_qty, pos.lighter_qty
            );
            tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms)).await;
            continue;
        };
        if mismatch_notional > cfg.risk.max_position_mismatch_usd {
            if account_fresh {
                mismatch_consecutive = mismatch_consecutive.saturating_add(1);
                warn!(
                    "position mismatch too large: aster={} lighter={} net={}; pausing ({} consecutive of {} needed)",
                    pos.aster_qty,
                    pos.lighter_qty,
                    pos.net_qty(),
                    mismatch_consecutive,
                    cfg.risk.mismatch_flatten_after_checks
                );
            }
            // Ask the refresher for an early re-read so the next check sees a new generation
            // sooner than the 15 s refresh (it spaces forced re-reads 5 s apart).
            account_refresh_now.notify_one();
            // A residual that persists across several reconciles is a real naked position
            // (e.g. an external fill, or an Unknown-outcome leg that landed): act on it
            // with the same reduce-only emergency-bound machinery as the rescue path,
            // instead of riding market moves until a human notices.
            // Flattening places LIVE reduce-only orders. Only an instance holding
            // execution rights may act: under `run` XEMM shares the accounts, and its
            // transiently-unhedged legs must never be raced by a second flattener.
            let (execution_allowed, _) =
                execution_lease_enabled(&mut lease_cache, &options, &spec, now, want.is_some());
            if cfg.risk.auto_flatten_on_mismatch
                && mismatch_consecutive >= cfg.risk.mismatch_flatten_after_checks
                && !execution_allowed
            {
                let log_now = match last_flatten_denied_log_at {
                    Some(ts) => ts.elapsed() >= Duration::from_secs(5),
                    None => true,
                };
                if log_now {
                    error!(
                        "position mismatch persists ({mismatch_consecutive} checks, ${mismatch_notional}) but this instance holds no execution rights; NOT auto-flattening"
                    );
                    last_flatten_denied_log_at = Some(tokio::time::Instant::now());
                }
            }
            if cfg.risk.auto_flatten_on_mismatch
                && mismatch_consecutive >= cfg.risk.mismatch_flatten_after_checks
                && execution_allowed
            {
                error!(
                    "position mismatch persisted {mismatch_consecutive} checks (${mismatch_notional}); auto-flattening residual reduce-only"
                );
                begin_execution(&execution_epoch);
                if let Some(want) = want.as_mut() { want.attempted = true; }
                let recovery_result =
                    recover_if_needed(&cfg, &spec, &aster, &lighter, &http, account.margins, &session, &execution_journal).await;
                finish_execution(&execution_epoch);
                match recovery_result {
                    Ok(recovery) => {
                        mismatch_consecutive = 0;
                        warn!(
                            "mismatch auto-flatten complete action_taken={} estimated_loss=${} final_aster={} final_lighter={}",
                            recovery.action_taken,
                            recovery.estimated_loss_usdc,
                            recovery.position.aster_qty,
                            recovery.position.lighter_qty
                        );
                        if recovery.action_taken {
                            let hourly_breaker = recovered_failures.record(recovery.estimated_loss_usdc, &cfg);
                            record_recovery_loss(&mut pnl, &spec, &recovery, &session, hourly_breaker).await?;
                        }
                        account = AccountSnapshot {
                            execution_epoch: finish_execution(&execution_epoch),
                            aster_open_orders: 0,
                            lighter_open_orders: 0,
                            position: recovery.position,
                            lighter_ws_qty: recovery.lighter_ws_qty,
                            lighter_ws_rest_divergence_qty: recovery
                                .lighter_ws_qty
                                .map(|ws| (ws - recovery.position.lighter_qty).abs()),
                            margins: recovery.margin_after,
                            refreshed_at: tokio::time::Instant::now(),
                        };
                        publish_account(&account_tx, account);
                    }
                    Err(e) => {
                        if session.unresolved() { bail!("mismatch recovery unresolved; no further order may be submitted: {e:#}"); }
                        error!("mismatch auto-flatten failed: {e:#}");
                        // Repeated failure to even inspect/flatten means the bot cannot
                        // guarantee its own safety: return an error so the controller (or a
                        // standalone `taker run`'s exit code) halts loudly instead of pausing
                        // on a naked position forever.
                        if mismatch_consecutive
                            >= cfg.risk.mismatch_flatten_after_checks.saturating_mul(3)
                        {
                            bail!(
                                "position mismatch persisted {mismatch_consecutive} checks and auto-flatten kept failing: {e:#}"
                            );
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms)).await;
            continue;
        }
        mismatch_consecutive = 0;
        if cfg.pnl.enabled {
            if let (Some(baseline),Some(current)) = (session.baseline_equity(),account.margins.total_equity_usd()) {
                if let Some(loss) = session_marked_loss(baseline,current,cfg.pnl.max_loss_usdc) {
                    execution_journal.try_append(ExecutionRecord::Row(serde_json::json!({"schema_version":2,"economic_status":"confirmed",
                        "timestamp":Utc::now(),"session_id":session.id(),"market":spec.market_id.to_string(),
                        "outcome":"session_marked_loss","pnl_metric":"session_marked_equity_loss",
                        "baseline_equity_usd":baseline,"current_equity_usd":current,"loss_usd":loss,
                        "max_loss_usdc":cfg.pnl.max_loss_usdc})))?;
                    bail!("session marked-equity loss ${loss} reached ${}; new execution stopped",cfg.pnl.max_loss_usdc);
                }
            }
        }
        let mark = aster_book
            .mid()
            .or_else(|| lighter_book.mid())
            .unwrap_or(Decimal::ZERO);
        if let (Some(ws_qty), Some(divergence_qty)) = (
            account.lighter_ws_qty,
            account.lighter_ws_rest_divergence_qty,
        ) {
            let divergence_notional = divergence_qty * mark;
            if divergence_notional > cfg.risk.max_position_mismatch_usd {
                warn!(
                    "Lighter REST/WS position divergence too large; pausing new entries: rest_qty={} ws_qty={} divergence_qty={} divergence_notional=${}",
                    pos.lighter_qty,
                    ws_qty,
                    divergence_qty,
                    divergence_notional
                );
                tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms)).await;
                continue;
            }
        }
        // Entries stay gated by the cooldown: an early wake (fresh account snapshot
        // during cooldown) may only do the mismatch/divergence work above, never size
        // or enter. The select at the top blocks until the cooldown elapses.
        if tokio::time::Instant::now() < cooldown_until {
            continue;
        }
        last_sized = Some((aster_book.clone(), lighter_book.clone(), account_gen));
        last_full_eval = tokio::time::Instant::now();
        let margins = account.margins;
        let Some(pos_f) = PositionF64::from_snapshot(pos) else {
            warn!(
                "position f64 conversion failed; skipping scan iteration: aster={} lighter={}",
                pos.aster_qty, pos.lighter_qty
            );
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        };
        let Some(margins_f) = MarginF64::from_snapshot(margins) else {
            warn!(
                "margin f64 conversion failed; skipping scan iteration: aster_available={} lighter_available={}",
                margins.aster_available_usd, margins.lighter_available_usd
            );
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        };
        if tracing::enabled!(Level::DEBUG) {
            log_scan_state(&cfg, &spec, &aster_book, &lighter_book, pos, margins);
        }
        let (execution_enabled, _valid_lease) =
            execution_lease_enabled(&mut lease_cache, &options, &spec, now, want.is_some());
        if let Some(want) = want.as_mut() { want.granted |= execution_enabled; }
        let Some(opp) = best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster_book,
            &lighter_book,
            pos_f,
            margins_f,
            options.min_size,
            options.exposure_filter,
        ) else {
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        };
        if let Some(sanity) = book_sanity.entry_block() {
            if exposure_effect_f64(
                pos_f.aster_qty,
                pos_f.lighter_qty,
                opp.direction,
                opp.qty_f64,
                &math,
            ) != ExposureEffect::Reduce
            {
                let log_now = match last_book_sanity_block_log_at {
                    Some(ts) => ts.elapsed() >= Duration::from_secs(5),
                    None => true,
                };
                if log_now {
                    warn!(
                        "book sanity blocked new ARB entry market={} direction={} qty={} reason={:?} blocked_until={:?} failure_streak={} success_streak={}",
                        spec.market_id,
                        opp.direction.as_str(spec.hedge),
                        opp.qty,
                        sanity.last_reason,
                        sanity.blocked_until,
                        sanity.failure_streak,
                        sanity.success_streak
                    );
                    last_book_sanity_block_log_at = Some(tokio::time::Instant::now());
                }
                wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
                continue;
            }
        } else {
            last_book_sanity_block_log_at = None;
        }
        if tracing::enabled!(Level::DEBUG) {
            debug!(
                "arb opportunity {} qty={} gross={}bps net_margin={}bps sell_vwap={} buy_vwap={} expected_gross=${} expected_fee=${} expected_net=${} threshold_margin=${} min_qty={} desired_qty={} top_depth={} depth_supported={} liquidity_multiple={} sell_depth_target={} buy_depth_target={} sell_depth_available={} buy_depth_available={} sell_levels_used={} buy_levels_used={} headroom={} margin_room={}",
                opp.direction.as_str(spec.hedge),
                opp.qty,
                opp.gross_edge_bps,
                opp.expected_net_margin_bps,
                opp.sell_px,
                opp.buy_px,
                opp.expected_gross_usd,
                opp.expected_fee_usd,
                opp.expected_net_usd,
                opp.required_margin_usd,
                opp.min_qty,
                opp.desired_qty,
                opp.top_depth_qty,
                opp.depth_supported_qty,
                opp.liquidity_multiple,
                opp.sell_depth_target_qty,
                opp.buy_depth_target_qty,
                opp.sell_depth_available_qty,
                opp.buy_depth_available_qty,
                opp.sell_depth_levels_used,
                opp.buy_depth_levels_used,
                opp.headroom_qty,
                opp.margin_room_qty
            );
        }
        let gate = entry_gate.evaluate(
            OpportunityGateInput {
                timestamp: now,
                direction: opp.direction.as_str(spec.hedge),
                gross_edge_bps: opp.gross_edge_bps,
                expected_net_margin_bps: opp.expected_net_margin_bps,
                expected_net_usd: opp.expected_net_usd,
                qty: opp.qty,
                sell_px: opp.sell_px,
                buy_px: opp.buy_px,
                ref_px: opp.ref_px,
                top_depth_qty: opp.top_depth_qty,
                depth_guard_enabled: opp.depth_guard_enabled,
                liquidity_multiple: opp.liquidity_multiple,
                depth_supported_qty: opp.depth_supported_qty,
                sell_depth_target_qty: opp.sell_depth_target_qty,
                buy_depth_target_qty: opp.buy_depth_target_qty,
                sell_depth_available_qty: opp.sell_depth_available_qty,
                buy_depth_available_qty: opp.buy_depth_available_qty,
                sell_depth_worst_px: opp.sell_depth_worst_px,
                buy_depth_worst_px: opp.buy_depth_worst_px,
                sell_depth_levels_used: opp.sell_depth_levels_used,
                buy_depth_levels_used: opp.buy_depth_levels_used,
                sell_best_px: opp.sell_best_px,
                buy_best_px: opp.buy_best_px,
                sell_best_qty: opp.sell_best_qty,
                buy_best_qty: opp.buy_best_qty,
                aster_book_age_ms: aster_book.age_ms(now),
                lighter_book_age_ms: lighter_book.age_ms(now),
                force_record: execution_enabled,
            },
            cfg.arb.required_gross_edge_bps(),
        );
        if gate.recorded || !gate.allow_execution {
            // A visible-but-gated edge re-evaluates every poll; only log transitions
            // and a 5s heartbeat. Recorded samples (<=1/s by design) always log.
            let log_now = gate.recorded
                || match last_gate_log {
                    Some((decision, at)) => {
                        decision != gate.decision || at.elapsed() >= Duration::from_secs(5)
                    }
                    None => true,
                };
            if log_now {
                info!(
                    "entry gate decision market={} mode={} decision={} allow_execution={} would_allow={} gross={}bps threshold={:?} samples={} recorded={}",
                    spec.market_id,
                    cfg.arb.entry_gate.mode.as_str(),
                    gate.decision,
                    gate.allow_execution,
                    gate.would_allow,
                    opp.gross_edge_bps,
                    gate.threshold_bps,
                    gate.sample_count,
                    gate.recorded
                );
                last_gate_log = Some((gate.decision, tokio::time::Instant::now()));
            }
        }
        if !gate.allow_execution {
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }
        if !execution_enabled {
            if let (Some(tx), None) = (&options.want, &want) {
                if !options.pause.as_ref().is_some_and(|pause| pause.load(Ordering::Acquire))
                    && lighter.tx_ready() && !session.unresolved() {
                    next_want_id += 1;
                    want = Some(Want { id: next_want_id, at: tokio::time::Instant::now(), granted: false, attempted: false });
                    tx.send_replace(Some(next_want_id));
                    info!("asking XEMM for the execution rights market={} direction={} gross={}bps want_id={next_want_id}",
                        spec.market_id, opp.direction.as_str(spec.hedge), opp.gross_edge_bps);
                }
            }
            // Standby/observe hits this every poll while an edge exists; 5s heartbeat.
            let log_now = match last_standby_log_at {
                Some(at) => at.elapsed() >= Duration::from_secs(5),
                None => true,
            };
            if log_now {
                info!(
                    "standby skip order submission market={} direction={} qty={} gross={}bps expected_net=${} gate_decision={} threshold={:?} samples={} recorded={} observe_only={} lease_required={}",
                    spec.market_id,
                    opp.direction.as_str(spec.hedge),
                    opp.qty,
                    opp.gross_edge_bps,
                    opp.expected_net_usd,
                    gate.decision,
                    gate.threshold_bps,
                    gate.sample_count,
                    gate.recorded,
                    options.observe_only,
                    options.lease.is_some()
                );
                last_standby_log_at = Some(tokio::time::Instant::now());
            }
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }
        let reduce_only = options.exposure_filter == ExposureFilter::Reduce;
        if reduce_only
            && exposure_effect_f64(
                pos_f.aster_qty,
                pos_f.lighter_qty,
                opp.direction,
                opp.qty_f64,
                &math,
            ) != ExposureEffect::Reduce
        {
            warn!(
                "reduce-only execution guard skipped non-reducing opportunity market={} direction={} qty={} pos_aster={} pos_lighter={}",
                spec.market_id,
                opp.direction.as_str(spec.hedge),
                opp.qty,
                pos.aster_qty,
                pos.lighter_qty
            );
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }
        // No await between this final in-memory validation and starting the execution epoch.
        let final_now = Utc::now();
        let final_account = *account_rx.borrow();
        // A feed reset between the two reads is a skip, like the first read, not a process exit.
        let Ok((final_aster, final_lighter)) = fetch_books(&spec, &aster_books, &lighter) else {
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        };
        if !execution_lease_enabled(&mut lease_cache, &options, &spec, final_now, want.is_some()).0
            || options.pause.as_ref().is_some_and(|pause| pause.load(Ordering::Acquire))
            || !lighter.tx_ready() || !entry_gate.healthy() || !execution_journal.healthy() || session.unresolved()
            || !Arc::ptr_eq(&final_aster, &aster_book) || !Arc::ptr_eq(&final_lighter, &lighter_book)
            || !book_ok(&final_aster, final_now, cfg.arb.max_book_staleness_ms)
            || !book_ok(&final_lighter, final_now, cfg.arb.max_book_staleness_ms)
            || final_account.execution_epoch != account.execution_epoch
            || final_account.refreshed_at != account.refreshed_at
            || final_account.is_stale(account_snapshot_max_age)
            || (cfg.pnl.enabled && (session.baseline_equity().is_none() || final_account.margins.total_equity_usd().is_none()))
            || final_account.aster_open_orders != 0 || final_account.lighter_open_orders != 0 {
            wait_for_scan(&scan_wake, cfg.arb.poll_interval_ms).await;
            continue;
        }
        begin_execution(&execution_epoch);
        if let Some(want) = want.as_mut() { want.attempted = true; }
        let execution_result = execute_opportunity(
            &cfg,
            &spec,
            &aster,
            &lighter,
            &opp,
            pos,
            margins,
            reduce_only,
            &execution_journal,
            &session,
        )
        .await;
        match execution_result {
            Ok(report) => {
                account = AccountSnapshot {
                            execution_epoch: finish_execution(&execution_epoch),
                            aster_open_orders: 0,
                            lighter_open_orders: 0,
                    position: report.position,
                    lighter_ws_qty: report.lighter_ws_qty,
                    lighter_ws_rest_divergence_qty: report.lighter_ws_rest_divergence_qty,
                    margins: report.margin_after,
                    refreshed_at: tokio::time::Instant::now(),
                };
                publish_account(&account_tx, account);
                finish_execution(&execution_epoch);
                trades_executed += 1;
                info!(
                    "trade report count={} expected_net=${} actual_gross=${} actual_fees=${} actual_net=${} actual_net_bps={} fill_qty_mismatch={} aster_fill_qty={} aster_vwap={} aster_notional=${} aster_fee=${} lighter_fill_qty={} lighter_vwap={} lighter_notional=${} lighter_fee=${} available_margin_delta=${} available_before=${} available_after=${} aster_available_before=${} aster_available_after=${} lighter_available_before=${} lighter_available_after=${} final_aster_pos={} final_lighter_pos={} final_net_pos={}",
                    trades_executed,
                    opp.expected_net_usd,
                    report.economics.gross_usd,
                    report.economics.fees_usd,
                    report.economics.net_usd,
                    report.economics.net_bps,
                    report.economics.fill_qty_mismatch,
                    report.economics.aster_fill.qty,
                    report.economics.aster_fill.vwap,
                    report.economics.aster_fill.notional,
                    report.economics.aster_fill.fee_usd,
                    report.economics.lighter_fill.qty,
                    report.economics.lighter_fill.vwap,
                    report.economics.lighter_fill.notional,
                    report.economics.lighter_fill.fee_usd,
                    report.available_margin_delta_usd(),
                    report.margin_before.aster_available_usd + report.margin_before.lighter_available_usd,
                    report.margin_after.aster_available_usd + report.margin_after.lighter_available_usd,
                    report.margin_before.aster_available_usd,
                    report.margin_after.aster_available_usd,
                    report.margin_before.lighter_available_usd,
                    report.margin_after.lighter_available_usd,
                    report.position.aster_qty,
                    report.position.lighter_qty,
                    report.position.net_qty()
                );
                let hourly_breaker = if report.hedge_retry_action_taken {
                    recovered_failures.record(Decimal::ZERO, &cfg)
                } else { None };
                let row = pnl_trade_row(&spec, &opp, &report);
                if let Some(update) = commit_accounting(&mut pnl, row, &session, hourly_breaker).await? {
                    info!("pnl update market={} trades={} cumulative_pnl=${}",
                        spec.market_id, update.trade_count, update.cumulative_pnl_usdc);
                }
                if options.max_trades.is_some_and(|max| trades_executed >= max) {
                    info!("max_trades reached; stopping");
                    break;
                }
                let cooldown_ms = if reduce_only {
                    options.reduce_cooldown_ms
                } else {
                    cfg.arb.cooldown_ms
                };
                cooldown_until = tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
            }
            Err(e) => {
                if matches!(e, ExecutionError::OutcomeUnresolved { .. } | ExecutionError::Other(_)) {
                    bail!("execution stopped with session barrier retained: {e:#}");
                }
                if e.is_skip() {
                    warn!("arb execution skipped: {e:#}");
                    let cooldown_ms = if options.exposure_filter == ExposureFilter::Reduce {
                        options.reduce_cooldown_ms.max(1000)
                    } else {
                        cfg.arb.cooldown_ms.max(1000)
                    };
                    cooldown_until =
                        tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
                    finish_execution(&execution_epoch);
                    continue;
                }
                let needs_recovery = e.needs_recovery();
                if needs_recovery {
                    error!("arb execution failed; checking for rescue: {e:#}");
                } else {
                    warn!("arb execution failed without one-sided acceptance: {e:#}");
                }
                if !needs_recovery {
                    let cooldown_ms = if options.exposure_filter == ExposureFilter::Reduce {
                        options.reduce_cooldown_ms.max(1000)
                    } else {
                        cfg.arb.cooldown_ms.max(1000)
                    };
                    cooldown_until =
                        tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
                    finish_execution(&execution_epoch);
                    continue;
                }
                begin_execution(&execution_epoch);
                let recovery_result =
                    recover_if_needed(&cfg, &spec, &aster, &lighter, &http, margins, &session, &execution_journal).await;
                let recovery = recovery_result?;
                info!(
                    "rescue check complete action_taken={} estimated_loss=${} final_aster_pos={} final_lighter_pos={} lighter_ws={:?} aster_open_orders={} lighter_open_orders={} available_after=${}",
                    recovery.action_taken,
                    recovery.estimated_loss_usdc,
                    recovery.position.aster_qty,
                    recovery.position.lighter_qty,
                    recovery.lighter_ws_qty,
                    recovery.aster_open_orders,
                    recovery.lighter_open_orders,
                    recovery.margin_after.aster_available_usd + recovery.margin_after.lighter_available_usd
                );
                account = AccountSnapshot {
                            execution_epoch: finish_execution(&execution_epoch),
                            aster_open_orders: 0,
                            lighter_open_orders: 0,
                    position: recovery.position,
                    lighter_ws_qty: recovery.lighter_ws_qty,
                    lighter_ws_rest_divergence_qty: recovery
                        .lighter_ws_qty
                        .map(|ws| (ws - recovery.position.lighter_qty).abs()),
                    margins: recovery.margin_after,
                    refreshed_at: tokio::time::Instant::now(),
                };
                publish_account(&account_tx, account);
                finish_execution(&execution_epoch);
                let hourly_breaker = if recovery.action_taken {
                    recovered_failures.record(recovery.estimated_loss_usdc, &cfg)
                } else { None };
                // Every failed execution is accounted before its session barrier is released.
                record_recovery_loss(&mut pnl, &spec, &recovery, &session, hourly_breaker).await?;
                match refresh_account_snapshot(&spec.market_id, &aster, &lighter, &execution_epoch).await {
                    Ok(snapshot) => {
                        account = snapshot;
                        publish_account(&account_tx, account);
                    }
                    Err(refresh_err) => {
                        warn!("account snapshot refresh failed after recovery: {refresh_err:#}")
                    }
                }
                let cooldown_ms = if options.exposure_filter == ExposureFilter::Reduce {
                    options.reduce_cooldown_ms.max(1000)
                } else {
                    cfg.arb.cooldown_ms.max(1000)
                };
                cooldown_until = tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
            }
        }
    }
    Ok(())
    }.await;
    if let Some(task) = lease_cache.task.take() { task.abort(); }
    _account_refresh_task.abort();
    let (history_drained, executions_drained, pnl_drained) = tokio::join!(
        entry_gate.shutdown(), execution_journal.shutdown(),
        async { if let Some(pnl) = pnl.as_ref() { pnl.shutdown().await } else { Ok(()) } },
    );
    let drained = history_drained.and(executions_drained).and(pnl_drained);
    // A session that never armed sent no order, so it has nothing to verify; the account's
    // orders may be another engine's (XEMM quotes under `run`).
    let shutdown = if drained.is_ok() && session.armed() && !session.unresolved() {
        finish_execution(&execution_epoch);
        match refresh_account_snapshot(&spec.market_id, &aster, &lighter, &execution_epoch).await {
            Ok(snapshot) if snapshot.aster_open_orders == 0 && snapshot.lighter_open_orders == 0 => {
                // Book errors join the shutdown result so they cannot mask `run_result`.
                match fetch_books(&spec, &aster_books, &lighter) {
                    Ok((a_book, l_book)) if net_mismatch_notional(snapshot.position, &a_book, &l_book)
                        .is_some_and(|amount| amount <= cfg.risk.max_position_mismatch_usd) => session.clear_verified().await,
                    Ok(_) => Err(anyhow::anyhow!("shutdown position mismatch; session remains armed")),
                    Err(error) => Err(anyhow::anyhow!("shutdown books unavailable ({error:#}); session remains armed")),
                }
            }
            _ => Err(anyhow::anyhow!("shutdown account/orders unverified; session remains armed")),
        }
    } else if session.unresolved() { Err(anyhow::anyhow!("unresolved execution; session remains armed")) }
    else { drained };
    run_result.and(shutdown)
}

fn fetch_books(
    spec: &MarketSpec,
    aster_books: &AsterBookFeed,
    lighter: &OtherLeg,
) -> Result<(Arc<OrderBook>, Arc<OrderBook>)> {
    let aster = aster_books.order_book_arc()?;
    let lighter_book = lighter.order_book_arc(&spec.market_id)?;
    Ok((aster, lighter_book))
}

/// Scan-loop change-detection skip: both books unchanged (same published Arcs — each
/// applied update stores a fresh Arc, so pointer identity is a sound change proxy), no
/// new account snapshot generation, and a full evaluation ran within the floor. The
/// floor is a HARD bound: lease pickup, nonce-refresh completion, sanity-gate sampling,
/// and the mismatch guard all ride full passes, so none may be deferred longer than it.
fn scan_inputs_unchanged(
    last_sized: &Option<(Arc<OrderBook>, Arc<OrderBook>, u64)>,
    aster_book: &Arc<OrderBook>,
    lighter_book: &Arc<OrderBook>,
    account_gen: u64,
    since_full_eval: Duration,
    floor: Duration,
) -> bool {
    match last_sized {
        Some((a, l, gen)) => {
            Arc::ptr_eq(a, aster_book)
                && Arc::ptr_eq(l, lighter_book)
                && *gen == account_gen
                && since_full_eval < floor
        }
        None => false,
    }
}


fn book_ok(book: &OrderBook, now: chrono::DateTime<Utc>, max_age_ms: i64) -> bool {
    book.best_bid().is_some() && book.best_ask().is_some() && !book.is_crossed()
        && book.is_fresh(now, max_age_ms)
}

fn best_opportunity(
    cfg: &Config,
    spec: &MarketSpec,
    math: &MarketMathF64,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionF64,
    margins: MarginF64,
    min_size: bool,
    exposure_filter: ExposureFilter,
) -> Option<Opportunity> {
    let required = cfg.arb.required_gross_edge_bps();
    // f64 mirror of `required` for the cheap pre-filter; a non-representable Decimal
    // falls back to NEG_INFINITY, which disables early skipping (never fails open).
    let required_f = decimal_to_f64(required).unwrap_or(f64::NEG_INFINITY);

    let mut best: Option<Opportunity> = None;
    for direction in [
        Direction::SellAsterBuyLighter,
        Direction::SellLighterBuyAster,
    ] {
        let (sell_book, buy_book) = match direction {
            Direction::SellAsterBuyLighter => (aster, lighter),
            Direction::SellLighterBuyAster => (lighter, aster),
        };
        let (Some((top_sell, _)), Some((top_buy, _)), Some(reference)) =
            (sell_book.best_bid_f64(), buy_book.best_ask_f64(), aster.mid_f64()) else { continue; };
        if reference <= 0.0 || (top_sell - top_buy) / reference * 10_000.0
            < required_f - EDGE_PREFILTER_EPS_BPS { continue; }
        let reduce_cap = if exposure_filter == ExposureFilter::Reduce {
            let reduces_aster = match direction.aster_side() { Side::Buy => pos.aster_qty < 0.0, Side::Sell => pos.aster_qty > 0.0 };
            let reduces_lighter = match direction.lighter_side() { Side::Buy => pos.lighter_qty < 0.0, Side::Sell => pos.lighter_qty > 0.0 };
            if !reduces_aster || !reduces_lighter { continue; }
            Some(pos.aster_qty.abs().min(pos.lighter_qty.abs()))
        } else { None };
        let Some((sizing, sell_px, buy_px, ref_px)) =
            depth_priced_sizing(cfg, spec, math, direction, aster, lighter, pos, margins, min_size, reduce_cap)
        else {
            continue;
        };
        // Same expression as build_opportunity_f64 -> identical f64 by IEEE determinism.
        let gross_edge_bps_f = (sell_px - buy_px) / ref_px * 10_000.0;
        if gross_edge_bps_f < required_f - EDGE_PREFILTER_EPS_BPS {
            // Clearly below the edge floor: skip the ~30-conversion Decimal build.
            // Candidates within the epsilon band fall through to the exact filter.
            continue;
        }
        let opp = build_opportunity_f64(cfg, math, direction, sizing, sell_px, buy_px, ref_px);
        if opp.gross_edge_bps < required {
            continue;
        }
        if opportunity_allowed_by_exposure_filter(pos, &opp, exposure_filter, math) {
            best = Some(choose_better_opportunity(best, opp));
        }
    }

    best
}

fn opportunity_allowed_by_exposure_filter(
    pos: PositionF64,
    opp: &Opportunity,
    exposure_filter: ExposureFilter,
    math: &MarketMathF64,
) -> bool {
    match exposure_filter {
        ExposureFilter::Any => true,
        ExposureFilter::Reduce => {
            exposure_effect_f64(
                pos.aster_qty,
                pos.lighter_qty,
                opp.direction,
                opp.qty_f64,
                math,
            ) == ExposureEffect::Reduce
        }
    }
}


fn exposure_effect_f64(
    aster_qty: f64,
    lighter_qty: f64,
    direction: Direction,
    qty: f64,
    math: &MarketMathF64,
) -> ExposureEffect {
    if qty <= 0.0 || !qty.is_finite() || !aster_qty.is_finite() || !lighter_qty.is_finite() {
        return ExposureEffect::Unknown;
    }
    let a_sign = if matches!(direction.aster_side(), Side::Buy) {
        1.0
    } else {
        -1.0
    };
    let l_sign = -a_sign;
    let before = aster_qty.abs().max(lighter_qty.abs());
    let after_a = aster_qty + a_sign * qty;
    let after_l = lighter_qty + l_sign * qty;
    let after = after_a.abs().max(after_l.abs());
    let tol = qty_cmp_tol(math, before.max(qty), after);
    if after + tol < before {
        ExposureEffect::Reduce
    } else if after > before + tol {
        ExposureEffect::Increase
    } else {
        ExposureEffect::Flat
    }
}

fn choose_better_opportunity(current: Option<Opportunity>, candidate: Opportunity) -> Opportunity {
    match current {
        None => candidate,
        Some(existing) => {
            if candidate.expected_net_usd > existing.expected_net_usd
                || (candidate.expected_net_usd == existing.expected_net_usd
                    && candidate.expected_net_margin_bps > existing.expected_net_margin_bps)
            {
                candidate
            } else {
                existing
            }
        }
    }
}

fn build_opportunity_f64(
    cfg: &Config,
    math: &MarketMathF64,
    direction: Direction,
    sizing: SizingDecisionF64,
    sell_px_f: f64,
    buy_px_f: f64,
    ref_px_f: f64,
) -> Opportunity {
    let gross_edge_bps_f = (sell_px_f - buy_px_f) / ref_px_f * 10_000.0;
    let aster_px_f = if matches!(direction.aster_side(), Side::Sell) {
        sell_px_f
    } else {
        buy_px_f
    };
    let lighter_px_f = if matches!(direction.lighter_side(), Side::Sell) {
        sell_px_f
    } else {
        buy_px_f
    };
    let gross_usd_f = sizing.qty * (sell_px_f - buy_px_f);
    let fee_usd_f = sizing.qty
        * (aster_px_f * math.aster_taker_fee_rate
            + lighter_px_f * math.lighter_taker_fee_rate);
    let required_margin_usd_f = sizing.qty * ref_px_f * math.margin_rate;
    let sell_px = f64_to_dec(sell_px_f);
    let buy_px = f64_to_dec(buy_px_f);
    let ref_px = f64_to_dec(ref_px_f);
    let gross_edge_bps = f64_to_dec(gross_edge_bps_f);
    let gross_usd = f64_to_dec(gross_usd_f);
    let fee_usd = f64_to_dec(fee_usd_f);
    Opportunity {
        direction,
        qty: qty_f64_to_dec(math, sizing.qty),
        qty_f64: sizing.qty,
        gross_edge_bps,
        expected_net_margin_bps: gross_edge_bps - cfg.arb.required_gross_edge_bps(),
        sell_px,
        buy_px,
        ref_px,
        top_depth_qty: f64_to_dec(sizing.top_depth_qty),
        depth_guard_enabled: sizing.depth_guard_enabled,
        liquidity_multiple: f64_to_dec(sizing.liquidity_multiple),
        depth_supported_qty: f64_to_dec(sizing.depth_supported_qty),
        sell_depth_target_qty: f64_to_dec(sizing.sell_depth_target_qty),
        buy_depth_target_qty: f64_to_dec(sizing.buy_depth_target_qty),
        sell_depth_available_qty: f64_to_dec(sizing.sell_depth_available_qty),
        buy_depth_available_qty: f64_to_dec(sizing.buy_depth_available_qty),
        sell_depth_worst_px: f64_to_dec(sizing.sell_depth_worst_px),
        buy_depth_worst_px: f64_to_dec(sizing.buy_depth_worst_px),
        sell_depth_levels_used: sizing.sell_depth_levels_used,
        buy_depth_levels_used: sizing.buy_depth_levels_used,
        sell_best_px: f64_to_dec(sizing.sell_best_px),
        buy_best_px: f64_to_dec(sizing.buy_best_px),
        sell_best_qty: f64_to_dec(sizing.sell_best_qty),
        buy_best_qty: f64_to_dec(sizing.buy_best_qty),
        desired_qty: f64_to_dec(sizing.desired_qty),
        min_qty: f64_to_dec(sizing.min_qty),
        headroom_qty: f64_to_dec(sizing.headroom_qty),
        margin_room_qty: f64_to_dec(sizing.margin_room_qty),
        expected_gross_usd: gross_usd,
        expected_fee_usd: fee_usd,
        expected_net_usd: gross_usd - fee_usd,
        required_margin_usd: f64_to_dec(required_margin_usd_f),
    }
}


/// Depth sizing stays in f64; only candidates clearing its conservative edge floor
/// are converted to Decimal for submission and accounting.
fn depth_priced_sizing(
    cfg: &Config,
    spec: &MarketSpec,
    math: &MarketMathF64,
    direction: Direction,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionF64,
    margins: MarginF64,
    min_size: bool,
    reduce_cap: Option<f64>,
) -> Option<(SizingDecisionF64, f64, f64, f64)> {
    let ref_px_f = aster.mid_f64().or_else(|| lighter.mid_f64())?;
    if ref_px_f <= 0.0 {
        return None;
    }
    let (sell_book, buy_book) = match direction {
        Direction::SellAsterBuyLighter => (aster, lighter),
        Direction::SellLighterBuyAster => (lighter, aster),
    };
    let sell_top = sell_book.best_bid_f64()?;
    let buy_top = buy_book.best_ask_f64()?;
    let top_depth_qty_f = sell_top.1.min(buy_top.1);
    let desired_f = math.desired_notional / ref_px_f;
    let est_aster_px = if matches!(direction.aster_side(), Side::Sell) {
        sell_top.0
    } else {
        buy_top.0
    };
    let est_lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
        sell_top.0
    } else {
        buy_top.0
    };
    let est_min_qty = min_trade_qty_f64(math, est_aster_px, est_lighter_px)?;
    let a_delta_sign = if matches!(direction.aster_side(), Side::Buy) {
        1.0
    } else {
        -1.0
    };
    let l_delta_sign = -a_delta_sign;
    let max_abs_qty_f = math.max_abs_position_notional_usd / ref_px_f;
    let headroom = max_qty_by_headroom_f64(
        max_abs_qty_f,
        pos.aster_qty,
        pos.lighter_qty,
        a_delta_sign,
        l_delta_sign,
    );
    let margin_room = max_qty_by_available_margin_f64(
        ref_px_f,
        pos.aster_qty,
        pos.lighter_qty,
        a_delta_sign,
        l_delta_sign,
        margins.aster_available_usd,
        margins.lighter_available_usd,
        math.margin_buffer_usd,
    );

    let depth_guard_enabled = cfg.arb.depth_guard.enabled;
    let liquidity_multiple = math.liquidity_multiple(depth_guard_enabled);
    let max_levels = if depth_guard_enabled {
        cfg.arb.depth_guard.max_levels
    } else {
        1
    };
    let sell_available = sell_book.cumulative_qty_f64(Side::Sell, max_levels)?;
    let buy_available = buy_book.cumulative_qty_f64(Side::Buy, max_levels)?;
    let depth_supported_qty = sell_available.min(buy_available) / liquidity_multiple;
    let max_qty = depth_supported_qty.min(headroom).min(margin_room).min(reduce_cap.unwrap_or(f64::MAX));
    if max_qty <= 0.0 {
        return None;
    }

    let initial_qty = if min_size {
        let q = ceil_to_common_step_f64(est_min_qty, math);
        if qty_le(q, max_qty, math) {
            q
        } else {
            return None;
        }
    } else {
        let q = floor_to_common_step_f64(desired_f.min(max_qty), math);
        if qty_ge(q, est_min_qty, math) {
            q
        } else {
            return None;
        }
    };

    let (sizing, sell_px, buy_px) = depth_price_sized_qty_f64(
        spec,
        math,
        direction,
        sell_book,
        buy_book,
        initial_qty,
        desired_f,
        top_depth_qty_f,
        headroom,
        margin_room,
        depth_guard_enabled,
        liquidity_multiple,
        max_levels,
        depth_supported_qty,
        min_size,
    )?;
    Some((sizing, sell_px, buy_px, ref_px_f))
}

fn depth_price_sized_qty_f64(
    _spec: &MarketSpec,
    math: &MarketMathF64,
    direction: Direction,
    sell_book: &OrderBook,
    buy_book: &OrderBook,
    initial_qty: f64,
    desired_qty: f64,
    top_depth_qty: f64,
    headroom_qty: f64,
    margin_room_qty: f64,
    depth_guard_enabled: bool,
    liquidity_multiple: f64,
    max_levels: usize,
    depth_supported_qty: f64,
    min_size: bool,
) -> Option<(SizingDecisionF64, f64, f64)> {
    let mut qty = initial_qty;
    for _ in 0..3 {
        if qty <= 0.0 || !qty.is_finite() || qty_gt(qty, depth_supported_qty, math) {
            return None;
        }
        let depth_target = qty * liquidity_multiple;
        let sell_quote = sell_book.depth_vwap_f64(Side::Sell, depth_target, max_levels)?;
        let buy_quote = buy_book.depth_vwap_f64(Side::Buy, depth_target, max_levels)?;
        let sell_px = sell_quote.vwap_px;
        let buy_px = buy_quote.vwap_px;
        let aster_px = if matches!(direction.aster_side(), Side::Sell) {
            sell_px
        } else {
            buy_px
        };
        let lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
            sell_px
        } else {
            buy_px
        };
        let min_qty = min_trade_qty_f64(math, aster_px, lighter_px)?;
        if min_size {
            let min_step_qty = ceil_to_common_step_f64(min_qty, math);
            if qty_gt(min_step_qty, qty, math) {
                if qty_gt(min_step_qty, depth_supported_qty, math)
                    || qty_gt(min_step_qty, headroom_qty, math)
                    || qty_gt(min_step_qty, margin_room_qty, math)
                {
                    return None;
                }
                qty = min_step_qty;
                continue;
            }
        } else if !qty_ge(qty, min_qty, math) {
            return None;
        }
        return Some((
            SizingDecisionF64 {
                qty,
                desired_qty,
                min_qty,
                top_depth_qty,
                depth_guard_enabled,
                liquidity_multiple,
                depth_supported_qty,
                sell_depth_target_qty: sell_quote.target_qty,
                buy_depth_target_qty: buy_quote.target_qty,
                sell_depth_available_qty: sell_quote.available_qty,
                buy_depth_available_qty: buy_quote.available_qty,
                sell_depth_worst_px: sell_quote.worst_px,
                buy_depth_worst_px: buy_quote.worst_px,
                sell_depth_levels_used: sell_quote.levels_used,
                buy_depth_levels_used: buy_quote.levels_used,
                sell_best_px: sell_quote.best_px,
                buy_best_px: buy_quote.best_px,
                sell_best_qty: sell_quote.best_qty,
                buy_best_qty: buy_quote.best_qty,
                headroom_qty,
                margin_room_qty,
            },
            sell_px,
            buy_px,
        ));
    }
    None
}

fn min_trade_qty_f64(math: &MarketMathF64, aster_px: f64, lighter_px: f64) -> Option<f64> {
    if aster_px <= 0.0 || lighter_px <= 0.0 || !aster_px.is_finite() || !lighter_px.is_finite() {
        return None;
    }
    Some(
        math.aster_min_qty
            .max(math.aster_min_notional / aster_px)
            .max(math.lighter_min_notional / lighter_px),
    )
}

fn max_qty_by_headroom_f64(
    max_abs_qty: f64,
    aster_qty: f64,
    lighter_qty: f64,
    a_sign: f64,
    l_sign: f64,
) -> f64 {
    fn leg(max_abs_qty: f64, current: f64, sign: f64) -> f64 {
        let same_direction = current == 0.0
            || (current > 0.0 && sign > 0.0)
            || (current < 0.0 && sign < 0.0);
        if same_direction {
            (max_abs_qty - current.abs()).max(0.0)
        } else {
            current.abs() + max_abs_qty
        }
    }
    leg(max_abs_qty, aster_qty, a_sign).min(leg(max_abs_qty, lighter_qty, l_sign))
}

fn max_qty_by_available_margin_f64(
    ref_px: f64,
    aster_qty: f64,
    lighter_qty: f64,
    a_sign: f64,
    l_sign: f64,
    aster_available: f64,
    lighter_available: f64,
    buffer: f64,
) -> f64 {
    fn leg(ref_px: f64, current: f64, sign: f64, available: f64, buffer: f64) -> f64 {
        let increases_abs = current == 0.0
            || (current > 0.0 && sign > 0.0)
            || (current < 0.0 && sign < 0.0);
        let usable = available - buffer;
        let margin_qty = if usable <= 0.0 || ref_px <= 0.0 {
            0.0
        } else {
            usable / ref_px
        };
        if !increases_abs {
            // Reducing is margin-free only up to |current|: qty beyond that CROSSES
            // through flat and re-opens on the other side, consuming margin like a
            // fresh increase (the old unconditional f64::MAX let a crossing trade
            // open a new position with no margin room at all).
            return current.abs() + margin_qty;
        }
        margin_qty
    }
    leg(ref_px, aster_qty, a_sign, aster_available, buffer)
        .min(leg(ref_px, lighter_qty, l_sign, lighter_available, buffer))
}

fn floor_to_common_step_f64(qty: f64, math: &MarketMathF64) -> f64 {
    if qty <= 0.0 || !qty.is_finite() {
        return 0.0;
    }
    let units = snap_step_units(qty / math.common_qty_step).floor();
    round_qty_to_scale_f64(units * math.common_qty_step, math)
}

fn ceil_to_common_step_f64(qty: f64, math: &MarketMathF64) -> f64 {
    if qty <= 0.0 || !qty.is_finite() {
        return 0.0;
    }
    let units = snap_step_units(qty / math.common_qty_step).ceil();
    round_qty_to_scale_f64(units * math.common_qty_step, math)
}


fn next_execution_id() -> String {
    let seq = EXECUTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}-{seq}", Utc::now().timestamp_nanos_opt().unwrap_or(0), std::process::id())
}

fn execution_log_path(cfg: &Config, market: &MarketId) -> PathBuf {
    PathBuf::from(&cfg.pnl.persist_dir).join(format!("executions_{}.jsonl", market_component(market)))
}

#[derive(Serialize)]
struct ExecutionStart {
    schema_version: u32,
    economic_status: &'static str,
    timestamp: DateTime<Utc>,
    execution_id: String,
    session_id: String,
    market: String,
    outcome: &'static str,
    direction: &'static str,
    qty: Decimal,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ExecutionRecord {
    Start(ExecutionStart),
    Row(serde_json::Value),
}

type ExecutionJournal = ColdJournal<ExecutionRecord>;

async fn append_execution_record(journal: &ExecutionJournal, row: serde_json::Value) -> Result<()> {
    journal.append_confirmed(ExecutionRecord::Row(row)).await
}

#[derive(Debug, Serialize)]
pub(crate) struct LegEvidence {
    pub(crate) fee_evidence: Vec<FeeEvidence>,
    pub(crate) terminal: bool,
    pub(crate) qty: Option<Decimal>,
    pub(crate) fill: Option<FillSummary>,
    pub(crate) order_id: Option<i64>,
    pub(crate) error: Option<String>,
}

impl LegEvidence {
    fn not_submitted() -> Self {
        Self { fee_evidence:Vec::new(), terminal: true, qty: Some(Decimal::ZERO), fill: Some(FillSummary::zero()),
            order_id: None, error: None }
    }
    fn unresolved(error: String) -> Self {
        Self { fee_evidence:Vec::new(), terminal: false, qty: None, fill: None, order_id: None, error: Some(error) }
    }
}

pub(crate) fn aster_order_identity(outcome: &AsterOutcome, side: Side, qty: Decimal) -> serde_json::Value {
    let mut identity = match outcome {
        AsterOutcome::Accepted { client_order_id, venue_order_id, .. } => serde_json::json!({
            "venue": "aster", "submitted": true, "client_order_id": client_order_id, "order_id": venue_order_id }),
        AsterOutcome::Unknown { client_order_id, .. } => serde_json::json!({
            "venue": "aster", "submitted": true, "client_order_id": client_order_id }),
        AsterOutcome::Rejected { .. } => serde_json::json!({ "venue": "aster", "submitted": false }),
    };
    identity["side"] = serde_json::json!(side.as_str());
    identity["qty"] = serde_json::json!(qty);
    identity
}

pub(crate) fn lighter_order_identity(outcome: &LighterOutcome, side: Side, qty: Decimal) -> serde_json::Value {
    let mut identity = match outcome {
        LighterOutcome::Accepted { client_order_index, tx_hash, nonce, .. }
        | LighterOutcome::Unknown { client_order_index, tx_hash, nonce, .. } => serde_json::json!({
            "venue": "lighter", "submitted": true, "client_order_index": client_order_index,
            "tx_hash": tx_hash, "nonce": nonce }),
        _ => serde_json::json!({ "venue": "lighter", "submitted": false }),
    };
    identity["side"] = serde_json::json!(side.as_str());
    identity["qty"] = serde_json::json!(qty);
    identity
}

pub(crate) async fn resolve_aster_evidence(
    spec: &MarketSpec, aster: &AsterRest, outcome: &AsterOutcome, timeout: Duration,
) -> LegEvidence {
    let (client, initial) = match outcome {
        AsterOutcome::Accepted { client_order_id, raw, .. } => (client_order_id, Some(raw.clone())),
        AsterOutcome::Unknown { client_order_id, .. } => (client_order_id, None),
        AsterOutcome::Rejected { .. } => return LegEvidence::not_submitted(),
    };
    let deadline = tokio::time::Instant::now() + timeout;
    let mut known_terminal = None;
    let resolved = tokio::time::timeout_at(deadline, async {
        let mut body = initial;
        loop {
            if let Some(raw) = body.take() {
                if order_response_is_terminal(&raw)? {
                    let order: serde_json::Value = serde_json::from_str(&raw)?;
                    if let Some(returned) = order.get("clientOrderId").and_then(serde_json::Value::as_str) {
                        anyhow::ensure!(returned == client, "Aster terminal order identity mismatch");
                    }
                    let immediate = immediate_fill_from_order_response(&raw)?;
                    let order_id = order.get("orderId").and_then(serde_json::Value::as_i64)
                        .context("Aster terminal order lacks order id")?;
                    known_terminal = Some(LegEvidence { fee_evidence:Vec::new(), terminal: true, qty: Some(immediate.qty),
                        fill: if immediate.qty == Decimal::ZERO { Some(FillSummary::zero()) }
                            else { FillSummary::from_qty_notional(immediate.qty, immediate.notional, Decimal::ZERO)
                                .map(|fill| fill.with_fee_provenance(FeeProvenance::Unknown)) },
                        order_id: Some(order_id), error: Some("fill accounting deadline".to_string()) });
                    let fill = if immediate.qty == Decimal::ZERO { Some(FillSummary::zero()) }
                        else {
                            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                            match aster.wait_order_fill_summary(&spec.market_id, order_id, immediate.qty, remaining).await {
                                Ok(fill) => Some(fill),
                                Err(_) => FillSummary::from_qty_notional(immediate.qty, immediate.notional, Decimal::ZERO)
                                    .map(|fill| fill.with_fee_provenance(FeeProvenance::Unknown)),
                            }
                        };
                    return Ok::<_, anyhow::Error>(LegEvidence { fee_evidence:Vec::new(), terminal: true, qty: Some(immediate.qty),
                        fill, order_id: Some(order_id), error: None });
                }
            }
            match aster.query_order(&spec.market_id, client).await {
                Ok(order) => {
                    anyhow::ensure!(order.get("clientOrderId").and_then(serde_json::Value::as_str) == Some(client.as_str()),
                        "Aster queried order identity unavailable/mismatched");
                    body = Some(serde_json::to_string(&order)?);
                }
                Err(error) => debug!("Aster order not yet resolved client={client}: {error:#}"),
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }).await;
    match resolved {
        Ok(Ok(evidence)) => evidence,
        Ok(Err(error)) => LegEvidence::unresolved(format!("{error:#}")),
        Err(_) => known_terminal.unwrap_or_else(|| LegEvidence::unresolved(format!("Aster order {client} unresolved at deadline"))),
    }
}

pub(crate) async fn resolve_lighter_evidence(
    spec: &MarketSpec, lighter: &impl TerminalOrders, outcome: &LighterOutcome,
    mut pending: Option<PendingFill>, side: Side, qty: Decimal, timeout: Duration,
) -> LegEvidence {
    let client = match outcome {
        LighterOutcome::Accepted { client_order_index, .. }
        | LighterOutcome::Unknown { client_order_index, .. } => *client_order_index,
        _ => return LegEvidence::not_submitted(),
    };
    let start = tokio::time::Instant::now();
    let mut confirmation = if let Some(pending) = pending.as_mut() {
        Some(pending.observe_confirmed(timeout.min(Duration::from_secs(3))).await)
    } else { None };
    let confirmed_terminal = |value: &LighterFillConfirmation| {
        value.terminal_order.as_ref().is_some_and(|order| order.is_terminal())
            || (value.filled_qty == qty && value.fill.is_some())
    };
    let needs_history = confirmation.as_ref().is_none_or(|value| !confirmed_terminal(value)
        || value.fill.as_ref().is_none_or(|fill| fill.fee_provenance != FeeProvenance::Venue));
    if needs_history {
        let remaining = timeout.saturating_sub(start.elapsed());
        let history = lighter.resolve_order_terminal(&spec.market_id, client, side, remaining);
        tokio::pin!(history);
        if let Some(pending) = pending.as_mut() {
            let stream = pending.observe_confirmed(remaining);
            tokio::pin!(stream);
            tokio::select! {
                value = &mut stream => {
                    let strong = confirmed_terminal(&value) && value.fill.as_ref()
                        .is_some_and(|fill| fill.fee_provenance == FeeProvenance::Venue);
                    confirmation = Some(value);
                    if !strong { if let Ok(value) = history.await { confirmation = Some(value); } }
                }
                result = &mut history => match result {
                    Ok(value) => confirmation = Some(value),
                    Err(_) => confirmation = Some(stream.await),
                }
            }
        } else if let Ok(value) = history.await { confirmation = Some(value); }
    }
    match confirmation {
        Some(value) if confirmed_terminal(&value) => LegEvidence { fee_evidence:value.fee_evidence, terminal: true,
            qty: Some(value.filled_qty), fill: value.fill,
            order_id: value.terminal_order.and_then(|order| order.order_index), error: None },
        _ => LegEvidence::unresolved(format!("Lighter order {client} has no terminal evidence")),
    }
}

async fn wait_position_evidence(
    cfg: &Config, spec: &MarketSpec, aster: &AsterRest, lighter: &OtherLeg,
    expected: PositionSnapshot, reference: Decimal,
) -> Result<PositionSnapshot> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    tokio::time::timeout_at(deadline, async {
        loop {
            if let Ok(position) = reconcile_positions(&spec.market_id, aster, lighter).await {
                if (position.aster_qty - expected.aster_qty).abs() * reference <= cfg.risk.max_position_mismatch_usd
                    && (position.lighter_qty - expected.lighter_qty).abs() * reference <= cfg.risk.max_position_mismatch_usd {
                    return Ok(position);
                }
            }
            tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms.max(250))).await;
        }
    }).await.context("positions did not reflect terminal fill evidence")?
}

async fn execute_opportunity(
    cfg: &Config, spec: &MarketSpec, aster: &AsterRest, lighter: &OtherLeg,
    opp: &Opportunity, pre_position: PositionSnapshot, margin_before: MarginSnapshot,
    reduce_only: bool, journal: &ExecutionJournal, session: &ActiveSession,
) -> std::result::Result<TradeReport, ExecutionError> {
    let execution_id = next_execution_id();
    let started_at = Utc::now();
    let aster_side = opp.direction.aster_side();
    let lighter_side = opp.direction.lighter_side();
    let aster_bound = aster_price_bound(opp, cfg.arb.max_aster_slippage_bps);
    let lighter_bound = lighter_price_bound(opp, cfg.arb.max_lighter_slippage_bps);
    // The durable session was armed cold. This bounded queue operation never waits for disk.
    journal.try_append(ExecutionRecord::Start(ExecutionStart { schema_version:2,economic_status:"incomplete",
        timestamp:started_at,execution_id:execution_id.clone(),session_id:session.id().to_string(),
        market:spec.market_id.to_string(),outcome:"submitting",direction:opp.direction.as_str(spec.hedge),qty:opp.qty }))?;
    let (a_res, (l_res, pending)) = tokio::join!(
        aster.submit_ioc_order(&spec.market_id, aster_side, opp.qty, aster_bound, reduce_only),
        lighter.submit_market_order_deferred_fill(&spec.market_id, lighter_side, opp.qty, lighter_bound, reduce_only),
    );
    let mut orders = vec![aster_order_identity(&a_res, aster_side, opp.qty), lighter_order_identity(&l_res, lighter_side, opp.qty)];
    session.record_unresolved(serde_json::json!({ "schema_version": 2, "economic_status": "incomplete",
        "timestamp": Utc::now(), "execution_id": execution_id, "session_id": session.id(),
        "market": spec.market_id.to_string(), "outcome": "resolving", "orders": orders,
        "orders_complete": true, "pre_positions": {"aster_qty": pre_position.aster_qty, "lighter_qty": pre_position.lighter_qty} })).await?;
    let (a, l) = tokio::join!(
        resolve_aster_evidence(spec, aster, &a_res, Duration::from_secs(10)),
        resolve_lighter_evidence(spec, lighter, &l_res, pending, lighter_side, opp.qty, Duration::from_secs(10)),
    );
    let mut row = serde_json::json!({
        "schema_version": 2, "economic_status": "incomplete", "timestamp": Utc::now(),
        "started_at": started_at, "execution_id": execution_id, "session_id": session.id(), "market": spec.market_id.to_string(),
        "execution_mode": "concurrent_confirm_rescue", "direction": opp.direction.as_str(spec.hedge), "hedge_venue": spec.hedge,
        "qty": opp.qty, "reduce_only": reduce_only, "orders": orders, "orders_complete": true,
        "aster_submit": format!("{a_res:?}"), "lighter_submit": format!("{l_res:?}"),
        "aster_confirmation": a, "lighter_confirmation": l,
        "gross_edge_bps": opp.gross_edge_bps, "expected_net_usd": opp.expected_net_usd,
        "aster_bound": aster_bound, "lighter_bound": lighter_bound,
        "pre_positions": {"aster_qty": pre_position.aster_qty, "lighter_qty": pre_position.lighter_qty},
        "depth_guard": { "enabled": opp.depth_guard_enabled, "liquidity_multiple": opp.liquidity_multiple,
            "sell_depth_target_qty": opp.sell_depth_target_qty, "buy_depth_target_qty": opp.buy_depth_target_qty,
            "sell_depth_worst_px": opp.sell_depth_worst_px, "buy_depth_worst_px": opp.buy_depth_worst_px }
    });
    if !a.terminal || !l.terminal {
        row["outcome"] = serde_json::json!("unresolved");
        session.record_unresolved(row.clone()).await?;
        append_execution_record(journal,row).await?;
        return Err(ExecutionError::OutcomeUnresolved { details: "submission terminal evidence unavailable; session remains armed".to_string() });
    }
    let mut aster_fill = a.fill;
    let mut lighter_fill = l.fill;
    let mut lighter_fee_evidence = l.fee_evidence.clone();
    let a_sign = if aster_side == Side::Buy { Decimal::ONE } else { -Decimal::ONE };
    let expected = PositionSnapshot {
        aster_qty: pre_position.aster_qty + a_sign * a.qty.unwrap_or(Decimal::ZERO),
        lighter_qty: pre_position.lighter_qty - a_sign * l.qty.unwrap_or(Decimal::ZERO),
    };
    let mut position = match wait_position_evidence(cfg, spec, aster, lighter, expected, opp.ref_px).await {
        Ok(position) => position,
        Err(error) => {
            row["outcome"] = serde_json::json!("unreconciled");
            row["error"] = serde_json::json!(format!("{error:#}"));
            append_execution_record(journal,row).await?;
            return Err(ExecutionError::Unreconciled { details: format!("{error:#}") });
        }
    };
    let mut hedge_retry_action_taken = false;
    let mut aster_order_id = a.order_id.unwrap_or(0);
    let mut lighter_client_order_index = match &l_res {
        LighterOutcome::Accepted { client_order_index, .. } | LighterOutcome::Unknown { client_order_index, .. } => *client_order_index,
        _ => 0,
    };
    if position.net_qty().abs() * opp.ref_px > cfg.risk.max_position_mismatch_usd {
        let retry = try_missing_hedge_retry(cfg, spec, aster, lighter, opp, pre_position, position, reduce_only).await;
        hedge_retry_action_taken = retry.attempted;
        for attempt in &retry.attempts {
            orders.push(attempt.identity.clone());
            if attempt.fill_status.as_deref() == Some("unresolved") {
                row["outcome"] = serde_json::json!("unresolved_hedge_retry");
                row["orders_complete"] = serde_json::json!(!orders.iter().any(|order|
                    order.get("identity_unavailable").and_then(serde_json::Value::as_bool) == Some(true)));
                row["orders"] = serde_json::json!(orders);
                row["hedge_retry"] = hedge_retry_report_json(Some(&retry));
                session.record_unresolved(row.clone()).await?;
                append_execution_record(journal,row).await?;
                return Err(ExecutionError::OutcomeUnresolved { details: "hedge retry remains unresolved".to_string() });
            }
            match attempt.venue {
                HedgeRetryVenue::Aster => {
                    aster_fill = add_fill_summary(aster_fill, attempt.fill);
                    if aster_order_id == 0 { aster_order_id = attempt.identity.get("order_id").and_then(serde_json::Value::as_i64).unwrap_or(0); }
                }
                HedgeRetryVenue::Lighter => {
                    lighter_fill = add_fill_summary(lighter_fill, attempt.fill);
                    lighter_fee_evidence.extend(attempt.fee_evidence.iter().cloned());
                    if lighter_client_order_index == 0 { lighter_client_order_index = attempt.identity.get("client_order_index").and_then(serde_json::Value::as_i64).unwrap_or(0); }
                }
            }
        }
        row["hedge_retry"] = hedge_retry_report_json(Some(&retry));
        if retry.succeeded {
            position = retry.final_position.expect("successful retry has final position");
        } else {
            row["outcome"] = serde_json::json!("unreconciled");
            row["orders"] = serde_json::json!(orders);
            append_execution_record(journal,row).await?;
            return Err(ExecutionError::Unreconciled { details: retry.error.unwrap_or_else(|| "missing hedge could not be completed".to_string()) });
        }
    }
    let (a_open, l_open, margin_after) = tokio::join!(aster.open_orders(&spec.market_id),
        lighter.rest_open_orders_count(&spec.market_id), reconcile_margins(aster, lighter));
    let economics = match (aster_fill, lighter_fill) {
        (Some(a), Some(l)) => actual_economics(cfg, opp, a, l),
        _ => Err(ExecutionError::AccountingUnavailable { details: "terminal fill economics incomplete".to_string() }),
    };
    row["orders"] = serde_json::json!(orders);
    row["aster_fill"] = serde_json::json!(aster_fill);
    row["lighter_fill"] = serde_json::json!(lighter_fill);
    row["lighter_fee_evidence"] = serde_json::json!(lighter_fee_evidence);
    row["aster_order_id"] = serde_json::json!(aster_order_id);
    row["lighter_client_order_index"] = serde_json::json!(lighter_client_order_index);
    row["final_positions"] = serde_json::json!({"aster_qty": position.aster_qty,
        "lighter_rest_qty": position.lighter_qty, "net_qty": position.net_qty()});
    let clean_orders = a_open.as_ref().is_ok_and(|orders| orders.is_empty()) && l_open.as_ref().is_ok_and(|count| *count == 0);
    row["outcome"] = serde_json::json!(if economics.is_ok() && clean_orders && margin_after.is_ok() { "success" } else { "accounting_unavailable" });
    if let Ok(economics) = &economics {
        row["economic_status"] = serde_json::json!("confirmed");
        row["actual_economics"] = serde_json::json!({"gross_usd": economics.gross_usd,
            "fees_usd": economics.fees_usd, "net_usd": economics.net_usd, "net_bps": economics.net_bps,
            "fill_qty_mismatch": economics.fill_qty_mismatch, "residual_qty": economics.residual_qty,
            "residual_notional_usd": economics.residual_notional_usd});
    }
    append_execution_record(journal,row).await?;
    if !clean_orders { return Err(ExecutionError::AccountingUnavailable { details: "open-order evidence unavailable or nonempty".to_string() }); }
    let margin_after = margin_after.map_err(|error| ExecutionError::AccountingUnavailable { details: format!("{error:#}") })?;
    let economics = economics?;
    if a.qty == Some(Decimal::ZERO) && l.qty == Some(Decimal::ZERO) && !hedge_retry_action_taken {
        session.resolve_execution().await?;
        return Err(ExecutionError::Skipped { details: "both legs confirmed terminal without fills".to_string() });
    }
    let lighter_ws_qty = lighter.ws_position_qty(&spec.market_id).ok();
    Ok(TradeReport { execution_id, lighter_fee_evidence, position, lighter_ws_qty,
        lighter_ws_rest_divergence_qty: lighter_ws_qty.map(|ws| (ws - position.lighter_qty).abs()),
        margin_before, margin_after, economics, aster_order_id, lighter_client_order_index, hedge_retry_action_taken })
}

fn zero_fill_summary() -> FillSummary { FillSummary::zero() }


/// Ledger row for a recovery: books the conservative equity-delta estimate into cumulative
/// PnL so the loss breaker cannot develop blind spots. The rescue path books it even when no
/// close was needed, because that execution's own economics were unavailable; auto-flatten
/// books it only after acting. Only acting recoveries feed the hourly recovered-loss limiter.
fn recovery_loss_row(spec: &MarketSpec, recovery: &RecoveryReport) -> TradeLedgerRow {
    let loss = recovery.estimated_loss_usdc;
    let timestamp = Utc::now();
    let event_id = recovery.execution_id.clone();
    TradeLedgerRow {
        schema_version: 2,
        economic_status: EconomicStatus::Estimated,
        execution_id: Some(event_id.clone()),
        source_event_id: Some(format!("recovery-{event_id}")),
        timestamp,
        market: spec.market_id.0.clone(),
        direction: "RECOVERY".to_string(),
        qty: Decimal::ZERO,
        expected_net_usd: Decimal::ZERO,
        actual_gross_usd: -loss,
        actual_fees_usd: Decimal::ZERO,
        actual_net_usd: -loss,
        actual_net_bps: Decimal::ZERO,
        fill_qty_mismatch: Decimal::ZERO,
        aster_fill: zero_fill_summary().with_fee_provenance(FeeProvenance::Unknown),
        hedge_venue: spec.hedge,
        hedge_fill: zero_fill_summary().with_fee_provenance(FeeProvenance::Unknown),
        hedge_fee_evidence: Vec::new(),
        // The controller's realized-loss stop dedups rows on `taker:<aster_order_id>:<lighter_client_order_index>`;
        // a constant 0 collapsed every recovery after the first into one key, hiding
        // repeat losses from downstream accounting. The row timestamp keys each recovery.
        aster_order_id: -timestamp.timestamp_micros(),
        hedge_client_order_index: 0,
        final_aster_position: recovery.position.aster_qty,
        final_hedge_position: recovery.position.lighter_qty,
        final_net_position: recovery.position.net_qty(),
        available_before_usd: recovery.margin_after.aster_available_usd
            + recovery.margin_after.lighter_available_usd
            + loss,
        available_after_usd: recovery.margin_after.aster_available_usd
            + recovery.margin_after.lighter_available_usd,
        aster_available_before_usd: recovery.margin_after.aster_available_usd,
        aster_available_after_usd: recovery.margin_after.aster_available_usd,
        hedge_available_before_usd: recovery.margin_after.lighter_available_usd,
        hedge_available_after_usd: recovery.margin_after.lighter_available_usd,
    }
}

/// The only completed-execution accounting exit: acknowledge the row before enforcing
/// either breaker, and retain the session marker if persistence fails.
async fn commit_accounting(
    pnl: &mut Option<PnlTracker>, row: TradeLedgerRow, session: &ActiveSession,
    hourly_breaker: Option<String>,
) -> Result<Option<PnlUpdate>> {
    let update = if let Some(pnl) = pnl.as_mut() { Some(pnl.record_trade(row).await?) } else { None };
    session.resolve_execution().await?;
    if let Some(breaker) = update.as_ref().and_then(|update| update.breaker.as_ref()) {
        bail!("circuit breaker triggered: cumulative PnL ${} <= -${}; manual reset required",
            breaker.cumulative_pnl_usdc, breaker.max_loss_usdc);
    }
    if let Some(reason) = hourly_breaker { bail!("recovered-failure breaker triggered: {reason}"); }
    Ok(update)
}

/// Record a recovery-loss ledger row (if the PnL tracker is enabled) and enforce the
/// cumulative-loss breaker, mirroring the normal trade path.
async fn record_recovery_loss(
    pnl: &mut Option<PnlTracker>, spec: &MarketSpec, recovery: &RecoveryReport,
    session: &ActiveSession, hourly_breaker: Option<String>,
) -> Result<()> {
    if let Some(update) = commit_accounting(pnl, recovery_loss_row(spec, recovery), session, hourly_breaker).await? {
        warn!("recovery estimate recorded: market={} loss=${} cumulative_guard_pnl=${}",
            spec.market_id, recovery.estimated_loss_usdc, update.cumulative_pnl_usdc);
    }
    Ok(())
}

fn pnl_trade_row(spec: &MarketSpec, opp: &Opportunity, report: &TradeReport) -> TradeLedgerRow {
    TradeLedgerRow {
        schema_version: 2,
        economic_status: EconomicStatus::Confirmed,
        execution_id: Some(report.execution_id.clone()),
        source_event_id: Some(report.execution_id.clone()),
        timestamp: Utc::now(),
        market: spec.market_id.0.clone(),
        direction: opp.direction.as_str(spec.hedge).to_string(),
        qty: report
            .economics
            .aster_fill
            .qty
            .min(report.economics.lighter_fill.qty),
        expected_net_usd: opp.expected_net_usd,
        actual_gross_usd: report.economics.gross_usd,
        actual_fees_usd: report.economics.fees_usd,
        actual_net_usd: report.economics.net_usd,
        actual_net_bps: report.economics.net_bps,
        fill_qty_mismatch: report.economics.fill_qty_mismatch,
        aster_fill: report.economics.aster_fill,
        hedge_venue: spec.hedge,
        hedge_fill: report.economics.lighter_fill,
        hedge_fee_evidence: report.lighter_fee_evidence.clone(),
        aster_order_id: report.aster_order_id,
        hedge_client_order_index: report.lighter_client_order_index,
        final_aster_position: report.position.aster_qty,
        final_hedge_position: report.position.lighter_qty,
        final_net_position: report.position.net_qty(),
        available_before_usd: report.margin_before.aster_available_usd
            + report.margin_before.lighter_available_usd,
        available_after_usd: report.margin_after.aster_available_usd
            + report.margin_after.lighter_available_usd,
        aster_available_before_usd: report.margin_before.aster_available_usd,
        aster_available_after_usd: report.margin_after.aster_available_usd,
        hedge_available_before_usd: report.margin_before.lighter_available_usd,
        hedge_available_after_usd: report.margin_after.lighter_available_usd,
    }
}

fn actual_economics(
    cfg: &Config,
    opp: &Opportunity,
    aster_fill: FillSummary,
    lighter_fill: FillSummary,
) -> std::result::Result<ActualEconomics, ExecutionError> {
    if aster_fill.fee_provenance != FeeProvenance::Venue || lighter_fill.fee_provenance != FeeProvenance::Venue {
        return Err(ExecutionError::AccountingUnavailable {
            details: "one or both execution fees are unknown or estimated".to_string(),
        });
    }
    let fill_qty_mismatch = (aster_fill.qty - lighter_fill.qty).abs();
    if fill_qty_mismatch * opp.ref_px > cfg.risk.max_position_mismatch_usd {
        return Err(ExecutionError::AccountingUnavailable {
            details: format!(
                "fill accounting qty mismatch too large: aster_qty={} lighter_qty={} mismatch={} notional=${}",
                aster_fill.qty,
                lighter_fill.qty,
                fill_qty_mismatch,
                fill_qty_mismatch * opp.ref_px
            ),
        });
    }
    let (sell_fill, buy_fill) = match opp.direction {
        Direction::SellAsterBuyLighter => (aster_fill, lighter_fill),
        Direction::SellLighterBuyAster => (lighter_fill, aster_fill),
    };
    // Matched spread capture uses the matched quantity at the two VWAPs; it is not
    // realized account PnL while cross-venue inventory remains open. With unequal legs
    // (tolerated up to max_position_mismatch_usd) the naive `sell.notional − buy.notional`
    // books the unhedged residual as pure profit/loss — masking the loss breaker with
    // fiction and later mirrored by the residual close. The residual is reported as open
    // exposure instead; recovery losses book via `estimated_loss_usdc`.
    let matched_qty = sell_fill.qty.min(buy_fill.qty);
    let gross_usd = matched_qty * (sell_fill.vwap - buy_fill.vwap);
    let residual_qty = sell_fill.qty - buy_fill.qty;
    let residual_notional_usd = residual_qty.abs() * opp.ref_px;
    let fees_usd = aster_fill.fee_usd + lighter_fill.fee_usd;
    let net_usd = gross_usd - fees_usd;
    let denom = (aster_fill.notional + lighter_fill.notional) / Decimal::from(2u32);
    let net_bps = if denom > Decimal::ZERO {
        net_usd / denom * Decimal::from(10_000u32)
    } else {
        Decimal::ZERO
    };
    Ok(ActualEconomics {
        aster_fill,
        lighter_fill,
        gross_usd,
        fees_usd,
        net_usd,
        net_bps,
        fill_qty_mismatch,
        residual_qty,
        residual_notional_usd,
    })
}

fn add_fill_summary(base: Option<FillSummary>, extra: Option<FillSummary>) -> Option<FillSummary> {
    match (base, extra) {
        (Some(a), Some(b)) => {
            let qty = a.qty + b.qty;
            let notional = a.notional + b.notional;
            if qty <= Decimal::ZERO || notional <= Decimal::ZERO {
                None
            } else {
                Some(FillSummary {
                    qty,
                    vwap: notional / qty,
                    notional,
                    fee_usd: a.fee_usd + b.fee_usd,
                    fee_provenance: if a.fee_provenance == FeeProvenance::Venue && b.fee_provenance == FeeProvenance::Venue {
                        FeeProvenance::Venue
                    } else { FeeProvenance::Unknown },
                })
            }
        }
        (Some(_), None) | (None, Some(_)) => None,
        (None, None) => None,
    }
}

fn expected_post_position(
    pre_position: PositionSnapshot,
    direction: Direction,
    qty: Decimal,
) -> PositionSnapshot {
    let aster_sign = if matches!(direction.aster_side(), Side::Buy) {
        Decimal::ONE
    } else {
        -Decimal::ONE
    };
    PositionSnapshot {
        aster_qty: pre_position.aster_qty + aster_sign * qty,
        lighter_qty: pre_position.lighter_qty - aster_sign * qty,
    }
}

fn retry_price_bound(opp: &Opportunity, side: Side, slippage_bps: Decimal) -> Decimal {
    let rate = bps_to_rate(slippage_bps);
    match side {
        Side::Buy => opp.buy_px * (Decimal::ONE + rate),
        Side::Sell => opp.sell_px * (Decimal::ONE - rate),
    }
}

fn hedge_retry_plan(
    cfg: &Config,
    spec: &MarketSpec,
    opp: &Opportunity,
    pre_position: PositionSnapshot,
    current_position: PositionSnapshot,
) -> Option<HedgeRetryPlan> {
    let expected = expected_post_position(pre_position, opp.direction, opp.qty);
    let aster_missing = expected.aster_qty - current_position.aster_qty;
    let lighter_missing = expected.lighter_qty - current_position.lighter_qty;
    let aster_missing_notional = aster_missing.abs() * opp.ref_px;
    let lighter_missing_notional = lighter_missing.abs() * opp.ref_px;
    let aster_needs_retry = aster_missing_notional > cfg.risk.max_position_mismatch_usd;
    let lighter_needs_retry = lighter_missing_notional > cfg.risk.max_position_mismatch_usd;
    if aster_needs_retry == lighter_needs_retry {
        return None;
    }
    let (venue, missing_qty, step, min_qty, min_notional) = if aster_needs_retry {
        (
            HedgeRetryVenue::Aster,
            aster_missing,
            spec.step,
            spec.aster_min_qty,
            spec.aster_min_notional,
        )
    } else {
        (
            HedgeRetryVenue::Lighter,
            lighter_missing,
            spec.lighter_qty_step,
            spec.lighter_qty_step,
            spec.lighter_min_notional,
        )
    };
    let qty = floor_to_step(missing_qty.abs(), step);
    if qty <= Decimal::ZERO
        || qty < min_qty
        || qty * opp.ref_px < min_notional
        || qty * opp.ref_px <= cfg.risk.max_position_mismatch_usd
    {
        return None;
    }
    let side = if missing_qty > Decimal::ZERO {
        Side::Buy
    } else {
        Side::Sell
    };
    Some(HedgeRetryPlan {
        venue,
        side,
        qty,
        price_bound: retry_price_bound(opp, side, cfg.arb.hedge_retry_slippage_bps),
    })
}

async fn submit_aster_hedge_retry(
    _cfg: &Config, spec: &MarketSpec, aster: &AsterRest, plan: HedgeRetryPlan,
    reduce_only: bool, timeout: Duration,
) -> (String, Option<FillSummary>, Option<String>, Option<String>, serde_json::Value, Vec<FeeEvidence>) {
    let start = tokio::time::Instant::now();
    let outcome = match tokio::time::timeout(timeout,
        aster.submit_ioc_order(&spec.market_id, plan.side, plan.qty, plan.price_bound, reduce_only)).await {
        Ok(outcome) => outcome,
        Err(_) => return ("Aster hedge retry submit deadline".to_string(), None, Some("unresolved".to_string()),
            Some("submission identity unavailable at deadline".to_string()), serde_json::json!({
                "venue":"aster","submitted":true,"identity_unavailable":true,"side":plan.side.as_str(),"qty":plan.qty}),Vec::new()),
    };
    let identity = aster_order_identity(&outcome, plan.side, plan.qty);
    let evidence = resolve_aster_evidence(spec, aster, &outcome, timeout.saturating_sub(start.elapsed())).await;
    (format!("{outcome:?}"), evidence.fill,
        Some(if evidence.terminal { "terminal" } else { "unresolved" }.to_string()), evidence.error, identity, evidence.fee_evidence)
}

async fn submit_lighter_hedge_retry(
    spec: &MarketSpec, lighter: &OtherLeg, plan: HedgeRetryPlan,
    reduce_only: bool, timeout: Duration,
) -> (String, Option<FillSummary>, Option<String>, Option<String>, serde_json::Value, Vec<FeeEvidence>) {
    let start = tokio::time::Instant::now();
    let (outcome, pending) = match tokio::time::timeout(timeout, lighter.submit_market_order_deferred_fill(
        &spec.market_id, plan.side, plan.qty, plan.price_bound, reduce_only)).await {
        Ok(result) => result,
        Err(_) => return ("Lighter hedge retry submit deadline".to_string(), None, Some("unresolved".to_string()),
            Some("submission identity unavailable at deadline".to_string()), serde_json::json!({
                "venue":"lighter","submitted":true,"identity_unavailable":true,"side":plan.side.as_str(),"qty":plan.qty}),Vec::new()),
    };
    let identity = lighter_order_identity(&outcome, plan.side, plan.qty);
    let evidence = resolve_lighter_evidence(spec, lighter, &outcome, pending, plan.side, plan.qty,
        timeout.saturating_sub(start.elapsed())).await;
    (format!("{outcome:?}"), evidence.fill,
        Some(if evidence.terminal { "terminal" } else { "unresolved" }.to_string()), evidence.error, identity, evidence.fee_evidence)
}

async fn try_missing_hedge_retry(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &OtherLeg,
    opp: &Opportunity,
    pre_position: PositionSnapshot,
    mut current_position: PositionSnapshot,
    reduce_only: bool,
) -> HedgeRetryReport {
    let timeout = Duration::from_millis(cfg.arb.hedge_retry_timeout_ms);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut report = HedgeRetryReport::empty(cfg.arb.hedge_retry_slippage_bps, None);
    for attempt_no in 1..=cfg.arb.max_hedge_retry_attempts {
        let Some(plan) = hedge_retry_plan(cfg, spec, opp, pre_position, current_position) else {
            report.error = Some(format!(
                "no single missing hedge retry plan for current positions aster={} lighter={} expected_aster={} expected_lighter={}",
                current_position.aster_qty,
                current_position.lighter_qty,
                expected_post_position(pre_position, opp.direction, opp.qty).aster_qty,
                expected_post_position(pre_position, opp.direction, opp.qty).lighter_qty
            ));
            return report;
        };
        report.attempted = true;
        warn!(
            "missing hedge retry attempt={} venue={} side={} qty={} bound={} reduce_only={} slippage_bps={}",
            attempt_no,
            plan.venue.as_str(),
            plan.side,
            plan.qty,
            plan.price_bound,
            reduce_only,
            cfg.arb.hedge_retry_slippage_bps
        );
        let (submit_result, fill, fill_status, submit_error, identity, fee_evidence) = match plan.venue {
            HedgeRetryVenue::Aster => {
                submit_aster_hedge_retry(cfg, spec, aster, plan, reduce_only, timeout).await
            }
            HedgeRetryVenue::Lighter => {
                submit_lighter_hedge_retry(spec, lighter, plan, reduce_only, timeout).await
            }
        };
        let unresolved = fill_status.as_deref() == Some("unresolved");
        report.attempts.push(HedgeRetryAttempt {
            fee_evidence,
            identity,
            attempt: attempt_no,
            venue: plan.venue,
            side: plan.side,
            qty: plan.qty,
            price_bound: plan.price_bound,
            reduce_only,
            submit_result: Some(submit_result),
            fill,
            fill_status,
            error: submit_error.clone(),
        });
        if unresolved {
            report.error = Some("retry order terminal evidence unavailable".to_string());
            return report;
        }
        let verified = tokio::time::timeout_at(deadline, async { tokio::join!(
            wait_post_trade_reconciled_for(cfg, spec, aster, lighter, opp,
                deadline.saturating_duration_since(tokio::time::Instant::now())),
            aster.open_orders(&spec.market_id),
            lighter.rest_open_orders_count(&spec.market_id),
        ) }).await;
        let (reconciled, open_a, open_l) = match verified {
            Ok(result) => result,
            Err(_) => { report.error = Some("hedge retry verification deadline".to_string()); return report; }
        };
        match (reconciled, open_a, open_l) {
            (Ok((position, net_notional)), Ok(aster_orders), Ok(lighter_orders))
                if aster_orders.is_empty() && lighter_orders == 0 =>
            {
                report.succeeded = true;
                report.final_position = Some(position);
                report.net_notional = Some(net_notional);
                report.aster_open_orders = Some(aster_orders.len());
                report.lighter_open_orders = Some(lighter_orders);
                warn!(
                    "missing hedge retry succeeded attempt={} venue={} final_aster={} final_lighter={} net_notional=${}",
                    attempt_no,
                    plan.venue.as_str(),
                    position.aster_qty,
                    position.lighter_qty,
                    net_notional
                );
                return report;
            }
            (Ok((position, net_notional)), Ok(aster_orders), Ok(lighter_orders)) => {
                current_position = position;
                report.final_position = Some(position);
                report.net_notional = Some(net_notional);
                report.aster_open_orders = Some(aster_orders.len());
                report.lighter_open_orders = Some(lighter_orders);
                report.error = Some(format!(
                    "hedge retry left open orders: aster={} lighter={}",
                    aster_orders.len(),
                    lighter_orders
                ));
            }
            (reconciled, open_a, open_l) => {
                if let Ok(Ok(position)) = tokio::time::timeout_at(deadline,
                    reconcile_positions(&spec.market_id, aster, lighter)).await {
                    current_position = position;
                    report.final_position = Some(position);
                }
                report.error = Some(format!(
                    "hedge retry verification failed: reconciled={:?} aster_open={:?} lighter_open={:?}",
                    reconciled.as_ref().map(|(_, notional)| *notional),
                    open_a.as_ref().map(|orders| orders.len()),
                    open_l
                ));
            }
        }
    }
    report
}

fn hedge_retry_report_json(report: Option<&HedgeRetryReport>) -> serde_json::Value {
    let Some(report) = report else {
        return serde_json::Value::Null;
    };
    let attempts: Vec<_> = report
        .attempts
        .iter()
        .map(|attempt| {
            serde_json::json!({
                "identity": attempt.identity,
                "fee_evidence": attempt.fee_evidence,
                "attempt": attempt.attempt,
                "venue": attempt.venue.as_str(),
                "side": attempt.side.as_str(),
                "qty": attempt.qty,
                "price_bound": attempt.price_bound,
                "reduce_only": attempt.reduce_only,
                "submit_result": attempt.submit_result,
                "fill": attempt.fill,
                "fill_status": attempt.fill_status,
                "error": attempt.error,
            })
        })
        .collect();
    serde_json::json!({
        "attempted": report.attempted,
        "succeeded": report.succeeded,
        "slippage_bps": report.slippage_bps,
        "attempts": attempts,
        "final_position": report.final_position.map(|position| serde_json::json!({
            "aster_qty": position.aster_qty,
            "lighter_qty": position.lighter_qty,
            "net_qty": position.net_qty(),
        })),
        "net_notional": report.net_notional,
        "aster_open_orders": report.aster_open_orders,
        "lighter_open_orders": report.lighter_open_orders,
        "error": report.error,
    })
}


async fn wait_post_trade_reconciled_for(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &OtherLeg,
    opp: &Opportunity,
    timeout: Duration,
) -> std::result::Result<(PositionSnapshot, Decimal), ExecutionError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let poll = Duration::from_millis(cfg.risk.min_reconcile_interval_ms.max(250));
    loop {
        tokio::time::sleep(poll).await;
        // A transient position-query failure must NOT abort as a generic error (which is
        // excluded from recovery): retry within the deadline, and if the venue still can't
        // be read, classify as Unreconciled so the rescue path runs — the trade may have
        // filled and the positions are simply unverified.
        let pos = match reconcile_positions(&spec.market_id, aster, lighter).await {
            Ok(pos) => pos,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ExecutionError::Unreconciled {
                        details: format!(
                            "post-trade position query kept failing (positions unverified): {e:#}"
                        ),
                    });
                }
                warn!("post-trade position query failed; retrying: {e:#}");
                continue;
            }
        };
        let net_notional = pos.net_qty().abs() * opp.ref_px;
        if net_notional <= cfg.risk.max_position_mismatch_usd {
            return Ok((pos, net_notional));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ExecutionError::Unreconciled {
                details: format!(
                    "post-trade residual too large after wait: aster={} lighter={} net_notional=${}",
                    pos.aster_qty, pos.lighter_qty, net_notional
                ),
            });
        }
    }
}

async fn recover_if_needed(
    cfg: &Config, spec: &MarketSpec, aster: &AsterRest, lighter: &OtherLeg,
    http: &reqwest::Client, margin_before: MarginSnapshot, session: &ActiveSession,
    journal: &ExecutionJournal,
) -> Result<RecoveryReport> {
    let execution_id = next_execution_id();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut orders = Vec::new();
    let mut action_taken = false;
    let mut in_flight = false;
    let mut baseline = None;
    let result = tokio::time::timeout_at(deadline, async {
        for attempt in 0..=3 {
            let (position, a_book, l_book) = tokio::join!(
                reconcile_positions(&spec.market_id, aster, lighter),
                rest_book::fetch_aster_book(http, &cfg.venues.aster_base_url, &spec.aster_symbol, 20),
                async { match &*lighter {
                    OtherLeg::Lighter(_) => rest_book::fetch_lighter_book(http, &cfg.venues.lighter_base_url, spec.lighter_market_id, 20).await,
                    OtherLeg::Hyperliquid(_) => crate::connectors::rest_book::fetch_hyperliquid_book(http, &cfg.venues.hyperliquid_base_url, &spec.lighter_symbol)
                        .await.map(crate::taker::venues::hyperliquid::taker_book),
                } },
            );
            let position = position?;
            if baseline.is_none() { baseline = Some(position); }
            let (a_book, l_book) = (a_book?, l_book?);
            let mark = a_book.mid().context("recovery Aster mark unavailable")?;
            let l_mark = l_book.mid().context("recovery Lighter mark unavailable")?;
            anyhow::ensure!(mark > Decimal::ZERO && l_mark > Decimal::ZERO
                && !a_book.is_crossed() && !l_book.is_crossed(), "invalid recovery books");
            // Recovery removes the unhedged residual only; the hedged inventory stays open.
            let balanced = position.net_qty().abs() * mark <= cfg.risk.max_position_mismatch_usd;
            if balanced {
                let (a_open, l_open, margins) = tokio::join!(aster.open_orders(&spec.market_id),
                    lighter.rest_open_orders_count(&spec.market_id), reconcile_margins(aster, lighter));
                anyhow::ensure!(a_open?.is_empty() && l_open? == 0, "recovery has unverified/live open orders");
                let margins = margins?;
                return Ok::<_, anyhow::Error>(RecoveryReport { execution_id: execution_id.clone(), action_taken,
                    position, lighter_ws_qty: lighter.ws_position_qty(&spec.market_id).ok(),
                    margin_after: margins, estimated_loss_usdc: estimated_recovery_loss(margin_before, margins),
                    aster_open_orders: 0, lighter_open_orders: 0 });
            }
            anyhow::ensure!(attempt < 3, "recovery residual remains after three close attempts");
            let (side, a_qty, l_qty) = residual_close_qtys(position, spec.step, spec.lighter_qty_step);
            let (a_side, l_side) = (side, side);
            in_flight = true;
            let (a_result, l_result) = tokio::join!(
                async { if a_qty > Decimal::ZERO {
                    Some(aster.submit_ioc_order(&spec.market_id, a_side, a_qty,
                        emergency_close_bound(mark, a_side, cfg.arb.emergency_slippage_bps), true).await)
                } else { None } },
                async { if l_qty > Decimal::ZERO {
                    Some(lighter.submit_market_order_deferred_fill(&spec.market_id, l_side, l_qty,
                        emergency_close_bound(l_mark, l_side, cfg.arb.emergency_slippage_bps), true).await)
                } else { None } },
            );
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now()).min(Duration::from_secs(5));
            if let Some(outcome) = &a_result { orders.push(aster_order_identity(outcome, a_side, a_qty)); }
            if let Some((outcome, _)) = &l_result { orders.push(lighter_order_identity(outcome, l_side, l_qty)); }
            in_flight = false;
            session.record_unresolved(serde_json::json!({"schema_version":2,"economic_status":"incomplete",
                "execution_id":execution_id,"session_id":session.id(),"market":spec.market_id.to_string(),
                "outcome":"recovery_resolving","orders":orders,"orders_complete":true,
                "pre_positions":baseline.map(|position|serde_json::json!({"aster_qty":position.aster_qty,"lighter_qty":position.lighter_qty}))})).await?;
            action_taken |= a_result.is_some() || l_result.is_some();
            let (a_evidence, l_evidence) = tokio::join!(
                async { match &a_result {
                    Some(outcome) => resolve_aster_evidence(spec, aster, outcome, remaining).await,
                    None => LegEvidence::not_submitted(),
                } },
                async { match l_result {
                    Some((outcome, pending)) => resolve_lighter_evidence(spec, lighter, &outcome, pending, l_side, l_qty, remaining).await,
                    None => LegEvidence::not_submitted(),
                } },
            );
            if !a_evidence.terminal || !l_evidence.terminal {
                bail!("recovery close terminal evidence unavailable; no further close may be submitted");
            }
            // The next attempt sizes from REST positions: wait until they show these closes, or
            // a lagging read would close the same residual twice.
            let sign = if side == Side::Buy { Decimal::ONE } else { -Decimal::ONE };
            let expected = PositionSnapshot {
                aster_qty: position.aster_qty + sign * a_evidence.qty.unwrap_or(Decimal::ZERO),
                lighter_qty: position.lighter_qty + sign * l_evidence.qty.unwrap_or(Decimal::ZERO),
            };
            wait_position_evidence(cfg, spec, aster, lighter, expected, mark).await?;
        }
        unreachable!()
    }).await;
    match result {
        Ok(Ok(report)) => {
            append_execution_record(journal,serde_json::json!({ "schema_version": 2, "economic_status": "estimated",
                "timestamp": Utc::now(), "execution_id": execution_id, "session_id": session.id(),
                "market": spec.market_id.to_string(), "outcome": "recovery_complete", "orders": orders,
                "action_taken": action_taken, "estimated_loss_usdc": report.estimated_loss_usdc,
                "final_positions": {"aster_qty": report.position.aster_qty, "lighter_rest_qty": report.position.lighter_qty} })).await?;
            Ok(report)
        }
        result => {
            let detail = match result {
                Ok(Err(error)) => format!("{error:#}"),
                Err(_) => "recovery exceeded thirty seconds".to_string(),
                _ => unreachable!(),
            };
            let evidence = serde_json::json!({ "schema_version": 2, "economic_status": "incomplete",
                "timestamp": Utc::now(), "execution_id": execution_id, "session_id": session.id(),
                "market": spec.market_id.to_string(), "outcome": "recovery_unresolved", "orders": orders,
                "orders_complete": !in_flight, "pre_positions": baseline.map(|position| serde_json::json!({
                    "aster_qty":position.aster_qty,"lighter_qty":position.lighter_qty})), "error": detail });
            session.record_unresolved(evidence.clone()).await?;
            append_execution_record(journal,evidence).await?;
            bail!("{detail}")
        }
    }
}

fn session_marked_loss(baseline: Decimal, current: Decimal, limit: Decimal) -> Option<Decimal> {
    let loss = baseline-current;
    (loss >= limit).then_some(loss)
}

/// Estimated realized loss across a recovery window. Prefers the total-equity delta:
/// closing a position RELEASES available margin, so the available-only delta can report
/// zero (or a gain) while a loss was realized. Falls back to the available-margin delta
/// when either snapshot lacks equity. Floored at zero — this feeds loss breakers and must
/// never book phantom gains.
fn estimated_recovery_loss(before: MarginSnapshot, after: MarginSnapshot) -> Decimal {
    let delta = match (before.total_equity_usd(), after.total_equity_usd()) {
        (Some(before_eq), Some(after_eq)) => before_eq - after_eq,
        _ => {
            (before.aster_available_usd + before.lighter_available_usd)
                - (after.aster_available_usd + after.lighter_available_usd)
        }
    };
    delta.max(Decimal::ZERO)
}

/// Reduce-only quantities that close a net cross-venue residual without touching the hedged
/// inventory. Only a venue holding the residual's sign can reduce it: Aster first (as in the
/// XEMM correction), Lighter takes any remainder; each is floored to its step, so a sub-step
/// remainder stays as dust inside the mismatch tolerance. Temporary twin of XEMM's
/// `dispatch_correction` sizing until the venue layers merge.
fn residual_close_qtys(position: PositionSnapshot, aster_step: Decimal, lighter_step: Decimal) -> (Side, Decimal, Decimal) {
    let net = position.net_qty();
    let side = if net > Decimal::ZERO { Side::Sell } else { Side::Buy };
    let same_sign = |qty: Decimal| qty != Decimal::ZERO && (qty > Decimal::ZERO) == (net > Decimal::ZERO);
    let a_qty = if same_sign(position.aster_qty) {
        floor_to_step(net.abs().min(position.aster_qty.abs()), aster_step)
    } else { Decimal::ZERO };
    let l_qty = if same_sign(position.lighter_qty) {
        floor_to_step((net.abs() - a_qty).min(position.lighter_qty.abs()), lighter_step)
    } else { Decimal::ZERO };
    (side, a_qty, l_qty)
}

/// Marketable IOC price bound for an emergency reduce-only close: cross the spread by
/// `bps` past the venue's mark so the order executes, while capping the worst fill.
fn emergency_close_bound(mark: Decimal, side: Side, bps: Decimal) -> Decimal {
    match side {
        Side::Buy => mark * (Decimal::ONE + bps_to_rate(bps)),
        Side::Sell => mark * (Decimal::ONE - bps_to_rate(bps)),
    }
}

async fn reconcile_positions(
    market: &MarketId,
    aster: &AsterRest,
    lighter: &OtherLeg,
) -> Result<PositionSnapshot> {
    let (a, l) = tokio::join!(
        aster.position_qty(market),
        lighter.rest_position_qty(market)
    );
    Ok(PositionSnapshot {
        aster_qty: a?,
        lighter_qty: l?,
    })
}

async fn refresh_account_snapshot(
    market: &MarketId,
    aster: &AsterRest,
    lighter: &OtherLeg,
    execution_epoch: &AtomicU64,
) -> Result<AccountSnapshot> {
    let epoch = execution_epoch.load(Ordering::Acquire);
    anyhow::ensure!(epoch % 2 == 0, "execution active; account refresh deferred");
    let observed_at = tokio::time::Instant::now();
    let (aster_pos, aster_balance, lighter_account, aster_orders, lighter_orders) = tokio::join!(
        aster.position_qty(market),
        aster.balance_snapshot(),
        lighter.account_snapshot(market),
        aster.open_orders(market),
        lighter.rest_open_orders_count(market)
    );
    anyhow::ensure!(execution_epoch.load(Ordering::Acquire) == epoch,
        "execution changed while account query was in flight");
    let lighter_account = lighter_account?;
    let aster_balance = aster_balance?;
    let position = PositionSnapshot {
        aster_qty: aster_pos?,
        lighter_qty: lighter_account.position_qty,
    };
    let lighter_ws_qty = lighter.ws_position_qty(market).ok();
    let lighter_ws_rest_divergence_qty = lighter_ws_qty.map(|ws| (ws - position.lighter_qty).abs());
    let margins = MarginSnapshot {
        aster_available_usd: aster_balance.available_usd,
        lighter_available_usd: lighter_account.available_usdc,
        aster_equity_usd: aster_balance.equity_usd(),
        lighter_equity_usd: lighter_account.equity_usdc(),
    };
    Ok(AccountSnapshot {
        execution_epoch: epoch,
        aster_open_orders: aster_orders?.len(),
        lighter_open_orders: lighter_orders?,
        position,
        lighter_ws_qty,
        lighter_ws_rest_divergence_qty,
        margins,
        refreshed_at: observed_at,
    })
}

fn spawn_account_snapshot_refresher(
    cfg: &Config,
    market: MarketId,
    aster: Arc<AsterRest>,
    lighter: Arc<OtherLeg>,
    tx: watch::Sender<AccountSnapshot>,
    execution_epoch: Arc<AtomicU64>,
    refresh_now: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    let refresh_ms = (cfg.live.max_account_snapshot_age_ms as u64 / 2).max(10_000);
    let retry_ms = cfg.risk.min_reconcile_interval_ms.max(5_000);
    let rate_limit_retry_ms = 30_000u64;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(refresh_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last = tokio::time::Instant::now();
        loop {
            // A forced refresh (scan loop stuck on a mismatch) skips the wait; the
            // pause check below still applies, so recovery windows also suppress
            // forced refreshes — a stored permit consumed post-unpause is harmless.
            // The scan asks every 500 ms while a mismatch lasts, so forced reads come at
            // least 5 s apart: each costs two of the IP's 60 Lighter REST calls a minute.
            tokio::select! {
                _ = tick.tick() => {}
                _ = refresh_now.notified() => tokio::time::sleep_until(last + Duration::from_secs(5)).await,
            }
            if execution_epoch.load(Ordering::Acquire) % 2 != 0 {
                continue;
            }
            last = tokio::time::Instant::now();
            match refresh_account_snapshot(&market, &aster, &lighter, &execution_epoch).await {
                Ok(snapshot) => {
                    // Re-check the pause flag before publishing: a refresh already in
                    // flight when the executor set paused=true would otherwise
                    // overwrite the fresh post-trade snapshot with pre-trade positions
                    // stamped refreshed_at=now, defeating the staleness gate for up to
                    // a full refresh interval.
                    if execution_epoch.load(Ordering::Acquire) % 2 != 0 {
                        continue;
                    }
                    debug!(
                        "cold account snapshot refreshed: age_ms=0 aster_pos={} lighter_pos={} lighter_ws_pos={:?} lighter_rest_ws_divergence_qty={:?} aster_available_usd={} lighter_available_usd={}",
                        snapshot.position.aster_qty,
                        snapshot.position.lighter_qty,
                        snapshot.lighter_ws_qty,
                        snapshot.lighter_ws_rest_divergence_qty,
                        snapshot.margins.aster_available_usd,
                        snapshot.margins.lighter_available_usd
                    );
                    if tx.is_closed() { break; }
                    publish_account(&tx, snapshot);
                }
                Err(e) => {
                    let sleep_ms = if is_rate_limit_error(&e) {
                        rate_limit_retry_ms
                    } else {
                        retry_ms
                    };
                    warn!("cold account snapshot refresh failed: {e:#}; retrying in {sleep_ms}ms");
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
            }
        }
    })
}

fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
            == Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
            || cause.to_string().contains("429 Too Many Requests")
    })
}

async fn reconcile_margins(aster: &AsterRest, lighter: &OtherLeg) -> Result<MarginSnapshot> {
    // Same endpoints as the available-only reads (Aster /fapi/v3/balance, Lighter
    // account payload), so carrying equity costs no extra REST calls.
    let (a, l) = tokio::join!(aster.balance_snapshot(), lighter.rest_margin_snapshot());
    let (a, l) = (a?, l?);
    Ok(MarginSnapshot {
        aster_available_usd: a.available_usd,
        lighter_available_usd: l.available_usdc,
        aster_equity_usd: a.equity_usd(),
        lighter_equity_usd: l.equity_usdc,
    })
}

async fn ensure_clean_start(
    cfg: &Config,
    spec: &MarketSpec,
    aster_books: &AsterBookFeed,
    aster: &AsterRest,
    lighter: &OtherLeg,
    observe_only: bool,
) -> Result<()> {
    let pos = reconcile_positions(&spec.market_id, aster, lighter).await?;
    let lighter_ws_qty = lighter.ws_position_qty(&spec.market_id).ok();
    let (aster_book, lighter_book) = fetch_books(spec, aster_books, lighter)?;
    let open_a = aster.open_orders(&spec.market_id).await?;
    let open_l = lighter.open_orders_count(&spec.market_id).await?;
    if !open_a.is_empty() || open_l > 0 {
        if observe_only {
            warn!(
                "observe-only start: existing open orders present; continuing without order submission: Aster open_orders={} Lighter open_orders={}",
                open_a.len(),
                open_l
            );
        } else {
            bail!(
                "clean-start failed: Aster open_orders={} Lighter open_orders={}",
                open_a.len(),
                open_l
            );
        }
    }
    let mark = aster_book
        .mid()
        .or_else(|| lighter_book.mid())
        .unwrap_or(Decimal::ONE);
    let mismatch = pos.net_qty().abs() * mark;
    if let Some(ws_qty) = lighter_ws_qty {
        let divergence_notional = (ws_qty - pos.lighter_qty).abs() * mark;
        if divergence_notional > cfg.risk.max_position_mismatch_usd {
            bail!(
                "clean-start failed: Lighter REST/WS position divergence rest={} ws={} divergence_notional=${}",
                pos.lighter_qty,
                ws_qty,
                divergence_notional
            );
        }
    }
    if mismatch > cfg.risk.max_position_mismatch_usd {
        if observe_only {
            // A read-only start (observe-only, or waiting for XEMM's lease) must tolerate
            // XEMM's transient residuals: bailing here would halt `run` exactly while
            // XEMM is mid-hedge.
            warn!(
                "observe-only start: positions not balanced (likely the active bot's transient); continuing without order submission: aster={} lighter={} net={}",
                pos.aster_qty,
                pos.lighter_qty,
                pos.net_qty()
            );
        } else {
            bail!(
                "clean-start failed: positions not balanced aster={} lighter={} net={}",
                pos.aster_qty,
                pos.lighter_qty,
                pos.net_qty()
            );
        }
    }
    info!(
        "clean start confirmed: aster={} lighter_rest={} lighter_ws={:?} aster_open_orders={} lighter_open_orders={} observe_only={}",
        pos.aster_qty,
        pos.lighter_qty,
        lighter_ws_qty,
        open_a.len(),
        open_l,
        observe_only
    );
    Ok(())
}

fn net_mismatch_notional(
    pos: PositionSnapshot,
    aster: &OrderBook,
    lighter: &OrderBook,
) -> Option<Decimal> {
    let mark_f = aster
        .mid_f64()
        .or_else(|| lighter.mid_f64())
        ?;
    let net_f = decimal_to_f64(pos.aster_qty)? + decimal_to_f64(pos.lighter_qty)?;
    Some(f64_to_dec(net_f.abs() * mark_f))
}

fn log_scan_state(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionSnapshot,
    margins: MarginSnapshot,
) {
    let Some(a_bid) = aster.best_bid() else {
        return;
    };
    let Some(a_ask) = aster.best_ask() else {
        return;
    };
    let Some(l_bid) = lighter.best_bid() else {
        return;
    };
    let Some(l_ask) = lighter.best_ask() else {
        return;
    };
    let Some(ref_px) = aster.mid().or_else(|| lighter.mid()) else {
        return;
    };
    let edge_sell_aster = (a_bid.px - l_ask.px) / ref_px * Decimal::from(10_000);
    let edge_sell_lighter = (l_bid.px - a_ask.px) / ref_px * Decimal::from(10_000);
    debug!(
        "arb scan market={} a_bid={}x{} a_ask={}x{} l_bid={}x{} l_ask={}x{} edge_sell_aster={}bps edge_sell_lighter={}bps required={}bps pos_aster={} pos_lighter={} net_pos={} margin_aster=${} margin_lighter=${}",
        spec.market_id,
        a_bid.px,
        a_bid.qty,
        a_ask.px,
        a_ask.qty,
        l_bid.px,
        l_bid.qty,
        l_ask.px,
        l_ask.qty,
        edge_sell_aster,
        edge_sell_lighter,
        cfg.arb.required_gross_edge_bps(),
        pos.aster_qty,
        pos.lighter_qty,
        pos.net_qty(),
        margins.aster_available_usd,
        margins.lighter_available_usd
    );
}

fn aster_price_bound(opp: &Opportunity, slippage_bps: Decimal) -> Decimal {
    let rate = bps_to_rate(slippage_bps);
    match opp.direction.aster_side() {
        Side::Buy => opp.buy_px * (Decimal::ONE + rate),
        Side::Sell => opp.sell_px * (Decimal::ONE - rate),
    }
}

fn lighter_price_bound(opp: &Opportunity, slippage_bps: Decimal) -> Decimal {
    let rate = bps_to_rate(slippage_bps);
    match opp.direction.lighter_side() {
        Side::Buy => opp.buy_px * (Decimal::ONE + rate),
        Side::Sell => opp.sell_px * (Decimal::ONE - rate),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::taker::config::{ArbCfg, LiveCfg, PnlCfg, RiskCfg, VenueCfg};
    use rust_decimal_macros::dec;

    #[test]
    fn a_missing_book_warns_once_then_every_five_seconds() {
        let (t0, ms) = (tokio::time::Instant::now(), Duration::from_millis);
        let mut outage = BookOutage::default();
        assert_eq!(outage.failed(t0), Some(Duration::ZERO), "the first failure warns");
        assert_eq!(outage.failed(t0 + ms(10)), None, "retries every poll interval stay quiet");
        assert_eq!(outage.failed(t0 + ms(5_000)), Some(ms(5_000)), "a reminder every 5 s");
        assert_eq!(outage.failed(t0 + ms(5_010)), None);
        assert_eq!(outage.recovered(t0 + ms(9_000)), Some(ms(9_000)), "the recovery gives the length");
        assert_eq!(outage.recovered(t0 + ms(9_010)), None, "only once");
        assert_eq!(outage.failed(t0 + ms(9_020)), Some(Duration::ZERO), "a new outage warns at once");
    }

    #[test]
    fn scan_skip_requires_same_books_same_generation_and_recent_full_eval() {
        let ts = Utc::now();
        let book = |px: i64| {
            Arc::new(OrderBook::from_levels(
                vec![(Decimal::from(px), dec!(1))],
                vec![(Decimal::from(px + 1), dec!(1))],
                ts,
                ts,
            ))
        };
        let (a, l) = (book(100), book(100));
        let floor = Duration::from_millis(250);
        let within = Duration::from_millis(10);

        // No prior full evaluation: never skip.
        assert!(!scan_inputs_unchanged(&None, &a, &l, 0, within, floor));

        let last = Some((a.clone(), l.clone(), 0u64));
        // Identical inputs within the floor: skip.
        assert!(scan_inputs_unchanged(&last, &a, &l, 0, within, floor));
        // Either book replaced (fresh Arc, even with identical contents): no skip.
        assert!(!scan_inputs_unchanged(&last, &book(100), &l, 0, within, floor));
        assert!(!scan_inputs_unchanged(&last, &a, &book(100), 0, within, floor));
        // New account snapshot generation: no skip.
        assert!(!scan_inputs_unchanged(&last, &a, &l, 1, within, floor));
        // Floor exceeded: full pass regardless (lease pickup / gate sampling bound).
        assert!(!scan_inputs_unchanged(&last, &a, &l, 0, floor, floor));
    }

    fn test_cfg() -> Config {
        Config {
            arb: ArbCfg {
                entry_gate: crate::taker::config::EntryGateCfg {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            pnl: PnlCfg::default(),
            live: LiveCfg::default(),
            venues: VenueCfg::default(),
            risk: RiskCfg::default(),
            markets: vec![],
        }
    }

    fn test_spec() -> MarketSpec {
        MarketSpec {
            market_id: MarketId("HYPE".to_string()),
            aster_symbol: "HYPEUSDT".to_string(),
            lighter_symbol: "HYPE".to_string(),
            lighter_market_id: 24,
            lighter_price_decimals: 4,
            lighter_size_decimals: 2,
            lighter_price_tick: dec!(0.0001), hedge: Default::default(),
            tick: dec!(0.001),
            step: dec!(0.01),
            aster_min_qty: dec!(0.01),
            aster_min_notional: dec!(10),
            lighter_qty_step: dec!(0.01),
            lighter_min_notional: dec!(10),
        }
    }

    fn book(bid: Decimal, ask: Decimal) -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels([(bid, dec!(10))], [(ask, dec!(10))], now, now)
    }

    fn depth_book(
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
    ) -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels(bids, asks, now, now)
    }

    fn margins() -> MarginSnapshot {
        MarginSnapshot {
            aster_available_usd: dec!(1000),
            lighter_available_usd: dec!(1000),
            aster_equity_usd: None,
            lighter_equity_usd: None,
        }
    }

    fn test_math(cfg: &Config, spec: &MarketSpec) -> MarketMathF64 {
        MarketMathF64::from_config_spec(cfg, spec).unwrap()
    }

    fn pos_f64(pos: PositionSnapshot) -> PositionF64 {
        PositionF64::from_snapshot(pos).unwrap()
    }

    fn margins_f64(margins: MarginSnapshot) -> MarginF64 {
        MarginF64::from_snapshot(margins).unwrap()
    }

    fn retry_opp(direction: Direction) -> Opportunity {
        Opportunity {
            direction,
            qty: dec!(0.20),
            qty_f64: 0.20,
            gross_edge_bps: dec!(9),
            expected_net_margin_bps: dec!(3),
            sell_px: dec!(63.8000),
            buy_px: dec!(63.7200),
            ref_px: dec!(63.7600),
            top_depth_qty: dec!(0.20),
            depth_guard_enabled: true,
            liquidity_multiple: dec!(10),
            depth_supported_qty: dec!(0.20),
            sell_depth_target_qty: dec!(2.00),
            buy_depth_target_qty: dec!(2.00),
            sell_depth_available_qty: dec!(2.00),
            buy_depth_available_qty: dec!(2.00),
            sell_depth_worst_px: dec!(63.8000),
            buy_depth_worst_px: dec!(63.7200),
            sell_depth_levels_used: 1,
            buy_depth_levels_used: 1,
            sell_best_px: dec!(63.8000),
            buy_best_px: dec!(63.7200),
            sell_best_qty: dec!(2.00),
            buy_best_qty: dec!(2.00),
            desired_qty: dec!(0.20),
            min_qty: dec!(0.20),
            headroom_qty: dec!(10),
            margin_room_qty: dec!(10),
            expected_gross_usd: dec!(0.016),
            expected_fee_usd: dec!(0.005),
            expected_net_usd: dec!(0.011),
            required_margin_usd: dec!(0.0025),
        }
    }

    #[test]
    fn actual_economics_books_only_matched_qty() {
        // Unequal legs: sell 0.20 @ 63.80, buy 0.16 @ 63.72. The naive notional difference
        // (12.76 − 10.1952 = +2.56) would book the unhedged 0.04 residual as phantom
        // profit; real matched PnL is 0.16 × (63.80 − 63.72) = 0.0128.
        let cfg = test_cfg();
        let opp = retry_opp(Direction::SellAsterBuyLighter);
        let aster_fill = FillSummary {
            qty: dec!(0.20),
            vwap: dec!(63.80),
            notional: dec!(12.76),
            fee_usd: dec!(0.005),
            fee_provenance: FeeProvenance::Venue,
        };
        let lighter_fill = FillSummary {
            qty: dec!(0.16),
            vwap: dec!(63.72),
            notional: dec!(10.1952),
            fee_usd: dec!(0),
            fee_provenance: FeeProvenance::Venue,
        };
        let e = actual_economics(&cfg, &opp, aster_fill, lighter_fill).expect("within tolerance");
        assert_eq!(e.gross_usd, dec!(0.0128));
        assert_eq!(e.net_usd, dec!(0.0078));
        assert_eq!(e.residual_qty, dec!(0.04));
        assert_eq!(e.residual_notional_usd, dec!(0.04) * opp.ref_px);
    }

    #[test]
    fn headroom_allows_reducing_then_flipping() {
        let pos = PositionSnapshot {
            aster_qty: dec!(1),
            lighter_qty: dec!(-1),
        };
        let q = max_qty_by_headroom_f64(2.0, pos.aster_qty.to_f64().unwrap(), pos.lighter_qty.to_f64().unwrap(), -1.0, 1.0);
        assert_eq!(q, 3.0);
    }

    #[test]
    fn f64_common_step_snaps_near_integer_units() {
        let cfg = test_cfg();
        let mut spec = test_spec();
        spec.step = dec!(0.1);
        spec.lighter_qty_step = dec!(0.1);
        let math = test_math(&cfg, &spec);
        let floored = floor_to_common_step_f64(0.3, &math);
        assert!(
            (floored - 0.3).abs() < 1e-12,
            "floor should snap 0.3 to the 0.1 grid, got {floored:?}"
        );
        let ceiled = ceil_to_common_step_f64(0.30000000000000004, &math);
        assert!(
            (ceiled - 0.3).abs() < 1e-12,
            "ceil should not jump f64-noisy 0.3 to 0.4, got {ceiled:?}"
        );
    }

    #[test]
    fn reduce_filter_selects_reducing_opportunity() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: dec!(-1),
            lighter_qty: dec!(1),
        };
        let opp = best_opportunity(
            &cfg,
            &spec,
            &math,
            &book(dec!(99), dec!(100)),
            &book(dec!(101), dec!(101.1)),
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Reduce,
        )
        .expect("reduce opportunity should be selected");
        assert_eq!(opp.direction, Direction::SellLighterBuyAster);
        assert_eq!(opp.qty, dec!(0.13));
    }

    #[test]
    fn reduce_filter_ignores_increasing_opportunity() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: dec!(-1),
            lighter_qty: dec!(1),
        };
        let aster = book(dec!(101), dec!(101.1));
        let lighter = book(dec!(100), dec!(100.1));
        let any = best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .expect("increasing opportunity exists");
        assert_eq!(any.direction, Direction::SellAsterBuyLighter);
        assert_eq!(any.qty, dec!(0.12));
        assert!(best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Reduce,
        )
        .is_none());
    }

    #[test]
    fn depth_guard_rejects_profitable_but_thin_top_of_book() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let aster = depth_book(
            [(dec!(101), dec!(0.20)), (dec!(99), dec!(10))],
            [(dec!(103), dec!(10))],
        );
        let lighter = depth_book(
            [(dec!(98), dec!(10))],
            [(dec!(100), dec!(0.20)), (dec!(102), dec!(10))],
        );
        assert!(best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .is_none());

        let mut top_only_cfg = cfg.clone();
        top_only_cfg.arb.depth_guard.enabled = false;
        let top_only_math = test_math(&top_only_cfg, &spec);
        assert!(best_opportunity(
            &top_only_cfg,
            &spec,
            &top_only_math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .is_some());
    }

    #[test]
    fn opportunity_profitability_uses_depth_vwap() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let aster = depth_book(
            [(dec!(101), dec!(0.20)), (dec!(100.90), dec!(10))],
            [(dec!(103), dec!(10))],
        );
        let lighter = depth_book(
            [(dec!(98), dec!(10))],
            [(dec!(100), dec!(0.20)), (dec!(100.10), dec!(10))],
        );
        let opp = best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .expect("depth-priced opportunity should remain profitable");
        assert_eq!(opp.direction, Direction::SellAsterBuyLighter);
        assert!(opp.sell_px < dec!(101));
        assert!(opp.buy_px > dec!(100));
        assert_eq!(opp.sell_depth_target_qty, opp.qty * dec!(10));
        assert_eq!(opp.buy_depth_target_qty, opp.qty * dec!(10));
        assert_eq!(opp.sell_depth_levels_used, 2);
        assert_eq!(opp.buy_depth_levels_used, 2);
        assert!(opp.gross_edge_bps < dec!(100));
        assert!(opp.gross_edge_bps >= cfg.arb.required_gross_edge_bps());
    }

    #[test]
    fn production_edge_filter_accepts_and_rejects_both_sides_of_fee_floor() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        // test_cfg requires 4 + 0 + 2 = 6 bps; ref = aster mid = s + 0.1, so
        // (s - 100)/(s + 0.1) * 1e4 >= 6 <=> s >= 100.00006/0.9994 = 100.0600961:
        // tick 0 (s = 100.0601) is the first accepted step, tick -1 the last rejected.
        for tick in -20..=20 {
            let sell = dec!(100.0601) + Decimal::from(tick) * dec!(0.00001);
            let result = best_opportunity(&cfg,&spec,&math,
                &book(sell,sell+dec!(0.2)), &book(dec!(98),dec!(100)),
                PositionF64 {aster_qty:0.0,lighter_qty:0.0},margins_f64(margins()),false,ExposureFilter::Any);
            assert_eq!(result.map(|o| o.direction), (tick >= 0).then_some(Direction::SellAsterBuyLighter), "sell={sell}");
        }
    }

    #[test]
    fn cached_execution_rights_require_fresh_validated_epoch_and_unexpired_lease() {
        let spec = test_spec();
        let now = Utc::now();
        let epoch = Arc::new(AtomicU64::new(0));
        let state = ControlSnapshot { lease: Some(ExecutionLease { market: "HYPE".to_string(),
            lease_id: "lease-1".to_string(), expires_at: now + chrono::Duration::seconds(60) }),
            observed_at: tokio::time::Instant::now(), validated_epoch: Some(0) };
        let (tx, rx) = watch::channel(state.clone());
        let mut cache = LeaseCache { rx, execution_epoch: epoch.clone(), task: None };
        let (_grants, lease_rx) = watch::channel(None::<ExecutionLease>);
        let mut options = RunOptions { lease: Some(lease_rx), ..RunOptions::default() };
        assert!(execution_lease_enabled(&mut cache, &options, &spec, now, false).0);
        options.observe_only = true;
        assert!(!execution_lease_enabled(&mut cache, &options, &spec, now, false).0);
        options.observe_only = false;
        assert!(!execution_lease_enabled(&mut cache, &options, &spec, now + chrono::Duration::seconds(60), false).0);
        begin_execution(&epoch);
        finish_execution(&epoch);
        assert!(!execution_lease_enabled(&mut cache, &options, &spec, now, false).0);
        let mut fresh = state.clone();
        fresh.validated_epoch = Some(2);
        tx.send(fresh.clone()).unwrap();
        assert!(execution_lease_enabled(&mut cache, &options, &spec, now, false).0);
        fresh.observed_at = tokio::time::Instant::now() - CONTROL_MAX_AGE - Duration::from_millis(1);
        tx.send(fresh).unwrap();
        assert!(!execution_lease_enabled(&mut cache, &options, &spec, now, false).0);
    }

    #[test]
    fn stale_pretrade_account_cannot_replace_completed_execution() {
        let snapshot = |epoch, qty| AccountSnapshot { execution_epoch: epoch, aster_open_orders: 0,
            lighter_open_orders: 0, position: PositionSnapshot { aster_qty: qty, lighter_qty: -qty },
            lighter_ws_qty: None, lighter_ws_rest_divergence_qty: None, margins: margins(),
            refreshed_at: tokio::time::Instant::now() };
        let (tx, rx) = watch::channel(snapshot(0, dec!(0)));
        publish_account(&tx, snapshot(2, dec!(1)));
        // The old request completes later, so a timestamp-only guard would accept it.
        publish_account(&tx, snapshot(0, dec!(0)));
        assert_eq!(rx.borrow().execution_epoch, 2);
        assert_eq!(rx.borrow().position.aster_qty, dec!(1));
    }

    #[test]
    fn hedge_retry_plan_completes_missing_lighter_buy() {
        let cfg = test_cfg();
        let spec = test_spec();
        let opp = retry_opp(Direction::SellAsterBuyLighter);
        let pre = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.60),
        };
        let current = PositionSnapshot {
            aster_qty: dec!(-0.80),
            lighter_qty: dec!(0.60),
        };
        let plan = hedge_retry_plan(&cfg, &spec, &opp, pre, current).unwrap();
        assert_eq!(plan.venue, HedgeRetryVenue::Lighter);
        assert_eq!(plan.side, Side::Buy);
        assert_eq!(plan.qty, dec!(0.20));
        assert_eq!(plan.price_bound, dec!(63.9111600));
    }

    #[test]
    fn hedge_retry_plan_completes_missing_aster_buy() {
        let cfg = test_cfg();
        let spec = test_spec();
        let opp = retry_opp(Direction::SellLighterBuyAster);
        let pre = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.60),
        };
        let current = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.40),
        };
        let plan = hedge_retry_plan(&cfg, &spec, &opp, pre, current).unwrap();
        assert_eq!(plan.venue, HedgeRetryVenue::Aster);
        assert_eq!(plan.side, Side::Buy);
        assert_eq!(plan.qty, dec!(0.20));
        assert_eq!(plan.price_bound, dec!(63.9111600));
    }

    #[test]
    fn hedge_retry_plan_skips_ambiguous_two_sided_miss() {
        let cfg = test_cfg();
        let spec = test_spec();
        let opp = retry_opp(Direction::SellAsterBuyLighter);
        let pre = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.60),
        };
        assert!(hedge_retry_plan(&cfg, &spec, &opp, pre, pre).is_none());
    }

    #[test]
    fn recovered_failure_tracker_trips_count_limit() {
        let mut cfg = test_cfg();
        cfg.arb.max_recovered_failures_per_hour = 2;
        let mut tracker = RecoveredFailureTracker::default();
        assert!(tracker.record(dec!(0.01), &cfg).is_none());
        assert!(tracker.record(dec!(0.01), &cfg).is_none());
        let reason = tracker
            .record(dec!(0.01), &cfg)
            .expect("third recovered failure should trip count breaker");
        assert!(reason.contains("count"));
        assert_eq!(tracker.event_count(), 3);
    }

    #[test]
    fn recovered_failure_tracker_trips_loss_limit() {
        let mut cfg = test_cfg();
        cfg.arb.max_recovered_loss_usdc_per_hour = dec!(0.02);
        let mut tracker = RecoveredFailureTracker::default();
        assert!(tracker.record(dec!(0.01), &cfg).is_none());
        let reason = tracker
            .record(dec!(0.011), &cfg)
            .expect("loss sum above limit should trip loss breaker");
        assert!(reason.contains("loss"));
        assert_eq!(tracker.event_count(), 2);
    }

    #[test]
    fn depth_sizing_matches_hand_calculated_two_level_cashflows() {
        let mut cfg = test_cfg();
        cfg.arb.desired_notional = dec!(104);
        cfg.arb.depth_guard.liquidity_multiple = dec!(2);
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let aster = depth_book([(dec!(103),dec!(0.5)),(dec!(102),dec!(5))],[(dec!(105),dec!(5))]);
        let lighter = depth_book([(dec!(99),dec!(5))],[(dec!(100),dec!(0.5)),(dec!(101),dec!(5))]);
        let opp = best_opportunity(&cfg, &spec, &math, &aster, &lighter,
            PositionF64 { aster_qty:0.0,lighter_qty:0.0 }, margins_f64(margins()), false, ExposureFilter::Any).unwrap();
        assert_eq!(opp.qty, dec!(1));
        assert_eq!(opp.sell_depth_levels_used, 2);
        assert_eq!(opp.buy_depth_levels_used, 2);
        assert_eq!(opp.sell_px, dec!(102.25)); // (0.5*103 + 1.5*102) / 2
        assert_eq!(opp.buy_px, dec!(100.75)); // (0.5*100 + 1.5*101) / 2
        assert!((opp.expected_net_usd - dec!(1.4591)).abs() < dec!(0.00000001));
    }

    #[test]
    fn minimum_size_uses_the_real_common_lattice() {
        let cfg = test_cfg();
        let mut spec = test_spec();
        spec.step = dec!(0.02);
        spec.lighter_qty_step = dec!(0.03);
        let math = test_math(&cfg, &spec);
        let opp = best_opportunity(&cfg, &spec, &math,
            &book(dec!(20),dec!(20.1)), &book(dec!(19.8),dec!(19.9)),
            PositionF64 { aster_qty:0.0,lighter_qty:0.0 }, margins_f64(margins()), true, ExposureFilter::Any).unwrap();
        assert_eq!(opp.qty, dec!(0.54));
        assert_eq!(opp.qty % spec.step, Decimal::ZERO);
        assert_eq!(opp.qty % spec.lighter_qty_step, Decimal::ZERO);
        assert!(dec!(0.48) * dec!(19.9) < spec.lighter_min_notional);
    }

    #[test]
    fn reduce_execution_caps_quantity_at_both_existing_positions() {
        let mut cfg = test_cfg();
        cfg.arb.desired_notional = dec!(100);
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let opp = best_opportunity(&cfg, &spec, &math,
            &book(dec!(99),dec!(100)), &book(dec!(101),dec!(102)),
            PositionF64 { aster_qty:-0.2,lighter_qty:0.2 }, margins_f64(margins()), false, ExposureFilter::Reduce).unwrap();
        assert_eq!(opp.qty, dec!(0.2));
        assert_eq!(opp.direction, Direction::SellLighterBuyAster);
    }

    #[test]
    fn exposure_effect_matches_known_position_changes() {
        let math = test_math(&test_cfg(), &test_spec());
        for (a,l,qty,expected) in [
            (1.0,-1.0,0.2,ExposureEffect::Reduce),
            (0.0,0.0,0.5,ExposureEffect::Increase),
            (2.0,-2.0,3.0,ExposureEffect::Reduce),
            (1.0,-1.0,2.0,ExposureEffect::Flat),
            (1.0,-1.0,0.0,ExposureEffect::Unknown),
        ] { assert_eq!(exposure_effect_f64(a,l,Direction::SellAsterBuyLighter,qty,&math), expected); }
    }

    #[test]
    fn exposure_effect_f64_treats_noisy_boundary_as_flat() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        assert_eq!(
            exposure_effect_f64(
                0.2,
                -0.1,
                Direction::SellAsterBuyLighter,
                0.3,
                &math,
            ),
            ExposureEffect::Flat
        );
    }

    #[test]
    fn net_mismatch_values_only_unhedged_quantity() {
        for (a,l,expected) in [(dec!(1),dec!(-1),dec!(0)),(dec!(0.5),dec!(-0.3),dec!(10.1))] {
            let result = net_mismatch_notional(PositionSnapshot { aster_qty:a,lighter_qty:l },
                &book(dec!(50),dec!(51)), &book(dec!(50),dec!(51))).unwrap();
            assert!((result - expected).abs() < dec!(0.0000001));
        }
    }

    fn margin_snapshot(
        aster_avail: Decimal,
        lighter_avail: Decimal,
        aster_eq: Option<Decimal>,
        lighter_eq: Option<Decimal>,
    ) -> MarginSnapshot {
        MarginSnapshot {
            aster_available_usd: aster_avail,
            lighter_available_usd: lighter_avail,
            aster_equity_usd: aster_eq,
            lighter_equity_usd: lighter_eq,
        }
    }

    #[test]
    fn estimated_recovery_loss_uses_equity_even_when_margin_release_masks_it() {
        // Closing the naked leg RELEASED margin (available rose 200 -> 240) while equity
        // dropped 300 -> 290: the loss is real and must be reported, not masked.
        let before = margin_snapshot(dec!(100), dec!(100), Some(dec!(180)), Some(dec!(120)));
        let after = margin_snapshot(dec!(140), dec!(100), Some(dec!(175)), Some(dec!(115)));
        assert_eq!(estimated_recovery_loss(before, after), dec!(10));
    }

    #[test]
    fn estimated_recovery_loss_falls_back_to_available_delta_without_equity() {
        let before = margin_snapshot(dec!(100), dec!(100), None, Some(dec!(120)));
        let after = margin_snapshot(dec!(97), dec!(100), None, Some(dec!(115)));
        assert_eq!(estimated_recovery_loss(before, after), dec!(3));
    }

    #[test]
    fn estimated_recovery_loss_floors_gains_at_zero() {
        let before = margin_snapshot(dec!(100), dec!(100), Some(dec!(150)), Some(dec!(150)));
        let after = margin_snapshot(dec!(90), dec!(90), Some(dec!(160)), Some(dec!(150)));
        assert_eq!(estimated_recovery_loss(before, after), Decimal::ZERO);
    }

    #[test]
    fn recovery_closes_only_the_residual_and_keeps_hedged_inventory() {
        let pos = |aster_qty, lighter_qty| PositionSnapshot { aster_qty, lighter_qty };
        let plan = |aster, lighter| residual_close_qtys(pos(aster, lighter), dec!(0.01), dec!(0.01));
        // Hedged 4 HYPE plus a naked 0.5 Aster short: buy back 0.5 on Aster only.
        assert_eq!(plan(dec!(-4.5), dec!(4)), (Side::Buy, dec!(0.5), dec!(0)));
        // The naked leg is on Lighter: sell 0.5 there; Aster's opposite-sign short is untouched.
        assert_eq!(plan(dec!(-4), dec!(4.5)), (Side::Sell, dec!(0), dec!(0.5)));
        // Both venues hold the residual's sign: Aster first, Lighter takes the remainder.
        assert_eq!(plan(dec!(0.3), dec!(0.2)), (Side::Sell, dec!(0.3), dec!(0.2)));
        // Sub-step remainders stay as dust instead of rounding up past the residual.
        assert_eq!(plan(dec!(-4.505), dec!(4)), (Side::Buy, dec!(0.5), dec!(0)));
        assert_eq!(plan(dec!(1), dec!(-1)), (Side::Buy, dec!(0), dec!(0)));
    }

    #[test]
    fn recovery_rows_preserve_distinct_source_execution_ids() {
        let spec = test_spec();
        let recovery = RecoveryReport {
            execution_id: "test-recovery-1".to_string(),
            action_taken: true,
            position: PositionSnapshot {
                aster_qty: Decimal::ZERO,
                lighter_qty: Decimal::ZERO,
            },
            lighter_ws_qty: None,
            margin_after: margins(),
            estimated_loss_usdc: dec!(1.25),
            aster_open_orders: 0,
            lighter_open_orders: 0,
        };
        let row = recovery_loss_row(&spec, &recovery);
        // Deterministic given the row's own timestamp, and nonzero — so the dedup key
        // `taker:<aster_order_id>:0` is unique per recovery.
        let mut other = recovery.clone();
        other.execution_id = "test-recovery-2".to_string();
        let second = recovery_loss_row(&spec,&other);
        assert_ne!(row.source_event_id,second.source_event_id);
        assert_eq!(row.economic_status,EconomicStatus::Estimated);
        assert_eq!(row.hedge_client_order_index, 0);
        assert_eq!(row.actual_net_usd, dec!(-1.25));
    }

    #[test]
    fn emergency_close_bound_crosses_past_mark_per_side() {
        let mark = dec!(100);
        // Buy bound above mark, sell bound below, scaled by bps.
        assert_eq!(emergency_close_bound(mark, Side::Buy, dec!(50)), dec!(100.5));
        assert_eq!(emergency_close_bound(mark, Side::Sell, dec!(50)), dec!(99.5));
        assert_eq!(
            emergency_close_bound(mark, Side::Buy, dec!(100)),
            dec!(101.0)
        );
        assert_eq!(emergency_close_bound(mark, Side::Buy, dec!(0)), mark);
    }

    #[test]
    fn margin_room_reduce_is_position_plus_margin_not_unbounded() {
        // Long 5 aster / short 5 lighter, both legs reducing (a_sign=-1, l_sign=+1):
        // room = |current| (margin-free reduce) + usable margin priced at ref_px for
        // the crossing remainder — never the old unconditional f64::MAX.
        let room = max_qty_by_available_margin_f64(10.0, 5.0, -5.0, -1.0, 1.0, 150.0, 150.0, 50.0);
        assert_eq!(room, 15.0);
    }

    #[test]
    fn margin_room_crossing_reduce_with_thin_margin_caps_at_position() {
        // No usable margin beyond the buffer: a reduce may still close the existing
        // position but cannot cross into a new one.
        let room = max_qty_by_available_margin_f64(10.0, 5.0, -5.0, -1.0, 1.0, 50.0, 50.0, 50.0);
        assert_eq!(room, 5.0);
        // Increases stay fully margin-blocked.
        let room = max_qty_by_available_margin_f64(10.0, 0.0, 0.0, 1.0, -1.0, 50.0, 50.0, 50.0);
        assert_eq!(room, 0.0);
    }

    #[test]
    fn margin_room_respects_the_limiting_venue_and_crossing_inventory() {
        for (a,l,a_sign,l_sign,expected) in [
            (5.0,-5.0,-1.0,1.0,12.0), (5.0,-5.0,1.0,-1.0,7.0),
            (0.3,-5.0,-1.0,1.0,10.3), (0.0,0.0,1.0,-1.0,7.0),
        ] {
            assert!((max_qty_by_available_margin_f64(10.0,a,l,a_sign,l_sign,150.0,120.0,50.0)-expected).abs()<1e-10);
        }
    }

    #[tokio::test]
    async fn hourly_breaker_returns_only_after_recovery_row_is_durable() {
        let dir = std::env::temp_dir().join(format!("taker_commit_{}", next_execution_id()));
        let spec = test_spec();
        let config = PnlCfg { persist_dir:dir.to_string_lossy().into_owned(), max_loss_usdc:dec!(10), ..Default::default() };
        let mut pnl = Some(PnlTracker::new(&config,&spec.market_id,Utc::now()).unwrap());
        let session = ActiveSession::new(crate::taker::pnl::session_path(&config,&spec.market_id),
            serde_json::json!({"session_id":"commit-test"}));
        session.arm().await.unwrap();
        session.record_unresolved(serde_json::json!({"execution_id":"recovery-test"})).await.unwrap();
        let recovery = RecoveryReport { execution_id:"recovery-test".to_string(), action_taken:true,
            position:PositionSnapshot {aster_qty:dec!(0),lighter_qty:dec!(0)},lighter_ws_qty:None,
            margin_after:margins(),estimated_loss_usdc:dec!(0.26),aster_open_orders:0,lighter_open_orders:0 };
        let error = record_recovery_loss(&mut pnl,&spec,&recovery,&session,Some("hourly loss".to_string())).await.unwrap_err();
        assert!(error.to_string().contains("hourly loss"));
        let path = pnl.as_ref().unwrap().snapshot().ledger_path;
        let row: TradeLedgerRow = serde_json::from_str(std::fs::read_to_string(path).unwrap().trim()).unwrap();
        assert_eq!(row.actual_net_usd,dec!(-0.26));
        assert_eq!(row.source_event_id.as_deref(),Some("recovery-recovery-test"));
        assert!(!session.unresolved());
        pnl.as_ref().unwrap().shutdown().await.unwrap();
        session.clear_verified().await.unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn accounting_worker_failure_keeps_the_unresolved_session_barrier() {
        let dir = std::env::temp_dir().join(format!("taker_commit_failure_{}", next_execution_id()));
        let spec = test_spec();
        let config = PnlCfg {persist_dir:dir.to_string_lossy().into_owned(),..Default::default()};
        let mut pnl = Some(PnlTracker::new(&config,&spec.market_id,Utc::now()).unwrap());
        pnl.as_ref().unwrap().shutdown().await.unwrap();
        let session = ActiveSession::new(crate::taker::pnl::session_path(&config,&spec.market_id),
            serde_json::json!({"session_id":"failed-commit-test"}));
        session.arm().await.unwrap();
        session.record_unresolved(serde_json::json!({"execution_id":"failed"})).await.unwrap();
        let recovery = RecoveryReport {execution_id:"failed".to_string(),action_taken:true,
            position:PositionSnapshot {aster_qty:dec!(0),lighter_qty:dec!(0)},lighter_ws_qty:None,
            margin_after:margins(),estimated_loss_usdc:dec!(0.26),aster_open_orders:0,lighter_open_orders:0};
        assert!(record_recovery_loss(&mut pnl,&spec,&recovery,&session,None).await.is_err());
        assert!(session.unresolved());
        assert!(session.clear_verified().await.is_err());
        drop(session);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn session_marked_guard_cancels_equal_opposite_venue_moves_and_trips_at_boundary() {
        let initial = margin_snapshot(dec!(50),dec!(50),Some(dec!(100)),Some(dec!(100)));
        let balanced_move = margin_snapshot(dec!(40),dec!(60),Some(dec!(90)),Some(dec!(110)));
        assert_eq!(session_marked_loss(initial.total_equity_usd().unwrap(),balanced_move.total_equity_usd().unwrap(),dec!(10)),None);
        assert_eq!(session_marked_loss(dec!(200),dec!(190.01),dec!(10)),None);
        assert_eq!(session_marked_loss(dec!(200),dec!(190),dec!(10)),Some(dec!(10)));
    }

}

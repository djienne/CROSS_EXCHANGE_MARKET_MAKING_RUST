//! Strategy/order hot path. A single-owner loop reprices each market
//! side, places/cancels/replaces the Aster maker order, and reacts to fills. It REUSES the
//! deterministic, well-tested quote math (`quote_engine::compute_desired_quote` and
//! `resting_quote_net_edge_bps`) rather than re-deriving the edge stack in integer math —
//! the integer hot types accelerate the touch/crossed/staleness pre-checks and carry the
//! order representation, but money math stays exact.
//!
//! This file holds the **pure decision table** (`evaluate_side_with_hl_sources`) — exhaustively testable —
//! and the async driver ([`run_strategy`]) that turns decisions into [`ExecCommand`]s and
//! folds fills into the hedge/risk state machine.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::future::FutureExt;
use rust_decimal::Decimal;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::book::OrderBook;
use crate::config::Config;
use crate::edge::EdgeConfig;
use crate::hot_types::HotBook;
use crate::hotpath::{VenueRegistry, VenueTag};
use crate::inventory::{self, HedgeabilityRules, PendingInventory};
use crate::markets::MarketSpec;
use crate::quote_engine::{
    compute_desired_quote_with_aster_touch_source, resting_quote_net_edge_bps, DesiredQuote,
    PositionContext, QuoteEngineConfig,
};
use crate::position::SignedPosition;
use crate::types::{MarketId, RejectReason, Side};

use super::account::{AccountState, Venue};
use super::exec::command::{ExecCommand, ExecEvent, HedgeCommand, MakerPermit};
use super::fills::{AsterFill, FillDedup, HedgeIntent, HedgeState, IntentPurpose};
use super::ids::{SessionId, Cloid};
use super::journal::{Journal, JournalDetail, QuoteRecord, DiagnosticRecord};
use super::orders::{CancelAfterAckReason, CancelTarget, OrderLifecycle, OrderManager};
use super::risk::{evaluate_maker_gate, position_mismatch, CooldownScope, CooldownState, MakerGateInputs};
use super::precheck::{hot_precheck_side, HotPrecheck};
use super::scale::MarketScale;

const MAKER_GATE_FROZEN: &str = "FROZEN";
/// The controller's network pause: quotes are pulled; hedging and corrections carry on.
const MAKER_GATE_NETWORK_PAUSE: &str = "NETWORK_PAUSE";
const MAKER_GATE_YIELDED: &str = "YIELDED_TO_TAKER";

/// XEMM's side of the execution rights under `run`; the taker's side is `RunOptions::want`
/// and `RunOptions::lease` (taker::arb).
pub struct Rights {
    /// `Some(id)`: the taker asks for the rights (an arbitrage passed its entry gate).
    pub want: tokio::sync::watch::Receiver<Option<u64>>,
    pub lease: tokio::sync::watch::Sender<Option<crate::taker::arb::ExecutionLease>>,
}

/// How long a grant lets the taker start an execution; one already started runs to its end.
const GRANT_TTL_MS: i64 = 3_000;

/// XEMM's turn in `run`, one regime at a time (Hummingbot's XEMM with the taker's arbitrage
/// in between): quote (`Idle`); on the taker's request cancel both sides and wait until
/// nothing rests or is in flight (`Yielding`); grant (`Granted`); after the hand-back re-read
/// the positions the taker changed (`Resuming`) before quoting again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Yield {
    Idle,
    Yielding { id: u64, since_ns: i64, fills: u32 },
    Granted { id: u64, at_ns: i64 },
    /// `seen`: the generation of the last snapshot read after `released_ns` (0: none yet).
    Resuming { id: u64, released_ns: i64, seen: u64 },
}
// Keep a small cushion of Aster command-queue slots for risk-reducing commands
// (targeted cancels, CancelAllBot, dead-man refresh). Optional quote churn must
// not be allowed to consume the entire bounded queue and then block a cancel.
const EXEC_CANCEL_RESERVE: usize = 64;
/// Circuit-breaker baseline = median of this many fresh marked equity samples (~10s at the
/// 2s reconcile cadence). A single-read baseline let one bad startup sample manufacture
/// phantom loss for a whole run (2026-07-04 incident).
const BREAKER_BASELINE_SAMPLES: usize = 5;
/// Consecutive fresh marked samples that must breach the loss limit before the breaker
/// trips (~4-6s at the 2s reconcile cadence). One anomalous snapshot must not halt the bot.
const BREAKER_TRIP_STREAK: u32 = 3;
/// Rolling window of the Aster REST command budget (the per-minute cap and its safety reserve).
const ASTER_CMD_WINDOW_NS: i64 = 60_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsterCommandPriority {
    /// Optional quote creation/churn. Must leave the configured safety reserve unused.
    Optional,
    /// Risk-reducing but targeted work, e.g. cancel a known slot or flatten an orphan.
    RiskReducing,
    /// Bulk safety operation such as CancelAllBot. Uses the reserved portion but still obeys the hard cap.
    Safety,
    /// Dead-man refresh. Important, but when the budget is exhausted the fail-safe is to let the
    /// venue countdown cancel orders rather than keep flooding refreshes into a 429 storm.
    Deadman,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecDispatch {
    Sent,
    QueueFull,
    QueueClosed,
    BudgetBlocked,
}

/// The minimal view of the current resting order the pure decision function needs.
#[derive(Debug, Clone, Copy)]
pub struct CurrentOrder {
    pub price: Decimal,
    pub qty: Decimal,
}

/// Why a resting quote is pulled or replaced (journaled via [`ReplaceReason::as_str`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceReason {
    PriceChanged,
    QuantityChanged,
    QuoteTooCloseToTouch,
    NoLongerProfitable,
    /// The market-data feed went stale; pull the quote (we can't trust the hedge price).
    FeedStale,
}

impl ReplaceReason {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplaceReason::PriceChanged => "PRICE_CHANGED",
            ReplaceReason::QuantityChanged => "QUANTITY_CHANGED",
            ReplaceReason::QuoteTooCloseToTouch => "QUOTE_TOO_CLOSE_TO_TOUCH",
            ReplaceReason::NoLongerProfitable => "NO_LONGER_PROFITABLE",
            ReplaceReason::FeedStale => "FEED_STALE",
        }
    }

    /// Map a quote-rejection cause into the reason we tag the cancel of a now-invalid
    /// standing quote. Feed-state failures — a stale or absent book on either venue —
    /// surface as `FeedStale`; every other cause (no profitable edge, crossed book,
    /// position cap, insufficient depth, …) collapses to `NoLongerProfitable`. The full
    /// `RejectReason` is still what the per-side decision note records.
    pub fn from_reject(reason: RejectReason) -> ReplaceReason {
        use RejectReason::*;
        match reason {
            AsterBookStale | HlBookStale | MissingAsterBook | MissingHlBook | HlBboThinAndL2Stale | AsterEffectiveTouchUnavailable => {
                ReplaceReason::FeedStale
            }
            QuoteTooCloseToTouch => ReplaceReason::QuoteTooCloseToTouch,
            _ => ReplaceReason::NoLongerProfitable,
        }
    }
}

/// What to do with one market side this evaluation.
#[derive(Debug, Clone)]
pub enum SideDecision {
    /// Leave the slot as is.
    Hold,
    /// Pull the resting order and place nothing (gate closed / cooldown / stale / no edge).
    Cancel { reason: ReplaceReason },
    /// Place a fresh quote (slot was empty).
    Place(Box<DesiredQuote>),
    /// Replace the resting quote (price moved / qty changed / no longer profitable).
    Replace { desired: Box<DesiredQuote>, reason: ReplaceReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HlQuoteSource {
    Bbo,
    L2,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsterQuoteSource {
    Bbo,
    L2,
}

impl AsterQuoteSource {
    fn as_str(self) -> &'static str {
        match self {
            AsterQuoteSource::Bbo => "bbo",
            AsterQuoteSource::L2 => "l2",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HlHedgePath {
    Decimal,
    Hot,
}

impl HlHedgePath {
    fn as_str(self, source: HlQuoteSource) -> &'static str {
        match (self, source) {
            (HlHedgePath::Decimal, HlQuoteSource::Bbo) => "decimal_bbo",
            (HlHedgePath::Decimal, HlQuoteSource::L2) => "decimal_l2",
            (HlHedgePath::Hot, HlQuoteSource::Bbo) => "hot_bbo",
            (HlHedgePath::Hot, HlQuoteSource::L2) => "hot_l2",
        }
    }
}

#[derive(Debug, Clone)]
struct SelectedHlBook {
    source: HlQuoteSource,
    path: HlHedgePath,
    book: Arc<OrderBook>,
    age_ms: i64,
}

#[derive(Debug, Clone)]
struct SelectedHlHotBook {
    source: HlQuoteSource,
    book: Arc<HotBook>,
    age_ms: i64,
}

#[derive(Debug, Clone)]
struct SelectedAsterTouch {
    source: AsterQuoteSource,
    book: Arc<OrderBook>,
    age_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsterTouchGuardStatus {
    Off,
    Active,
    Expired,
}


fn quote_cfg_for_touch_guard(
    quote: &QuoteEngineConfig,
    touch_guard_blocked: bool,
    current: Option<CurrentOrder>,
) -> QuoteEngineConfig {
    let mut q = quote.clone();
    if touch_guard_blocked
        && current.is_none()
        && quote.min_aster_touch_distance_bps > Decimal::ZERO
        && quote.min_aster_touch_hysteresis_bps > Decimal::ZERO
    {
        q.min_aster_touch_distance_bps = quote.aster_touch_rearm_distance_bps();
    }
    q
}

fn fresh_hot_book(book: &HotBook, now_ns: i64, max_stale_ns: i64) -> bool {
    book.age_ms(now_ns) <= max_stale_ns / 1_000_000 && !book.is_crossed()
}

#[inline]
fn executable_hot_book(book: &HotBook) -> bool {
    !book.is_crossed() && book.best_bid_ticks().is_some() && book.best_ask_ticks().is_some()
}

fn fresh_quote_book(book: &OrderBook, now: DateTime<Utc>, max_staleness_ms: i64) -> bool {
    book.age_ms(now) <= max_staleness_ms
        && !book.is_crossed()
        && book.best_bid().is_some()
        && book.best_ask().is_some()
}

#[inline]
fn executable_quote_book(book: &OrderBook) -> bool {
    !book.is_crossed() && book.best_bid().is_some() && book.best_ask().is_some()
}

#[inline]
fn hl_bbo_top_qty(book: &OrderBook, hedge_side: Side) -> Option<Decimal> {
    match hedge_side {
        Side::Buy => book.best_ask().map(|ask| ask.qty),
        Side::Sell => book.best_bid().map(|bid| bid.qty),
    }
}

#[inline]
fn hl_bbo_top_lots(book: &HotBook, hedge_side: Side) -> Option<i64> {
    match hedge_side {
        Side::Buy => book.asks().first().map(|ask| ask.qty_lots),
        Side::Sell => book.bids().first().map(|bid| bid.qty_lots),
    }
}

#[inline]
fn hl_bbo_depth_sufficient(
    book: &OrderBook,
    hedge_side: Side,
    hedge_qty: Decimal,
    multiple: Decimal,
) -> bool {
    let required_qty = hedge_qty * multiple.max(Decimal::ONE);
    hl_bbo_top_qty(book, hedge_side).is_some_and(|qty| qty >= required_qty)
}

fn hl_bbo_hot_depth_sufficient(
    scale: &MarketScale,
    book: &HotBook,
    hedge_side: Side,
    hedge_qty: Decimal,
    multiple: Decimal,
) -> bool {
    let required_lots = scale.hl_qty_to_lots_ceil(hedge_qty * multiple.max(Decimal::ONE));
    required_lots > 0 && hl_bbo_top_lots(book, hedge_side).is_some_and(|qty| qty >= required_lots)
}

#[inline]
fn bbo_not_older_than_l2(bbo: &OrderBook, l2: Option<&OrderBook>) -> bool {
    l2.is_none_or(|l2| bbo.exch_ts >= l2.exch_ts)
}

#[inline]
fn hot_bbo_not_older_than_l2(bbo: &HotBook, l2: Option<&HotBook>) -> bool {
    l2.is_none_or(|l2| bbo.exch_ms >= l2.exch_ms)
}

fn select_aster_hot_for_precheck<'a>(
    l2: Option<&'a HotBook>,
    bbo: Option<&'a HotBook>,
    now_ns: i64,
    max_stale_ns: i64,
) -> Option<&'a HotBook> {
    if let Some(bbo) = bbo
        .filter(|b| fresh_hot_book(b, now_ns, max_stale_ns))
        .filter(|b| hot_bbo_not_older_than_l2(b, l2))
    {
        return Some(bbo);
    }
    l2
}

fn select_hl_hot_for_precheck<'a>(
    l2: Option<&'a HotBook>,
    bbo: Option<&'a HotBook>,
    now_ns: i64,
    max_stale_ns: i64,
) -> Option<&'a HotBook> {
    if let Some(bbo) = bbo
        .filter(|b| fresh_hot_book(b, now_ns, max_stale_ns))
        .filter(|b| hot_bbo_not_older_than_l2(b, l2))
    {
        return Some(bbo);
    }
    l2
}

fn select_aster_touch_book<'a>(
    l2: &'a OrderBook,
    bbo: Option<&'a OrderBook>,
    now: DateTime<Utc>,
    max_staleness_ms: i64,
) -> Result<(&'a OrderBook, AsterQuoteSource), RejectReason> {
    if let Some(bbo) = bbo
        .filter(|b| fresh_quote_book(b, now, max_staleness_ms))
        .filter(|b| bbo_not_older_than_l2(b, Some(l2)))
    {
        return Ok((bbo, AsterQuoteSource::Bbo));
    }
    if fresh_quote_book(l2, now, max_staleness_ms) {
        return Ok((l2, AsterQuoteSource::L2));
    }
    if l2.best_bid().is_none() || l2.best_ask().is_none() {
        return Err(RejectReason::MissingAsterBook);
    }
    if l2.is_crossed() {
        return Err(RejectReason::BookCrossed);
    }
    Err(RejectReason::AsterBookStale)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn compute_desired_quote_select_hl<'a>(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_book: &'a OrderBook,
    hl_l2_book: Option<&'a OrderBook>,
    hl_bbo_book: Option<&'a OrderBook>,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
) -> Result<(DesiredQuote, &'a OrderBook, HlQuoteSource), RejectReason> {
    let (desired, hl_book, hl_source, _aster_source) = compute_desired_quote_select_books(
        edge,
        quote,
        aster_book,
        None,
        hl_l2_book,
        hl_bbo_book,
        side,
        spec,
        max_staleness_ms,
        now,
        pos,
    )?;
    Ok((desired, hl_book, hl_source))
}

#[allow(clippy::too_many_arguments)]
fn compute_desired_quote_select_books<'a>(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_depth_book: &'a OrderBook,
    aster_bbo_book: Option<&'a OrderBook>,
    hl_l2_book: Option<&'a OrderBook>,
    hl_bbo_book: Option<&'a OrderBook>,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
) -> Result<(DesiredQuote, &'a OrderBook, HlQuoteSource, AsterQuoteSource), RejectReason> {
    let (aster_touch_book, aster_source) = select_aster_touch_book(aster_depth_book, aster_bbo_book, now, max_staleness_ms)?;
    let fresh_bbo = hl_bbo_book.filter(|b| {
        b.age_ms(now) <= max_staleness_ms
            && !b.is_crossed()
            && b.best_bid().is_some()
            && b.best_ask().is_some()
            && bbo_not_older_than_l2(b, hl_l2_book)
    });
    if let Some(bbo) = fresh_bbo {
        match compute_desired_quote_with_aster_touch_source(
            edge,
            quote,
            aster_depth_book,
            aster_touch_book,
            matches!(aster_source, AsterQuoteSource::Bbo),
            bbo,
            side,
            spec.tick,
            spec.step,
            spec.aster_min_qty,
            spec.aster_min_notional,
            spec.hl_min_notional,
            max_staleness_ms,
            now,
            pos,
        ) {
            Ok(desired) => {
                if hl_bbo_depth_sufficient(
                    bbo,
                    desired.hedge_side,
                    desired.qty,
                    quote.depth_liquidity_multiple,
                ) {
                    return Ok((desired, bbo, HlQuoteSource::Bbo, aster_source));
                }
                let Some(l2) = hl_l2_book else {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                };
                if l2.age_ms(now) > max_staleness_ms || l2.is_crossed() {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                }
            }
            Err(RejectReason::HlHedgeVwapUnavailable) => {
                let Some(l2) = hl_l2_book else {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                };
                if l2.age_ms(now) > max_staleness_ms || l2.is_crossed() {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                }
            }
            Err(reason) => return Err(reason),
        }
    }

    let l2 = hl_l2_book.ok_or(RejectReason::MissingHlBook)?;
    let desired = compute_desired_quote_with_aster_touch_source(
        edge,
        quote,
        aster_depth_book,
        aster_touch_book,
        matches!(aster_source, AsterQuoteSource::Bbo),
        l2,
        side,
        spec.tick,
        spec.step,
        spec.aster_min_qty,
        spec.aster_min_notional,
        spec.hl_min_notional,
        max_staleness_ms,
        now,
        pos,
    )?;
    Ok((desired, l2, HlQuoteSource::L2, aster_source))
}

/// Aggressive IOC hedge price that CROSSES the executable Lighter touch: a buy hedge crosses the best
/// ask (+slippage), a sell hedge crosses the best bid (−slippage). Pricing off the touch (NOT mid,
/// NOT the Aster fill price) guarantees the IOC takes liquidity unless the touch moved more than
/// `slip_bps` since the snapshot — far more robust than `mid ± slip` on a sparse book (a live ETH
/// failure on the earlier Hyperliquid hedge venue: `l2Book` ≈0.46 updates/s, so mid was 1–3 s
/// stale and mid±10 bps did not cross).
/// `None` when the relevant book side is empty — the caller must NOT hedge off a fallback price.
fn crossing_hedge_px(book: &OrderBook, hedge_side: Side, slip_bps: Decimal) -> Option<Decimal> {
    let f = slip_bps / Decimal::from(10_000);
    match hedge_side {
        Side::Buy => book.best_ask().map(|ask| ask.px * (Decimal::ONE + f)),
        Side::Sell => book.best_bid().map(|bid| bid.px * (Decimal::ONE - f)),
    }
}

/// Pure decision for one market side. `may_quote` folds the feed gate + risk freeze +
/// cooldown (false ⇒ we may only cancel). `current` is the resting order, if any.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub fn evaluate_side(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_book: &OrderBook,
    hl_book: &OrderBook,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
    may_quote: bool,
    current: Option<CurrentOrder>,
    replace_immediately_if_unprofitable: bool,
) -> SideDecision {
    evaluate_side_with_hl_sources(
        edge,
        quote,
        aster_book,
        None,
        Some(hl_book),
        None,
        side,
        spec,
        max_staleness_ms,
        now,
        pos,
        may_quote,
        current,
        replace_immediately_if_unprofitable,
    )
    .0
}

/// Returns the side decision plus, when the quote engine rejected the candidate, the
/// reject reason — so callers (the empty-side touch-hysteresis latch) never need to
/// re-run the engine just to learn WHY a Hold happened.
#[allow(clippy::too_many_arguments)]
fn evaluate_side_with_hl_sources(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_book: &OrderBook,
    aster_bbo_book: Option<&OrderBook>,
    hl_l2_book: Option<&OrderBook>,
    hl_bbo_book: Option<&OrderBook>,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
    may_quote: bool,
    current: Option<CurrentOrder>,
    replace_immediately_if_unprofitable: bool,
) -> (SideDecision, Option<RejectReason>) {
    // Gate closed / cooldown / risk freeze: cancel anything resting, place nothing.
    if !may_quote {
        return (
            match current {
                Some(_) => SideDecision::Cancel { reason: ReplaceReason::FeedStale },
                None => SideDecision::Hold,
            },
            None,
        );
    }

    let desired = compute_desired_quote_select_books(
        edge,
        quote,
        aster_book,
        aster_bbo_book,
        hl_l2_book,
        hl_bbo_book,
        side,
        spec,
        max_staleness_ms,
        now,
        pos,
    );

    let desired = match desired {
        Ok(d) => d,
        Err(reason) => {
            // No acceptable quote right now. Pull a resting order with an honest reason
            // (stale feed vs no-longer-profitable).
            return (
                match current {
                    Some(_) => SideDecision::Cancel { reason: ReplaceReason::from_reject(reason) },
                    None => SideDecision::Hold,
                },
                Some(reason),
            );
        }
    };
    let (desired, selected_hl_book, _hl_source, _aster_source) = desired;

    let decision = match current {
        None => SideDecision::Place(Box::new(desired)),
        Some(cur) => {
            // Urgent safety check FIRST: a resting order that no longer clears the minimum edge
            // must be pulled/replaced even when the desired price moved by less than the churn
            // deadband. The deadband is only allowed to hold quotes that are still profitable.
            if replace_immediately_if_unprofitable {
                let still_ok = resting_quote_net_edge_bps(
                    edge,
                    selected_hl_book,
                    side,
                    cur.price,
                    cur.qty,
                    desired.ref_px,
                    quote.depth_liquidity_multiple,
                )
                    .is_some_and(|e| e >= edge.min_net_profit_bps);
                if !still_ok {
                    return (
                        SideDecision::Replace {
                            desired: Box::new(desired),
                            reason: ReplaceReason::NoLongerProfitable,
                        },
                        None,
                    );
                }
            }
            // Per-side requote DEADBAND (don't churn on sub-bps moves): if the new desired
            // price is within `min_requote_bps` of the current resting price, leave the quote in
            // place only after the profitability recheck above passed. A genuine qty change still
            // requotes.
            let move_bps = if cur.price > Decimal::ZERO {
                (cur.price - desired.price).abs() / cur.price * Decimal::from(10_000)
            } else {
                Decimal::from(10_000)
            };
            if move_bps < quote.min_requote_bps && cur.qty == desired.qty {
                return (SideDecision::Hold, None);
            }
            // Non-urgent: replace only if price moved past the tick threshold or qty changed.
            let threshold = Decimal::from(quote.price_change_ticks_to_requote) * spec.tick;
            if (cur.price - desired.price).abs() >= threshold {
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::PriceChanged }
            } else if cur.qty != desired.qty {
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::QuantityChanged }
            } else {
                SideDecision::Hold
            }
        }
    };
    (decision, None)
}

/// Per-market immutable context resolved at startup.
struct MarketCtx {
    spec: Arc<MarketSpec>,
    scale: MarketScale,
    aster_cell: Arc<crate::hotpath::VenueBook>,
    hedge_cell: Arc<crate::hotpath::VenueBook>,
    /// false ⇒ not eligible for live trading under the partial policy (never quoted).
    eligible: bool,
}

/// Per-market generation tracking for the generation-gated reprice.
struct GenSlot {
    last_aster_gen: u64,
    last_hl_gen: u64,
    decisions: [&'static str; 2],
}

#[derive(Debug, Clone, Copy)]
struct SweepState {
    requested_ns: i64,
    last_attempt_ns: i64,
    reason: &'static str,
}

#[derive(Clone)]
struct MakerCoverage {
    logical_id: Cloid,
    qty: Decimal,
    quote: Option<Decimal>,
}

/// All the wiring the strategy loop owns (single-thread; no locks).
pub struct Strategy {
    cfg: Config,
    markets: Vec<MarketId>,
    ctx: HashMap<MarketId, MarketCtx>,
    registry: Arc<VenueRegistry>,
    account: AccountState,
    journal: Journal,
    orders: OrderManager,
    cooldown: CooldownState,
    dedup: FillDedup,
    /// In-flight hedge obligations keyed by cloid hex.
    hedges: HashMap<String, HedgeIntent>,
    /// Predicted signed positions per market on each leg (for the cap + mismatch checks).
    aster_pos: HashMap<MarketId, SignedPosition>,
    hl_pos: HashMap<MarketId, SignedPosition>,
    /// Sub-min UNHEDGED Aster inventory per market: partial fills accumulate here and hedge on
    /// Lighter the moment the net clears the Lighter minimum (the primary fast-hedge path — never a
    /// per-partial taker flatten). A residual that genuinely lingers is flattened in `on_tick`.
    pending: HashMap<MarketId, PendingInventory>,
    logical_ids: HashMap<MarketId, Cloid>,
    maker_coverage: HashMap<String, MakerCoverage>,
    uncertain_makers: std::collections::HashSet<String>,
    rights: Option<Rights>,
    yield_state: Yield,
    correction_needed: std::collections::HashSet<MarketId>,
    correction_attempts: HashMap<MarketId, u32>,
    hedge_readiness: Option<super::exec::hyperliquid::HedgeReadiness>,
    maker_epoch: Arc<std::sync::atomic::AtomicU64>,
    draining: bool,
    drain_control: Option<Arc<super::exec::command::DrainControl>>,
    exec_tx: Sender<ExecCommand>,
    /// Optional priority lane to the Aster exec worker (acked cancels + flattens jump
    /// queued places/replaces — see `exec::command::is_priority_cmd`). `None` (tests
    /// without the lane) falls back to the FIFO `exec_tx`.
    exec_prio_tx: Option<Sender<ExecCommand>>,
    hedge_tx: Sender<HedgeCommand>,
    cooldown_ns: i64,
    /// Startup reconciliation done — no quoting before this.
    clean_start: bool,
    /// Latched freeze after an orphan-leg danger (hedge reject / timeout, failed dispatch,
    /// confirmed residual); `recover_orphans` self-heals it once the clean condition holds in
    /// two successive snapshots (see `heal_confirm`).
    frozen: bool,
    /// A safety cancel-all is pending; local maker slots are not trusted until a fresh
    /// account snapshot proves no bot-owned Aster orders remain.
    sweep_pending: Option<SweepState>,
    /// Rolling one-minute budget of Aster REST commands successfully enqueued by the strategy.
    /// A cancel+place replace counts as two because the worker performs two REST writes.
    aster_cmd_times_ns: VecDeque<i64>,
    /// Local freeze/backoff deadline after an Aster HTTP 429 / code -1003 notification.
    aster_rate_limited_until_ns: i64,
    /// Count of Aster REST rate-limit notifications observed in this strategy process.
    aster_429_count: u64,
    /// Persistence gate for the orphan backstop: `(signed_orphan_net, snapshot source_ts_ns)` of
    /// the FIRST snapshot a net delta was seen. The backstop only ACTS when the SAME orphan (same
    /// sign, comparable size) is still present in a STRICTLY NEWER snapshot — so a transient
    /// snapshot lag (a primary hedge that resolved but hasn't appeared in the reported snapshot
    /// yet) is filtered out instead of triggering a redundant recovery hedge. This keeps recovery
    /// EXCEPTIONAL (real persistent orphans only), so fast fills are hedged by the primary path.
    orphan_seen: HashMap<MarketId, (Decimal, i64)>,
    /// Monotonic ns of the most recent hot action (maker fill processed / primary hedge dispatched)
    /// per market. The orphan backstop ignores any snapshot whose READS BEGAN before this — such a
    /// snapshot cannot yet reflect the action, so acting on it could double-hedge (the fast-network
    /// straddle race). Self-clocking: no fixed timing constant.
    last_hot_action_ns: HashMap<MarketId, i64>,
    /// Self-heal persistence: snapshot `source_ts_ns` at which the freeze-clear condition (no
    /// outstanding hedges + positions reconciled + stream fresh) was FIRST observed. The freeze
    /// clears only once that condition holds again in a STRICTLY NEWER snapshot, so a transient
    /// snapshot lag can't unfreeze on a phantom-clean reading. Mirrors the `orphan_seen` gate.
    heal_confirm: Option<i64>,
    /// Aster user-stream liveness: gates quoting on stream freshness. `None` until
    /// wired (tests without a stream).
    aster_stream: Option<Arc<super::userstream::StreamLiveness>>,
    /// Cumulative-loss circuit breaker. Cloned XEMM engine shutdown token, cancelled when the
    /// breaker trips; set via [`Strategy::arm_circuit_breaker`].
    shutdown: tokio_util::sync::CancellationToken,
    /// Where to write the persistent trip latch on a trip (set with the shutdown token).
    trip_file_path: Option<std::path::PathBuf>,
    /// Total cross-venue equity baseline: the MEDIAN of the first
    /// [`BREAKER_BASELINE_SAMPLES`] fresh marked snapshots (a single-read baseline let one
    /// bad startup sample manufacture phantom loss for the whole run — 2026-07-04 incident).
    breaker_baseline_equity: Option<Decimal>,
    /// Fresh marked equity samples collected while arming the baseline.
    breaker_baseline_samples: Vec<Decimal>,
    /// Consecutive fresh marked samples breaching the limit; trips at
    /// [`BREAKER_TRIP_STREAK`]. Reset by any non-breaching, stale, or unmarked sample.
    breaker_breach_streak: u32,
    /// Snapshot generation of the last sample the breaker processed — each published
    /// snapshot is counted at most once (the tick and publish cadences would otherwise let
    /// one bad snapshot be seen several times in a row).
    breaker_last_generation: u64,
    /// Latched once the breaker fires (prevents re-tripping / duplicate latch writes).
    breaker_tripped: bool,
    /// In-memory trip flag shared with `run.rs` (set via [`Strategy::set_trip_flag`]). Set BEFORE
    /// the persistent latch write so a failed/unwritable trip file still makes `run` return an
    /// error at shutdown — otherwise the controller would restart it straight back into trading.
    trip_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    pause_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Per-market maker-gate suppression tracking, for OBSERVABILITY: `(since_ns, reason, logged)`.
    /// A closed maker gate (orphan hedge / unhedged-over-limit / stale snapshot / stale feed / …)
    /// otherwise suppresses quoting with NO log and no `frozen` latch — the exact failure mode that
    /// left a live bot dead for hours with zero signal. We record when the gate first closed; once
    /// the closure PERSISTS past a short grace (so normal post-fill cooldowns / transient feed blips
    /// don't spam), `logged` flips true and we emit a WARN + journal entry naming the reason. When the
    /// gate reopens we log a RESUMED line with the duration. Pure observability — does not gate.
    quote_suppressed: HashMap<MarketId, (i64, &'static str, bool)>,
    /// Per-(market, side) margin-reject suppression timestamp (monotonic ns).
    /// Set when a PlaceReject contains "insufficient" (Aster -2019).
    /// Cleared on: fill on that market, or 10s cooldown expiry.
    margin_suppressed: HashMap<(MarketId, Side), i64>,
    /// Per-side Aster touch hysteresis latch. Set after a side is rejected/cancelled
    /// for `QUOTE_TOO_CLOSE_TO_TOUCH`; while the slot is empty, placement requires
    /// `min_aster_touch_distance_bps + min_aster_touch_hysteresis_bps` clearance.
    aster_touch_guard_blocked: HashMap<(MarketId, Side), i64>,
    /// Shared dirty-market bitset — `None` in tests without dirty wiring.
    dirty: Option<Arc<crate::hotpath::dirty::DirtyMarkets>>,
    /// Per-market last-seen generation for each venue — skip reprice when unchanged.
    gen_slots: Vec<GenSlot>,
    /// Hot-integer precheck config (built from Config at startup).
    precheck_cfg: super::precheck::HotPrecheckConfig,
    /// Per-market Lighter mid mark cache, refreshed once per wake/tick batch to avoid O(N²)
    /// book loads in `positions_reconciled` (called per-market inside `reprice_market`).
    mark_cache: HashMap<MarketId, Decimal>,
}

impl Strategy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Config,
        specs: &[MarketSpec],
        eligibility: &HashMap<MarketId, bool>,
        registry: Arc<VenueRegistry>,
        account: AccountState,
        journal: Journal,
        session: SessionId,
        exec_tx: Sender<ExecCommand>,
        hedge_tx: Sender<HedgeCommand>,
    ) -> Self {
        let markets: Vec<MarketId> = specs.iter().map(|s| s.market_id.clone()).collect();
        let ctx: HashMap<MarketId, MarketCtx> = specs
            .iter()
            .map(|s| {
                (
                    s.market_id.clone(),
                    MarketCtx {
                        spec: Arc::new(s.clone()),
                        scale: MarketScale::from_spec(s),
                        aster_cell: registry.cell(&s.market_id, VenueTag::Aster).expect("market registry has Aster cell"),
                        hedge_cell: registry.cell(&s.market_id, VenueTag::Hyperliquid).expect("market registry has hedge cell"),
                        eligible: *eligibility.get(&s.market_id).unwrap_or(&false),
                    },
                )
            })
            .collect();
        let scope = if cfg.live.cooldown_is_global() {
            CooldownScope::Global
        } else {
            CooldownScope::PerMarket
        };
        let cooldown_ns = cfg.live.post_trade_cooldown_ms.max(0) * 1_000_000;
        let orders = OrderManager::new(session, &markets);
        let num_markets = registry.num_markets();
        let precheck_cfg = super::precheck::HotPrecheckConfig {
            max_book_stale_ns: cfg.live.max_book_staleness_ms * 1_000_000,
        };
        Strategy {
            cfg,
            markets,
            ctx,
            registry,
            account,
            journal,
            orders,
            cooldown: CooldownState::new(scope),
            dedup: FillDedup::new(),
            hedges: HashMap::new(),
            aster_pos: HashMap::new(),
            hl_pos: HashMap::new(),
            pending: HashMap::new(),
            logical_ids: HashMap::new(),
            maker_coverage: HashMap::new(),
            uncertain_makers: std::collections::HashSet::new(),
            rights: None,
            yield_state: Yield::Idle,
            correction_needed: std::collections::HashSet::new(),
            correction_attempts: HashMap::new(),
            hedge_readiness: None,
            maker_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            draining: false,
            drain_control: None,
            exec_tx,
            exec_prio_tx: None,
            hedge_tx,
            cooldown_ns,
            clean_start: false,
            frozen: false,
            sweep_pending: None,
            aster_cmd_times_ns: VecDeque::new(),
            aster_rate_limited_until_ns: 0,
            aster_429_count: 0,
            orphan_seen: HashMap::new(),
            last_hot_action_ns: HashMap::new(),
            heal_confirm: None,
            aster_stream: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
            trip_file_path: None,
            breaker_baseline_equity: None,
            breaker_baseline_samples: Vec::new(),
            breaker_breach_streak: 0,
            breaker_last_generation: 0,
            breaker_tripped: false,
            trip_flag: None,
            pause_flag: None,
            quote_suppressed: HashMap::new(),
            margin_suppressed: HashMap::new(),
            aster_touch_guard_blocked: HashMap::new(),
            dirty: None,
            gen_slots: (0..num_markets).map(|_| GenSlot { last_aster_gen: 0, last_hl_gen: 0, decisions: ["NOT_EVALUATED"; 2] }).collect(),
            precheck_cfg,
            mark_cache: HashMap::new(),
        }
    }

    pub fn set_drain_control(&mut self, control: Arc<super::exec::command::DrainControl>) { self.drain_control = Some(control); }

    pub fn set_hedge_readiness(&mut self, readiness: super::exec::hyperliquid::HedgeReadiness) {
        self.hedge_readiness = Some(readiness);
    }

    fn logical_id(&mut self, market: &MarketId) -> Cloid {
        if let Some(id) = self.logical_ids.get(market) { return *id; }
        let id = self.orders.next_attempt_id(market);
        self.logical_ids.insert(market.clone(), id);
        id
    }

    fn publish_execution_queries(&self) {
        self.account.publish_pending_exec(self.hedges.values()
            .filter(|h| h.venue == Venue::Aster && h.unresolved()).cloned().collect());
    }

    /// Wire the shared dirty-market bitset (set by the registry builder in `run.rs`).
    pub fn set_dirty(&mut self, dirty: Arc<crate::hotpath::dirty::DirtyMarkets>) {
        self.dirty = Some(dirty);
    }

    /// Arm the cumulative-loss circuit breaker: give the strategy the engine shutdown token (to
    /// halt on a trip) and the trip-latch path (to persist the trip). Called once at wire-up, before
    /// `run_strategy`. The breaker acts only when `live.circuit_breaker.enabled`.
    pub fn arm_circuit_breaker(
        &mut self,
        trip_file_path: std::path::PathBuf,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        self.journal.configure_trip_path(trip_file_path.clone());
        self.trip_file_path = Some(trip_file_path);
        self.shutdown = shutdown;
    }

    /// Wire the in-memory breaker trip flag (read by `run.rs` at shutdown to guarantee an error
    /// return even when the persistent trip-latch write failed).
    pub fn set_trip_flag(&mut self, f: Arc<std::sync::atomic::AtomicBool>) {
        self.trip_flag = Some(f);
    }

    pub fn set_pause_flag(&mut self, f: Arc<std::sync::atomic::AtomicBool>) {
        self.pause_flag = Some(f);
    }

    pub fn set_rights(&mut self, rights: Option<Rights>) {
        self.rights = rights;
    }

    /// Nothing rests and nothing is in flight: Hummingbot's `ready_for_new_trades`, plus the
    /// states only this engine has (uncertain cancels, sub-minimum partials, corrections).
    fn settled(&self) -> bool {
        self.orders.live_slots().is_empty() && self.uncertain_makers.is_empty() && self.pending.is_empty()
            && self.correction_needed.is_empty() && !self.hedges.values().any(|h| h.unresolved()) && !self.has_orphan_hedge()
    }

    /// Advances the hand-over to the taker (see [`Yield`]); `want` is the taker's current
    /// request. Runs after every event, so a settle or a release is acted on at once.
    fn drive_yield(&mut self, want: Option<u64>, now_ns: i64) {
        let ms = |since: i64| now_ns.saturating_sub(since) / 1_000_000;
        match (self.yield_state, want) {
            (Yield::Idle, Some(id)) => {
                self.revoke_makers();
                for m in self.markets.clone() { self.cancel_both_sides(&m, now_ns); }
                self.yield_state = Yield::Yielding { id, since_ns: now_ns, fills: 0 };
            }
            (Yield::Yielding { id, since_ns, fills }, None) => {
                self.journal.record(now_ns, "yield_withdrawn", None, serde_json::json!({"want_id": id, "waited_ms": ms(since_ns), "fills": fills}));
                self.yield_state = Yield::Idle;
            }
            (Yield::Yielding { id, since_ns, fills }, Some(_)) if self.settled() => {
                let market = self.markets[0].0.clone();
                let expires_at = Utc::now() + chrono::Duration::milliseconds(GRANT_TTL_MS);
                if let Some(rights) = &self.rights {
                    rights.lease.send_replace(Some(crate::taker::arb::ExecutionLease { market, lease_id: format!("xemm-{id}"), expires_at }));
                }
                self.journal.record(now_ns, "yield", None, serde_json::json!({"want_id": id, "cancel_to_grant_ms": ms(since_ns), "fills": fills}));
                self.yield_state = Yield::Granted { id, at_ns: now_ns };
            }
            (Yield::Granted { id, at_ns }, None) => {
                if let Some(rights) = &self.rights { rights.lease.send_replace(None); }
                if self.hedge_tx.try_send(HedgeCommand::RefreshNonce).is_err() { self.freeze(now_ns, "hedge_queue_send_failed"); }
                self.journal.record(now_ns, "rights_returned", None, serde_json::json!({"want_id": id, "held_ms": ms(at_ns)}));
                self.yield_state = Yield::Resuming { id, released_ns: now_ns, seen: 0 };
            }
            (Yield::Resuming { id, released_ns, seen }, _) => {
                // The taker traded on the same accounts: adopt what the venues report after the
                // hand-back, until two snapshots in a row agree (a venue cache may lag a fill).
                let snap = self.account.load();
                if snap.read_start_ns <= released_ns || snap.generation == seen { return; }
                if seen != 0 && self.positions_reconciled() {
                    self.journal.record(now_ns, "resumed", None, serde_json::json!({"want_id": id, "resume_ms": ms(released_ns)}));
                    self.yield_state = Yield::Idle;
                } else {
                    self.adopt_reported_positions(now_ns);
                    self.yield_state = Yield::Resuming { id, released_ns, seen: snap.generation };
                }
            }
            _ => {}
        }
    }

    /// Mark startup reconciliation complete — quoting may begin (still gated by feeds/cooldown).
    pub fn set_exec_prio_lane(&mut self, tx: Sender<ExecCommand>) {
        self.exec_prio_tx = Some(tx);
    }

    pub fn mark_clean_start(&mut self) {
        self.clean_start = true;
    }

    /// Adopt the venue-REPORTED positions from the startup snapshot as the predicted positions.
    /// Called once from `run.rs` after the initial reconcile published its snapshot, before the
    /// strategy thread spawns. The predicted maps start empty and prior-session fills are never
    /// re-attributed (client ids are session-prefixed), so after a NON-neutral restart the old
    /// code never reached the dust-branch sync: the snapshot-predicted cross-check deferred
    /// forever ("predicted balanced but snapshot disagrees") while `positions_reconciled` kept
    /// the maker gate closed — quoting frozen AND the imbalance left unhedged indefinitely.
    /// Seeding predicted from the reported snapshot lets the normal reconcile/orphan machinery
    /// (with all its confirmation gates) take over. Requires a FRESH snapshot: if it is absent
    /// or stale we adopt nothing, which degrades to the old freeze — never trusts stale data.
    pub fn adopt_reported_positions(&mut self, now_ns: i64) {
        let snap = self.account.load();
        let max_age_ns = self.cfg.live.max_account_snapshot_age_ms.saturating_mul(1_000_000);
        if snap.source_ts_ns == 0 || now_ns.saturating_sub(snap.source_ts_ns) > max_age_ns {
            error!(
                "adopt_reported_positions: startup snapshot absent/stale (age_ms={}); NOT adopting — \
                 predicted stays empty and the maker gate stays closed until reconciled",
                self.account.age_ms(now_ns)
            );
            return;
        }
        let mut adopted = Vec::new();
        for m in &self.markets {
            let find = |list: &[super::account::ScaledPosition]| {
                list.iter()
                    .find(|p| &p.market == m)
                    .map(|p| (p.signed_qty, p.entry_px))
                    .unwrap_or((Decimal::ZERO, Decimal::ZERO))
            };
            let (a_qty, a_px) = find(&snap.aster_positions);
            let (h_qty, h_px) = find(&snap.hl_positions);
            self.aster_pos.insert(m.clone(), SignedPosition { qty: a_qty, avg_px: a_px });
            self.hl_pos.insert(m.clone(), SignedPosition { qty: h_qty, avg_px: h_px });
            if a_qty != Decimal::ZERO || h_qty != Decimal::ZERO {
                warn!(
                    "adopted prior-session position on {m}: aster={a_qty}@{a_px} lighter={h_qty}@{h_px} \
                     (net {}) — recovery machinery will confirm and neutralize any imbalance",
                    a_qty + h_qty
                );
            }
            adopted.push(serde_json::json!({
                "market": m.0.clone(),
                "aster_qty": a_qty.to_string(),
                "aster_px": a_px.to_string(),
                "hl_qty": h_qty.to_string(),
                "hl_px": h_px.to_string(),
            }));
        }
        self.journal.record(
            now_ns,
            "adopt_positions",
            None,
            serde_json::json!({
                "snapshot_generation": snap.generation,
                "snapshot_age_ms": self.account.age_ms(now_ns),
                "positions": adopted,
            }),
        );
    }

    /// Wire the Aster user-stream liveness so [`may_quote`](Self::may_quote) can freeze on a
    /// silently-dead fill stream.
    pub fn set_user_stream(&mut self, s: Arc<super::userstream::StreamLiveness>) {
        self.aster_stream = Some(s);
    }

    #[inline]
    fn touch_hysteresis_enabled(&self) -> bool {
        self.cfg.quote.min_aster_touch_distance_bps > Decimal::ZERO
            && self.cfg.quote.min_aster_touch_hysteresis_bps > Decimal::ZERO
    }

    fn latch_aster_touch_guard(&mut self, market: &MarketId, side: Side, now_ns: i64) {
        if !self.touch_hysteresis_enabled() {
            return;
        }
        let key = (market.clone(), side);
        if !self.aster_touch_guard_blocked.contains_key(&key) {
            self.aster_touch_guard_blocked.insert(key, now_ns);
            debug!(
                "Aster touch guard blocked for {market} {side:?}; rearm at {}bps",
                self.cfg.quote.aster_touch_rearm_distance_bps()
            );
        }
    }

    fn clear_aster_touch_guard(&mut self, market: &MarketId, side: Side, now_ns: i64) {
        if let Some(since_ns) = self.aster_touch_guard_blocked.remove(&(market.clone(), side)) {
            debug!(
                "Aster touch guard rearmed for {market} {side:?} after {}ms",
                now_ns.saturating_sub(since_ns) / 1_000_000
            );
        }
    }

    fn aster_touch_guard_status_for_empty(
        &self,
        market: &MarketId,
        side: Side,
        current: Option<CurrentOrder>,
        now_ns: i64,
    ) -> AsterTouchGuardStatus {
        if current.is_some() {
            return AsterTouchGuardStatus::Off;
        }
        let Some(since_ns) = self.aster_touch_guard_blocked.get(&(market.clone(), side)) else {
            return AsterTouchGuardStatus::Off;
        };
        let max_ms = self.cfg.quote.max_aster_touch_hysteresis_ms;
        if max_ms > 0 && now_ns.saturating_sub(*since_ns) >= max_ms.saturating_mul(1_000_000) {
            AsterTouchGuardStatus::Expired
        } else {
            AsterTouchGuardStatus::Active
        }
    }

    fn expire_aster_touch_guard_if_needed(
        &mut self,
        market: &MarketId,
        side: Side,
        current: Option<CurrentOrder>,
        now_ns: i64,
    ) -> AsterTouchGuardStatus {
        let status = self.aster_touch_guard_status_for_empty(market, side, current, now_ns);
        if status == AsterTouchGuardStatus::Expired {
            self.clear_aster_touch_guard(market, side, now_ns);
        }
        status
    }

    fn current_order_for_decision(
        &self,
        market: &MarketId,
        side: Side,
        scale: &MarketScale,
    ) -> Option<CurrentOrder> {
        self.orders.slot(market, side).and_then(|s| {
            (s.is_live() && s.remaining_lots() > 0).then(|| CurrentOrder {
                price: scale.ticks_to_price(s.price_ticks),
                qty: scale.lots_to_qty(s.remaining_lots()),
            })
        })
    }

    /// Latch the hysteresis state for an empty side that just rejected on the base
    /// touch threshold. Existing orders are handled by `apply_decision` when the
    /// cancel reason is `QUOTE_TOO_CLOSE_TO_TOUCH`. The reject reason is the one the
    /// side evaluation already computed — whenever this latch is reachable (guard not
    /// active) that evaluation ran on the base quote config, so re-running the quote
    /// engine here (the pre-2026-07 behavior, ~2x reprice cost in the standby state)
    /// would produce the identical result.
    fn latch_empty_touch_reject_if_needed(
        &mut self,
        market: &MarketId,
        side: Side,
        decision: &SideDecision,
        reject: Option<RejectReason>,
        current: Option<CurrentOrder>,
        now_ns: i64,
    ) {
        if !self.touch_hysteresis_enabled() {
            return;
        }
        if current.is_some() || !matches!(decision, SideDecision::Hold) {
            return;
        }
        if self.aster_touch_guard_blocked.contains_key(&(market.clone(), side)) {
            return;
        }
        if reject == Some(RejectReason::QuoteTooCloseToTouch) {
            self.latch_aster_touch_guard(market, side, now_ns);
        }
    }

    fn revoke_makers(&self) { self.maker_epoch.fetch_add(1, std::sync::atomic::Ordering::AcqRel); }

    /// Latch a freeze. Maker quoting stops until the cold reconciler proves the account/order state
    /// is clean across snapshots and self-heals it.
    fn freeze(&mut self, now_ns: i64, cause: &'static str) {
        self.revoke_makers();
        if !self.frozen {
            self.frozen = true;
            warn!("maker quoting FROZEN: {cause}");
            self.journal.reason(now_ns, "freeze", None, cause);
        }
    }

    fn freeze_and_sweep(&mut self, now_ns: i64, cause: &'static str) {
        self.freeze(now_ns, cause);
        self.request_safety_sweep(now_ns, cause);
    }

    #[inline]
    fn exec_queue_low_for_optional_work(&self) -> bool {
        self.exec_tx.capacity() <= EXEC_CANCEL_RESERVE
    }

    #[inline]
    fn aster_backoff_remaining_ms(&self, now_ns: i64) -> i64 {
        self.aster_rate_limited_until_ns.saturating_sub(now_ns) / 1_000_000
    }

    fn prune_aster_cmd_budget(&mut self, now_ns: i64) {
        let cutoff = now_ns.saturating_sub(ASTER_CMD_WINDOW_NS);
        while self.aster_cmd_times_ns.front().is_some_and(|&t| t < cutoff) {
            self.aster_cmd_times_ns.pop_front();
        }
    }

    fn aster_cmds_in_window(&self, now_ns: i64) -> u32 {
        let cutoff = now_ns.saturating_sub(ASTER_CMD_WINDOW_NS);
        self.aster_cmd_times_ns.iter().filter(|&&t| t >= cutoff).count() as u32
    }

    fn aster_command_cost(&self, cmd: &ExecCommand) -> u32 {
        match cmd {
            ExecCommand::Replace { .. } => 2, // worker performs cancel + place
            ExecCommand::CancelAllBot => self.markets.len().max(1) as u32,
            ExecCommand::Shutdown | ExecCommand::Barrier { .. } => 0,
            _ => 1,
        }
    }

    fn aster_budget_allows(&mut self, priority: AsterCommandPriority, cost: u32, now_ns: i64) -> bool {
        if cost == 0 {
            return true;
        }
        if now_ns < self.aster_rate_limited_until_ns {
            return false;
        }
        self.prune_aster_cmd_budget(now_ns);
        let cap = self.cfg.live.aster.effective_max_rest_requests_per_minute();
        if cap == 0 {
            return false;
        }
        let used = self.aster_cmd_times_ns.len() as u32;
        let reserve = self.cfg.live.aster.effective_optional_rest_reserve_per_minute();
        let limit = match priority {
            AsterCommandPriority::Optional => cap.saturating_sub(reserve),
            AsterCommandPriority::RiskReducing | AsterCommandPriority::Safety | AsterCommandPriority::Deadman => cap,
        };
        used.saturating_add(cost) <= limit
    }

    fn record_aster_command_dispatch(&mut self, cost: u32, now_ns: i64) {
        if cost == 0 {
            return;
        }
        self.prune_aster_cmd_budget(now_ns);
        for _ in 0..cost {
            self.aster_cmd_times_ns.push_back(now_ns);
        }
    }

    fn try_send_aster_cmd(&mut self, cmd: ExecCommand, priority: AsterCommandPriority, now_ns: i64) -> ExecDispatch {
        let cost = self.aster_command_cost(&cmd);
        if !self.aster_budget_allows(priority, cost, now_ns) {
            return ExecDispatch::BudgetBlocked;
        }
        // Priority-lane routing: an acked cancel/flatten jumps the FIFO of queued places.
        // A full (or missing) priority queue falls back to the normal lane — ordering-safe
        // by definition, since the FIFO lane is where these commands lived before.
        let cmd = if super::exec::command::is_priority_cmd(&cmd) {
            match &self.exec_prio_tx {
                Some(ptx) => match ptx.try_send(cmd) {
                    Ok(()) => {
                        self.record_aster_command_dispatch(cost, now_ns);
                        return ExecDispatch::Sent;
                    }
                    Err(TrySendError::Full(cmd)) | Err(TrySendError::Closed(cmd)) => cmd,
                },
                None => cmd,
            }
        } else {
            cmd
        };
        match self.exec_tx.try_send(cmd) {
            Ok(()) => {
                self.record_aster_command_dispatch(cost, now_ns);
                ExecDispatch::Sent
            }
            Err(TrySendError::Full(_)) => ExecDispatch::QueueFull,
            Err(TrySendError::Closed(_)) => ExecDispatch::QueueClosed,
        }
    }

    fn note_aster_budget_block(&mut self, now_ns: i64, cause: &'static str, priority: AsterCommandPriority) {
        let remaining = self.aster_backoff_remaining_ms(now_ns);
        if remaining > 0 {
            warn!("Aster REST command deferred during rate-limit backoff ({remaining}ms remaining): {cause}");
        } else {
            warn!(
                "Aster REST command budget exhausted: {cause} priority={priority:?} used={}/{} reserve={} exec_capacity={}",
                self.aster_cmds_in_window(now_ns),
                self.cfg.live.aster.effective_max_rest_requests_per_minute(),
                self.cfg.live.aster.effective_optional_rest_reserve_per_minute(),
                self.exec_tx.capacity()
            );
        }
        self.journal.reason(now_ns, "aster_cmd_blocked", None, cause);
    }

    fn on_aster_rate_limited(&mut self, now_ns: i64, reason: String, backoff_ms: i64) {
        let backoff_ns = backoff_ms.max(1).saturating_mul(1_000_000);
        self.aster_rate_limited_until_ns = self.aster_rate_limited_until_ns.max(now_ns.saturating_add(backoff_ns));
        self.aster_429_count = self.aster_429_count.saturating_add(1);
        warn!(
            "maker quoting FROZEN: Aster REST rate limit ({}ms backoff, count={}): {}",
            backoff_ms,
            self.aster_429_count,
            reason
        );
        self.freeze(now_ns, "aster_rate_limited");
        self.sweep_pending.get_or_insert(SweepState {
            requested_ns: now_ns,
            last_attempt_ns: now_ns,
            reason: "aster_rate_limited",
        });
        self.journal.reason(now_ns, "aster_rate_limited", None, "venue rate limit");
    }

    fn cancel_target(&mut self, market: &MarketId, side: Side, now_ns: i64) -> CancelTarget {
        self.orders.revoke_queued(market, side);
        if self.sweep_pending.is_some() {
            return CancelTarget::Suppressed;
        }
        self.orders
            .cancel_target(market, side, now_ns, self.cfg.live.aster.cancel_retry_backoff_ms)
    }

    fn cell(&self, market: &MarketId, venue: VenueTag) -> Option<&crate::hotpath::VenueBook> {
        self.ctx.get(market).map(|ctx| match venue {
            VenueTag::Aster => ctx.aster_cell.as_ref(),
            VenueTag::Hyperliquid => ctx.hedge_cell.as_ref(),
        })
    }

    fn book(&self, market: &MarketId, venue: VenueTag) -> Option<Arc<OrderBook>> {
        self.cell(market, venue).and_then(|c| c.load())
    }

    /// Fresh executable Lighter quote source for immediate hedging: prefer BBO, then L2.
    ///
    /// Uses the VenueBook monotonic stamps, not OrderBook wall-clock age, so NTP jumps
    /// cannot make stale data look fresh. The returned Arc keeps the chosen book alive
    /// across subsequent &mut self work in the fill handler.
    fn fresh_hl_quote_book(&self, market: &MarketId, now_ns: i64) -> Option<SelectedHlBook> {
        let cell = self.cell(market, VenueTag::Hyperliquid)?;
        if cell.stream_down() || cell.is_divergent() { return None; }
        let max_stale_ms = self.cfg.live.max_book_staleness_ms;

        // Read age before the ArcSwap pointer so a concurrent publish cannot pair an
        // old book Arc with a newer freshness stamp. A false negative is safe; a false
        // fresh hedge source is not.
        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo();
        if bbo_age_ms <= max_stale_ms && bbo.as_deref().is_some_and(executable_quote_book) {
            return bbo.map(|book| SelectedHlBook { source: HlQuoteSource::Bbo, path: HlHedgePath::Decimal, book, age_ms: bbo_age_ms });
        }

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        if l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book) {
            return l2.map(|book| SelectedHlBook { source: HlQuoteSource::L2, path: HlHedgePath::Decimal, book, age_ms: l2_age_ms });
        }

        None
    }

    /// Fresh executable Lighter book for a known hedge quantity. BBO is trusted only when the
    /// relevant top size is materially deeper than the intended hedge; otherwise use fresh L2.
    fn fresh_hl_hedge_book(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
    ) -> Option<SelectedHlBook> {
        let cell = self.cell(market, VenueTag::Hyperliquid)?;
        if cell.stream_down() || cell.is_divergent() { return None; }
        let max_stale_ms = self.cfg.live.max_book_staleness_ms;
        let depth_multiple = self.cfg.quote.depth_liquidity_multiple;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        let l2_ok = l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book);

        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo();
        if bbo_age_ms <= max_stale_ms
            && bbo.as_deref().is_some_and(executable_quote_book)
            && bbo.as_deref().is_some_and(|b| bbo_not_older_than_l2(b, l2.as_deref()))
        {
            let book = bbo.as_ref().expect("checked above");
            if hl_bbo_depth_sufficient(book.as_ref(), hedge_side, hedge_qty, depth_multiple) {
                return Some(SelectedHlBook {
                    source: HlQuoteSource::Bbo,
                    path: HlHedgePath::Decimal,
                    book: Arc::clone(book),
                    age_ms: bbo_age_ms,
                });
            }
        }

        if l2_ok {
            return l2.map(|book| SelectedHlBook { source: HlQuoteSource::L2, path: HlHedgePath::Decimal, book, age_ms: l2_age_ms });
        }

        None
    }

    /// Fresh executable Lighter hot book for a known hedge quantity. This mirrors
    /// `fresh_hl_hedge_book` but uses the prebuilt integer books and Lighter quantity lots.
    fn fresh_hl_hedge_hot(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
    ) -> Option<SelectedHlHotBook> {
        let cell = self.cell(market, VenueTag::Hyperliquid)?;
        if cell.stream_down() || cell.is_divergent() { return None; }
        let ctx = self.ctx.get(market)?;
        let max_stale_ns = self.cfg.live.max_book_staleness_ms * 1_000_000;
        let depth_multiple = self.cfg.quote.depth_liquidity_multiple;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load_hot();
        let l2_ok = l2
            .as_deref()
            .is_some_and(|b| b.age_ms(now_ns) <= max_stale_ns / 1_000_000 && executable_hot_book(b));

        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo_hot();
        if bbo
            .as_deref()
            .is_some_and(|b| b.age_ms(now_ns) <= max_stale_ns / 1_000_000 && executable_hot_book(b))
            && bbo.as_deref().is_some_and(|b| hot_bbo_not_older_than_l2(b, l2.as_deref()))
        {
            let book = bbo.as_ref().expect("checked above");
            if hl_bbo_hot_depth_sufficient(&ctx.scale, book.as_ref(), hedge_side, hedge_qty, depth_multiple) {
                return Some(SelectedHlHotBook {
                    source: HlQuoteSource::Bbo,
                    book: Arc::clone(book),
                    age_ms: bbo_age_ms,
                });
            }
        }

        if l2_ok {
            return l2.map(|book| SelectedHlHotBook { source: HlQuoteSource::L2, book, age_ms: l2_age_ms });
        }

        None
    }

    /// Try to hydrate a hot hedge-source choice to a matching raw book for the existing
    /// Decimal IOC crossing-price calculation. If the raw book has not caught up to the hot
    /// snapshot yet, return None and let the caller use the existing Decimal fallback path.
    fn hydrate_hl_hot_hedge_book(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
        selected: &SelectedHlHotBook,
    ) -> Option<SelectedHlBook> {
        let cell = self.cell(market, VenueTag::Hyperliquid)?;
        if cell.stream_down() || cell.is_divergent() { return None; }
        let max_stale_ms = self.cfg.live.max_book_staleness_ms;
        let depth_multiple = self.cfg.quote.depth_liquidity_multiple;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        let l2_ok = l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book);

        match selected.source {
            HlQuoteSource::Bbo => {
                let bbo_age_ms = cell.bbo_age_ms(now_ns);
                let bbo = cell.load_bbo()?;
                if bbo_age_ms > max_stale_ms
                    || !executable_quote_book(bbo.as_ref())
                    || bbo.exch_ts.timestamp_millis() < selected.book.exch_ms
                    || !bbo_not_older_than_l2(bbo.as_ref(), l2.as_deref())
                {
                    return None;
                }
                if !hl_bbo_depth_sufficient(bbo.as_ref(), hedge_side, hedge_qty, depth_multiple) {
                    return None;
                }
                Some(SelectedHlBook {
                    source: HlQuoteSource::Bbo,
                    path: HlHedgePath::Hot,
                    book: bbo,
                    age_ms: selected.age_ms,
                })
            }
            HlQuoteSource::L2 => {
                let l2 = l2?;
                if !l2_ok || l2.exch_ts.timestamp_millis() < selected.book.exch_ms {
                    return None;
                }
                Some(SelectedHlBook {
                    source: HlQuoteSource::L2,
                    path: HlHedgePath::Hot,
                    book: l2,
                    age_ms: selected.age_ms,
                })
            }
        }
    }

    fn fresh_hl_hedge_book_hot_first(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
    ) -> Option<SelectedHlBook> {
        if let Some(hot) = self.fresh_hl_hedge_hot(market, now_ns, hedge_side, hedge_qty) {
            if let Some(raw) = self.hydrate_hl_hot_hedge_book(market, now_ns, hedge_side, hedge_qty, &hot) {
                return Some(raw);
            }
        }
        self.fresh_hl_hedge_book(market, now_ns, hedge_side, hedge_qty)
    }

    /// Fresh executable Aster touch source for fill-time diagnostics: prefer BBO when it is
    /// fresh and not exchange-older than the installed L2 book, otherwise use fresh L2.
    fn fresh_aster_touch_book(&self, market: &MarketId, now_ns: i64) -> Option<SelectedAsterTouch> {
        let cell = self.cell(market, VenueTag::Aster)?;
        if cell.stream_down() || cell.is_divergent() { return None; }
        let max_stale_ms = self.cfg.live.max_book_staleness_ms;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        let l2_ok = l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book);

        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo();
        if bbo_age_ms <= max_stale_ms
            && bbo
                .as_deref()
                .is_some_and(|b| executable_quote_book(b) && bbo_not_older_than_l2(b, l2.as_deref()))
        {
            return bbo.map(|book| SelectedAsterTouch { source: AsterQuoteSource::Bbo, book, age_ms: bbo_age_ms });
        }

        if l2_ok {
            return l2.map(|book| SelectedAsterTouch { source: AsterQuoteSource::L2, book, age_ms: l2_age_ms });
        }

        None
    }

    fn has_open_aster_bot_orders_in(&self, snap: &super::account::AccountSnapshot) -> bool {
        snap.open_orders.iter().any(|o| {
            o.venue == Venue::Aster && self.ctx.contains_key(&o.market) && o.is_bot_order()
        })
    }

    fn request_safety_sweep(&mut self, now_ns: i64, reason: &'static str) {
        self.revoke_makers();
        let should_send = self
            .sweep_pending
            .is_none_or(|s| now_ns.saturating_sub(s.last_attempt_ns) >= (self.cfg.live.aster.safety_sweep_retry_ms as i64).saturating_mul(1_000_000));
        if !should_send {
            return;
        }

        let requested_ns = self.sweep_pending.map(|s| s.requested_ns).unwrap_or(now_ns);
        match self.try_send_aster_cmd(ExecCommand::CancelAllBot, AsterCommandPriority::Safety, now_ns) {
            ExecDispatch::Sent => {
                warn!("safety sweep requested: {reason}");
                self.journal.reason(now_ns, "safety_sweep", None, reason);
                self.sweep_pending = Some(SweepState { requested_ns, last_attempt_ns: now_ns, reason });
            }
            ExecDispatch::BudgetBlocked => {
                self.note_aster_budget_block(now_ns, reason, AsterCommandPriority::Safety);
                self.sweep_pending = Some(SweepState { requested_ns, last_attempt_ns: now_ns, reason });
                self.freeze(now_ns, "safety_sweep_budget_blocked");
            }
            ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                error!("CRITICAL: could not enqueue safety sweep ({reason}); will retry");
                self.sweep_pending = Some(SweepState { requested_ns, last_attempt_ns: now_ns, reason });
                self.freeze(now_ns, "safety_sweep_dispatch_failed");
            }
        }
    }

    fn drive_safety_sweep(&mut self, now_ns: i64) {
        let Some(sweep) = self.sweep_pending else {
            return;
        };

        let snap = self.account.load();
        // Trust only a snapshot whose reads began after the original sweep request. While a sweep
        // is pending, maker quoting is gated, so a fresh snapshot with no bot-owned Aster orders is
        // enough to clear local slots; retry timestamps are delivery backoff, not a new proof bar.
        if snap.read_start_ns > sweep.requested_ns && self.uncertain_makers.is_empty() && !self.has_open_aster_bot_orders_in(&snap) {
            for (m, side) in self.orders.live_slots() {
                self.orders.on_closed(&m, side);
            }
            self.sweep_pending = None;
            warn!("safety sweep confirmed clean by account snapshot");
            self.journal.reason(now_ns, "safety_sweep_clean", None, sweep.reason);
            return;
        }

        if now_ns.saturating_sub(sweep.last_attempt_ns) >= (self.cfg.live.aster.safety_sweep_retry_ms as i64).saturating_mul(1_000_000) {
            self.request_safety_sweep(now_ns, sweep.reason);
        }
    }

    /// Per-market feed freshness — the per-`(market)` analogue of the global watchdog
    /// `TradingGate`, which over-broadly halts ALL pairs when ANY single feed is stale. This
    /// market may quote only when its Aster book is fresh and Lighter has fresh quote-touch
    /// data (fast BBO or L2 snapshot) AND neither side is REST-divergent,
    /// so a stale or divergent feed on one pair no longer suppresses quoting on every other
    /// pair. Connection-staleness (the watchdog's 60 s reconnect threshold) is subsumed: a
    /// dead socket implies a stale book, which the tighter book-staleness test already catches.
    /// Uses the monotonic `now_ns` (same clock `publish` stamps), matching the watchdog scan.
    fn market_feeds_fresh(&self, market: &MarketId, now_ns: i64) -> bool {
        let max_stale = self.cfg.live.max_book_staleness_ms;
        let aster_fresh = self
            .cell(market, VenueTag::Aster)
            // Aster depth is still the queue/depth source, but a fresh bookTicker/BBO is
            // sufficient for live quote-touch safety on quiet event-driven depth feeds.
            // stream_down: the connector KNOWS the stream dropped — the last book may still
            // read young, but it is blind; close the gate immediately, not at age expiry.
            .is_some_and(|c| c.quote_age_ms(now_ns) <= max_stale && !c.is_divergent() && !c.stream_down());
        let hl_fresh = self
            .cell(market, VenueTag::Hyperliquid)
            .is_some_and(|c| c.quote_age_ms(now_ns) <= max_stale && !c.is_divergent() && !c.stream_down());
        aster_fresh && hl_fresh
    }

    fn position_context(&self, market: &MarketId, now_ns: i64) -> PositionContext {
        let a = self.aster_pos.get(market).copied().unwrap_or_default();
        let h = self.hl_pos.get(market).copied().unwrap_or_default();
        let mut a_cap = self.cfg.capital.aster_cap_notional();
        let mut h_cap = self.cfg.capital.hyperliquid_cap_notional();
        if self.cfg.live.margin_guard.enabled {
            let snap = self.account.load();
            let mark = self.book(market, VenueTag::Hyperliquid).and_then(|b| b.mid()).unwrap_or(Decimal::ZERO);
            let fresh = |origin: i64| origin > 0 && now_ns.saturating_sub(origin) / 1_000_000 <= self.cfg.live.max_account_snapshot_age_ms;
            let margin_cap = |venue, free: Decimal, buffer: Decimal, origin: i64| {
                if !fresh(origin) || mark <= Decimal::ZERO { return Decimal::ZERO; }
                snap.reported_position(venue, market).abs() * mark + (free - buffer).max(Decimal::ZERO)
            };
            a_cap = a_cap.min(margin_cap(Venue::Aster, snap.aster_available_usd,
                self.cfg.live.margin_guard.aster_safety_buffer_usd, snap.aster_margin_source_ns));
            h_cap = h_cap.min(margin_cap(Venue::Hyperliquid, snap.hl_withdrawable_usd,
                self.cfg.live.margin_guard.lighter_safety_buffer_usd, snap.hl_margin_source_ns));
        }
        PositionContext { aster_pos_qty: a.qty, hl_pos_qty: h.qty, aster_cap_notional: a_cap,
            hl_cap_notional: h_cap, enforce: self.cfg.capital.enforce_position_cap,
            reduce_position_only: self.cfg.live.quote.reduce_position_only }
    }

    /// Reserve the full interval of still-possible positions, including resting
    /// makers' future hedges. Free margin already excludes reported-position margin.
    fn margin_allows(&self, market: &MarketId, maker_side: Side, qty: Decimal, price: Decimal, now_ns: i64) -> bool {
        if !self.cfg.live.margin_guard.enabled { return true; }
        let Some(ctx) = self.ctx.get(market) else { return false };
        let snap = self.account.load();
        let a = self.aster_pos.get(market).map(|p| p.qty).unwrap_or_default();
        let h = self.hl_pos.get(market).map(|p| p.qty).unwrap_or_default();
        let mark = self.book(market, VenueTag::Hyperliquid).and_then(|b| b.mid()).unwrap_or(price)
            .max(price) * (Decimal::ONE + self.cfg.live.hyperliquid.normal_slippage_bps / Decimal::from(10_000));
        for (venue, predicted, free, buffer, origin) in [
            (Venue::Aster, a, snap.aster_available_usd, self.cfg.live.margin_guard.aster_safety_buffer_usd, snap.aster_margin_source_ns),
            (Venue::Hyperliquid, h, snap.hl_withdrawable_usd, self.cfg.live.margin_guard.lighter_safety_buffer_usd, snap.hl_margin_source_ns),
        ] {
            if origin <= 0 || now_ns.saturating_sub(origin) / 1_000_000 > self.cfg.live.max_account_snapshot_age_ms { return false; }
            let mut buys = Decimal::ZERO;
            let mut sells = Decimal::ZERO;
            for side in [Side::Buy, Side::Sell] {
                let mut possible = ctx.scale.lots_to_qty(self.orders.potential_lots(market, side));
                if side == maker_side { possible += qty; }
                let leg_side = if venue == Venue::Aster { side } else { side.opposite() };
                if leg_side == Side::Buy { buys += possible; } else { sells += possible; }
            }
            for intent in self.hedges.values().filter(|i| &i.market == market && i.venue == venue && i.unresolved()) {
                if intent.hedge_side == Side::Buy { buys += intent.remaining_qty(); } else { sells += intent.remaining_qty(); }
            }
            if venue == Venue::Hyperliquid {
                let pending = self.pending.get(market).map(|p| p.signed_qty).unwrap_or_default();
                if pending < Decimal::ZERO { buys += -pending; } else { sells += pending; }
            }
            let reported = snap.reported_position(venue, market);
            let worst = (predicted + buys).abs().max((predicted - sells).abs());
            let required = (worst - reported.abs()).max(Decimal::ZERO) * mark;
            if required > (free - buffer).max(Decimal::ZERO) { return false; }
        }
        true
    }

    /// The reason new maker quoting is currently closed for `market`, or `None` if it may quote.
    /// Builds the full [`MakerGateInputs`] and runs the canonical [`evaluate_maker_gate`] (reopen
    /// conditions and orphan-leg invariants), then the cooldown. Risk-reducing actions ignore this.
    /// `Some(reason)` is the human-readable cause (a [`FreezeReason`] string or `"COOLDOWN"`) so a
    /// closure can be surfaced instead of silently stopping quotes — see
    /// [`note_quote_gate`](Self::note_quote_gate).
    fn maker_gate_reason(&self, market: &MarketId, now_ns: i64) -> Option<&'static str> {
        if self.draining { return Some("QUIESCING"); }
        if self.yield_state != Yield::Idle { return Some(MAKER_GATE_YIELDED); }
        if self.pause_flag.as_ref().is_some_and(|p| p.load(std::sync::atomic::Ordering::Acquire)) {
            return Some(MAKER_GATE_NETWORK_PAUSE);
        }
        if !self.uncertain_makers.is_empty() { return Some("MAKER_EXECUTION_UNCERTAIN"); }
        if !self.journal.healthy() { return Some("JOURNAL_UNHEALTHY"); }
        if self.correction_needed.contains(market) { return Some("RESIDUAL_CORRECTION"); }
        if self.hedge_readiness.as_ref().is_some_and(|r| !r.is_ready()) { return Some("HEDGE_TRANSPORT_NOT_READY"); }
        if self.exec_tx.is_closed() || self.hedge_tx.is_closed() { return Some("EXECUTION_WORKER_UNAVAILABLE"); }
        if self.sweep_pending.is_some() {
            return Some("SAFETY_SWEEP_PENDING");
        }
        if self.frozen && self.clean_start {
            return Some(MAKER_GATE_FROZEN);
        }
        let inputs = MakerGateInputs {
            clean_start_done: self.clean_start,
            // Per-market (NOT the global watchdog gate): only THIS market's own Aster+Lighter feed
            // freshness gates it, so one stale low-liquidity pair no longer pulls resting
            // quotes on every other pair. The global gate stays a logged gauge in the watchdog.
            feed_gate_open: self.market_feeds_fresh(market, now_ns),
            account_fresh: self.account.age_ms(now_ns) <= self.cfg.live.max_account_snapshot_age_ms,
            // Aster fill stream liveness: a silently-dead stream stops us SEEING fills, so
            // freeze new quoting (the reconciler backstop still recovers any orphan meanwhile). If
            // the stream isn't wired, default fresh to avoid a startup deadlock.
            aster_stream_fresh: self
                .aster_stream
                .as_ref()
                .is_none_or(|s| s.age_ms(now_ns) <= self.cfg.live.max_user_stream_staleness_ms),
            positions_reconciled: self.positions_reconciled(),
            no_orphan_hedge: !self.has_orphan_hedge(),
            unhedged_within_limits: self.unhedged_within_limits(now_ns),
        };
        if let Err(reason) = evaluate_maker_gate(&inputs) {
            return Some(reason.as_str());
        }
        if self.cooldown.active(now_ns, market) {
            return Some("COOLDOWN");
        }
        None
    }

    /// Whether new maker quoting is currently allowed (the gate is open). Side-effect-free wrapper
    /// over [`maker_gate_reason`](Self::maker_gate_reason); production drives the gate through
    /// [`note_quote_gate`](Self::note_quote_gate) (which also logs transitions), so this is used
    /// only by tests.
    #[cfg(test)]
    fn may_quote(&self, market: &MarketId, now_ns: i64) -> bool {
        self.maker_gate_reason(market, now_ns).is_none()
    }

    /// Evaluate the maker gate for `market`, LOGGING + journaling the transition so a lasting
    /// suppression is never invisible — the failure mode where the gate closes (orphan hedge /
    /// unhedged-over-limit / stale account snapshot / stale feed / position mismatch) and quoting
    /// silently stops with no log and no `frozen` latch. Returns whether quoting is allowed. Logs
    /// only on a *persistent* closure (past a short grace ≈ the normal post-trade cooldown) and on
    /// resume, so routine cooldowns / one-tick feed blips never spam the log.
    fn note_quote_gate(&mut self, market: &MarketId, now_ns: i64) -> bool {
        let reason = self.maker_gate_reason(market, now_ns);
        // Never log a closure shorter than this: a normal post-fill COOLDOWN (and brief feed blips)
        // clear well within it; a real latch (orphan / stale snapshot) lasts far longer.
        let grace_ns = self.cooldown_ns.saturating_mul(2).max(5_000_000_000);
        match reason {
            Some(r) => {
                self.orders.revoke_queued(market, Side::Buy);
                self.orders.revoke_queued(market, Side::Sell);
                match self.quote_suppressed.get(market).copied() {
                    // New closure, or the reason changed: (re)start the timer; not yet logged.
                    Some((_, prev_r, _)) if prev_r != r => {
                        self.quote_suppressed.insert(market.clone(), (now_ns, r, false));
                    }
                    None => {
                        self.quote_suppressed.insert(market.clone(), (now_ns, r, false));
                    }
                    // Same reason, already logged: nothing to do (no spam).
                    Some((_, _, true)) => {}
                    // Same reason, not yet logged: log ONCE it has persisted past the grace.
                    Some((since_ns, _, false)) => {
                        if now_ns.saturating_sub(since_ns) >= grace_ns {
                            warn!("maker quoting SUPPRESSED on {market}: {r} (gate closed, placing no new quotes)");
                            self.journal.reason(now_ns, "quote_suppressed", Some(market.0.clone()), r);
                            self.quote_suppressed.insert(market.clone(), (since_ns, r, true));
                        }
                    }
                }
                let user_stream_stale = r == "ASTER_USER_STREAM_STALE";
                let should_sweep = r != "COOLDOWN"
                    && r != "SAFETY_SWEEP_PENDING"
                    && r != MAKER_GATE_FROZEN
                    // The gate-closed cancel pulls quotes, and offline the deadman does: a
                    // cancel-all sweep would only fail every 2 s through an outage.
                    && r != MAKER_GATE_NETWORK_PAUSE
                    // Yielding cancels both sides itself.
                    && r != MAKER_GATE_YIELDED
                    && (self.cfg.live.cancel_all_on_gate_close
                        || (user_stream_stale && self.cfg.live.cancel_all_on_user_stream_stale));
                if should_sweep {
                    self.request_safety_sweep(now_ns, r);
                }
                false
            }
            None => {
                // Gate open. If we had logged a suppression, announce the resume with its duration.
                if let Some((since_ns, prev_r, logged)) = self.quote_suppressed.remove(market) {
                    if logged {
                        let secs = now_ns.saturating_sub(since_ns) as f64 / 1e9;
                        info!("maker quoting RESUMED on {market} after {secs:.1}s suppressed ({prev_r})");
                        self.journal.reason(now_ns, "quote_resumed", Some(market.0.clone()), prev_r);
                    }
                }
                true
            }
        }
    }

    /// Refresh the per-market Lighter mid mark cache. Called once per wake/tick batch to avoid
    /// O(N²) book loads in `positions_reconciled` (which is called per-market inside
    /// `reprice_market`, and iterates all markets internally).
    fn refresh_mark_cache(&mut self) {
        self.mark_cache.clear();
        for m in &self.markets {
            let mark = self
                .book(m, VenueTag::Hyperliquid)
                .and_then(|b| b.mid())
                .unwrap_or(Decimal::ZERO);
            if mark > Decimal::ZERO {
                self.mark_cache.insert(m.clone(), mark);
            }
        }
    }

    /// True when every market's predicted position agrees with the exchange-reported snapshot
    /// within `max_position_mismatch_usd`. A single mismatch ⇒ freeze (returns
    /// false).
    fn positions_reconciled(&self) -> bool {
        let snap = self.account.load();
        let tol = self.cfg.live.max_position_mismatch_usd;
        for m in &self.markets {
            let mark = self.mark_cache.get(m).copied().unwrap_or(Decimal::ZERO);
            if mark <= Decimal::ZERO {
                continue; // no mark ⇒ can't judge; don't spuriously freeze on a missing book
            }
            let pred_a = self.aster_pos.get(m).map(|p| p.qty).unwrap_or(Decimal::ZERO);
            let rep_a = snap.reported_position(super::account::Venue::Aster, m);
            let pred_h = self.hl_pos.get(m).map(|p| p.qty).unwrap_or(Decimal::ZERO);
            let rep_h = snap.reported_position(super::account::Venue::Hyperliquid, m);
            if position_mismatch(pred_a, rep_a, mark, tol) || position_mismatch(pred_h, rep_h, mark, tol) {
                return false;
            }
        }
        true
    }

    /// True while the total in-flight (not yet hedged) Aster notional and the oldest unhedged
    /// fill's age are both within the configured limits. An
    /// in-flight hedge's outstanding leg is the risk; resolved hedges don't count.
    fn unhedged_within_limits(&self, now_ns: i64) -> bool {
        let max_notional = self.cfg.live.max_unhedged_notional_usd;
        let max_age_ns = self.cfg.live.max_unhedged_age_ms.max(0) * 1_000_000;
        let mut total_notional = Decimal::ZERO;
        for h in self.hedges.values() {
            if !h.state.is_in_flight() {
                continue;
            }
            let mark = self
                .book(&h.market, VenueTag::Hyperliquid)
                .and_then(|b| b.mid())
                .unwrap_or(h.aster_fill_px);
            total_notional += h.remaining_qty() * mark.abs();
            if now_ns.saturating_sub(h.created_ns) > max_age_ns {
                return false; // an unhedged leg has aged out
            }
        }
        total_notional <= max_notional
    }

    /// DIAGNOSTIC (live): log loop-liveness + the exact per-side quote decision, so a silent
    /// no-quote state is explainable — is the maker gate closed (and why), is `compute_desired_quote`
    /// REJECTING (and why), or is a side already resting? Called on a throttle from `run_strategy`;
    /// read-only, never changes behaviour. If these lines stop appearing the strategy loop itself
    /// has stalled; if they keep appearing the loop is alive and the reason field explains the
    /// no-quote. (Added to root-cause the stuck-after-fill no-quote without a blind redeploy.)
    pub fn log_quote_diag(&self, now_ns: i64) {
        for market in &self.markets {
            let decisions = self.registry.market_idx(market).map(|idx| self.gen_slots[idx.0 as usize].decisions).unwrap_or(["NOT_EVALUATED"; 2]);
            let aster_age_ms = self.cell(market, VenueTag::Aster).map(|c| c.quote_age_ms(now_ns)).unwrap_or(i64::MAX);
            let lighter_age_ms = self.cell(market, VenueTag::Hyperliquid).map(|c| c.quote_age_ms(now_ns)).unwrap_or(i64::MAX);
            self.journal.typed(now_ns, "quote_diagnostic", Some(market.0.clone()), JournalDetail::Diagnostic(DiagnosticRecord {
                gate: self.maker_gate_reason(market, now_ns).unwrap_or("OPEN"),
                aster_qty: self.aster_pos.get(market).map(|p| p.qty).unwrap_or_default(),
                lighter_qty: self.hl_pos.get(market).map(|p| p.qty).unwrap_or_default(),
                pending_qty: self.pending.get(market).map(|p| p.signed_qty).unwrap_or_default(),
                outstanding_attempts: self.hedges.values().filter(|h| &h.market == market).count(),
                aster_age_ms, lighter_age_ms, bid_decision: decisions[0], ask_decision: decisions[1],
            }), "confirmed");
        }
    }

    fn note_decision(&mut self, market: &MarketId, side: Side, reason: &'static str) {
        if let Some(idx) = self.registry.market_idx(market) {
            self.gen_slots[idx.0 as usize].decisions[if side == Side::Buy { 0 } else { 1 }] = reason;
        }
    }

    /// Re-evaluate one market (both sides) and emit the resulting commands.
    /// When `force` is false (wake path), skip if neither venue's book generation changed.
    pub async fn reprice_market(&mut self, market: &MarketId, now: DateTime<Utc>, now_ns: i64, force: bool) {
        let t0 = crate::hotpath::clock::mono_now_ns();
        let Some(ctx) = self.ctx.get(market) else { return };
        if !ctx.eligible {
            return; // pair not eligible under the partial policy
        }
        let spec = ctx.spec.clone();
        let scale = ctx.scale.clone();
        if !force {
            if let Some(idx) = self.registry.market_idx(market) {
                let a_gen = self.cell(market, VenueTag::Aster).map_or(0, |c| c.quote_generation());
                let h_gen = self.cell(market, VenueTag::Hyperliquid).map_or(0, |c| c.quote_generation());
                let slot = &mut self.gen_slots[idx.0 as usize];
                if a_gen == slot.last_aster_gen && h_gen == slot.last_hl_gen {
                    return;
                }
                slot.last_aster_gen = a_gen;
                slot.last_hl_gen = h_gen;
            }
        }
        let mut fast_cancelled = [false; 2]; // [Buy, Sell]
        if self.cfg.live.quote.use_hot_integer_math && !force {
            if let (Some(a_cell), Some(h_cell)) = (
                self.cell(market, VenueTag::Aster),
                self.cell(market, VenueTag::Hyperliquid),
            ) {
                let a_arc = a_cell.load_hot();
                let a_bbo_arc = a_cell.load_bbo_hot();
                let h_arc = h_cell.load_hot();
                let h_bbo_arc = h_cell.load_bbo_hot();
                if let Some(a_hot) = select_aster_hot_for_precheck(
                    a_arc.as_deref(),
                    a_bbo_arc.as_deref(),
                    now_ns,
                    self.precheck_cfg.max_book_stale_ns,
                ) {
                    for side in [Side::Buy, Side::Sell] {
                        let current = self.orders.current_hot_order(market, side);
                        let Some(h_hot) = select_hl_hot_for_precheck(
                            h_arc.as_deref(),
                            h_bbo_arc.as_deref(),
                            now_ns,
                            self.precheck_cfg.max_book_stale_ns,
                        ) else {
                            continue;
                        };
                        match hot_precheck_side(a_hot, h_hot, side, current, now_ns, &self.precheck_cfg) {
                            HotPrecheck::CancelFast(_reason) => {
                                let target = self.cancel_target(market, side, now_ns);
                                let CancelTarget::Send { client_id, venue_order_id } = target else {
                                    continue;
                                };
                                let cmd = ExecCommand::Cancel {
                                    market: market.clone(), client_id, venue_order_id,
                                };
                                match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                                    ExecDispatch::Sent => {
                                        self.orders.on_cancel_sent(market, side, now_ns);
                                        let idx = if side == Side::Buy { 0 } else { 1 };
                                        fast_cancelled[idx] = true;
                                    }
                                    ExecDispatch::BudgetBlocked => {
                                        self.note_aster_budget_block(now_ns, "fast_cancel_budget_blocked", AsterCommandPriority::RiskReducing);
                                        self.freeze_and_sweep(now_ns, "fast_cancel_budget_blocked");
                                    }
                                    ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                                        self.freeze_and_sweep(now_ns, "fast_cancel_dispatch_failed");
                                    }
                                }
                            }
                            HotPrecheck::NeedExactQuote => {}
                        }
                    }
                }
            }
        }
        let (Some(a_cell), Some(h_cell)) = (
            self.cell(market, VenueTag::Aster),
            self.cell(market, VenueTag::Hyperliquid),
        ) else {
            return;
        };
        // A direct string-to-integer hot publish may arrive a few microseconds before the raw
        // Decimal book/BBO used by exact quote placement. Use it for fast cancels above, but do
        // not place or replace from raw data until the matching full publish clears the guard.
        if self.cfg.live.quote.use_hot_integer_math && (a_cell.has_hot_only_update() || h_cell.has_hot_only_update()) {
            return;
        }

        let max_stale = self.cfg.live.max_book_staleness_ms;

        // Read freshness before the ArcSwap pointers so a concurrent publish cannot
        // pair an older book Arc with a newer stamp. At worst we skip one fresh update
        // until the next wake/tick; we never quote from a falsely-fresh stale Arc.
        let book_versions = (a_cell.content_version(), h_cell.content_version());
        let a_bbo_age_ms = a_cell.bbo_age_ms(now_ns);
        let hl_l2_age_ms = h_cell.book_age_ms(now_ns);
        let hl_bbo_age_ms = h_cell.bbo_age_ms(now_ns);

        let ab = a_cell.load();
        let a_bbo = a_cell.load_bbo();
        let hl_l2 = h_cell.load();
        let hl_bbo = h_cell.load_bbo();

        // Keep the full Aster book even if stale: evaluate_side() must be able to
        // return a cancel decision when may_quote is false. Fast/optional feeds are
        // filtered by monotonic VenueBook age before they can influence quote price.
        let Some(aster_book) = ab.as_deref() else {
            return;
        };

        let aster_bbo_book = a_bbo
            .as_deref()
            .filter(|b| a_bbo_age_ms <= max_stale && executable_quote_book(b));

        let hl_l2_book = hl_l2
            .as_deref()
            .filter(|b| hl_l2_age_ms <= max_stale && executable_quote_book(b));

        let hl_bbo_book = hl_bbo
            .as_deref()
            .filter(|b| hl_bbo_age_ms <= max_stale && executable_quote_book(b));
        let replace_unprof = self.cfg.live.quote.replace_immediately_if_unprofitable;
        // Evaluate the maker gate BEFORE building the position context. The gate's account-freshness
        // load must happen first so that, if the gate is open (snapshot fresh), the subsequent
        // position_context load sees a same-or-newer (hence still-fresh) snapshot and applies the
        // dynamic margin cap. Were pos built first, a publish landing between the two loads could open
        // the gate while pos still held the static cap (now_ns is shared, so a newer ts is only fresher).
        let may = self.note_quote_gate(market, now_ns);
        let pos = self.position_context(market, now_ns);

        for side in [Side::Buy, Side::Sell] {
            let fc_idx = if side == Side::Buy { 0 } else { 1 };
            if fast_cancelled[fc_idx] {
                continue;
            }
            let current = self.current_order_for_decision(market, side, &scale);
            let touch_status = self.expire_aster_touch_guard_if_needed(market, side, current, now_ns);
            let touch_blocked = touch_status == AsterTouchGuardStatus::Active;
            let quote_cfg = quote_cfg_for_touch_guard(&self.cfg.quote, touch_blocked, current);
            let (decision, reject) = evaluate_side_with_hl_sources(
                &self.cfg.edge,
                &quote_cfg,
                aster_book,
                aster_bbo_book,
                hl_l2_book,
                hl_bbo_book,
                side,
                &spec,
                max_stale,
                now,
                &pos,
                may,
                current,
                replace_unprof,
            );
            self.note_decision(market, side, reject.map(|r| r.as_str()).unwrap_or(match &decision {
                SideDecision::Hold => "HOLD", SideDecision::Place(_) => "PLACE", SideDecision::Replace { .. } => "REPLACE",
                SideDecision::Cancel { reason } => reason.as_str(),
            }));
            self.latch_empty_touch_reject_if_needed(market, side, &decision, reject, current, now_ns);
            self.apply_decision_with_books(market, side, decision, &scale, now_ns, Some(book_versions)).await;
        }
        crate::metrics::SINGLE_REPRICE.record((crate::hotpath::clock::mono_now_ns() - t0) as u64);
    }

    fn maker_permit(&self, market: &MarketId, now_ns: i64, versions: Option<(u64, u64)>) -> MakerPermit {
        #[cfg(test)]
        if versions.is_none() { return MakerPermit::for_test(); }
        let (a_version, h_version) = versions.expect("production quotes carry book versions");
        let ctx = &self.ctx[market];
        let max_ms = self.cfg.live.max_book_staleness_ms;
        let mut remaining_ms = max_ms;
        for age in [ctx.aster_cell.book_age_ms(now_ns), ctx.aster_cell.bbo_age_ms(now_ns),
            ctx.hedge_cell.book_age_ms(now_ns), ctx.hedge_cell.bbo_age_ms(now_ns)] {
            if age <= max_ms { remaining_ms = remaining_ms.min(max_ms.saturating_sub(age)); }
        }
        remaining_ms = remaining_ms.min(self.cfg.live.max_account_snapshot_age_ms.saturating_sub(self.account.age_ms(now_ns)));
        MakerPermit::new([(ctx.aster_cell.clone(), a_version), (ctx.hedge_cell.clone(), h_version)],
            self.maker_epoch.clone(), now_ns.saturating_add(remaining_ms.max(0).saturating_mul(1_000_000)), self.hedge_readiness.clone())
    }

    #[cfg(test)]
    async fn apply_decision(&mut self, market: &MarketId, side: Side, decision: SideDecision, scale: &MarketScale, now_ns: i64) {
        self.apply_decision_with_books(market, side, decision, scale, now_ns, None).await;
    }

    async fn apply_decision_with_books(&mut self, market: &MarketId, side: Side, decision: SideDecision, scale: &MarketScale, now_ns: i64, versions: Option<(u64, u64)>) {
        match decision {
            SideDecision::Hold => {}
            SideDecision::Cancel { reason } => {
                let target = self.cancel_target(market, side, now_ns);
                let CancelTarget::Send { client_id, venue_order_id } = target else {
                    return;
                };
                // Dispatch FIRST; mutate local state only if the command is actually queued.
                // A dropped cancel that silently desyncs local state is a safety hazard.
                let cmd = ExecCommand::Cancel { market: market.clone(), client_id, venue_order_id };
                match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                    ExecDispatch::Sent => {
                        self.orders.on_cancel_sent(market, side, now_ns);
                        if reason == ReplaceReason::QuoteTooCloseToTouch {
                            self.latch_aster_touch_guard(market, side, now_ns);
                        }
                        self.journal.typed(now_ns, "cancel", Some(market.0.clone()), JournalDetail::Quote(QuoteRecord { side, price: None, qty: None, reason: Some(reason.as_str()), client_id: self.orders.slot(market, side).and_then(|s| s.client_id.clone()) }), "confirmed");
                    }
                    ExecDispatch::BudgetBlocked => {
                        self.note_aster_budget_block(now_ns, "targeted_cancel_budget_blocked", AsterCommandPriority::RiskReducing);
                        self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                    }
                    ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                        warn!("exec queue full/closed: cancel NOT sent for {market} {side:?}; freezing + safety sweep");
                        self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                    }
                }
            }
            SideDecision::Place(desired) => {
                if !self.margin_allows(market, side, desired.qty, desired.price, now_ns) { self.note_decision(market, side, "MARGIN_INSUFFICIENT"); return; }
                if let Some(&suppress_ns) = self.margin_suppressed.get(&(market.clone(), side)) {
                    if now_ns.saturating_sub(suppress_ns) < 10_000_000_000 {
                        return;
                    }
                    self.margin_suppressed.remove(&(market.clone(), side));
                    info!("margin suppression expired for {market} {side:?}");
                }
                if !self.orders.replace_rate_ok(market, self.cfg.live.quote.effective_max_replaces_per_minute_per_symbol(), now_ns) {
                    return;
                }
                let price_ticks = scale.price_to_ticks(desired.price);
                let qty_lots = scale.qty_to_lots(desired.qty);
                if qty_lots <= 0 {
                    return;
                }
                if self.exec_queue_low_for_optional_work() {
                    debug!(
                        "exec queue backpressure: skipping optional place for {market} {side:?} (capacity={})",
                        self.exec_tx.capacity()
                    );
                    return;
                }
                if let Some(cid) = self.orders.next_client_id(market, side) {
                    let permit = self.maker_permit(market, now_ns, versions);
                    let admission = permit.admission.clone();
                    let cmd = ExecCommand::Place { permit, market: market.clone(), side, price_ticks, qty_lots, client_id: cid.clone() };
                    match self.try_send_aster_cmd(cmd, AsterCommandPriority::Optional, now_ns) {
                        ExecDispatch::Sent => {
                            self.orders.on_place_sent(market, side, cid, price_ticks, qty_lots, now_ns);
                            self.orders.bind_admission(market, side, admission);
                            self.clear_aster_touch_guard(market, side, now_ns);
                            self.journal.typed(now_ns, "place", Some(market.0.clone()), JournalDetail::Quote(QuoteRecord { side, price: Some(desired.price), qty: Some(desired.qty), reason: None, client_id: self.orders.slot(market, side).and_then(|s| s.client_id.clone()) }), "confirmed");
                        }
                        ExecDispatch::BudgetBlocked => {
                            debug!("Aster command budget/backoff: optional place deferred for {market} {side:?}");
                        }
                        ExecDispatch::QueueFull => {
                            warn!("exec queue full: optional place deferred for {market} {side:?}");
                        }
                        ExecDispatch::QueueClosed => {
                            error!("exec queue closed: place NOT sent for {market} {side:?}; freezing");
                            self.freeze(now_ns, "exec_queue_closed");
                        }
                    }
                }
            }
            SideDecision::Replace { desired, reason } => {
                if !self.margin_allows(market, side, desired.qty, desired.price, now_ns) {
                    self.note_decision(market, side, "MARGIN_INSUFFICIENT");
                    self.cancel_both_sides(market, now_ns);
                    return;
                }
                if let Some(&suppress_ns) = self.margin_suppressed.get(&(market.clone(), side)) {
                    if now_ns.saturating_sub(suppress_ns) < 10_000_000_000 {
                        return;
                    }
                    self.margin_suppressed.remove(&(market.clone(), side));
                    info!("margin suppression expired for {market} {side:?}");
                }
                if self.cfg.live.quote.reduce_position_only && reason == ReplaceReason::NoLongerProfitable {
                    let target = self.cancel_target(market, side, now_ns);
                    let CancelTarget::Send { client_id, venue_order_id } = target else {
                        return;
                    };
                    let cmd = ExecCommand::Cancel {
                        market: market.clone(),
                        client_id,
                        venue_order_id,
                    };
                    match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                        ExecDispatch::Sent => {
                            self.orders.on_cancel_sent(market, side, now_ns);
                            self.journal.reason(now_ns, "cancel", Some(market.0.clone()), "NO_LONGER_PROFITABLE_CANCEL_ONLY");
                        }
                        ExecDispatch::BudgetBlocked => {
                            self.note_aster_budget_block(now_ns, "no_longer_profitable_cancel_only", AsterCommandPriority::RiskReducing);
                            self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                        }
                        ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                            warn!("exec queue full/closed: no-longer-profitable cancel-only NOT sent for {market} {side:?}; freezing + safety sweep");
                            self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                        }
                    }
                    return;
                }
                // Requote pacing: honor `min_requote_interval_ms` for NON-URGENT requotes
                // (price/qty drift). Outside reduce-only cancel-only mode above, an urgent
                // `NoLongerProfitable` replace BYPASSES this small per-side throttle, but it still
                // obeys the global Aster command budget/backoff below.
                // Deliberately so: urgent no-longer-profitable work must be risk-reducing, not
                // an unbounded cancel+place flood that consumes the safety queue and trips venue 429s.
                let non_urgent = matches!(&reason, ReplaceReason::PriceChanged | ReplaceReason::QuantityChanged);
                if non_urgent
                    && !self
                        .orders
                        .slot(market, side)
                        .is_some_and(|s| s.throttle_ok(now_ns, self.cfg.live.quote.min_requote_interval_ms))
                {
                    return; // too soon since the last requote on this side — skip this non-urgent replace
                }
                if !self.orders.replace_rate_ok(market, self.cfg.live.quote.effective_max_replaces_per_minute_per_symbol(), now_ns) {
                    return;
                }
                let price_ticks = scale.price_to_ticks(desired.price);
                let qty_lots = scale.qty_to_lots(desired.qty);
                if qty_lots <= 0 {
                    return;
                }
                let old_cid = match self.orders.slot(market, side) {
                    Some(s) if s.state == OrderLifecycle::Open => s.client_id.clone(),
                    None => None,
                    Some(_) => return,
                };
                let Some(old_cid) = old_cid else { return };
                if self.sweep_pending.is_some() {
                    return;
                }
                let full_replace_budget_ok = self.aster_budget_allows(AsterCommandPriority::Optional, 2, now_ns);
                if non_urgent && (self.exec_queue_low_for_optional_work() || !full_replace_budget_ok) {
                    debug!(
                        "exec queue/budget backpressure: skipping optional replace for {market} {side:?} ({}) (capacity={} budget_ok={})",
                        reason.as_str(),
                        self.exec_tx.capacity(),
                        full_replace_budget_ok
                    );
                    return;
                }
                if !non_urgent && (self.exec_queue_low_for_optional_work() || !full_replace_budget_ok) {
                    // Under backpressure, prefer a cancel-only risk reduction over adding a cancel+place
                    // replace. This drains stale/unprofitable exposure while preserving queue reserve.
                    let target = self.cancel_target(market, side, now_ns);
                    let CancelTarget::Send { client_id, venue_order_id } = target else {
                        return;
                    };
                    let cmd = ExecCommand::Cancel {
                        market: market.clone(),
                        client_id,
                        venue_order_id,
                    };
                    match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                        ExecDispatch::Sent => {
                            self.orders.on_cancel_sent(market, side, now_ns);
                            self.journal.reason(now_ns, "cancel", Some(market.0.clone()), "BACKPRESSURE_CANCEL_ONLY");
                        }
                        ExecDispatch::BudgetBlocked => {
                            self.note_aster_budget_block(now_ns, "urgent_cancel_only_budget_blocked", AsterCommandPriority::RiskReducing);
                            self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                        }
                        ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                            warn!("exec queue full/closed: backpressure cancel-only NOT sent for {market} {side:?}; freezing + safety sweep");
                            self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                        }
                    }
                    return;
                }
                if let Some(new_cid) = self.orders.next_client_id(market, side) {
                    let permit = self.maker_permit(market, now_ns, versions);
                    let admission = permit.admission.clone();
                    let cmd = ExecCommand::Replace {
                        permit,
                        market: market.clone(),
                        side,
                        old_client_id: old_cid,
                        new_client_id: new_cid.clone(),
                        price_ticks,
                        qty_lots,
                    };
                    let priority = if non_urgent { AsterCommandPriority::Optional } else { AsterCommandPriority::RiskReducing };
                    match self.try_send_aster_cmd(cmd, priority, now_ns) {
                        ExecDispatch::Sent => {
                            // Keep the old client id active until its cancel is verified. The worker
                            // emits CancelAck(old) before PlaceAck(new); only then do we promote the
                            // replacement to PendingPlace. This preserves fill/cancel attribution during
                            // the cancel-then-place race window.
                            self.orders.on_replace_sent(market, side, new_cid, price_ticks, qty_lots, now_ns);
                            self.orders.bind_admission(market, side, admission);
                            self.clear_aster_touch_guard(market, side, now_ns);
                            self.journal.typed(now_ns, "replace", Some(market.0.clone()), JournalDetail::Quote(QuoteRecord { side, price: Some(desired.price), qty: Some(desired.qty), reason: Some(reason.as_str()), client_id: self.orders.slot(market, side).and_then(|s| s.client_id.clone()) }), "confirmed");
                        }
                        ExecDispatch::BudgetBlocked if non_urgent => {
                            debug!("Aster command budget/backoff: optional replace deferred for {market} {side:?} ({})", reason.as_str());
                        }
                        ExecDispatch::BudgetBlocked => {
                            self.note_aster_budget_block(now_ns, "urgent_replace_budget_blocked", AsterCommandPriority::RiskReducing);
                            self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                        }
                        ExecDispatch::QueueFull if non_urgent => {
                            warn!("exec queue full: optional replace deferred for {market} {side:?} ({})", reason.as_str());
                        }
                        ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                            warn!("exec queue full/closed: replace NOT sent for {market} {side:?}; freezing + safety sweep");
                            self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                        }
                    }
                }
            }
        }
    }

    /// Handle an Aster maker fill from the user stream.
    /// Exactly-once hedging: a deduped repeat is ignored. Triggers the
    /// post-trade cooldown and cancels the residual on that side.
    pub async fn handle_maker_fill(&mut self, fill: AsterFill, now_ns: i64) {
        if !self.orders.is_own_client_id(&fill.client_id) { return; }
        if !self.dedup.observe(&fill) { return; }
        self.revoke_makers();
        if self.uncertain_makers.contains(&fill.client_id) {
            if let (Some(ctx), Some(lots)) = (self.ctx.get(&fill.market), self.orders.expected_lots(&fill.client_id)) {
                if ctx.scale.qty_to_lots(fill.cum_filled_qty) >= lots { self.uncertain_makers.remove(&fill.client_id); }
            }
        }
        let tracked = self.hedges.values().find(|h| h.client_id.as_deref() == Some(&fill.client_id))
            .map(|h| (h.cloid, h.logical_id, h.filled_qty, h.filled_quote_usd, h.fee_usd, h.qty));
        let logical_id = tracked.map(|h| h.1)
            .or_else(|| self.maker_coverage.get(&fill.client_id).map(|c| c.logical_id))
            .unwrap_or_else(|| self.logical_id(&fill.market));
        if fill.reduce_only {
            if let Some((cloid, _, old_qty, old_quote, old_fee, requested)) = tracked {
                if fill.cum_filled_qty > old_qty {
                    let exact_increment = fill.cum_filled_qty - old_qty == fill.last_fill_qty;
                    let quote = if exact_increment { old_quote.map(|q| q + fill.last_fill_qty * fill.last_fill_px) } else { None };
                    let fee = if exact_increment { old_fee.zip(fill.usd_fee()).map(|(a, b)| a + b) } else { None };
                    self.handle_exec_event(ExecEvent::ExecutionProgress { cloid,
                        cumulative_qty: fill.cum_filled_qty, cumulative_quote_usd: quote,
                        cumulative_fee_usd: fee, terminal: fill.cum_filled_qty == requested,
                        venue_order_id: Some(fill.order_id.clone()), event_time_ms: (fill.event_time_ms > 0).then_some(fill.event_time_ms) }, now_ns);
                }
            } else if !self.maker_coverage.get(&fill.client_id).is_some_and(|c| c.qty >= fill.cum_filled_qty) {
                self.freeze_and_sweep(now_ns, "untracked_reduce_only_fill");
            }
            self.journal.maker_fill(now_ns, logical_id.to_hex(), &fill);
            return;
        }
        let previous = self.maker_coverage.get(&fill.client_id).cloned();
        let old_qty = previous.as_ref().map(|c| c.qty).unwrap_or(Decimal::ZERO);
        let delta = (fill.cum_filled_qty - old_qty).max(Decimal::ZERO);
        if delta > Decimal::ZERO {
            let exact_increment = delta == fill.last_fill_qty;
            let quote = if exact_increment {
                previous.as_ref().map(|c| c.quote).unwrap_or(Some(Decimal::ZERO))
                    .map(|q| q + fill.last_fill_qty * fill.last_fill_px)
            } else { None };
            self.maker_coverage.insert(fill.client_id.clone(), MakerCoverage { logical_id, qty: fill.cum_filled_qty, quote });
            if let Yield::Yielding { fills, .. } = &mut self.yield_state { *fills += 1; }
            let mut delta_fill = fill.clone();
            delta_fill.last_fill_qty = delta;
            self.process_maker_delta(&delta_fill, logical_id, now_ns).await;
            if !exact_increment {
                self.journal.maker_progress(now_ns, logical_id, &fill.market, fill.aster_side,
                    &fill.client_id, &fill.order_id, fill.cum_filled_qty, quote, false, (fill.event_time_ms > 0).then_some(fill.event_time_ms));
            }
        }
        // Native per-trade economics are preserved even if REST covered the quantity first.
        self.journal.maker_fill(now_ns, logical_id.to_hex(), &fill);
    }

    pub async fn handle_maker_order_progress(&mut self, market: MarketId, side: Side, client_id: String,
        order_id: String, qty: Decimal, quote: Option<Decimal>, terminal: bool, event_time_ms: i64, now_ns: i64) {
        if !self.orders.is_own_client_id(&client_id) || qty < Decimal::ZERO { return; }
        if terminal { self.uncertain_makers.remove(&client_id); }
        if qty == Decimal::ZERO && !self.maker_coverage.contains_key(&client_id) { return; }
        let logical_id = self.maker_coverage.get(&client_id).map(|c| c.logical_id).unwrap_or_else(|| self.logical_id(&market));
        let previous = self.maker_coverage.get(&client_id).cloned();
        let old_qty = previous.as_ref().map(|c| c.qty).unwrap_or(Decimal::ZERO);
        if qty < old_qty { return; }
        let delta = qty - old_qty;
        let delta_quote = quote.zip(previous.as_ref().map(|c| c.quote).unwrap_or(Some(Decimal::ZERO))).map(|(q, old)| q-old);
        self.maker_coverage.insert(client_id.clone(), MakerCoverage { logical_id, qty, quote });
        if delta > Decimal::ZERO {
            let px = delta_quote.filter(|q| *q > Decimal::ZERO).map(|q| q / delta)
                .or_else(|| self.book(&market, VenueTag::Aster).and_then(|b| b.mid())).unwrap_or(Decimal::ZERO);
            if px <= Decimal::ZERO { self.freeze_and_sweep(now_ns, "unpriced_maker_backfill"); return; }
            let fill = AsterFill { market: market.clone(), aster_side: side, order_id: order_id.clone(),
                trade_id: String::new(), client_id: client_id.clone(), last_fill_qty: delta, last_fill_px: px,
                cum_filled_qty: qty, event_time_ms, reduce_only: false, commission: None, commission_asset: None };
            self.process_maker_delta(&fill, logical_id, now_ns).await;
        }
        self.journal.maker_progress(now_ns, logical_id, &market, side, &client_id, &order_id, qty, quote, terminal, (event_time_ms > 0).then_some(event_time_ms));
    }

    async fn process_maker_delta(&mut self, fill: &AsterFill, logical_id: Cloid, now_ns: i64) {
        self.aster_pos.entry(fill.market.clone()).or_default()
            .apply_fill(SignedPosition::signed(fill.aster_side, fill.last_fill_qty), fill.last_fill_px);
        if let Some(ctx) = self.ctx.get(&fill.market) {
            self.orders.on_maker_fill_progress(&fill.market, fill.aster_side, &fill.client_id, ctx.scale.qty_to_lots(fill.cum_filled_qty));
        }
        self.last_hot_action_ns.insert(fill.market.clone(), now_ns);
        self.cooldown.trigger(now_ns, self.cooldown_ns, &fill.market);
        let Some(ctx) = self.ctx.get(&fill.market) else { self.freeze_and_sweep(now_ns, "unknown_fill_market"); return };
        let rules = HedgeabilityRules { hyperliquid_min_notional: ctx.spec.hl_min_notional, hyperliquid_qty_step: ctx.spec.hl_qty_step };
        let step = ctx.spec.hl_qty_step;
        let mark = self.fresh_hl_quote_book(&fill.market, now_ns).and_then(|b| b.book.mid()).unwrap_or(fill.last_fill_px);
        let previous = self.pending.remove(&fill.market);
        let first_fill_ts = previous.as_ref().map(|p| p.first_fill_ts).unwrap_or_else(Utc::now);
        let outcome = inventory::handle_fill_parts(fill.aster_side, fill.last_fill_qty, fill.last_fill_px,
            Utc::now(), previous, &rules, mark, self.cfg.edge.aster_maker_fee_bps / Decimal::from(10_000));
        if let Some(pending) = outcome.pending { self.pending.insert(fill.market.clone(), pending); }
        if let Some(hedge) = outcome.hedge {
            let qty = crate::decimal::floor_to_step(hedge.qty, step);
            let remainder = hedge.qty - qty;
            if remainder > Decimal::ZERO {
                self.pending.insert(fill.market.clone(), PendingInventory {
                    signed_qty: SignedPosition::signed(hedge.hedge_side.opposite(), remainder),
                    avg_aster_px: hedge.avg_aster_px, first_fill_ts, last_fill_ts: Utc::now(),
                });
            }
            let cloid = self.orders.next_attempt_id(&fill.market);
            let mut intent = HedgeIntent::with_qty(cloid, fill.market.clone(), hedge.hedge_side, qty, hedge.avg_aster_px, now_ns);
            intent.logical_id = logical_id;
            intent.arm_admission(self.cfg.live.max_unhedged_age_ms);
            let slip = self.cfg.live.hyperliquid.normal_slippage_bps;
            let source = self.fresh_hl_hedge_book_hot_first(&fill.market, now_ns, hedge.hedge_side, qty);
            intent.book_source = source.as_ref().map(|b| b.path.as_str(b.source));
            intent.book_age_ms = source.as_ref().map(|b| b.age_ms);
            let price = source.and_then(|b| crossing_hedge_px(&b.book, hedge.hedge_side, slip));
            if let Some(aggressive_px) = price.filter(|_| qty > Decimal::ZERO) {
                intent.mark_submitted(now_ns);
                self.hedges.insert(cloid.to_hex(), intent.clone());
                if self.hedge_tx.try_send(HedgeCommand::Hedge { intent, aggressive_px }).is_err() {
                    if let Some(h) = self.hedges.get_mut(&cloid.to_hex()) {
                        h.admission.cancel_queued(); h.mark_rejected(); h.terminal_ns = Some(now_ns);
                    }
                    self.correction_needed.insert(fill.market.clone());
                    self.freeze(now_ns, "hedge_dispatch_failed");
                }
            } else {
                intent.admission.cancel_queued(); intent.mark_rejected(); intent.terminal_ns = Some(now_ns);
                self.hedges.insert(cloid.to_hex(), intent);
                self.correction_needed.insert(fill.market.clone());
                self.freeze(now_ns, "hedge_source_unavailable");
            }
            self.cancel_both_sides(&fill.market, now_ns);
            if let Some(h) = self.hedges.get(&cloid.to_hex()) { self.journal.progress(now_ns, h); }
        } else { self.cancel_both_sides(&fill.market, now_ns); }
        if let Some(net) = outcome.netted {
            self.journal.typed(now_ns, "net", Some(fill.market.0.clone()), JournalDetail::Net {
                closed_qty: net.closed_qty, realized_pnl: net.realized_pnl }, "estimated");
        }
    }

    /// Cancel BOTH resting maker sides for `market` via TARGETED per-order cancels
    /// (cancel-opposite). NOT a per-symbol cancel-all (allOpenOrders) — that emits no per-order ack, so the
    /// slot tracking would desync; each targeted Cancel emits a CancelAck that closes the
    /// slot via `close_by_client_id`. Called after a fill (the post-fill
    /// cooldown means neither side should rest while we hedge).
    fn cancel_both_sides(&mut self, market: &MarketId, now_ns: i64) {
        for side in [Side::Buy, Side::Sell] {
            let target = self.cancel_target(market, side, now_ns);
            let CancelTarget::Send { client_id, venue_order_id } = target else {
                continue;
            };
            // Dispatch FIRST; a dropped post-fill cancel leaves a maker order resting (could
            // re-fill) while local state says cancelled — escalate to a freeze, never silent.
            let cmd = ExecCommand::Cancel { market: market.clone(), client_id, venue_order_id };
            match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                ExecDispatch::Sent => self.orders.on_cancel_sent(market, side, now_ns),
                ExecDispatch::BudgetBlocked => {
                    self.note_aster_budget_block(now_ns, "post_fill_cancel_budget_blocked", AsterCommandPriority::RiskReducing);
                    self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                }
                ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                    error!("CRITICAL: post-fill cancel for {market} {side:?} dropped (queue full/closed); freezing");
                    self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                }
            }
        }
    }

    /// Fold a worker/venue event back into the order + hedge state.
    pub fn handle_exec_event(&mut self, ev: ExecEvent, now_ns: i64) {
        if matches!(&ev, ExecEvent::MakerOrderProgress { .. } | ExecEvent::ExecutionProgress { .. }
            | ExecEvent::AsterFlattenAck { .. }) { self.revoke_makers(); }
        let order_evidence = match &ev {
            ExecEvent::PlaceAck { client_id, venue_order_id } => Some(JournalDetail::Order { client_id: client_id.clone(), venue_order_id: Some(venue_order_id.clone()), state: "accepted" }),
            ExecEvent::CancelAck { client_id } => Some(JournalDetail::Order { client_id: client_id.clone(), venue_order_id: None, state: "cancelled" }),
            ExecEvent::CancelFilledOrExpired { client_id } => Some(JournalDetail::Order { client_id: client_id.clone(), venue_order_id: None, state: "filled_or_expired" }),
            _ => None,
        };
        match ev {
            ExecEvent::PlaceAck { client_id, venue_order_id } => {
                if let Some((market, side, cancel_venue_order_id, cancel_reason)) =
                    self.ack_by_client_id(&client_id, venue_order_id)
                {
                    warn!(
                        "replacement {client_id} acked after {}; cancelling it immediately",
                        cancel_reason.as_str()
                    );
                    let cmd = ExecCommand::Cancel {
                        market: market.clone(),
                        client_id: client_id.clone(),
                        venue_order_id: cancel_venue_order_id,
                    };
                    if self.sweep_pending.is_some() {
                        warn!(
                            "cancel-after-ack for {market} {side:?} suppressed while safety sweep is pending; sweep owns cleanup"
                        );
                    } else {
                        match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                            ExecDispatch::Sent => {
                                self.orders.on_cancel_sent(&market, side, now_ns);
                                self.journal.reason(now_ns, "cancel_after_ack", Some(market.0.clone()), cancel_reason.as_str());
                            }
                            ExecDispatch::BudgetBlocked => {
                                self.note_aster_budget_block(now_ns, "cancel_after_ack", AsterCommandPriority::RiskReducing);
                                self.freeze(now_ns, "cancel_after_ack_budget_blocked");
                                self.request_safety_sweep(now_ns, "cancel_after_ack_budget_blocked");
                            }
                            ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                                error!(
                                    "CRITICAL: cancel-after-ack for {market} {side:?} dropped (queue/backpressure); freezing + safety sweep"
                                );
                                self.freeze(now_ns, "cancel_after_ack_dispatch_failed");
                                self.request_safety_sweep(now_ns, "cancel_after_ack_dispatch_failed");
                            }
                        }
                    }
                }
            }
            ExecEvent::PlaceReject { client_id, reason } => {
                warn!("place rejected (client {client_id}): {reason}");
                let slot_info = self.find_slot_by_client(&client_id);
                self.close_by_client_id(&client_id);
                if reason.to_ascii_lowercase().contains("insufficient") {
                    if let Some((m, side)) = slot_info {
                        if !self.margin_suppressed.contains_key(&(m.clone(), side)) {
                            warn!("margin suppressed {m} {side:?}: {reason}");
                        }
                        self.margin_suppressed.insert((m, side), now_ns);
                    }
                }
            }
            ExecEvent::PlaceUnknown { client_id, reason } => {
                self.uncertain_makers.insert(client_id.clone());
                // The order may be resting. Do NOT close the local slot. Freeze and
                // sweep/reconcile so account/openOrders becomes the source of truth.
                warn!("place outcome UNKNOWN (client {client_id}): {reason}; sweeping all bot orders + freezing");
                self.freeze(now_ns, "place_unknown");
                self.request_safety_sweep(now_ns, "place_unknown");
            }
            ExecEvent::CancelAck { client_id } => {
                self.cancel_ack_by_client_id(&client_id);
            }
            ExecEvent::CancelFilledOrExpired { client_id } => {
                // Not resting any more, so no sweep. The gate stays closed until the fill (user
                // stream) or a terminal backfill (`recover_orphans`) accounts for the order.
                self.uncertain_makers.insert(client_id.clone());
                self.cancel_ack_by_client_id(&client_id);
            }
            ExecEvent::CancelReject { client_id, reason } => {
                self.uncertain_makers.insert(client_id.clone());
                // The cancel FAILED, so the order may still be resting. Freeze and request a
                // cancel-all, but do NOT forget local slots until a newer account snapshot proves
                // no bot-owned Aster orders remain.
                warn!("cancel REJECTED (client {client_id}): {reason}; sweeping all bot orders + freezing");
                self.freeze(now_ns, "cancel_rejected");
                self.request_safety_sweep(now_ns, "cancel_rejected");
            }
            ExecEvent::MakerOrderProgress { .. } => {
                // Routed through handle_maker_fill by the driver; nothing here.
            }
            ExecEvent::AsterRateLimited { reason, backoff_ms } => {
                self.on_aster_rate_limited(now_ns, reason, backoff_ms);
            }
            ExecEvent::AttemptStarted { cloid, proof } => {
                if let Some(h) = self.hedges.get_mut(&cloid.to_hex()) {
                    self.last_hot_action_ns.entry(h.market.clone()).and_modify(|v| *v = (*v).max(proof.sent_ns)).or_insert(proof.sent_ns);
                    h.wire = Some(proof);
                    self.journal.progress(now_ns, h);
                }
            }
            ExecEvent::AttemptNotSent { cloid, reason } => {
                self.handle_definitive_reject(cloid, reason, true, now_ns);
            }
            ExecEvent::HedgeReject { cloid, reason } => {
                let retry = super::exec::hyperliquid::hedge_reject_is_definitive_no_fill(&reason);
                self.handle_definitive_reject(cloid, reason, retry, now_ns);
            }
            ExecEvent::ExecutionProgress { cloid, cumulative_qty, cumulative_quote_usd, cumulative_fee_usd, terminal, venue_order_id, event_time_ms } => {
                self.apply_execution_progress(cloid, cumulative_qty, cumulative_quote_usd, cumulative_fee_usd, terminal, venue_order_id, event_time_ms, now_ns);
            }
            ExecEvent::HedgeUnknown { cloid, reason } => {
                if let Some(h) = self.hedges.get_mut(&cloid.to_hex()) {
                    if !h.terminal { h.mark_unknown(); }
                    self.journal.progress(now_ns, h);
                }
                warn!("execution remains unresolved {}: {reason}", cloid.to_hex());
                self.freeze_and_sweep(now_ns, "execution_unknown");
            }
            ExecEvent::AsterFlattenAck { cloid, .. } => {
                if let Some(h) = self.hedges.get_mut(&cloid.to_hex()) { h.state = HedgeState::Acked; }
            }
            ExecEvent::AsterFlattenReject { cloid, reason, terminal, .. } => {
                if terminal { self.handle_definitive_reject(cloid, reason, false, now_ns); }
                else {
                    if let Some(h) = self.hedges.get_mut(&cloid.to_hex()) { h.mark_unknown(); }
                    self.freeze_and_sweep(now_ns, "aster_correction_unknown");
                }
            }
        }
        if let Some(detail) = order_evidence { self.journal.typed(now_ns, "order_update", None, detail, "confirmed"); }
        self.publish_execution_queries();
    }

    fn apply_execution_progress(&mut self, cloid: Cloid, qty: Decimal, quote: Option<Decimal>, fee: Option<Decimal>,
        terminal: bool, order_id: Option<String>, event_time_ms: Option<i64>, now_ns: i64) {
        let Some(h) = self.hedges.get_mut(&cloid.to_hex()) else { return };
        let (delta, delta_quote) = h.apply_progress(qty, quote, fee, terminal, now_ns);
        if delta > Decimal::ZERO || h.event_time_ms.is_none() { h.event_time_ms = event_time_ms; }
        if let Some(oid) = order_id { h.hl_oid = Some(oid); }
        if delta > Decimal::ZERO {
            let px = delta_quote.filter(|q| *q > Decimal::ZERO).map(|q| q / delta).unwrap_or(h.aster_fill_px);
            let positions = if h.venue == Venue::Aster { &mut self.aster_pos } else { &mut self.hl_pos };
            positions.entry(h.market.clone()).or_default().apply_fill(SignedPosition::signed(h.hedge_side, delta), px);
            self.last_hot_action_ns.insert(h.market.clone(), now_ns);
        }
        if h.venue == Venue::Aster {
            if let Some(client_id) = &h.client_id {
                self.maker_coverage.insert(client_id.clone(), MakerCoverage {
                    logical_id: h.logical_id, qty: h.filled_qty, quote: h.filled_quote_usd,
                });
            }
        }
        if terminal && h.remaining_qty() > Decimal::ZERO { self.correction_needed.insert(h.market.clone()); }
        self.journal.progress(now_ns, h);
    }

    fn handle_definitive_reject(&mut self, cloid: Cloid, reason: String, retryable: bool, now_ns: i64) {
        let Some(h) = self.hedges.get_mut(&cloid.to_hex()) else { return };
        if h.filled_qty > Decimal::ZERO {
            h.mark_unknown(); h.terminal = false;
            self.freeze_and_sweep(now_ns, "contradictory_execution_reject");
            return;
        }
        h.admission.cancel_queued(); h.mark_rejected(); h.terminal_ns = Some(now_ns);
        self.journal.progress(now_ns, h);
        let old = h.clone();
        if retryable && old.purpose == IntentPurpose::Hedge && old.attempts < 2 {
            let slip = self.cfg.live.hyperliquid.emergency_slippage_bps;
            let px = self.fresh_hl_hedge_book_hot_first(&old.market, now_ns, old.hedge_side, old.remaining_qty())
                .and_then(|b| crossing_hedge_px(&b.book, old.hedge_side, slip));
            if let Some(aggressive_px) = px {
                let next = self.orders.next_attempt_id(&old.market);
                let mut intent = HedgeIntent::with_qty(next, old.market.clone(), old.hedge_side, old.remaining_qty(), old.aster_fill_px, now_ns);
                intent.logical_id = old.logical_id;
                intent.attempts = old.attempts;
                intent.arm_admission(self.cfg.live.max_unhedged_age_ms);
                intent.mark_submitted(now_ns);
                self.hedges.insert(next.to_hex(), intent.clone());
                if self.hedge_tx.try_send(HedgeCommand::Hedge { intent, aggressive_px }).is_ok() { return; }
                if let Some(h) = self.hedges.get_mut(&next.to_hex()) {
                    h.admission.cancel_queued(); h.mark_rejected(); h.terminal_ns = Some(now_ns);
                }
            }
        }
        warn!("execution rejected {}: {reason}", cloid.to_hex());
        self.correction_needed.insert(old.market);
        self.freeze_and_sweep(now_ns, "execution_rejected");
    }

    /// Ack a placed order by client id. Returns `(market, side, venue_order_id)` when this ack
    /// belongs to a replacement that was already marked for post-fill/gate-close cancellation.
    fn ack_by_client_id(
        &mut self,
        client_id: &str,
        venue_order_id: String,
    ) -> Option<(MarketId, Side, Option<String>, CancelAfterAckReason)> {
        if let Some((m, side)) = self.find_slot_by_client(client_id) {
            if let Some(cancel_after_ack_reason) = self.orders.on_acked(&m, side, venue_order_id) {
                let venue_order_id = self
                    .orders
                    .slot(&m, side)
                    .and_then(|slot| slot.venue_order_id.clone());
                return Some((m, side, venue_order_id, cancel_after_ack_reason));
            }
        }
        None
    }
    fn cancel_ack_by_client_id(&mut self, client_id: &str) {
        if let Some((m, side)) = self.find_slot_by_client(client_id) {
            self.orders.on_cancel_acked(&m, side);
        }
    }
    fn close_by_client_id(&mut self, client_id: &str) {
        if let Some((m, side)) = self.find_slot_by_client(client_id) {
            self.orders.on_closed(&m, side);
        }
    }
    fn find_slot_by_client(&self, client_id: &str) -> Option<(MarketId, Side)> {
        for m in &self.markets {
            for side in [Side::Buy, Side::Sell] {
                if self.orders.slot(m, side).and_then(|s| s.client_id.as_deref()) == Some(client_id) {
                    return Some((m.clone(), side));
                }
            }
        }
        None
    }

    /// Whether any in-flight hedge is in a dangerous (orphan) state — feeds the risk gate.
    pub fn has_orphan_hedge(&self) -> bool {
        self.hedges.values().any(|h| h.state.is_dangerous())
    }

    /// Cumulative-loss circuit breaker. Measures TOTAL cross-venue MARKED equity
    /// (Aster wallet+unrealized + Lighter portfolio_value + marked Lighter uPnL) against a
    /// baseline armed from the median of the first [`BREAKER_BASELINE_SAMPLES`] fresh marked
    /// snapshots; a drawdown beyond `live.circuit_breaker.max_cumulative_loss_usdc` must
    /// persist for [`BREAKER_TRIP_STREAK`] consecutive fresh marked samples before tripping
    /// (accepted trade-off: a true catastrophic drawdown trips ~4-6s later — the breaker only
    /// cancels quotes and halts, positions stay open regardless — in exchange for immunity to
    /// single-sample venue glitches). On trip: cancels orders via the graceful-shutdown path,
    /// LEAVES the delta-neutral position open, writes a persistent trip latch, and stops the
    /// XEMM engine (which then refuses to restart until reset, and returns an error so the
    /// controller safe-halts in one step). Off the money path (cold tick), counting each
    /// published snapshot generation at most once. NEVER trips on untrusted data (no
    /// snapshot yet, stale snapshot, unmarked Lighter uPnL, or non-positive equity).
    fn check_circuit_breaker(&mut self, now_ns: i64) {
        if !self.cfg.live.circuit_breaker.enabled || self.breaker_tripped {
            return;
        }
        let limit = self.cfg.live.circuit_breaker.max_cumulative_loss_usdc;
        let snap = self.account.load();
        if snap.source_ts_ns == 0 || self.account.age_ms(now_ns) > self.cfg.live.max_account_snapshot_age_ms {
            // No snapshot yet or stale — don't trip on data we can't trust, and don't let
            // pre-staleness breaches persist across the gap.
            self.breaker_breach_streak = 0;
            return;
        }
        if snap.generation == self.breaker_last_generation {
            return; // same sample as last time: neither counts toward nor resets the streak
        }
        self.breaker_last_generation = snap.generation;
        if !snap.hl_upnl_marked {
            // The Lighter leg's uPnL could not be marked (missing mark or entry px): the
            // combined equity is the exact distorted metric that false-tripped 2026-07-04.
            // Skip the sample entirely (no trip, no baseline arming, streak reset).
            self.breaker_breach_streak = 0;
            return;
        }
        let equity = snap.total_equity_usd();
        if equity <= Decimal::ZERO {
            // A zero/garbage read (e.g. a failed parse) must never look like a total loss.
            self.breaker_breach_streak = 0;
            return;
        }
        let baseline = match self.breaker_baseline_equity {
            Some(b) => b,
            None => {
                // Arm from the MEDIAN of the first K fresh marked samples: one outlier
                // startup read must not set a phantom reference for the whole run.
                self.breaker_baseline_samples.push(equity);
                if self.breaker_baseline_samples.len() >= BREAKER_BASELINE_SAMPLES {
                    let mut v = self.breaker_baseline_samples.clone();
                    v.sort();
                    let b = v[v.len() / 2];
                    self.breaker_baseline_equity = Some(b);
                    info!(
                        "circuit breaker armed: baseline equity = {b} USD (median of {} samples), limit = {limit} USD",
                        v.len()
                    );
                }
                return;
            }
        };
        let loss = baseline - equity;
        if loss <= limit {
            self.breaker_breach_streak = 0;
            return;
        }
        self.breaker_breach_streak += 1;
        if self.breaker_breach_streak < BREAKER_TRIP_STREAK {
            warn!(
                "circuit breaker: breach {} of {BREAKER_TRIP_STREAK} (loss {loss} USD > limit {limit} USD); not tripping yet",
                self.breaker_breach_streak
            );
            return;
        }
        self.breaker_tripped = true;
        if let Some(flag) = &self.trip_flag { flag.store(true, std::sync::atomic::Ordering::Release); }
        // Safety dispatch precedes every persistence action. The writer owns JSON,
        // write/flush/sync and rename; a stalled filesystem cannot delay cancellation.
        self.freeze(now_ns, "circuit_breaker");
        if self.cfg.live.shutdown_cancel_all { self.request_safety_sweep(now_ns, "circuit_breaker"); }
        self.shutdown.cancel();
        self.journal.trip(super::breaker::TripSnapshot {
            ts_ms: Utc::now().timestamp_millis(),
            market: self.markets.first().map(|m| m.0.clone()).unwrap_or_default(),
            baseline_usd: baseline, equity_usd: equity, loss_usd: loss, limit_usd: limit,
        });
    }

    /// Periodic maintenance: time out overdue hedges, run the orphan-recovery backstop, and
    /// refresh the dead-man countdown.
    pub async fn on_tick(&mut self, now_ns: i64) {
        if !self.draining { self.check_circuit_breaker(now_ns); }
        if self.breaker_tripped && !self.draining { return; }
        self.drive_safety_sweep(now_ns);
        let timeout_ns = self.cfg.live.max_unhedged_age_ms.max(0).saturating_mul(1_000_000);
        let mut overdue = false;
        for h in self.hedges.values_mut() {
            let previous = h.state;
            h.check_timeout(now_ns, timeout_ns);
            if h.state != previous && h.state.is_dangerous() {
                overdue = true;
                if h.terminal { self.correction_needed.insert(h.market.clone()); }
                self.journal.progress(now_ns, h);
            }
        }
        if overdue { self.freeze_and_sweep(now_ns, "execution_deadline"); }
        let mut expired = Vec::new();
        for (m, inv) in &self.pending {
            let mark = self.book(m, VenueTag::Aster).and_then(|b| b.mid()).unwrap_or(inv.avg_aster_px);
            if inventory::check_pending_limits(inv, self.cfg.live.partials.max_pending_notional_usd,
                self.cfg.live.partials.max_pending_age_ms, mark, Utc::now()) {
                expired.push(m.clone());
            }
        }
        for m in expired {
            if self.correction_needed.insert(m.clone()) {
                self.journal.reason(now_ns, "pending_limit", Some(m.0), "pending age/notional limit");
            }
            self.freeze_and_sweep(now_ns, "pending_limit");
        }
        // While the taker holds or just returned the rights, its trade would read as a
        // position mismatch here; `drive_yield` re-reads the positions instead.
        if matches!(self.yield_state, Yield::Idle | Yield::Yielding { .. }) { self.recover_orphans(now_ns); }
        self.publish_execution_queries();
        if self.cfg.live.aster.deadman_enabled && !self.draining {
            for m in self.markets.clone() {
                if self.ctx.get(&m).is_some_and(|c| c.eligible) {
                    if self.try_send_aster_cmd(ExecCommand::RefreshDeadman { market: m }, AsterCommandPriority::Deadman, now_ns) == ExecDispatch::QueueClosed {
                        self.freeze(now_ns, "deadman_queue_closed");
                    }
                }
            }
        }
    }

    fn recover_orphans(&mut self, now_ns: i64) {
        let snap = self.account.load();
        if snap.source_ts_ns == 0 || now_ns.saturating_sub(snap.source_ts_ns) / 1_000_000 > self.cfg.live.max_account_snapshot_age_ms { return; }
        let mut need_maker_backfill = !self.uncertain_makers.is_empty();
        for m in self.markets.clone() {
            // An Unknown remains a live reservation. Position snapshots cannot
            // prove that a queued/accepted transaction will never execute later.
            if self.hedges.values().any(|h| h.market == m && h.unresolved()) { continue; }
            let last_terminal = self.hedges.values().filter(|h| h.market == m)
                .filter_map(|h| h.terminal_ns).max().unwrap_or(0);
            let last_action = self.last_hot_action_ns.get(&m).copied().unwrap_or(0).max(last_terminal);
            if snap.read_start_ns <= last_action { continue; }
            let rep_a = snap.reported_position(Venue::Aster, &m);
            let rep_h = snap.reported_position(Venue::Hyperliquid, &m);
            let pred_a = self.aster_pos.get(&m).map(|p| p.qty).unwrap_or_default();
            let pred_h = self.hl_pos.get(&m).map(|p| p.qty).unwrap_or_default();
            if pred_a != rep_a || pred_h != rep_h {
                // Backfill native cumulative maker execution before changing the
                // economic ledger. Late private fills then apply zero extra delta.
                need_maker_backfill |= pred_a != rep_a;
                self.freeze_and_sweep(now_ns, "position_mismatch_reconcile");
                continue;
            }
            let completed: Vec<String> = self.hedges.iter().filter(|(_, h)| h.market == m && h.terminal)
                .map(|(id, _)| id.clone()).collect();
            let corrected = completed.iter().any(|id| self.hedges[id].purpose == IntentPurpose::ReduceDelta);
            for id in completed {
                if let Some(mut h) = self.hedges.remove(&id) {
                    h.mark_reconciled();
                    self.journal.progress(now_ns, &h);
                    if let Some(client) = h.client_id {
                        self.maker_coverage.insert(client, MakerCoverage { logical_id: h.logical_id, qty: h.filled_qty, quote: h.filled_quote_usd });
                    }
                }
            }
            let net = pred_a + pred_h;
            if net == Decimal::ZERO {
                self.pending.remove(&m);
                self.correction_needed.remove(&m);
                self.correction_attempts.remove(&m);
                self.orphan_seen.remove(&m);
                if self.uncertain_makers.is_empty() && !self.orders.live_slots().iter().any(|(market, _)| market == &m) { self.logical_ids.remove(&m); }
                continue;
            }
            if corrected {
                if let Some(pending) = self.pending.get_mut(&m) { pending.signed_qty = net; }
                self.correction_needed.insert(m.clone());
            }
            let pending = self.pending.get(&m).map(|p| p.signed_qty).unwrap_or_default();
            if !self.correction_needed.contains(&m) && net == pending { continue; }
            if !self.correction_needed.contains(&m) {
                let confirmed = self.orphan_seen.get(&m).is_some_and(|&(old_net, src)| old_net == net && snap.source_ts_ns > src);
                if !confirmed { self.orphan_seen.insert(m.clone(), (net, snap.source_ts_ns)); continue; }
                self.correction_needed.insert(m.clone());
                self.freeze_and_sweep(now_ns, "confirmed_residual");
            }
            self.dispatch_correction(&m, net, pred_a, pred_h, now_ns);
        }
        let queries_empty = self.account.maker_queries().is_empty();
        if need_maker_backfill && queries_empty { self.account.publish_maker_queries(self.orders.recent_makers()); }
        else if !need_maker_backfill && !queries_empty { self.account.publish_maker_queries(Vec::new()); }
        let stream_fresh = self.aster_stream.as_ref().is_none_or(|s| s.age_ms(now_ns) <= self.cfg.live.max_user_stream_staleness_ms);
        let clean = self.frozen && !self.draining && !self.breaker_tripped && self.journal.healthy()
            && self.uncertain_makers.is_empty() && self.correction_needed.is_empty() && self.hedges.is_empty()
            && self.positions_reconciled() && stream_fresh && self.sweep_pending.is_none()
            && !self.has_open_aster_bot_orders_in(&snap)
            && !self.hedge_tx.is_closed() && !self.exec_tx.is_closed()
            && self.hedge_readiness.as_ref().is_none_or(|r| r.is_ready());
        if clean {
            match self.heal_confirm {
                Some(src) if snap.source_ts_ns > src => {
                    self.frozen = false;
                    self.heal_confirm = None;
                    self.journal.reason(now_ns, "unfreeze", None, "terminal executions and positions reconciled");
                }
                None => self.heal_confirm = Some(snap.source_ts_ns),
                _ => {}
            }
        } else { self.heal_confirm = None; }
    }

    fn dispatch_correction(&mut self, market: &MarketId, net: Decimal, aster: Decimal, lighter: Decimal, now_ns: i64) {
        if self.correction_attempts.get(market).copied().unwrap_or(0) >= 2 { return; }
        if self.hedges.values().any(|h| &h.market == market && !h.state.is_resolved()) { return; }
        let Some(ctx) = self.ctx.get(market) else { return };
        let aster_step = ctx.spec.step;
        let lighter_step = ctx.spec.hl_qty_step;
        let side = if net > Decimal::ZERO { Side::Sell } else { Side::Buy };
        let same_sign = |q: Decimal| q != Decimal::ZERO && (q > Decimal::ZERO) == (net > Decimal::ZERO);
        let a_qty = if same_sign(aster) { crate::decimal::floor_to_step(net.abs().min(aster.abs()), aster_step) } else { Decimal::ZERO };
        let h_qty = if same_sign(lighter) { crate::decimal::floor_to_step(net.abs().min(lighter.abs()), lighter_step) } else { Decimal::ZERO };
        let slip = self.cfg.live.hyperliquid.emergency_slippage_bps;
        let selected = if a_qty > Decimal::ZERO {
            self.fresh_aster_touch_book(market, now_ns).and_then(|b| b.book.mid().map(|p| (Venue::Aster, a_qty, p, b.source.as_str(), b.age_ms)))
        } else { None }.or_else(|| {
            if h_qty <= Decimal::ZERO { return None; }
            self.fresh_hl_hedge_book_hot_first(market, now_ns, side, h_qty)
                .and_then(|b| crossing_hedge_px(&b.book, side, slip).map(|p| (Venue::Hyperliquid, h_qty, p, b.path.as_str(b.source), b.age_ms)))
        });
        let Some((venue, qty, price, source, age_ms)) = selected else { return };
        let cloid = self.orders.next_attempt_id(market);
        let logical_id = self.logical_id(market);
        let mut intent = HedgeIntent::with_qty(cloid, market.clone(), side, qty, price, now_ns);
        intent.logical_id = logical_id;
        intent.venue = venue;
        intent.book_source = Some(source);
        intent.book_age_ms = Some(age_ms);
        intent.purpose = IntentPurpose::ReduceDelta;
        intent.recovery = true;
        intent.arm_admission(self.cfg.live.max_unhedged_age_ms);
        intent.mark_submitted(now_ns);
        let sent = if venue == Venue::Aster {
            let client_id = self.orders.next_flatten_client_id(market);
            intent.client_id = Some(client_id.clone());
            self.hedges.insert(cloid.to_hex(), intent.clone());
            self.try_send_aster_cmd(ExecCommand::FlattenAster { intent, client_id }, AsterCommandPriority::Safety, now_ns) == ExecDispatch::Sent
        } else {
            self.hedges.insert(cloid.to_hex(), intent.clone());
            self.hedge_tx.try_send(HedgeCommand::Hedge { intent, aggressive_px: price }).is_ok()
        };
        if sent {
            *self.correction_attempts.entry(market.clone()).or_insert(0) += 1;
            self.last_hot_action_ns.insert(market.clone(), now_ns);
        } else if let Some(h) = self.hedges.get_mut(&cloid.to_hex()) {
            h.admission.cancel_queued(); h.mark_rejected(); h.terminal_ns = Some(now_ns);
        }
        if let Some(h) = self.hedges.get(&cloid.to_hex()) { self.journal.progress(now_ns, h); }
    }

}

const PRIORITY_DRAIN_LIMIT: usize = 64;

async fn dispatch_execution_event(strat: &mut Strategy, ev: ExecEvent, now_ns: i64) {
    match ev {
        ExecEvent::MakerOrderProgress { market, side, client_id, order_id, cumulative_qty, cumulative_quote_usd, terminal, event_time_ms } => {
            strat.handle_maker_order_progress(market, side, client_id, order_id, cumulative_qty,
                cumulative_quote_usd, terminal, event_time_ms, now_ns).await;
        }
        ev => strat.handle_exec_event(ev, now_ns),
    }
}

/// Drain latency-critical events between markets during a tick/wake reprice sweep.
/// This prevents a maker fill from waiting behind a full all-market reprice batch.
async fn drain_priority_events(
    strat: &mut Strategy,
    exec_events: &mut Receiver<ExecEvent>,
    maker_fills: &mut Receiver<AsterFill>,
) {
    for _ in 0..PRIORITY_DRAIN_LIMIT {
        let now_ns = crate::hotpath::clock::mono_now_ns();

        if let Ok(fill) = maker_fills.try_recv() {
            strat.handle_maker_fill(fill, now_ns).await;
            continue;
        }

        if let Ok(ev) = exec_events.try_recv() {
            dispatch_execution_event(strat, ev, now_ns).await;
            continue;
        }

        break;
    }
}

/// Drive the strategy: wake on a book change (the coalescing registry `Notify`) or a
/// periodic tick, and consume worker events. Runs until `shutdown` resolves.
pub async fn run_strategy(
    mut strat: Strategy,
    wake: Arc<Notify>,
    mut exec_events: Receiver<ExecEvent>,
    mut maker_fills: Receiver<AsterFill>,
    shutdown: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    use crate::hotpath::clock::mono_now_ns;
    info!("strategy loop started ({} markets)", strat.markets.len());
    let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(
        strat.cfg.live.aster.deadman_refresh_ms.max(100) as u64,
    ));
    let mut last_diag_ns: i64 = 0; // throttle for the quote diagnostic (see log_quote_diag)
    let mut dirty_idx_buf = Vec::with_capacity(strat.markets.len());
    let mut dirty_market_buf = Vec::with_capacity(strat.markets.len());
    let mut want = strat.rights.as_ref().map(|r| r.want.clone());
    loop {
        strat.drive_yield(want.as_ref().and_then(|w| *w.borrow()), mono_now_ns());
        // BIASED: the latency-critical fill->hedge and hedge-event arms are polled FIRST, so a
        // pending maker fill deterministically preempts the reprice-all-markets (`wake`) and cold
        // `on_tick` work instead of waiting behind it (random select could schedule them first).
        // Order: shutdown > maker fills > exec events > tick > wake. `tick`
        // precedes `wake` so the recovery/deadman tick is never starved by a continuously-ready
        // book `wake`. This is a tie-break change only (same handlers); on a fast VPS it removes
        // head-of-line jitter from the hot path.
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            Some(fill) = maker_fills.recv() => {
                strat.handle_maker_fill(fill, mono_now_ns()).await;
            }
            Some(ev) = exec_events.recv() => {
                dispatch_execution_event(&mut strat, ev, mono_now_ns()).await;
            }
            // Only wakes the loop: `drive_yield` reads the request at the top. A taker gone
            // (closed channel) reads as no request.
            changed = async { match want.as_mut() { Some(w) => w.changed().await, None => std::future::pending().await } } => {
                if changed.is_err() { want = None; }
            }
            _ = tick.tick() => {
                let now_ns = mono_now_ns();
                strat.refresh_mark_cache();
                strat.on_tick(now_ns).await;
                // Throttled quote diagnostic (~every 20s): proves the loop is alive and explains any
                // no-quote (gate closed vs compute_desired_quote reject vs already resting).
                if now_ns.saturating_sub(last_diag_ns) >= 20_000_000_000 {
                    strat.log_quote_diag(now_ns);
                    let bb = crate::metrics::BOOK_BUILD.snapshot();
                    let vp = crate::metrics::VENUE_PUBLISH.snapshot();
                    let wr = crate::metrics::WAKE_REPRICE.snapshot();
                    let tr = crate::metrics::TICK_REPRICE.snapshot();
                    let sr = crate::metrics::SINGLE_REPRICE.snapshot();
                    info!(
                        "[metrics] book_build=p50:{}us/p99:{}us  publish=p50:{}us/p99:{}us  \
                         wake=p50:{}us/p99:{}us  tick=p50:{}us/p99:{}us  reprice=p50:{}us/p99:{}us  n={}",
                        bb.p50_us, bb.p99_us, vp.p50_us, vp.p99_us,
                        wr.p50_us, wr.p99_us, tr.p50_us, tr.p99_us,
                        sr.p50_us, sr.p99_us, sr.count,
                    );
                    crate::metrics::BOOK_BUILD.reset();
                    crate::metrics::VENUE_PUBLISH.reset();
                    crate::metrics::WAKE_REPRICE.reset();
                    crate::metrics::TICK_REPRICE.reset();
                    crate::metrics::SINGLE_REPRICE.reset();
                    last_diag_ns = now_ns;
                }
                // Reprice on every tick too, so a gate close / cooldown expiry is acted on
                // promptly even without a book change (cancel-all on gate close).
                // If a book-change wake is pending, consume it and use force=false (the
                // generation gate skips unchanged markets — cheaper than force=true, and
                // the pending wake's dirty markets get processed now instead of waiting
                // for the next select iteration). If no wake is pending, use force=true
                // to catch gate-close / cooldown-expiry that wouldn't otherwise trigger.
                let wake_pending = wake.notified().now_or_never().is_some();
                let force = !wake_pending;
                let t0_tick = mono_now_ns();
                let now = Utc::now();
                let n_markets = strat.markets.len();
                for i in 0..n_markets {
                    let m = strat.markets[i].clone();
                    strat.reprice_market(&m, now, now_ns, force).await;
                    drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills).await;
                }
                crate::metrics::TICK_REPRICE.record((mono_now_ns() - t0_tick) as u64);
            }
            _ = wake.notified() => {
                let (now, now_ns) = (Utc::now(), mono_now_ns());
                let t0_wake = now_ns;
                strat.refresh_mark_cache();
                match &strat.dirty {
                    Some(dirty) => {
                        dirty.take_into(&mut dirty_idx_buf);
                        dirty_market_buf.clear();
                        dirty_market_buf.extend(
                            dirty_idx_buf
                                .iter()
                                .filter_map(|&idx| strat.registry.market_id(idx).cloned()),
                        );
                        for m in &dirty_market_buf {
                            strat.reprice_market(m, now, now_ns, false).await;
                            drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills).await;
                        }
                    }
                    None => {
                        let n_markets = strat.markets.len();
                        for i in 0..n_markets {
                            let m = strat.markets[i].clone();
                            strat.reprice_market(&m, now, now_ns, false).await;
                            drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills).await;
                        }
                    }
                }
                crate::metrics::WAKE_REPRICE.record((mono_now_ns() - t0_wake) as u64);
            }
        }
    }
    strat.revoke_makers();
    strat.draining = true;
    strat.frozen = true;
    strat.correction_needed.extend(strat.pending.keys().cloned());
    for m in strat.markets.clone() { strat.cancel_both_sides(&m, mono_now_ns()); }
    if let Some(control) = &strat.drain_control { control.quiesced.store(true, std::sync::atomic::Ordering::Release); }
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(65);
    let mut drain_tick = tokio::time::interval(tokio::time::Duration::from_millis(100));
    loop {
        drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills).await;
        let now_ns = mono_now_ns();
        let barrier_ns = strat.drain_control.as_ref().map(|c| c.maker_barrier.completed_ns()).unwrap_or(1);
        let snap = strat.account.load();
        let maker_clear = barrier_ns > 0 && snap.read_start_ns > barrier_ns && !strat.has_open_aster_bot_orders_in(&snap);
        if maker_clear && strat.uncertain_makers.is_empty() && strat.hedges.is_empty() && strat.pending.is_empty() && strat.correction_needed.is_empty()
            && maker_fills.is_empty() && exec_events.is_empty() && strat.orders.live_slots().is_empty() {
            info!("strategy quiesced with all execution evidence reconciled");
            return Ok(());
        }
        tokio::select! {
            biased;
            Some(fill) = maker_fills.recv() => strat.handle_maker_fill(fill, mono_now_ns()).await,
            Some(ev) = exec_events.recv() => dispatch_execution_event(&mut strat, ev, mono_now_ns()).await,
            _ = drain_tick.tick() => { strat.refresh_mark_cache(); strat.on_tick(now_ns).await; }
            _ = tokio::time::sleep_until(deadline) => {
                strat.journal.reason(mono_now_ns(), "shutdown_unresolved", None, "execution or residual unresolved after bounded drain");
                anyhow::bail!("shutdown retains {} execution attempts and {} residual obligations", strat.hedges.len(), strat.correction_needed.len());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quote_engine::tests::{edge, ts};
    use rust_decimal_macros::dec;

    fn qcfg() -> QuoteEngineConfig {
        QuoteEngineConfig {
            desired_notional: dec!(100),
            max_quote_distance_bps: dec!(50.0),
            min_aster_touch_distance_bps: dec!(0.0),
            min_aster_touch_hysteresis_bps: dec!(2.0),
            max_aster_touch_hysteresis_ms: 300_000,
            depth_liquidity_multiple: dec!(10.0),
            max_hedge_slippage_bps: dec!(50.0),
            price_change_ticks_to_requote: 1,
            clamp_to_min_lot: true,
            min_requote_bps: dec!(1.0),
        }
    }
    fn spec() -> MarketSpec {
        MarketSpec {
            market_id: "BTC".into(),
            aster_symbol: "BTCUSDT".into(),
            hl_coin: "BTC".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 2,
            lighter_size_decimals: 3,
            lighter_price_tick: dec!(0.01),
            tick: dec!(0.01),
            step: dec!(0.001),
            aster_min_qty: dec!(0.001),
            aster_min_notional: dec!(5),
            hl_sz_decimals: 3,
            hl_qty_step: dec!(0.001),
            hl_min_notional: dec!(5),
        }
    }
    fn books() -> (OrderBook, OrderBook) {
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            ts(),
            ts(),
        );
        let h = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            ts(),
            ts(),
        );
        (a, h)
    }

    fn hl_bbo_at(bid_qty: Decimal, ask_qty: Decimal, local_recv_ts: DateTime<Utc>) -> OrderBook {
        hl_bbo_at_ts(bid_qty, ask_qty, local_recv_ts, local_recv_ts)
    }

    fn hl_bbo_at_ts(
        bid_qty: Decimal,
        ask_qty: Decimal,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> OrderBook {
        OrderBook::from_levels(
            vec![(dec!(99.96), bid_qty)],
            vec![(dec!(100.04), ask_qty)],
            exch_ts,
            local_recv_ts,
        )
    }

    fn aster_bbo_at(bid_px: Decimal, ask_px: Decimal, local_recv_ts: DateTime<Utc>) -> OrderBook {
        aster_bbo_at_ts(bid_px, ask_px, local_recv_ts, local_recv_ts)
    }

    fn aster_bbo_at_ts(
        bid_px: Decimal,
        ask_px: Decimal,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> OrderBook {
        OrderBook::from_levels(
            vec![(bid_px, dec!(1000))],
            vec![(ask_px, dec!(1000))],
            exch_ts,
            local_recv_ts,
        )
    }

    fn publish_hl_l2_hot(strat: &Strategy, book: OrderBook, recv_ns: i64) {
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let hot = crate::livebot::scale::build_hot_book_with_qty_scale(
            &book,
            &scale,
            crate::livebot::scale::HotQtyScale::Hyperliquid,
            recv_ns,
        );
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_hot(book, hot);
    }

    fn publish_hl_bbo_hot(strat: &Strategy, book: OrderBook, recv_ns: i64) {
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let hot = crate::livebot::scale::build_hot_book_with_qty_scale(
            &book,
            &scale,
            crate::livebot::scale::HotQtyScale::Hyperliquid,
            recv_ns,
        );
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo_hot(book, hot);
    }

    #[test]
    fn fill_telemetry_hl_source_prefers_fresh_bbo() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(hl_bbo_at(dec!(2), dec!(3), ts()));

        let selected = strat.fresh_hl_quote_book(&m, crate::hotpath::clock::mono_now_ns()).unwrap();
        assert_eq!(selected.source, HlQuoteSource::Bbo);
        assert_eq!(selected.book.best_bid().unwrap().qty, dec!(2));
        assert_eq!(selected.book.best_ask().unwrap().qty, dec!(3));
    }

    #[test]
    fn fill_telemetry_hl_source_falls_back_when_bbo_crossed() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        let crossed = OrderBook::from_levels(
            vec![(dec!(100.10), dec!(2))],
            vec![(dec!(100.00), dec!(2))],
            ts(),
            ts(),
        );
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(crossed);

        let selected = strat.fresh_hl_quote_book(&m, crate::hotpath::clock::mono_now_ns()).unwrap();
        assert_eq!(selected.source, HlQuoteSource::L2);
        assert_eq!(selected.book.best_bid().unwrap().px, dec!(99.95));
    }

    #[test]
    fn fill_hedge_source_falls_back_when_bbo_depth_insufficient() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(hl_bbo_at(dec!(0.2), dec!(0.2), ts()));

        let selected = strat
            .fresh_hl_hedge_book(&m, crate::hotpath::clock::mono_now_ns(), Side::Sell, dec!(0.2))
            .unwrap();
        assert_eq!(selected.source, HlQuoteSource::L2);
    }

    #[test]
    fn fill_hedge_source_uses_bbo_with_depth_factor() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(hl_bbo_at(dec!(2.1), dec!(2.1), ts()));

        let selected = strat
            .fresh_hl_hedge_book(&m, crate::hotpath::clock::mono_now_ns(), Side::Sell, dec!(0.2))
            .unwrap();
        assert_eq!(selected.source, HlQuoteSource::Bbo);
    }

    #[test]
    fn hot_hl_bbo_selected_when_fresh_and_10x_deep() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        publish_hl_l2_hot(&strat, books().1, 1_000_000);
        publish_hl_bbo_hot(&strat, hl_bbo_at(dec!(2.1), dec!(2.1), ts()), 1_000_000);

        let selected = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Sell, dec!(0.2))
            .unwrap();

        assert_eq!(selected.source, HlQuoteSource::Bbo);
        assert_eq!(selected.path, HlHedgePath::Hot);
    }

    #[test]
    fn hot_hl_bbo_depth_check_is_side_specific() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        publish_hl_l2_hot(&strat, books().1, 1_000_000);
        // Bid is too thin for a SELL hedge, ask is deep enough for a BUY hedge.
        publish_hl_bbo_hot(&strat, hl_bbo_at(dec!(0.2), dec!(2.1), ts()), 1_000_000);

        let sell = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Sell, dec!(0.2))
            .unwrap();
        assert_eq!(sell.source, HlQuoteSource::L2);
        assert_eq!(sell.path, HlHedgePath::Hot);

        let buy = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Buy, dec!(0.2))
            .unwrap();
        assert_eq!(buy.source, HlQuoteSource::Bbo);
        assert_eq!(buy.path, HlHedgePath::Hot);
    }

    #[test]
    fn hot_hl_bbo_older_than_l2_is_ignored() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        let newer = ts() + chrono::Duration::milliseconds(10);
        let l2 = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            newer,
            newer,
        );
        publish_hl_l2_hot(&strat, l2, 1_000_000);
        publish_hl_bbo_hot(&strat, hl_bbo_at_ts(dec!(2.1), dec!(2.1), ts(), newer), 1_500_000);

        let selected = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Sell, dec!(0.2))
            .unwrap();

        assert_eq!(selected.source, HlQuoteSource::L2);
        assert_eq!(selected.path, HlHedgePath::Hot);
    }

    #[tokio::test]
    async fn primary_fill_hedge_prefers_hot_bbo_when_deep_enough() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        publish_hl_l2_hot(&strat, books().1, 1_000_000);
        publish_hl_bbo_hot(&strat, hl_bbo_at(dec!(2.1), dec!(2.1), ts()), 1_000_000);

        let fill = AsterFill {
            market: m.clone(),
            aster_side: Side::Buy,
            order_id: "oid-fill-hot".into(),
            trade_id: "trade-fill-hot".into(),
            client_id: strat.orders.next_client_id(&m, Side::Buy).unwrap(),
            last_fill_qty: dec!(0.2),
            last_fill_px: dec!(100),
            cum_filled_qty: dec!(0.2),
            event_time_ms: 1_700_000_000_000,
            reduce_only: false,
            commission: None,
            commission_asset: None,
        };

        strat.handle_maker_fill(fill, 2_000_000).await;

        let cmd = hrx.try_recv().expect("primary hedge command must be sent");
        match cmd {
            HedgeCommand::Hedge { aggressive_px, intent } => {
                assert_eq!(intent.hedge_side, Side::Sell);
                assert_eq!(intent.qty, dec!(0.2));
                assert_eq!(aggressive_px, dec!(99.91002)); // hot BBO bid 99.96 x (1 - default 5 bps)
            }
            other => panic!("expected primary hedge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn priority_lane_routes_acked_cancels_and_falls_back_when_full() {
        use crate::livebot::exec::command::ExecCommand;
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(8);
        let (htx, _hrx) = tokio::sync::mpsc::channel(8);
        let mut strat = live_strat(etx, htx, account);
        let (ptx, mut prx) = tokio::sync::mpsc::channel(1); // depth 1: makes "full" testable
        strat.set_exec_prio_lane(ptx);
        let m: MarketId = "BTC".into();

        let acked = |cid: &str| ExecCommand::Cancel {
            market: m.clone(),
            client_id: cid.into(),
            venue_order_id: Some("v1".into()),
        };
        // Acked cancel → priority lane, not the FIFO lane.
        assert!(matches!(
            strat.try_send_aster_cmd(acked("c1"), AsterCommandPriority::RiskReducing, 1),
            ExecDispatch::Sent
        ));
        // Prio lane full → falls back to the normal lane (ordering-safe), still Sent.
        assert!(matches!(
            strat.try_send_aster_cmd(acked("c2"), AsterCommandPriority::RiskReducing, 2),
            ExecDispatch::Sent
        ));
        // Un-acked cancel → normal lane only.
        let unacked = ExecCommand::Cancel {
            market: m.clone(),
            client_id: "c3".into(),
            venue_order_id: None,
        };
        assert!(matches!(
            strat.try_send_aster_cmd(unacked, AsterCommandPriority::RiskReducing, 3),
            ExecDispatch::Sent
        ));

        let on_prio = prx.try_recv().expect("acked cancel must ride the priority lane");
        assert!(matches!(on_prio, ExecCommand::Cancel { ref client_id, .. } if client_id == "c1"));
        assert!(prx.try_recv().is_err(), "only one command fits the prio lane");
        let fallback = erx.try_recv().expect("overflow must fall back to the FIFO lane");
        assert!(matches!(fallback, ExecCommand::Cancel { ref client_id, .. } if client_id == "c2"));
        let normal = erx.try_recv().expect("un-acked cancel stays on the FIFO lane");
        assert!(matches!(normal, ExecCommand::Cancel { ref client_id, .. } if client_id == "c3"));
    }

    #[tokio::test]
    async fn shutdown_drain_dispatches_fill_queued_at_cancellation() {
        // A maker fill already queued when the shutdown token fires must still reach
        // the hedge dispatch: the loop exits into a bounded drain instead of dropping
        // the queue on the floor (run() keeps the workers up until this returns).
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        publish_hl_l2_hot(&strat, books().1, 1_000_000);
        publish_hl_bbo_hot(&strat, hl_bbo_at(dec!(2.1), dec!(2.1), ts()), 1_000_000);
        let client_id = strat.orders.next_client_id(&"BTC".into(), Side::Buy).unwrap();

        let wake = Arc::new(tokio::sync::Notify::new());
        let (ev_tx, ev_rx) = tokio::sync::mpsc::channel(16);
        let (fill_tx, fill_rx) = tokio::sync::mpsc::channel(16);
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        fill_tx
            .send(AsterFill {
                market: "BTC".into(),
                aster_side: Side::Buy,
                order_id: "oid-drain".into(),
                trade_id: "trade-drain".into(),
                client_id,
                last_fill_qty: dec!(0.2),
                last_fill_px: dec!(100),
                cum_filled_qty: dec!(0.2),
                event_time_ms: 1_700_000_000_000,
                reduce_only: false,
            commission: None,
            commission_asset: None,
            })
            .await
            .unwrap();
        drop(fill_tx); // mirrors the userstream's shutdown arm dropping its sender

        let task = tokio::spawn(run_strategy(strat, wake, ev_rx, fill_rx, shutdown));
        let cmd = tokio::time::timeout(std::time::Duration::from_secs(2), hrx.recv()).await.unwrap().unwrap();
        let HedgeCommand::Hedge { intent, aggressive_px, .. } = cmd else { panic!("hedge expected") };
        // The Lighter worker reports the IOC fully filled...
        ev_tx.send(ExecEvent::ExecutionProgress { cloid: intent.cloid, cumulative_qty: intent.qty,
            cumulative_quote_usd: Some(intent.qty * aggressive_px), cumulative_fee_usd: Some(Decimal::ZERO),
            terminal: true, venue_order_id: None, event_time_ms: None }).await.unwrap();
        // ...and the reconciler's later snapshots confirm both legs, which ends the drain.
        let drained = async {
            while !task.is_finished() {
                account.publish(funded_snapshot(crate::hotpath::clock::mono_now_ns(), dec!(0.2), dec!(-0.2)));
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            task.await
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), drained).await.unwrap().unwrap().unwrap();
    }

    #[test]
    fn hl_bbo_selected_when_fresh_and_sufficient() {
        let (a, h) = books();
        let bbo = hl_bbo_at(dec!(20), dec!(20), ts());
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::Bbo);
        assert_eq!(desired.expected_hl_vwap, dec!(99.96));
    }

    #[test]
    fn hl_bbo_depth_factor_rejects_under_10x_and_falls_back_to_l2() {
        let (a, h) = books();
        let bbo = hl_bbo_at(dec!(9.99), dec!(9.99), ts());
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::L2);
        assert_eq!(desired.expected_hl_vwap, dec!(99.95));
    }

    #[test]
    fn locally_fresh_but_exchange_older_hl_bbo_is_ignored() {
        let now = ts() + chrono::Duration::milliseconds(10);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let h_newer = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            now,
            now,
        );
        // Local receive is fresh, but the BBO exchange timestamp predates the installed L2.
        let old_bbo = hl_bbo_at_ts(dec!(2), dec!(2), ts(), now);
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h_newer),
            Some(&old_bbo),
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::L2);
        assert_eq!(desired.expected_hl_vwap, dec!(99.95));
    }

    #[test]
    fn thin_hl_bbo_falls_back_to_fresh_l2() {
        let (a, h) = books();
        let bbo = hl_bbo_at(dec!(0.1), dec!(0.1), ts());
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::L2);
        assert_eq!(desired.expected_hl_vwap, dec!(99.95));
    }

    #[test]
    fn thin_hl_bbo_with_stale_l2_rejects_explicitly() {
        let (_, h) = books();
        let now = ts() + chrono::Duration::milliseconds(10_000);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let bbo = hl_bbo_at(dec!(0.1), dec!(0.1), now);
        let err = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap_err();
        assert_eq!(err, RejectReason::HlBboThinAndL2Stale);
    }

    #[test]
    fn hl_bbo_depth_factor_with_stale_l2_rejects_explicitly() {
        let (_, h) = books();
        let now = ts() + chrono::Duration::milliseconds(10_000);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let bbo = hl_bbo_at(dec!(9.99), dec!(9.99), now);
        let err = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap_err();
        assert_eq!(err, RejectReason::HlBboThinAndL2Stale);
    }

    #[test]
    fn aster_bbo_selected_when_fresh() {
        let (a, h) = books();
        let a_bbo = aster_bbo_at(dec!(99.99), dec!(100.03), ts());
        let (_desired, _hl_book, hl_source, aster_source) = compute_desired_quote_select_books(
            &edge(),
            &qcfg(),
            &a,
            Some(&a_bbo),
            Some(&h),
            None,
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(aster_source, AsterQuoteSource::Bbo);
        assert_eq!(hl_source, HlQuoteSource::L2);
    }

    #[test]
    fn locally_fresh_but_exchange_older_aster_bbo_is_ignored() {
        let now = ts() + chrono::Duration::milliseconds(10);
        let a_newer = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let h = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            now,
            now,
        );
        let old_bbo = aster_bbo_at_ts(dec!(99.99), dec!(100.03), ts(), now);
        let (_desired, _hl_book, _hl_source, aster_source) = compute_desired_quote_select_books(
            &edge(),
            &qcfg(),
            &a_newer,
            Some(&old_bbo),
            Some(&h),
            None,
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(aster_source, AsterQuoteSource::L2);
    }

    #[test]
    fn stale_aster_bbo_falls_back_to_l2() {
        let now = ts() + chrono::Duration::milliseconds(10_000);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let h = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            now,
            now,
        );
        let stale_bbo = aster_bbo_at(dec!(99.99), dec!(100.03), ts());
        let (_desired, _hl_book, _hl_source, aster_source) = compute_desired_quote_select_books(
            &edge(),
            &qcfg(),
            &a,
            Some(&stale_bbo),
            Some(&h),
            None,
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(aster_source, AsterQuoteSource::L2);
    }

    #[test]
    fn hot_precheck_ignores_exchange_older_bbo() {
        let scale = MarketScale::from_spec(&spec());
        let newer_exch = ts() + chrono::Duration::milliseconds(10);
        let l2 = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            newer_exch,
            newer_exch,
        );
        let old_bbo = aster_bbo_at_ts(dec!(99.99), dec!(100.03), ts(), newer_exch);
        let l2_hot = crate::livebot::scale::build_hot_book(&l2, &scale, 1_000);
        let bbo_hot = crate::livebot::scale::build_hot_book(&old_bbo, &scale, 2_000);
        let selected = select_aster_hot_for_precheck(Some(&l2_hot), Some(&bbo_hot), 2_000, 5_000_000_000)
            .expect("must pick one book");
        assert_eq!(selected.exch_ms, l2_hot.exch_ms);
        assert_eq!(selected.best_bid_ticks(), l2_hot.best_bid_ticks());
    }

    #[test]
    fn crossing_hedge_px_crosses_the_opposite_touch() {
        let (_a, h) = books(); // Lighter book: best bid 99.95 / best ask 100.05
        let slip = dec!(10); // 10 bps
        // A BUY hedge prices off the ask, a SELL hedge off the bid (not mid, which would not cross).
        assert_eq!(crossing_hedge_px(&h, Side::Buy, slip), Some(dec!(100.15005))); // 100.05 x 1.001
        assert_eq!(crossing_hedge_px(&h, Side::Sell, slip), Some(dec!(99.85005))); // 99.95 x 0.999
        // An empty book side yields None — the caller must NOT hedge off a fallback price.
        let empty = OrderBook::from_levels(vec![], vec![], ts(), ts());
        assert!(crossing_hedge_px(&empty, Side::Buy, slip).is_none());
        assert!(crossing_hedge_px(&empty, Side::Sell, slip).is_none());
    }

    #[test]
    fn gate_closed_cancels_resting_holds_empty() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        // resting order + may_quote=false => cancel
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, false, Some(CurrentOrder { price: dec!(99), qty: dec!(1) }), true);
        assert!(matches!(d, SideDecision::Cancel { reason: ReplaceReason::FeedStale }));
        // empty slot + may_quote=false => hold
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, false, None, true);
        assert!(matches!(d, SideDecision::Hold));
    }

    #[test]
    fn empty_slot_places_when_profitable() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, None, true);
        assert!(matches!(d, SideDecision::Place(_)));
    }

    #[test]
    fn stale_book_cancels_with_feed_stale() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        let later = ts() + chrono::Duration::milliseconds(10_000); // both books now stale
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, later, &pos, true, Some(CurrentOrder { price: dec!(99), qty: dec!(1) }), true);
        assert!(matches!(d, SideDecision::Cancel { reason: ReplaceReason::FeedStale }));
    }

    #[test]
    fn quote_too_close_maps_to_specific_cancel_reason() {
        assert_eq!(
            ReplaceReason::from_reject(RejectReason::QuoteTooCloseToTouch),
            ReplaceReason::QuoteTooCloseToTouch
        );
        assert_eq!(ReplaceReason::QuoteTooCloseToTouch.as_str(), "QUOTE_TOO_CLOSE_TO_TOUCH");
    }

    #[test]
    fn requote_deadband_holds_within_min_bps_but_replaces_beyond() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        // Find where the engine wants to quote (empty slot -> Place).
        let place = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, None, true);
        let SideDecision::Place(d) = place else { panic!("expected a Place for an empty profitable slot") };
        let (px, qty) = (d.price, d.qty);
        // A resting order AT that price (0 bps away) must HOLD — the deadband suppresses churn.
        let near = CurrentOrder { price: px, qty };
        let held = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, Some(near), true);
        assert!(matches!(held, SideDecision::Hold), "within {}bps deadband must Hold", qcfg().min_requote_bps);
        // A resting order far away (~100 bps) must REPLACE.
        let far = CurrentOrder { price: px * dec!(0.99), qty };
        let replaced = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, Some(far), true);
        assert!(matches!(replaced, SideDecision::Replace { .. }), "beyond the deadband must Replace");
    }

    #[test]
    fn deadband_does_not_hold_unprofitable_resting_quote() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        let mut cfg = qcfg();
        cfg.min_requote_bps = dec!(10.0); // make a one-tick bad move sit inside the deadband

        let place = evaluate_side(&edge(), &cfg, &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, None, true);
        let SideDecision::Place(d) = place else { panic!("expected a Place for an empty profitable slot") };

        // A buy quote one tick ABOVE the computed profitable bound is a worse maker price. Even
        // though this is inside the configured 10 bps deadband, it must take the urgent safety path.
        let current = CurrentOrder { price: d.price + spec().tick, qty: d.qty };
        let decision = evaluate_side(&edge(), &cfg, &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, Some(current), true);
        assert!(
            matches!(decision, SideDecision::Replace { reason: ReplaceReason::NoLongerProfitable, .. }),
            "unprofitable quote inside the deadband must still be replaced"
        );
    }

    fn full_cfg() -> Config {
        let toml = r#"
[edge]
min_net_profit_bps = "3.0"
slippage_buffer_bps = "1.5"
latency_buffer_bps = "2.0"
basis_buffer_bps = "1.0"
funding_buffer_bps = "0.0"
aster_maker_fee_bps = "0.0"
taker_fee_bps = "4.5"
[quote]
desired_notional = "100"
max_quote_distance_bps = "50.0"
max_hedge_slippage_bps = "50.0"
price_change_ticks_to_requote = 1
clamp_to_min_lot = true
[live]
max_book_staleness_ms = 5000
[[markets]]
aster_symbol = "BTCUSDT"
lighter_symbol = "BTC"
"#;
        toml::from_str(toml).unwrap()
    }

    #[tokio::test]
    async fn may_quote_reflects_clean_start_and_per_market_feeds() {
        use crate::hotpath::clock::mono_now_ns;
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        // Publish fresh books on BOTH of BTC's cells so its per-market feeds read fresh
        // (freshness is the cell's mono publish stamp, not the book's embedded ts).
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = Strategy::new(
            full_cfg(), &specs, &elig, reg.clone(), account.clone(), Journal::null(),
            SessionId::from_tag("t"), etx, htx,
        );
        let now = mono_now_ns();
        account.publish(funded_snapshot(now, Decimal::ZERO, Decimal::ZERO));
        // Before clean-start: no quoting.
        assert!(!strat.may_quote(&"BTC".into(), now));
        // After clean-start + fresh per-market feeds: quoting allowed (fresh flat account
        // snapshot, positions reconciled, no hedges in flight).
        strat.mark_clean_start();
        assert!(strat.may_quote(&"BTC".into(), now));
        // A REST-divergent feed on THIS market closes quoting for it (per-market, not global).
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_divergent(true);
        assert!(!strat.may_quote(&"BTC".into(), now));
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_divergent(false);
        assert!(strat.may_quote(&"BTC".into(), now));
        // A stale book also closes quoting (evaluated >max_book_staleness_ms past the publish),
        // even with a fresh account snapshot.
        let stale_now = now + 10_000_000_000; // +10s
        account.publish(funded_snapshot(stale_now, Decimal::ZERO, Decimal::ZERO));
        assert!(!strat.may_quote(&"BTC".into(), stale_now));
        // A latched freeze (e.g. hedge reject) stops quoting even with everything else green.
        strat.freeze(now, "test");
        assert!(!strat.may_quote(&"BTC".into(), now));
    }

    #[tokio::test]
    async fn may_quote_closes_on_stream_down_and_reopens_after_fresh_publish() {
        use crate::hotpath::clock::mono_now_ns;
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = Strategy::new(
            full_cfg(), &specs, &elig, reg.clone(), account.clone(), Journal::null(),
            SessionId::from_tag("t"), etx, htx,
        );
        account.publish(funded_snapshot(mono_now_ns(), Decimal::ZERO, Decimal::ZERO));
        strat.mark_clean_start();
        assert!(strat.may_quote(&"BTC".into(), mono_now_ns()));
        // A KNOWN disconnect closes the gate immediately, even though the book is still young.
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_stream_down();
        assert!(!strat.may_quote(&"BTC".into(), mono_now_ns()), "known disconnect must close the gate");
        // Liveness frames alone must not reopen it — only a full snapshot re-earns trust.
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().touch();
        assert!(!strat.may_quote(&"BTC".into(), mono_now_ns()));
        let (ab2, _) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab2);
        assert!(strat.may_quote(&"BTC".into(), mono_now_ns()), "fresh snapshot must reopen the gate");
        // Same per-market behavior for the Lighter cell.
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().mark_stream_down();
        assert!(!strat.may_quote(&"BTC".into(), mono_now_ns()));
        let (_, hb2) = books();
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb2);
        assert!(strat.may_quote(&"BTC".into(), mono_now_ns()));
    }

    #[test]
    fn correction_waits_for_its_venue_and_reduces_only_the_net_delta() {
        let account=AccountState::default(); let (etx,mut erx)=tokio::sync::mpsc::channel(16); let (htx,mut hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account); let m:MarketId="BTC".into(); let now=crate::hotpath::clock::mono_now_ns();
        strat.registry.cell(&m,VenueTag::Hyperliquid).unwrap().mark_stream_down(); strat.dispatch_correction(&m,dec!(0.05),dec!(-0.95),dec!(1),now);
        assert!(hrx.try_recv().is_err() && erx.try_recv().is_err()); let (_,book)=books(); strat.registry.cell(&m,VenueTag::Hyperliquid).unwrap().publish(book);
        strat.dispatch_correction(&m,dec!(0.05),dec!(-0.95),dec!(1),crate::hotpath::clock::mono_now_ns());
        let HedgeCommand::Hedge { intent,.. }=hrx.try_recv().unwrap() else {panic!("reduce-only hedge expected")};
        assert_eq!((intent.venue,intent.purpose,intent.hedge_side,intent.qty),(Venue::Hyperliquid,IntentPurpose::ReduceDelta,Side::Sell,dec!(0.05)));
        assert!(erx.try_recv().is_err());
    }

    #[test]
    fn terminal_partial_keeps_one_executable_lot_as_a_residual() {
        let account=AccountState::default(); let (etx,_erx)=tokio::sync::mpsc::channel(16); let (htx,_hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account); let m:MarketId="BTC".into(); let id=strat.orders.next_attempt_id(&m);
        strat.hedges.insert(id.to_hex(),HedgeIntent::with_qty(id,m.clone(),Side::Sell,dec!(0.5),dec!(100),1));
        strat.apply_execution_progress(id,dec!(0.499),Some(dec!(49.9)),Some(dec!(0)),true,None,Some(1700000000000),2);
        assert_eq!(strat.hedges[&id.to_hex()].remaining_qty(),dec!(0.001)); assert!(!strat.hedges[&id.to_hex()].state.is_resolved()); assert!(strat.correction_needed.contains(&m));
    }

    #[tokio::test]
    async fn hedge_unknown_marks_dangerous_and_freezes() {
        use crate::hotpath::clock::mono_now_ns;
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = Strategy::new(
            full_cfg(), &specs, &elig, reg, account, Journal::null(),
            SessionId::from_tag("t"), etx, htx,
        );
        strat.mark_clean_start();
        let now = mono_now_ns();
        let cloid = crate::livebot::ids::Cloid::recovery(&"BTC".into(), 9);
        let mut h = HedgeIntent::with_qty(cloid, "BTC".into(), Side::Sell, dec!(0.5), dec!(100), now);
        h.mark_submitted(now);
        strat.hedges.insert(cloid.to_hex(), h);

        strat.handle_exec_event(ExecEvent::HedgeUnknown { cloid, reason: "timeout".into() }, now);

        let h = strat.hedges.get(&cloid.to_hex()).unwrap();
        assert_eq!(h.state, crate::livebot::fills::HedgeState::Unknown);
        assert!(h.state.is_dangerous());
        assert!(strat.frozen);
        // The freeze also requests a safety sweep, which gates quoting until it clears.
        assert!(matches!(erx.try_recv(), Ok(ExecCommand::CancelAllBot)));
        assert_eq!(strat.maker_gate_reason(&"BTC".into(), now), Some("SAFETY_SWEEP_PENDING"));
    }

    #[test]
    fn touch_guard_hysteresis_only_raises_empty_blocked_slots() {
        let mut cfg = qcfg();
        cfg.min_aster_touch_distance_bps = dec!(24.0);
        cfg.min_aster_touch_hysteresis_bps = dec!(2.0);

        let empty_blocked = quote_cfg_for_touch_guard(&cfg, true, None);
        assert_eq!(empty_blocked.min_aster_touch_distance_bps, dec!(26.0));

        let current = Some(CurrentOrder { price: dec!(100), qty: dec!(1) });
        let resting_blocked = quote_cfg_for_touch_guard(&cfg, true, current);
        assert_eq!(resting_blocked.min_aster_touch_distance_bps, dec!(24.0));

        let empty_unblocked = quote_cfg_for_touch_guard(&cfg, false, None);
        assert_eq!(empty_unblocked.min_aster_touch_distance_bps, dec!(24.0));
    }

    // --- fast-VPS hardening tests (Tier 1/2) ---

    fn funded_snapshot(src: i64, a: Decimal, h: Decimal) -> AccountSnapshot {
        let mut snap = AccountSnapshot::empty();
        snap.source_ts_ns=src; snap.read_start_ns=src; snap.aster_margin_source_ns=src; snap.hl_margin_source_ns=src;
        snap.aster_available_usd=dec!(1000); snap.hl_withdrawable_usd=dec!(1000);
        snap.aster_equity_usd=dec!(1000); snap.hl_equity_usd=dec!(1000);
        for (venue, qty) in [(Venue::Aster,a),(Venue::Hyperliquid,h)] {
            let pos = crate::livebot::account::ScaledPosition { venue, market:"BTC".into(), signed_qty:qty, entry_px:dec!(100) };
            if venue==Venue::Aster { snap.aster_positions.push(pos); } else { snap.hl_positions.push(pos); }
        }
        snap
    }

    fn live_strat(
        etx: tokio::sync::mpsc::Sender<ExecCommand>,
        htx: tokio::sync::mpsc::Sender<HedgeCommand>,
        account: AccountState,
    ) -> Strategy {
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        Strategy::new(full_cfg(), &specs, &elig, reg, account, Journal::null(), SessionId::from_tag("t"), etx, htx)
    }

    #[test]
    fn touch_guard_status_expires_to_base_guard_after_timeout() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        strat.cfg.quote.min_aster_touch_distance_bps = dec!(24.0);
        strat.cfg.quote.min_aster_touch_hysteresis_bps = dec!(1.0);
        strat.cfg.quote.max_aster_touch_hysteresis_ms = 300_000;
        let m: MarketId = "BTC".into();
        let t0 = 1_000_000_000_i64;

        strat.latch_aster_touch_guard(&m, Side::Sell, t0);
        let active = strat.aster_touch_guard_status_for_empty(&m, Side::Sell, None, t0 + 299_999_000_000);
        assert_eq!(active, AsterTouchGuardStatus::Active);
        let active_cfg = quote_cfg_for_touch_guard(&strat.cfg.quote, active == AsterTouchGuardStatus::Active, None);
        assert_eq!(active_cfg.min_aster_touch_distance_bps, dec!(25.0));

        let expired = strat.expire_aster_touch_guard_if_needed(&m, Side::Sell, None, t0 + 300_000_000_000);
        assert_eq!(expired, AsterTouchGuardStatus::Expired);
        assert_eq!(
            strat.aster_touch_guard_status_for_empty(&m, Side::Sell, None, t0 + 300_000_000_001),
            AsterTouchGuardStatus::Off
        );
        let base_cfg = quote_cfg_for_touch_guard(&strat.cfg.quote, false, None);
        assert_eq!(base_cfg.min_aster_touch_distance_bps, dec!(24.0));
    }

    #[test]
    fn zero_touch_hysteresis_timeout_keeps_latch_until_rearm() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        strat.cfg.quote.min_aster_touch_distance_bps = dec!(24.0);
        strat.cfg.quote.min_aster_touch_hysteresis_bps = dec!(1.0);
        strat.cfg.quote.max_aster_touch_hysteresis_ms = 0;
        let m: MarketId = "BTC".into();
        let t0 = 1_000_000_000_i64;

        strat.latch_aster_touch_guard(&m, Side::Sell, t0);
        assert_eq!(
            strat.expire_aster_touch_guard_if_needed(&m, Side::Sell, None, t0 + 3_600_000_000_000),
            AsterTouchGuardStatus::Active
        );
    }

    #[tokio::test]
    async fn queued_maker_is_not_sent_after_freeze_cancel_or_book_change() {
        for cause in ["freeze", "targeted_cancel", "book_update"] {
            // Above EXEC_CANCEL_RESERVE, so the optional place is not held back.
            let (etx, mut erx) = tokio::sync::mpsc::channel(128);
            let (htx, _hrx) = tokio::sync::mpsc::channel(16);
            let account = AccountState::default();
            let mut strat = live_strat(etx, htx, account.clone());
            let market: MarketId = "BTC".into();
            let now = crate::hotpath::clock::mono_now_ns();
            account.publish(funded_snapshot(now, Decimal::ZERO, Decimal::ZERO));
            let versions = (strat.ctx[&market].aster_cell.content_version(), strat.ctx[&market].hedge_cell.content_version());
            let decision = evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy,
                &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true);
            strat.apply_decision_with_books(&market, Side::Buy, decision, &MarketScale::from_spec(&spec()), now, Some(versions)).await;
            let command = erx.try_recv().expect("quote enqueued before invalidation");
            match cause {
                "freeze" => strat.freeze(now, "test"),
                "targeted_cancel" => { strat.cancel_target(&market, Side::Buy, now); }
                _ => strat.ctx[&market].hedge_cell.publish(books().1),
            }
            // The Aster worker's send-time check: a revoked permit is never claimed, only rejected.
            let ExecCommand::Place { client_id, permit, .. } = command else { panic!("{cause}: place expected") };
            assert!(!permit.try_claim(crate::hotpath::clock::mono_now_ns()), "{cause}: revoked quote must never be sent");
            assert!(permit.is_cancelled(), "{cause}: revoked quote must be rejected, not left ambiguous");
            strat.handle_exec_event(ExecEvent::PlaceReject { client_id, reason: "revoked".into() }, now);
            assert!(!strat.orders.slot(&market, Side::Buy).unwrap().is_live());
        }
    }

    #[tokio::test]
    async fn min_requote_interval_throttles_nonurgent_replace_only() {
        // The live path honors `min_requote_interval_ms`. NON-URGENT replaces (price/qty
        // drift) are throttled; an urgent `NoLongerProfitable` replace BYPASSES. (`Place`/`Cancel`
        // are never gated.) Reduce-only mode would turn that urgent replace into a cancel-only.
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        strat.cfg.live.quote.reduce_position_only = false;
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let min_ms = full_cfg().live.quote.min_requote_interval_ms as i64; // 20 (default)
        let t0 = 1_000_000_000_i64;
        account.publish(funded_snapshot(t0, Decimal::ZERO, Decimal::ZERO)); // fresh margin for the guard
        // Seed a resting order (records last_requote_ns = t0 + a client id).
        strat.apply_decision(&m, Side::Buy, SideDecision::Place(Box::new(desired.clone())), &scale, t0).await;
        let place_cid = match erx.try_recv() {
            Ok(ExecCommand::Place { client_id, .. }) => client_id,
            other => panic!("place must emit, got {other:?}"),
        };
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: place_cid, venue_order_id: "oid0".into() }, t0 + 1);
        // Non-urgent replace within the interval => THROTTLED (no command emitted).
        let t_soon = t0 + (min_ms - 5) * 1_000_000;
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired.clone()), reason: ReplaceReason::PriceChanged }, &scale, t_soon).await;
        assert!(erx.try_recv().is_err(), "non-urgent replace within min_requote_interval must be throttled");
        // Urgent replace within the interval => BYPASSES (records last_requote_ns = t_soon).
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired.clone()), reason: ReplaceReason::NoLongerProfitable }, &scale, t_soon).await;
        let (old_cid, new_cid) = match erx.try_recv() {
            Ok(ExecCommand::Replace { old_client_id, new_client_id, .. }) => (old_client_id, new_client_id),
            other => panic!("urgent NoLongerProfitable replace must bypass the throttle, got {other:?}"),
        };
        // After the interval (measured from t_soon), a non-urgent replace goes through.
        let t_later = t_soon + (min_ms + 5) * 1_000_000;
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired.clone()), reason: ReplaceReason::QuantityChanged }, &scale, t_later).await;
        assert!(erx.try_recv().is_err(), "must not send a second replace while the first replace is pending");
        strat.handle_exec_event(ExecEvent::CancelAck { client_id: old_cid }, t_soon + 1);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: new_cid, venue_order_id: "oid1".into() }, t_soon + 2);
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::QuantityChanged }, &scale, t_later).await;
        assert!(matches!(erx.try_recv(), Ok(ExecCommand::Replace { .. })), "non-urgent replace after the interval must go through");
    }

    #[tokio::test]
    async fn live_reduce_only_no_longer_profitable_uses_cancel_only() {
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        strat.cfg.live.quote.reduce_position_only = true;
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let t0 = 1_000_000_000_i64;

        let place_cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, place_cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: place_cid, venue_order_id: "oid0".into() }, t0 + 1);

        strat
            .apply_decision(
                &m,
                Side::Buy,
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::NoLongerProfitable },
                &scale,
                t0 + 2,
            )
            .await;

        assert!(matches!(erx.try_recv(), Ok(ExecCommand::Cancel { .. })), "live reduce-only stale quote must be cancel-only");
        assert!(erx.try_recv().is_err(), "cancel-only path must not enqueue a replacement place");
    }

    #[test]
    fn self_heal_unfreezes_only_after_two_clean_snapshots() {
        // A latched freeze clears only when the clean condition (no outstanding hedges +
        // positions reconciled + stream fresh) holds again in a STRICTLY NEWER snapshot.
        use crate::livebot::account::AccountSnapshot;
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        strat.freeze(0, "test");
        assert!(strat.frozen);
        let now = 1_000_000_000_i64;
        let flat = |src: i64| AccountSnapshot {
            aster_available_usd: dec!(1000),
            aster_margin_source_ns: src,
            hl_margin_source_ns: src,
            hl_withdrawable_usd: dec!(1000),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(1000),
            hl_unrealized_usd: dec!(0),
            hl_upnl_marked: true,
            aster_positions: vec![],
            hl_positions: vec![],
            open_orders: vec![],
            generation: 0,
            source_ts_ns: src,
            read_start_ns: src,
        };
        // First clean snapshot: records heal_confirm, does NOT unfreeze yet.
        account.publish(flat(now));
        strat.recover_orphans(now + 1_000_000);
        assert!(strat.frozen, "must not unfreeze on the first clean snapshot");
        assert_eq!(strat.heal_confirm, Some(now));
        // A strictly-newer clean snapshot: NOW unfreeze.
        account.publish(flat(now + 500_000_000));
        strat.recover_orphans(now + 501_000_000);
        assert!(!strat.frozen, "must unfreeze once clean persists into a strictly-newer snapshot");
        assert_eq!(strat.heal_confirm, None);
    }

    #[test]
    fn frozen_after_clean_start_is_not_reported_as_not_reconciled() {
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        strat.mark_clean_start();
        strat.freeze(0, "exec_queue_send_failed");

        assert_eq!(strat.maker_gate_reason(&m, 1_000_000_000), Some(MAKER_GATE_FROZEN));
        assert!(!strat.note_quote_gate(&m, 1_000_000_000));
        assert!(strat.sweep_pending.is_none(), "FROZEN itself must not re-arm safety sweeps");
        assert!(erx.try_recv().is_err(), "FROZEN gate evaluation must not enqueue CancelAllBot");
    }

    #[test]
    fn network_pause_closes_the_quote_gate_without_a_sweep() {
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, AccountState::default());
        let m: MarketId = "BTC".into();
        let pause = Arc::new(std::sync::atomic::AtomicBool::new(true));
        strat.set_pause_flag(pause.clone());
        strat.mark_clean_start();

        assert_eq!(strat.maker_gate_reason(&m, 1_000_000_000), Some(MAKER_GATE_NETWORK_PAUSE));
        assert!(!strat.note_quote_gate(&m, 1_000_000_000));
        assert!(strat.sweep_pending.is_none() && erx.try_recv().is_err(), "a pause must not sweep");
        pause.store(false, std::sync::atomic::Ordering::Release);
        assert_ne!(strat.maker_gate_reason(&m, 1_000_000_000), Some(MAKER_GATE_NETWORK_PAUSE));
    }

    #[test]
    fn clean_safety_sweep_is_not_immediately_requeued_by_frozen_gate() {
        use crate::livebot::account::AccountSnapshot;
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        let m: MarketId = "BTC".into();
        strat.mark_clean_start();
        strat.freeze(0, "exec_queue_send_failed");
        strat.sweep_pending = Some(SweepState {
            requested_ns: 100,
            last_attempt_ns: 100,
            reason: "exec_queue_send_failed",
        });

        account.publish(AccountSnapshot {
            aster_available_usd: dec!(1000),
            aster_margin_source_ns: 200,
            hl_margin_source_ns: 200,
            hl_withdrawable_usd: dec!(1000),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(1000),
            hl_unrealized_usd: dec!(0),
            hl_upnl_marked: true,
            aster_positions: vec![],
            hl_positions: vec![],
            open_orders: vec![],
            generation: 0,
            source_ts_ns: 200,
            read_start_ns: 200,
        });

        strat.drive_safety_sweep(300);
        assert!(strat.sweep_pending.is_none(), "clean snapshot should clear the pending sweep");
        assert!(!strat.note_quote_gate(&m, 301));
        assert!(strat.sweep_pending.is_none(), "frozen gate must not re-add the just-cleared sweep");
        assert!(erx.try_recv().is_err(), "no new CancelAllBot should be queued");
    }

    #[tokio::test]
    async fn failed_cancel_dispatch_freezes_and_arms_safety_sweep() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(1);
        let etx_fill = etx.clone();
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let t0 = 1_000_000_000_i64;

        let cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);

        etx_fill.try_send(ExecCommand::RefreshDeadman { market: m.clone() }).unwrap();
        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, t0 + 2)
            .await;

        assert!(strat.frozen, "a dropped risk-reducing cancel must freeze");
        assert!(strat.sweep_pending.is_some(), "a dropped risk-reducing cancel must arm sweep recovery");
    }

    #[tokio::test]
    async fn sweep_pending_suppresses_targeted_cancel_spam() {
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let t0 = 1_000_000_000_i64;

        let cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);
        strat.sweep_pending = Some(SweepState {
            requested_ns: t0 + 2,
            last_attempt_ns: t0 + 2,
            reason: "test_sweep",
        });

        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, t0 + 3)
            .await;
        assert!(erx.try_recv().is_err(), "targeted cancel must be suppressed while CancelAllBot sweep is pending");
    }

    #[tokio::test]
    async fn pending_cancel_retry_backoff_suppresses_repeat_cancel() {
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let t0 = 1_000_000_000_i64;

        let cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);

        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, t0 + 2)
            .await;
        assert!(matches!(erx.try_recv(), Ok(ExecCommand::Cancel { .. })), "first cancel must send");

        let retry_too_soon = t0 + 2 + (strat.cfg.live.aster.cancel_retry_backoff_ms as i64 - 1) * 1_000_000;
        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, retry_too_soon)
            .await;
        assert!(erx.try_recv().is_err(), "duplicate cancel must be suppressed until retry backoff expires");
    }

    #[tokio::test]
    async fn urgent_no_longer_profitable_respects_aster_command_budget() {
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        strat.cfg.live.aster.max_rest_requests_per_minute = 1;
        strat.cfg.live.aster.optional_rest_reserve_per_minute = 0;
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let t0 = 1_000_000_000_i64;
        strat.account.publish(funded_snapshot(t0, dec!(0), dec!(0)));

        strat.apply_decision(&m, Side::Buy, SideDecision::Place(Box::new(desired.clone())), &scale, t0).await;
        let cid = match erx.try_recv() {
            Ok(ExecCommand::Place { client_id, .. }) => client_id,
            other => panic!("initial place should consume the one-command budget, got {other:?}"),
        };
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);

        strat
            .apply_decision(
                &m,
                Side::Buy,
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::NoLongerProfitable },
                &scale,
                t0 + 2,
            )
            .await;
        assert!(erx.try_recv().is_err(), "urgent cancel-only must not bypass the global Aster REST command budget");
        assert!(strat.frozen, "blocked urgent risk-reducing work must fail closed");
        assert!(strat.sweep_pending.is_some(), "blocked urgent cancel-only must arm sweep/reconcile recovery");
    }

    #[tokio::test]
    async fn aster_rate_limit_event_freezes_and_backs_off_commands() {
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let t0 = 1_000_000_000_i64;

        strat.handle_exec_event(
            ExecEvent::AsterRateLimited { reason: "HTTP 429 code -1003".into(), backoff_ms: 10_000 },
            t0,
        );
        assert!(strat.frozen);
        assert_eq!(strat.aster_429_count, 1);
        assert!(strat.aster_backoff_remaining_ms(t0 + 1_000_000) > 0);

        strat.apply_decision(&m, Side::Buy, SideDecision::Place(Box::new(desired)), &scale, t0 + 1_000_000).await;
        assert!(erx.try_recv().is_err(), "no Aster REST command should be enqueued during 429 backoff");
    }

    #[test]
    fn straddle_guard_skips_recovery_when_snapshot_predates_action() {
        // A snapshot whose REST reads BEGAN at-or-before this market's last hot action cannot
        // yet reflect it, so the orphan backstop ignores it — no dispatch AND the persistence gate is
        // not seeded. The boundary is STRICT: read_start == action is also skipped (a same-tick
        // `mono_now_ns()` collision must not be trusted). Only reads that began STRICTLY AFTER the
        // action are trusted (seed the gate).
        use crate::livebot::account::{AccountSnapshot, ScaledPosition, Venue};
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        let m: MarketId = "BTC".into();
        let t_action = 1_000_000_000_i64;
        strat.last_hot_action_ns.insert(m.clone(), t_action);
        // Simulate a fill that updated predicted Aster position (hedge failed, so Lighter stays 0).
        // This ensures the predicted-net cross-check sees the orphan as genuine (both predicted
        // and snapshot agree on the imbalance), not a phantom snapshot glitch.
        strat.aster_pos.insert(m.clone(), SignedPosition { qty: dec!(0.5), avg_px: dec!(100) });
        let orphan = |src: i64, read_start: i64| AccountSnapshot {
            aster_available_usd: dec!(1000),
            aster_margin_source_ns: src,
            hl_margin_source_ns: src,
            hl_withdrawable_usd: dec!(1000),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(1000),
            hl_unrealized_usd: dec!(0),
            hl_upnl_marked: true,
            aster_positions: vec![ScaledPosition { venue: Venue::Aster, market: m.clone(), signed_qty: dec!(0.5), entry_px: dec!(100) }],
            hl_positions: vec![],
            open_orders: vec![],
            generation: 0,
            source_ts_ns: src,
            read_start_ns: read_start,
        };
        // Reads BEGAN before the action (straddled) => guard SKIPS: no dispatch, gate not seeded.
        account.publish(orphan(t_action + 2_000_000, t_action - 1_000_000));
        strat.recover_orphans(t_action + 3_000_000);
        assert!(hrx.try_recv().is_err(), "straddled snapshot must not dispatch recovery");
        assert!(!strat.orphan_seen.contains_key(&m), "straddled snapshot must not seed the persistence gate");
        // EXACT-EQUALITY boundary: reads that began the SAME tick as the action (read_start == action)
        // are still untrustworthy (a same-instant `mono_now_ns()` collision) => guard SKIPS, gate not seeded.
        account.publish(orphan(t_action + 2_000_000, t_action));
        strat.recover_orphans(t_action + 3_000_000);
        assert!(hrx.try_recv().is_err(), "same-tick snapshot must not dispatch recovery");
        assert!(!strat.orphan_seen.contains_key(&m), "same-tick snapshot (read_start == action) must not seed the gate");
        // Reads BEGAN after the action => trusted: the persistence gate records the first sighting
        // (still no dispatch on the first valid snapshot — recovery needs a confirming snapshot).
        account.publish(orphan(t_action + 6_000_000, t_action + 4_000_000));
        strat.recover_orphans(t_action + 7_000_000);
        assert!(strat.orphan_seen.contains_key(&m), "post-action snapshot must seed the persistence gate");
        assert!(hrx.try_recv().is_err(), "first valid sighting must not dispatch yet");
    }

    fn orphan_snapshot_for(m: &MarketId, qty: Decimal, src: i64, read_start: i64) -> crate::livebot::account::AccountSnapshot {
        use crate::livebot::account::{ScaledPosition, Venue};
        let mut s = crate::livebot::account::AccountSnapshot::empty();
        s.aster_available_usd = dec!(1000);
        s.hl_withdrawable_usd = dec!(1000);
        s.aster_equity_usd = dec!(1000);
        s.hl_equity_usd = dec!(1000);
        s.aster_positions = vec![ScaledPosition { venue: Venue::Aster, market: m.clone(), signed_qty: qty, entry_px: dec!(100) }];
        s.source_ts_ns = src;
        s.read_start_ns = read_start;
        s
    }

    #[test]
    fn recovery_skips_redispatch_while_recovery_in_flight() {
        // An in-flight recovery intent that only partially covers the net must NOT trigger a
        // second overlapping recovery order — skip and defer, keep the outstanding record.
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(16);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        let m: MarketId = "BTC".into();
        strat.aster_pos.insert(m.clone(), SignedPosition { qty: dec!(0.5), avg_px: dec!(100) });
        let cloid = crate::livebot::ids::Cloid::recovery(&m, crate::livebot::fills::cum_scaled(dec!(0.5)));
        let mut intent = HedgeIntent::with_qty(cloid, m.clone(), Side::Sell, dec!(0.2), dec!(100), 1);
        intent.recovery = true;
        intent.mark_submitted(1);
        strat.hedges.insert(cloid.to_hex(), intent);

        // Two confirming snapshots, both read after any hot action (none recorded).
        account.publish(orphan_snapshot_for(&m, dec!(0.5), 10_000_000, 9_000_000));
        strat.recover_orphans(11_000_000);
        account.publish(orphan_snapshot_for(&m, dec!(0.5), 20_000_000_000, 19_000_000_000));
        strat.recover_orphans(21_000_000_000);

        assert!(hrx.try_recv().is_err(), "no second recovery order while one is in flight");
        assert!(erx.try_recv().is_err(), "no flatten either");
        assert!(
            strat.hedges.contains_key(&cloid.to_hex()),
            "outstanding in-flight record must not be overwritten"
        );
    }

    #[test]
    fn claimed_timeout_never_redispatches_from_position_snapshots() {
        let account=AccountState::default(); let (etx,_erx)=tokio::sync::mpsc::channel(16); let (htx,mut hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account.clone()); let m:MarketId="BTC".into(); let now=crate::hotpath::clock::mono_now_ns();
        strat.aster_pos.insert(m.clone(),SignedPosition { qty:dec!(0.5),avg_px:dec!(100) });
        let id=strat.orders.next_attempt_id(&m); let mut h=HedgeIntent::with_qty(id,m.clone(),Side::Sell,dec!(0.5),dec!(100),now-9_000_000_000);
        h.arm_admission(8000); h.mark_submitted(h.created_ns); assert!(h.admission.try_claim(h.created_ns+1)); h.check_timeout(now,8_000_000_000);
        assert!(!h.terminal); strat.hedges.insert(id.to_hex(),h);
        assert!(strat.fresh_hl_hedge_book_hot_first(&m,now,Side::Sell,dec!(0.5)).is_some());
        for tick in 1..=3 { account.publish(funded_snapshot(now+tick,dec!(0.5),dec!(0))); strat.recover_orphans(now+tick+1); }
        assert!(hrx.try_recv().is_err()); assert!(strat.hedges[&id.to_hex()].unresolved());
    }

    // --- startup position adoption + cross-check escalation ---

    /// A fresh snapshot reporting positions on BOTH venues for `m`.
    fn adopt_snapshot_for(
        m: &MarketId,
        aster: (Decimal, Decimal),
        hl: (Decimal, Decimal),
        src: i64,
    ) -> crate::livebot::account::AccountSnapshot {
        use crate::livebot::account::{ScaledPosition, Venue};
        let mut s = crate::livebot::account::AccountSnapshot::empty();
        s.aster_available_usd = dec!(1000);
        s.hl_withdrawable_usd = dec!(1000);
        s.aster_equity_usd = dec!(1000);
        s.hl_equity_usd = dec!(1000);
        if aster.0 != Decimal::ZERO {
            s.aster_positions =
                vec![ScaledPosition { venue: Venue::Aster, market: m.clone(), signed_qty: aster.0, entry_px: aster.1 }];
        }
        if hl.0 != Decimal::ZERO {
            s.hl_positions =
                vec![ScaledPosition { venue: Venue::Hyperliquid, market: m.clone(), signed_qty: hl.0, entry_px: hl.1 }];
        }
        s.source_ts_ns = src;
        s.read_start_ns = src - 1_000_000;
        s
    }

    #[test]
    fn adopt_seeds_predicted_from_snapshot() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        let m: MarketId = "BTC".into();
        let src = 10_000_000_000_i64;
        account.publish(adopt_snapshot_for(&m, (dec!(0.5), dec!(101)), (dec!(-0.3), dec!(102)), src));
        strat.adopt_reported_positions(src + 1_000_000); // 1ms later: fresh
        let a = strat.aster_pos.get(&m).expect("aster leg adopted");
        assert_eq!((a.qty, a.avg_px), (dec!(0.5), dec!(101)));
        let h = strat.hl_pos.get(&m).expect("hl leg adopted");
        assert_eq!((h.qty, h.avg_px), (dec!(-0.3), dec!(102)));
    }

    #[test]
    fn adopt_refuses_stale_snapshot() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        let m: MarketId = "BTC".into();
        // No snapshot at all: refuse.
        strat.adopt_reported_positions(1_000_000_000);
        assert!(strat.aster_pos.is_empty() && strat.hl_pos.is_empty(), "no snapshot ⇒ no adoption");
        // Stale snapshot (older than max_account_snapshot_age_ms): refuse.
        let src = 10_000_000_000_i64;
        account.publish(adopt_snapshot_for(&m, (dec!(0.5), dec!(101)), (Decimal::ZERO, Decimal::ZERO), src));
        let stale_now = src + (strat.cfg.live.max_account_snapshot_age_ms + 1) * 1_000_000;
        strat.adopt_reported_positions(stale_now);
        assert!(strat.aster_pos.is_empty() && strat.hl_pos.is_empty(), "stale snapshot ⇒ no adoption");
    }

    #[test]
    fn confirmed_restart_imbalance_reduces_the_excess_leg() {
        let account=AccountState::default(); let (etx,mut erx)=tokio::sync::mpsc::channel(16); let (htx,mut hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account.clone()); let now=crate::hotpath::clock::mono_now_ns();
        account.publish(funded_snapshot(now,dec!(0.5),dec!(0))); strat.adopt_reported_positions(now+1); strat.recover_orphans(now+2);
        account.publish(funded_snapshot(now+3,dec!(0.5),dec!(0))); strat.recover_orphans(now+4);
        assert!(std::iter::from_fn(||erx.try_recv().ok()).any(|cmd| matches!(cmd,ExecCommand::FlattenAster { intent,.. } if intent.qty==dec!(0.5) && intent.hedge_side==Side::Sell && intent.purpose==IntentPurpose::ReduceDelta)));
        assert!(hrx.try_recv().is_err());
    }

    #[tokio::test]
    async fn maker_backfill_then_late_private_fill_hedges_quantity_once() {
        let account=AccountState::default(); let (etx,_erx)=tokio::sync::mpsc::channel(16); let (htx,mut hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account.clone()); let m:MarketId="BTC".into(); let now=crate::hotpath::clock::mono_now_ns();
        let client=strat.orders.next_client_id(&m,Side::Buy).unwrap(); strat.orders.on_place_sent(&m,Side::Buy,client.clone(),10000,500,now);
        strat.orders.on_acked(&m,Side::Buy,"17".into()); account.publish(funded_snapshot(now+1,dec!(0.5),dec!(0))); strat.recover_orphans(now+2);
        assert!(!account.maker_queries().is_empty()); assert!(hrx.try_recv().is_err());
        strat.handle_maker_order_progress(m.clone(),Side::Buy,client.clone(),"17".into(),dec!(0.5),Some(dec!(50)),true,1700000000000,now+3).await;
        let HedgeCommand::Hedge { intent,.. }=hrx.try_recv().unwrap() else {panic!("hedge expected")}; assert_eq!(intent.qty,dec!(0.5));
        let late=AsterFill { market:m.clone(),aster_side:Side::Buy,order_id:"17".into(),trade_id:"99".into(),client_id:client,last_fill_qty:dec!(0.5),last_fill_px:dec!(100),cum_filled_qty:dec!(0.5),event_time_ms:1700000000000,reduce_only:false,commission:Some(dec!(0)),commission_asset:Some("USDT".into()) };
        strat.handle_maker_fill(late,now+4).await; assert!(hrx.try_recv().is_err()); assert_eq!(strat.aster_pos[&m].qty,dec!(0.5)); assert_eq!(strat.logical_ids[&m],intent.logical_id);
    }

    #[tokio::test]
    async fn cancel_filled_or_expired_closes_the_slot_hedges_the_backfill_without_freezing() {
        let account=AccountState::default(); let (etx,mut erx)=tokio::sync::mpsc::channel(16); let (htx,mut hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account.clone()); let m:MarketId="BTC".into(); let now=crate::hotpath::clock::mono_now_ns();
        let client=strat.orders.next_client_id(&m,Side::Buy).unwrap(); strat.orders.on_place_sent(&m,Side::Buy,client.clone(),10000,500,now);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: client.clone(), venue_order_id: "17".into() }, now+1);
        strat.handle_exec_event(ExecEvent::CancelFilledOrExpired { client_id: client.clone() }, now+2);
        assert!(!strat.frozen && strat.sweep_pending.is_none() && erx.try_recv().is_err(), "a finished order needs no freeze or sweep");
        assert!(strat.orders.live_slots().is_empty(), "the order no longer rests");
        assert_eq!(strat.maker_gate_reason(&m, now+2), Some("MAKER_EXECUTION_UNCERTAIN"));
        account.publish(funded_snapshot(now+3,dec!(0),dec!(0))); strat.recover_orphans(now+4);
        assert!(!account.maker_queries().is_empty(), "the uncertain order is backfilled");
        strat.handle_maker_order_progress(m.clone(),Side::Buy,client,"17".into(),dec!(0.5),Some(dec!(50)),true,1700000000000,now+5).await;
        let HedgeCommand::Hedge { intent,.. }=hrx.try_recv().unwrap() else {panic!("hedge expected")}; assert_eq!(intent.qty,dec!(0.5));
        assert!(strat.uncertain_makers.is_empty() && !strat.frozen);
    }

    #[tokio::test]
    async fn yield_cancels_grants_once_settled_and_resumes_after_two_post_release_snapshots() {
        let account=AccountState::default(); let (etx,mut erx)=tokio::sync::mpsc::channel(16); let (htx,mut hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account.clone()); let m:MarketId="BTC".into(); let now=crate::hotpath::clock::mono_now_ns();
        let ((_want_tx,want),(lease,granted))=(tokio::sync::watch::channel(None),tokio::sync::watch::channel(None));
        strat.set_rights(Some(Rights { want, lease })); strat.mark_cache.insert(m.clone(),dec!(100));
        let quotes:Vec<String>=[(Side::Buy,9900),(Side::Sell,10100)].into_iter().map(|(side,px)| {
            let client=strat.orders.next_client_id(&m,side).unwrap(); strat.orders.on_place_sent(&m,side,client.clone(),px,500,now);
            strat.handle_exec_event(ExecEvent::PlaceAck { client_id: client.clone(), venue_order_id: px.to_string() }, now+1); client
        }).collect();
        // The taker asks: both quotes are cancelled and the gate closes.
        strat.drive_yield(Some(1),now+2);
        let cancels=std::iter::from_fn(|| erx.try_recv().ok()).filter(|c| matches!(c, ExecCommand::Cancel { .. })).count();
        assert_eq!((cancels,strat.maker_gate_reason(&m,now+2)),(2,Some(MAKER_GATE_YIELDED)));
        // One cancel lands, the other finds its quote filled: no grant until that fill is hedged.
        strat.handle_exec_event(ExecEvent::CancelAck { client_id: quotes[0].clone() }, now+3);
        strat.handle_exec_event(ExecEvent::CancelFilledOrExpired { client_id: quotes[1].clone() }, now+3);
        strat.drive_yield(Some(1),now+4); assert!(granted.borrow().is_none(), "an uncertain quote");
        strat.handle_maker_order_progress(m.clone(),Side::Sell,quotes[1].clone(),"10100".into(),dec!(0.5),Some(dec!(50)),true,1700000000000,now+5).await;
        let HedgeCommand::Hedge { intent, aggressive_px, .. }=hrx.try_recv().unwrap() else {panic!("hedge expected")};
        strat.drive_yield(Some(1),now+6); assert!(granted.borrow().is_none(), "a hedge in flight");
        strat.handle_exec_event(ExecEvent::ExecutionProgress { cloid: intent.cloid, cumulative_qty: intent.qty, cumulative_quote_usd: Some(intent.qty*aggressive_px),
            cumulative_fee_usd: Some(Decimal::ZERO), terminal: true, venue_order_id: None, event_time_ms: None }, now+7);
        strat.drive_yield(Some(1),now+8);
        assert_eq!(granted.borrow().as_ref().map(|l| l.lease_id.clone()), Some("xemm-1".to_string()));
        // The hand-back revokes the lease and re-reads the Lighter nonce the taker used.
        let released=now+9; strat.drive_yield(None,released);
        assert!(granted.borrow().is_none() && matches!(hrx.try_recv(), Ok(HedgeCommand::RefreshNonce)));
        // The taker bought 0.13 on Aster and sold it on Lighter. A read from before the hand-back
        // does not count, and a read may still lag the taker's legs: XEMM quotes again only once
        // two reads in a row agree.
        for (read_ns,aster,lighter,resumed) in [(released-1,dec!(-0.5),dec!(0.5),false),(released+1,dec!(-0.5),dec!(0.5),false),
            (released+2,dec!(-0.37),dec!(0.5),false),(released+3,dec!(-0.37),dec!(0.37),false),(released+4,dec!(-0.37),dec!(0.37),true)] {
            account.publish(funded_snapshot(read_ns,aster,lighter)); strat.drive_yield(None,released+5);
            assert_eq!(strat.yield_state==Yield::Idle,resumed,"read at {}",read_ns-released);
        }
        assert_eq!((strat.aster_pos[&m].qty,strat.hl_pos[&m].qty),(dec!(-0.37),dec!(0.37)));
        assert!(!strat.frozen && strat.maker_gate_reason(&m,released+5)!=Some(MAKER_GATE_YIELDED));
    }

    // --- circuit breaker ---
    use crate::livebot::account::AccountSnapshot;
    use tokio_util::sync::CancellationToken;

    /// A fresh, flat snapshot whose total cross-venue equity is `total` at monotonic `src`.
    fn equity_snap(total: Decimal, src: i64) -> AccountSnapshot {
        let mut s = AccountSnapshot::empty();
        s.aster_equity_usd = total; // total_equity_usd() = aster + hl; put it all on one venue
        s.hl_equity_usd = Decimal::ZERO;
        s.source_ts_ns = src;
        s.read_start_ns = src;
        s
    }

    fn tmp_trip_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("xemm_cb_{}_{}.trip.json", std::process::id(), tag))
    }

    #[test]
    fn circuit_breaker_trips_on_equity_drawdown_and_latches() {
        let account = AccountState::default();
        let (etx, mut erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone());
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        let trip = tmp_trip_path("trips");
        let _ = std::fs::remove_file(&trip);
        strat.arm_circuit_breaker(trip.clone(), tok.clone());
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        strat.set_trip_flag(flag.clone());

        let mut t = 1_000_000_000_i64;
        // Baseline arms from the MEDIAN of the first K fresh marked samples; not before.
        for sample in [dec!(100), dec!(1), dec!(10000), dec!(99), dec!(101)] {
            assert!(strat.breaker_baseline_equity.is_none());
            account.publish(equity_snap(sample, t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_baseline_equity, Some(dec!(100)));
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());

        // Drawdown within the limit (loss 4 <= 5) does NOT trip.
        account.publish(equity_snap(dec!(96), t));
        strat.check_circuit_breaker(t);
        t += 1_000_000;
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());

        // Drawdown beyond the limit (loss 6 > 5) must PERSIST: breaches 1 and 2 warn only...
        for expect_streak in 1..BREAKER_TRIP_STREAK {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
            assert_eq!(strat.breaker_breach_streak, expect_streak);
            assert!(!strat.breaker_tripped);
            assert!(!tok.is_cancelled());
        }
        assert!(!flag.load(std::sync::atomic::Ordering::Acquire), "flag must not be set pre-trip");
        // ...the Nth consecutive breach TRIPS: cancels shutdown + writes the latch.
        account.publish(equity_snap(dec!(94), t));
        strat.check_circuit_breaker(t);
        t += 1_000_000;
        assert!(strat.breaker_tripped);
        assert!(tok.is_cancelled(), "breaker must cancel the shutdown token to halt the process");
        assert!(!trip.exists(), "hot path cannot persist");
        strat.journal.persist_trip_cold().unwrap();
        assert!(trip.exists(), "cold writer persists the latch");
        assert!(
            flag.load(std::sync::atomic::Ordering::Acquire),
            "breaker must set the in-memory trip flag for the shutdown exit-code backstop"
        );
        assert!(matches!(erx.try_recv().unwrap(), ExecCommand::CancelAllBot));

        // Latched: a further reading neither un-trips nor rewrites the latch.
        let latch = std::fs::read_to_string(&trip).unwrap();
        account.publish(equity_snap(dec!(80), t));
        strat.check_circuit_breaker(t);
        assert!(strat.breaker_tripped);
        assert_eq!(std::fs::read_to_string(&trip).unwrap(), latch, "latch must not be rewritten");
        let _ = std::fs::remove_file(&trip);
    }

    #[test]
    fn breaker_trip_flag_and_halt_survive_unwritable_latch_path() {
        // If write_trip fails (unwritable runs/ dir), the persistent latch never lands and the
        // shutdown check_shutdown() passes — the in-memory flag is the exit-code backstop. It
        // (and the halt token) must be set BEFORE / regardless of the latch write outcome.
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(64);
        let mut strat = live_strat(etx, htx, account.clone());
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        // Point the latch at a CHILD path of an existing regular FILE so create/rename fails.
        let blocker = std::env::temp_dir().join(format!("xemm_cb_blocker_{}", std::process::id()));
        std::fs::write(&blocker, b"x").unwrap();
        let trip = blocker.join("cannot.trip.json");
        strat.arm_circuit_breaker(trip.clone(), tok.clone());
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        strat.set_trip_flag(flag.clone());

        let mut t = 4_000_000_000_i64;
        for _ in 0..BREAKER_BASELINE_SAMPLES {
            account.publish(equity_snap(dec!(100), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        for _ in 0..BREAKER_TRIP_STREAK {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert!(strat.breaker_tripped);
        assert!(strat.journal.persist_trip_cold().is_err());
        assert!(!trip.exists(), "precondition: the latch write must actually have failed");
        assert!(tok.is_cancelled(), "halt must not depend on the latch write succeeding");
        assert!(
            flag.load(std::sync::atomic::Ordering::Acquire),
            "in-memory flag must be set even when the latch path is unwritable"
        );
        let _ = std::fs::remove_file(&blocker);
    }

    #[test]
    fn circuit_breaker_never_trips_on_stale_snapshot() {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        // A STALE snapshot (age > max_account_snapshot_age_ms) must never trip.
        let mut strat = live_strat(etx, htx, account.clone());
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        strat.arm_circuit_breaker(tmp_trip_path("stale"), tok.clone());
        let now = 2_000_000_000_i64;
        account.publish(equity_snap(dec!(100), now));
        // now_ns is far ahead of the snapshot's source_ts_ns => age >> max age => skip (no baseline).
        let way_later = now + 10_000 * 1_000_000;
        strat.check_circuit_breaker(way_later);
        assert!(strat.breaker_baseline_equity.is_none(), "stale snapshot must not arm/trip");
        assert_eq!(strat.breaker_breach_streak, 0, "stale sample must reset the breach streak");
        assert!(!tok.is_cancelled());
    }

    /// A live strategy with the breaker enabled (limit 5) and an armed baseline of 100,
    /// built from `BREAKER_BASELINE_SAMPLES` fresh publishes. Returns (strat, account,
    /// token, next monotonic ns).
    fn armed_breaker_strat(
        tag: &str,
    ) -> (Strategy, AccountState, CancellationToken, i64) {
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(64);
        let mut strat = live_strat(etx, htx, account.clone());
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        let trip = tmp_trip_path(tag);
        let _ = std::fs::remove_file(&trip);
        strat.arm_circuit_breaker(trip, tok.clone());
        let mut t = 3_000_000_000_i64;
        for _ in 0..BREAKER_BASELINE_SAMPLES {
            account.publish(equity_snap(dec!(100), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_baseline_equity, Some(dec!(100)));
        (strat, account, tok, t)
    }

    #[test]
    fn breaker_outlier_sample_does_not_trip() {
        let (mut strat, account, tok, mut t) = armed_breaker_strat("outlier");
        // Two breaching samples, then a recovered one: the streak resets.
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_breach_streak, 2);
        account.publish(equity_snap(dec!(100), t));
        strat.check_circuit_breaker(t);
        t += 1_000_000;
        assert_eq!(strat.breaker_breach_streak, 0);
        // Two more breaches still don't trip (the earlier pair must not carry over)...
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());
        // ...but a third consecutive one does.
        account.publish(equity_snap(dec!(94), t));
        strat.check_circuit_breaker(t);
        assert!(strat.breaker_tripped);
        let _ = std::fs::remove_file(tmp_trip_path("outlier"));
    }

    #[test]
    fn breaker_stale_sample_resets_streak() {
        let (mut strat, account, tok, mut t) = armed_breaker_strat("stale_reset");
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_breach_streak, 2);
        // The same (last) snapshot seen way later is stale: the streak must reset.
        strat.check_circuit_breaker(t + 10_000 * 1_000_000);
        assert_eq!(strat.breaker_breach_streak, 0);
        // Two fresh breaches after the gap must not trip (need a full new streak).
        t += 11_000 * 1_000_000;
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());
        let _ = std::fs::remove_file(tmp_trip_path("stale_reset"));
    }

    #[test]
    fn breaker_unmarked_sample_resets_streak_and_never_arms() {
        // Unmarked samples must never ARM a baseline...
        let account = AccountState::default();
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(64);
        let mut strat = live_strat(etx, htx, account.clone());
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        strat.arm_circuit_breaker(tmp_trip_path("unmarked_arm"), tok.clone());
        let mut t = 4_000_000_000_i64;
        for _ in 0..(BREAKER_BASELINE_SAMPLES + 2) {
            let mut s = equity_snap(dec!(100), t);
            s.hl_upnl_marked = false;
            account.publish(s);
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert!(strat.breaker_baseline_equity.is_none(), "unmarked samples must not arm");

        // ...and must RESET an in-progress breach streak, never count toward a trip.
        let (mut strat2, account2, tok2, mut t2) = armed_breaker_strat("unmarked_reset");
        for _ in 0..2 {
            account2.publish(equity_snap(dec!(94), t2));
            strat2.check_circuit_breaker(t2);
            t2 += 1_000_000;
        }
        assert_eq!(strat2.breaker_breach_streak, 2);
        let mut s = equity_snap(dec!(94), t2);
        s.hl_upnl_marked = false;
        account2.publish(s);
        strat2.check_circuit_breaker(t2);
        t2 += 1_000_000;
        assert_eq!(strat2.breaker_breach_streak, 0, "unmarked sample must reset the streak");
        for _ in 0..2 {
            account2.publish(equity_snap(dec!(94), t2));
            strat2.check_circuit_breaker(t2);
            t2 += 1_000_000;
        }
        assert!(!strat2.breaker_tripped);
        assert!(!tok2.is_cancelled());
        let _ = std::fs::remove_file(tmp_trip_path("unmarked_reset"));
    }

    #[test]
    fn breaker_same_generation_not_double_counted() {
        let (mut strat, account, tok, t) = armed_breaker_strat("same_gen");
        // ONE breaching snapshot observed on three consecutive ticks counts once.
        account.publish(equity_snap(dec!(94), t));
        for i in 0..3 {
            strat.check_circuit_breaker(t + i * 1_000_000);
        }
        assert_eq!(strat.breaker_breach_streak, 1, "one published sample must count once");
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());
        let _ = std::fs::remove_file(tmp_trip_path("same_gen"));
    }
    #[test]
    fn real_margin_reserves_resting_hedges_but_allows_reduction() {
        let account=AccountState::default();
        let (etx,_erx)=tokio::sync::mpsc::channel(128); let (htx,_hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account.clone()); let m:MarketId="BTC".into();
        let now=crate::hotpath::clock::mono_now_ns();
        let mut snap=funded_snapshot(now,dec!(0),dec!(0)); snap.hl_withdrawable_usd=dec!(1); account.publish(snap);
        assert!(!strat.margin_allows(&m,Side::Buy,dec!(0.13),dec!(100),now));
        let mut snap=funded_snapshot(now,dec!(-1),dec!(1)); snap.hl_withdrawable_usd=dec!(0); account.publish(snap);
        strat.aster_pos.insert(m.clone(),SignedPosition { qty:dec!(-1),avg_px:dec!(100) });
        strat.hl_pos.insert(m.clone(),SignedPosition { qty:dec!(1),avg_px:dec!(100) });
        assert!(strat.margin_allows(&m,Side::Buy,dec!(0.05),dec!(100),now));
        strat.aster_pos.clear(); strat.hl_pos.clear();
        let mut snap=funded_snapshot(now,dec!(0),dec!(0)); snap.hl_withdrawable_usd=dec!(40); account.publish(snap);
        assert!(strat.margin_allows(&m,Side::Buy,dec!(0.13),dec!(100),now));
        strat.orders.on_place_sent(&m,Side::Buy,"existing".into(),10000,130,now);
        assert!(!strat.margin_allows(&m,Side::Buy,dec!(0.13),dec!(100),now), "existing maker can still require its own hedge");
    }

    #[test]
    fn correction_dispatch_has_two_attempt_incident_limit() {
        let account=AccountState::default(); let (etx,mut erx)=tokio::sync::mpsc::channel(128); let (htx,_hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account); let m:MarketId="BTC".into(); let now=crate::hotpath::clock::mono_now_ns();
        for attempt in 0..2 {
            strat.dispatch_correction(&m,dec!(0.05),dec!(0.05),dec!(0),now+attempt);
            let command=erx.try_recv().unwrap();
            let ExecCommand::FlattenAster { intent,.. }=command else {panic!("correction expected")};
            assert_eq!(intent.qty,dec!(0.05));
            // Completed rejection is reconciled before a fresh attempt; do not reset incident count.
            strat.hedges.remove(&intent.cloid.to_hex());
        }
        strat.dispatch_correction(&m,dec!(0.05),dec!(0.05),dec!(0),now+3);
        assert!(erx.try_recv().is_err()); assert_eq!(strat.correction_attempts[&m],2);
    }

    #[tokio::test]
    async fn partials_netting_and_retry_share_one_economic_logical_id() {
        let account=AccountState::default(); let (etx,_erx)=tokio::sync::mpsc::channel(128); let (htx,mut hrx)=tokio::sync::mpsc::channel(16);
        let mut strat=live_strat(etx,htx,account); let m:MarketId="BTC".into();
        let (journal,rx)=Journal::channel(); strat.journal=journal.clone();
        let fill=|client:&str,side:Side,id:&str,last:Decimal,cum:Decimal| AsterFill { market:m.clone(),aster_side:side,order_id:client.into(),trade_id:id.into(),client_id:client.into(),last_fill_qty:last,last_fill_px:dec!(100),cum_filled_qty:cum,event_time_ms:1700000000000,reduce_only:false,commission:Some(dec!(0)),commission_asset:Some("USDT".into()) };
        let now=crate::hotpath::clock::mono_now_ns();
        let buy=strat.orders.next_client_id(&m,Side::Buy).unwrap(); let sell=strat.orders.next_client_id(&m,Side::Sell).unwrap();
        strat.handle_maker_fill(fill(&buy,Side::Buy,"1",dec!(0.03),dec!(0.03)),now).await;
        let logical=strat.logical_ids[&m]; assert!(hrx.try_recv().is_err());
        strat.handle_maker_fill(fill(&sell,Side::Sell,"2",dec!(0.01),dec!(0.01)),now+1).await;
        assert_eq!(strat.pending[&m].signed_qty,dec!(0.02));
        strat.handle_maker_fill(fill(&buy,Side::Buy,"3",dec!(0.03),dec!(0.06)),now+2).await;
        let HedgeCommand::Hedge { intent,.. }=hrx.try_recv().unwrap() else {panic!("hedge expected")};
        assert_eq!(intent.qty,dec!(0.05)); assert_eq!(intent.logical_id,logical);
        strat.handle_exec_event(ExecEvent::AttemptNotSent { cloid:intent.cloid,reason:"not sent".into() },now+3);
        let HedgeCommand::Hedge { intent:retry,.. }=hrx.try_recv().unwrap() else {panic!("retry expected")};
        assert_ne!(retry.cloid,intent.cloid); assert_eq!(retry.logical_id,logical);
        drop(strat); drop(journal); let mut bytes=Vec::new(); super::super::journal::run_journal_writer(rx,&mut bytes).await.unwrap();
        let rows:Vec<serde_json::Value>=String::from_utf8(bytes).unwrap().lines().map(|l|serde_json::from_str(l).unwrap()).collect();
        let maker:Vec<_>=rows.iter().filter(|r|r["kind"]=="maker_fill").collect(); assert_eq!(maker.len(),3);
        assert!(maker.iter().all(|r|r["detail"]["logical_id"]==logical.to_hex()));
    }

}

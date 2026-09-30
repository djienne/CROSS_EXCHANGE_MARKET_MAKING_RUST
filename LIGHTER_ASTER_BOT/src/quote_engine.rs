//! Compute the desired Aster maker quote for one side, priced backward from the
//! configured hedge venue, with an unconditional post-only cap re-asserted after tick
//! rounding, plus staleness / crossed / min-qty / min-notional gates.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::book::OrderBook;
use crate::decimal::{ceil_to_step, floor_to_step};
use crate::edge::{
    max_profitable_aster_bid, min_profitable_aster_ask, net_edge_bps_after_fees_and_buffers,
    EdgeConfig,
};
use crate::types::{RejectReason, Side};
use crate::vwap::vwap_take;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteEngineConfig {
    pub desired_notional: Decimal,
    pub max_quote_distance_bps: Decimal,
    /// Minimum distance from Aster's own touch. Prevents quote placement right next to
    /// a thin Aster BBO even when the cross-venue edge math says the quote is profitable.
    #[serde(default)]
    pub min_aster_touch_distance_bps: Decimal,
    /// Extra clearance required only after the live strategy has tripped the Aster touch
    /// guard for this side. Example: min=24 and hysteresis=2 means cancel/reject inside
    /// 24 bps, then resume placing that side only once it clears 26 bps. Default 2.0.
    #[serde(default = "default_min_aster_touch_hysteresis_bps")]
    pub min_aster_touch_hysteresis_bps: Decimal,
    /// Maximum time to require the wider hysteresis re-arm distance after a touch-guard
    /// reject. After this timeout, the side returns to the base touch-distance guard.
    /// Set 0 to keep hysteresis latched until the wider re-arm distance is reached.
    #[serde(default = "default_max_aster_touch_hysteresis_ms")]
    pub max_aster_touch_hysteresis_ms: i64,
    /// Visible depth multiple required when pricing quote safety. Example: 10.0 means
    /// a 0.20 HYPE quote prices the hedge and Aster effective touch using
    /// at least 2.0 HYPE of visible book depth.
    #[serde(default = "default_depth_liquidity_multiple")]
    #[serde(alias = "min_hl_bbo_depth_multiple")]
    #[serde(alias = "min_lighter_bbo_depth_multiple")]
    pub depth_liquidity_multiple: Decimal,
    pub max_hedge_slippage_bps: Decimal,
    pub price_change_ticks_to_requote: u32,
    /// When `desired_notional` buys fewer than the venue minimum lot (e.g. $50 is
    /// below BTC's ~0.001 BTC minimum), clamp the order UP to the smallest size
    /// both venues accept instead of rejecting it. Bounded by capital headroom.
    #[serde(default = "default_clamp_to_min_lot")]
    pub clamp_to_min_lot: bool,
    /// Per-side requote DEADBAND in bps: do NOT cancel/replace a still-profitable resting quote
    /// while the new desired price is within this many bps of the current resting price. This kills
    /// churn from sub-bps oscillation without ever masking the urgent `NoLongerProfitable` path. A
    /// qty change still requotes. Default 1.0 bps.
    #[serde(default = "default_min_requote_bps")]
    pub min_requote_bps: Decimal,
}

fn default_clamp_to_min_lot() -> bool {
    true
}

fn default_min_aster_touch_hysteresis_bps() -> Decimal {
    Decimal::from(2)
}

fn default_max_aster_touch_hysteresis_ms() -> i64 {
    300_000
}

fn default_depth_liquidity_multiple() -> Decimal {
    Decimal::from(10)
}

fn default_min_requote_bps() -> Decimal {
    Decimal::ONE
}

impl QuoteEngineConfig {
    /// Touch-distance threshold required to re-arm a side after it was suppressed by the
    /// Aster touch guard. This is intentionally separate from normal quote validation:
    /// existing/resting quotes are still cancelled at `min_aster_touch_distance_bps`,
    /// while new placements after a touch reject wait for this wider clearance.
    pub fn aster_touch_rearm_distance_bps(&self) -> Decimal {
        self.min_aster_touch_distance_bps
            + self.min_aster_touch_hysteresis_bps.max(Decimal::ZERO)
    }
}

/// A fully-specified candidate quote with its pricing and depth diagnostics.
#[derive(Debug, Clone)]
pub struct DesiredQuote {
    pub price: Decimal,
    pub qty: Decimal,

    pub hedge_side: Side,
    pub expected_hl_vwap: Decimal,
    pub expected_hl_depth_filled_qty: Decimal,
    pub expected_hl_worst_px: Decimal,
    pub expected_hl_depth_levels_used: usize,

    pub instant_edge_bps: Decimal,
    pub required_bps: Decimal,

    pub ref_px: Decimal,

    /// Same-side Aster touch used for the distance gate after filtering out BBO
    /// levels too small to cover this candidate quote.
    pub effective_aster_touch_px: Decimal,
    pub depth_liquidity_multiple: Decimal,
    pub depth_target_qty: Decimal,
    pub aster_depth_filled_qty: Decimal,
    pub aster_depth_levels_used: usize,
}

/// The current signed position on each leg plus the per-leg capital caps, used to
/// clamp a candidate quote to the remaining headroom. Both legs are perpetual
/// futures at leverage 1, so a leg's max position notional equals its capital.
#[derive(Debug, Clone, Copy)]
pub struct PositionContext {
    /// Signed Aster maker-leg position (+ long, − short).
    pub aster_pos_qty: Decimal,
    /// Signed hedge-leg position (+ long, − short).
    pub hl_pos_qty: Decimal,
    pub aster_cap_notional: Decimal,
    pub hl_cap_notional: Decimal,
    pub enforce: bool,
    /// When true, reject any candidate whose paired Aster fill + hedge would not reduce
    /// absolute cross-venue inventory.
    pub reduce_position_only: bool,
}

impl PositionContext {
    /// No cap (used by unit tests).
    #[cfg(test)]
    pub fn unconstrained() -> Self {
        PositionContext {
            aster_pos_qty: Decimal::ZERO,
            hl_pos_qty: Decimal::ZERO,
            aster_cap_notional: Decimal::ZERO,
            hl_cap_notional: Decimal::ZERO,
            enforce: false,
            reduce_position_only: false,
        }
    }
}

fn max_reduce_position_qty(pos: &PositionContext, side: Side) -> Result<Option<Decimal>, RejectReason> {
    if pos.aster_pos_qty == Decimal::ZERO && pos.hl_pos_qty == Decimal::ZERO {
        return Ok(None);
    }
    match side {
        Side::Buy if pos.aster_pos_qty < Decimal::ZERO && pos.hl_pos_qty > Decimal::ZERO => {
            Ok(Some((-pos.aster_pos_qty).min(pos.hl_pos_qty)))
        }
        Side::Sell if pos.aster_pos_qty > Decimal::ZERO && pos.hl_pos_qty < Decimal::ZERO => {
            Ok(Some(pos.aster_pos_qty.min(-pos.hl_pos_qty)))
        }
        _ => Err(RejectReason::PositionReduceOnly),
    }
}

/// Compute the desired quote, or the reason it was rejected.
#[allow(clippy::too_many_arguments)]
pub fn compute_desired_quote(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_book: &OrderBook,
    hl_book: &OrderBook,
    side: Side,
    tick: Decimal,
    step: Decimal,
    min_qty: Decimal,
    min_notional: Decimal,
    hl_min_notional: Decimal,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
) -> Result<DesiredQuote, RejectReason> {
    compute_desired_quote_with_aster_touch(
        edge,
        quote,
        aster_book,
        aster_book,
        hl_book,
        side,
        tick,
        step,
        min_qty,
        min_notional,
        hl_min_notional,
        max_staleness_ms,
        now,
        pos,
    )
}

/// Compute the desired quote with a separate Aster touch source. Live trading can
/// use fast `bookTicker` for post-only/touch-distance safety while keeping
/// `depth20` as the canonical queue/depth source. Passing the same book for both
/// arguments is equivalent to [`compute_desired_quote`].
#[allow(clippy::too_many_arguments)]
pub fn compute_desired_quote_with_aster_touch(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_depth_book: &OrderBook,
    aster_touch_book: &OrderBook,
    hl_book: &OrderBook,
    side: Side,
    tick: Decimal,
    step: Decimal,
    min_qty: Decimal,
    min_notional: Decimal,
    hl_min_notional: Decimal,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
) -> Result<DesiredQuote, RejectReason> {
    compute_desired_quote_with_aster_touch_source(
        edge,
        quote,
        aster_depth_book,
        aster_touch_book,
        false,
        hl_book,
        side,
        tick,
        step,
        min_qty,
        min_notional,
        hl_min_notional,
        max_staleness_ms,
        now,
        pos,
    )
}

/// Source-aware form used by live trading. When the selected Aster touch is BBO,
/// same-side top quantity must cover the candidate quote size or the distance gate
/// falls back to an effective touch from fresh `depth20`.
#[allow(clippy::too_many_arguments)]
pub fn compute_desired_quote_with_aster_touch_source(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_depth_book: &OrderBook,
    aster_touch_book: &OrderBook,
    aster_touch_is_bbo: bool,
    hl_book: &OrderBook,
    side: Side,
    tick: Decimal,
    step: Decimal,
    min_qty: Decimal,
    min_notional: Decimal,
    hl_min_notional: Decimal,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
) -> Result<DesiredQuote, RejectReason> {
    // --- book sanity + staleness gates ---
    let aster_bid = aster_touch_book.best_bid().ok_or(RejectReason::MissingAsterBook)?;
    let aster_ask = aster_touch_book.best_ask().ok_or(RejectReason::MissingAsterBook)?;
    if aster_touch_book.is_crossed() {
        return Err(RejectReason::BookCrossed);
    }
    let aster_mid = aster_touch_book.mid().ok_or(RejectReason::MissingMid)?;
    let hl_mid = hl_book.mid().ok_or(RejectReason::MissingMid)?;
    if aster_touch_book.age_ms(now) > max_staleness_ms {
        return Err(RejectReason::AsterBookStale);
    }
    if hl_book.age_ms(now) > max_staleness_ms {
        return Err(RejectReason::HlBookStale);
    }

    let two = Decimal::from(2);
    let ten_k = Decimal::from(10_000);
    let ref_px = (aster_mid + hl_mid) / two;
    // Defensive: both mids come from books whose non-positive levels are dropped at
    // construction (`OrderBook::from_levels`), so ref_px > 0 in practice (the
    // downstream `ref_px > 0` checks are then redundant but kept as belt-and-
    // suspenders). Guard the division anyway — a zero ref_px would panic the sizing
    // math (rust_decimal divide-by-zero). A non-positive mid is, in effect, missing.
    if ref_px <= Decimal::ZERO {
        return Err(RejectReason::MissingMid);
    }

    // --- sizing, clamped to the remaining capital headroom on BOTH legs ---
    let desired_qty = floor_to_step(quote.desired_notional / ref_px, step);
    // The smallest order BOTH venues accept (hedged 1:1): the Aster qty floor, the
    // Aster min-notional, and the Lighter min-notional, expressed in Aster steps. A
    // sub-step notional residual from pricing off ref_px is covered by the ceil.
    let eff_min_qty = if ref_px > Decimal::ZERO {
        min_qty
            .max(ceil_to_step(min_notional / ref_px, step))
            .max(ceil_to_step(hl_min_notional / ref_px, step))
    } else {
        min_qty
    };
    let mut qty = desired_qty;
    // Set when the capital cap shrinks the order; carries the binding leg's reason
    // so a clamp-to-zero (or below the min order size) reports the cap, not a plain
    // QuantityBelowMinimum.
    let mut cap_binding: Option<RejectReason> = None;
    // The most this side may fill given the cap (None when the cap is disabled).
    let mut headroom_qty: Option<Decimal> = None;
    // The most this side may fill while still reducing paired cross-venue inventory.
    let mut reduce_only_qty: Option<Decimal> = None;
    if pos.enforce && ref_px > Decimal::ZERO {
        let aster_cap_qty = pos.aster_cap_notional / ref_px;
        let hl_cap_qty = pos.hl_cap_notional / ref_px;
        // Headroom = how much this side may fill before |position| would breach a
        // leg's cap on the far end. A Buy grows Aster long / Lighter short; a Sell grows
        // Aster short / Lighter long. A reducing order may unwind to (but not past) the
        // opposite cap.
        let (aster_headroom, hl_headroom) = match side {
            Side::Buy => (
                (aster_cap_qty - pos.aster_pos_qty).max(Decimal::ZERO),
                (hl_cap_qty + pos.hl_pos_qty).max(Decimal::ZERO),
            ),
            Side::Sell => (
                (aster_cap_qty + pos.aster_pos_qty).max(Decimal::ZERO),
                (hl_cap_qty - pos.hl_pos_qty).max(Decimal::ZERO),
            ),
        };
        let (headroom, binding) = if aster_headroom <= hl_headroom {
            (aster_headroom, RejectReason::AsterPositionCapReached)
        } else {
            (hl_headroom, RejectReason::LighterPositionCapReached)
        };
        let capped = floor_to_step(headroom, step);
        headroom_qty = Some(capped);
        if capped < qty {
            qty = capped;
            cap_binding = Some(binding);
        }
    }
    if pos.reduce_position_only {
        if let Some(max_reduce) = max_reduce_position_qty(pos, side)? {
            let capped = floor_to_step(max_reduce, step);
            if capped <= Decimal::ZERO {
                return Err(RejectReason::PositionReduceOnly);
            }
            reduce_only_qty = Some(capped);
            if capped < qty {
                qty = capped;
            }
        }
    }
    if reduce_only_qty.is_some_and(|h| eff_min_qty > h) {
        return Err(RejectReason::PositionReduceOnly);
    }
    // Clamp UP to the venue minimum lot when enabled (e.g. $50 < BTC's 0.001 lot):
    // post the smallest valid order instead of rejecting. Capital headroom still
    // bounds it — if even the minimum won't fit, reject transparently (preferring
    // the cap reason when the cap was already the binding constraint).
    if quote.clamp_to_min_lot && qty < eff_min_qty {
        if headroom_qty.is_some_and(|h| eff_min_qty > h) {
            return Err(cap_binding.unwrap_or(RejectReason::MinLotExceedsHeadroom));
        }
        qty = eff_min_qty;
    }
    // Below the both-venue minimum (clamping off, or a cap shrank it): its fill could not be
    // hedged on Lighter.
    if qty <= Decimal::ZERO || qty < eff_min_qty {
        if reduce_only_qty.is_some_and(|h| h < min_qty) {
            return Err(RejectReason::PositionReduceOnly);
        }
        return Err(cap_binding.unwrap_or(RejectReason::QuantityBelowMinimum));
    }

    let depth_liquidity_multiple = quote.depth_liquidity_multiple.max(Decimal::ONE);
    let depth_target_qty = qty * depth_liquidity_multiple;

    // --- hedge VWAP estimate at the configured visible-depth target ---
    let hedge_side = side.opposite();
    let hv = vwap_take(hl_book, hedge_side, depth_target_qty).ok_or(RejectReason::HlHedgeVwapUnavailable)?;
    if hv.slippage_bps > quote.max_hedge_slippage_bps {
        return Err(RejectReason::HlHedgeSlippageTooHigh);
    }

    let price = match side {
        Side::Buy => {
            let cap = aster_ask.px - tick; // post-only: bid must rest below best ask
            let bound =
                max_profitable_aster_bid(hv.vwap, ref_px, edge).ok_or(RejectReason::NoProfitableAsterBid)?;
            let px = floor_to_step(cap.min(bound), tick);
            if px >= aster_ask.px {
                return Err(RejectReason::AsterPostOnlyPriceInvalid);
            }
            if px <= Decimal::ZERO {
                return Err(RejectReason::NoProfitableAsterBid);
            }
            px
        }
        Side::Sell => {
            let floor = aster_bid.px + tick; // post-only: ask must rest above best bid
            let bound =
                min_profitable_aster_ask(hv.vwap, ref_px, edge).ok_or(RejectReason::NoProfitableAsterAsk)?;
            let px = ceil_to_step(floor.max(bound), tick);
            if px <= aster_bid.px {
                return Err(RejectReason::AsterPostOnlyPriceInvalid);
            }
            px
        }
    };

    // --- recheck edge after rounding; enforce min notional ---
    let instant_edge_bps = net_edge_bps_after_fees_and_buffers(side, price, hv.vwap, ref_px, edge);
    if instant_edge_bps < edge.min_net_profit_bps {
        return Err(RejectReason::EdgeBelowMinAfterRounding);
    }
    if qty * price < min_notional {
        if pos.reduce_position_only {
            return Err(RejectReason::PositionReduceOnly);
        }
        return Err(cap_binding.unwrap_or(RejectReason::QuantityBelowMinimum));
    }

    // --- distance-from-touch gate ---
    let effective_touch = effective_aster_touch(
        aster_depth_book,
        aster_touch_book,
        aster_touch_is_bbo,
        side,
        depth_target_qty,
        max_staleness_ms,
        now,
    )?;
    let distance_bps = match side {
        Side::Buy => (effective_touch.px - price).max(Decimal::ZERO) / ref_px * ten_k,
        Side::Sell => (price - effective_touch.px).max(Decimal::ZERO) / ref_px * ten_k,
    };
    if distance_bps > quote.max_quote_distance_bps {
        return Err(RejectReason::QuoteTooFarFromTouch);
    }
    if distance_bps < quote.min_aster_touch_distance_bps {
        return Err(RejectReason::QuoteTooCloseToTouch);
    }

    Ok(DesiredQuote {
        price,
        qty,
        hedge_side,
        expected_hl_vwap: hv.vwap,
        expected_hl_depth_filled_qty: hv.filled_qty,
        expected_hl_worst_px: hv.worst_px,
        expected_hl_depth_levels_used: hv.levels_used,
        instant_edge_bps,
        required_bps: edge.required_bps(),
        ref_px,
        effective_aster_touch_px: effective_touch.px,
        depth_liquidity_multiple,
        depth_target_qty,
        aster_depth_filled_qty: effective_touch.filled_qty,
        aster_depth_levels_used: effective_touch.levels_used,
    })
}

#[derive(Debug, Clone, Copy)]
struct EffectiveAsterTouch {
    px: Decimal,
    filled_qty: Decimal,
    levels_used: usize,
}

fn effective_aster_touch(
    depth_book: &OrderBook,
    raw_touch_book: &OrderBook,
    raw_touch_is_bbo: bool,
    side: Side,
    target_qty: Decimal,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
) -> Result<EffectiveAsterTouch, RejectReason> {
    let raw = match side {
        Side::Buy => raw_touch_book.best_bid(),
        Side::Sell => raw_touch_book.best_ask(),
    }
    .ok_or(RejectReason::MissingAsterBook)?;
    if raw_touch_is_bbo && raw.qty >= target_qty {
        return Ok(EffectiveAsterTouch {
            px: raw.px,
            filled_qty: target_qty,
            levels_used: 1,
        });
    }

    if depth_book.age_ms(now) > max_staleness_ms
        || depth_book.is_crossed()
        || depth_book.best_bid().is_none()
        || depth_book.best_ask().is_none()
    {
        return Err(RejectReason::AsterEffectiveTouchUnavailable);
    }
    let levels = match side {
        Side::Buy => depth_book.bids.as_slice(),
        Side::Sell => depth_book.asks.as_slice(),
    };
    let mut cumulative = Decimal::ZERO;
    let mut levels_used = 0;
    for level in levels {
        cumulative += level.qty;
        levels_used += 1;
        if cumulative >= target_qty {
            return Ok(EffectiveAsterTouch {
                px: level.px,
                filled_qty: target_qty,
                levels_used,
            });
        }
    }
    Err(RejectReason::AsterEffectiveTouchUnavailable)
}

/// Net edge (bps, after fees and buffers) of an already-resting quote at `price`
/// for `qty`, hedged against the *current* `hl_book`. Used to re-validate a resting
/// quote on every book move: if this falls below `min_net_profit_bps` the quote can
/// no longer be hedged profitably and must be pulled. `None` when the hedge VWAP is
/// unavailable (empty book side) — the caller treats that as "pull the quote".
pub fn resting_quote_net_edge_bps(
    edge: &EdgeConfig,
    hl_book: &OrderBook,
    side: Side,
    price: Decimal,
    qty: Decimal,
    ref_px: Decimal,
    depth_liquidity_multiple: Decimal,
) -> Option<Decimal> {
    let depth_target_qty = qty * depth_liquidity_multiple.max(Decimal::ONE);
    let hv = vwap_take(hl_book, side.opposite(), depth_target_qty)?;
    Some(net_edge_bps_after_fees_and_buffers(side, price, hv.vwap, ref_px, edge))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    pub(crate) fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    pub(crate) fn edge() -> EdgeConfig {
        EdgeConfig {
            min_net_profit_bps: dec!(3.0),
            slippage_buffer_bps: dec!(1.5),
            latency_buffer_bps: dec!(2.0),
            basis_buffer_bps: dec!(1.0),
            funding_buffer_bps: dec!(0.0),
            aster_maker_fee_bps: dec!(0.0),
            taker_fee_bps: dec!(4.5),
            hyperliquid_taker_fee_bps: None,
        }
    }

    fn qcfg() -> QuoteEngineConfig {
        QuoteEngineConfig {
            desired_notional: dec!(100),
            max_quote_distance_bps: dec!(5.0),
            min_aster_touch_distance_bps: dec!(0.0),
            min_aster_touch_hysteresis_bps: dec!(2.0),
            max_aster_touch_hysteresis_ms: 300_000,
            depth_liquidity_multiple: dec!(10.0),
            max_hedge_slippage_bps: dec!(5.0),
            price_change_ticks_to_requote: 1,
            clamp_to_min_lot: true,
            min_requote_bps: dec!(1.0),
        }
    }

    fn position_ctx(aster_pos_qty: Decimal, hl_pos_qty: Decimal, reduce_position_only: bool) -> PositionContext {
        PositionContext {
            aster_pos_qty,
            hl_pos_qty,
            aster_cap_notional: Decimal::ZERO,
            hl_cap_notional: Decimal::ZERO,
            enforce: false,
            reduce_position_only,
        }
    }

    #[test]
    fn aster_touch_rearm_distance_adds_hysteresis() {
        let mut cfg = qcfg();
        cfg.min_aster_touch_distance_bps = dec!(24.0);
        cfg.min_aster_touch_hysteresis_bps = dec!(2.0);
        assert_eq!(cfg.aster_touch_rearm_distance_bps(), dec!(26.0));
        cfg.min_aster_touch_hysteresis_bps = dec!(-5.0);
        assert_eq!(cfg.aster_touch_rearm_distance_bps(), dec!(24.0));
    }

    // A wide, deep, symmetric book around 100 so a profitable bid exists.
    fn books() -> (OrderBook, OrderBook) {
        let aster = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100)), (dec!(99.40), dec!(100))],
            vec![(dec!(100.50), dec!(100)), (dec!(100.60), dec!(100))],
            ts(),
            ts(),
        );
        let hl = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100)), (dec!(99.90), dec!(100))],
            vec![(dec!(100.05), dec!(100)), (dec!(100.10), dec!(100))],
            ts(),
            ts(),
        );
        (aster, hl)
    }

    #[test]
    fn quotes_price_backward_from_hedge_vwap_on_both_sides() {
        // ref = 100, qty = $100/100 = 1, req = 3+1.5+2+1 = 7.5 bps, Lighter taker 4.5 bps, Aster maker 0:
        // bid <= 99.95*(1-0.00045) - 0.075 = 99.8300225 -> floor 99.83;
        // ask >= 100.05*(1+0.00045) + 0.075 = 100.1700225 -> ceil 100.18.
        let (a, h) = books();
        let quote = |side| compute_desired_quote(
            &edge(), &qcfg(), &a, &h, side,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        let bid = quote(Side::Buy);
        assert_eq!((bid.price, bid.qty, bid.hedge_side), (dec!(99.83), dec!(1), Side::Sell));
        let ask = quote(Side::Sell);
        assert_eq!((ask.price, ask.qty, ask.hedge_side), (dec!(100.18), dec!(1), Side::Buy));
    }

    #[test]
    fn lighter_hedge_vwap_uses_depth_target_not_quote_qty() {
        let aster = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            ts(),
            ts(),
        );
        let hl = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(1)), (dec!(99.85), dec!(20))],
            vec![(dec!(100.05), dec!(100))],
            ts(),
            ts(),
        );
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(500.0);
        cfg.max_hedge_slippage_bps = dec!(50.0);
        let q = compute_desired_quote(
            &edge(), &cfg, &aster, &hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(q.qty, dec!(1.000));
        assert_eq!(q.depth_target_qty, dec!(10.0000));
        assert_eq!(q.expected_hl_vwap, dec!(99.86));
        // Priced off the 10-lot VWAP: 99.86*(1-0.00045) - 0.075 = 99.740063 -> 99.74 (1-lot VWAP would give 99.83).
        assert_eq!(q.price, dec!(99.74));
        assert_eq!(q.expected_hl_worst_px, dec!(99.85));
        assert_eq!(q.expected_hl_depth_levels_used, 2);
        assert_eq!(q.expected_hl_depth_filled_qty, q.depth_target_qty);
    }

    #[test]
    fn stale_book_rejected() {
        let (a, h) = books();
        let later = ts() + chrono::Duration::milliseconds(5_000);
        let r = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, later,
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::AsterBookStale);
    }

    #[test]
    fn far_from_touch_rejected() {
        // Lighter well below Aster: the profitable backward-priced bid sits far below
        // the Aster touch, so the distance gate rejects it.
        let (a, _) = books();
        let low_hl = OrderBook::from_levels(
            vec![(dec!(98.00), dec!(100))],
            vec![(dec!(98.01), dec!(100))],
            ts(),
            ts(),
        );
        let r = compute_desired_quote(
            &edge(), &qcfg(), &a, &low_hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::QuoteTooFarFromTouch);
    }

    // Aster and Lighter tightly aligned, so the backward-priced profitable quote rests
    // at its natural ~(required+fees) depth (~12bps) rather than at the touch.
    fn tight_books() -> (OrderBook, OrderBook) {
        let aster = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(100))],
            vec![(dec!(100.01), dec!(100))],
            ts(),
            ts(),
        );
        let hl = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(100))],
            vec![(dec!(100.01), dec!(100))],
            ts(),
            ts(),
        );
        (aster, hl)
    }

    #[test]
    fn profitable_quote_always_exists_but_rests_deep() {
        let (a, h) = tight_books();
        // A 5bps gate rejects the (always-existing) profitable quote — it rests deeper.
        let mut tight = qcfg();
        tight.max_quote_distance_bps = dec!(5.0);
        let r = compute_desired_quote(
            &edge(), &tight, &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::QuoteTooFarFromTouch);
        // With a loose sanity bound the same profitable quote posts, ~12bps deep.
        let mut loose = qcfg();
        loose.max_quote_distance_bps = dec!(50.0);
        let dq = compute_desired_quote(
            &edge(), &loose, &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        // 99.99*(1-0.00045) - 0.075 = 99.8700045 -> 99.87, i.e. 12 bps below the 99.99 bid.
        assert_eq!(dq.price, dec!(99.87));
    }

    #[test]
    fn min_touch_distance_rejects_buy_too_close_to_aster_bid() {
        let (a, h) = tight_books();
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(50.0);
        cfg.min_aster_touch_distance_bps = dec!(20.0);
        let r = compute_desired_quote(
            &edge(), &cfg, &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::QuoteTooCloseToTouch);
    }

    #[test]
    fn min_touch_distance_rejects_sell_too_close_to_aster_ask() {
        let (a, h) = tight_books();
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(50.0);
        cfg.min_aster_touch_distance_bps = dec!(20.0);
        let r = compute_desired_quote(
            &edge(), &cfg, &a, &h, Side::Sell,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::QuoteTooCloseToTouch);
    }

    #[test]
    fn aster_touch_book_controls_min_distance() {
        let (depth, _) = books();
        let (_, hl) = tight_books();
        let touch = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(1000))],
            vec![(dec!(100.01), dec!(1000))],
            ts(),
            ts(),
        );
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(50.0);
        cfg.min_aster_touch_distance_bps = dec!(20.0);
        let r = compute_desired_quote_with_aster_touch(
            &edge(), &cfg, &depth, &touch, &hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::QuoteTooCloseToTouch);
    }

    #[test]
    fn effective_touch_uses_bbo_when_top_covers_depth_target() {
        let (depth, hl) = tight_books();
        let bbo = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(20))],
            vec![(dec!(100.01), dec!(20))],
            ts(),
            ts(),
        );
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(50.0);
        let dq = compute_desired_quote_with_aster_touch_source(
            &edge(), &cfg, &depth, &bbo, true, &hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(dq.effective_aster_touch_px, dec!(99.99));
    }

    #[test]
    fn effective_touch_uses_depth_when_bbo_top_is_thin() {
        let hl = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(100))],
            vec![(dec!(100.01), dec!(100))],
            ts(),
            ts(),
        );
        let depth = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(0.25)), (dec!(99.80), dec!(20))],
            vec![(dec!(100.01), dec!(2))],
            ts(),
            ts(),
        );
        let bbo = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(0.25))],
            vec![(dec!(100.01), dec!(2))],
            ts(),
            ts(),
        );
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(50.0);
        let dq = compute_desired_quote_with_aster_touch_source(
            &edge(), &cfg, &depth, &bbo, true, &hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(dq.effective_aster_touch_px, dec!(99.80));
    }

    #[test]
    fn thin_bbo_rejects_when_depth_cannot_cover_quote_qty() {
        let hl = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(100))],
            vec![(dec!(100.01), dec!(100))],
            ts(),
            ts(),
        );
        let depth = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(0.25))],
            vec![(dec!(100.01), dec!(2))],
            ts(),
            ts(),
        );
        let bbo = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(0.25))],
            vec![(dec!(100.01), dec!(2))],
            ts(),
            ts(),
        );
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(50.0);
        let r = compute_desired_quote_with_aster_touch_source(
            &edge(), &cfg, &depth, &bbo, true, &hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::AsterEffectiveTouchUnavailable);
    }

    #[test]
    fn thin_bbo_rejects_when_depth_is_stale() {
        let now = ts() + chrono::Duration::milliseconds(10_000);
        let hl = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(100))],
            vec![(dec!(100.01), dec!(100))],
            now,
            now,
        );
        let depth = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(2))],
            vec![(dec!(100.01), dec!(2))],
            ts(),
            ts(),
        );
        let bbo = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(0.25))],
            vec![(dec!(100.01), dec!(2))],
            now,
            now,
        );
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(50.0);
        let r = compute_desired_quote_with_aster_touch_source(
            &edge(), &cfg, &depth, &bbo, true, &hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 5000, now,
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::AsterEffectiveTouchUnavailable);
    }

    #[test]
    fn post_only_still_uses_raw_opposite_touch_when_bbo_is_tiny() {
        let depth = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(20))],
            vec![(dec!(100.50), dec!(2))],
            ts(),
            ts(),
        );
        let hl = OrderBook::from_levels(
            vec![(dec!(99.99), dec!(100))],
            vec![(dec!(100.01), dec!(100))],
            ts(),
            ts(),
        );
        let bbo = OrderBook::from_levels(
            vec![(dec!(99.80), dec!(2))],
            vec![(dec!(99.90), dec!(0.01))],
            ts(),
            ts(),
        );
        let mut cfg = qcfg();
        cfg.max_quote_distance_bps = dec!(500.0);
        let dq = compute_desired_quote_with_aster_touch_source(
            &edge(), &cfg, &depth, &bbo, true, &hl, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert!(dq.price < dec!(99.90), "post-only cap must respect raw tiny ask");
    }

    #[test]
    fn capital_cap_suppresses_increasing_side_keeps_reducing() {
        let (a, h) = books();
        // ref ~100, cap $100 => cap_qty ~1.0. Already long ~0.9995 => buy headroom
        // ~0.0005 (below the 0.001 min) on both legs => the increasing Buy is rejected,
        // while the reducing Sell still posts at full size.
        let pos = PositionContext {
            aster_pos_qty: dec!(0.9995),
            hl_pos_qty: dec!(-0.9995),
            aster_cap_notional: dec!(100),
            hl_cap_notional: dec!(100),
            enforce: true,
            reduce_position_only: false,
        };
        let buy = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        );
        assert_eq!(buy.unwrap_err(), RejectReason::AsterPositionCapReached);
        compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Sell,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        )
        .unwrap();
    }

    #[test]
    fn capital_cap_clamps_order_below_full_size() {
        let (a, h) = books();
        // Long 0.5 of a 1.0 cap_qty => buy headroom 0.5, so a desired 1.0 order is
        // clamped to 0.5 (not rejected).
        let pos = PositionContext {
            aster_pos_qty: dec!(0.5),
            hl_pos_qty: dec!(-0.5),
            aster_cap_notional: dec!(100),
            hl_cap_notional: dec!(100),
            enforce: true,
            reduce_position_only: false,
        };
        let buy = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        )
        .unwrap();
        assert_eq!(buy.qty, dec!(0.5));
    }

    #[test]
    fn reduce_position_only_short_aster_long_hl_allows_only_buy() {
        let (a, h) = books();
        let pos = position_ctx(dec!(-1.5), dec!(1.5), true);
        compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        )
        .unwrap();
        let sell = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Sell,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        );
        assert_eq!(sell.unwrap_err(), RejectReason::PositionReduceOnly);
    }

    #[test]
    fn reduce_position_only_long_aster_short_hl_allows_only_sell() {
        let (a, h) = books();
        let pos = position_ctx(dec!(1.5), dec!(-1.5), true);
        let buy = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        );
        assert_eq!(buy.unwrap_err(), RejectReason::PositionReduceOnly);
        compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Sell,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        )
        .unwrap();
    }

    #[test]
    fn reduce_position_only_flat_allows_both_sides() {
        let (a, h) = books();
        let pos = position_ctx(dec!(0), dec!(0), true);
        for side in [Side::Buy, Side::Sell] {
            compute_desired_quote(
                &edge(), &qcfg(), &a, &h, side,
                dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
            )
            .unwrap();
        }
    }

    #[test]
    fn reduce_position_only_clamps_to_remaining_paired_inventory() {
        let (a, h) = books();
        let pos = position_ctx(dec!(-0.25), dec!(0.25), true);
        let buy = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        )
        .unwrap();
        assert_eq!(buy.qty, dec!(0.25));
    }

    #[test]
    fn reduce_position_only_dust_below_minimum_rejects() {
        let (a, h) = books();
        let pos = position_ctx(dec!(-0.01), dec!(0.01), true);
        let r = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
        );
        assert_eq!(r.unwrap_err(), RejectReason::PositionReduceOnly);
    }

    #[test]
    fn reduce_position_only_disabled_preserves_two_sided_quotes() {
        let (a, h) = books();
        let pos = position_ctx(dec!(0), dec!(0), false);
        for side in [Side::Buy, Side::Sell] {
            compute_desired_quote(
                &edge(), &qcfg(), &a, &h, side,
                dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750, ts(), &pos,
            )
            .unwrap();
        }
    }

    #[test]
    fn min_lot_clamps_up_small_order() {
        let (a, h) = books();
        // $100 at ref ~100 => desired 1.0, but the venue min lot is 2.0: clamp UP
        // to 2.0 and flag it (transparency), rather than reject — and still profitable.
        let dq = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(2.0), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(dq.qty, dec!(2.0));
    }

    #[test]
    fn min_lot_exceeds_headroom_rejects() {
        let (a, h) = books();
        // A large Lighter min-notional forces eff_min ~3.0, but only ~2.0 headroom remains
        // and the cap didn't bind the small desired order => reject transparently.
        let pos = PositionContext {
            aster_pos_qty: dec!(1.0),
            hl_pos_qty: dec!(-1.0),
            aster_cap_notional: dec!(300),
            hl_cap_notional: dec!(300),
            enforce: true,
            reduce_position_only: false,
        };
        let r = compute_desired_quote(
            &edge(), &qcfg(), &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(300), 750, ts(), &pos,
        );
        assert_eq!(r.unwrap_err(), RejectReason::MinLotExceedsHeadroom);
    }

    #[test]
    fn clamp_disabled_rejects_small_order() {
        let (a, h) = books();
        let mut cfg = qcfg();
        cfg.clamp_to_min_lot = false; // old behavior: reject when below min
        let r = compute_desired_quote(
            &edge(), &cfg, &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(2.0), dec!(5), dec!(5), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::QuantityBelowMinimum);
        // Above Aster's minimum but under a $150 Lighter minimum: its fill could not be hedged.
        let r = compute_desired_quote(
            &edge(), &cfg, &a, &h, Side::Buy,
            dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(150), 750, ts(),
            &PositionContext::unconstrained(),
        );
        assert_eq!(r.unwrap_err(), RejectReason::QuantityBelowMinimum);
    }
}

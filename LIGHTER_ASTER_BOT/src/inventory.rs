//! Pending-inventory accumulation for sub-min-notional fills.
//!
//! An opposite-direction fill nets the position down and books
//! the realized PnL on the closed quantity (it never silently zeroes the average
//! price). If the residual flips sign it opens fresh at the new fill price. A
//! same-direction fill accumulates with a size-weighted average.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::config::HedgeVenue;
use crate::decimal::ceil_to_step;
use crate::types::Side;

#[derive(Debug, Clone)]
pub struct HedgeabilityRules {
    pub hedge_min_notional: Decimal,
    pub hedge_qty_step: Decimal,
}

#[derive(Debug, Clone)]
pub struct PendingInventory {
    /// Positive => net long Aster (hedge by selling on Lighter); negative => net short.
    pub signed_qty: Decimal,
    pub avg_aster_px: Decimal,
    pub first_fill_ts: DateTime<Utc>,
    pub last_fill_ts: DateTime<Utc>,
}

/// Realized PnL booked when an opposite fill closes part of the pending position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NettedRecord {
    pub closed_qty: Decimal,
    pub open_px: Decimal,
    pub close_px: Decimal,
    pub realized_pnl: Decimal,
}

/// A hedge that should now be sent (the net inventory reached hedgeable size).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HedgeOrder {
    pub hedge_side: Side,
    pub qty: Decimal,
    pub avg_aster_px: Decimal,
}

#[derive(Debug, Clone)]
pub struct FillOutcome {
    /// Inventory still pending after this fill (None if flushed to a hedge or zeroed).
    pub pending: Option<PendingInventory>,
    /// Realized PnL booked from netting, if any.
    pub netted: Option<NettedRecord>,
    /// A hedge to schedule, if the net inventory became hedgeable.
    pub hedge: Option<HedgeOrder>,
}

/// Minimum hedgeable quantity on Lighter for a given reference price (the configured min
/// notional, `lighter_min_notional`, rounded up to the size step, but at least one step).
pub fn hl_min_hedge_qty(rules: &HedgeabilityRules, ref_px: Decimal) -> Decimal {
    if ref_px <= Decimal::ZERO {
        return rules.hedge_qty_step;
    }
    let by_notional = ceil_to_step(rules.hedge_min_notional / ref_px, rules.hedge_qty_step);
    by_notional.max(rules.hedge_qty_step)
}

/// Hyperliquid accepts a sub-minimum reduce-only order only for a full close (live 2026-09-28).
/// Buffer the limit price by 2%; any excess reduction is corrected on the other leg next.
pub fn reduce_only_hedge_qty(venue: HedgeVenue, qty: Decimal, position: Decimal, rules: &HedgeabilityRules, limit_px: Decimal) -> Decimal {
    if venue == HedgeVenue::Hyperliquid && qty > Decimal::ZERO {
        qty.max(hl_min_hedge_qty(rules, limit_px * Decimal::new(98, 2))).min(position.abs())
    } else { qty }
}

#[inline]
pub fn signed_aster_qty(side: Side, qty: Decimal) -> Decimal {
    match side {
        Side::Buy => qty,
        Side::Sell => -qty,
    }
}

/// Hedge side for a signed inventory: long Aster -> sell Lighter; short Aster -> buy Lighter.
pub fn hedge_side_for_signed(signed_qty: Decimal) -> Option<Side> {
    if signed_qty > Decimal::ZERO {
        Some(Side::Sell)
    } else if signed_qty < Decimal::ZERO {
        Some(Side::Buy)
    } else {
        None
    }
}

/// Fold a fill into pending inventory, returning what to book / hedge / keep. A same-direction
/// fill accumulates with a size-weighted average; an opposite fill nets down and books realized
/// PnL; the result carries a [`HedgeOrder`] the MOMENT the net clears the Lighter minimum (primary
/// fast-hedge path), else keeps the sub-min residual pending — never per-partial flattening.
#[allow(clippy::too_many_arguments)]
pub fn handle_fill_parts(
    aster_side: Side,
    fill_qty: Decimal,
    fill_px: Decimal,
    recv_ts: DateTime<Utc>,
    pending: Option<PendingInventory>,
    rules: &HedgeabilityRules,
    ref_px: Decimal,
    aster_maker_fee_rate: Decimal,
) -> FillOutcome {
    let fill_signed = signed_aster_qty(aster_side, fill_qty);

    let mut inv = pending.unwrap_or(PendingInventory {
        signed_qty: Decimal::ZERO,
        avg_aster_px: fill_px,
        first_fill_ts: recv_ts,
        last_fill_ts: recv_ts,
    });

    let mut netted = None;

    let same_dir = inv.signed_qty == Decimal::ZERO
        || (inv.signed_qty > Decimal::ZERO) == (fill_signed > Decimal::ZERO);
    if same_dir {
        // Size-weighted average accumulation.
        let old_abs = inv.signed_qty.abs();
        let new_abs = fill_signed.abs();
        let total = old_abs + new_abs;
        if total > Decimal::ZERO {
            inv.avg_aster_px = (inv.avg_aster_px * old_abs + fill_px * new_abs) / total;
        }
        inv.signed_qty += fill_signed;
    } else {
        // Opposite fill: close as much as possible, book realized PnL.
        let closed = inv.signed_qty.abs().min(fill_signed.abs());
        let gross = if inv.signed_qty > Decimal::ZERO {
            // Was long, this fill sells: profit if closing above entry.
            closed * (fill_px - inv.avg_aster_px)
        } else {
            // Was short, this fill buys: profit if closing below entry.
            closed * (inv.avg_aster_px - fill_px)
        };
        let fees = closed * inv.avg_aster_px * aster_maker_fee_rate
            + closed * fill_px * aster_maker_fee_rate;
        netted = Some(NettedRecord {
            closed_qty: closed,
            open_px: inv.avg_aster_px,
            close_px: fill_px,
            realized_pnl: gross - fees,
        });

        let new_signed = inv.signed_qty + fill_signed;
        if new_signed == Decimal::ZERO {
            inv.signed_qty = Decimal::ZERO;
        } else if (new_signed > Decimal::ZERO) == (inv.signed_qty > Decimal::ZERO) {
            // Partial close, residual on the original side: average unchanged.
            inv.signed_qty = new_signed;
        } else {
            // Flipped: residual opens fresh at this fill price.
            inv.signed_qty = new_signed;
            inv.avg_aster_px = fill_px;
        }
    }
    inv.last_fill_ts = recv_ts;

    let abs = inv.signed_qty.abs();
    if abs == Decimal::ZERO {
        return FillOutcome {
            pending: None,
            netted,
            hedge: None,
        };
    }

    let min_hedge = hl_min_hedge_qty(rules, ref_px);
    if abs >= min_hedge {
        let hedge = HedgeOrder {
            hedge_side: hedge_side_for_signed(inv.signed_qty).expect("non-zero inventory"),
            qty: abs,
            avg_aster_px: inv.avg_aster_px,
        };
        FillOutcome {
            pending: None,
            netted,
            hedge: Some(hedge),
        }
    } else {
        FillOutcome {
            pending: Some(inv),
            netted,
            hedge: None,
        }
    }
}

/// True when pending inventory has aged out or grown too large (notional at `mark_px`).
/// The caller freezes new exposure and retains the inventory until an actual execution
/// changes the position.
pub fn check_pending_limits(
    inv: &PendingInventory,
    max_pending_notional: Decimal,
    max_pending_age_ms: i64,
    mark_px: Decimal,
    now: DateTime<Utc>,
) -> bool {
    let abs = inv.signed_qty.abs();
    abs > Decimal::ZERO
        && (abs * mark_px > max_pending_notional
            || (now - inv.first_fill_ts).num_milliseconds() > max_pending_age_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn rules() -> HedgeabilityRules {
        HedgeabilityRules {
            hedge_min_notional: dec!(10),
            hedge_qty_step: dec!(0.001),
        }
    }

    #[test]
    fn min_hedge_qty_from_notional() {
        // ref 100, min $10 => 0.1 base, step 0.001.
        assert_eq!(hl_min_hedge_qty(&rules(), dec!(100)), dec!(0.1));
    }

    #[test]
    fn sub_min_accumulates_then_hedges() {
        // ref 100 => min hedge 0.1.
        let o1 = handle_fill_parts(Side::Buy, dec!(0.05), dec!(100), ts(), None, &rules(), dec!(100), dec!(0));
        assert!(o1.hedge.is_none());
        let inv = o1.pending.unwrap();
        assert_eq!(inv.signed_qty, dec!(0.05));

        let o2 = handle_fill_parts(Side::Buy, dec!(0.06), dec!(100), ts(), Some(inv), &rules(), dec!(100), dec!(0));
        let h = o2.hedge.unwrap();
        assert_eq!(h.hedge_side, Side::Sell); // long Aster => sell Lighter
        assert_eq!(h.qty, dec!(0.11));
        assert!(o2.pending.is_none());
    }

    #[test]
    fn opposite_fill_books_pnl_and_keeps_residual() {
        // Pending long 0.08 @ 100 (sub-min).
        let inv = handle_fill_parts(Side::Buy, dec!(0.08), dec!(100), ts(), None, &rules(), dec!(100), dec!(0))
            .pending
            .unwrap();
        // Opposite sell 0.05 @ 101 closes 0.05 for +0.05 gross.
        let o = handle_fill_parts(Side::Sell, dec!(0.05), dec!(101), ts(), Some(inv), &rules(), dec!(100), dec!(0));
        let n = o.netted.unwrap();
        assert_eq!(n.closed_qty, dec!(0.05));
        assert_eq!(n.realized_pnl, dec!(0.05));
        let resid = o.pending.unwrap();
        assert_eq!(resid.signed_qty, dec!(0.03)); // still long
        assert_eq!(resid.avg_aster_px, dec!(100)); // average unchanged
    }

    #[test]
    fn opposite_fill_flips_and_hedges() {
        let inv = handle_fill_parts(Side::Buy, dec!(0.08), dec!(100), ts(), None, &rules(), dec!(100), dec!(0))
            .pending
            .unwrap();
        // Big opposite sell 0.2 @ 101: closes 0.08 (+0.08), flips to short 0.12 @ 101.
        let o = handle_fill_parts(Side::Sell, dec!(0.2), dec!(101), ts(), Some(inv), &rules(), dec!(100), dec!(0));
        assert_eq!(o.netted.unwrap().realized_pnl, dec!(0.08));
        let h = o.hedge.unwrap();
        assert_eq!(h.hedge_side, Side::Buy); // short Aster => buy Lighter
        assert_eq!(h.qty, dec!(0.12));
        assert_eq!(h.avg_aster_px, dec!(101)); // fresh at flip price
    }

    #[test]
    fn pending_too_old_hits_limit() {
        let inv = PendingInventory {
            signed_qty: dec!(0.05),
            avg_aster_px: dec!(100),
            first_fill_ts: ts(),
            last_fill_ts: ts(),
        };
        let now = ts() + chrono::Duration::milliseconds(2_000);
        assert!(check_pending_limits(&inv, dec!(25), 1_000, dec!(99), now));
    }
}

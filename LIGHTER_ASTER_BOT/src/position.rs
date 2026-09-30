//! Running signed futures position per leg (Aster maker / Lighter or Hyperliquid hedge
//! leg), used to enforce the per-exchange capital cap. It carries the *same*
//! netting math as the sub-min pending-inventory layer ([`crate::inventory`]):
//! same-direction fills average in, opposite fills net the position down and book
//! realized PnL, and a fill that flips the sign opens fresh at the new price. Both
//! legs are perpetual futures, so long and short are symmetric and, at leverage 1,
//! the margin a position consumes equals its notional.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::Side;

/// Signed open position on one leg. `qty > 0` is net long, `qty < 0` is net short.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedPosition {
    pub qty: Decimal,
    /// Size-weighted average entry price of the currently-open position (0 when flat).
    pub avg_px: Decimal,
}

impl SignedPosition {
    /// A fill of `qty` on `side` as a signed delta (+ for buy, − for sell).
    #[inline]
    pub fn signed(side: Side, qty: Decimal) -> Decimal {
        match side {
            Side::Buy => qty,
            Side::Sell => -qty,
        }
    }

    /// Fold a signed fill (`signed_qty` base units, executed at `px`) into the
    /// position. Returns realized PnL booked on whatever quantity this fill closed
    /// (0 if it only opened or extended the position).
    pub fn apply_fill(&mut self, signed_qty: Decimal, px: Decimal) -> Decimal {
        if signed_qty == Decimal::ZERO {
            return Decimal::ZERO;
        }
        let same_dir =
            self.qty == Decimal::ZERO || (self.qty > Decimal::ZERO) == (signed_qty > Decimal::ZERO);
        if same_dir {
            // Open or extend: size-weighted average.
            let old_abs = self.qty.abs();
            let add_abs = signed_qty.abs();
            let total = old_abs + add_abs;
            if total > Decimal::ZERO {
                self.avg_px = (self.avg_px * old_abs + px * add_abs) / total;
            }
            self.qty += signed_qty;
            Decimal::ZERO
        } else {
            // Opposite fill: close as much as possible and book realized PnL.
            let closed = self.qty.abs().min(signed_qty.abs());
            let realized = if self.qty > Decimal::ZERO {
                // Was long, closing by selling at px.
                closed * (px - self.avg_px)
            } else {
                // Was short, closing by buying at px.
                closed * (self.avg_px - px)
            };
            let new_qty = self.qty + signed_qty;
            if new_qty == Decimal::ZERO {
                self.qty = Decimal::ZERO;
                self.avg_px = Decimal::ZERO;
            } else if (new_qty > Decimal::ZERO) == (self.qty > Decimal::ZERO) {
                // Partial close, residual stays on the original side: average unchanged.
                self.qty = new_qty;
            } else {
                // Flipped through flat: the residual opens fresh at this fill price.
                self.qty = new_qty;
                self.avg_px = px;
            }
            realized
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn open_and_extend_averages() {
        let mut p = SignedPosition::default();
        assert_eq!(p.apply_fill(dec!(1), dec!(100)), dec!(0));
        assert_eq!(p.qty, dec!(1));
        assert_eq!(p.avg_px, dec!(100));
        // Add 1 more at 102 -> avg 101.
        assert_eq!(p.apply_fill(dec!(1), dec!(102)), dec!(0));
        assert_eq!(p.qty, dec!(2));
        assert_eq!(p.avg_px, dec!(101));
    }

    #[test]
    fn partial_close_books_pnl_keeps_avg() {
        let mut p = SignedPosition::default();
        p.apply_fill(dec!(2), dec!(100)); // long 2 @ 100
        // Sell 1 @ 105 -> realized +5, residual long 1 @ 100.
        let realized = p.apply_fill(dec!(-1), dec!(105));
        assert_eq!(realized, dec!(5));
        assert_eq!(p.qty, dec!(1));
        assert_eq!(p.avg_px, dec!(100));
    }

    #[test]
    fn full_close_flattens() {
        let mut p = SignedPosition::default();
        p.apply_fill(dec!(1), dec!(100));
        let realized = p.apply_fill(dec!(-1), dec!(99)); // long closed at a loss
        assert_eq!(realized, dec!(-1));
        assert_eq!(p.qty, dec!(0));
        assert_eq!(p.avg_px, dec!(0));
    }

    #[test]
    fn flip_opens_fresh_at_new_price() {
        let mut p = SignedPosition::default();
        p.apply_fill(dec!(1), dec!(100)); // long 1 @ 100
        // Sell 3 @ 110: closes 1 (+10), flips to short 2 @ 110.
        let realized = p.apply_fill(dec!(-3), dec!(110));
        assert_eq!(realized, dec!(10));
        assert_eq!(p.qty, dec!(-2));
        assert_eq!(p.avg_px, dec!(110));
    }

    #[test]
    fn short_then_cover_signs() {
        let mut p = SignedPosition::default();
        p.apply_fill(dec!(-2), dec!(100)); // short 2 @ 100
        assert_eq!(p.qty, dec!(-2));
        // Buy 1 @ 98 to cover -> realized +2 (short profits when buying lower).
        let realized = p.apply_fill(dec!(1), dec!(98));
        assert_eq!(realized, dec!(2));
        assert_eq!(p.qty, dec!(-1));
        assert_eq!(p.avg_px, dec!(100));
    }
}

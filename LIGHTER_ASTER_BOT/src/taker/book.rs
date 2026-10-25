use arrayvec::ArrayVec;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::book::build_side;
pub use crate::book::{Level, MAX_BOOK_LEVELS};
use crate::taker::types::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthQuote {
    pub side: Side,
    pub target_qty: Decimal,
    pub available_qty: Decimal,
    pub vwap_px: Decimal,
    pub worst_px: Decimal,
    pub best_px: Decimal,
    pub best_qty: Decimal,
    pub levels_used: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DepthQuoteF64 {
    pub side: Side,
    pub target_qty: f64,
    pub available_qty: f64,
    pub vwap_px: f64,
    pub worst_px: f64,
    pub best_px: f64,
    pub best_qty: f64,
    pub levels_used: usize,
}


fn decimal_to_f64(value: Decimal) -> Option<f64> {
    let out = value.to_f64()?;
    out.is_finite().then_some(out)
}

fn level_to_f64(level: &Level) -> Option<(f64, f64)> {
    Some((decimal_to_f64(level.px)?, decimal_to_f64(level.qty)?))
}

fn f64_qty_tol(qty: f64) -> f64 {
    (qty.abs() * f64::EPSILON * 64.0).max(f64::EPSILON)
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: ArrayVec<Level, MAX_BOOK_LEVELS>,
    pub asks: ArrayVec<Level, MAX_BOOK_LEVELS>,
    pub exch_ts: DateTime<Utc>,
    pub engine_ts: Option<DateTime<Utc>>,
    pub local_recv_ts: DateTime<Utc>,
}

impl OrderBook {
    pub fn from_levels(
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> Self {
        OrderBook {
            bids: build_side(bids, true),
            asks: build_side(asks, false),
            exch_ts,
            engine_ts: None,
            local_recv_ts,
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids.first().copied()
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first().copied()
    }

    pub fn mid(&self) -> Option<Decimal> {
        Some((self.best_bid()?.px + self.best_ask()?.px) / Decimal::from(2))
    }

    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => b.px >= a.px,
            _ => false,
        }
    }

    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        (now - self.local_recv_ts).num_milliseconds()
    }

    pub fn source_age_ms(&self, now: DateTime<Utc>) -> Option<i64> {
        if self.exch_ts == DateTime::<Utc>::UNIX_EPOCH { return None; }
        let age = (now - self.exch_ts).num_milliseconds();
        (age >= -1000).then_some(age.max(0))
    }

    pub fn engine_age_ms(&self, now: DateTime<Utc>) -> Option<i64> {
        self.engine_ts.map(|timestamp| (now - timestamp).num_milliseconds().max(0))
    }

    pub fn is_fresh(&self, now: DateTime<Utc>, max_age_ms: i64) -> bool {
        let local_age = self.age_ms(now);
        local_age >= 0 && local_age <= max_age_ms
            && self.source_age_ms(now).is_some_and(|age| age <= max_age_ms)
    }

    pub fn side_levels(&self, side: Side) -> &[Level] {
        match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        }
    }

    pub fn cumulative_qty(&self, side: Side, max_levels: usize) -> Decimal {
        self.side_levels(side)
            .iter()
            .take(max_levels.min(MAX_BOOK_LEVELS))
            .fold(Decimal::ZERO, |acc, level| acc + level.qty)
    }

    pub fn depth_vwap(
        &self,
        side: Side,
        target_qty: Decimal,
        max_levels: usize,
    ) -> Option<DepthQuote> {
        if target_qty <= Decimal::ZERO || max_levels == 0 {
            return None;
        }
        let levels = self.side_levels(side);
        let best = levels.first().copied()?;
        let mut remaining = target_qty;
        let mut filled = Decimal::ZERO;
        let mut notional = Decimal::ZERO;
        let mut available = Decimal::ZERO;
        let mut worst_px = best.px;
        let mut levels_used = 0usize;
        for level in levels.iter().take(max_levels.min(MAX_BOOK_LEVELS)) {
            available += level.qty;
            if remaining > Decimal::ZERO {
                let take = level.qty.min(remaining);
                if take > Decimal::ZERO {
                    filled += take;
                    notional += take * level.px;
                    remaining -= take;
                    worst_px = level.px;
                    levels_used += 1;
                }
            }
        }
        if filled < target_qty {
            return None;
        }
        Some(DepthQuote {
            side,
            target_qty,
            available_qty: available,
            vwap_px: notional / target_qty,
            worst_px,
            best_px: best.px,
            best_qty: best.qty,
            levels_used,
        })
    }

    pub fn mid_f64(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let mid = (decimal_to_f64(bid.px)? + decimal_to_f64(ask.px)?) / 2.0;
        mid.is_finite().then_some(mid)
    }

    pub fn best_bid_f64(&self) -> Option<(f64, f64)> {
        self.bids.first().and_then(level_to_f64)
    }

    pub fn best_ask_f64(&self) -> Option<(f64, f64)> {
        self.asks.first().and_then(level_to_f64)
    }

    pub fn cumulative_qty_f64(&self, side: Side, max_levels: usize) -> Option<f64> {
        self.side_levels(side)
            .iter()
            .take(max_levels.min(MAX_BOOK_LEVELS))
            .try_fold(0.0, |acc, level| Some(acc + decimal_to_f64(level.qty)?))
    }

    pub fn depth_vwap_f64(
        &self,
        side: Side,
        target_qty: f64,
        max_levels: usize,
    ) -> Option<DepthQuoteF64> {
        if target_qty <= 0.0 || !target_qty.is_finite() || max_levels == 0 {
            return None;
        }
        let levels = self.side_levels(side);
        let best = levels.first().copied()?;
        let mut remaining = target_qty;
        let mut filled = 0.0;
        let mut notional = 0.0;
        let mut available = 0.0;
        let mut worst_px = decimal_to_f64(best.px)?;
        let best_px = worst_px;
        let best_qty = decimal_to_f64(best.qty)?;
        let mut levels_used = 0usize;
        for level in levels.iter().take(max_levels.min(MAX_BOOK_LEVELS)) {
            let px = decimal_to_f64(level.px)?;
            let qty = decimal_to_f64(level.qty)?;
            available += qty;
            if remaining > 0.0 {
                let take = qty.min(remaining);
                if take > 0.0 {
                    filled += take;
                    notional += take * px;
                    remaining -= take;
                    worst_px = px;
                    levels_used += 1;
                }
            }
        }
        if filled + f64_qty_tol(target_qty) < target_qty {
            return None;
        }
        Some(DepthQuoteF64 {
            side,
            target_qty,
            available_qty: available,
            vwap_px: notional / target_qty,
            worst_px,
            best_px,
            best_qty,
            levels_used,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn depth_vwap_consumes_partial_final_level() {
        let now = Utc::now();
        let book = OrderBook::from_levels(
            [(dec!(10.00), dec!(1)), (dec!(9.90), dec!(5))],
            [(dec!(10.10), dec!(1)), (dec!(10.20), dec!(3))],
            now,
            now,
        );
        let quote = book.depth_vwap(Side::Buy, dec!(2), 20).unwrap();
        assert_eq!(quote.best_px, dec!(10.10));
        assert_eq!(quote.best_qty, dec!(1));
        assert_eq!(quote.worst_px, dec!(10.20));
        assert_eq!(quote.vwap_px, dec!(10.15));
        assert_eq!(quote.levels_used, 2);
    }

    #[test]
    fn depth_vwap_requires_full_target_depth() {
        let now = Utc::now();
        let book = OrderBook::from_levels(
            [(dec!(10.00), dec!(1))],
            [(dec!(10.10), dec!(1))],
            now,
            now,
        );
        assert!(book.depth_vwap(Side::Sell, dec!(2), 20).is_none());
    }

    #[test]
    fn depth_vwap_f64_accepts_decimal_depth_sum_noise() {
        let now = Utc::now();
        let asks = (0..10).map(|i| (dec!(10.00) + Decimal::from(i) / Decimal::from(100), dec!(0.1)));
        let book = OrderBook::from_levels([(dec!(9.90), dec!(1))], asks, now, now);
        let quote = book.depth_vwap_f64(Side::Buy, 1.0, 20).unwrap();
        assert_eq!(quote.levels_used, 10);
    }
    #[test]
    fn emitter_freshness_is_independent_of_quiet_engine_and_receive_time() {
        let now = Utc::now();
        let mut book = OrderBook::from_levels([(dec!(10),dec!(1))],[(dec!(11),dec!(1))], now, now);
        book.engine_ts = Some(now - chrono::Duration::hours(1));
        assert!(book.is_fresh(now, 2000));
        book.exch_ts = now - chrono::Duration::milliseconds(2001);
        assert!(!book.is_fresh(now, 2000));
        book.exch_ts = now + chrono::Duration::milliseconds(1000);
        assert!(book.is_fresh(now, 2000));
        book.exch_ts = now + chrono::Duration::milliseconds(1001);
        assert!(!book.is_fresh(now, 2000));
        book.exch_ts = DateTime::<Utc>::UNIX_EPOCH;
        assert!(!book.is_fresh(now, 2000));
    }

}

//! In-memory order book built from partial-depth levels (Aster pushes whole snapshots;
//! Lighter deltas are merged in `lighter::local_book` first, so this type keeps no
//! diff/sequence state). Bids are sorted descending, asks ascending; zero-qty levels
//! are dropped on build.

use arrayvec::ArrayVec;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

pub const MAX_BOOK_LEVELS: usize = 20;

/// One raw `(px, qty)` depth row as parsed off the wire.
pub type PriceLevel = (Decimal, Decimal);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub px: Decimal,
    pub qty: Decimal,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: ArrayVec<Level, MAX_BOOK_LEVELS>, // descending by px
    pub asks: ArrayVec<Level, MAX_BOOK_LEVELS>, // ascending by px
    pub exch_ts: DateTime<Utc>,
    pub local_recv_ts: DateTime<Utc>,
}

impl OrderBook {
    /// Normalize snapshot rows: aggregate duplicate prices, drop non-positive prices/quantities,
    /// sort each side and retain the best [`MAX_BOOK_LEVELS`]. Lighter may supply a larger book.
    pub fn from_levels(
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> Self {
        // Keep the best levels in fixed storage without a temporary Vec.
        let bids = build_side(bids, true);
        let asks = build_side(asks, false);

        OrderBook { bids, asks, exch_ts, local_recv_ts }
    }

    #[inline]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.first().copied()
    }

    #[inline]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first().copied()
    }

    #[inline]
    pub fn mid(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b.px + a.px) / Decimal::from(2)),
            _ => None,
        }
    }

    /// True if the top of book is crossed or locked (bid >= ask).
    #[inline]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => b.px >= a.px,
            _ => false,
        }
    }

    /// Age in milliseconds of this book relative to `now` (by local receive time).
    #[inline]
    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        let source_age = crate::hot_types::source_age_at_receive_ms(
            self.exch_ts.timestamp_millis(), self.local_recv_ts.timestamp_millis());
        (now - self.local_recv_ts).num_milliseconds().max(0).saturating_add(source_age)
    }
}

#[inline]
fn build_side(
    levels: impl IntoIterator<Item = (Decimal, Decimal)>,
    descending: bool,
) -> ArrayVec<Level, MAX_BOOK_LEVELS> {
    let mut out = ArrayVec::<Level, MAX_BOOK_LEVELS>::new();
    for (px, qty) in levels {
        if px <= Decimal::ZERO || qty <= Decimal::ZERO {
            continue;
        }
        upsert_sorted(&mut out, Level { px, qty }, descending);
    }
    out
}

#[inline]
fn upsert_sorted(out: &mut ArrayVec<Level, MAX_BOOK_LEVELS>, level: Level, descending: bool) {
    // `out` is maintained in canonical book order, so one binary search both finds an
    // existing duplicate price (aggregate quantity) and the insertion point for a new level.
    let pos = match out.binary_search_by(|cur| {
        if descending {
            cur.px.cmp(&level.px).reverse()
        } else {
            cur.px.cmp(&level.px)
        }
    }) {
        Ok(idx) => {
            out[idx].qty += level.qty;
            return;
        }
        Err(idx) => idx,
    };

    if out.len() < MAX_BOOK_LEVELS {
        out.insert(pos, level);
    } else if pos < MAX_BOOK_LEVELS {
        // Keep only the best MAX_BOOK_LEVELS. Drop the current worst before inserting.
        let _ = out.pop();
        out.insert(pos, level);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn sample() -> OrderBook {
        OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(99.9), dec!(5)), (dec!(99.8), dec!(7))],
            vec![(dec!(100.1), dec!(3)), (dec!(100.2), dec!(4)), (dec!(100.3), dec!(0))],
            ts(),
            ts(),
        )
    }

    #[test]
    fn sorting_and_zero_drop() {
        let b = sample();
        assert_eq!(b.best_bid().unwrap().px, dec!(100.0));
        assert_eq!(b.best_ask().unwrap().px, dec!(100.1));
        assert_eq!(b.asks.len(), 2); // zero-qty ask dropped
        assert_eq!(b.mid().unwrap(), dec!(100.05));
        assert!(!b.is_crossed());
    }

    #[test]
    fn nonpositive_price_dropped() {
        let b = OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(0), dec!(5)), (dec!(-1), dec!(9))],
            vec![(dec!(100.1), dec!(3)), (dec!(0), dec!(4))],
            ts(),
            ts(),
        );
        assert_eq!(b.bids.len(), 1);
        assert_eq!(b.asks.len(), 1);
        assert_eq!(b.best_bid().unwrap().px, dec!(100.0));
        assert_eq!(b.best_ask().unwrap().px, dec!(100.1));
        assert_eq!(b.mid().unwrap(), dec!(100.05));

        let all_bad = OrderBook::from_levels(
            vec![(dec!(0), dec!(5))],
            vec![(dec!(100.1), dec!(3))],
            ts(),
            ts(),
        );
        assert!(all_bad.best_bid().is_none());
        assert!(all_bad.mid().is_none());
    }


    #[test]
    fn duplicate_price_levels_are_aggregated() {
        let b = OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(100.0), dec!(3)), (dec!(99.9), dec!(1))],
            vec![(dec!(100.1), dec!(4)), (dec!(100.1), dec!(6))],
            ts(),
            ts(),
        );
        assert_eq!(b.bids.len(), 2);
        assert_eq!(b.asks.len(), 1);
        assert_eq!(b.best_bid().unwrap().qty, dec!(5));
        assert_eq!(b.best_ask().unwrap().qty, dec!(10));
    }

    #[test]
    fn crossed_detection() {
        let b = OrderBook::from_levels(
            vec![(dec!(101), dec!(1))],
            vec![(dec!(100), dec!(1))],
            ts(),
            ts(),
        );
        assert!(b.is_crossed());
    }

    #[test]
    fn truncates_to_max_levels() {
        let many_bids: Vec<(Decimal, Decimal)> = (0..30)
            .map(|i| (dec!(100) - Decimal::from(i) * dec!(0.1), dec!(1)))
            .collect();
        let b = OrderBook::from_levels(many_bids, vec![(dec!(101), dec!(1))], ts(), ts());
        assert_eq!(b.bids.len(), MAX_BOOK_LEVELS);
        assert_eq!(b.best_bid().unwrap().px, dec!(100.0));
    }

    #[test]
    fn truncation_keeps_best_levels_from_unsorted_input() {
        let mut bids: Vec<(Decimal, Decimal)> = (0..30)
            .map(|i| (dec!(90) + Decimal::from(i) * dec!(0.1), dec!(1)))
            .collect();
        bids.reverse();
        bids.push((dec!(105), dec!(1))); // best level arrives after the first 20 rows
        let b = OrderBook::from_levels(bids, vec![(dec!(106), dec!(1))], ts(), ts());
        assert_eq!(b.bids.len(), MAX_BOOK_LEVELS);
        assert_eq!(b.best_bid().unwrap().px, dec!(105));
        assert!(b.bids.iter().all(|l| l.px >= dec!(91.0)));
    }
}

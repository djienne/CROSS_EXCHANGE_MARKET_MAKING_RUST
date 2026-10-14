//! The replica: one market's visible book as the simulated venue holds it (the real feed,
//! shifted by D), plus the liquidity our own simulated trades took out of it.
//!
//! Our trades never reach the real market, so the feed keeps showing what we took. Each level
//! remembers what we consumed from it, cut to the level's size whenever the feed shows it
//! smaller (what others took counts against ours), and forgets it once the level is gone.
//! Nothing refills it: no market maker reacts to our trades (pessimistic, and the only model of
//! our own impact).

use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::lighter::local_book::{BookSide, LocalBook};
use crate::types::Side;

/// `(price, size)`.
pub type Level = (Decimal, Decimal);

#[derive(Debug, Clone, PartialEq)]
pub enum BookUpdate {
    /// The whole visible book: an Aster depth20 snapshot, or a Lighter snapshot.
    Replace { bids: Vec<Level>, asks: Vec<Level> },
    /// Absolute sizes at the given prices; 0 removes the level (Lighter).
    Delta { bids: Vec<Level>, asks: Vec<Level> },
    /// Best levels only (Aster bookTicker): overlaid on the last snapshot, and every level
    /// better than them is gone.
    Top { bid: Level, ask: Level },
}

#[derive(Debug, Clone, Default)]
pub struct Replica {
    book: LocalBook,
    /// Levels per side the feed shows (Aster depth20); `None` for a full book (Lighter).
    depth: Option<usize>,
    /// Worst price each side of the last snapshot showed, when the depth cut it: past it, the
    /// size is unknown rather than zero.
    bid_floor: Option<Decimal>,
    ask_ceiling: Option<Decimal>,
    replaced_us: i64,
    top: Option<(i64, Level, Level)>,
    /// What our trades took, per side (`[bids, asks]`) and price.
    consumed: [BTreeMap<Decimal, Decimal>; 2],
}

fn slot(side: Side) -> usize {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

impl Replica {
    pub fn new(depth: Option<usize>) -> Self {
        Self { depth, ..Default::default() }
    }

    pub fn warm(&self) -> bool {
        self.book.initialized
    }

    pub fn mid(&self) -> Option<Decimal> {
        self.book.mid()
    }

    /// The side `side` orders rest on (bids for buys).
    fn book_side(&self, side: Side) -> &BookSide {
        match side {
            Side::Buy => &self.book.bids,
            Side::Sell => &self.book.asks,
        }
    }

    /// Levels resting on `side`, best first.
    pub fn levels(&self, side: Side) -> Box<dyn Iterator<Item = Level> + '_> {
        match side {
            Side::Buy => Box::new(self.book.bids.top_descending(usize::MAX)),
            Side::Sell => Box::new(self.book.asks.top_ascending(usize::MAX)),
        }
    }

    pub fn best(&self, side: Side) -> Option<Level> {
        self.levels(side).next()
    }

    /// Visible size at `price` on `side`; `None` where the feed's depth cut hides it.
    pub fn visible(&self, side: Side, price: Decimal) -> Option<Decimal> {
        let cut = match side {
            Side::Buy => self.bid_floor.is_some_and(|floor| price < floor),
            Side::Sell => self.ask_ceiling.is_some_and(|ceiling| price > ceiling),
        };
        (!cut).then(|| self.book_side(side).size_at(price))
    }

    fn consumed_at(&self, side: Side, price: Decimal) -> Decimal {
        self.consumed[slot(side)].get(&price).copied().unwrap_or_default()
    }

    pub fn consume(&mut self, side: Side, price: Decimal, qty: Decimal) {
        *self.consumed[slot(side)].entry(price).or_default() += qty;
    }

    /// Upstream gap: unusable until the next snapshot. What we consumed stays, and the next
    /// snapshot clamps it.
    pub fn invalidate(&mut self) {
        self.book.reset();
        self.top = None;
    }

    /// Applies one feed update in exchange-time order. Returns false for a stale one: a
    /// snapshot or top older than the state it would overwrite (streams lag differently).
    pub fn apply(&mut self, exch_us: i64, update: &BookUpdate) -> bool {
        match update {
            BookUpdate::Replace { bids, asks } => {
                if self.book.initialized && exch_us < self.replaced_us {
                    return false;
                }
                let cut = |n: usize| self.depth.is_some_and(|depth| n >= depth);
                self.bid_floor = if cut(bids.len()) { bids.iter().map(|l| l.0).min() } else { None };
                self.ask_ceiling = if cut(asks.len()) { asks.iter().map(|l| l.0).max() } else { None };
                self.book.apply_snapshot(bids.clone(), asks.clone());
                self.replaced_us = exch_us;
                if let Some((at, bid, ask)) = self.top {
                    if at > exch_us {
                        self.overlay(bid, ask);
                    }
                }
            }
            BookUpdate::Delta { bids, asks } => self.book.apply_delta(bids, asks),
            BookUpdate::Top { bid, ask } => {
                if exch_us < self.replaced_us || self.top.is_some_and(|(at, ..)| exch_us < at) {
                    return false;
                }
                self.top = Some((exch_us, *bid, *ask));
                if self.book.initialized {
                    self.overlay(*bid, *ask);
                }
            }
        }
        for (side, taken) in [&self.book.bids, &self.book.asks].into_iter().zip(&mut self.consumed) {
            taken.retain(|price, qty| {
                *qty = (*qty).min(side.size_at(*price));
                !qty.is_zero()
            });
        }
        true
    }

    fn overlay(&mut self, bid: Level, ask: Level) {
        if bid.0 >= ask.0 {
            return;
        }
        self.book.bids.remove_above(bid.0);
        self.book.bids.upsert(bid.0, bid.1);
        self.book.asks.remove_below(ask.0);
        self.book.asks.upsert(ask.0, ask.1);
    }

    /// Liquidity a `taker` order can take, best first, up to `limit`: the worse of this state
    /// and `next` (the state after the next update, when the bracket has one; matching must
    /// assume the worse of the two), less what we already took.
    pub fn takeable(&self, next: Option<&Replica>, taker: Side, limit: Option<Decimal>) -> Vec<Level> {
        let side = taker.opposite();
        let within = |price: Decimal| {
            limit.is_none_or(|limit| match taker {
                Side::Buy => price <= limit,
                Side::Sell => price >= limit,
            })
        };
        let shown = |replica: &Replica| -> Vec<Level> { replica.levels(side).take_while(|&(price, _)| within(price)).collect() };
        let levels = match next {
            Some(next) => worse(&shown(self), &shown(next), taker),
            None => shown(self),
        };
        levels.into_iter()
            .filter_map(|(price, size)| {
                let free = size - self.consumed_at(side, price);
                (free > Decimal::ZERO).then_some((price, free))
            })
            .collect()
    }

    /// Would a `side` order at `price` take liquidity here (cross the best level we have not
    /// already emptied)?
    pub fn crosses(&self, side: Side, price: Decimal) -> bool {
        !self.takeable(None, side, Some(price)).is_empty()
    }
}

/// The worse of two states for a `taker` at every quantity, as levels best first: the lower
/// envelope of their depth curves (the size at prices at least as good). A level-by-level
/// minimum would be worse than both whenever liquidity moved between prices, and empty once
/// the market moved through every level shown (Hyperliquid's 5 levels within one ~0.5 s
/// snapshot: IOC hedges refused "no match" with the tape well inside their bound, 2026-10-12).
fn worse(a: &[Level], b: &[Level], taker: Side) -> Vec<Level> {
    let before = |x: Decimal, y: Decimal| match taker {
        Side::Buy => x < y,
        Side::Sell => x > y,
    };
    let (mut i, mut j) = (0, 0);
    let (mut depth_a, mut depth_b, mut listed) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    let mut levels = Vec::new();
    loop {
        let price = match (a.get(i), b.get(j)) {
            (Some(x), Some(y)) if before(y.0, x.0) => y.0,
            (Some(x), _) => x.0,
            (None, Some(y)) => y.0,
            (None, None) => return levels,
        };
        if a.get(i).is_some_and(|l| l.0 == price) {
            depth_a += a[i].1;
            i += 1;
        }
        if b.get(j).is_some_and(|l| l.0 == price) {
            depth_b += b[j].1;
            j += 1;
        }
        let depth = depth_a.min(depth_b);
        if depth > listed {
            levels.push((price, depth - listed));
            listed = depth;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn levels(v: &[(i64, i64)]) -> Vec<Level> {
        v.iter().map(|&(p, q)| (Decimal::from(p), Decimal::from(q))).collect()
    }

    fn aster() -> Replica {
        let mut r = Replica::new(Some(3));
        let update = BookUpdate::Replace { bids: levels(&[(99, 5), (98, 5), (97, 5)]), asks: levels(&[(101, 5), (102, 5), (103, 5)]) };
        assert!(r.apply(1_000, &update));
        r
    }

    #[test]
    fn top_overlay_removes_better_levels_and_survives_an_older_snapshot() {
        let mut r = aster();
        // A sweep took 101 and 102: bookTicker shows 103 as the best ask.
        assert!(r.apply(2_000, &BookUpdate::Top { bid: (dec!(99), dec!(4)), ask: (dec!(103), dec!(2)) }));
        assert_eq!(r.best(Side::Sell), Some((dec!(103), dec!(2))));
        assert_eq!(r.best(Side::Buy), Some((dec!(99), dec!(4))));
        // A snapshot taken before that top is re-overlaid; one older than the last snapshot drops.
        let older = BookUpdate::Replace { bids: levels(&[(99, 5)]), asks: levels(&[(101, 5), (103, 5)]) };
        assert!(r.apply(1_500, &older));
        assert_eq!(r.best(Side::Sell), Some((dec!(103), dec!(2))));
        assert!(!r.apply(1_200, &older));
        assert!(!r.apply(1_900, &BookUpdate::Top { bid: (dec!(99), dec!(1)), ask: (dec!(101), dec!(1)) }));
    }

    #[test]
    fn depth_cut_hides_sizes_past_the_last_visible_level() {
        let r = aster();
        assert_eq!(r.visible(Side::Buy, dec!(97)), Some(dec!(5)));
        assert_eq!(r.visible(Side::Buy, dec!(97.5)), Some(dec!(0)));
        assert_eq!(r.visible(Side::Buy, dec!(96)), None);
        assert_eq!(r.visible(Side::Sell, dec!(104)), None);
        let mut full = Replica::new(None);
        full.apply(1_000, &BookUpdate::Replace { bids: levels(&[(99, 5)]), asks: levels(&[(101, 5)]) });
        assert_eq!(full.visible(Side::Buy, dec!(50)), Some(dec!(0)));
    }

    #[test]
    fn consumed_liquidity_persists_until_the_level_shrinks_or_vanishes() {
        let mut r = aster();
        r.consume(Side::Sell, dec!(101), dec!(4));
        assert_eq!(r.takeable(None, Side::Buy, Some(dec!(101))), vec![(dec!(101), dec!(1))]);
        // Someone else took 3 of the real 5: 2 left for the feed, none for us.
        r.apply(2_000, &BookUpdate::Delta { bids: vec![], asks: levels(&[(101, 2)]) });
        assert!(r.takeable(None, Side::Buy, Some(dec!(101))).is_empty());
        // New size joins behind: it is ours to take. A vanished level forgets us entirely.
        r.apply(3_000, &BookUpdate::Delta { bids: vec![], asks: levels(&[(101, 6)]) });
        assert_eq!(r.takeable(None, Side::Buy, Some(dec!(101))), vec![(dec!(101), dec!(4))]);
        r.apply(4_000, &BookUpdate::Delta { bids: vec![], asks: levels(&[(101, 0)]) });
        r.apply(5_000, &BookUpdate::Delta { bids: vec![], asks: levels(&[(101, 3)]) });
        assert_eq!(r.takeable(None, Side::Buy, Some(dec!(101))), vec![(dec!(101), dec!(3))]);
    }

    #[test]
    fn takers_get_the_worse_of_the_bracketing_states() {
        let cur = aster();
        let mut next = cur.clone();
        next.apply(2_000, &BookUpdate::Delta { bids: vec![], asks: levels(&[(101, 0), (102, 9)]) });
        // Up to 102: 10 now (5 of them at 101), 9 next, all at 102. The worse is next's.
        let got = cur.takeable(Some(&next), Side::Buy, Some(dec!(102)));
        assert_eq!(got, vec![(dec!(102), dec!(9))]);
        // Sharing a price with neither, the market moved through every level shown: the
        // taker still gets the next state's, never the empty overlap.
        let mut moved = cur.clone();
        moved.apply(3_000, &BookUpdate::Replace { bids: levels(&[(96, 4), (95, 6)]), asks: levels(&[(104, 2)]) });
        assert_eq!(cur.takeable(Some(&moved), Side::Sell, Some(dec!(95))), levels(&[(96, 4), (95, 6)]));
        assert_eq!(cur.takeable(Some(&moved), Side::Buy, Some(dec!(104))), levels(&[(104, 2)]));
        assert!(!cur.crosses(Side::Buy, dec!(100)));
        assert!(cur.crosses(Side::Buy, dec!(101)) && !next.crosses(Side::Buy, dec!(101)));
        assert!(cur.crosses(Side::Sell, dec!(99)));
    }
}

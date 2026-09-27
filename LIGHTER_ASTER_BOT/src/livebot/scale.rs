//! Scaled-integer market scale + the live hot-path order book builder.
//!
//! The deterministic cold path uses `rust_decimal::Decimal` everywhere (exact, but
//! heap-ish and slow). The live quote loop instead works in **scaled integers**:
//! prices as integer multiples of the venue tick (`px_ticks`) and quantities as
//! integer multiples of the venue lot/step (`qty_lots`). Comparisons, requote-threshold
//! checks, and crossed/touch tests are then branch-light `i64` math with no allocation.
//!
//! `Decimal` is kept for config, edge/PnL math, and cold reconciliation — we convert at
//! the boundary when building a [`MarketScale`] from a [`MarketSpec`] and when emitting an
//! order. We deliberately do NOT reimplement the edge/VWAP stack in integer math: that is
//! the exact, well-tested money math, and re-deriving it in `i64` for a few microseconds
//! would be a real-funds correctness hazard. The integers carry the *hot, hot* part
//! (touch/crossed/staleness/price-move detection + order representation); the proven
//! `Decimal` quote engine prices the actual quote.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::book::OrderBook;
use crate::markets::MarketSpec;

// The hot book types this module builds, re-exported beside the builders.
pub use crate::hot_types::{HotBook, HotLevel, HOT_LEVELS};

/// Per-market conversion between exact `Decimal` prices/quantities and the scaled `i64`
/// ticks/lots the hot path uses. Built once at startup from the resolved [`MarketSpec`].
#[derive(Debug, Clone)]
pub struct MarketScale {
    pub tick: Decimal,
    pub step: Decimal,
    /// Lighter hedge-leg size step (10^-size_decimals) — hedges round to this.
    pub hl_qty_step: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotQtyScale {
    Aster,
    Hedge,
}

impl MarketScale {
    pub fn from_spec(spec: &MarketSpec) -> Self {
        MarketScale {
            tick: spec.tick,
            step: spec.step,
            hl_qty_step: spec.hl_qty_step,
        }
    }

    /// Quantize a price to an integer number of ticks (nearest tick). Saturates to 0 on a
    /// non-representable / non-positive input (a garbage price never becomes a live order).
    #[inline]
    pub fn price_to_ticks(&self, px: Decimal) -> i64 {
        if self.tick <= Decimal::ZERO || px <= Decimal::ZERO {
            return 0;
        }
        (px / self.tick).round().to_i64().unwrap_or(0)
    }

    /// Round a price DOWN to a whole tick (post-only bid pricing).
    #[inline]
    pub fn price_floor_ticks(&self, px: Decimal) -> i64 {
        if self.tick <= Decimal::ZERO || px <= Decimal::ZERO {
            return 0;
        }
        (px / self.tick).floor().to_i64().unwrap_or(0)
    }

    /// Round a price UP to a whole tick (post-only ask pricing).
    #[inline]
    pub fn price_ceil_ticks(&self, px: Decimal) -> i64 {
        if self.tick <= Decimal::ZERO || px <= Decimal::ZERO {
            return 0;
        }
        (px / self.tick).ceil().to_i64().unwrap_or(0)
    }

    /// Exact `Decimal` price for a tick count.
    #[inline]
    pub fn ticks_to_price(&self, ticks: i64) -> Decimal {
        Decimal::from(ticks) * self.tick
    }

    /// Quantize a quantity DOWN to whole lots (you never round a size up into more risk).
    #[inline]
    pub fn qty_to_lots(&self, qty: Decimal) -> i64 {
        if self.step <= Decimal::ZERO || qty <= Decimal::ZERO {
            return 0;
        }
        (qty / self.step).floor().to_i64().unwrap_or(0)
    }

    /// Quantize a Lighter quantity DOWN to whole Lighter lots.
    #[inline]
    pub fn hl_qty_to_lots(&self, qty: Decimal) -> i64 {
        if self.hl_qty_step <= Decimal::ZERO || qty <= Decimal::ZERO {
            return 0;
        }
        (qty / self.hl_qty_step).floor().to_i64().unwrap_or(0)
    }

    /// Quantize a Lighter quantity UP to whole Lighter lots. Used for minimum
    /// visible-depth requirements so the hot path never under-requires liquidity.
    #[inline]
    pub fn hl_qty_to_lots_ceil(&self, qty: Decimal) -> i64 {
        if self.hl_qty_step <= Decimal::ZERO || qty <= Decimal::ZERO {
            return 0;
        }
        (qty / self.hl_qty_step).ceil().to_i64().unwrap_or(0)
    }

    /// Parse an exchange decimal price string directly into integer ticks, without
    /// constructing a `Decimal` on the websocket hot path. Prices are rounded to the
    /// nearest tick to mirror [`price_to_ticks`].
    #[inline]
    pub fn price_str_to_ticks(&self, px: &str) -> Option<i64> {
        decimal_str_to_units(px, self.tick, UnitRound::Nearest)
    }

    /// Parse an exchange decimal quantity string directly into integer lots, without
    /// constructing a `Decimal` on the websocket hot path. Quantities are rounded down
    /// to mirror [`qty_to_lots`] and avoid overstating visible size.
    #[inline]
    pub fn qty_str_to_lots(&self, qty: &str) -> Option<i64> {
        decimal_str_to_units(qty, self.step, UnitRound::Floor)
    }

    /// Parse a Lighter quantity string directly into Lighter lots.
    #[inline]
    pub fn hl_qty_str_to_lots(&self, qty: &str) -> Option<i64> {
        decimal_str_to_units(qty, self.hl_qty_step, UnitRound::Floor)
    }

    /// Exact `Decimal` quantity for a lot count.
    #[inline]
    pub fn lots_to_qty(&self, lots: i64) -> Decimal {
        Decimal::from(lots) * self.step
    }

    /// Exact ticks for an already-rounded Decimal price (nearest tick) — the numeric
    /// twin of [`price_str_to_ticks`]: it feeds the Decimal's own (mantissa, scale)
    /// into the SAME i128 rational core, so ticks are bit-identical to the string
    /// path. Deliberately NOT [`price_to_ticks`], whose Decimal division can round
    /// differently in >28-digit quotients.
    #[inline]
    pub fn price_dec_to_ticks(&self, px: Decimal) -> Option<i64> {
        dec_units(px, self.tick, UnitRound::Nearest)
    }

    /// Numeric twin of [`qty_str_to_lots`] (floor). See [`price_dec_to_ticks`].
    #[inline]
    pub fn qty_dec_to_lots(&self, qty: Decimal) -> Option<i64> {
        dec_units(qty, self.step, UnitRound::Floor)
    }

    /// Numeric twin of [`hl_qty_str_to_lots`] (floor). See [`price_dec_to_ticks`].
    #[inline]
    pub fn hl_qty_dec_to_lots(&self, qty: Decimal) -> Option<i64> {
        dec_units(qty, self.hl_qty_step, UnitRound::Floor)
    }
}

#[derive(Debug, Clone, Copy)]
enum UnitRound {
    Floor,
    Nearest,
}

#[inline]
fn decimal_str_to_units(s: &str, unit: Decimal, round: UnitRound) -> Option<i64> {
    let (mant, scale) = parse_positive_decimal(s)?;
    units_from_parts(mant, scale, unit, round)
}

/// A positive Decimal's (mantissa, scale) through the same exact i128 rational core
/// as the string path — the two entry points must stay bit-identical.
#[inline]
fn dec_units(v: Decimal, unit: Decimal, round: UnitRound) -> Option<i64> {
    if v.is_sign_negative() || v.is_zero() {
        return None;
    }
    units_from_parts(v.mantissa(), v.scale(), unit, round)
}

#[inline]
fn units_from_parts(mant: i128, scale: u32, unit: Decimal, round: UnitRound) -> Option<i64> {
    if unit <= Decimal::ZERO || mant <= 0 {
        return None;
    }
    let unit_mant = unit.mantissa().abs();
    if unit_mant <= 0 {
        return None;
    }
    let numerator = mant.checked_mul(pow10(unit.scale())?)?;
    let denominator = unit_mant.checked_mul(pow10(scale)?)?;
    if denominator <= 0 {
        return None;
    }
    let units = match round {
        UnitRound::Floor => numerator / denominator,
        UnitRound::Nearest => (numerator + denominator / 2) / denominator,
    };
    i64::try_from(units).ok().filter(|v| *v > 0)
}

fn parse_positive_decimal(s: &str) -> Option<(i128, u32)> {
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return None;
    }
    let s = s.strip_prefix('+').unwrap_or(s);
    let mut mant = 0i128;
    let mut scale = 0u32;
    let mut saw_digit = false;
    let mut saw_dot = false;
    for b in s.bytes() {
        match b {
            b'0'..=b'9' => {
                saw_digit = true;
                mant = mant.checked_mul(10)?.checked_add((b - b'0') as i128)?;
                if saw_dot {
                    scale = scale.checked_add(1)?;
                }
            }
            b'.' if !saw_dot => saw_dot = true,
            _ => return None,
        }
    }
    if !saw_digit || mant <= 0 {
        return None;
    }
    Some((mant, scale))
}

#[inline]
fn pow10(exp: u32) -> Option<i128> {
    let mut v = 1i128;
    for _ in 0..exp {
        v = v.checked_mul(10)?;
    }
    Some(v)
}

/// Convert a `Decimal` [`OrderBook`] into a scaled-integer [`HotBook`], truncating to
/// [`HOT_LEVELS`] and dropping any level whose scaled price or qty rounds to <= 0.
#[cfg(test)]
pub fn build_hot_book(book: &OrderBook, scale: &MarketScale, recv_ns: i64) -> HotBook {
    build_hot_book_with_qty_scale(book, scale, HotQtyScale::Aster, recv_ns)
}

pub fn build_hot_book_with_qty_scale(
    book: &OrderBook,
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    recv_ns: i64,
) -> HotBook {
    let mut bids = [HotLevel::default(); HOT_LEVELS];
    let mut asks = [HotLevel::default(); HOT_LEVELS];
    let bid_len = fill(&mut bids, &book.bids, scale, qty_scale);
    let ask_len = fill(&mut asks, &book.asks, scale, qty_scale);
    HotBook::new(
        bids,
        asks,
        bid_len,
        ask_len,
        recv_ns,
        book.exch_ts.timestamp_millis(),
    )
}

/// Build a [`HotBook`] directly from exchange decimal strings. This is the websocket
/// hot-path builder: it avoids `rust_decimal::Decimal` allocation/construction for the
/// integer precheck representation while preserving canonical ordering, duplicate-price
/// aggregation, non-positive filtering, and [`HOT_LEVELS`] truncation.
pub fn build_hot_book_from_strs_with_qty_scale<'a, I, J>(
    bids_in: I,
    asks_in: J,
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    recv_ns: i64,
    exch_ms: i64,
) -> HotBook
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
    J: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut bids = [HotLevel::default(); HOT_LEVELS];
    let mut asks = [HotLevel::default(); HOT_LEVELS];
    let bid_len = fill_raw(&mut bids, bids_in, scale, qty_scale, true);
    let ask_len = fill_raw(&mut asks, asks_in, scale, qty_scale, false);
    HotBook::new(bids, asks, bid_len, ask_len, recv_ns, exch_ms)
}

/// Build a [`HotBook`] from already-rounded Decimal levels (best-first), mirroring
/// [`build_hot_book_from_strs_with_qty_scale`] without the string round-trip: same
/// duplicate-price aggregation, non-positive filtering, and [`HOT_LEVELS`] truncation
/// via `upsert_hot_sorted`, and bit-identical ticks/lots via the shared i128 unit core.
pub fn build_hot_book_from_dec_levels_with_qty_scale(
    bids_in: &[(Decimal, Decimal)],
    asks_in: &[(Decimal, Decimal)],
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    recv_ns: i64,
    exch_ms: i64,
) -> HotBook {
    let mut bids = [HotLevel::default(); HOT_LEVELS];
    let mut asks = [HotLevel::default(); HOT_LEVELS];
    let bid_len = fill_dec(&mut bids, bids_in, scale, qty_scale, true);
    let ask_len = fill_dec(&mut asks, asks_in, scale, qty_scale, false);
    HotBook::new(bids, asks, bid_len, ask_len, recv_ns, exch_ms)
}

/// Fill a fixed level array from `Decimal` levels (already canonically sorted by
/// `OrderBook::from_levels`). Returns the count written.
fn fill(out: &mut [HotLevel; HOT_LEVELS], levels: &[crate::book::Level], scale: &MarketScale, qty_scale: HotQtyScale) -> u8 {
    let mut n = 0usize;
    for lvl in levels {
        if n >= HOT_LEVELS {
            break;
        }
        let px_ticks = scale.price_to_ticks(lvl.px);
        let qty_lots = match qty_scale {
            HotQtyScale::Aster => scale.qty_to_lots(lvl.qty),
            HotQtyScale::Hedge => scale.hl_qty_to_lots(lvl.qty),
        };
        if px_ticks <= 0 || qty_lots <= 0 {
            continue;
        }
        out[n] = HotLevel { px_ticks, qty_lots };
        n += 1;
    }
    n as u8
}

fn fill_raw<'a, I>(
    out: &mut [HotLevel; HOT_LEVELS],
    levels: I,
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    descending: bool,
) -> u8
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut n = 0usize;
    for (px_s, qty_s) in levels {
        let Some(px_ticks) = scale.price_str_to_ticks(px_s) else { continue };
        let qty_lots = match qty_scale {
            HotQtyScale::Aster => scale.qty_str_to_lots(qty_s),
            HotQtyScale::Hedge => scale.hl_qty_str_to_lots(qty_s),
        };
        let Some(qty_lots) = qty_lots else { continue };
        if px_ticks <= 0 || qty_lots <= 0 {
            continue;
        }
        upsert_hot_sorted(out, &mut n, HotLevel { px_ticks, qty_lots }, descending);
    }
    n as u8
}

fn fill_dec(
    out: &mut [HotLevel; HOT_LEVELS],
    levels: &[(Decimal, Decimal)],
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    descending: bool,
) -> u8 {
    let mut n = 0usize;
    for &(px, qty) in levels {
        let Some(px_ticks) = scale.price_dec_to_ticks(px) else { continue };
        let qty_lots = match qty_scale {
            HotQtyScale::Aster => scale.qty_dec_to_lots(qty),
            HotQtyScale::Hedge => scale.hl_qty_dec_to_lots(qty),
        };
        let Some(qty_lots) = qty_lots else { continue };
        if px_ticks <= 0 || qty_lots <= 0 {
            continue;
        }
        upsert_hot_sorted(out, &mut n, HotLevel { px_ticks, qty_lots }, descending);
    }
    n as u8
}

fn upsert_hot_sorted(out: &mut [HotLevel; HOT_LEVELS], len: &mut usize, level: HotLevel, descending: bool) {
    for cur in out.iter_mut().take(*len) {
        if cur.px_ticks == level.px_ticks {
            cur.qty_lots = cur.qty_lots.saturating_add(level.qty_lots);
            return;
        }
    }

    let pos = (0..*len)
        .find(|&i| if descending { level.px_ticks > out[i].px_ticks } else { level.px_ticks < out[i].px_ticks })
        .unwrap_or(*len);

    if *len < HOT_LEVELS {
        for i in (pos..*len).rev() {
            out[i + 1] = out[i];
        }
        out[pos] = level;
        *len += 1;
    } else if pos < HOT_LEVELS {
        for i in (pos..HOT_LEVELS - 1).rev() {
            out[i + 1] = out[i];
        }
        out[pos] = level;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    pub(crate) fn spec() -> MarketSpec {
        MarketSpec {
            market_id: "BTC".into(),
            aster_symbol: "BTCUSDT".into(),
            hl_coin: "BTC".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 1,
            lighter_size_decimals: 3,
            lighter_price_tick: dec!(0.1),
            tick: dec!(0.1),
            step: dec!(0.001),
            aster_min_qty: dec!(0.001),
            aster_min_notional: dec!(5),
            hl_sz_decimals: 3,
            hl_qty_step: dec!(0.001),
            hl_min_notional: dec!(10),
        }
    }

    #[test]
    fn decimal_and_wire_units_have_independent_expected_values() {
        let scale = MarketScale::from_spec(&spec());
        for (text, ticks, lots) in [("100.04", 1000, 100040), ("100.06", 1001, 100060), ("0.0059", 0, 5)] {
            let value: Decimal = text.parse().unwrap();
            assert_eq!(scale.price_to_ticks(value), ticks);
            assert_eq!(scale.qty_to_lots(value), lots);
            assert_eq!(scale.qty_str_to_lots(text), Some(lots));
        }
        assert_eq!(scale.price_dec_to_ticks(dec!(0)), None);
        assert_eq!(scale.price_str_to_ticks("-1"), None);
    }

    #[test]
    fn exact_decimal_levels_aggregate_ticks_and_drop_sublots() {
        let scale = MarketScale::from_spec(&spec());
        let raw = [("100.04", "0.005"), ("100.041", "0.003"), ("99.9", "1.5"), ("99.8", "0.0004"), ("98.15", "0.25")];
        let decimals: Vec<(Decimal, Decimal)> = raw.iter().map(|(p,q)| (p.parse().unwrap(), q.parse().unwrap())).collect();
        let decimal_book = build_hot_book_from_dec_levels_with_qty_scale(&decimals, &decimals, &scale, HotQtyScale::Hedge, 123, 456);
        let wire_book = build_hot_book_from_strs_with_qty_scale(raw, raw, &scale, HotQtyScale::Hedge, 123, 456);
        for book in [decimal_book, wire_book] {
            let bids: Vec<_> = book.bids().iter().map(|l| (l.px_ticks, l.qty_lots)).collect();
            assert_eq!(bids, vec![(1000, 8), (999, 1500), (982, 250)]);
        }
    }

    #[test]
    fn price_qty_round_trip() {
        let s = MarketScale::from_spec(&spec());
        assert_eq!(s.price_to_ticks(dec!(100.0)), 1000);
        assert_eq!(s.ticks_to_price(1000), dec!(100.0));
        assert_eq!(s.price_to_ticks(dec!(100.04)), 1000);
        assert_eq!(s.price_to_ticks(dec!(100.06)), 1001);
        assert_eq!(s.price_floor_ticks(dec!(100.06)), 1000);
        assert_eq!(s.price_ceil_ticks(dec!(100.04)), 1001);
        assert_eq!(s.qty_to_lots(dec!(0.0059)), 5);
        assert_eq!(s.lots_to_qty(5), dec!(0.005));
        assert_eq!(s.price_to_ticks(dec!(0)), 0);
        assert_eq!(s.qty_to_lots(dec!(-1)), 0);
    }

    fn book() -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(99.9), dec!(5))],
            vec![(dec!(100.1), dec!(3)), (dec!(100.2), dec!(4))],
            now,
            now,
        )
    }

    #[test]
    fn parses_exchange_strings_to_scaled_units_without_decimal() {
        let s = MarketScale::from_spec(&spec());
        assert_eq!(s.price_str_to_ticks("100.04"), Some(1000));
        assert_eq!(s.price_str_to_ticks("100.06"), Some(1001));
        assert_eq!(s.qty_str_to_lots("0.0059"), Some(5));
        assert_eq!(s.hl_qty_str_to_lots("0.0059"), Some(5));
        assert_eq!(s.price_str_to_ticks("0"), None);
        assert_eq!(s.qty_str_to_lots("bad"), None);
    }

    #[test]
    fn hedge_hot_quantities_use_the_hedge_step() {
        let mut spec = spec();
        spec.step = dec!(0.01);
        spec.hl_qty_step = dec!(0.001);
        let s = MarketScale::from_spec(&spec);

        let aster = build_hot_book_from_strs_with_qty_scale(
            [("100.0", "0.019")],
            [("100.1", "0.019")],
            &s,
            HotQtyScale::Aster,
            100,
            1_700_000_000_000,
        );
        let hl = build_hot_book_from_strs_with_qty_scale(
            [("100.0", "0.019")],
            [("100.1", "0.019")],
            &s,
            HotQtyScale::Hedge,
            100,
            1_700_000_000_000,
        );

        assert_eq!(aster.bids()[0].qty_lots, 1);
        assert_eq!(hl.bids()[0].qty_lots, 19);
        assert_eq!(s.hl_qty_to_lots(dec!(0.0199)), 19);
        assert_eq!(s.hl_qty_to_lots_ceil(dec!(0.0191)), 20);
    }

    #[test]
    fn hot_book_from_strings_sorts_and_aggregates() {
        let s = MarketScale::from_spec(&spec());
        let hb = build_hot_book_from_strs_with_qty_scale(
            [("99.9", "1"), ("100.0", "2"), ("100.0", "3")],
            [("100.2", "1"), ("100.1", "4")],
            &s,
            HotQtyScale::Aster,
            123,
            1_700_000_000_000,
        );
        assert_eq!(hb.exch_ms, 1_700_000_000_000);
        assert_eq!(hb.best_bid_ticks(), Some(1000));
        assert_eq!(hb.bids()[0].qty_lots, 5000);
        assert_eq!(hb.best_ask_ticks(), Some(1001));
    }

    #[test]
    fn hot_book_from_decimal() {
        let s = MarketScale::from_spec(&spec());
        let hb = build_hot_book(&book(), &s, 123);
        assert_eq!(hb.recv_ns, 123);
        assert_eq!(hb.best_bid_ticks(), Some(1000));
        assert_eq!(hb.best_ask_ticks(), Some(1001));
        assert_eq!(hb.bids().len(), 2);
        assert_eq!(hb.asks().len(), 2);
        assert!(!hb.is_crossed());
    }

    #[test]
    fn hot_book_detects_crossed() {
        let s = MarketScale::from_spec(&spec());
        let now = Utc::now();
        let crossed = OrderBook::from_levels(vec![(dec!(101), dec!(1))], vec![(dec!(100), dec!(1))], now, now);
        let hb = build_hot_book(&crossed, &s, 0);
        assert!(hb.is_crossed());
    }

    #[test]
    fn hot_book_truncates_to_capacity() {
        let s = MarketScale::from_spec(&spec());
        let now = Utc::now();
        let many: Vec<(Decimal, Decimal)> = (0..30).map(|i| (dec!(100) - Decimal::from(i) * dec!(0.1), dec!(1))).collect();
        let deep = OrderBook::from_levels(many, vec![(dec!(100.1), dec!(1))], now, now);
        let hb = build_hot_book(&deep, &s, 0);
        assert_eq!(hb.bids().len(), HOT_LEVELS);
    }
}

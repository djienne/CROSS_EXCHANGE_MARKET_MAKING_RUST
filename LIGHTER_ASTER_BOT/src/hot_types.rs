//! Shared hot-path data types — plain `Copy` structs.
//!
//! `HotBook` and `HotLevel` are the scaled-integer order book representation used by
//! the live strategy loop and the `VenueBook` cell. They live here (outside `hotpath`
//! and `livebot`) so both modules can import them.

/// Hot-book capacity: 20 levels for Aster/Lighter; Hyperliquid fast L2 supplies five.
pub const HOT_LEVELS: usize = 20;

/// Source age at receipt, computed once by ingest. Missing timestamps and clocks
/// over one second in the future are unusable; small exchange clock lead is tolerated.
#[inline]
pub fn source_age_at_receive_ms(source_ms: i64, receive_ms: i64) -> i64 {
    if source_ms <= 0 || source_ms > receive_ms.saturating_add(1_000) {
        i64::MAX
    } else {
        receive_ms.saturating_sub(source_ms).max(0)
    }
}


/// One book level in scaled-integer form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HotLevel {
    pub px_ticks: i64,
    pub qty_lots: i64,
}

/// The live hot-path order book: fixed-capacity integer levels and the monotonic receive
/// time. Built from a [`crate::book::OrderBook`] snapshot via a
/// [`crate::livebot::scale::MarketScale`]. No heap, no `Decimal` — cheap to copy and
/// compare on the quote loop.
#[derive(Debug, Clone, Copy)]
pub struct HotBook {
    bids: [HotLevel; HOT_LEVELS], // descending by px_ticks
    asks: [HotLevel; HOT_LEVELS], // ascending by px_ticks
    bid_len: u8,
    ask_len: u8,
    pub recv_ns: i64,
    /// Exchange timestamp (milliseconds since Unix epoch) carried by this snapshot. Used to
    /// merge independent BBO/depth feeds without letting a locally-late older BBO override
    /// a newer L2 book. Zero means "unknown" and is treated conservatively by callers.
    pub exch_ms: i64,
    pub source_age_at_recv_ms: i64,
}

impl HotBook {
    /// Construct directly from pre-filled level arrays. Used by `build_hot_book` in
    /// `livebot::scale`.
    pub fn new(
        bids: [HotLevel; HOT_LEVELS],
        asks: [HotLevel; HOT_LEVELS],
        bid_len: u8,
        ask_len: u8,
        recv_ns: i64,
        exch_ms: i64,
    ) -> Self {
        HotBook { bids, asks, bid_len, ask_len, recv_ns, exch_ms,
            source_age_at_recv_ms: if exch_ms > 0 { 0 } else { i64::MAX } }
    }

    #[inline]
    pub fn bids(&self) -> &[HotLevel] {
        &self.bids[..self.bid_len as usize]
    }
    #[inline]
    pub fn asks(&self) -> &[HotLevel] {
        &self.asks[..self.ask_len as usize]
    }

    #[inline]
    pub fn best_bid_ticks(&self) -> Option<i64> {
        self.bids().first().map(|l| l.px_ticks)
    }
    #[inline]
    pub fn best_ask_ticks(&self) -> Option<i64> {
        self.asks().first().map(|l| l.px_ticks)
    }

    /// True when the top of book is crossed or locked (bid >= ask), or when either
    /// side is missing (an incomplete book is untradeable). Callers that already
    /// check `best_bid_ticks()`/`best_ask_ticks()` for `None` separately will see
    /// no behavior change; for any future direct caller this is the safe default.
    #[inline]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid_ticks(), self.best_ask_ticks()) {
            (Some(b), Some(a)) => b >= a,
            _ => true,
        }
    }

    /// Source age plus monotonic time since receipt; no wall-clock call on the strategy path.
    #[inline]
    pub fn age_ms(&self, now_ns: i64) -> i64 {
        (now_ns.saturating_sub(self.recv_ns).max(0) / 1_000_000).saturating_add(self.source_age_at_recv_ms)
    }
}

#[cfg(test)]
mod source_time_tests {
    use super::*;

    #[test]
    fn delayed_or_unknown_source_does_not_become_fresh_on_receipt() {
        assert_eq!(source_age_at_receive_ms(1_700_000_000_000, 1_700_000_010_000), 10_000);
        assert_eq!(source_age_at_receive_ms(0, 1_700_000_010_000), i64::MAX);
        assert_eq!(source_age_at_receive_ms(1_700_000_010_500, 1_700_000_010_000), 0);
        assert_eq!(source_age_at_receive_ms(1_700_000_011_001, 1_700_000_010_000), i64::MAX);
        let mut book = HotBook::new([HotLevel::default(); HOT_LEVELS], [HotLevel::default(); HOT_LEVELS],
            0, 0, 1_000_000_000, 1_700_000_000_000);
        book.source_age_at_recv_ms = 10_000;
        assert_eq!(book.age_ms(1_005_000_000), 10_005);
    }
}

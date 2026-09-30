//! Live account state: capital + positions + open orders, reconciled from both venues.
//! Published atomically for the strategy and cold diagnostics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use rust_decimal::Decimal;

use crate::types::{MarketId, Side};

/// Which venue a position / order belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Aster,
    /// Lighter or Hyperliquid; serialized as "lighter" for journal compatibility.
    #[serde(rename = "lighter")]
    Hedge,
}

/// A reconciled signed position on one venue for one market. `signed_qty > 0` long.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScaledPosition {
    pub venue: Venue,
    pub market: MarketId,
    /// Signed base-unit quantity (positive long, negative short). Exact `Decimal`.
    pub signed_qty: Decimal,
    pub entry_px: Decimal,
}

/// A reconciled open order known to the venue at snapshot time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOrderSnapshot {
    pub venue: Venue,
    pub market: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    /// Bot-assigned client id, if this order is recognized as ours.
    pub client_id: Option<String>,
    /// Venue-assigned order identity.
    pub venue_order_id: Option<String>,
}

impl OpenOrderSnapshot {
    /// Whether this order looks like one of ours (client id carries our `X` prefix).
    pub fn is_bot_order(&self) -> bool {
        self.client_id.as_deref().is_some_and(|c| c.starts_with('X'))
    }
}

/// A full, immutable account snapshot. Published atomically via [`AccountState`].
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    pub aster_available_usd: Decimal,
    pub aster_margin_source_ns: i64,
    pub hl_margin_source_ns: i64,
    pub hl_withdrawable_usd: Decimal,
    /// Aster total equity = wallet balance + unrealized PnL (mark-to-market), NOT the free-margin
    /// `aster_available_usd`. Used by the circuit breaker so opening a hedge (which locks margin)
    /// does not look like a loss.
    pub aster_equity_usd: Decimal,
    /// Lighter collateral-style `portfolio_value`, or Hyperliquid's already-marked `accountValue`.
    /// Lighter's does not mark open positions (frozen 41 h while the uPnL moved $8, 2026-07-04),
    /// hence `hl_unrealized_usd`.
    pub hl_equity_usd: Decimal,
    /// Lighter position uPnL marked to fresh hedge mids and added to collateral.
    /// Zero for Hyperliquid, whose `hl_equity_usd` already includes uPnL.
    pub hl_unrealized_usd: Decimal,
    /// Every nonzero hedge position has a positive entry and a mark. Lighter uses fresh mids;
    /// Hyperliquid uses entry prices so extra uPnL stays zero. False samples cannot trip the breaker.
    pub hl_upnl_marked: bool,
    pub aster_positions: Vec<ScaledPosition>,
    pub hl_positions: Vec<ScaledPosition>,
    pub open_orders: Vec<OpenOrderSnapshot>,
    /// Bumped on each publish so the strategy can detect a changed snapshot.
    pub generation: u64,
    /// Monotonic-clock nanos when this snapshot finished being assembled (set AFTER the venue reads;
    /// used for the freshness check).
    pub source_ts_ns: i64,
    /// Monotonic nanos before the first venue read. Position confirmation requires reads begun
    /// after the latest relevant execution; `source_ts_ns` instead records completion time.
    pub read_start_ns: i64,
}

impl AccountSnapshot {
    /// An empty snapshot (pre-bootstrap): no capital, no positions, generation 0.
    pub fn empty() -> Self {
        AccountSnapshot {
            aster_available_usd: Decimal::ZERO,
            aster_margin_source_ns: 0,
            hl_margin_source_ns: 0,
            hl_withdrawable_usd: Decimal::ZERO,
            aster_equity_usd: Decimal::ZERO,
            hl_equity_usd: Decimal::ZERO,
            // A flat book is trivially marked: uPnL of nothing is exactly zero, and the
            // startup baseline (usually armed before any position exists) must not stall.
            hl_unrealized_usd: Decimal::ZERO,
            hl_upnl_marked: true,
            aster_positions: Vec::new(),
            hl_positions: Vec::new(),
            open_orders: Vec::new(),
            generation: 0,
            source_ts_ns: 0,
            read_start_ns: 0,
        }
    }

    /// Cross-venue marked equity (USD), including fees, funding, basis changes and transfers.
    /// Opposite position marks need not cancel exactly, especially across asynchronous reads.
    pub fn total_equity_usd(&self) -> Decimal {
        self.aster_equity_usd + self.hl_equity_usd + self.hl_unrealized_usd
    }

    /// Reported signed position for a (venue, market), 0 if none.
    pub fn reported_position(&self, venue: Venue, market: &MarketId) -> Decimal {
        let list = match venue {
            Venue::Aster => &self.aster_positions,
            Venue::Hedge => &self.hl_positions,
        };
        list.iter()
            .find(|p| &p.market == market)
            .map(|p| p.signed_qty)
            .unwrap_or(Decimal::ZERO)
    }
}

/// The published account state: an ArcSwap snapshot and a single-writer generation. Cloneable
/// handle (shares the inner `Arc`s), so each plane holds one.
#[derive(Clone)]
pub struct AccountState {
    snapshot: Arc<ArcSwap<AccountSnapshot>>,
    generation: Arc<AtomicU64>,
    pending_exec: Arc<ArcSwap<Vec<super::fills::HedgeIntent>>>,
    maker_queries: Arc<ArcSwap<Vec<MakerQuery>>>,
}

#[derive(Debug, Clone)]
pub struct MakerQuery {
    pub market: MarketId,
    pub side: Side,
    pub client_id: String,
    pub qty_lots: i64,
}

impl Default for AccountState {
    fn default() -> Self {
        AccountState {
            snapshot: Arc::new(ArcSwap::from_pointee(AccountSnapshot::empty())),
            generation: Arc::new(AtomicU64::new(0)),
            pending_exec: Arc::new(ArcSwap::from_pointee(Vec::new())),
            maker_queries: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }

}

impl AccountState {
    /// Publish one immutable snapshot and its generation; the reconciler is the single writer.
    pub fn publish(&self, mut snap: AccountSnapshot) {
        let next_generation = self.generation.load(Ordering::Acquire).saturating_add(1);
        snap.generation = next_generation;
        self.snapshot.store(Arc::new(snap));
        self.generation.store(next_generation, Ordering::Release);
    }

    pub fn publish_pending_exec(&self, intents: Vec<super::fills::HedgeIntent>) {
        self.pending_exec.store(Arc::new(intents));
    }

    pub fn pending_exec(&self) -> Arc<Vec<super::fills::HedgeIntent>> { self.pending_exec.load_full() }

    pub fn publish_maker_queries(&self, queries: Vec<MakerQuery>) { self.maker_queries.store(Arc::new(queries)); }
    pub fn maker_queries(&self) -> Arc<Vec<MakerQuery>> { self.maker_queries.load_full() }

    /// Lock-free read of the current snapshot.
    pub fn load(&self) -> Arc<AccountSnapshot> {
        self.snapshot.load_full()
    }

    /// Snapshot age in ms at monotonic `now_ns` (`i64::MAX` before the first publish).
    pub fn age_ms(&self, now_ns: i64) -> i64 {
        let ts = self.load().source_ts_ns;
        if ts == 0 {
            i64::MAX
        } else {
            now_ns.saturating_sub(ts) / 1_000_000
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn snap() -> AccountSnapshot {
        AccountSnapshot {
            aster_available_usd: dec!(1000),
            aster_margin_source_ns: 1,
            hl_margin_source_ns: 1,
            hl_withdrawable_usd: dec!(900),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(900),
            hl_unrealized_usd: dec!(0),
            hl_upnl_marked: true,
            aster_positions: vec![ScaledPosition {
                venue: Venue::Aster,
                market: "BTC".into(),
                signed_qty: dec!(0.5),
                entry_px: dec!(100),
            }],
            hl_positions: vec![ScaledPosition {
                venue: Venue::Hedge,
                market: "BTC".into(),
                signed_qty: dec!(-0.5),
                entry_px: dec!(100),
            }],
            open_orders: vec![
                OpenOrderSnapshot {
                    venue: Venue::Aster,
                    market: "BTC".into(),
                    side: Side::Buy,
                    price: dec!(99),
                    qty: dec!(0.1),
                    client_id: Some("Xabc-BTC-B-0".into()),
                    venue_order_id: Some("111".into()),
                },
                OpenOrderSnapshot {
                    venue: Venue::Aster,
                    market: "BTC".into(),
                    side: Side::Sell,
                    price: dec!(101),
                    qty: dec!(0.1),
                    client_id: Some("manual-order".into()), // not ours (no X prefix)
                    venue_order_id: Some("222".into()),
                },
            ],
            generation: 3,
            source_ts_ns: 5_000_000,
            read_start_ns: 4_000_000,
        }
    }

    #[test]
    fn reported_position_lookup() {
        let s = snap();
        assert_eq!(s.reported_position(Venue::Aster, &"BTC".into()), dec!(0.5));
        assert_eq!(s.reported_position(Venue::Hedge, &"BTC".into()), dec!(-0.5));
        assert_eq!(s.reported_position(Venue::Aster, &"ETH".into()), dec!(0)); // absent
    }

    #[test]
    fn account_state_publish_and_generation() {
        let st = AccountState::default();
        assert_eq!(st.load().generation, 0);
        st.publish(snap());
        assert_eq!(st.load().generation, 1);
        assert_eq!(st.load().generation, 1);
        assert_eq!(st.load().aster_available_usd, dec!(1000));
    }

    #[test]
    fn total_equity_includes_hl_unrealized() {
        let mut s = snap();
        assert_eq!(s.total_equity_usd(), dec!(1900));
        s.hl_unrealized_usd = dec!(8.5);
        assert_eq!(s.total_equity_usd(), dec!(1908.5));
        s.hl_unrealized_usd = dec!(-3.25);
        assert_eq!(s.total_equity_usd(), dec!(1896.75));
    }

}

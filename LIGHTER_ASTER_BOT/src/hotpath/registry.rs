//! The shared map of lock-free book cells, keyed by (market, venue). The live
//! driver builds it once and clones `Arc<VenueBook>` handles into each ingest
//! thread (writers) and the watchdog (reader).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::types::{MarketId, MarketIdx};

use super::book_cell::{VenueBook, VenueTag};
use super::dirty::DirtyMarkets;

/// All venue book cells for a live session. Built once, shared read-only; the cells
/// themselves are interior-mutable via atomics, so no lock is needed to publish.
pub struct VenueRegistry {
    cells: HashMap<(MarketId, VenueTag), Arc<VenueBook>>,
    market_to_idx: HashMap<MarketId, MarketIdx>,
    idx_to_market: Vec<MarketId>,
}

impl VenueRegistry {
    /// Build a registry with an Aster and a Lighter cell for each market.
    #[cfg(test)]
    pub fn new(markets: &[MarketId]) -> Self {
        Self::build(markets, |_| VenueBook::new())
    }

    /// Like [`new`] but every cell publishes into a shared coalescing wakeup `Notify`,
    /// so a live strategy loop parked on it wakes on the next book change of ANY cell,
    /// and marks its market in a shared dirty-market bitset.
    pub fn with_wake_and_dirty(markets: &[MarketId], wake: Arc<Notify>, dirty: Arc<DirtyMarkets>) -> Self {
        Self::build(markets, |idx| VenueBook::with_wake_and_dirty(wake.clone(), dirty.clone(), idx))
    }

    fn build(markets: &[MarketId], new_cell: impl Fn(MarketIdx) -> VenueBook) -> Self {
        let mut cells = HashMap::new();
        let mut market_to_idx = HashMap::new();
        let mut idx_to_market = Vec::new();

        for (i, m) in markets.iter().enumerate() {
            let idx = MarketIdx(i as u16);
            market_to_idx.insert(m.clone(), idx);
            idx_to_market.push(m.clone());

            for v in [VenueTag::Aster, VenueTag::Hedge] {
                cells.insert((m.clone(), v), Arc::new(new_cell(idx)));
            }
        }
        VenueRegistry { cells, market_to_idx, idx_to_market }
    }

    /// The cell for one (market, venue), if present.
    pub fn cell(&self, market: &MarketId, venue: VenueTag) -> Option<Arc<VenueBook>> {
        self.cells.get(&(market.clone(), venue)).cloned()
    }

    /// Iterate every cell with its key — used by the watchdog's staleness scan.
    pub fn iter(&self) -> impl Iterator<Item = (&(MarketId, VenueTag), &Arc<VenueBook>)> {
        self.cells.iter()
    }

    /// Dense index for a market (assigned at construction).
    pub fn market_idx(&self, market: &MarketId) -> Option<MarketIdx> {
        self.market_to_idx.get(market).copied()
    }

    /// Market ID from a dense index.
    pub fn market_id(&self, idx: MarketIdx) -> Option<&MarketId> {
        self.idx_to_market.get(idx.0 as usize)
    }

    /// Number of distinct markets (not cells — each market has 2 cells).
    pub fn num_markets(&self) -> usize {
        self.idx_to_market.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_two_cells_per_market() {
        let reg = VenueRegistry::new(&["BTC".into(), "DOGE".into()]);
        assert_eq!(reg.iter().count(), 4);
        assert!(reg.cell(&"BTC".into(), VenueTag::Aster).is_some());
        assert!(reg.cell(&"BTC".into(), VenueTag::Hedge).is_some());
        assert!(reg.cell(&"ETH".into(), VenueTag::Aster).is_none());
    }

    #[test]
    fn cells_are_shared_handles() {
        let reg = VenueRegistry::new(&["BTC".into()]);
        let a = reg.cell(&"BTC".into(), VenueTag::Aster).unwrap();
        let b = reg.cell(&"BTC".into(), VenueTag::Aster).unwrap();
        // Both handles point at the same cell: a publish through one is seen by the other.
        a.touch();
        assert_eq!(a.last_msg_ns(), b.last_msg_ns());
        assert!(b.last_msg_ns() > 0);
    }

    #[test]
    fn market_idx_roundtrip() {
        let reg = VenueRegistry::new(&["BTC".into(), "ETH".into()]);
        let btc_idx = reg.market_idx(&"BTC".into()).unwrap();
        let eth_idx = reg.market_idx(&"ETH".into()).unwrap();
        assert_ne!(btc_idx, eth_idx);
        assert_eq!(reg.market_id(btc_idx).unwrap().0, "BTC");
        assert_eq!(reg.market_id(eth_idx).unwrap().0, "ETH");
        assert_eq!(reg.num_markets(), 2);
    }
}

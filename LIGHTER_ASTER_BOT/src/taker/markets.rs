use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::taker::types::MarketId;

/// A market's two legs. The `aster_*`, `tick` and `step` fields describe the first leg, Aster's
/// or Lighter's (`first`); the `lighter_*` ones the second, Lighter's or Hyperliquid's (`hedge`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSpec {
    pub market_id: MarketId,
    pub aster_symbol: String,
    pub lighter_symbol: String,
    pub lighter_market_id: u32,
    pub lighter_price_decimals: u32,
    pub lighter_size_decimals: u32,
    pub lighter_price_tick: Decimal,
    pub tick: Decimal,
    pub step: Decimal,
    pub aster_min_qty: Decimal,
    pub aster_min_notional: Decimal,
    pub lighter_qty_step: Decimal,
    pub lighter_min_notional: Decimal,
    /// The second leg's venue; the `lighter_*` fields describe its market there.
    #[serde(default)]
    pub hedge: crate::config::HedgeVenue,
    #[serde(default)]
    pub first: crate::config::FirstVenue,
    /// The first leg's Lighter market index, when Lighter is the first leg.
    #[serde(default)]
    pub first_market_index: u32,
}

impl MarketSpec {
    /// The first leg's market in the second leg's fields, which a Lighter venue client reads.
    pub fn first_leg_view(&self) -> MarketSpec {
        MarketSpec {
            lighter_symbol: self.aster_symbol.clone(),
            lighter_market_id: self.first_market_index,
            lighter_price_decimals: self.tick.scale(),
            lighter_size_decimals: self.step.scale(),
            lighter_price_tick: self.tick,
            lighter_qty_step: self.step,
            lighter_min_notional: self.aster_min_notional,
            hedge: crate::config::HedgeVenue::Lighter,
            ..self.clone()
        }
    }
}

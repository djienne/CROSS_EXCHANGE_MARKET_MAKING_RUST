use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::taker::types::MarketId;

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
}

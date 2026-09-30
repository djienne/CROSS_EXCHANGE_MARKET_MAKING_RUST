//! Resolved XEMM market specs. Legacy `hl_*`/`lighter_*` fields describe either hedge venue.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::MarketId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSpec {
    pub market_id: MarketId,
    pub aster_symbol: String,
    pub hl_coin: String,
    #[serde(default)]
    pub hedge: crate::config::HedgeVenue,
    #[serde(default)]
    pub lighter_market_id: u32,
    #[serde(default)]
    pub lighter_price_decimals: u32,
    #[serde(default)]
    pub lighter_size_decimals: u32,
    #[serde(default)]
    pub lighter_price_tick: Decimal,
    pub tick: Decimal,
    pub step: Decimal,
    pub aster_min_qty: Decimal,
    pub aster_min_notional: Decimal,
    pub hl_sz_decimals: i32,
    pub hl_qty_step: Decimal,
    pub hl_min_notional: Decimal,
}

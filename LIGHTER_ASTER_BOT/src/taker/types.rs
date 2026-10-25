use serde::{Deserialize, Serialize};

use rust_decimal::Decimal;

pub use crate::types::{MarketId, Side, TxSendStatus};

/// Only venue-reported rates/commissions establish a known execution fee.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeProvenance {
    Venue,
    Estimated,
    #[default]
    Unknown,
}

/// Cold provenance for one Lighter trade; fee_ticks is the venue's role-specific ppm rate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeEvidence {
    pub trade_id: Option<i64>,
    pub order_id: Option<i64>,
    pub client_order_index: i64,
    pub maker: Option<bool>,
    pub notional_usd: Decimal,
    pub fee_ticks: Option<Decimal>,
    pub fee_usd: Option<Decimal>,
    pub event_time_ms: Option<i64>,
    pub source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FillSummary {
    pub qty: Decimal,
    pub vwap: Decimal,
    pub notional: Decimal,
    pub fee_usd: Decimal,
    #[serde(default)]
    pub fee_provenance: FeeProvenance,
}

impl FillSummary {
    pub fn zero() -> Self {
        Self {
            qty: Decimal::ZERO,
            vwap: Decimal::ZERO,
            notional: Decimal::ZERO,
            fee_usd: Decimal::ZERO,
            fee_provenance: FeeProvenance::Venue,
        }
    }

    pub fn with_fee_provenance(mut self, provenance: FeeProvenance) -> Self {
        self.fee_provenance = provenance;
        self
    }

    pub fn from_qty_notional(qty: Decimal, notional: Decimal, fee_usd: Decimal) -> Option<Self> {
        if qty <= Decimal::ZERO || notional <= Decimal::ZERO {
            return None;
        }
        Some(Self {
            qty,
            vwap: notional / qty,
            notional,
            fee_usd,
            fee_provenance: FeeProvenance::Venue,
        })
    }
}

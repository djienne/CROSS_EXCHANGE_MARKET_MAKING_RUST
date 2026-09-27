//! Edge math, in exact `Decimal`. The quote is priced
//! *backward* from the Lighter hedge: given the hedge VWAP and a reference
//! price, find the most aggressive Aster price that still clears the required
//! edge after fees and buffers.
//!
//! Notation: `ref` normalizes edge to bps; `f_a` = Aster maker fee rate; `f_l`
//! = Lighter taker fee rate; `req` = (min_net_profit + slippage + latency + basis +
//! funding) / 10000.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::decimal::{bps_to_rate, rate_to_bps};
use crate::types::Side;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    pub min_net_profit_bps: Decimal,
    pub slippage_buffer_bps: Decimal,
    pub latency_buffer_bps: Decimal,
    pub basis_buffer_bps: Decimal,
    pub funding_buffer_bps: Decimal,
    pub aster_maker_fee_bps: Decimal,
    /// The hedge's taker fee on Lighter.
    pub taker_fee_bps: Decimal,
    /// The hedge's taker fee on Hyperliquid (the account's `userCrossRate`); a market hedged
    /// there needs it.
    #[serde(default)]
    pub hyperliquid_taker_fee_bps: Option<Decimal>,
}

impl EdgeConfig {
    /// min_net_profit + all buffers (the edge a quote must clear at placement).
    pub fn required_bps(&self) -> Decimal {
        self.min_net_profit_bps + self.total_buffer_bps()
    }

    /// Just the safety buffers (slippage + latency + basis + funding), excluding
    /// `min_net_profit_bps`.
    pub fn total_buffer_bps(&self) -> Decimal {
        self.slippage_buffer_bps
            + self.latency_buffer_bps
            + self.basis_buffer_bps
            + self.funding_buffer_bps
    }

    pub fn required_rate(&self) -> Decimal {
        bps_to_rate(self.required_bps())
    }

    pub fn aster_maker_fee_rate(&self) -> Decimal {
        bps_to_rate(self.aster_maker_fee_bps)
    }

    pub fn taker_fee_rate(&self) -> Decimal {
        bps_to_rate(self.taker_fee_bps)
    }
}

/// Aster maker buy hedged by a Lighter taker sell: the maximum Aster bid that still
/// satisfies the required edge. `None` if no positive price clears it.
pub fn max_profitable_aster_bid(
    lighter_sell_vwap: Decimal,
    ref_px: Decimal,
    cfg: &EdgeConfig,
) -> Option<Decimal> {
    let one = Decimal::ONE;
    let numerator = lighter_sell_vwap * (one - cfg.taker_fee_rate()) - cfg.required_rate() * ref_px;
    let denominator = one + cfg.aster_maker_fee_rate();
    if denominator <= Decimal::ZERO || numerator <= Decimal::ZERO {
        return None;
    }
    Some(numerator / denominator)
}

/// Aster maker sell hedged by a Lighter taker buy: the minimum Aster ask that still
/// satisfies the required edge.
pub fn min_profitable_aster_ask(
    lighter_buy_vwap: Decimal,
    ref_px: Decimal,
    cfg: &EdgeConfig,
) -> Option<Decimal> {
    let one = Decimal::ONE;
    let numerator = lighter_buy_vwap * (one + cfg.taker_fee_rate()) + cfg.required_rate() * ref_px;
    let denominator = one - cfg.aster_maker_fee_rate();
    if denominator <= Decimal::ZERO || numerator <= Decimal::ZERO {
        return None;
    }
    Some(numerator / denominator)
}

/// Net edge in bps after fees and buffers, but BEFORE subtracting
/// `min_net_profit_bps`. A quote is acceptable iff this is `>= min_net_profit_bps`.
pub fn net_edge_bps_after_fees_and_buffers(
    aster_side: Side,
    aster_px: Decimal,
    lighter_hedge_vwap: Decimal,
    ref_px: Decimal,
    cfg: &EdgeConfig,
) -> Decimal {
    let one = Decimal::ONE;
    let f_a = cfg.aster_maker_fee_rate();
    let f_l = cfg.taker_fee_rate();
    let net_unit = match aster_side {
        Side::Buy => lighter_hedge_vwap * (one - f_l) - aster_px * (one + f_a),
        Side::Sell => aster_px * (one - f_a) - lighter_hedge_vwap * (one + f_l),
    };
    if ref_px <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    rate_to_bps(net_unit / ref_px)
        - cfg.slippage_buffer_bps
        - cfg.latency_buffer_bps
        - cfg.basis_buffer_bps
        - cfg.funding_buffer_bps
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn cfg() -> EdgeConfig {
        EdgeConfig {
            min_net_profit_bps: dec!(3.0),
            slippage_buffer_bps: dec!(1.5),
            latency_buffer_bps: dec!(2.0),
            basis_buffer_bps: dec!(1.0),
            funding_buffer_bps: dec!(0.0),
            aster_maker_fee_bps: dec!(1.0),
            taker_fee_bps: dec!(4.5),
            hyperliquid_taker_fee_bps: None,
        }
    }

    /// The headline invariant: a quote priced exactly at the profitable bound,
    /// fed back through the net-edge formula, yields net == min_net_profit_bps.
    /// The round trip alone would pass a sign error shared by both formulas, so the
    /// bound is also pinned to a hand-computed value.
    #[test]
    fn bid_round_trip() {
        let cfg = cfg();
        let hl_sell_vwap = dec!(100.0);
        let ref_px = dec!(100.0);
        let bound = max_profitable_aster_bid(hl_sell_vwap, ref_px, &cfg).unwrap();
        // (100*(1-0.00045) - 0.00075*100)/(1+0.0001) = 99.88/1.0001
        assert!((bound - dec!(99.870013)).abs() < dec!(0.000001), "bound={bound}");
        let net = net_edge_bps_after_fees_and_buffers(Side::Buy, bound, hl_sell_vwap, ref_px, &cfg);
        assert!((net - cfg.min_net_profit_bps).abs() < dec!(0.0001), "net={net}");
    }

    #[test]
    fn ask_round_trip() {
        let cfg = cfg();
        let hl_buy_vwap = dec!(100.0);
        let ref_px = dec!(100.0);
        let bound = min_profitable_aster_ask(hl_buy_vwap, ref_px, &cfg).unwrap();
        // (100*(1+0.00045) + 0.00075*100)/(1-0.0001) = 100.12/0.9999
        assert!((bound - dec!(100.130013)).abs() < dec!(0.000001), "bound={bound}");
        let net = net_edge_bps_after_fees_and_buffers(Side::Sell, bound, hl_buy_vwap, ref_px, &cfg);
        assert!((net - cfg.min_net_profit_bps).abs() < dec!(0.0001), "net={net}");
    }

    #[test]
    fn bid_below_bound_increases_edge() {
        // Buying lower than the bound only improves edge.
        let cfg = cfg();
        let (hl, refp) = (dec!(100.0), dec!(100.0));
        let bound = max_profitable_aster_bid(hl, refp, &cfg).unwrap();
        let cheaper = bound - dec!(0.01);
        let net = net_edge_bps_after_fees_and_buffers(Side::Buy, cheaper, hl, refp, &cfg);
        assert!(net > cfg.min_net_profit_bps);
    }

    #[test]
    fn net_edge_zero_ref_is_safe() {
        let cfg = cfg();
        let edge = net_edge_bps_after_fees_and_buffers(Side::Buy, dec!(100), dec!(100), dec!(0), &cfg);
        assert_eq!(edge, dec!(0));
    }

    #[test]
    fn unprofitable_bound_is_none() {
        let mut cfg = cfg();
        cfg.min_net_profit_bps = dec!(100000); // absurd requirement
        assert!(max_profitable_aster_bid(dec!(100), dec!(100), &cfg).is_none());
    }
}

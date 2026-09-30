//! One cross-margined perp account per simulated venue: collateral, positions, fees, funding
//! and margin, marked at the replica mid.
//!
//! Books close exactly: balance = initial + realized - fees + funding at every step.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::position::SignedPosition;
use crate::types::Side;

/// Simplified 2% maintenance assumption for the breach flag; venue/market tiers are not modeled.
const MAINTENANCE_RATE: Decimal = Decimal::from_parts(2, 0, 0, false, 2);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub initial: Decimal,
    pub balance: Decimal,
    pub positions: BTreeMap<String, SignedPosition>,
    pub realized: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PositionView {
    pub market: String,
    pub qty: Decimal,
    pub entry: Decimal,
    pub mark: Decimal,
    pub unrealized: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountView {
    pub balance: Decimal,
    pub unrealized: Decimal,
    pub equity: Decimal,
    /// Initial margin of positions and open orders.
    pub margin: Decimal,
    pub available: Decimal,
    pub positions: Vec<PositionView>,
    /// Equity is below maintenance: a real venue would be liquidating (not simulated).
    pub maintenance_breach: bool,
}

/// Open-order quantity per market and side, for margin.
#[derive(Debug, Clone, Default)]
pub struct Working {
    pub buys: Decimal,
    pub sells: Decimal,
}

impl Account {
    pub fn new(initial: Decimal) -> Self {
        Self {
            initial,
            balance: initial,
            positions: BTreeMap::new(),
            realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
        }
    }

    pub fn position(&self, market: &str) -> SignedPosition {
        self.positions.get(market).copied().unwrap_or_default()
    }

    /// Books a fill; returns the realized PnL it closed.
    pub fn fill(&mut self, market: &str, side: Side, qty: Decimal, price: Decimal, fee: Decimal) -> Decimal {
        let realized = self.positions.entry(market.to_string()).or_default()
            .apply_fill(SignedPosition::signed(side, qty), price);
        self.realized += realized;
        self.fees += fee;
        self.balance += realized - fee;
        realized
    }

    /// Books a funding payment (positive = received).
    pub fn fund(&mut self, amount: Decimal) {
        self.funding += amount;
        self.balance += amount;
    }

    /// Initial margin at `leverage`: per market, the larger exposure the position reaches if
    /// every working order on one side fills, times the mark.
    pub fn margin(
        &self,
        marks: &impl Fn(&str) -> Option<Decimal>,
        working: &BTreeMap<String, Working>,
        leverage: Decimal,
    ) -> Decimal {
        let flat = Working::default();
        let mut markets: Vec<&String> = self.positions.keys().chain(working.keys()).collect();
        markets.sort();
        markets.dedup();
        let mut total = Decimal::ZERO;
        for market in markets {
            let qty = self.position(market).qty;
            let w = working.get(market).unwrap_or(&flat);
            let exposure = (qty + w.buys).abs().max((qty - w.sells).abs());
            total += exposure * marks(market).unwrap_or_default() / leverage;
        }
        total
    }

    pub fn view(
        &self,
        marks: &impl Fn(&str) -> Option<Decimal>,
        working: &BTreeMap<String, Working>,
        leverage: Decimal,
    ) -> AccountView {
        let mut unrealized = Decimal::ZERO;
        let mut maintenance = Decimal::ZERO;
        let positions = self.positions.iter().map(|(market, p)| {
            let mark = marks(market).unwrap_or(p.avg_px);
            let pnl = p.qty * (mark - p.avg_px);
            unrealized += pnl;
            maintenance += p.qty.abs() * mark * MAINTENANCE_RATE;
            PositionView { market: market.clone(), qty: p.qty, entry: p.avg_px, mark, unrealized: pnl }
        }).collect();
        let equity = self.balance + unrealized;
        let margin = self.margin(marks, working, leverage);
        AccountView {
            balance: self.balance,
            unrealized,
            equity,
            margin,
            available: equity - margin,
            positions,
            maintenance_breach: equity < maintenance,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn balance_closes_on_realized_fees_and_funding_through_a_flip() {
        let mut a = Account::new(dec!(200));
        a.fill("HYPE", Side::Buy, dec!(2), dec!(40), dec!(0.032));
        a.fill("HYPE", Side::Sell, dec!(3), dec!(41), dec!(0.0492));
        a.fund(dec!(-0.01));
        a.fill("HYPE", Side::Buy, dec!(1), dec!(39.5), dec!(0));
        assert_eq!(a.position("HYPE"), SignedPosition::default());
        assert_eq!(a.realized, dec!(2) + dec!(1.5));
        assert_eq!(a.balance, a.initial + a.realized - a.fees + a.funding);
        assert_eq!(a.balance, dec!(203.4088));
    }

    #[test]
    fn margin_counts_the_worse_side_of_working_orders() {
        let mut a = Account::new(dec!(100));
        a.fill("HYPE", Side::Buy, dec!(1), dec!(40), dec!(0));
        let marks = |_: &str| Some(dec!(50));
        let mut working = BTreeMap::new();
        // A sell that only closes the long adds nothing; buys extend it.
        working.insert("HYPE".to_string(), Working { buys: dec!(0), sells: dec!(1) });
        assert_eq!(a.margin(&marks, &working, dec!(1)), dec!(50));
        working.insert("HYPE".to_string(), Working { buys: dec!(0.5), sells: dec!(3) });
        assert_eq!(a.margin(&marks, &working, dec!(2)), dec!(50));
        let v = a.view(&marks, &working, dec!(2));
        assert_eq!((v.unrealized, v.equity, v.available), (dec!(10), dec!(110), dec!(60)));
        assert!(!v.maintenance_breach);
    }
}

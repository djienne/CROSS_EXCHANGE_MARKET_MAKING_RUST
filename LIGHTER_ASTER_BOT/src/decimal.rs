//! Pure `Decimal` helpers: bps<->rate conversion, tick/step rounding and parsing.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

#[inline]
pub fn bps_to_rate(bps: Decimal) -> Decimal {
    bps / Decimal::from(10_000)
}

#[inline]
pub fn rate_to_bps(rate: Decimal) -> Decimal {
    rate * Decimal::from(10_000)
}

/// Largest multiple of `step` that is `<= value` (round toward negative infinity).
/// Returns `value` unchanged if `step <= 0`.
#[inline]
pub fn floor_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).floor() * step
}

/// Smallest multiple of `step` that is `>= value` (round toward positive infinity).
/// Returns `value` unchanged if `step <= 0`.
#[inline]
pub fn ceil_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).ceil() * step
}

/// Parse a decimal from an exchange string, trimming whitespace.
pub fn parse_dec(s: &str) -> Result<Decimal> {
    Decimal::from_str(s.trim()).with_context(|| format!("invalid decimal: {s:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn bps_roundtrip() {
        assert_eq!(bps_to_rate(dec!(4.5)), dec!(0.00045));
        assert_eq!(rate_to_bps(dec!(0.00045)), dec!(4.5));
        assert_eq!(rate_to_bps(bps_to_rate(dec!(3.0))), dec!(3.0));
    }

    #[test]
    fn floor_step() {
        assert_eq!(floor_to_step(dec!(100.45), dec!(0.1)), dec!(100.4));
        assert_eq!(floor_to_step(dec!(100.40), dec!(0.1)), dec!(100.4));
        assert_eq!(floor_to_step(dec!(0.00057), dec!(0.0001)), dec!(0.0005));
        // step <= 0 is a no-op
        assert_eq!(floor_to_step(dec!(1.23), dec!(0)), dec!(1.23));
    }

    #[test]
    fn ceil_step() {
        assert_eq!(ceil_to_step(dec!(100.41), dec!(0.1)), dec!(100.5));
        assert_eq!(ceil_to_step(dec!(100.40), dec!(0.1)), dec!(100.4));
        assert_eq!(ceil_to_step(dec!(0.00051), dec!(0.0001)), dec!(0.0006));
    }

    #[test]
    fn parse_ok_and_err() {
        assert_eq!(parse_dec(" 12345.6 ").unwrap(), dec!(12345.6));
        assert!(parse_dec("not-a-number").is_err());
    }

}

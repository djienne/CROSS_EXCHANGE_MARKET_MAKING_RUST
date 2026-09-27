//! Cold-path REST order-book snapshots for both venues, used to cross-check the
//! WebSocket-driven books. This is a slow reconciliation aid (seconds cadence), NOT
//! on the quote hot path: it confirms each venue's WS feed is building the book
//! faithfully (no parse drift, stuck snapshot, or wrong symbol). Aster uses the
//! Binance-style futures depth endpoint; Lighter uses `orderBookOrders`, Hyperliquid `l2Book`.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::book::OrderBook;
use crate::decimal::parse_dec;
use crate::types::Side;
use crate::vwap::vwap_take;

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

/// HTTP client for the periodic cross-check. Short timeout: a slow REST call must
/// never stall the reconciler past its scan interval.
pub fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("building book-check http client")
}

#[derive(Deserialize)]
struct AsterDepthResp {
    #[serde(rename = "E", default)]
    event_time: i64,
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
}

/// Fetch the Aster partial-depth snapshot via REST (mirrors the `@depth20` WS feed).
pub async fn fetch_aster_book_from_base(
    client: &reqwest::Client,
    base_url: &str,
    symbol_upper: &str,
    limit: u32,
) -> Result<OrderBook> {
    let limit_s = limit.to_string();
    let url = endpoint(base_url, "/fapi/v3/depth");
    let resp: AsterDepthResp = client
        .get(&url)
        .query(&[("symbol", symbol_upper), ("limit", limit_s.as_str())])
        .send()
        .await
        .with_context(|| format!("GET Aster depth {symbol_upper} from {base_url}"))?
        .error_for_status()
        .with_context(|| format!("Aster depth {symbol_upper} status"))?
        .json()
        .await
        .with_context(|| format!("parsing Aster depth {symbol_upper}"))?;
    Ok(OrderBook::from_levels(
        parse_pairs(&resp.bids),
        parse_pairs(&resp.asks),
        ms_to_dt(resp.event_time),
        Utc::now(),
    ))
}

/// Fetch the Lighter book snapshot via REST.
pub async fn fetch_lighter_book_from_base(client: &reqwest::Client, base_url: &str, market_id: u32, limit: u32) -> Result<OrderBook> {
    let url = endpoint(base_url, "/api/v1/orderBookOrders");
    let resp: serde_json::Value = client
        .get(&url)
        .query(&[("market_id", market_id.to_string()), ("limit", limit.to_string())])
        .send()
        .await
        .with_context(|| format!("GET Lighter orderBookOrders {market_id} from {base_url}"))?
        .error_for_status()
        .with_context(|| format!("Lighter orderBookOrders {market_id} status"))?
        .json()
        .await
        .with_context(|| format!("parsing Lighter orderBookOrders {market_id}"))?;
    let bids = lighter_rows(resp.get("bids").or_else(|| resp.pointer("/order_book/bids")));
    let asks = lighter_rows(resp.get("asks").or_else(|| resp.pointer("/order_book/asks")));
    Ok(OrderBook::from_levels(bids, asks, Utc::now(), Utc::now()))
}

/// Hyperliquid's `l2Book` (/info weight 2, up to 20 levels a side), stamped with its `time`.
pub async fn fetch_hyperliquid_book(client: &reqwest::Client, base_url: &str, coin: &str) -> Result<OrderBook> {
    let book = crate::hyperliquid::client::info(client, base_url, serde_json::json!({"type": "l2Book", "coin": coin})).await?;
    let time = book["time"].as_i64().with_context(|| format!("Hyperliquid l2Book {coin} without a time"))?;
    Ok(OrderBook::from_levels(lighter_rows(book.pointer("/levels/0")), lighter_rows(book.pointer("/levels/1")), ms_to_dt(time), Utc::now()))
}

/// The REST book of `spec`'s hedge venue.
pub async fn fetch_hedge_book(client: &reqwest::Client, live: &crate::config::LiveCfg, spec: &crate::markets::MarketSpec, limit: u32) -> Result<OrderBook> {
    match spec.hedge {
        crate::config::HedgeVenue::Lighter => fetch_lighter_book_from_base(client, &live.lighter.base_url, spec.lighter_market_id, limit).await,
        crate::config::HedgeVenue::Hyperliquid => fetch_hyperliquid_book(client, &live.hyperliquid.base_url, &spec.hl_coin).await,
    }
}

fn parse_pairs(rows: &[[String; 2]]) -> Vec<(Decimal, Decimal)> {
    rows.iter()
        .filter_map(|r| match (parse_dec(&r[0]), parse_dec(&r[1])) {
            (Ok(p), Ok(q)) => Some((p, q)),
            _ => None,
        })
        .collect()
}

/// Lighter rows (`price`/`remaining_base_amount`) or Hyperliquid's (`px`/`sz`).
fn lighter_rows(v: Option<&serde_json::Value>) -> Vec<(Decimal, Decimal)> {
    let Some(rows) = v.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|r| {
            let p = r
                .get("price")
                .or_else(|| r.get("px"))
                .and_then(value_dec)?;
            let q = r
                .get("remaining_base_amount")
                .or_else(|| r.get("size"))
                .or_else(|| r.get("sz"))
                .and_then(value_dec)?;
            (p > Decimal::ZERO && q > Decimal::ZERO).then_some((p, q))
        })
        .collect()
}

fn value_dec(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::String(s) => parse_dec(s).ok(),
        serde_json::Value::Number(n) => parse_dec(&n.to_string()).ok(),
        _ => None,
    }
}

fn ms_to_dt(ms: i64) -> chrono::DateTime<chrono::Utc> {
    if ms > 0 { chrono::DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now) } else { Utc::now() }
}

/// A websocket-vs-REST top-of-book comparison for one (market, venue). Pure data —
/// the caller decides what divergence is actionable (tolerance, repeat count).
#[derive(Debug, Clone)]
pub struct BookComparison {
    pub ws_mid: Option<Decimal>,
    pub rest_mid: Option<Decimal>,
    /// `|ws_mid - rest_mid| / rest_mid` in basis points, if both mids exist.
    pub mid_diff_bps: Option<Decimal>,
    /// Worst of the buy/sell VWAP-to-fill-`vwap_size` divergences between WS and REST,
    /// `|ws_vwap - rest_vwap| / rest_mid` in bps. `None` when neither side can fill the
    /// probe size on both books. Catches deep-book drift the top-of-book mid misses
    /// (a feed whose top is right but whose hedge depth is stale or malformed).
    pub vwap_diff_bps: Option<Decimal>,
    pub ws_crossed: bool,
}

impl BookComparison {
    /// Compare WS vs REST top-of-book AND the VWAP to take `vwap_size` base units from
    /// each (a deep-book check). Pass `vwap_size = 0` to skip the VWAP comparison.
    pub fn compute(ws: &OrderBook, rest: &OrderBook, vwap_size: Decimal) -> Self {
        let ws_mid = ws.mid();
        let rest_mid = rest.mid();
        let mid_diff_bps = match (ws_mid, rest_mid) {
            (Some(w), Some(r)) if r > Decimal::ZERO => Some((w - r).abs() / r * Decimal::from(10_000)),
            _ => None,
        };
        let vwap_diff_bps = vwap_divergence_bps(ws, rest, vwap_size, rest_mid);
        BookComparison {
            ws_mid,
            rest_mid,
            mid_diff_bps,
            vwap_diff_bps,
            ws_crossed: ws.is_crossed(),
        }
    }
}

/// Worst-side VWAP divergence (bps vs `rest_mid`) to take `size` base units from each
/// book. Only a side where BOTH books fill `size` without exhausting captured depth
/// contributes (via the strict [`vwap_take`]); the larger of the two sides is returned.
/// `None` if neither side qualifies, `size <= 0`, or `rest_mid` is absent.
fn vwap_divergence_bps(
    ws: &OrderBook,
    rest: &OrderBook,
    size: Decimal,
    rest_mid: Option<Decimal>,
) -> Option<Decimal> {
    let rmid = rest_mid?;
    if size <= Decimal::ZERO || rmid <= Decimal::ZERO {
        return None;
    }
    let ten_k = Decimal::from(10_000);
    let side_diff = |side: Side| -> Option<Decimal> {
        let w = vwap_take(ws, side, size)?;
        let r = vwap_take(rest, side, size)?;
        Some((w.vwap - r.vwap).abs() / rmid * ten_k)
    };
    match (side_diff(Side::Buy), side_diff(Side::Sell)) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) | (None, Some(a)) => Some(a),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn book(bid: Decimal, ask: Decimal) -> OrderBook {
        OrderBook::from_levels(vec![(bid, dec!(1))], vec![(ask, dec!(1))], ts(), ts())
    }

    #[test]
    fn agreeing_books_have_near_zero_diff() {
        let ws = book(dec!(100.0), dec!(100.2));
        let rest = book(dec!(100.0), dec!(100.2));
        let c = BookComparison::compute(&ws, &rest, dec!(1));
        assert_eq!(c.mid_diff_bps, Some(dec!(0)));
        assert_eq!(c.vwap_diff_bps, Some(dec!(0)));
        assert!(!c.ws_crossed);
    }

    #[test]
    fn diverging_mid_reports_bps() {
        // ws mid 100, rest mid 101 => ~99 bps on both the mid and the VWAP probe.
        let ws = book(dec!(99.9), dec!(100.1));
        let rest = book(dec!(100.9), dec!(101.1));
        let c = BookComparison::compute(&ws, &rest, dec!(1));
        let bps = c.mid_diff_bps.unwrap();
        assert!((bps - dec!(99.0099)).abs() < dec!(0.01), "bps {bps}");
        let vbps = c.vwap_diff_bps.unwrap();
        assert!((vbps - dec!(99.0099)).abs() < dec!(0.01), "vwap_bps {vbps}");
    }

    #[test]
    fn deep_book_divergence_caught_when_top_agrees() {
        // Identical top of book, but REST has a thick second ask and WS a thin one: a
        // probe larger than the top level reveals the hedge-depth divergence the mid misses.
        let ws = OrderBook::from_levels(
            vec![(dec!(100.0), dec!(5))],
            vec![(dec!(100.2), dec!(1)), (dec!(105.0), dec!(50))],
            ts(),
            ts(),
        );
        let rest = OrderBook::from_levels(
            vec![(dec!(100.0), dec!(5))],
            vec![(dec!(100.2), dec!(1)), (dec!(100.3), dec!(50))],
            ts(),
            ts(),
        );
        let c = BookComparison::compute(&ws, &rest, dec!(10));
        // Top-of-book mid agrees exactly...
        assert_eq!(c.mid_diff_bps, Some(dec!(0)));
        // ...but the buy-side VWAP to fill 10 differs a lot (WS reaches 105, REST 100.3).
        assert!(c.vwap_diff_bps.unwrap() > dec!(100), "vwap_bps {:?}", c.vwap_diff_bps);
    }

    #[test]
    fn detects_crossed_ws_book() {
        let ws = OrderBook::from_levels(vec![(dec!(101), dec!(1))], vec![(dec!(100), dec!(1))], ts(), ts());
        let rest = book(dec!(100.0), dec!(100.2));
        let c = BookComparison::compute(&ws, &rest, dec!(1));
        assert!(c.ws_crossed);
    }
}

//! One-shot REST fetch of market specifications: Aster `exchangeInfo` (tick /
//! step / min-qty / min-notional) and the hedge venue's metadata (Lighter `orderBooks`,
//! Hyperliquid `meta`), combined into `MarketSpec`s.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::config::{HedgeVenue, LiveCfg, MarketCfg};
use crate::decimal::parse_dec;
use crate::markets::MarketSpec;

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("building http client")
}

/// One symbol's order filters from Aster `exchangeInfo`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AsterFilters {
    pub tick: Decimal,
    pub step: Decimal,
    pub min_qty: Decimal,
    pub min_notional: Decimal,
    /// PERCENT_PRICE `(multiplierDown, multiplierUp)`: buys at most mark × up, sells at least
    /// mark × down.
    pub percent_price: Option<(Decimal, Decimal)>,
}

/// Map of Aster symbol -> filters.
pub async fn fetch_aster_exchange_info_from_base(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<HashMap<String, AsterFilters>> {
    let url = endpoint(base_url, "/fapi/v3/exchangeInfo");
    let body = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET Aster exchangeInfo from {base_url}"))?
        .error_for_status()?
        .text()
        .await?;
    parse_aster_exchange_info(&body)
}

/// The filters of every symbol in an `exchangeInfo` body that has a tick and a step.
pub fn parse_aster_exchange_info(body: &str) -> Result<HashMap<String, AsterFilters>> {
    let info: ExchangeInfo = serde_json::from_str(body).context("parsing Aster exchangeInfo")?;
    let mut out = HashMap::new();
    for s in info.symbols {
        let mut tick = None;
        let mut step = None;
        let mut min_qty = None;
        let mut min_notional = None;
        let mut percent_price = None;
        for f in &s.filters {
            match f.get("filterType").and_then(|v| v.as_str()) {
                Some("PRICE_FILTER") => tick = field_dec(f, "tickSize"),
                Some("LOT_SIZE") => {
                    step = field_dec(f, "stepSize");
                    min_qty = field_dec(f, "minQty");
                }
                Some("MIN_NOTIONAL") => min_notional = field_dec(f, "notional"),
                Some("PERCENT_PRICE") => percent_price = field_dec(f, "multiplierDown").zip(field_dec(f, "multiplierUp")),
                _ => {}
            }
        }
        if let (Some(tick), Some(step)) = (tick, step) {
            let (min_qty, min_notional) = (min_qty.unwrap_or(step), min_notional.unwrap_or(Decimal::from(5)));
            out.insert(s.symbol, AsterFilters { tick, step, min_qty, min_notional, percent_price });
        }
    }
    Ok(out)
}

fn field_dec(f: &serde_json::Value, key: &str) -> Option<Decimal> {
    f.get(key).and_then(|v| v.as_str()).and_then(|s| parse_dec(s).ok())
}

#[derive(Debug, Clone)]
pub struct LighterMarketMeta {
    pub market_id: u32,
    pub symbol: String,
    pub size_decimals: u32,
    pub price_decimals: u32,
    pub min_quote_amount: Decimal,
}

/// Map of Lighter symbol -> market metadata.
pub async fn fetch_lighter_meta_from_base(client: &reqwest::Client, base_url: &str) -> Result<HashMap<String, LighterMarketMeta>> {
    let url = endpoint(base_url, "/api/v1/orderBooks");
    let resp: crate::lighter::messages::OrderBooksResponse = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET Lighter orderBooks from {base_url}"))?
        .error_for_status()?
        .json()
        .await
        .context("parsing Lighter orderBooks")?;
    let mut out = HashMap::new();
    for b in resp.order_books {
        let min_quote_amount = parse_dec(&b.min_quote_amount).unwrap_or(Decimal::ZERO);
        out.insert(
            b.symbol.to_ascii_uppercase(),
            LighterMarketMeta {
                market_id: b.market_id,
                symbol: b.symbol,
                size_decimals: b.supported_size_decimals,
                price_decimals: b.supported_price_decimals,
                min_quote_amount,
            },
        );
    }
    Ok(out)
}

/// Buffered Hyperliquid opening/partial-close minimum ($10 at the limit price): the extra
/// $0.50 covers a sell IOC below the mark. Full reduce-only closes are exempt from the minimum.
pub(crate) const HYPERLIQUID_MIN_NOTIONAL: Decimal = rust_decimal_macros::dec!(10.5);

/// Resolve `MarketSpec`s for the configured markets from the venues' REST base URLs; a hedge
/// venue is read only if a market hedges there.
pub async fn build_market_specs(markets: &[MarketCfg], live: &LiveCfg) -> Result<Vec<MarketSpec>> {
    let client = client()?;
    let aster = fetch_aster_exchange_info_from_base(&client, &live.aster.base_url).await?;
    let uses = |venue| markets.iter().any(|m| m.hedge_venue == venue);
    let lighter = if uses(HedgeVenue::Lighter) { fetch_lighter_meta_from_base(&client, &live.lighter.base_url).await? } else { HashMap::new() };
    let hyperliquid = if uses(HedgeVenue::Hyperliquid) {
        crate::hyperliquid::client::info(&client, &live.hyperliquid.base_url, serde_json::json!({"type": "meta"})).await?
    } else {
        serde_json::Value::Null
    };

    let mut specs = Vec::new();
    for m in markets {
        let AsterFilters { tick, step, min_qty, min_notional, .. } = aster
            .get(&m.aster_symbol)
            .copied()
            .ok_or_else(|| anyhow!("Aster symbol {} not found in exchangeInfo", m.aster_symbol))?;
        let (hl_coin, lm, size_decimals, hedge_min_notional) = match m.hedge_venue {
            HedgeVenue::Lighter => {
                let lm = lighter
                    .get(&m.hl_coin.to_ascii_uppercase())
                    .ok_or_else(|| anyhow!("Lighter symbol {} not found in orderBooks", m.hl_coin))?;
                let min = if lm.min_quote_amount > Decimal::ZERO { lm.min_quote_amount } else { live.partials.lighter_min_notional };
                (lm.symbol.clone(), Some(lm), lm.size_decimals, min)
            }
            HedgeVenue::Hyperliquid => {
                let asset = crate::hyperliquid::client::asset_in(&hyperliquid, &m.hl_coin)?;
                (asset.coin, None, asset.sz_decimals, HYPERLIQUID_MIN_NOTIONAL)
            }
        };
        specs.push(MarketSpec {
            market_id: m.id(),
            aster_symbol: m.aster_symbol.clone(),
            hl_coin,
            hedge: m.hedge_venue,
            lighter_market_id: lm.map_or(0, |l| l.market_id),
            lighter_price_decimals: lm.map_or(0, |l| l.price_decimals),
            lighter_size_decimals: lm.map_or(0, |l| l.size_decimals),
            lighter_price_tick: lm.map_or(Decimal::ZERO, |l| Decimal::new(1, l.price_decimals)),
            tick,
            step,
            aster_min_qty: min_qty,
            aster_min_notional: min_notional,
            hl_sz_decimals: size_decimals as i32,
            hl_qty_step: Decimal::new(1, size_decimals),
            hl_min_notional: hedge_min_notional,
        });
    }
    Ok(specs)
}

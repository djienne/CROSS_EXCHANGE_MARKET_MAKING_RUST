use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::config::HedgeVenue;
use crate::taker::config::MarketCfg;
use crate::taker::decimal::parse_dec;
use crate::taker::markets::MarketSpec;

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("building specs http client")
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

/// The markets' specs. A market hedged on Hyperliquid needs `hyperliquid_base_url`: the commands
/// that trade Lighter only pass none, and are refused it.
pub async fn build_market_specs(
    markets: &[MarketCfg],
    aster_base_url: &str,
    lighter_base_url: &str,
    hyperliquid_base_url: Option<&str>,
) -> Result<Vec<MarketSpec>> {
    let client = client()?;
    let aster = fetch_aster_exchange_info(&client, aster_base_url).await?;
    let needs_lighter_rest = markets.iter().any(|m| m.hedge_venue == HedgeVenue::Lighter && manual_lighter_meta(m).is_none());
    let lighter = if needs_lighter_rest {
        fetch_lighter_meta(&client, lighter_base_url).await?
    } else {
        HashMap::new()
    };

    let mut specs = Vec::new();
    for m in markets {
        let symbol = m.aster_symbol.to_ascii_uppercase();
        let (tick, step, min_qty, min_notional) = aster
            .get(&symbol)
            .copied()
            .ok_or_else(|| anyhow!("Aster symbol {} not found in exchangeInfo", m.aster_symbol))?;
        let lm = match (m.hedge_venue, hyperliquid_base_url) {
            (HedgeVenue::Lighter, _) => manual_lighter_meta(m)
                .or_else(|| lighter.get(&m.lighter_symbol.to_ascii_uppercase()).cloned())
                .ok_or_else(|| {
                    anyhow!(
                        "Lighter symbol {} not configured and not found in orderBooks",
                        m.lighter_symbol
                    )
                })?,
            (HedgeVenue::Hyperliquid, Some(base)) => hyperliquid_meta(&client, base, &m.lighter_symbol).await?,
            (HedgeVenue::Hyperliquid, None) => bail!("{} hedges on Hyperliquid, which this command does not trade", m.id()),
        };
        specs.push(MarketSpec {
            market_id: m.id(),
            aster_symbol: symbol,
            lighter_symbol: lm.symbol,
            lighter_market_id: lm.market_id,
            lighter_price_decimals: lm.price_decimals,
            lighter_size_decimals: lm.size_decimals,
            lighter_price_tick: Decimal::new(1, lm.price_decimals),
            tick,
            step,
            aster_min_qty: min_qty,
            aster_min_notional: min_notional,
            lighter_qty_step: Decimal::new(1, lm.size_decimals),
            lighter_min_notional: lm.min_quote_amount,
            hedge: m.hedge_venue,
        });
    }
    Ok(specs)
}

/// A Hyperliquid coin in the Lighter meta's shape: prices carry at most 6 - szDecimals decimals
/// (and 5 significant figures, which the client rounds to).
async fn hyperliquid_meta(client: &reqwest::Client, base_url: &str, coin: &str) -> Result<LighterMarketMeta> {
    let meta = crate::hyperliquid::client::info(client, base_url, serde_json::json!({"type": "meta"})).await?;
    let asset = crate::hyperliquid::client::asset_in(&meta, coin)?;
    Ok(LighterMarketMeta {
        market_id: asset.index,
        symbol: asset.coin,
        size_decimals: asset.sz_decimals,
        price_decimals: 6u32.saturating_sub(asset.sz_decimals),
        min_quote_amount: crate::connectors::rest_specs::HYPERLIQUID_MIN_NOTIONAL,
    })
}

fn manual_lighter_meta(m: &MarketCfg) -> Option<LighterMarketMeta> {
    Some(LighterMarketMeta {
        market_id: m.lighter_market_index?,
        symbol: m.lighter_symbol.clone(),
        size_decimals: m.lighter_size_decimals?,
        price_decimals: m.lighter_price_decimals?,
        min_quote_amount: m.lighter_min_notional?,
    })
}

async fn fetch_aster_exchange_info(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<HashMap<String, (Decimal, Decimal, Decimal, Decimal)>> {
    let url = endpoint(base_url, "/fapi/v3/exchangeInfo");
    let info: ExchangeInfo = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET Aster exchangeInfo from {base_url}"))?
        .error_for_status()?
        .json()
        .await
        .context("parse Aster exchangeInfo")?;
    let mut out = HashMap::new();
    for s in info.symbols {
        let mut tick = None;
        let mut step = None;
        let mut min_qty = None;
        let mut min_notional = None;
        for f in &s.filters {
            match f.get("filterType").and_then(|v| v.as_str()) {
                Some("PRICE_FILTER") => tick = field_dec(f, "tickSize"),
                Some("LOT_SIZE") => {
                    step = field_dec(f, "stepSize");
                    min_qty = field_dec(f, "minQty");
                }
                Some("MIN_NOTIONAL") => min_notional = field_dec(f, "notional"),
                _ => {}
            }
        }
        if let (Some(tick), Some(step)) = (tick, step) {
            out.insert(
                s.symbol.to_ascii_uppercase(),
                (
                    tick,
                    step,
                    min_qty.unwrap_or(step),
                    min_notional.unwrap_or(Decimal::from(5)),
                ),
            );
        }
    }
    Ok(out)
}

fn field_dec(f: &serde_json::Value, key: &str) -> Option<Decimal> {
    f.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| parse_dec(s).ok())
}

#[derive(Debug, Clone)]
struct LighterMarketMeta {
    market_id: u32,
    symbol: String,
    size_decimals: u32,
    price_decimals: u32,
    min_quote_amount: Decimal,
}

async fn fetch_lighter_meta(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<HashMap<String, LighterMarketMeta>> {
    let url = endpoint(base_url, "/api/v1/orderBooks");
    let resp: crate::lighter::messages::OrderBooksResponse = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET Lighter orderBooks from {base_url}"))?
        .error_for_status()?
        .json()
        .await
        .context("parse Lighter orderBooks")?;
    let mut out = HashMap::new();
    for b in resp.order_books {
        let min_quote_amount = parse_dec(&b.min_quote_amount).unwrap_or(Decimal::from(10));
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

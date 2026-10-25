//! The taker's market specs, from the shared fetchers (`crate::connectors::rest_specs`).

use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use rust_decimal::Decimal;

use crate::config::{FirstVenue, HedgeVenue};
use crate::connectors::rest_specs::{self as shared, AsterFilters, LighterMarketMeta};
use crate::taker::config::MarketCfg;
use crate::taker::markets::MarketSpec;

/// The markets' specs. A market hedged on Hyperliquid needs `hyperliquid_base_url`: the commands
/// that trade Lighter only pass none, and are refused it.
pub async fn build_market_specs(
    markets: &[MarketCfg],
    aster_base_url: &str,
    lighter_base_url: &str,
    hyperliquid_base_url: Option<&str>,
) -> Result<Vec<MarketSpec>> {
    let client = shared::client()?;
    let aster: HashMap<String, AsterFilters> = if markets.iter().any(|m| m.first_venue == FirstVenue::Aster) {
        shared::fetch_aster_exchange_info_from_base(&client, aster_base_url).await?.into_iter().map(|(s, f)| (s.to_ascii_uppercase(), f)).collect()
    } else {
        HashMap::new()
    };
    let needs_lighter_rest = markets.iter().any(|m| {
        (m.hedge_venue == HedgeVenue::Lighter || m.first_venue == FirstVenue::Lighter) && manual_lighter_meta(m).is_none()
    });
    // An unreadable or zero Lighter minimum is $10 here (the shared fetch leaves 0).
    let lighter = if needs_lighter_rest {
        let mut meta = shared::fetch_lighter_meta_from_base(&client, lighter_base_url).await?;
        meta.values_mut().filter(|m| m.min_quote_amount <= Decimal::ZERO).for_each(|m| m.min_quote_amount = Decimal::from(10));
        meta
    } else {
        HashMap::new()
    };

    let mut specs = Vec::new();
    for m in markets {
        let lighter_meta = || {
            manual_lighter_meta(m)
                .or_else(|| lighter.get(&m.lighter_symbol.to_ascii_uppercase()).cloned())
                .ok_or_else(|| anyhow!("Lighter symbol {} not configured and not found in orderBooks", m.lighter_symbol))
        };
        // Lighter's minimum size is one size step, as its hedges assume.
        let (symbol, (tick, step, min_qty, min_notional), first_market_index) = match m.first_venue {
            FirstVenue::Aster => {
                let symbol = m.aster_symbol.to_ascii_uppercase();
                let f = aster
                    .get(&symbol)
                    .copied()
                    .ok_or_else(|| anyhow!("Aster symbol {} not found in exchangeInfo", m.aster_symbol))?;
                (symbol, (f.tick, f.step, f.min_qty, f.min_notional), 0)
            }
            FirstVenue::Lighter => {
                let lm = lighter_meta()?;
                let step = Decimal::new(1, lm.size_decimals);
                (lm.symbol, (Decimal::new(1, lm.price_decimals), step, step, lm.min_quote_amount), lm.market_id)
            }
        };
        let lm = match (m.hedge_venue, hyperliquid_base_url) {
            (HedgeVenue::Lighter, _) => lighter_meta()?,
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
            first: m.first_venue,
            first_market_index,
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
        min_quote_amount: shared::HYPERLIQUID_MIN_NOTIONAL,
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

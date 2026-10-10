//! The pairs to watch: perps listed on two of Aster, Lighter and Hyperliquid, matched by name,
//! confirmed by price and filtered by 24 h volume.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::Collect;

pub const ASTER_REST: &str = "https://fapi.asterdex.com";
pub const LIGHTER_REST: &str = "https://mainnet.zklighter.elliot.ai";
pub const HYPERLIQUID_INFO: &str = "https://api.hyperliquid.xyz/info";
/// A name match counts only if the prices agree this closely (Aster `BB` and Lighter `BB` differ
/// ~7.9M-fold: different assets).
const MAX_PRICE_MISMATCH: f64 = 0.02;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue { Aster, Lighter, Hyperliquid }

impl Venue {
    pub fn code(self) -> &'static str {
        match self { Self::Aster => "A", Self::Lighter => "L", Self::Hyperliquid => "H" }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Market {
    pub venue: Venue,
    pub symbol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtypes: Vec<String>,
    pub volume_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pair {
    /// Venue-qualified key; also used by summaries and the gate history.
    pub name: String,
    pub left: Market,
    pub right: Market,
    /// Right price = scale * left price; divide prices and multiply sizes on the right.
    pub scale: f64,
}

/// Old files used unqualified Lighter symbols. There is no HIP-3 namespace in this screener.
pub fn qualified(name: &str) -> String {
    if name.contains(':') { name.to_string() } else { format!("A-L:{name}") }
}

/// Only the reader knows the old schema. Existing compressed files are never rewritten.
pub fn read_pairs(text: &str) -> Result<Vec<Pair>> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Stored {
        Current(Pair),
        Legacy { name: String, aster: String, lighter_id: u32, scale: f64, aster_subtypes: Vec<String>, aster_volume_usd: f64, lighter_volume_usd: f64 },
    }
    serde_json::from_str::<Vec<Stored>>(text)?.into_iter().map(|p| {
        let p = match p {
            Stored::Current(p) => p,
            Stored::Legacy { name, aster, lighter_id, scale, aster_subtypes, aster_volume_usd, lighter_volume_usd } => Pair {
                name: qualified(&name), scale,
                left: Market { venue: Venue::Aster, symbol: aster, id: None, subtypes: aster_subtypes, volume_usd: aster_volume_usd },
                right: Market { venue: Venue::Lighter, symbol: name, id: Some(lighter_id), subtypes: vec![], volume_usd: lighter_volume_usd },
            },
        };
        ensure!(p.scale.is_finite() && p.scale > 0.0 && p.left.venue != p.right.venue, "invalid pair {}", p.name);
        Ok(p)
    }).collect()
}

#[derive(Debug, Clone)]
pub struct AsterListing {
    pub symbol: String,
    pub price: f64,
    pub volume_usd: f64,
    pub subtypes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LighterListing {
    pub symbol: String,
    pub id: u32,
    pub price: f64,
    pub volume_usd: f64,
}

/// The pairs (most liquid first, at most `max_pairs`), and the name matches dropped by the price
/// check with their price ratio.
pub fn match_pairs(aster: &[AsterListing], lighter: &[LighterListing], min_volume_usd: f64, max_pairs: usize) -> (Vec<Pair>, Vec<(String, f64)>) {
    let by_symbol: HashMap<&str, &AsterListing> = aster.iter().map(|a| (a.symbol.as_str(), a)).collect();
    let (mut pairs, mut mismatched) = (Vec::new(), Vec::new());
    for l in lighter {
        let s = &l.symbol;
        let candidates = [
            Some((format!("{s}USDT"), 1.0)),
            s.ends_with("USD").then(|| (format!("{s}T"), 1.0)),
            s.strip_prefix("1000").map(|base| (format!("{base}USDT"), 1000.0)),
        ];
        let Some((a, scale)) = candidates.into_iter().flatten().find_map(|(sym, scale)| by_symbol.get(sym.as_str()).map(|a| (*a, scale))) else {
            continue;
        };
        let ratio = l.price / (a.price * scale);
        if !ratio.is_finite() || (ratio - 1.0).abs() > MAX_PRICE_MISMATCH {
            mismatched.push((s.clone(), ratio));
            continue;
        }
        if a.volume_usd.min(l.volume_usd) >= min_volume_usd {
            pairs.push(Pair {
                name: qualified(s),
                left: Market { venue: Venue::Aster, symbol: a.symbol.clone(), id: None, subtypes: a.subtypes.clone(), volume_usd: a.volume_usd },
                right: Market { venue: Venue::Lighter, symbol: s.clone(), id: Some(l.id), subtypes: vec![], volume_usd: l.volume_usd },
                scale,
            });
        }
    }
    pairs.sort_by(|x, y| liquidity(y).total_cmp(&liquidity(x)));
    pairs.truncate(max_pairs);
    (pairs, mismatched)
}

fn liquidity(p: &Pair) -> f64 { p.left.volume_usd.min(p.right.volume_usd) }

/// Explicit contract aliases, confirmed by the same price check as exact names. Never strip an
/// arbitrary leading k: e.g. KAITO is not a thousand AITO. Native HL has these six k contracts.
fn units(symbol: &str) -> (&str, f64) {
    match symbol {
        "kPEPE" | "kSHIB" | "kBONK" | "kLUNC" | "kFLOKI" | "kNEIRO" => (&symbol[1..], 1000.0),
        _ => symbol.strip_prefix("1000").map_or((symbol, 1.0), |s| (s, 1000.0)),
    }
}

fn match_hyperliquid(markets: &[(Market, f64)], hl: &[(Market, f64)], cfg: &Collect) -> (Vec<Pair>, Vec<(String, f64)>) {
    let mut pairs = Vec::new();
    let mut mismatched = Vec::new();
    for (h, price) in hl {
        let (base, count) = units(&h.symbol);
        let mut matches = Vec::new();
        for (m, other) in markets {
            let symbol = if m.venue == Venue::Aster { m.symbol.strip_suffix("USDT").unwrap_or(&m.symbol) } else { &m.symbol };
            let (other_base, other_count) = units(symbol);
            if base != other_base { continue; }
            let scale = count / other_count;
            let ratio = price / (other * scale);
            let name = format!("{}-H:{}", m.venue.code(), h.symbol);
            if !ratio.is_finite() || (ratio - 1.0).abs() > MAX_PRICE_MISMATCH {
                mismatched.push((name, ratio));
            } else if m.volume_usd.min(h.volume_usd) >= cfg.min_volume_usd {
                matches.push(Pair { name, left: m.clone(), right: h.clone(), scale });
            }
        }
        // Ambiguous listings must not overwrite the same gate key.
        if matches.len() == 1 { pairs.push(matches.pop().unwrap()); }
    }
    pairs.sort_by(|x, y| liquidity(y).total_cmp(&liquidity(x)));
    pairs.truncate(cfg.max_pairs);
    (pairs, mismatched)
}

pub fn hyperliquid_listings(text: &str) -> Result<Vec<(Market, f64)>> {
    #[derive(Deserialize)]
    struct Meta { universe: Vec<Asset> }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Asset { name: String, #[serde(default)] is_delisted: bool }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Ctx { mid_px: Option<String>, mark_px: String, day_ntl_vlm: String }
    let (meta, contexts): (Meta, Vec<Ctx>) = serde_json::from_str(text)?;
    ensure!(meta.universe.len() == contexts.len(), "Hyperliquid metadata/context lengths differ");
    Ok(meta.universe.into_iter().zip(contexts).filter_map(|(m, c)| {
        if m.is_delisted || m.name.contains(':') { return None; }
        let price: f64 = c.mid_px.as_ref().unwrap_or(&c.mark_px).parse().ok()?;
        let volume_usd: f64 = c.day_ntl_vlm.parse().ok()?;
        if !(price.is_finite() && price > 0.0 && volume_usd.is_finite() && volume_usd >= 0.0) { return None; }
        Some((Market { venue: Venue::Hyperliquid, symbol: m.name, id: None, subtypes: vec![], volume_usd }, price))
    }).collect())
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<AsterSymbol>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AsterSymbol {
    symbol: String,
    status: String,
    contract_type: String,
    quote_asset: String,
    #[serde(default)]
    underlying_sub_type: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AsterTicker {
    symbol: String,
    last_price: String,
    quote_volume: String,
}

#[derive(Deserialize)]
struct LighterDetails {
    order_book_details: Vec<LighterMarket>,
}

#[derive(Deserialize)]
struct LighterMarket {
    symbol: String,
    market_id: u32,
    status: String,
    market_type: String,
    last_trade_price: f64,
    daily_quote_token_volume: f64,
}

/// Today's pairs, from Aster exchangeInfo and 24 h tickers, Lighter orderBookDetails and
/// Hyperliquid metaAndAssetCtxs.
pub async fn discover(cfg: &Collect) -> Result<(Vec<Pair>, Vec<(String, f64)>)> {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let get = |url: String| {
        let request = http.get(url);
        async move { anyhow::Ok(request.send().await?.error_for_status()?.text().await?) }
    };
    let aster = async {
        let info: ExchangeInfo = serde_json::from_str(&get(format!("{ASTER_REST}/fapi/v3/exchangeInfo")).await?).context("Aster exchangeInfo")?;
        let tickers: Vec<AsterTicker> = serde_json::from_str(&get(format!("{ASTER_REST}/fapi/v3/ticker/24hr")).await?).context("Aster ticker/24hr")?;
        let tickers: HashMap<String, AsterTicker> = tickers.into_iter().map(|t| (t.symbol.clone(), t)).collect();
        anyhow::Ok(info.symbols.into_iter()
            .filter(|s| s.status == "TRADING" && s.contract_type == "PERPETUAL" && s.quote_asset == "USDT")
            .filter_map(|s| {
                let t = tickers.get(&s.symbol)?;
                let price: f64 = t.last_price.parse().ok()?;
                let volume_usd: f64 = t.quote_volume.parse().ok()?;
                (price.is_finite() && price > 0.0 && volume_usd.is_finite() && volume_usd >= 0.0)
                    .then_some(AsterListing { price, volume_usd, symbol: s.symbol, subtypes: s.underlying_sub_type })
            }).collect::<Vec<_>>())
    };
    let lighter = async {
        let details: LighterDetails = serde_json::from_str(&get(format!("{LIGHTER_REST}/api/v1/orderBookDetails")).await?).context("Lighter orderBookDetails")?;
        anyhow::Ok(details.order_book_details.into_iter()
            .filter(|m| m.status == "active" && m.market_type == "perp" && m.last_trade_price.is_finite() && m.last_trade_price > 0.0 && m.daily_quote_token_volume.is_finite() && m.daily_quote_token_volume >= 0.0)
            .map(|m| LighterListing { symbol: m.symbol, id: m.market_id, price: m.last_trade_price, volume_usd: m.daily_quote_token_volume }).collect::<Vec<_>>())
    };
    let hl = async {
        let text = http.post(HYPERLIQUID_INFO).header("Content-Type", "application/json")
            .body(r#"{"type":"metaAndAssetCtxs"}"#).send().await?.error_for_status()?.text().await?;
        hyperliquid_listings(&text)
    };
    // One unavailable venue cannot hold up routes between the other two. Missing venues retry
    // at the next UTC discovery; a total outage takes the collector's existing retry path.
    let (aster, lighter, hl) = tokio::join!(aster, lighter, hl);
    let aster = aster.unwrap_or_else(|e| { tracing::warn!("Aster discovery unavailable: {e:#}"); Vec::new() });
    let lighter = lighter.unwrap_or_else(|e| { tracing::warn!("Lighter discovery unavailable: {e:#}"); Vec::new() });
    let (mut pairs, mut mismatched) = match_pairs(&aster, &lighter, cfg.min_volume_usd, cfg.max_pairs);
    match hl {
        Ok(hl) => {
            let a: Vec<_> = aster.iter().map(|m| (Market { venue: Venue::Aster, symbol: m.symbol.clone(), id: None, subtypes: m.subtypes.clone(), volume_usd: m.volume_usd }, m.price)).collect();
            let l: Vec<_> = lighter.iter().map(|m| (Market { venue: Venue::Lighter, symbol: m.symbol.clone(), id: Some(m.id), subtypes: vec![], volume_usd: m.volume_usd }, m.price)).collect();
            for markets in [&a, &l] {
                let (p, m) = match_hyperliquid(markets, &hl, cfg);
                pairs.extend(p); mismatched.extend(m);
            }
        }
        Err(e) => tracing::warn!("Hyperliquid discovery unavailable; collecting Aster/Lighter only: {e:#}"),
    }
    ensure!(!pairs.is_empty(), "no eligible pairs on available venues");
    Ok((pairs, mismatched))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aster(symbol: &str, price: f64, volume_usd: f64) -> AsterListing {
        AsterListing { symbol: symbol.into(), price, volume_usd, subtypes: vec![] }
    }

    fn lighter(symbol: &str, id: u32, price: f64, volume_usd: f64) -> LighterListing {
        LighterListing { symbol: symbol.into(), id, price, volume_usd }
    }

    #[test]
    fn names_match_by_rule_prices_confirm_and_volume_filters() {
        let a = [
            aster("HYPEUSDT", 92.4, 24e6),
            aster("NOTUSDT", 0.0021, 3e6),
            aster("SKHYNIXUSDT", 610.0, 1e6),
            aster("BBUSDT", 0.5, 5e6),
            aster("DOGEUSDT", 0.2, 50_000.0),
        ];
        let l = [
            lighter("HYPE", 24, 92.41, 45e6),
            lighter("1000NOT", 7, 2.1, 2e6),
            lighter("SKHYNIXUSD", 90, 612.0, 4e6),
            lighter("BB", 50, 3_950_000.0, 5e6),
            lighter("DOGE", 3, 0.2, 9e6),
            lighter("ZZZ", 99, 1.0, 9e6),
        ];
        let (pairs, mismatched) = match_pairs(&a, &l, 200_000.0, 10);
        let names: Vec<(&str, &str, f64)> = pairs.iter().map(|p| (p.right.symbol.as_str(), p.left.symbol.as_str(), p.scale)).collect();
        // Ordered by the smaller venue volume: HYPE 24M, NOT 2M, SKHYNIX 1M; DOGE's Aster side is thin.
        assert_eq!(names, [("HYPE", "HYPEUSDT", 1.0), ("1000NOT", "NOTUSDT", 1000.0), ("SKHYNIXUSD", "SKHYNIXUSDT", 1.0)]);
        assert_eq!(mismatched, [("BB".to_string(), 7_900_000.0)]);
        assert_eq!(match_pairs(&a, &l, 200_000.0, 1).0.len(), 1);
    }

    #[test]
    fn native_metadata_aliases_units_and_pair_limits() {
        let hl = hyperliquid_listings(r#"[{"universe":[{"name":"kPEPE"},{"name":"BTC"},{"name":"OLD","isDelisted":true},{"name":"xyz:BTC"},{"name":"BAD"}]},[{"midPx":"0.01","markPx":"0.01","dayNtlVlm":"400000"},{"midPx":null,"markPx":"60000","dayNtlVlm":"500000"},{"midPx":"1","markPx":"1","dayNtlVlm":"900000"},{"midPx":"60000","markPx":"60000","dayNtlVlm":"900000"},{"midPx":"NaN","markPx":"1","dayNtlVlm":"900000"}]]"#).unwrap();
        assert_eq!(hl.len(), 2);
        assert!(hyperliquid_listings(r#"[{"universe":[]},[{"midPx":null,"markPx":"1","dayNtlVlm":"0"}]]"#).is_err());
        let market = |venue, symbol: &str, price| (Market { venue, symbol: symbol.into(), id: Some(1), subtypes: vec![], volume_usd: 300_000.0 }, price);
        let a = [market(Venue::Aster, "PEPEUSDT", 0.00001), market(Venue::Aster, "BTCUSDT", 60000.0)];
        let l = [market(Venue::Lighter, "1000PEPE", 0.01), market(Venue::Lighter, "BTC", 6.0)];
        let cfg = Collect { min_volume_usd: 200_000.0, max_pairs: 100 };
        let (ap, _) = match_hyperliquid(&a, &hl, &cfg);
        let (lp, skipped) = match_hyperliquid(&l, &hl, &cfg);
        assert_eq!((ap.len(), lp.len(), skipped.len()), (2, 1, 1));
        let pepe = ap.iter().find(|p| p.name == "A-H:kPEPE").unwrap();
        assert_eq!(pepe.scale, 1000.0);
        assert_eq!(lp[0].scale, 1.0);
        assert_eq!((0.01 / pepe.scale) * (7.0 * pepe.scale), 0.01 * 7.0);
        assert_eq!(match_hyperliquid(&a, &hl, &Collect { max_pairs: 1, ..cfg }).0.len(), 1);
    }
}

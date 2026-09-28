use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::taker::aster::sign::{AsterNonce, AsterSigner};
use crate::taker::decimal::{ceil_to_step, floor_to_step, trim_dec};
use crate::taker::markets::MarketSpec;
use crate::taker::types::{FeeProvenance, FillSummary, MarketId, Side};

const ASTER_ORDER_PATH: &str = "/fapi/v3/order";
const USER_AGENT: &str = "lighter-aster-taker-arb";


#[derive(Debug, thiserror::Error)]
#[error("Aster request was not sent: {0}")]
struct RequestNotSent(String);

#[derive(Debug, thiserror::Error)]
#[error("Aster HTTP {status}, code {code:?}: {body}")]
struct VenueFailure {
    status: u16,
    code: Option<i64>,
    body: String,
}

/// Only a pre-write failure or explicit, non-ambiguous venue rejection releases exposure.
pub fn definitive_no_fill(error: &anyhow::Error) -> bool {
    error.downcast_ref::<RequestNotSent>().is_some()
        || error.downcast_ref::<reqwest::Error>().is_some_and(|e| e.is_connect())
        || error.downcast_ref::<VenueFailure>()
        .is_some_and(|e| e.status < 500 && e.code.is_some_and(|c| c < 0
            && !matches!(c, -1000 | -1001 | -1006 | -1007)))
}

#[derive(Clone)]
struct MarketWire {
    symbol: String,
    step: Decimal,
    tick: Decimal,
}

#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    Accepted {
        venue_order_id: Option<i64>,
        client_order_id: String,
        raw: String,
    },
    Rejected {
        reason: String,
    },
    Unknown {
        client_order_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterPositionRow {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterBalanceRow {
    pub asset: String,
    #[serde(rename = "crossWalletBalance", default)]
    pub cross_wallet_balance: String,
    #[serde(rename = "crossUnPnl", default)]
    pub cross_un_pnl: String,
}

#[derive(Debug, Clone, Copy)]
pub struct AsterBalanceSnapshot {
    pub available_usd: Decimal,
    pub cross_wallet_balance_usd: Option<Decimal>,
    pub cross_unrealized_pnl_usd: Option<Decimal>,
}

impl AsterBalanceSnapshot {
    pub fn equity_usd(self) -> Option<Decimal> {
        match (self.cross_wallet_balance_usd, self.cross_unrealized_pnl_usd) {
            (Some(wallet), Some(unpnl)) => Some(wallet + unpnl),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AsterOrderResp {
    #[serde(rename = "orderId")]
    order_id: Option<i64>,
    status: Option<String>,
    #[serde(rename = "executedQty")]
    executed_qty: Option<String>,
    #[serde(rename = "cumQty")]
    cum_qty: Option<String>,
    #[serde(rename = "cumQuote")]
    cum_quote: Option<String>,
    #[serde(rename = "avgPrice")]
    avg_price: Option<String>,
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterUserTrade {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    pub price: String,
    pub qty: String,
    #[serde(rename = "quoteQty", default)]
    pub quote_qty: String,
    #[serde(default)]
    pub commission: Option<serde_json::Value>,
    #[serde(rename = "commissionAsset", default)]
    pub commission_asset: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct AsterImmediateFill {
    pub qty: Decimal,
    pub notional: Decimal,
}

pub struct AsterRest {
    client: reqwest::Client,
    base_url: String,
    signer: Arc<dyn AsterSigner>,
    nonce: AsterNonce,
    markets: HashMap<MarketId, MarketWire>,
}

impl AsterRest {
    pub fn new(
        base_url: String,
        signer: Arc<dyn AsterSigner>,
        specs: &[MarketSpec],
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .pool_idle_timeout(Some(Duration::from_secs(120)))
            .pool_max_idle_per_host(4)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()?;
        let markets = specs
            .iter()
            .map(|s| {
                (
                    s.market_id.clone(),
                    MarketWire {
                        symbol: s.aster_symbol.clone(),
                        step: s.step,
                        tick: s.tick,
                    },
                )
            })
            .collect();
        let nonce = AsterNonce::for_signer(&base_url, signer.signer_address())?;
        Ok(AsterRest {
            client,
            base_url,
            signer,
            nonce,
            markets,
        })
    }

    fn wire(&self, market: &MarketId) -> Result<&MarketWire> {
        self.markets
            .get(market)
            .ok_or_else(|| anyhow!("no Aster wire context for {market}"))
    }


    /// A single account-level available margin value; asset projections must not be summed.
    pub async fn account_available_balance(&self) -> Result<Decimal> {
        let body = self.signed_request(Method::GET, "/fapi/v3/account", vec![]).await?;
        let value: serde_json::Value = serde_json::from_str(&body)?;
        value.get("availableBalance").and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Aster account is missing availableBalance"))?
            .parse::<Decimal>().context("malformed Aster availableBalance")
    }

    /// Resolve the same submitted identity on the cold path. An absent/error result is unknown.
    pub async fn query_order(&self, market: &MarketId, client_order_id: &str) -> Result<serde_json::Value> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self.signed_request(Method::GET, ASTER_ORDER_PATH, vec![
            ("symbol".into(), symbol),
            ("origClientOrderId".into(), client_order_id.to_string()),
        ]).await?;
        serde_json::from_str(&body).context("parse Aster order query")
    }

    async fn signed_request(&self, method: Method, path: &str, business: Vec<(String, String)>) -> Result<String> {
        let mut params = business;
        let nonce = self.nonce.next().map_err(|e| RequestNotSent(e.to_string()))?;
        params.push(("nonce".into(), nonce.to_string()));
        params.push(("user".into(), self.signer.user_address().to_string()));
        params.push(("signer".into(), self.signer.signer_address().to_string()));
        // Serialize ONCE: the EIP-712 message is exactly the transmitted form string.
        let mut encoded = reqwest::Url::parse("http://localhost/").expect("static encoding URL");
        encoded.query_pairs_mut().extend_pairs(params.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let unsigned = encoded.query().unwrap_or_default();
        let signature = self.signer.sign_v3(unsigned).map_err(|e| RequestNotSent(e.to_string()))?;
        let payload = format!("{unsigned}&signature={}", signature.0);
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let builder = match method {
            Method::GET => self.client.get(format!("{url}?{payload}")),
            Method::POST => self.client.post(&url).header("Content-Type", "application/x-www-form-urlencoded").body(payload),
            Method::DELETE => self.client.delete(&url).header("Content-Type", "application/x-www-form-urlencoded").body(payload),
            Method::PUT => self.client.put(&url).header("Content-Type", "application/x-www-form-urlencoded").body(payload),
            other => return Err(RequestNotSent(format!("unsupported Aster method {other}")).into()),
        };
        let response = builder.header("User-Agent", USER_AGENT).send().await.map_err(reqwest::Error::without_url)?;
        let status = response.status();
        let body = response.text().await.map_err(reqwest::Error::without_url)?;
        let code = serde_json::from_str::<serde_json::Value>(&body).ok()
            .and_then(|v| v.get("code").and_then(|c| c.as_i64()));
        if !status.is_success() || code.is_some_and(|c| c != 0 && c != 200) {
            return Err(VenueFailure { status: status.as_u16(), code, body }.into());
        }
        Ok(body)
    }

    pub async fn submit_ioc_order(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        price_bound: Decimal,
        reduce_only: bool,
    ) -> SubmitOutcome {
        let mut params = match self.limit_ioc_params(market, side, qty, price_bound, reduce_only) {
            Ok(p) => p,
            Err(e) => {
                return SubmitOutcome::Rejected {
                    reason: e.to_string(),
                }
            }
        };
        let client_order_id = match self.nonce.next() {
            Ok(nonce) => format!("ta-{nonce}"),
            Err(e) => return SubmitOutcome::Rejected { reason: e.to_string() },
        };
        params.push(("newClientOrderId".into(), client_order_id.clone()));
        match self.signed_request(Method::POST, ASTER_ORDER_PATH, params).await {
            Ok(body) => classify_order_response(&client_order_id, &body),
            Err(e) if definitive_no_fill(&e) => SubmitOutcome::Rejected { reason: e.to_string() },
            Err(e) => SubmitOutcome::Unknown { client_order_id, reason: e.to_string() },
        }
    }

    fn limit_ioc_params(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        price_bound: Decimal,
        reduce_only: bool,
    ) -> Result<Vec<(String, String)>> {
        let w = self.wire(market)?;
        let qty = floor_to_step(qty, w.step);
        if qty <= Decimal::ZERO {
            anyhow::bail!("Aster quantity rounds to zero");
        }
        let price = match side {
            Side::Buy => ceil_to_step(price_bound, w.tick),
            Side::Sell => floor_to_step(price_bound, w.tick),
        };
        if price <= Decimal::ZERO {
            anyhow::bail!("Aster price bound rounds to zero");
        }
        let mut p = vec![
            ("symbol".into(), w.symbol.clone()),
            ("side".into(), side.as_str().to_string()),
            ("type".into(), "LIMIT".into()),
            ("timeInForce".into(), "IOC".into()),
            ("newOrderRespType".into(), "RESULT".into()),
            ("quantity".into(), trim_dec(qty)),
            ("price".into(), trim_dec(price)),
            ("positionSide".into(), "BOTH".into()),
        ];
        if reduce_only {
            p.push(("reduceOnly".into(), "true".into()));
        }
        Ok(p)
    }

    pub async fn position_qty(&self, market: &MarketId) -> Result<Decimal> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self
            .signed_request(
                Method::GET,
                "/fapi/v3/positionRisk",
                vec![("symbol".into(), symbol.clone())],
            )
            .await?;
        let rows: Vec<AsterPositionRow> = serde_json::from_str(&body)
            .map_err(|e| anyhow!("parse Aster positionRisk: {e}: {body}"))?;
        position_qty_from_rows(&rows, &symbol)
    }

    pub async fn available_usdc(&self) -> Result<Decimal> {
        Ok(self.balance_snapshot().await?.available_usd)
    }

    pub async fn balance_snapshot(&self) -> Result<AsterBalanceSnapshot> {
        let (body, available_usd) = tokio::try_join!(
            self.signed_request(Method::GET, "/fapi/v3/balance", vec![]),
            self.account_available_balance(),
        )?;
        let rows: Vec<AsterBalanceRow> =
            serde_json::from_str(&body).map_err(|e| anyhow!("parse Aster balance: {e}: {body}"))?;
        // ALL USD-pegged rows, not just USDT/USDC: cross-margin settles funding/PnL per
        // asset, so an account collateralized in USDC can carry a NEGATIVE USDT row
        // (real debt — observed live 2026-07-04).
        let stable_rows: Vec<AsterBalanceRow> = rows
            .into_iter()
            .filter(|r| !r.asset.is_empty() && is_usd_stable_asset(&r.asset))
            .collect();
        if stable_rows.is_empty() {
            return Ok(AsterBalanceSnapshot {
                available_usd: Decimal::ZERO,
                cross_wallet_balance_usd: None,
                cross_unrealized_pnl_usd: None,
            });
        }
        // SIGNED sums across ALL stable rows for the equity terms: the old
        // positive-only `.max()` picked the USDC collateral row and dropped the negative
        // USDT debt row, so `equity_usd()` overstated equity by the debt — and per-asset
        // crossUnPnl on rows other than the single positive one was silently dropped.
        // Incomplete per-asset evidence stays unknown rather than silently dropping debt/PnL.
        let cross_vals: Vec<Decimal> = stable_rows
            .iter()
            .filter_map(|r| parse_optional_dec(&r.cross_wallet_balance))
            .collect();
        let cross_wallet_balance_usd =
            (cross_vals.len() == stable_rows.len()).then(|| cross_vals.into_iter().sum::<Decimal>());
        let unpnl_vals: Vec<Decimal> = stable_rows
            .iter()
            .filter_map(|r| parse_optional_dec(&r.cross_un_pnl))
            .collect();
        let cross_unrealized_pnl_usd =
            (unpnl_vals.len() == stable_rows.len()).then(|| unpnl_vals.into_iter().sum::<Decimal>());
        Ok(AsterBalanceSnapshot {
            available_usd,
            cross_wallet_balance_usd,
            cross_unrealized_pnl_usd,
        })
    }

    /// Only the count is read.
    pub async fn open_orders(&self, market: &MarketId) -> Result<Vec<serde::de::IgnoredAny>> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self
            .signed_request(
                Method::GET,
                "/fapi/v3/openOrders",
                vec![("symbol".into(), symbol)],
            )
            .await
            .context("Aster openOrders")?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse Aster openOrders: {e}: {body}"))
    }

    pub async fn order_trades(
        &self,
        market: &MarketId,
        order_id: i64,
    ) -> Result<Vec<AsterUserTrade>> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self
            .signed_request(
                Method::GET,
                "/fapi/v3/userTrades",
                vec![
                    ("symbol".into(), symbol),
                    ("orderId".into(), order_id.to_string()),
                    ("limit".into(), "1000".into()),
                ],
            )
            .await
            .context("Aster userTrades")?;
        let mut rows: Vec<AsterUserTrade> = serde_json::from_str(&body)
            .map_err(|e| anyhow!("parse Aster userTrades: {e}: {body}"))?;
        rows.retain(|r| r.order_id == order_id);
        Ok(rows)
    }

    pub async fn wait_order_fill_summary(
        &self,
        market: &MarketId,
        order_id: i64,
        expected_qty: Decimal,
        timeout: Duration,
    ) -> Result<FillSummary> {
        let deadline = tokio::time::Instant::now() + timeout;
        let min_expected = expected_qty * Decimal::from(999u32) / Decimal::from(1000u32);
        let mut last = None;
        // Assigned by every match arm below before any read, so no initializer.
        let mut last_err: Option<anyhow::Error>;
        loop {
            // Retry ALL order_trades errors inside the deadline (mirrors
            // wait_post_trade_reconciled): a single transient REST failure used to
            // abort the whole wait and could leave a filled trade unbooked. A
            // persistent error just costs the deadline it already cost.
            match self.order_trades(market, order_id).await.and_then(|trades| summarize_user_trades(&trades)) {
                Ok(summary) => {
                    last_err = None;
                    if let Some(summary) = summary {
                        if summary.qty >= min_expected {
                            return Ok(summary);
                        }
                        last = Some(summary);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Aster userTrades poll failed for orderId={order_id} (retrying within deadline): {e:#}"
                    );
                    last_err = Some(e);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                // Best partial summary beats an error; an error beats nothing.
                if let Some(summary) = last {
                    return Ok(summary);
                }
                if let Some(e) = last_err {
                    return Err(e.context(format!(
                        "Aster userTrades kept failing for orderId={order_id} until the deadline"
                    )));
                }
                anyhow::bail!("no Aster userTrades found for orderId={order_id}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// Position from the positionRisk rows: an ABSENT row means genuinely flat (the venue
/// omits flat symbols) and maps to zero; a PRESENT row with a malformed positionAmt is a
/// parse failure and must surface as an error — reconciliation acting on a fabricated
/// zero would treat a live position as flat.
fn position_qty_from_rows(rows: &[AsterPositionRow], symbol: &str) -> Result<Decimal> {
    match rows.iter().find(|r| r.symbol.eq_ignore_ascii_case(symbol)) {
        None => Ok(Decimal::ZERO),
        Some(row) => row.position_amt.trim().parse::<Decimal>().map_err(|e| {
            anyhow!(
                "malformed Aster positionAmt {:?} for {symbol}: {e}",
                row.position_amt
            )
        }),
    }
}

fn parse_optional_dec(raw: &str) -> Option<Decimal> {
    let s = raw.trim();
    if s.is_empty() {
        None
    } else {
        s.parse::<Decimal>().ok()
    }
}

/// USD-pegged commission assets that need no conversion.
fn is_usd_stable_asset(asset: &str) -> bool {
    matches!(
        asset.to_ascii_uppercase().as_str(),
        "USD" | "USDT" | "USDC" | "BUSD" | "USDF" | "FDUSD" | "DAI"
    )
}

fn summarize_user_trades(rows: &[AsterUserTrade]) -> Result<Option<FillSummary>> {
    let mut qty = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut known_fee = true;
    for row in rows {
        let q = row.qty.parse::<Decimal>().context("malformed Aster trade quantity")?;
        let px = row.price.parse::<Decimal>().context("malformed Aster trade price")?;
        if q <= Decimal::ZERO || px <= Decimal::ZERO { anyhow::bail!("nonpositive Aster trade quantity/price"); }
        let quote = if row.quote_qty.is_empty() { q * px } else {
            row.quote_qty.parse::<Decimal>().context("malformed Aster trade quote quantity")?
        };
        if quote <= Decimal::ZERO { anyhow::bail!("nonpositive Aster trade quote quantity"); }
        qty += q;
        notional += quote;
        let commission = row.commission.as_ref().and_then(|v| match v {
            serde_json::Value::String(s) => s.parse::<Decimal>().ok(),
            serde_json::Value::Number(n) => n.to_string().parse::<Decimal>().ok(),
            _ => None,
        });
        match commission {
            Some(value) if value.is_zero() || row.commission_asset.as_deref().is_some_and(is_usd_stable_asset) => fee += value,
            _ => known_fee = false,
        }
    }
    Ok(FillSummary::from_qty_notional(qty, notional, fee).map(|mut summary| {
        summary.fee_provenance = if known_fee { FeeProvenance::Venue } else { FeeProvenance::Unknown };
        summary
    }))
}

pub fn immediate_fill_from_order_response(body: &str) -> Result<AsterImmediateFill> {
    let r: AsterOrderResp = serde_json::from_str(body)
        .map_err(|e| anyhow!("parse Aster order response immediate fill: {e}: {body}"))?;
    immediate_fill_from_order(&r)
}

pub fn order_response_is_terminal(body: &str) -> Result<bool> {
    let response: AsterOrderResp = serde_json::from_str(body)?;
    let status = response.status.as_deref().ok_or_else(|| anyhow!("Aster order status missing"))?;
    Ok(matches!(status, "FILLED" | "EXPIRED" | "CANCELED" | "REJECTED"))
}

fn immediate_fill_from_order(r: &AsterOrderResp) -> Result<AsterImmediateFill> {
    let quantity = r.executed_qty.as_deref().or(r.cum_qty.as_deref())
        .ok_or_else(|| anyhow!("Aster order response has no executed quantity"))?;
    let qty = quantity.parse::<Decimal>().context("malformed Aster executed quantity")?;
    if qty < Decimal::ZERO { anyhow::bail!("negative Aster executed quantity"); }
    if qty.is_zero() {
        return Ok(AsterImmediateFill { qty, notional: Decimal::ZERO });
    }
    let quote = r.cum_quote.as_deref().map(|s| s.parse::<Decimal>()).transpose()
        .context("malformed Aster cumulative quote")?;
    let price = r.avg_price.as_deref().map(|s| s.parse::<Decimal>()).transpose()
        .context("malformed Aster average price")?;
    let notional = quote.filter(|v| *v > Decimal::ZERO)
        .or_else(|| price.filter(|v| *v > Decimal::ZERO).map(|p| qty * p))
        .ok_or_else(|| anyhow!("Aster filled quantity has no positive quote amount or price"))?;
    Ok(AsterImmediateFill { qty, notional })
}

fn classify_order_response(client_order_id: &str, body: &str) -> SubmitOutcome {
    match serde_json::from_str::<AsterOrderResp>(body) {
        Ok(r) => {
            if let Some(code) = r.code.filter(|c| *c != 0 && *c != 200) {
                return SubmitOutcome::Rejected {
                    reason: format!("code {code}: {}", r.msg.unwrap_or_default()),
                };
            }
            match r.status.as_deref() {
                Some("NEW") | Some("PARTIALLY_FILLED") | Some("FILLED") => {
                    SubmitOutcome::Accepted {
                        venue_order_id: r.order_id,
                        client_order_id: client_order_id.to_string(),
                        raw: body.to_string(),
                    }
                }
                Some("EXPIRED") | Some("CANCELED") => match immediate_fill_from_order(&r) {
                    Ok(fill) if fill.qty > Decimal::ZERO => SubmitOutcome::Accepted {
                        venue_order_id: r.order_id, client_order_id: client_order_id.to_string(), raw: body.to_string(),
                    },
                    Ok(_) => SubmitOutcome::Rejected { reason: format!("terminal {} with zero execution", r.status.unwrap()) },
                    Err(error) => SubmitOutcome::Unknown {
                        client_order_id: client_order_id.to_string(),
                        reason: format!("terminal order has malformed execution evidence: {error}"),
                    },
                },
                Some("REJECTED") => SubmitOutcome::Rejected { reason: "status REJECTED".into() },
                Some(other) => SubmitOutcome::Unknown {
                    client_order_id: client_order_id.to_string(),
                    reason: format!("unexpected Aster order status {other}: {body}"),
                },
                None => SubmitOutcome::Unknown {
                    client_order_id: client_order_id.to_string(),
                    reason: format!("missing Aster order status: {body}"),
                },
            }
        }
        Err(e) => SubmitOutcome::Unknown {
            client_order_id: client_order_id.to_string(),
            reason: format!("unparseable Aster order response: {e}: {body}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0u8; 2048];
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "HTTP request ended before its body");
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(split) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = std::str::from_utf8(&bytes[..split]).unwrap();
                let length = head.lines().find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap())
                }).unwrap_or(0);
                if bytes.len() >= split + 4 + length {
                    return String::from_utf8(bytes).unwrap();
                }
            }
        }
    }

    async fn reply_http(stream: &mut tokio::net::TcpStream, status: &str, body: &str) {
        use tokio::io::AsyncWriteExt;
        let reply = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        stream.write_all(reply.as_bytes()).await.unwrap();
    }

    #[tokio::test]
    async fn transmitted_request_is_the_eip712_message_for_every_method() {
        use k256::ecdsa::signature::hazmat::PrehashVerifier;
        use super::super::sign::test_support::{TestSigner, TEST_KEY};
        for method in [Method::GET, Method::POST, Method::DELETE, Method::PUT] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                reply_http(&mut stream, "200 OK", r#"{"code":200}"#).await;
                request
            });
            let rest = AsterRest::new(url, Arc::new(TestSigner::new()), &[]).unwrap();
            rest.signed_request(method.clone(), "/fapi/v3/order", vec![
                ("newClientOrderId".into(), "order:1/two words".into()),
                ("symbol".into(), "BTCUSDT".into()),
            ]).await.unwrap();
            let request = server.await.unwrap();
            let (head, body) = request.split_once("\r\n\r\n").unwrap();
            let wire = if method == Method::GET {
                head.lines().next().unwrap().split_whitespace().nth(1).unwrap().split_once('?').unwrap().1
            } else { body };
            let (unsigned, signature) = wire.rsplit_once("&signature=").unwrap();
            assert!(unsigned.starts_with("newClientOrderId=order%3A1%2Ftwo+words&symbol=BTCUSDT&nonce="), "{unsigned}");
            assert!(unsigned.contains("&user=0x1111111111111111111111111111111111111111&signer=0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"));
            assert!(!unsigned.contains("recvWindow") && !unsigned.contains("timestamp"));
            let raw = hex::decode(signature.strip_prefix("0x").unwrap()).unwrap();
            let signature = k256::ecdsa::Signature::from_slice(&raw[..64]).unwrap();
            let key = k256::ecdsa::SigningKey::from_slice(&TEST_KEY).unwrap();
            key.verifying_key().verify_prehash(&super::super::crypto::aster_digest(unsigned), &signature).unwrap();
        }
    }

    use rust_decimal_macros::dec;


    #[test]
    fn terminal_status_needs_valid_execution_evidence_and_fees_keep_their_provenance() {
        assert!(matches!(classify_order_response("test", r#"{"orderId":1,"status":"EXPIRED"}"#),
            SubmitOutcome::Unknown { .. }));
        assert!(matches!(classify_order_response("test", r#"{"orderId":1,"status":"EXPIRED","executedQty":"0"}"#),
            SubmitOutcome::Rejected { .. }));
        assert!(matches!(classify_order_response("test", r#"{"orderId":1,"status":"EXPIRED","executedQty":"2","cumQuote":"20"}"#),
            SubmitOutcome::Accepted { .. }));
        assert!(!order_response_is_terminal(r#"{"status":"NEW","executedQty":"0"}"#).unwrap());
        for commission in [r#""0.02""#, "null", r#""not-a-number""#] {
            let raw = format!(r#"[{{"id":1,"orderId":1,"price":"10","qty":"2","quoteQty":"20","commission":{commission},"commissionAsset":"USDT"}}]"#);
            let rows: Vec<AsterUserTrade> = serde_json::from_str(&raw).unwrap();
            let summary = summarize_user_trades(&rows).unwrap().unwrap();
            assert_eq!(summary.qty, dec!(2));
            assert_eq!(summary.notional, dec!(20));
            assert_eq!(summary.fee_provenance, if commission == r#""0.02""# { FeeProvenance::Venue } else { FeeProvenance::Unknown });
        }
    }

    #[test]
    fn immediate_fill_parses_zero_ioc_fill() {
        let body = r#"{"orderId":2075571341,"status":"NEW","executedQty":"0","cumQty":"0","cumQuote":"0","avgPrice":"0"}"#;
        let fill = immediate_fill_from_order_response(body).expect("fill parse");
        assert_eq!(fill.qty, Decimal::ZERO);
        assert_eq!(fill.notional, Decimal::ZERO);
    }

    #[test]
    fn immediate_fill_prefers_cum_quote() {
        let body = r#"{"orderId":1,"status":"FILLED","executedQty":"0.21","cumQty":"0.21","cumQuote":"12.931107","avgPrice":"61.5767"}"#;
        let fill = immediate_fill_from_order_response(body).expect("fill parse");
        assert_eq!(fill.qty, dec!(0.21));
        assert_eq!(fill.notional, dec!(12.931107));
    }

    #[test]
    fn position_qty_absent_row_means_flat() {
        let rows: Vec<AsterPositionRow> =
            serde_json::from_str(r#"[{"symbol":"BTCUSDT","positionAmt":"1.5"}]"#).unwrap();
        assert_eq!(
            position_qty_from_rows(&rows, "HYPEUSDT").unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            position_qty_from_rows(&[], "HYPEUSDT").unwrap(),
            Decimal::ZERO
        );
    }

    #[test]
    fn position_qty_parses_present_row() {
        let rows: Vec<AsterPositionRow> =
            serde_json::from_str(r#"[{"symbol":"HYPEUSDT","positionAmt":"-0.42"}]"#).unwrap();
        assert_eq!(
            position_qty_from_rows(&rows, "HYPEUSDT").unwrap(),
            dec!(-0.42)
        );
    }

    #[test]
    fn position_qty_malformed_row_is_error_not_zero() {
        let rows: Vec<AsterPositionRow> =
            serde_json::from_str(r#"[{"symbol":"HYPEUSDT","positionAmt":"garbage"}]"#).unwrap();
        assert!(position_qty_from_rows(&rows, "HYPEUSDT").is_err());
    }


    #[test]
    fn immediate_fill_uses_cum_qty_when_executed_qty_missing() {
        let body = r#"{"orderId":1,"status":"PARTIALLY_FILLED","cumQty":"0.07","cumQuote":"4.305","avgPrice":"61.5"}"#;
        let fill = immediate_fill_from_order_response(body).expect("fill parse");
        assert_eq!(fill.qty, dec!(0.07));
        assert_eq!(fill.notional, dec!(4.305));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_ioc_takes_from_the_dry_run_venue() {
        let world = crate::dryrun::tests::World::start().await;
        let hype = MarketId("HYPE".into());
        let spec = MarketSpec {
            market_id: hype.clone(), aster_symbol: "HYPEUSDT".into(), lighter_symbol: "HYPE".into(),
            lighter_market_id: 24, lighter_price_decimals: 4, lighter_size_decimals: 2, lighter_price_tick: dec!(0.0001), hedge: Default::default(), first: Default::default(), first_market_index: 0,
            tick: dec!(0.001), step: dec!(0.01), aster_min_qty: dec!(0.01), aster_min_notional: dec!(5),
            lighter_qty_step: dec!(0.01), lighter_min_notional: dec!(10),
        };
        let rest = AsterRest::new(world.aster.clone(), crate::dryrun::tests::aster_signer(), &[spec]).unwrap();
        let outcome = rest.submit_ioc_order(&hype, Side::Buy, dec!(0.1), dec!(101.5), false).await;
        let SubmitOutcome::Accepted { raw, .. } = outcome else { panic!("{outcome:?}") };
        let fill = immediate_fill_from_order_response(&raw).unwrap();
        assert_eq!((fill.qty, fill.notional), (dec!(0.1), dec!(10.1)), "the IOC takes the ask it can reach");
        assert_eq!(rest.position_qty(&hype).await.unwrap(), dec!(0.1));
    }
}

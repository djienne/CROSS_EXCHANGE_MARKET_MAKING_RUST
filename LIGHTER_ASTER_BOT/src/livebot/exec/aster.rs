//! Aster V3 execution worker. Signs the exact transmitted query with EIP-712,
//! keeps request I/O off the strategy loop, and preserves ambiguous order outcomes.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{info, warn};

use super::command::{ExecCommand, ExecEvent, MakerPermit};
use super::sign::{AsterNonce, AsterSigner};
use crate::livebot::scale::MarketScale;
use crate::types::{MarketId, Side};

const ASTER_ORDER_PATH: &str = "/fapi/v3/order";
const ASTER_ALL_ORDERS_PATH: &str = "/fapi/v3/allOpenOrders";
const ASTER_DEADMAN_PATH: &str = "/fapi/v3/countdownCancelAll";
const ASTER_LISTEN_KEY_PATH: &str = "/fapi/v3/listenKey";
const ASTER_POSITION_SIDE_PATH: &str = "/fapi/v3/positionSide/dual";
const USER_AGENT: &str = "xemm-livebot";

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

/// Aster's -2013 "Order does not exist": the order never landed, or is long gone.
pub fn unknown_order(error: &anyhow::Error) -> bool {
    error.downcast_ref::<VenueFailure>().is_some_and(|e| e.status < 500 && e.code == Some(-2013))
}

#[derive(Clone)]
struct MarketWire {
    scale: MarketScale,
    symbol: String,
}

/// A row of the signed `/fapi/v3/balance` response.
#[derive(Debug, Clone, Deserialize)]
pub struct AsterBalanceRow {
    pub asset: String,
    pub balance: String,
    #[serde(rename = "crossWalletBalance", default)]
    pub cross_wallet_balance: String,
}

/// A row of the signed `/fapi/v3/positionRisk` response.
#[derive(Debug, Clone, Deserialize)]
pub struct AsterPositionRow {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
    #[serde(rename = "entryPrice", default)]
    pub entry_price: String,
    #[serde(rename = "unRealizedProfit", default)]
    pub unrealized_profit: String,
    #[serde(rename = "positionSide", default)]
    pub position_side: String,
    /// Account leverage for this symbol (read by the startup leverage gate).
    #[serde(default)]
    pub leverage: String,
}

/// A row of the signed `/fapi/v3/openOrders` response.
#[derive(Debug, Clone, Deserialize)]
pub struct AsterOpenOrder {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub price: String,
    #[serde(rename = "origQty", default)]
    pub orig_qty: String,
    #[serde(default)]
    pub status: String,
}

/// Parsed Aster order response (POST/DELETE `/fapi/v3/order`). Only the fields we act on.
#[derive(Debug, Deserialize)]
struct AsterOrderResp {
    #[serde(rename = "orderId")]
    order_id: Option<i64>,
    status: Option<String>,
    #[serde(rename = "clientOrderId")]
    client_order_id: Option<String>,
    code: Option<i64>,
    msg: Option<String>,
}

/// The signed Aster REST client + nonce + per-market wire context.
pub struct AsterRest {
    client: reqwest::Client,
    base_url: String,
    signer: Arc<dyn AsterSigner>,
    nonce: AsterNonce,
    markets: HashMap<MarketId, MarketWire>,
    deadman_countdown_ms: i64,
    /// The worker sends one dead-man refresh at a time, each on its own task.
    deadman_in_flight: AtomicBool,
    rate_limit_backoff_ms: i64,
    max_rest_requests_per_minute: u32,
}

impl AsterRest {
    pub fn new(
        base_url: String,
        signer: Arc<dyn AsterSigner>,
        market_scales: HashMap<MarketId, (MarketScale, String)>,
        deadman_countdown_ms: i64,
        rate_limit_backoff_ms: i64,
        max_rest_requests_per_minute: u32,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5)) // short: a stalled order call must not wedge the worker
            // Transport tuning (deployment-independent — helps on ANY network, not a fast-VPS knob):
            // disable Nagle so a small signed order POST goes out immediately; keep the pre-warmed
            // TLS connection alive between sparse orders so a hedge/cancel never pays a fresh handshake.
            .tcp_nodelay(true)
            .pool_idle_timeout(Some(Duration::from_secs(120)))
            .pool_max_idle_per_host(4)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()?;
        let markets = market_scales
            .into_iter()
            .map(|(m, (scale, symbol))| (m, MarketWire { scale, symbol }))
            .collect();
        let nonce = AsterNonce::for_signer(&base_url, signer.signer_address())?;
        Ok(AsterRest {
            client,
            base_url,
            signer,
            nonce,
            markets,
            deadman_countdown_ms: deadman_countdown_ms.max(1000),
            deadman_in_flight: AtomicBool::new(false),
            rate_limit_backoff_ms: rate_limit_backoff_ms.max(1000),
            max_rest_requests_per_minute: max_rest_requests_per_minute.max(1),
        })
    }

    fn wire(&self, market: &MarketId) -> Result<&MarketWire> {
        self.markets
            .get(market)
            .ok_or_else(|| anyhow!("no wire context for market {market}"))
    }

    /// Build business params for a post-only GTX maker order (one-way mode: `positionSide=BOTH`).
    fn place_params(
        &self,
        market: &MarketId,
        side: Side,
        price_ticks: i64,
        qty_lots: i64,
        client_id: &str,
        reduce_only: bool,
    ) -> Result<Vec<(String, String)>> {
        let w = self.wire(market)?;
        let price = w.scale.ticks_to_price(price_ticks);
        let qty = w.scale.lots_to_qty(qty_lots);
        let mut p = vec![
            ("symbol".into(), w.symbol.clone()),
            ("side".into(), side.as_str().to_string()), // BUY / SELL
            ("type".into(), "LIMIT".into()),
            ("timeInForce".into(), "GTX".into()), // Good Till Crossing = post-only
            ("quantity".into(), trim_dec(qty)),
            ("price".into(), trim_dec(price)),
            ("newClientOrderId".into(), client_id.to_string()),
            ("positionSide".into(), "BOTH".into()),
        ];
        if reduce_only {
            p.push(("reduceOnly".into(), "true".into()));
        }
        Ok(p)
    }

    /// A single account-level available margin value, independent of wallet/equity.
    pub async fn account_available_balance(&self) -> Result<Decimal> {
        let body = self.signed_request(Method::GET, "/fapi/v3/account", vec![]).await?;
        let value: serde_json::Value = serde_json::from_str(&body)?;
        value.get("availableBalance").and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Aster account is missing availableBalance"))?
            .parse::<Decimal>().context("malformed Aster availableBalance")
    }

    /// Resolve the original identity on the cold path; an absent/error result is unknown.
    pub async fn query_order(&self, market: &MarketId, client_order_id: &str) -> Result<serde_json::Value> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self.signed_request(Method::GET, ASTER_ORDER_PATH, vec![
            ("symbol".into(), symbol),
            ("origClientOrderId".into(), client_order_id.to_string()),
        ]).await?;
        serde_json::from_str(&body).context("parse Aster order query")
    }

    /// Encode authentication and business parameters once, sign those bytes, and send.
    /// Explicit venue errors stay distinct from potentially executed transport failures.
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

    /// Place a maker order from `Decimal` price/qty (rounds passively: buy floors, sell ceils
    /// to tick; qty floors to lot). Used by the `aster-place-cancel` probe.
    pub(crate) async fn place_decimal(
        &self,
        market: &MarketId,
        side: Side,
        px: Decimal,
        qty: Decimal,
        client_id: &str,
        reduce_only: bool,
    ) -> ExecEvent {
        let w = match self.wire(market) {
            Ok(w) => w,
            Err(e) => return ExecEvent::PlaceReject { client_id: client_id.to_string(), reason: e.to_string() },
        };
        let price_ticks = match side {
            Side::Buy => w.scale.price_floor_ticks(px),
            Side::Sell => w.scale.price_ceil_ticks(px),
        };
        let qty_lots = w.scale.qty_to_lots(qty);
        self.place(market, side, price_ticks, qty_lots, client_id, reduce_only).await
    }

    // --- signed reads (shared by the probe harness + the account reconciler) ---

    /// Signed USDⓈ-M balance read (`GET /fapi/v3/balance`).
    pub async fn balance(&self) -> Result<Vec<AsterBalanceRow>> {
        let body = self.signed_request(Method::GET, "/fapi/v3/balance", vec![]).await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse balance: {e}: {body}"))
    }

    /// Signed position read (`GET /fapi/v3/positionRisk`).
    pub async fn position_risk(&self) -> Result<Vec<AsterPositionRow>> {
        let body = self.signed_request(Method::GET, "/fapi/v3/positionRisk", vec![]).await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse positionRisk: {e}: {body}"))
    }

    /// Whether the account is in ONE-WAY position mode (`dualSidePosition == false`). The bot
    /// assumes one-way (`positionSide=BOTH`); hedge mode would mis-route orders and mis-report
    /// positions, so live trading refuses to start unless this is true.
    pub async fn is_one_way(&self) -> Result<bool> {
        let body = self.signed_request(Method::GET, ASTER_POSITION_SIDE_PATH, vec![]).await?;
        parse_one_way(&body)
    }

    /// Signed open-orders read (`GET /fapi/v3/openOrders`), optionally for one symbol.
    pub async fn open_orders(&self, market: Option<&MarketId>) -> Result<Vec<AsterOpenOrder>> {
        let params = match market {
            Some(m) => vec![("symbol".into(), self.wire(m)?.symbol.clone())],
            None => vec![],
        };
        let body = self.signed_request(Method::GET, "/fapi/v3/openOrders", params).await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse openOrders: {e}: {body}"))
    }

    // --- user-data stream listenKey lifecycle (signed; NO listenKey param per V3 docs) ---

    /// The websocket root of this client's venue, where the listenKey stream is served.
    pub fn ws_root(&self) -> String {
        crate::connectors::aster::ws_root(&self.base_url)
    }

    /// Create a user-data stream listenKey (`POST /fapi/v3/listenKey`).
    pub async fn create_listen_key(&self) -> Result<String> {
        let body = self.signed_request(Method::POST, "/fapi/v3/listenKey", vec![]).await?;
        let v: serde_json::Value = serde_json::from_str(&body)?;
        v.get("listenKey")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("no listenKey in response: {body}"))
    }

    /// Keep the listenKey alive (`PUT /fapi/v3/listenKey`, no params). ~30-min cadence.
    pub async fn keepalive_listen_key(&self) -> Result<()> {
        self.signed_request(Method::PUT, ASTER_LISTEN_KEY_PATH, vec![])
            .await
            .and_then(|body| reject_body_error(ASTER_LISTEN_KEY_PATH, &body))
            .map(|_| ())
    }

    /// Close the listenKey (`DELETE /fapi/v3/listenKey`) — only on graceful shutdown.
    pub async fn close_listen_key(&self) -> Result<()> {
        self.signed_request(Method::DELETE, ASTER_LISTEN_KEY_PATH, vec![])
            .await
            .and_then(|body| reject_body_error(ASTER_LISTEN_KEY_PATH, &body))
            .map(|_| ())
    }

    /// Place a maker order and classify the response into a place lifecycle event.
    pub(crate) async fn place(
        &self,
        market: &MarketId,
        side: Side,
        price_ticks: i64,
        qty_lots: i64,
        client_id: &str,
        reduce_only: bool,
    ) -> ExecEvent {
        let params = match self.place_params(market, side, price_ticks, qty_lots, client_id, reduce_only) {
            Ok(p) => p,
            Err(e) => return ExecEvent::PlaceReject { client_id: client_id.to_string(), reason: e.to_string() },
        };
        match self.signed_request(Method::POST, ASTER_ORDER_PATH, params).await {
            Ok(body) => classify_place(client_id, &body),
            Err(e) if definitive_no_fill(&e) => ExecEvent::PlaceReject {
                client_id: client_id.to_string(), reason: e.to_string(),
            },
            Err(e) => ExecEvent::PlaceUnknown {
                client_id: client_id.to_string(), reason: e.to_string(),
            },
        }
    }

    /// Cancel a specific order by client id (DELETE `/fapi/v3/order`). A `-2011` "unknown order"
    /// is treated as already-gone (success).
    pub(crate) async fn cancel_order(&self, market: &MarketId, client_id: &str) -> Result<CancelOutcome> {
        let w = self.wire(market)?;
        let params = vec![
            ("symbol".into(), w.symbol.clone()),
            ("origClientOrderId".into(), client_id.to_string()),
        ];
        match self.signed_request(Method::DELETE, ASTER_ORDER_PATH, params).await {
            // Aster returns HTTP 200 even for some venue errors ({code,msg} in the body), so
            // transport success != cancel success — classify the body (no false CancelAck).
            Ok(body) => classify_cancel(&body),
            Err(e) if e.downcast_ref::<VenueFailure>().is_some_and(|e| e.status < 500 && e.code == Some(-2011)) => Ok(CancelOutcome::AlreadyGone),
            Err(e) => Err(e),
        }
    }

    /// Modify a resting LIMIT order's price and quantity in place (PUT `/fapi/v3/order`): one
    /// round trip instead of cancel-then-place. A rejected modify leaves the original resting.
    pub(crate) async fn amend(&self, market: &MarketId, side: Side, price_ticks: i64, qty_lots: i64, client_id: &str) -> Result<String> {
        let w = self.wire(market)?;
        let params = vec![
            ("symbol".into(), w.symbol.clone()),
            ("origClientOrderId".into(), client_id.to_string()),
            ("side".into(), side.as_str().to_string()),
            ("quantity".into(), trim_dec(w.scale.lots_to_qty(qty_lots))),
            ("price".into(), trim_dec(w.scale.ticks_to_price(price_ticks))),
        ];
        self.signed_request(Method::PUT, ASTER_ORDER_PATH, params).await
    }

    /// Reduce-only MARKET (taker) order to flatten an orphaned position (recovery path).
    /// Reduce-only orders are exempt from the min-notional filter, so a sub-min residual can
    /// still be closed. `side` = SELL to close a long, BUY to close a short.
    pub(crate) async fn flatten_result(&self, market: &MarketId, side: Side, qty: Decimal, client_id: &str) -> Result<String> {
        let w = self.wire(market)?;
        let qty_lots = w.scale.qty_to_lots(qty);
        if qty_lots <= 0 {
            return Err(RequestNotSent("reduce-only residual is below one lot".into()).into());
        }
        let params = vec![
            ("symbol".into(), w.symbol.clone()),
            ("side".into(), side.as_str().into()),
            ("type".into(), "MARKET".into()),
            ("quantity".into(), trim_dec(w.scale.lots_to_qty(qty_lots))),
            ("newClientOrderId".into(), client_id.to_string()),
            ("newOrderRespType".into(), "RESULT".into()),
            ("positionSide".into(), "BOTH".into()),
            ("reduceOnly".into(), "true".into()),
        ];
        self.signed_request(Method::POST, ASTER_ORDER_PATH, params).await
    }

    /// Refresh the Aster dead-man countdown for a symbol (heartbeat).
    pub(crate) async fn refresh_deadman(&self, market: &MarketId) -> Result<()> {
        let w = self.wire(market)?;
        let params = vec![
            ("symbol".into(), w.symbol.clone()),
            ("countdownTime".into(), self.deadman_countdown_ms.to_string()),
        ];
        self.signed_request(Method::POST, ASTER_DEADMAN_PATH, params)
            .await
            .and_then(|body| reject_body_error(ASTER_DEADMAN_PATH, &body))
            .map(|_| ())
    }

    pub(crate) async fn cancel_all_symbol(&self, market: &MarketId) -> Result<()> {
        let w = self.wire(market)?;
        let params = vec![("symbol".into(), w.symbol.clone())];
        self.signed_request(Method::DELETE, ASTER_ALL_ORDERS_PATH, params)
            .await
            .and_then(|body| reject_body_error(ASTER_ALL_ORDERS_PATH, &body))
            .map(|_| ())
    }

    /// Read current leverage for startup validation; do not change account preferences.
    pub(crate) async fn get_leverage(&self, market: &MarketId) -> Result<u32> {
        let w = self.wire(market)?;
        let body = self
            .signed_request(Method::GET, "/fapi/v3/positionRisk", vec![("symbol".into(), w.symbol.clone())])
            .await?;
        let rows: Vec<AsterPositionRow> =
            serde_json::from_str(&body).map_err(|e| anyhow::anyhow!("parse positionRisk leverage: {e}: {body}"))?;
        let row = rows
            .iter()
            .find(|r| r.symbol.eq_ignore_ascii_case(&w.symbol))
            .ok_or_else(|| anyhow::anyhow!("aster positionRisk has no row for {}", w.symbol))?;
        row.leverage
            .trim()
            .parse::<f64>()
            .map(|l| l.round() as u32)
            .map_err(|e| anyhow::anyhow!("aster leverage parse '{}': {e}", row.leverage))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelOutcome {
    /// Venue explicitly confirmed cancel.
    Canceled,
    /// Venue says it does not know the order.
    AlreadyGone,
    /// The order is no longer resting because it filled/expired; its fill may still be on the way.
    FilledOrExpired,
}

/// Validate a JSON object acknowledgement and reject explicit venue errors.
fn reject_body_error(path: &str, body: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(body).context("invalid Aster acknowledgement JSON")?;
    let object = value.as_object().ok_or_else(|| anyhow!("Aster {path} acknowledgement is not an object"))?;
    if let Some(raw) = object.get("code") {
        let code = raw.as_i64().ok_or_else(|| anyhow!("Aster {path} acknowledgement code is not an integer"))?;
        if code != 0 && code != 200 {
            return Err(VenueFailure { status: 200, code: Some(code), body: body.to_string() }.into());
        }
    }
    Ok(body.to_string())
}

/// Parse the `GET /fapi/v3/positionSide/dual` body into "is one-way mode". Venue error envelopes
/// are rejected first, and `dualSidePosition` MUST be present and boolean — a malformed body must
/// not silently read as "one-way OK" (the startup gate bails instead: fail-closed).
fn parse_one_way(body: &str) -> Result<bool> {
    let body = reject_body_error(ASTER_POSITION_SIDE_PATH, body)?;
    let v: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| anyhow!("parse positionSide/dual: {e}: {body}"))?;
    match v.get("dualSidePosition").and_then(|d| d.as_bool()) {
        Some(dual) => Ok(!dual),
        None => Err(anyhow!("positionSide/dual response missing boolean dualSidePosition: {body}")),
    }
}

/// Classify a DELETE `/fapi/v3/order` response into cancel success/failure. Aster returns HTTP 200
/// even for some venue errors, so the BODY decides. `-2011` ("unknown order") is already-gone =
/// success. `FILLED`/`EXPIRED` likewise means not resting, but a fill may not have reached the
/// strategy yet. Any other code/status is a REAL failure the worker must report as a
/// `CancelReject` so the strategy keeps the (possibly still-resting) order and freezes.
fn classify_cancel(body: &str) -> Result<CancelOutcome> {
    match serde_json::from_str::<AsterOrderResp>(body) {
        Ok(r) => {
            if let Some(code) = r.code {
                if code == -2011 {
                    return Ok(CancelOutcome::AlreadyGone);
                }
                anyhow::bail!("cancel venue error code {code}: {}", r.msg.unwrap_or_default());
            }
            match r.status.as_deref() {
                Some("CANCELED") => Ok(CancelOutcome::Canceled),
                Some("FILLED") | Some("EXPIRED") => Ok(CancelOutcome::FilledOrExpired),
                // Conservative: a missing/unexpected status is NOT a confirmed cancel — treat it as a
                // failure (→ CancelReject → freeze + sweep). A false negative only costs a transient
                // freeze; a false positive (acking an un-cancelled order) is the dangerous case.
                Some(other) => anyhow::bail!("cancel unexpected status {other}"),
                None => anyhow::bail!("cancel response missing status (and no code): {body}"),
            }
        }
        Err(e) => anyhow::bail!("unparseable cancel response: {e}: {body}"),
    }
}

/// A cancel's event: only a confirmed cancel (or an order already gone) closes the slot.
fn cancel_event(client_id: String, outcome: Result<CancelOutcome>) -> ExecEvent {
    match outcome {
        Ok(CancelOutcome::Canceled | CancelOutcome::AlreadyGone) => ExecEvent::CancelAck { client_id },
        Ok(CancelOutcome::FilledOrExpired) => ExecEvent::CancelFilledOrExpired { client_id },
        Err(e) => {
            warn!("aster cancel {client_id} failed: {e:#}");
            ExecEvent::CancelReject { client_id, reason: e.to_string() }
        }
    }
}

/// Classify a PUT `/fapi/v3/order` outcome. The order keeps its ids, so resting at the new
/// values is a `PlaceAck`. An order no longer open (-2013, or FILLED/CANCELED/EXPIRED: Aster
/// cancels one amended to at most its filled qty) closes the slot like a cancel that found it
/// gone. A definitive refusal (-2036 when it would cross, live 2026-09-28) leaves it resting
/// unchanged. Anything else is ambiguous.
fn classify_amend(client_id: String, outcome: Result<String>) -> ExecEvent {
    let body = match outcome {
        Ok(body) => body,
        Err(e) if unknown_order(&e) => return ExecEvent::CancelFilledOrExpired { client_id },
        Err(e) if definitive_no_fill(&e) => return ExecEvent::AmendReject { client_id, reason: e.to_string() },
        Err(e) => return ExecEvent::PlaceUnknown { client_id, reason: e.to_string() },
    };
    match serde_json::from_str::<AsterOrderResp>(&body) {
        Ok(AsterOrderResp { status: Some(status), order_id: Some(order_id), .. }) => match status.as_str() {
            "NEW" | "PARTIALLY_FILLED" => ExecEvent::PlaceAck { client_id, venue_order_id: order_id.to_string() },
            "FILLED" | "CANCELED" | "EXPIRED" => ExecEvent::CancelFilledOrExpired { client_id },
            _ => ExecEvent::PlaceUnknown { client_id, reason: format!("unexpected amend status: {body}") },
        },
        _ => ExecEvent::PlaceUnknown { client_id, reason: format!("unparseable amend response: {body}") },
    }
}

/// Classify a `/fapi/v3/order` POST response body into a place lifecycle event. A GTX order that
/// would cross rests as `EXPIRED` (treat as a reject → re-quote, not an error). Only a clean
/// `NEW` response is a safe resting-order ack. `FILLED`/`PARTIALLY_FILLED`, malformed success
/// bodies, and missing `orderId` are ambiguous: an order may have existed and moved inventory
/// before the user stream reported it, so freeze+sweep via `PlaceUnknown` instead of silently
/// closing the local slot.
fn classify_place(client_id: &str, body: &str) -> ExecEvent {
    match serde_json::from_str::<AsterOrderResp>(body) {
        Ok(r) => {
            if let Some(code) = r.code {
                return ExecEvent::PlaceReject {
                    client_id: client_id.to_string(),
                    reason: format!("code {code}: {}", r.msg.unwrap_or_default()),
                };
            }
            match r.status.as_deref() {
                Some("NEW") => match r.order_id {
                    Some(oid) => ExecEvent::PlaceAck {
                        client_id: r.client_order_id.unwrap_or_else(|| client_id.to_string()),
                        venue_order_id: oid.to_string(),
                    },
                    None => ExecEvent::PlaceUnknown {
                        client_id: client_id.to_string(),
                        reason: format!("NEW response missing orderId: {body}"),
                    },
                },
                Some("EXPIRED") => ExecEvent::PlaceReject {
                    client_id: client_id.to_string(),
                    reason: "status EXPIRED".into(),
                },
                Some("REJECTED") => ExecEvent::PlaceReject {
                    client_id: client_id.to_string(),
                    reason: "status REJECTED".into(),
                },
                Some("PARTIALLY_FILLED") | Some("FILLED") => ExecEvent::PlaceUnknown {
                    client_id: r.client_order_id.unwrap_or_else(|| client_id.to_string()),
                    reason: format!("place returned fill status; wait for user stream/reconcile: {body}"),
                },
                Some(other) => ExecEvent::PlaceUnknown {
                    client_id: client_id.to_string(),
                    reason: format!("unexpected place status {other}: {body}"),
                },
                None => ExecEvent::PlaceUnknown {
                    client_id: client_id.to_string(),
                    reason: format!("place response missing status: {body}"),
                },
            }
        }
        Err(e) => ExecEvent::PlaceUnknown {
            client_id: client_id.to_string(),
            reason: format!("unparseable place response: {e}: {body}"),
        },
    }
}

fn is_aster_rate_limit_reason(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("http 429") || r.contains("code -1003") || r.contains("too many requests")
}

fn exec_event_rate_limit_reason(ev: &ExecEvent) -> Option<&str> {
    match ev {
        ExecEvent::PlaceReject { reason, .. }
        | ExecEvent::AmendReject { reason, .. }
        | ExecEvent::PlaceUnknown { reason, .. }
        | ExecEvent::CancelReject { reason, .. }
        | ExecEvent::AsterFlattenReject { reason, .. } if is_aster_rate_limit_reason(reason) => Some(reason.as_str()),
        _ => None,
    }
}

async fn notify_rate_limited(tx: &Sender<ExecEvent>, reason: String, backoff_ms: i64) {
    warn!("aster REST rate limited/backing off for {backoff_ms}ms: {reason}");
    let _ = tx.send(ExecEvent::AsterRateLimited { reason, backoff_ms }).await;
}

async fn reject_unsent_maker(tx: &Sender<ExecEvent>, permit: &MakerPermit, unsent: ExecEvent) {
    permit.cancel_queued();
    // A duplicate of a claimed command cannot prove the original was not sent.
    if permit.is_cancelled() {
        let _ = tx.send(unsent).await;
    }
}

async fn send_backoff_reject(tx: &Sender<ExecEvent>, cmd: ExecCommand, reason: String, backoff_ms: i64) {
    match cmd {
        ExecCommand::Place { client_id, permit, .. } => {
            reject_unsent_maker(tx, &permit, ExecEvent::PlaceReject { client_id, reason: reason.clone() }).await;
        }
        ExecCommand::Cancel { client_id, .. } => {
            let _ = tx.send(ExecEvent::CancelReject { client_id, reason: reason.clone() }).await;
        }
        ExecCommand::Amend { client_id, permit, .. } => {
            reject_unsent_maker(tx, &permit, ExecEvent::AmendReject { client_id, reason: reason.clone() }).await;
        }
        ExecCommand::FlattenAster { intent, .. } => {
            intent.admission.cancel_queued();
            if intent.admission.is_cancelled() {
                let _ = tx.send(ExecEvent::AttemptNotSent { cloid: intent.cloid, reason: reason.clone() }).await;
            }
        }
        ExecCommand::CancelAllBot
        | ExecCommand::RefreshDeadman { .. } => {}
        ExecCommand::Barrier { completion } => {
            completion.complete(crate::hotpath::clock::mono_now_ns());
            return;
        }
        ExecCommand::Shutdown => {}
    }
    notify_rate_limited(tx, reason, backoff_ms).await;
}

struct RestCommandLimiter {
    max_per_minute: u32,
    sent: VecDeque<tokio::time::Instant>,
}

impl RestCommandLimiter {
    fn new(max_per_minute: u32) -> Self {
        RestCommandLimiter { max_per_minute: max_per_minute.max(1), sent: VecDeque::new() }
    }

    fn next_ready_at(&mut self) -> Option<tokio::time::Instant> {
        let window = Duration::from_secs(60);
        let now = tokio::time::Instant::now();
        while self.sent.front().is_some_and(|&t| now.saturating_duration_since(t) >= window) {
            self.sent.pop_front();
        }
        if self.sent.len() >= self.max_per_minute as usize {
            self.sent.front().map(|t| *t + window)
        } else {
            None
        }
    }

    /// Count a request against the window WITHOUT ever sleeping — the priority lane's
    /// cancels/flattens are already budgeted by the strategy (`aster_budget_allows` counts
    /// every enqueue against the same per-minute cap), and a risk-reducing cancel must not
    /// wait behind the shared limiter. The recorded timestamp is still visible to later
    /// `next_ready_at()` calls, so normal-lane commands keep honoring the cap.
    fn record(&mut self) {
        let window = Duration::from_secs(60);
        let now = tokio::time::Instant::now();
        while self.sent.front().is_some_and(|&t| now.saturating_duration_since(t) >= window) {
            self.sent.pop_front();
        }
        self.sent.push_back(now);
    }

}

/// The Aster execution worker loop: drain commands, perform venue I/O, publish events.
pub async fn run_aster_worker(
    mut rx: Receiver<ExecCommand>,
    mut prio_rx: Receiver<ExecCommand>,
    tx: Sender<ExecEvent>,
    rest: AsterRest,
) {
    info!("aster live exec worker started (Aster V3 EIP-712)");
    let rest = Arc::new(rest);
    let mut backoff_until: Option<tokio::time::Instant> = None;
    let mut limiter = RestCommandLimiter::new(rest.max_rest_requests_per_minute);
    let mut prio_open = true;
    let mut pending_normal = None;
    loop {
        // Priority lane first (acked cancels + flattens — see is_priority_cmd): drain
        // without waiting, then block on both lanes biased toward priority. A cancel
        // arriving while N places sit queued is executed next, not N commands later.
        let (cmd, from_prio) = if prio_open {
            match prio_rx.try_recv() {
                Ok(c) => (c, true),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    prio_open = false;
                    continue;
                }
                Err(mpsc::error::TryRecvError::Empty) if pending_normal.is_some() => {
                    (pending_normal.take().unwrap(), false)
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    tokio::select! {
                        biased;
                        c = prio_rx.recv() => match c {
                            Some(c) => (c, true),
                            None => {
                                prio_open = false;
                                continue;
                            }
                        },
                        c = rx.recv() => match c {
                            Some(c) => (c, false),
                            None => break,
                        },
                    }
                }
            }
        } else if let Some(pending) = pending_normal.take() {
            (pending, false)
        } else {
            match rx.recv().await {
                Some(c) => (c, false),
                None => break,
            }
        };
        if let ExecCommand::Barrier { completion } = &cmd {
            completion.complete(crate::hotpath::clock::mono_now_ns());
            continue;
        }
        if matches!(cmd, ExecCommand::Shutdown) {
            // Drain safety work; stale optional placements must not appear after shutdown.
            for late in pending_normal.take().into_iter()
                .chain(std::iter::from_fn(|| prio_rx.try_recv().ok()))
                .chain(std::iter::from_fn(|| rx.try_recv().ok())) {
                match late {
                    ExecCommand::Place { client_id, permit, .. } => {
                        reject_unsent_maker(&tx, &permit, ExecEvent::PlaceReject { client_id, reason: "worker shutdown before send".into() }).await;
                    }
                    ExecCommand::Amend { client_id, permit, .. } => {
                        reject_unsent_maker(&tx, &permit, ExecEvent::AmendReject { client_id, reason: "worker shutdown before send".into() }).await;
                    }
                    ExecCommand::Shutdown => {}
                    other => process_cmd(other, true, &tx, &rest, &mut limiter, &mut backoff_until).await,
                }
            }
            break;
        }
        if backoff_until.is_some_and(|until| tokio::time::Instant::now() < until) {
            process_cmd(cmd, from_prio, &tx, &rest, &mut limiter, &mut backoff_until).await;
            continue;
        }
        if !from_prio {
            if let Some(ready_at) = limiter.next_ready_at() {
                pending_normal = Some(cmd);
                tokio::select! {
                    biased;
                    priority = prio_rx.recv(), if prio_open => {
                        match priority {
                            Some(priority) => {
                                process_cmd(priority, true, &tx, &rest, &mut limiter, &mut backoff_until).await;
                            }
                            None => prio_open = false,
                        }
                    }
                    _ = tokio::time::sleep_until(ready_at) => {}
                }
                continue;
            }
        }
        process_cmd(cmd, from_prio, &tx, &rest, &mut limiter, &mut backoff_until).await;
    }
    info!("aster live exec worker stopped");
}

/// One command's full lifecycle: 429-backoff gate, limiter gate, venue I/O, event
/// publication, and backoff arming. Factored out of the worker loop so the priority
/// lane and the shutdown drain share the exact same semantics.
async fn process_cmd(
    cmd: ExecCommand,
    _from_prio: bool,
    tx: &Sender<ExecEvent>,
    rest: &Arc<AsterRest>,
    limiter: &mut RestCommandLimiter,
    backoff_until: &mut Option<tokio::time::Instant>,
) {
    {
        if let Some(until) = *backoff_until {
            let now = tokio::time::Instant::now();
            if now < until {
                let remaining_ms = until.saturating_duration_since(now).as_millis() as i64;
                send_backoff_reject(
                    tx,
                    cmd,
                    format!("Aster REST backoff active ({}ms remaining)", remaining_ms),
                    remaining_ms.max(1),
                )
                .await;
                return;
            }
            *backoff_until = None;
        }
    }

        let mut rate_limit_reason: Option<String> = None;
        match cmd {
            ExecCommand::Place { market, side, price_ticks, qty_lots, client_id, permit } => {
                // This runs after every rate-limit wait.
                // Revalidate the captured books, rights, and deadline at send ownership.
                if !permit.try_claim(crate::hotpath::clock::mono_now_ns()) {
                    let reason = "maker admission revoked or expired before send".into();
                    reject_unsent_maker(tx, &permit, ExecEvent::PlaceReject { client_id, reason }).await;
                    return;
                }
                limiter.record();
                let ev = rest.place(&market, side, price_ticks, qty_lots, &client_id, false).await;
                if let Some(reason) = exec_event_rate_limit_reason(&ev) {
                    rate_limit_reason = Some(reason.to_string());
                }
                let _ = tx.send(ev).await;
            }
            ExecCommand::Cancel { client_id, market, .. } => {
                // Only ack a cancel that actually succeeded — a failed cancel must NOT close the
                // strategy's slot (the order may still be resting). Report the real outcome.
                limiter.record();
                let outcome = rest.cancel_order(&market, &client_id).await;
                let ev = cancel_event(client_id, outcome);
                if let Some(reason) = exec_event_rate_limit_reason(&ev) {
                    rate_limit_reason = Some(reason.to_string());
                }
                let _ = tx.send(ev).await;
            }
            ExecCommand::Amend { market, side, client_id, price_ticks, qty_lots, permit } => {
                // One round trip where cancel-then-place took two. Books that moved, or rights
                // revoked, before send pull the quote instead, as a replace's cancel always did.
                limiter.record();
                let ev = if permit.try_claim(crate::hotpath::clock::mono_now_ns()) {
                    let outcome = rest.amend(&market, side, price_ticks, qty_lots, &client_id).await;
                    classify_amend(client_id, outcome)
                } else {
                    let outcome = rest.cancel_order(&market, &client_id).await;
                    cancel_event(client_id, outcome)
                };
                if let Some(reason) = exec_event_rate_limit_reason(&ev) {
                    rate_limit_reason = Some(reason.to_string());
                }
                let _ = tx.send(ev).await;
            }
            ExecCommand::CancelAllBot => {
                for market in rest.markets.keys().cloned().collect::<Vec<_>>() {
                    limiter.record();
                    if let Err(e) = rest.cancel_all_symbol(&market).await {
                        let reason = e.to_string();
                        if is_aster_rate_limit_reason(&reason) {
                            rate_limit_reason = Some(reason.clone());
                            warn!("aster cancelAllBot hit rate limit on {market}: {e:#}");
                            break;
                        }
                        warn!("aster cancelAllBot failed on {market}: {e:#}");
                    }
                }
            }
            ExecCommand::FlattenAster { intent, client_id } => {
                let now_ns = crate::hotpath::clock::mono_now_ns();
                if !intent.admission.try_claim(now_ns) {
                    // A duplicate of an already-claimed command cannot prove the
                    // original was not sent; only the cancelled state permits that event.
                    if intent.admission.is_cancelled() {
                        let _ = tx.send(ExecEvent::AttemptNotSent {
                            cloid: intent.cloid, reason: "correction cancelled or expired before send".into(),
                        }).await;
                    }
                    return;
                }
                let market = intent.market.clone();
                let side = intent.hedge_side;
                let qty = intent.qty;
                limiter.record();
                let _ = tx.send(ExecEvent::AttemptStarted {
                    cloid: intent.cloid,
                    proof: crate::livebot::fills::WireProof {
                        tx_hash: None, nonce: None, client_order_index: None, sent_ns: now_ns,
                    },
                }).await;
                match rest.flatten_result(&market, side, qty, &client_id).await {
                    Ok(body) => {
                        let _ = tx.send(ExecEvent::AsterFlattenAck { cloid: intent.cloid }).await;
                        if let Some((filled, quote, terminal, order_id, event_time_ms)) = order_progress(&body) {
                            let _ = tx.send(ExecEvent::ExecutionProgress {
                                cloid: intent.cloid, cumulative_qty: filled, cumulative_quote_usd: quote,
                                cumulative_fee_usd: None, terminal, venue_order_id: order_id, event_time_ms,
                            }).await;
                        }
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        if is_aster_rate_limit_reason(&reason) { rate_limit_reason = Some(reason.clone()); }
                        let _ = tx.send(ExecEvent::AsterFlattenReject {
                            cloid: intent.cloid, reason, terminal: definitive_no_fill(&error),
                        }).await;
                    }
                }
            }
            ExecCommand::RefreshDeadman { market } => {
                // On its own task, like the listenKey keepalive: this round trip every 2 s must
                // not hold up a place or cancel queued behind it. A 429 freezes the strategy;
                // the worker's own backoff arms at the next request that meets it.
                if !rest.deadman_in_flight.swap(true, Ordering::AcqRel) {
                    limiter.record();
                    let (rest, tx) = (rest.clone(), tx.clone());
                    tokio::spawn(async move {
                        if let Err(e) = rest.refresh_deadman(&market).await {
                            warn!("aster deadman refresh failed: {e:#}");
                            let reason = e.to_string();
                            if is_aster_rate_limit_reason(&reason) {
                                notify_rate_limited(&tx, reason, rest.rate_limit_backoff_ms).await;
                            }
                        }
                        rest.deadman_in_flight.store(false, Ordering::Release);
                    });
                }
            }
            ExecCommand::Barrier { completion } => {
                completion.complete(crate::hotpath::clock::mono_now_ns());
            }
            ExecCommand::Shutdown => {
                debug_assert!(false, "Shutdown is intercepted by the worker loop");
            }
        }

        if let Some(reason) = rate_limit_reason {
            *backoff_until = Some(tokio::time::Instant::now() + Duration::from_millis(rest.rate_limit_backoff_ms as u64));
            notify_rate_limited(tx, reason, rest.rate_limit_backoff_ms).await;
        }
}

pub(crate) fn order_progress(body: &str) -> Option<(Decimal, Option<Decimal>, bool, Option<String>, Option<i64>)> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let status = value.get("status")?.as_str()?;
    let qty = value.get("executedQty").or_else(|| value.get("cumQty"))?.as_str()?.parse::<Decimal>().ok()?;
    if qty < Decimal::ZERO { return None; }
    let quote = value.get("cumQuote").and_then(|v| v.as_str()).and_then(|s| s.parse::<Decimal>().ok())
        .filter(|v| *v >= Decimal::ZERO);
    let terminal = matches!(status, "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED");
    let order_id = value.get("orderId").and_then(|v| v.as_i64()).map(|n| n.to_string());
    let event_time_ms = value.get("updateTime").and_then(serde_json::Value::as_i64).filter(|time| *time > 0);
    Some((qty, quote, terminal, order_id, event_time_ms))
}

/// Format a Decimal for the wire without scientific notation or trailing-zero noise.
fn trim_dec(d: Decimal) -> String {
    d.normalize().to_string()
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
            let rest = AsterRest::new(url, Arc::new(TestSigner::new()), HashMap::new(), 5_000, 10_000, 1_200).unwrap();
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

    use crate::livebot::exec::sign::test_support::TestSigner;
    use crate::livebot::scale::tests::spec;

    fn rest() -> AsterRest {
        let signer = Arc::new(TestSigner::new());
        let mut scales = HashMap::new();
        scales.insert("BTC".into(), (MarketScale::from_spec(&spec()), "BTCUSDT".to_string()));
        AsterRest::new("https://fapi.asterdex.com".into(), signer, scales, 5000, 10_000, 1_200).unwrap()
    }

    #[test]
    fn classify_cancel_distinguishes_success_from_venue_error() {
        // A cancel acks every not-resting outcome; FILLED/EXPIRED is told apart for the fill wait.
        assert_eq!(
            classify_cancel(r#"{"orderId":1,"status":"CANCELED","clientOrderId":"X1"}"#).unwrap(),
            CancelOutcome::Canceled
        );
        assert_eq!(
            classify_cancel(r#"{"code":-2011,"msg":"Unknown order sent."}"#).unwrap(),
            CancelOutcome::AlreadyGone
        );
        assert_eq!(
            classify_cancel(r#"{"orderId":1,"status":"FILLED"}"#).unwrap(),
            CancelOutcome::FilledOrExpired
        );
        assert_eq!(
            classify_cancel(r#"{"orderId":1,"status":"EXPIRED"}"#).unwrap(),
            CancelOutcome::FilledOrExpired
        );
        // A real venue error (HTTP 200 body) must be a FAILURE so the worker emits CancelReject.
        assert!(classify_cancel(r#"{"code":-4000,"msg":"rate limited"}"#).is_err());
        assert!(classify_cancel(r#"{"status":"NEW"}"#).is_err()); // unexpected: cancel didn't take
        assert!(classify_cancel("not json").is_err());
    }

    #[test]
    fn a_refused_amend_leaves_the_order_and_a_gone_order_closes_the_slot() {
        let venue = |status: u16, code: i64| -> Result<String> {
            Err(VenueFailure { status, code: Some(code), body: String::new() }.into())
        };
        let ev = |outcome: Result<String>| classify_amend("X1".into(), outcome);
        assert!(matches!(ev(Ok(r#"{"orderId":7,"status":"NEW","clientOrderId":"X1"}"#.into())),
            ExecEvent::PlaceAck { venue_order_id, .. } if venue_order_id == "7"));
        assert!(matches!(ev(Ok(r#"{"orderId":7,"status":"PARTIALLY_FILLED"}"#.into())), ExecEvent::PlaceAck { .. }));
        assert!(matches!(ev(Ok(r#"{"orderId":7,"status":"CANCELED"}"#.into())), ExecEvent::CancelFilledOrExpired { .. }));
        assert!(matches!(ev(venue(400, -2013)), ExecEvent::CancelFilledOrExpired { .. }));
        assert!(matches!(ev(venue(400, -2036)), ExecEvent::AmendReject { .. }));
        assert!(matches!(ev(venue(503, -1001)), ExecEvent::PlaceUnknown { .. }));
        assert!(matches!(ev(Ok(r#"{"status":"NEW"}"#.into())), ExecEvent::PlaceUnknown { .. }));
    }

    #[tokio::test]
    async fn an_amend_whose_permit_lapsed_cancels_the_order_instead() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            reply_http(&mut stream, "200 OK", r#"{"orderId":1,"status":"CANCELED","clientOrderId":"A"}"#).await;
            request
        });
        let permit = MakerPermit::unguarded();
        assert!(permit.cancel_queued(), "the strategy revoked it");
        let (tx, mut rx) = mpsc::channel(8);
        let amend = ExecCommand::Amend {
            permit, market: "BTC".into(), side: Side::Buy, client_id: "A".into(), price_ticks: 1000, qty_lots: 10,
        };
        process_cmd(amend, false, &tx, &Arc::new(rest_at(&url)), &mut RestCommandLimiter::new(100), &mut None).await;
        assert!(server.await.unwrap().starts_with("DELETE "), "a lapsed permit must not move the order");
        assert!(matches!(rx.try_recv(), Ok(ExecEvent::CancelAck { client_id }) if client_id == "A"));
    }

    fn rest_at(base_url: &str) -> AsterRest {
        let signer = Arc::new(TestSigner::new());
        let mut scales = HashMap::new();
        scales.insert("BTC".into(), (MarketScale::from_spec(&spec()), "BTCUSDT".to_string()));
        AsterRest::new(base_url.into(), signer, scales, 5000, 10_000, 1_200).unwrap()
    }

    #[test]
    fn priority_lane_admits_only_acked_cancels_and_flattens() {
        use super::super::command::is_priority_cmd;
        let m: MarketId = "BTC".into();
        assert!(is_priority_cmd(&ExecCommand::Cancel {
            market: m.clone(),
            client_id: "c".into(),
            venue_order_id: Some("42".into()),
        }));
        // Un-acked cancel: a Place for this id may still be queued — must stay FIFO.
        assert!(!is_priority_cmd(&ExecCommand::Cancel {
            market: m.clone(),
            client_id: "c".into(),
            venue_order_id: None,
        }));
        // Sweeps must run behind queued Places or they don't sweep them.
        assert!(!is_priority_cmd(&ExecCommand::CancelAllBot));
        assert!(!is_priority_cmd(&ExecCommand::Place {
            market: m.clone(),
            side: Side::Buy,
            price_ticks: 1,
            qty_lots: 1,
            client_id: "p".into(), permit: MakerPermit::unguarded(),
        }));
        assert!(!is_priority_cmd(&ExecCommand::RefreshDeadman { market: m }));
        assert!(!is_priority_cmd(&ExecCommand::Shutdown));
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_record_never_sleeps_but_counts_toward_next_ready_at() {
        let mut limiter = RestCommandLimiter::new(2);
        let t0 = tokio::time::Instant::now();
        limiter.record();
        limiter.record();
        limiter.record(); // over the cap: still returns without yielding
        assert_eq!(tokio::time::Instant::now(), t0, "record() must never sleep");
        // A following next_ready_at() must see the recorded stamps and wait out the window.
        let ready_at = limiter.next_ready_at().expect("normal request must wait");
        tokio::time::sleep_until(ready_at).await;
        assert!(limiter.next_ready_at().is_none());
        assert!(
            tokio::time::Instant::now().duration_since(t0) >= Duration::from_secs(60),
            "next_ready_at() must honor timestamps recorded by the priority lane"
        );
    }


    #[tokio::test]
    async fn priority_cancel_interrupts_a_normal_rate_limit_wait() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut first).await;
            reply_http(&mut first, "200 OK", r#"{"orderId":1,"status":"NEW","clientOrderId":"P0"}"#).await;
            drop(first);
            let _ = first_tx.send(());
            let (mut next, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut next).await;
            reply_http(&mut next, "200 OK", r#"{"orderId":2,"status":"CANCELED","clientOrderId":"urgent"}"#).await;
            request
        });
        let mut rest = rest_at(&url);
        rest.max_rest_requests_per_minute = 1;
        let (events_tx, _events_rx) = tokio::sync::mpsc::channel(32);
        let (normal_tx, normal_rx) = tokio::sync::mpsc::channel(32);
        let (priority_tx, priority_rx) = tokio::sync::mpsc::channel(32);
        for id in ["P0", "P1"] {
            normal_tx.send(ExecCommand::Place {
                market: "BTC".into(), side: Side::Buy, price_ticks: 1000,
                qty_lots: 10, client_id: id.into(), permit: MakerPermit::unguarded(),
            }).await.unwrap();
        }
        let worker = tokio::spawn(run_aster_worker(normal_rx, priority_rx, events_tx, rest));
        first_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        priority_tx.send(ExecCommand::Cancel {
            market: "BTC".into(), client_id: "urgent".into(),
            venue_order_id: Some("2".into()),
        }).await.unwrap();
        let request = tokio::time::timeout(Duration::from_secs(1), server).await
            .expect("priority cancel waited behind the 60-second normal limiter").unwrap();
        assert!(request.contains("origClientOrderId=urgent"), "{request}");
        worker.abort();
    }

    #[tokio::test]
    async fn slow_deadman_refresh_does_not_delay_a_queued_place() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            // The two requests race to connect: take them in either order.
            let (mut deadman, mut place) = (None, None);
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let deadman_request = read_http_request(&mut stream).await.contains("countdownCancelAll");
                let slot = if deadman_request { &mut deadman } else { &mut place };
                assert!(slot.replace(stream).is_none(), "two requests of one kind");
            }
            reply_http(place.as_mut().unwrap(), "200 OK", r#"{"orderId":1,"status":"NEW","clientOrderId":"P"}"#).await;
            assert!(tokio::time::timeout(Duration::from_millis(200), listener.accept()).await.is_err(),
                "the second refresh was sent while the first was in flight");
            let _ = release_rx.await;
            reply_http(deadman.as_mut().unwrap(), "200 OK", r#"{"symbol":"BTCUSDT","countdownTime":"5000"}"#).await;
        });
        let (events_tx, mut events_rx) = mpsc::channel(8);
        let (normal_tx, normal_rx) = mpsc::channel(8);
        let (_priority_tx, priority_rx) = mpsc::channel(8);
        normal_tx.send(ExecCommand::RefreshDeadman { market: "BTC".into() }).await.unwrap();
        // A second refresh while the first is in flight is dropped, not queued or sent.
        normal_tx.send(ExecCommand::RefreshDeadman { market: "BTC".into() }).await.unwrap();
        normal_tx.send(ExecCommand::Place {
            market: "BTC".into(), side: Side::Buy, price_ticks: 1000,
            qty_lots: 10, client_id: "P".into(), permit: MakerPermit::unguarded(),
        }).await.unwrap();
        let worker = tokio::spawn(run_aster_worker(normal_rx, priority_rx, events_tx, rest_at(&url)));
        let event = tokio::time::timeout(Duration::from_secs(2), events_rx.recv()).await
            .expect("the place waited behind the dead-man's response");
        assert!(matches!(event, Some(ExecEvent::PlaceAck { client_id, .. }) if client_id == "P"));
        release_tx.send(()).unwrap();
        server.await.unwrap();
        worker.abort();
    }

    #[tokio::test]
    async fn maker_cancelled_during_rate_limit_wait_is_never_sent() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            reply_http(&mut stream, "200 OK", r#"{"orderId":1,"status":"NEW","clientOrderId":"first"}"#).await;
            listener
        });
        let mut rest = rest_at(&url);
        rest.max_rest_requests_per_minute = 1;
        let (events_tx, mut events_rx) = mpsc::channel(8);
        let (normal_tx, normal_rx) = mpsc::channel(8);
        let (_priority_tx, priority_rx) = mpsc::channel(8);
        let queued = MakerPermit::unguarded();
        for (id, permit) in [("first", MakerPermit::unguarded()), ("queued", queued.clone())] {
            normal_tx.send(ExecCommand::Place {
                market: "BTC".into(), side: Side::Buy, price_ticks: 1000,
                qty_lots: 10, client_id: id.into(), permit,
            }).await.unwrap();
        }
        let worker = tokio::spawn(run_aster_worker(normal_rx, priority_rx, events_tx, rest));
        assert!(matches!(events_rx.recv().await, Some(ExecEvent::PlaceAck { client_id, .. }) if client_id == "first"));
        let listener = server.await.unwrap();
        tokio::task::yield_now().await;
        assert!(queued.cancel_queued(), "queued maker was claimed before its rate-limit wait ended");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        let event = tokio::time::timeout(Duration::from_secs(1), events_rx.recv()).await.unwrap();
        assert!(matches!(event, Some(ExecEvent::PlaceReject { client_id, .. }) if client_id == "queued"));
        assert!(tokio::time::timeout(Duration::from_millis(1), listener.accept()).await.is_err(),
            "revoked maker reached the HTTP transport");
        worker.abort();
    }

    #[tokio::test]
    async fn duplicate_claimed_maker_cannot_emit_a_false_unsent_rejection() {
        let permit = MakerPermit::unguarded();
        assert!(permit.try_claim(crate::hotpath::clock::mono_now_ns()));
        let (tx, mut rx) = mpsc::channel(8);
        let mut limiter = RestCommandLimiter::new(100);
        let mut backoff = None;
        let cmd = ExecCommand::Place {
            market: "BTC".into(), side: Side::Buy, price_ticks: 1000,
            qty_lots: 10, client_id: "claimed".into(), permit: permit.clone(),
        };
        process_cmd(cmd.clone(), false, &tx, &Arc::new(rest_at("http://127.0.0.1:9")),
            &mut limiter, &mut backoff).await;
        send_backoff_reject(&tx, cmd, "backoff".into(), 1).await;
        assert!(matches!(rx.try_recv(), Ok(ExecEvent::AsterRateLimited { .. })));
        assert!(rx.try_recv().is_err(), "duplicate must retain the original unresolved ownership");
        assert!(!permit.is_cancelled());
    }

    #[tokio::test]
    async fn priority_cancel_jumps_queued_places() {
        // Nothing listens on this port: every REST call fails fast (connection refused),
        // and the EVENT ORDER exposes the processing order.
        let rest = rest_at("http://127.0.0.1:9");
        let (tx, mut ev_rx) = tokio::sync::mpsc::channel(64);
        let (norm_tx, norm_rx) = tokio::sync::mpsc::channel(64);
        let (prio_tx, prio_rx) = tokio::sync::mpsc::channel(64);
        for i in 0..3 {
            norm_tx
                .send(ExecCommand::Place {
                    market: "BTC".into(),
                    side: Side::Buy,
                    price_ticks: 1000 + i,
                    qty_lots: 10,
                    client_id: format!("P{i}"), permit: MakerPermit::unguarded(),
                })
                .await
                .unwrap();
        }
        prio_tx
            .send(ExecCommand::Cancel {
                market: "BTC".into(),
                client_id: "C-prio".into(),
                venue_order_id: Some("42".into()),
            })
            .await
            .unwrap();
        norm_tx.send(ExecCommand::Shutdown).await.unwrap();

        run_aster_worker(norm_rx, prio_rx, tx, rest).await;

        let first = ev_rx.recv().await.expect("worker must emit events");
        assert!(
            matches!(first, ExecEvent::CancelReject { ref client_id, .. } if client_id == "C-prio"),
            "priority cancel must be processed before the queued places, got {first:?}"
        );
        // The queued places still execute (known pre-send rejection via refused transport).
        let mut places = 0;
        while let Some(ev) = ev_rx.recv().await {
            if matches!(ev, ExecEvent::PlaceReject { .. }) {
                places += 1;
            }
        }
        assert_eq!(places, 3, "normal-lane places must not be dropped");
    }

    #[tokio::test]
    async fn shutdown_drains_commands_queued_behind_it() {
        let rest = rest_at("http://127.0.0.1:9");
        let (tx, mut ev_rx) = tokio::sync::mpsc::channel(64);
        let (norm_tx, norm_rx) = tokio::sync::mpsc::channel(64);
        let (_prio_tx, prio_rx) = tokio::sync::mpsc::channel::<ExecCommand>(64);
        norm_tx.send(ExecCommand::Shutdown).await.unwrap();
        norm_tx
            .send(ExecCommand::Cancel {
                market: "BTC".into(),
                client_id: "C-late".into(),
                venue_order_id: None,
            })
            .await
            .unwrap();

        run_aster_worker(norm_rx, prio_rx, tx, rest).await;

        // The cancel that slipped in behind Shutdown must still be executed.
        let ev = ev_rx.recv().await.expect("drained command must emit its event");
        assert!(
            matches!(ev, ExecEvent::CancelReject { ref client_id, .. } if client_id == "C-late"),
            "expected the drained cancel's outcome, got {ev:?}"
        );
        assert!(ev_rx.recv().await.is_none(), "worker must stop after the drain");
    }

    #[test]
    fn place_params_are_post_only_gtx_one_way() {
        let r = rest();
        // 1000 ticks * 0.1 = 100.0 price; 5 lots * 0.001 = 0.005 qty.
        let p = r.place_params(&"BTC".into(), Side::Buy, 1000, 5, "Xs-BTC-B-0", false).unwrap();
        let map: HashMap<_, _> = p.iter().cloned().collect();
        assert_eq!(map["symbol"], "BTCUSDT");
        assert_eq!(map["side"], "BUY");
        assert_eq!(map["type"], "LIMIT");
        assert_eq!(map["timeInForce"], "GTX"); // post-only
        assert_eq!(map["price"], "100");
        assert_eq!(map["quantity"], "0.005");
        assert_eq!(map["newClientOrderId"], "Xs-BTC-B-0");
        assert_eq!(map["positionSide"], "BOTH"); // one-way mode
        assert!(!map.contains_key("reduceOnly"));
    }

    #[test]
    fn place_params_reduce_only_flag() {
        let r = rest();
        let p = r.place_params(&"BTC".into(), Side::Sell, 1000, 5, "Xs-BTC-S-0", true).unwrap();
        let map: HashMap<_, _> = p.iter().cloned().collect();
        assert_eq!(map["reduceOnly"], "true");
    }

    #[test]
    fn classify_place_ack_on_new() {
        let body = r#"{"orderId":2037568488,"symbol":"HYPEUSDT","status":"NEW","clientOrderId":"Xs-BTC-B-0","price":"100.0"}"#;
        match classify_place("Xs-BTC-B-0", body) {
            ExecEvent::PlaceAck { client_id, venue_order_id } => {
                assert_eq!(client_id, "Xs-BTC-B-0");
                assert_eq!(venue_order_id, "2037568488");
            }
            other => panic!("expected PlaceAck, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_reject_on_expired_post_only() {
        let body = r#"{"orderId":1,"symbol":"HYPEUSDT","status":"EXPIRED","clientOrderId":"x"}"#;
        assert!(matches!(classify_place("x", body), ExecEvent::PlaceReject { .. }));
    }

    #[test]
    fn classify_place_reject_on_error_code() {
        let body = r#"{"code":-4164,"msg":"Order's notional must be no smaller than 5"}"#;
        match classify_place("x", body) {
            ExecEvent::PlaceReject { reason, .. } => assert!(reason.contains("-4164")),
            other => panic!("expected PlaceReject, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_unknown_on_fill_status() {
        let body = r#"{"orderId":1,"symbol":"HYPEUSDT","status":"FILLED","clientOrderId":"x"}"#;
        match classify_place("x", body) {
            ExecEvent::PlaceUnknown { reason, .. } => assert!(reason.contains("fill status")),
            other => panic!("expected PlaceUnknown, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_unknown_on_unparseable_success_body() {
        match classify_place("x", "not json") {
            ExecEvent::PlaceUnknown { reason, .. } => assert!(reason.contains("unparseable")),
            other => panic!("expected PlaceUnknown, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_unknown_on_new_without_order_id() {
        let body = r#"{"status":"NEW","clientOrderId":"x"}"#;
        match classify_place("x", body) {
            ExecEvent::PlaceUnknown { reason, .. } => assert!(reason.contains("missing orderId")),
            other => panic!("expected PlaceUnknown, got {other:?}"),
        }
    }

    #[test]
    fn reject_body_error_passes_success_shapes_through() {
        // Order JSON without a code field (normal place/flatten success echo).
        let order = r#"{"orderId":2037568488,"symbol":"HYPEUSDT","status":"NEW"}"#;
        assert_eq!(reject_body_error("/p", order).unwrap(), order);
        // Cancel-all success echo carries code 200.
        let done = r#"{"code":200,"msg":"The operation of cancel all open order is done."}"#;
        assert_eq!(reject_body_error("/p", done).unwrap(), done);
        // code 0 is a success echo too.
        assert!(reject_body_error("/p", r#"{"code":0}"#).is_ok());
        // Arrays and non-JSON are rejected: an acknowledgement must be a JSON object.
        assert!(reject_body_error("/p", "[]").is_err());
        assert!(reject_body_error("/p", "not json").is_err());
    }

    #[test]
    fn reject_body_error_rejects_venue_error_codes() {
        for (code, msg) in [
            (-1003, "Too many requests."),
            (-2011, "Unknown order sent."),
            (-4164, "Order's notional must be no smaller than 5"),
        ] {
            let body = format!(r#"{{"code":{code},"msg":"{msg}"}}"#);
            let err = reject_body_error("/fapi/v3/order", &body).unwrap_err().to_string();
            assert!(err.contains(&code.to_string()), "error must include code {code}: {err}");
            assert!(err.contains(msg), "error must include msg: {err}");
        }
        let missing = |code: i64| unknown_order(&reject_body_error("/p", &format!(r#"{{"code":{code},"msg":"m"}}"#)).unwrap_err());
        assert!(missing(-2013) && !missing(-2011));
    }

    #[test]
    fn parse_one_way_requires_boolean_field() {
        assert!(parse_one_way(r#"{"dualSidePosition":false}"#).unwrap()); // one-way
        assert!(!parse_one_way(r#"{"dualSidePosition":true}"#).unwrap()); // hedge mode
        // Missing / non-boolean field must FAIL (never default to "one-way OK").
        let err = parse_one_way(r#"{"something":"else"}"#).unwrap_err().to_string();
        assert!(err.contains("dualSidePosition"), "{err}");
        assert!(parse_one_way(r#"{"dualSidePosition":"false"}"#).is_err());
        assert!(parse_one_way("not json").is_err());
        // Venue error envelope (HTTP 200 body) must FAIL, not read as one-way.
        let err = parse_one_way(r#"{"code":-1003,"msg":"Too many requests."}"#).unwrap_err().to_string();
        assert!(err.contains("-1003"), "{err}");
    }

}

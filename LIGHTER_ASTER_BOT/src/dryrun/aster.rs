//! The simulated Aster futures venue: the REST and websocket surface the bot uses, signed as in
//! live and answered by the matching core in Aster's shapes. Request weights, the nonce rules
//! and the error codes follow Aster's v3 API docs (github.com/asterdex/api-docs).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::account::AccountView;
use super::book::Level;
use super::clock::wall_us;
use super::matching::{End, Envelope, Event, Fill, Order, OrderRef, OrderSpec, Reject, Reply, Request, Status, Tif, Venue};
use super::server::{self, Handler, Response};
use super::Venues;
use crate::decimal::parse_dec;
use crate::livebot::exec::crypto::{address_hex, aster_recover_signer, keccak256};
use crate::types::Side;

/// A listen key lives this long past its last keepalive.
const LISTEN_KEY_TTL_US: i64 = 60 * 60 * 1_000_000;
/// A nonce must be within this distance of the venue's clock.
const NONCE_WINDOW_US: i64 = 60 * 1_000_000;
/// Nonces remembered per API wallet.
const NONCES_KEPT: usize = 100;

/// An Aster error: HTTP status, code and message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fail(u16, i64, &'static str);

const MALFORMED: Fail = Fail(400, -1102, "A mandatory parameter was not sent, was empty/null, or malformed.");
const BAD_SIGNATURE: Fail = Fail(400, -1022, "Signature for this request is not valid.");
const NONCE_EXPIRED: Fail = Fail(400, -4225, "Nonce Expired");
const NO_SUCH_ORDER: Fail = Fail(400, -2013, "Order does not exist.");
const NOT_FOUND: Fail = Fail(404, -1000, "The dry-run venue does not serve this endpoint.");

impl Fail {
    fn response(self) -> Response {
        Response::json(self.0, json!({"code": self.1, "msg": self.2}).to_string())
    }
}

fn fail(reject: Reject, side: Option<Side>) -> Fail {
    match reject {
        Reject::RateLimited => Fail(429, -1003, "Too many requests queued."),
        // Aster's 503: sent but unanswered, so the outcome is unknown to the client.
        Reject::Unavailable => Fail(503, -1001, "Internal error; unable to process your request. Please try again."),
        Reject::UnknownMarket => Fail(400, -1121, "Invalid symbol."),
        Reject::UnknownOrder => NO_SUCH_ORDER,
        Reject::TickSize => Fail(400, -4014, "Price not increased by tick size."),
        Reject::StepSize => Fail(400, -4023, "Qty not increased by step size."),
        Reject::MinQty => Fail(400, -4004, "Quantity less than min quantity."),
        Reject::MinNotional => Fail(400, -4164, "Order's notional must be no smaller than 5.0 (unless you choose reduce only)"),
        Reject::PriceBand if side == Some(Side::Sell) => Fail(400, -4024, "Price is lower than mark price multiplier floor."),
        Reject::PriceBand => Fail(400, -4016, "Price is higher than mark price multiplier cap."),
        Reject::ReduceOnly => Fail(400, -2022, "ReduceOnly Order is rejected."),
        Reject::Margin => Fail(400, -2019, "Margin is insufficient."),
        Reject::WouldCross => Fail(400, -2036, "order modification price cross."),
        Reject::BadNonce => NONCE_EXPIRED,
    }
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn status_str(status: Status) -> &'static str {
    match status {
        Status::New => "NEW",
        Status::PartiallyFilled => "PARTIALLY_FILLED",
        Status::Filled => "FILLED",
        // An IOC remainder and a post-only order that would have taken both expire.
        Status::Done(End::Ioc | End::PostOnly) => "EXPIRED",
        Status::Done(End::Rejected(_)) => "REJECTED",
        Status::Done(_) => "CANCELED",
    }
}

fn kind(order: &Order) -> &'static str {
    if order.price.is_some() { "LIMIT" } else { "MARKET" }
}

fn tif_str(tif: Tif) -> &'static str {
    match tif {
        Tif::Gtc => "GTC",
        Tif::Ioc => "IOC",
        Tif::PostOnly => "GTX",
    }
}

fn avg_price(order: &Order) -> Decimal {
    if order.filled.is_zero() { Decimal::ZERO } else { (order.filled_quote / order.filled).round_dp(8).normalize() }
}

fn order_json(o: &Order) -> Value {
    json!({
        "orderId": o.id,
        "symbol": o.market,
        "status": status_str(o.status),
        "clientOrderId": o.client_id,
        "price": o.price.unwrap_or_default().to_string(),
        "avgPrice": avg_price(o).to_string(),
        "origQty": o.qty.to_string(),
        "executedQty": o.filled.to_string(),
        "cumQty": o.filled.to_string(),
        "cumQuote": o.filled_quote.to_string(),
        "timeInForce": tif_str(o.tif),
        "type": kind(o),
        "origType": kind(o),
        "reduceOnly": o.reduce_only,
        "closePosition": false,
        "side": side_str(o.side),
        "positionSide": "BOTH",
        "stopPrice": "0",
        "workingType": "CONTRACT_PRICE",
        "priceProtect": false,
        "time": o.created_us / 1_000,
        "updateTime": o.updated_us / 1_000,
    })
}

fn trade_json(f: &Fill) -> Value {
    json!({
        "symbol": f.market,
        "id": f.id,
        "orderId": f.order_id,
        "side": side_str(f.side),
        "price": f.price.to_string(),
        "qty": f.qty.to_string(),
        "realizedPnl": f.realized.to_string(),
        "quoteQty": (f.price * f.qty).to_string(),
        "commission": f.fee.to_string(),
        "commissionAsset": "USDT",
        "time": f.at_us / 1_000,
        "positionSide": "BOTH",
        "buyer": f.side == Side::Buy,
        "maker": f.maker,
    })
}

/// The user-data frame for an order event (fills, acks and ends); other events have none the
/// bot reads.
fn order_update(event: &Event) -> Option<String> {
    let Event::Order { order, fill, .. } = event else { return None };
    let (execution, last_qty, last_price, trade_id, maker, realized) = match fill {
        Some(f) => ("TRADE", f.qty, f.price, f.id, f.maker, f.realized),
        None => {
            let execution = match order.status {
                Status::New => "NEW",
                Status::Done(End::Ioc | End::PostOnly) => "EXPIRED",
                _ => "CANCELED",
            };
            (execution, Decimal::ZERO, Decimal::ZERO, 0, false, Decimal::ZERO)
        }
    };
    let mut o = json!({
        "s": order.market,
        "c": order.client_id,
        "S": side_str(order.side),
        "o": kind(order),
        "f": tif_str(order.tif),
        "q": order.qty.to_string(),
        "p": order.price.unwrap_or_default().to_string(),
        "ap": avg_price(order).to_string(),
        "sp": "0",
        "x": execution,
        "X": status_str(order.status),
        "i": order.id,
        "l": last_qty.to_string(),
        "z": order.filled.to_string(),
        "L": last_price.to_string(),
        "T": order.updated_us / 1_000,
        "t": trade_id,
        "b": "0",
        "a": "0",
        "m": maker,
        "R": order.reduce_only,
        "wt": "CONTRACT_PRICE",
        "ot": kind(order),
        "ps": "BOTH",
        "cp": false,
        "rp": realized.to_string(),
    });
    // The venue omits the commission when there is none.
    if let Some(fee) = fill.as_ref().map(|f| f.fee).filter(|fee| !fee.is_zero()) {
        o["N"] = json!("USDT");
        o["n"] = json!(fee.to_string());
    }
    Some(json!({"e": "ORDER_TRADE_UPDATE", "E": wall_us() / 1_000, "T": order.updated_us / 1_000, "o": o}).to_string())
}

fn levels_json(levels: &[Level], limit: usize) -> Value {
    levels.iter().take(limit).map(|(price, size)| json!([price.to_string(), size.to_string()])).collect()
}

/// The simulated Aster venue.
pub struct Aster {
    venues: Venues,
    /// The real `exchangeInfo` body, fetched once at start.
    exchange_info: String,
    /// The traded symbols (`HYPEUSDT`).
    symbols: Vec<String>,
    leverage: Decimal,
    /// The account's listen key and its expiry (µs).
    listen_key: Mutex<Option<(String, i64)>>,
    /// Recent nonces per API wallet.
    nonces: Mutex<HashMap<String, BTreeSet<i64>>>,
}

impl Aster {
    pub fn new(venues: Venues, exchange_info: String, symbols: Vec<String>, leverage: Decimal) -> Self {
        Self { venues, exchange_info, symbols, leverage, listen_key: Mutex::new(None), nonces: Mutex::new(HashMap::new()) }
    }

    async fn call(&self, lane: u64, weight: u32, orders: u32, request: Request) -> Result<Reply, Fail> {
        let side = match &request {
            Request::Place(spec) => Some(spec.side),
            _ => None,
        };
        match self.venues.call(Envelope { venue: Venue::Aster, lane, weight, orders, nonce: None, request }).await {
            Reply::Reject(reject) => Err(fail(reject, side)),
            reply => Ok(reply),
        }
    }

    async fn account(&self, lane: u64) -> Result<AccountView, Fail> {
        match self.call(lane, 5, 0, Request::Account).await? {
            Reply::Account(view) => Ok(view),
            _ => Err(fail(Reject::Unavailable, None)),
        }
    }

    /// Checks a signed request's signature and nonce, as the venue does before anything else.
    fn verify(&self, request: &server::Request, params: &BTreeMap<String, String>) -> Result<(), Fail> {
        let raw = if request.method == "GET" { request.query.clone() } else { String::from_utf8_lossy(&request.body).into_owned() };
        let (signed, signature) = raw.rsplit_once("&signature=").ok_or(BAD_SIGNATURE)?;
        let signer = params.get("signer").ok_or(MALFORMED)?;
        let recovered = aster_recover_signer(signed, signature).map_err(|_| BAD_SIGNATURE)?;
        if !address_hex(&recovered).eq_ignore_ascii_case(signer) {
            tracing::warn!("dry-run Aster: {} {} signed by {} but claims signer {signer}", request.method, request.path, address_hex(&recovered));
            return Err(BAD_SIGNATURE);
        }
        let nonce: i64 = params.get("nonce").and_then(|n| n.parse().ok()).ok_or(MALFORMED)?;
        let mut nonces = self.nonces.lock().expect("nonce state poisoned");
        let recent = nonces.entry(signer.to_lowercase()).or_default();
        let expired = (nonce - wall_us()).abs() > NONCE_WINDOW_US
            || recent.contains(&nonce)
            || (recent.len() >= NONCES_KEPT && recent.first().is_some_and(|&oldest| nonce < oldest));
        if expired {
            tracing::warn!("dry-run Aster: {} {} with a stale or reused nonce {nonce}", request.method, request.path);
            return Err(NONCE_EXPIRED);
        }
        recent.insert(nonce);
        if recent.len() > NONCES_KEPT {
            recent.pop_first();
        }
        Ok(())
    }

    fn listen_key_live(&self, key: &str) -> bool {
        let slot = self.listen_key.lock().expect("listen key poisoned");
        slot.as_ref().is_some_and(|(live, deadline)| live == key && *deadline > wall_us())
    }

    async fn route(&self, lane: u64, request: &server::Request) -> Result<String, Fail> {
        let params = request.params();
        let get = |key: &str| params.get(key).map(String::as_str);
        let decimal = |key: &str| get(key).and_then(|v| parse_dec(v).ok()).ok_or(MALFORMED);
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/fapi/v3/exchangeInfo") => {
                self.call(lane, 1, 0, Request::Noop).await?;
                return Ok(self.exchange_info.clone());
            }
            ("GET", "/fapi/v3/depth") => {
                let symbol = get("symbol").ok_or(MALFORMED)?;
                let limit: usize = get("limit").and_then(|l| l.parse().ok()).unwrap_or(500);
                let weight = match limit {
                    0..=50 => 2,
                    51..=100 => 5,
                    101..=500 => 10,
                    _ => 20,
                };
                let Reply::Book { bids, asks, at_us } = self.call(lane, weight, 0, Request::Book { market: symbol.to_string() }).await? else {
                    return Err(fail(Reject::Unavailable, None));
                };
                let ms = at_us / 1_000;
                let body = json!({"lastUpdateId": at_us, "E": ms, "T": ms, "bids": levels_json(&bids, limit), "asks": levels_json(&asks, limit)});
                return Ok(body.to_string());
            }
            _ => {}
        }
        // Every endpoint served past here is signed (the bot signs all of them), so an unsigned
        // request is for a public endpoint this venue lacks: 404 and a WARN, not a signature error.
        if !params.contains_key("signature") {
            return Err(NOT_FOUND);
        }
        self.verify(request, &params)?;
        let symbol = get("symbol");
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/fapi/v3/balance") => {
                let view = self.account(lane).await?;
                Ok(json!([{
                    "accountAlias": "dryrun",
                    "asset": "USDT",
                    "balance": view.balance.to_string(),
                    "crossWalletBalance": view.balance.to_string(),
                    "crossUnPnl": view.unrealized.to_string(),
                    "availableBalance": view.available.to_string(),
                    "maxWithdrawAmount": view.available.max(Decimal::ZERO).to_string(),
                    "marginAvailable": true,
                    "updateTime": wall_us() / 1_000,
                }]).to_string())
            }
            ("GET", "/fapi/v3/account") => {
                let view = self.account(lane).await?;
                Ok(json!({
                    "feeTier": 0,
                    "canTrade": true,
                    "canDeposit": true,
                    "canWithdraw": true,
                    "totalInitialMargin": view.margin.to_string(),
                    "totalWalletBalance": view.balance.to_string(),
                    "totalUnrealizedProfit": view.unrealized.to_string(),
                    "totalMarginBalance": view.equity.to_string(),
                    "totalCrossWalletBalance": view.balance.to_string(),
                    "totalCrossUnPnl": view.unrealized.to_string(),
                    "availableBalance": view.available.to_string(),
                    "maxWithdrawAmount": view.available.max(Decimal::ZERO).to_string(),
                    "updateTime": wall_us() / 1_000,
                }).to_string())
            }
            ("GET", "/fapi/v3/positionRisk") => {
                let view = self.account(lane).await?;
                let rows: Vec<Value> = self.symbols.iter()
                    .filter(|s| symbol.is_none_or(|wanted| wanted == s.as_str()))
                    .map(|s| {
                        let p = view.positions.iter().find(|p| &p.market == s);
                        let (qty, entry, mark, pnl) = p.map_or(Default::default(), |p| (p.qty, p.entry, p.mark, p.unrealized));
                        json!({
                            "symbol": s,
                            "positionAmt": qty.to_string(),
                            "entryPrice": entry.to_string(),
                            "markPrice": mark.to_string(),
                            "unRealizedProfit": pnl.to_string(),
                            "liquidationPrice": "0",
                            "leverage": self.leverage.to_string(),
                            "marginType": "cross",
                            "isolatedMargin": "0",
                            "isAutoAddMargin": "false",
                            "positionSide": "BOTH",
                            "notional": (qty * mark).to_string(),
                            "updateTime": wall_us() / 1_000,
                        })
                    })
                    .collect();
                Ok(Value::from(rows).to_string())
            }
            ("GET", "/fapi/v3/positionSide/dual") => {
                self.call(lane, 30, 0, Request::Noop).await?;
                Ok(json!({"dualSidePosition": false}).to_string())
            }
            ("GET", "/fapi/v3/openOrders") => {
                let weight = if symbol.is_some() { 1 } else { 40 };
                let Reply::Orders(orders) = self.call(lane, weight, 0, Request::OpenOrders { market: symbol.map(str::to_string) }).await? else {
                    return Err(fail(Reject::Unavailable, None));
                };
                Ok(Value::from(orders.iter().map(order_json).collect::<Vec<_>>()).to_string())
            }
            (method @ ("GET" | "DELETE" | "PUT"), "/fapi/v3/order") => {
                let symbol = symbol.ok_or(MALFORMED)?.to_string();
                let order = match (get("origClientOrderId"), get("orderId").and_then(|id| id.parse().ok())) {
                    (Some(client_id), _) => OrderRef::Client(client_id.to_string()),
                    (None, Some(id)) => OrderRef::Id(id),
                    (None, None) => return Err(MALFORMED),
                };
                let request = match method {
                    "DELETE" => Request::Cancel { market: symbol, order },
                    "PUT" => Request::Amend { market: symbol, order, qty: decimal("quantity")?, price: decimal("price")? },
                    _ => Request::Order { order },
                };
                // An amend counts against the order limits.
                match self.call(lane, 1, u32::from(method == "PUT"), request).await {
                    Ok(Reply::Order(order)) => Ok(order_json(&order).to_string()),
                    // A cancel of an order that is no longer open.
                    Err(NO_SUCH_ORDER) if method == "DELETE" => Err(Fail(400, -2011, "Unknown order sent.")),
                    Err(fail) => Err(fail),
                    Ok(_) => Err(fail(Reject::Unavailable, None)),
                }
            }
            ("POST", "/fapi/v3/order") => {
                let side = match get("side") {
                    Some("BUY") => Side::Buy,
                    Some("SELL") => Side::Sell,
                    _ => return Err(MALFORMED),
                };
                let (price, tif) = match get("type") {
                    Some("LIMIT") => {
                        let tif = match get("timeInForce").unwrap_or("GTC") {
                            "GTC" => Tif::Gtc,
                            "IOC" => Tif::Ioc,
                            "GTX" => Tif::PostOnly,
                            _ => return Err(Fail(400, -1115, "Invalid timeInForce.")),
                        };
                        (Some(decimal("price")?), tif)
                    }
                    Some("MARKET") => (None, Tif::Ioc),
                    _ => return Err(Fail(400, -1116, "Invalid orderType.")),
                };
                if get("positionSide").is_some_and(|p| p != "BOTH") {
                    return Err(Fail(400, -4061, "Order's position side does not match user's setting."));
                }
                let spec = OrderSpec {
                    market: symbol.ok_or(MALFORMED)?.to_string(),
                    client_id: get("newClientOrderId").map_or_else(|| format!("dryrun-{}", wall_us()), str::to_string),
                    side,
                    qty: decimal("quantity")?,
                    price,
                    tif,
                    reduce_only: get("reduceOnly") == Some("true"),
                };
                // New orders weigh nothing on the IP limit; they count against the order limits.
                match self.call(lane, 0, 1, Request::Place(spec)).await? {
                    Reply::Order(order) => Ok(order_json(&order).to_string()),
                    _ => Err(fail(Reject::Unavailable, None)),
                }
            }
            ("DELETE", "/fapi/v3/allOpenOrders") => {
                self.call(lane, 1, 0, Request::CancelAll { market: Some(symbol.ok_or(MALFORMED)?.to_string()) }).await?;
                Ok(json!({"code": 200, "msg": "The operation of cancel all open order is done."}).to_string())
            }
            ("POST", "/fapi/v3/countdownCancelAll") => {
                let market = symbol.ok_or(MALFORMED)?.to_string();
                let countdown_ms: i64 = get("countdownTime").and_then(|t| t.parse().ok()).ok_or(MALFORMED)?;
                self.call(lane, 10, 0, Request::Deadman { market: market.clone(), countdown_ms }).await?;
                Ok(json!({"symbol": market, "countdownTime": countdown_ms.to_string()}).to_string())
            }
            ("GET", "/fapi/v3/userTrades") => {
                let Reply::Fills(fills) = self.call(lane, 5, 0, Request::Fills { market: symbol.map(str::to_string) }).await? else {
                    return Err(fail(Reject::Unavailable, None));
                };
                let order_id: Option<u64> = get("orderId").and_then(|id| id.parse().ok());
                let limit: usize = get("limit").and_then(|l| l.parse().ok()).unwrap_or(500);
                let rows: Vec<Value> = fills.iter().filter(|f| order_id.is_none_or(|id| f.order_id == id)).take(limit).map(trade_json).collect();
                Ok(Value::from(rows).to_string())
            }
            (method @ ("POST" | "PUT" | "DELETE"), "/fapi/v3/listenKey") => {
                self.call(lane, 1, 0, Request::Noop).await?;
                let now = wall_us();
                let mut slot = self.listen_key.lock().expect("listen key poisoned");
                let live = slot.as_ref().filter(|(_, deadline)| *deadline > now).map(|(key, _)| key.clone());
                match (method, live) {
                    // One key per account: a new request extends the live one.
                    ("POST", live) => {
                        let key = live.unwrap_or_else(|| hex::encode(keccak256(format!("listen key {now}").as_bytes())));
                        *slot = Some((key.clone(), now + LISTEN_KEY_TTL_US));
                        Ok(json!({"listenKey": key}).to_string())
                    }
                    ("PUT", Some(key)) => {
                        *slot = Some((key, now + LISTEN_KEY_TTL_US));
                        Ok("{}".into())
                    }
                    ("PUT", None) => Err(Fail(400, -1125, "This listenKey does not exist.")),
                    _ => {
                        *slot = None;
                        Ok("{}".into())
                    }
                }
            }
            _ => Err(NOT_FOUND),
        }
    }

    /// Public streams, as a Tokyo bot receives them.
    async fn market(&self, lane: u64, streams: &[String], combined: bool, mut ws: WebSocketStream<TcpStream>) {
        let mut follower = self.venues.follower(Venue::Aster, lane, combined);
        for stream in streams {
            if !follower.subscribe(stream) {
                tracing::warn!("dry-run Aster: the upstream does not carry {stream}; the connection gets nothing for it");
            }
        }
        loop {
            tokio::select! {
                incoming = ws.next() => match incoming {
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                    // Pings are answered by the socket as it is read.
                    Some(Ok(_)) => {}
                },
                frame = follower.next() => match frame {
                    Some(text) => {
                        if ws.send(Message::Text(text.to_string())).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        let _ = ws.close(None).await;
                        return;
                    }
                },
            }
        }
    }

    /// The user-data stream of listen key `key`.
    async fn user_stream(&self, key: &str, mut ws: WebSocketStream<TcpStream>) {
        if !self.listen_key_live(key) {
            let _ = ws.close(None).await;
            return;
        }
        let mut events = self.venues.events(Venue::Aster);
        let mut check = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                incoming = ws.next() => match incoming {
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                    Some(Ok(_)) => {}
                },
                event = events.recv() => match event {
                    Ok(event) => {
                        if let Some(frame) = order_update(&event) {
                            if ws.send(Message::Text(frame)).await.is_err() {
                                return;
                            }
                        }
                    }
                    // Fell too far behind: dropped, as a stuck consumer would be.
                    Err(_) => {
                        let _ = ws.close(None).await;
                        return;
                    }
                },
                _ = check.tick() => {
                    if !self.listen_key_live(key) {
                        let expired = json!({"e": "listenKeyExpired", "E": wall_us() / 1_000, "listenKey": key});
                        let _ = ws.send(Message::Text(expired.to_string())).await;
                        let _ = ws.close(None).await;
                        return;
                    }
                }
            }
        }
    }
}

impl Handler for Aster {
    async fn rest(&self, lane: u64, request: server::Request) -> Response {
        match self.route(lane, &request).await {
            Ok(body) => Response::json(200, body),
            Err(fail) => {
                if fail == NOT_FOUND {
                    tracing::warn!("dry-run Aster: no route for {} {}", request.method, request.path);
                }
                fail.response()
            }
        }
    }

    async fn websocket(&self, lane: u64, request: server::Request, ws: WebSocketStream<TcpStream>) {
        if request.path == "/stream" {
            let streams: Vec<String> = request.params().get("streams").map(|s| s.split('/').map(str::to_string).collect()).unwrap_or_default();
            return self.market(lane, &streams, true, ws).await;
        }
        match request.path.strip_prefix("/ws/") {
            Some(stream) if stream.contains('@') => self.market(lane, &[stream.to_string()], false, ws).await,
            Some(key) => self.user_stream(key, ws).await,
            None => tracing::warn!("dry-run Aster: no websocket at {}", request.path),
        }
    }
}

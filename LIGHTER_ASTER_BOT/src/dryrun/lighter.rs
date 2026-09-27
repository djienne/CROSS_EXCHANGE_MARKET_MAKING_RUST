//! The simulated Lighter venue: the REST and `/stream` websocket surface the bot uses, with
//! orders signed by the real signer library and answered by the matching core in Lighter's
//! shapes. A transaction is answered when the sequencer accepts it; what it did shows only on
//! the account streams, as on the venue. Signatures and auth tokens are not checked: the
//! library that makes them has no verifier.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::account::AccountView;
use super::book::Level;
use super::clock::wall_us;
use super::feed::Follower;
use super::matching::{End, Envelope, Event, Fill, Order, OrderSpec, Reject, Reply, Request, Status, Tif, Venue};
use super::server::{self, Handler, Response};
use super::Venues;
use crate::lighter::messages::OrderBookDetail;
use crate::livebot::exec::creds::LighterCreds;
use crate::livebot::exec::crypto::keccak256;
use crate::types::Side;

/// The signer's CreateOrder transaction type.
const TX_CREATE_ORDER: i64 = 14;

/// A Lighter error: HTTP status, code and message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fail(u16, i64, &'static str);

const MALFORMED: Fail = Fail(400, 20001, "invalid param");
const NO_AUTH: Fail = Fail(400, 20013, "invalid auth");
const NOT_FOUND: Fail = Fail(404, 404, "the dry-run venue does not serve this endpoint");

impl Fail {
    fn response(self) -> Response {
        Response::json(self.0, json!({"code": self.1, "message": self.2}).to_string())
    }

    /// The same error as the answer to a transaction frame.
    fn tx_answer(self) -> String {
        json!({"type": "jsonapi/sendtxbatch", "error": {"code": self.1, "message": self.2}}).to_string()
    }
}

fn fail(reject: Reject) -> Fail {
    match reject {
        Reject::RateLimited => Fail(429, 23000, "Too Many Requests!"),
        Reject::Unavailable => Fail(503, 29500, "service temporarily unavailable"),
        Reject::UnknownMarket => Fail(400, 21100, "invalid market index"),
        Reject::BadNonce => Fail(400, 21104, "invalid nonce"),
        _ => Fail(400, 21000, "invalid order"),
    }
}

fn status_str(order: &Order) -> &'static str {
    match order.status {
        Status::New | Status::PartiallyFilled => "open",
        Status::Filled => "filled",
        Status::Done(End::Canceled) => "canceled",
        Status::Done(End::Deadman) => "canceled-expired",
        Status::Done(End::Ioc) => "canceled-not-enough-liquidity",
        Status::Done(End::PostOnly) => "canceled-post-only",
        Status::Done(End::ReduceOnly | End::Rejected(Reject::ReduceOnly)) => "canceled-reduce-only",
        Status::Done(End::Rejected(Reject::Margin)) => "canceled-margin-not-allowed",
        Status::Done(End::Rejected(_)) => "canceled",
    }
}

fn int(s: &str) -> i64 {
    s.parse().unwrap_or_default()
}

fn order_json(o: &Order, account: i64) -> Value {
    json!({
        "order_index": o.id,
        "client_order_index": int(&o.client_id),
        "order_id": o.id.to_string(),
        "client_order_id": o.client_id,
        "market_index": int(&o.market),
        "owner_account_index": account,
        "initial_base_amount": o.qty.to_string(),
        "price": o.price.unwrap_or_default().to_string(),
        "remaining_base_amount": o.remaining().to_string(),
        "filled_base_amount": o.filled.to_string(),
        "filled_quote_amount": o.filled_quote.to_string(),
        "is_ask": o.side == Side::Sell,
        "side": if o.side == Side::Sell { "sell" } else { "buy" },
        "type": "limit",
        "time_in_force": match o.tif {
            Tif::Ioc => "immediate-or-cancel",
            Tif::Gtc => "good-till-time",
            Tif::PostOnly => "post-only",
        },
        "reduce_only": o.reduce_only,
        "trigger_price": "0",
        "order_expiry": 0,
        "status": status_str(o),
        "timestamp": o.created_us / 1_000_000,
        "updated_at": o.updated_us / 1_000,
    })
}

fn stats_json(view: &AccountView) -> Value {
    let notional: Decimal = view.positions.iter().map(|p| (p.qty * p.mark).abs()).sum();
    let ratio = |x: Decimal| if view.equity > Decimal::ZERO { (x / view.equity).round_dp(4) } else { Decimal::ZERO };
    json!({
        "collateral": view.balance.to_string(),
        // Collateral-style: the venue leaves open positions' uPnL out of it.
        "portfolio_value": view.balance.to_string(),
        "leverage": ratio(notional).to_string(),
        "available_balance": view.available.to_string(),
        "margin_usage": (ratio(view.margin) * Decimal::ONE_HUNDRED).to_string(),
        "buying_power": view.available.max(Decimal::ZERO).to_string(),
    })
}

/// The account channels a connection can follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    All,
    Positions,
    Orders,
    Stats,
}

impl Kind {
    fn parse(channel: &str) -> Option<(Kind, i64)> {
        let (name, account) = channel.split_once('/')?;
        let kind = match name {
            "account_all" => Kind::All,
            "account_all_positions" => Kind::Positions,
            "account_all_orders" => Kind::Orders,
            "user_stats" => Kind::Stats,
            _ => return None,
        };
        Some((kind, account.parse().ok()?))
    }

    fn name(self) -> &'static str {
        match self {
            Kind::All => "account_all",
            Kind::Positions => "account_all_positions",
            Kind::Orders => "account_all_orders",
            Kind::Stats => "user_stats",
        }
    }
}

/// One connection's account subscriptions, and the open orders it has shown the bot.
#[derive(Default)]
struct Private {
    channels: Vec<(Kind, i64)>,
    events: Option<broadcast::Receiver<Arc<Event>>>,
    open: BTreeMap<u64, Order>,
}

async fn next_event(events: &mut Option<broadcast::Receiver<Arc<Event>>>) -> Result<Arc<Event>, RecvError> {
    match events {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// The simulated Lighter venue.
pub struct Lighter {
    venues: Venues,
    /// The real `orderBooks` body, fetched once at start.
    order_books: String,
    /// The traded markets, by market index.
    markets: BTreeMap<String, OrderBookDetail>,
    /// Maker and taker fee rates in millionths, as the venue reports them on trades.
    fees_ppm: [i64; 2],
    leverage: Decimal,
    identity: LighterCreds,
}

impl Lighter {
    pub fn new(venues: Venues, order_books: String, markets: Vec<OrderBookDetail>, fees: [Decimal; 2], leverage: Decimal) -> Self {
        let ppm = |rate: Decimal| (rate * Decimal::from(1_000_000)).round().try_into().unwrap_or_default();
        Self {
            venues,
            order_books,
            markets: markets.into_iter().map(|m| (m.market_id.to_string(), m)).collect(),
            fees_ppm: [ppm(fees[0]), ppm(fees[1])],
            leverage,
            identity: LighterCreds::dry_run(),
        }
    }

    async fn call(&self, lane: u64, request: Request) -> Result<Reply, Fail> {
        match self.venues.call(Envelope { venue: Venue::Lighter, lane, weight: 1, orders: 0, nonce: None, request }).await {
            Reply::Reject(reject) => Err(fail(reject)),
            reply => Ok(reply),
        }
    }

    fn position_json(&self, view: &AccountView, market: &str) -> Value {
        let (qty, entry, mark, pnl) = view.positions.iter()
            .find(|p| p.market == market)
            .map_or(Default::default(), |p| (p.qty, p.entry, p.mark, p.unrealized));
        json!({
            "market_id": int(market),
            "symbol": self.markets.get(market).map_or("", |m| m.symbol.as_str()),
            // Leverage is 100 / this.
            "initial_margin_fraction": (Decimal::ONE_HUNDRED / self.leverage).round_dp(2).to_string(),
            "open_order_count": 0,
            "pending_order_count": 0,
            "position_tied_order_count": 0,
            "sign": if qty < Decimal::ZERO { -1 } else { 1 },
            "position": qty.abs().to_string(),
            "avg_entry_price": entry.to_string(),
            "position_value": (qty.abs() * mark).to_string(),
            "unrealized_pnl": pnl.to_string(),
            "realized_pnl": "0",
            "liquidation_price": "0",
            "margin_mode": 0,
            "allocated_margin": "0",
        })
    }

    fn positions_json(&self, view: &AccountView) -> Value {
        self.markets.keys().map(|m| (m.clone(), self.position_json(view, m))).collect::<serde_json::Map<_, _>>().into()
    }

    /// Every traded market's open orders, plus `changed` where it just finished: each message
    /// is the complete live set.
    fn orders_json(&self, open: &BTreeMap<u64, Order>, changed: Option<&Order>, account: i64) -> Value {
        let mut markets: serde_json::Map<String, Value> = self.markets.keys().map(|m| (m.clone(), json!([]))).collect();
        let done = changed.filter(|o| !o.working());
        for order in open.values().chain(done) {
            if let Some(Value::Array(rows)) = markets.get_mut(&order.market) {
                rows.push(order_json(order, account));
            }
        }
        markets.into()
    }

    fn trade_json(&self, fill: &Fill, account: i64) -> Value {
        let ours_ask = fill.side == Side::Sell;
        let (ours, theirs) = ((fill.order_id, int(&fill.client_id), account), (0, 0, 0));
        let (ask, bid) = if ours_ask { (ours, theirs) } else { (theirs, ours) };
        json!({
            "trade_id": fill.id,
            "tx_hash": hex::encode(keccak256(format!("trade {}", fill.id).as_bytes())),
            "type": "trade",
            "market_id": int(&fill.market),
            "size": fill.qty.to_string(),
            "price": fill.price.to_string(),
            "usd_amount": (fill.qty * fill.price).to_string(),
            "ask_id": ask.0,
            "bid_id": bid.0,
            "ask_client_id": ask.1,
            "bid_client_id": bid.1,
            "ask_account_id": ask.2,
            "bid_account_id": bid.2,
            "is_maker_ask": fill.maker == ours_ask,
            "block_height": 0,
            "timestamp": fill.at_us / 1_000,
            "transaction_time": fill.at_us,
            "maker_fee": self.fees_ppm[0],
            "taker_fee": self.fees_ppm[1],
        })
    }

    fn snapshot(&self, (kind, account): (Kind, i64), view: &AccountView, open: &BTreeMap<u64, Order>) -> String {
        let channel = format!("{}:{account}", kind.name());
        let kind_type = format!("subscribed/{}", kind.name());
        match kind {
            Kind::All => json!({"type": kind_type, "channel": channel, "account": account, "trades": {}, "positions": self.positions_json(view)}),
            Kind::Positions => json!({"type": kind_type, "channel": channel, "positions": self.positions_json(view)}),
            Kind::Orders => json!({"type": kind_type, "channel": channel, "orders": self.orders_json(open, None, account)}),
            Kind::Stats => json!({"type": kind_type, "channel": channel, "stats": stats_json(view)}),
        }
        .to_string()
    }

    /// What `event` sends on each account channel the connection follows.
    fn updates(&self, event: &Event, private: &mut Private) -> Vec<String> {
        let (order, fill, view) = match event {
            Event::Order { order, fill, account } => {
                if order.working() {
                    private.open.insert(order.id, order.clone());
                } else {
                    private.open.remove(&order.id);
                }
                (Some(order), fill.as_ref(), account)
            }
            Event::Funding { account, .. } => (None, None, account),
        };
        let mut out = Vec::new();
        for &(kind, account) in &private.channels {
            let channel = format!("{}:{account}", kind.name());
            let kind_type = format!("update/{}", kind.name());
            let frame = match (kind, order, fill) {
                (Kind::All, _, Some(fill)) => json!({
                    "type": kind_type, "channel": channel, "account": account,
                    "trades": {fill.market.clone(): [self.trade_json(fill, account)]},
                    "positions": {fill.market.clone(): self.position_json(view, &fill.market)},
                }),
                (Kind::Positions, _, Some(fill)) => json!({
                    "type": kind_type, "channel": channel,
                    "positions": {fill.market.clone(): self.position_json(view, &fill.market)},
                }),
                (Kind::Orders, Some(order), _) => json!({
                    "type": kind_type, "channel": channel, "orders": self.orders_json(&private.open, Some(order), account),
                }),
                (Kind::Stats, ..) => json!({"type": kind_type, "channel": channel, "stats": stats_json(view)}),
                _ => continue,
            };
            out.push(frame.to_string());
        }
        out
    }

    /// The orders in one `jsonapi/sendtxbatch` payload, each bound for the sequencer.
    fn batch(&self, lane: u64, data: &Value) -> Result<(Vec<Envelope>, Vec<String>), Fail> {
        let strings = |key: &str| data.get(key).and_then(Value::as_str).ok_or(MALFORMED);
        let kinds: Vec<i64> = serde_json::from_str(strings("tx_types")?).map_err(|_| MALFORMED)?;
        let infos: Vec<String> = serde_json::from_str(strings("tx_infos")?).map_err(|_| MALFORMED)?;
        if kinds.is_empty() || kinds.len() != infos.len() {
            return Err(MALFORMED);
        }
        let mut envelopes = Vec::new();
        for (kind, text) in kinds.into_iter().zip(&infos) {
            if kind != TX_CREATE_ORDER {
                tracing::warn!("dry-run Lighter: transaction type {kind} is not simulated");
                return Err(Fail(400, 21001, "transaction type not supported by the dry-run venue"));
            }
            let info: Value = serde_json::from_str(text).map_err(|_| MALFORMED)?;
            let field = |key: &str| info.get(key).and_then(Value::as_i64).ok_or(MALFORMED);
            // Limit and market orders only; a market order's price is its worst acceptable one.
            if field("Type")? > 1 {
                tracing::warn!("dry-run Lighter: order type {} is not simulated", field("Type")?);
                return Err(Fail(400, 21001, "order type not supported by the dry-run venue"));
            }
            let market = field("MarketIndex")?.to_string();
            let detail = self.markets.get(&market).ok_or(fail(Reject::UnknownMarket))?;
            let tif = match field("TimeInForce")? {
                0 => Tif::Ioc,
                1 => Tif::Gtc,
                2 => Tif::PostOnly,
                _ => return Err(MALFORMED),
            };
            let spec = OrderSpec {
                market,
                client_id: field("ClientOrderIndex")?.to_string(),
                side: if field("IsAsk")? == 1 { Side::Sell } else { Side::Buy },
                qty: Decimal::new(field("BaseAmount")?, detail.supported_size_decimals),
                price: Some(Decimal::new(field("Price")?, detail.supported_price_decimals)),
                tif,
                reduce_only: field("ReduceOnly")? == 1,
            };
            let key = u64::try_from(field("ApiKeyIndex")?).map_err(|_| MALFORMED)?;
            let nonce = Some((key, field("Nonce")?));
            envelopes.push(Envelope { venue: Venue::Lighter, lane, weight: 0, orders: 1, nonce, request: Request::Place(spec) });
        }
        let hashes = infos.iter().map(|text| hex::encode(keccak256(text.as_bytes()))).collect();
        Ok((envelopes, hashes))
    }

    /// Answers one inbound frame; transaction answers come back through `answers`.
    async fn on_text(&self, lane: u64, text: &str, follower: &mut Follower, private: &mut Private, answers: &mpsc::UnboundedSender<String>) -> Vec<String> {
        let Ok(msg) = serde_json::from_str::<Value>(text) else { return Vec::new() };
        let channel = msg.get("channel").and_then(Value::as_str).unwrap_or_default();
        match msg.get("type").and_then(Value::as_str).unwrap_or_default() {
            "ping" => vec![json!({"type": "pong"}).to_string()],
            "pong" => Vec::new(),
            "subscribe" if channel.starts_with("order_book/") && follower.subscribe(channel) => Vec::new(),
            "subscribe" => {
                let Some(sub) = Kind::parse(channel) else {
                    tracing::warn!("dry-run Lighter: channel {channel} is not simulated");
                    return vec![json!({"error": {"code": 30005, "message": format!("Invalid Channel: {channel}")}}).to_string()];
                };
                // Follow the events before reading the state, so none falls in between.
                if private.events.is_none() {
                    private.events = Some(self.venues.events(Venue::Lighter));
                }
                let Some((view, open)) = self.venues.peek(Venue::Lighter).await else { return Vec::new() };
                private.open = open.into_iter().map(|o| (o.id, o)).collect();
                private.channels.push(sub);
                vec![self.snapshot(sub, &view, &private.open)]
            }
            "unsubscribe" => {
                follower.unsubscribe(channel);
                private.channels.retain(|&sub| Some(sub) != Kind::parse(channel));
                Vec::new()
            }
            "jsonapi/sendtxbatch" => match self.batch(lane, msg.get("data").unwrap_or(&Value::Null)) {
                Err(fail) => vec![fail.tx_answer()],
                Ok((envelopes, hashes)) => {
                    let (venues, answers) = (self.venues.clone(), answers.clone());
                    tokio::spawn(async move {
                        let mut answer = None;
                        for envelope in envelopes {
                            if let Reply::Reject(reject) = venues.call(envelope).await {
                                answer = Some(fail(reject).tx_answer());
                                break;
                            }
                        }
                        let accepted = || json!({"type": "jsonapi/sendtxbatch", "data": {"code": 200, "message": "{\"ratelimit\": \"didn't use volume quota\"}", "tx_hash": hashes}}).to_string();
                        let _ = answers.send(answer.unwrap_or_else(accepted));
                    });
                    Vec::new()
                }
            },
            other => {
                tracing::warn!("dry-run Lighter: message type {other:?} is not simulated");
                Vec::new()
            }
        }
    }

    /// One `/stream` connection: public books, account channels and transactions.
    async fn stream(&self, lane: u64, mut ws: WebSocketStream<TcpStream>) {
        let session = hex::encode(&keccak256(format!("session {lane} {}", wall_us()).as_bytes())[..16]);
        if ws.send(Message::Text(json!({"session_id": session, "type": "connected"}).to_string())).await.is_err() {
            return;
        }
        let mut follower = self.venues.follower(Venue::Lighter, lane, false);
        let mut private = Private::default();
        let (answers_tx, mut answers) = mpsc::unbounded_channel();
        loop {
            let out = tokio::select! {
                incoming = ws.next() => match incoming {
                    Some(Ok(Message::Text(text))) => self.on_text(lane, &text, &mut follower, &mut private, &answers_tx).await,
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                    // Pings are answered by the socket as it is read.
                    Some(Ok(_)) => Vec::new(),
                },
                frame = follower.next() => match frame {
                    Some(text) => vec![text.to_string()],
                    None => {
                        let _ = ws.close(None).await;
                        return;
                    }
                },
                event = next_event(&mut private.events) => match event {
                    Ok(event) => self.updates(&event, &mut private),
                    // Fell too far behind: dropped, as a stuck consumer would be.
                    Err(_) => {
                        let _ = ws.close(None).await;
                        return;
                    }
                },
                Some(answer) = answers.recv() => vec![answer],
            };
            for text in out {
                if ws.send(Message::Text(text)).await.is_err() {
                    return;
                }
            }
        }
    }

    async fn route(&self, lane: u64, request: &server::Request) -> Result<String, Fail> {
        let params = request.params();
        let get = |key: &str| params.get(key).map(String::as_str);
        let number = |key: &str| get(key).and_then(|v| v.parse::<i64>().ok()).ok_or(MALFORMED);
        if matches!(request.path.as_str(), "/api/v1/accountActiveOrders" | "/api/v1/accountInactiveOrders" | "/api/v1/trades")
            && request.header("authorization").is_none_or(str::is_empty)
        {
            return Err(NO_AUTH);
        }
        let body = match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/api/v1/orderBooks") => {
                self.call(lane, Request::Noop).await?;
                return Ok(self.order_books.clone());
            }
            ("GET", "/api/v1/orderBookOrders") => {
                let market = number("market_id")?.to_string();
                let limit = usize::try_from(number("limit").unwrap_or(20)).unwrap_or(20);
                let Reply::Book { bids, asks, .. } = self.call(lane, Request::Book { market }).await? else {
                    return Err(fail(Reject::Unavailable));
                };
                let rows = |levels: &[Level]| -> Vec<Value> {
                    levels.iter().take(limit).map(|(price, size)| json!({
                        "order_index": 0,
                        "order_id": "0",
                        "owner_account_index": 0,
                        "initial_base_amount": size.to_string(),
                        "remaining_base_amount": size.to_string(),
                        "price": price.to_string(),
                        "order_expiry": 0,
                    })).collect()
                };
                let (asks, bids) = (rows(&asks), rows(&bids));
                json!({"code": 200, "total_asks": asks.len(), "asks": asks, "total_bids": bids.len(), "bids": bids})
            }
            ("GET", path @ ("/api/v1/nextNonce" | "/api/v1/apikeys")) => {
                // The signer library's own check may leave the ids out; the venue has one key.
                let account = number("account_index").unwrap_or(self.identity.account_index);
                let key = number("api_key_index").unwrap_or(self.identity.api_key_index.into());
                let Reply::Nonce(nonce) = self.call(lane, Request::NextNonce { key: u64::try_from(key).map_err(|_| MALFORMED)? }).await? else {
                    return Err(fail(Reject::Unavailable));
                };
                if path.ends_with("nextNonce") {
                    json!({"code": 200, "nonce": nonce})
                } else {
                    let key = json!({"account_index": account, "api_key_index": key, "nonce": nonce, "public_key": self.identity.api_public_key});
                    json!({"code": 200, "api_keys": [key]})
                }
            }
            ("GET", "/api/v1/account") => {
                let account = number("value")?;
                let Reply::Account(view) = self.call(lane, Request::Account).await? else {
                    return Err(fail(Reject::Unavailable));
                };
                let positions: Vec<Value> = self.markets.keys().map(|m| self.position_json(&view, m)).collect();
                json!({"code": 200, "total": 1, "accounts": [{
                    "code": 0,
                    "account_type": 0,
                    "index": account,
                    "account_index": account,
                    "status": 1,
                    "collateral": view.balance.to_string(),
                    "available_balance": view.available.to_string(),
                    "total_asset_value": view.equity.to_string(),
                    "cross_asset_value": view.equity.to_string(),
                    "positions": positions,
                }]})
            }
            ("GET", "/api/v1/accountActiveOrders") => {
                let account = number("account_index")?;
                let market = number("market_id")?.to_string();
                let Reply::Orders(orders) = self.call(lane, Request::OpenOrders { market: Some(market) }).await? else {
                    return Err(fail(Reject::Unavailable));
                };
                json!({"code": 200, "orders": orders.iter().map(|o| order_json(o, account)).collect::<Vec<_>>()})
            }
            ("GET", "/api/v1/accountInactiveOrders") => {
                let account = number("account_index")?;
                let market = get("market_id").map(str::to_string);
                let limit = usize::try_from(number("limit").unwrap_or(100)).unwrap_or(100);
                let skip = get("cursor").and_then(|c| c.parse::<usize>().ok()).unwrap_or(0);
                let Reply::Orders(orders) = self.call(lane, Request::ClosedOrders { market }).await? else {
                    return Err(fail(Reject::Unavailable));
                };
                let page: Vec<Value> = orders.iter().skip(skip).take(limit).map(|o| order_json(o, account)).collect();
                let next = if skip + limit < orders.len() { (skip + limit).to_string() } else { String::new() };
                json!({"code": 200, "orders": page, "next_cursor": next})
            }
            ("GET", "/api/v1/trades") => {
                let account = number("account_index")?;
                let order_id = number("order_index").ok().and_then(|id| u64::try_from(id).ok());
                let limit = usize::try_from(number("limit").unwrap_or(100)).unwrap_or(100);
                let Reply::Fills(fills) = self.call(lane, Request::Fills { market: None }).await? else {
                    return Err(fail(Reject::Unavailable));
                };
                let trades: Vec<Value> = fills.iter().rev()
                    .filter(|f| order_id.is_none_or(|id| f.order_id == id))
                    .take(limit)
                    .map(|f| self.trade_json(f, account))
                    .collect();
                json!({"code": 200, "trades": trades, "next_cursor": ""})
            }
            _ => return Err(NOT_FOUND),
        };
        Ok(body.to_string())
    }
}

impl Handler for Lighter {
    async fn rest(&self, lane: u64, request: server::Request) -> Response {
        match self.route(lane, &request).await {
            Ok(body) => Response::json(200, body),
            Err(fail) => {
                if fail == NOT_FOUND {
                    tracing::warn!("dry-run Lighter: no route for {} {}", request.method, request.path);
                }
                fail.response()
            }
        }
    }

    async fn websocket(&self, lane: u64, request: server::Request, ws: WebSocketStream<TcpStream>) {
        if request.path == "/stream" {
            self.stream(lane, ws).await;
        } else {
            tracing::warn!("dry-run Lighter: no websocket at {}", request.path);
        }
    }
}

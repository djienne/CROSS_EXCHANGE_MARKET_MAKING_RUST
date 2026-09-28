//! The simulated Hyperliquid perpetuals venue: the `/info` reads and `/exchange` actions of the
//! bot's client (`crate::hyperliquid::client`), answered by the matching core in Hyperliquid's
//! shapes, and the public `l2Book`/`bbo` streams on `/ws`. Weights follow Hyperliquid's docs (1200 a minute per IP). Signatures and nonces are
//! not checked: the only verifier would be our own hash, which the client's tests pin to the
//! Python SDK.

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::clock::wall_us;
use super::feed::Follower;
use super::matching::{End, Envelope, Fill, Order, OrderRef, OrderSpec, Reject, Reply, Request, Status, Tif, Venue};
use super::server::{self, Handler, Response};
use super::Venues;
use crate::types::Side;

/// A refusal of the whole request: HTTP status and body.
struct Fail(u16, &'static str);

const MALFORMED: Fail = Fail(422, "Failed to deserialize the JSON body into the target type");

/// Not answered: rate limited (the client's refusal), or sent with its outcome unknown.
fn unanswered(reject: Reject) -> Fail {
    match reject {
        Reject::RateLimited => Fail(429, "null"),
        _ => Fail(503, "null"),
    }
}

/// An order's refusal, as the `error` of its status.
fn refusal(reject: Reject) -> &'static str {
    match reject {
        Reject::TickSize => "Order has invalid price.",
        Reject::StepSize => "Order has invalid size.",
        Reject::MinQty | Reject::MinNotional => "Order must have minimum value of $10.",
        Reject::PriceBand => "Order price cannot be more than 80% away from the reference price.",
        Reject::ReduceOnly => "Reduce only order would increase position.",
        Reject::Margin => "Insufficient margin to place order.",
        _ => "Order could not be placed.",
    }
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "B",
        Side::Sell => "A",
    }
}

fn order_json(o: &Order) -> Value {
    json!({
        "coin": o.market, "side": side_str(o.side), "limitPx": o.price.unwrap_or_default(), "sz": o.remaining(),
        "oid": o.id, "timestamp": o.created_us / 1_000, "origSz": o.qty, "cloid": o.client_id,
    })
}

fn status_str(o: &Order) -> &'static str {
    match o.status {
        Status::New | Status::PartiallyFilled => "open",
        Status::Filled => "filled",
        Status::Done(End::Ioc) if o.filled.is_zero() => "iocCancelRejected",
        Status::Done(End::PostOnly | End::ReduceOnly | End::Rejected(_)) => "rejected",
        Status::Done(_) => "canceled",
    }
}

fn fill_json(f: &Fill) -> Value {
    json!({
        "coin": f.market, "px": f.price, "sz": f.qty, "side": side_str(f.side), "time": f.at_us / 1_000,
        "oid": f.order_id, "cloid": f.client_id, "crossed": !f.maker, "fee": f.fee, "feeToken": "USDC", "tid": f.id,
    })
}

/// The simulated Hyperliquid venue.
pub struct Hyperliquid {
    venues: Venues,
    /// The real `meta` body, fetched once at start: an asset id is a position in its universe.
    meta: Value,
    coins: Vec<String>,
    leverage: u32,
}

impl Hyperliquid {
    pub fn new(venues: Venues, meta: &str, leverage: Decimal) -> anyhow::Result<Self> {
        let meta: Value = serde_json::from_str(meta).context("parsing Hyperliquid meta")?;
        let universe = meta["universe"].as_array().context("Hyperliquid meta without a universe")?;
        let coins = universe.iter().map(|asset| asset["name"].as_str().unwrap_or_default().to_string()).collect();
        let leverage = leverage.to_u32().context("Hyperliquid leverage is a whole number")?;
        Ok(Self { venues, meta, coins, leverage })
    }

    async fn call(&self, lane: u64, weight: u32, orders: u32, request: Request) -> Reply {
        self.venues.call(Envelope { venue: Venue::Hyperliquid, lane, weight, orders, nonce: None, request }).await
    }

    async fn info(&self, lane: u64, body: &Value) -> Result<Value, Fail> {
        let kind = body["type"].as_str().unwrap_or_default();
        // Two for these reads, twenty for the others (the extra weight per 20 fills returned
        // is not modelled).
        let weight = if matches!(kind, "l2Book" | "clearinghouseState" | "orderStatus") { 2 } else { 20 };
        let coin = body["coin"].as_str().unwrap_or_default();
        let request = match kind {
            "l2Book" => Request::Book { market: coin.to_string() },
            "clearinghouseState" => Request::Account,
            "openOrders" => Request::OpenOrders { market: None },
            "userFillsByTime" => Request::Fills { market: None },
            "orderStatus" => Request::Order { order: match &body["oid"] {
                Value::String(cloid) => OrderRef::Client(cloid.clone()),
                oid => OrderRef::Id(oid.as_u64().ok_or(MALFORMED)?),
            } },
            _ => Request::Noop,
        };
        Ok(match (kind, self.call(lane, weight, 0, request).await) {
            ("orderStatus", Reply::Reject(Reject::UnknownOrder)) => json!({"status": "unknownOid"}),
            (_, Reply::Reject(reject)) => return Err(unanswered(reject)),
            ("meta", _) => self.meta.clone(),
            ("l2Book", Reply::Book { bids, asks, at_us }) => {
                let side = |levels: &[(Decimal, Decimal)]| -> Value {
                    levels.iter().take(20).map(|(px, sz)| json!({"px": px, "sz": sz, "n": 1})).collect()
                };
                json!({"coin": coin, "time": at_us / 1_000, "levels": [side(&bids), side(&asks)]})
            }
            ("clearinghouseState", Reply::Account(view)) => {
                let positions: Vec<Value> = view.positions.iter().filter(|p| !p.qty.is_zero()).map(|p| json!({"type": "oneWay", "position": {
                    "coin": p.market, "szi": p.qty, "entryPx": p.entry, "unrealizedPnl": p.unrealized,
                    "leverage": {"type": "cross", "value": self.leverage},
                }})).collect();
                json!({
                    "marginSummary": {"accountValue": view.equity, "totalMarginUsed": view.margin},
                    "withdrawable": view.available, "assetPositions": positions, "time": wall_us() / 1_000,
                })
            }
            ("openOrders", Reply::Orders(orders)) => orders.iter().map(order_json).collect(),
            ("orderStatus", Reply::Order(o)) => {
                json!({"status": "order", "order": {"order": order_json(&o), "status": status_str(&o), "statusTimestamp": o.updated_us / 1_000}})
            }
            ("userFillsByTime", Reply::Fills(fills)) => {
                let since_us = body["startTime"].as_i64().ok_or(MALFORMED)? * 1_000;
                let mut fills: Vec<&Fill> = fills.iter().filter(|f| f.at_us >= since_us).collect();
                fills.sort_by_key(|f| (f.at_us, f.id));
                fills.into_iter().map(fill_json).collect()
            }
            ("activeAssetData", _) => json!({"user": body["user"], "coin": coin, "leverage": {"type": "cross", "value": self.leverage}}),
            _ => return Err(MALFORMED),
        })
    }

    async fn exchange(&self, lane: u64, action: &Value) -> Result<Value, Fail> {
        let coin = |asset: &Value| asset.as_u64().and_then(|a| self.coins.get(a as usize)).ok_or(MALFORMED);
        let mut statuses = Vec::new();
        // Each order or cancel weighs 1 here; Hyperliquid charges an action 1 + 1 per 40 in it,
        // and the bot sends one at a time.
        let kind = match action["type"].as_str() {
            Some("order") => {
                for wire in action["orders"].as_array().ok_or(MALFORMED)? {
                    let tif = match wire["t"]["limit"]["tif"].as_str() {
                        Some("Alo") => Tif::PostOnly,
                        Some("Ioc") => Tif::Ioc,
                        Some("Gtc") => Tif::Gtc,
                        _ => return Err(MALFORMED),
                    };
                    let number = |key: &str| wire[key].as_str().and_then(|v| v.parse::<Decimal>().ok()).ok_or(MALFORMED);
                    let spec = OrderSpec {
                        market: coin(&wire["a"])?.clone(),
                        client_id: wire["c"].as_str().map_or_else(|| format!("dryrun-{}", wall_us()), str::to_string),
                        side: if wire["b"].as_bool().ok_or(MALFORMED)? { Side::Buy } else { Side::Sell },
                        qty: number("s")?,
                        price: Some(number("p")?),
                        tif,
                        reduce_only: wire["r"].as_bool() == Some(true),
                    };
                    let error = |text: &str| json!({"error": format!("{text} asset={}", wire["a"])});
                    statuses.push(match self.call(lane, 1, 1, Request::Place(spec)).await {
                        Reply::Order(o) if o.working() => json!({"resting": {"oid": o.id, "cloid": o.client_id}}),
                        Reply::Order(o) if !o.filled.is_zero() => {
                            let avg_px = (o.filled_quote / o.filled).normalize();
                            json!({"filled": {"totalSz": o.filled, "avgPx": avg_px, "oid": o.id, "cloid": o.client_id}})
                        }
                        Reply::Order(o) => match o.status {
                            Status::Done(End::PostOnly) => error("Post only order would have immediately matched."),
                            Status::Done(End::ReduceOnly) => error(refusal(Reject::ReduceOnly)),
                            Status::Done(End::Rejected(reject)) => error(refusal(reject)),
                            _ => error("Order could not immediately match against any resting orders."),
                        },
                        Reply::Reject(reject @ (Reject::RateLimited | Reject::Unavailable)) => return Err(unanswered(reject)),
                        Reply::Reject(reject) => error(refusal(reject)),
                        _ => return Err(unanswered(Reject::Unavailable)),
                    });
                }
                "order"
            }
            Some("cancelByCloid") => {
                for wire in action["cancels"].as_array().ok_or(MALFORMED)? {
                    let order = OrderRef::Client(wire["cloid"].as_str().ok_or(MALFORMED)?.to_string());
                    statuses.push(match self.call(lane, 1, 0, Request::Cancel { market: coin(&wire["asset"])?.clone(), order }).await {
                        Reply::Order(_) => json!("success"),
                        Reply::Reject(reject @ (Reject::RateLimited | Reject::Unavailable)) => return Err(unanswered(reject)),
                        _ => json!({"error": format!("Order was never placed, already canceled, or filled. asset={}", wire["asset"])}),
                    });
                }
                "cancel"
            }
            _ => return Ok(json!({"status": "err", "response": format!("Unsupported action: {action}")})),
        };
        Ok(json!({"status": "ok", "response": {"type": kind, "data": {"statuses": statuses}}}))
    }
}

impl Handler for Hyperliquid {
    async fn rest(&self, lane: u64, request: server::Request) -> Response {
        let body = serde_json::from_slice::<Value>(&request.body).map_err(|_| MALFORMED);
        let answer = match (request.method.as_str(), request.path.as_str(), body) {
            (_, _, Err(fail)) => Err(fail),
            ("POST", "/info", Ok(body)) => self.info(lane, &body).await,
            ("POST", "/exchange", Ok(body)) => self.exchange(lane, &body["action"]).await,
            _ => Err(Fail(404, "null")),
        };
        match answer {
            Ok(body) => Response::json(200, body.to_string()),
            Err(Fail(status, body)) => Response::json(status, body),
        }
    }

    /// Public streams, as a Tokyo bot receives them.
    async fn websocket(&self, lane: u64, _request: server::Request, mut ws: WebSocketStream<TcpStream>) {
        let mut follower = self.venues.follower(Venue::Hyperliquid, lane, false);
        loop {
            let text = tokio::select! {
                incoming = ws.next() => match incoming {
                    Some(Ok(Message::Text(text))) => on_text(&text, &mut follower),
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                    Some(Ok(_)) => continue,
                },
                frame = follower.next() => match frame {
                    Some(text) => text.to_string(),
                    None => {
                        let _ = ws.close(None).await;
                        return;
                    }
                },
            };
            if ws.send(Message::Text(text)).await.is_err() {
                return;
            }
        }
    }
}

/// Answers a `/ws` request: a subscription to a stream the upstream carries, or the app ping.
fn on_text(text: &str, follower: &mut Follower) -> String {
    let msg: Value = serde_json::from_str(text).unwrap_or_default();
    let sub = &msg["subscription"];
    match msg["method"].as_str() {
        Some("ping") => json!({"channel": "pong"}).to_string(),
        Some("subscribe") if follower.subscribe(&format!("{}/{}", sub["type"].as_str().unwrap_or_default(), sub["coin"].as_str().unwrap_or_default())) => {
            json!({"channel": "subscriptionResponse", "data": msg}).to_string()
        }
        _ => json!({"channel": "error", "data": format!("Invalid subscription {text}")}).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rust_decimal_macros::dec;
    use tokio::sync::mpsc;

    use super::*;
    use crate::dryrun::book::BookUpdate;
    use crate::dryrun::clock::Latency;
    use crate::dryrun::feed::{Hub, Input};
    use crate::dryrun::matching::{Exchange, FeedEvent, Fees, Filters, SimParams};
    use crate::hyperliquid::client::{dec as parse, Client, Placed, Tif as Wire};
    use crate::livebot::exec::creds::HyperliquidCreds;

    /// HYPE at index 1, as assets are numbered by their place in the universe.
    const META: &str = r#"{"universe":[{"name":"BTC","szDecimals":5},{"name":"HYPE","szDecimals":2}]}"#;

    /// The bot's own client against the simulated venue: every read and action it sends,
    /// answered in Hyperliquid's shapes; then its book connector on the venue's streams.
    #[tokio::test]
    async fn hyperliquid_answers_the_bots_own_client() {
        let fixed = Latency::try_from([1.0, 1.0]).unwrap();
        let fees = [Fees { maker: dec!(0), taker: dec!(0) }, Fees { maker: dec!(0.00015), taker: dec!(0.00045) }];
        let params = SimParams {
            shift_us: 300_000, seed: 1, effect_fraction: 0.9, rtt: [fixed; 2], private: [fixed; 2], lighter_taker_delay_us: 0,
            hidden_queue_multiplier: dec!(0), fees, leverage: dec!(3), balances: [dec!(1000); 2], venues: [Venue::Aster, Venue::Hyperliquid],
        };
        let mut core = Exchange::new(params, wall_us());
        core.trust_feed();
        let filters = Filters { tick: dec!(0.0001), step: dec!(0.01), min_qty: dec!(0.01), min_notional: dec!(10), percent_price: None, sig_figs: Some(5) };
        core.add_market(Venue::Hyperliquid, "HYPE", Some(20), filters);
        let streams = [vec![], vec!["l2Book/HYPE".to_string(), "bbo/HYPE".to_string()]];
        let hubs = streams.map(|streams| Arc::new(Mutex::new(Hub::new(300_000, streams))));
        let (inputs, feed) = mpsc::unbounded_channel();
        let venues = Venues::start(core, feed, hubs.clone(), [Latency::ZERO; 2], 1, None);
        let book = BookUpdate::Replace { bids: vec![(dec!(40), dec!(5))], asks: vec![(dec!(40.01), dec!(5))] };
        let event = FeedEvent::Book(book);
        inputs.send(Input::Frame { venue: Venue::Hyperliquid, market: "HYPE".into(), exch_us: wall_us() - 300_000, event }).unwrap();
        venues.warm(&[(Venue::Hyperliquid, "HYPE")]).await;
        let base = super::super::serve(0, Hyperliquid::new(venues, META, dec!(3)).unwrap()).await.unwrap();
        let client = Client::new(&base, HyperliquidCreds::dry_run()).unwrap();

        let asset = client.asset("HYPE").await.unwrap();
        assert_eq!((asset.index, asset.sz_decimals), (1, 2));
        assert_eq!(client.top("HYPE").await.unwrap(), (dec!(40), dec!(40.01)));

        // A post-only bid rests, shows, and is cancelled by its cloid.
        let cloid = "0x00000000000000000000000000000001";
        assert!(matches!(client.place(&asset, true, dec!(39.5), dec!(0.5), Wire::Alo, false, cloid).await, Placed::Resting { .. }));
        assert!(client.open_orders().await.unwrap().iter().any(|o| o["cloid"] == cloid));
        assert_eq!(client.order_status(cloid).await.unwrap().0, "open");
        client.cancel(&asset, cloid).await.unwrap();
        assert_eq!(client.order_status(cloid).await.unwrap().0, "canceled");
        assert!(client.cancel(&asset, cloid).await.is_err(), "already canceled");
        assert_eq!(client.order_status("0x00000000000000000000000000000009").await.unwrap(), ("unknownOid".into(), dec!(0)));
        let crossing = client.place(&asset, true, dec!(40.02), dec!(0.5), Wire::Alo, false, "0x02").await;
        assert!(matches!(&crossing, Placed::Rejected(e) if e.starts_with("Post only")), "{crossing:?}");

        // An IOC takes the ask; its fee (4.5 bps) shows in the fills; reduce-only closes no more
        // than the position; an IOC with nothing to take is refused.
        let since = wall_us() / 1_000;
        let bought = client.place(&asset, true, dec!(40.5), dec!(0.5), Wire::Ioc, false, "0x03").await;
        assert!(matches!(bought, Placed::Filled { size, avg_px, .. } if size == dec!(0.5) && avg_px == dec!(40.01)), "{bought:?}");
        assert_eq!(client.position("HYPE").await.unwrap(), dec!(0.5));
        let state = client.user_info("clearinghouseState").await.unwrap();
        assert!(parse(&state["marginSummary"]["accountValue"]).unwrap() > dec!(999), "{state}");
        let sold = client.place(&asset, false, dec!(39), dec!(1), Wire::Ioc, true, "0x04").await;
        assert!(matches!(sold, Placed::Filled { size, avg_px, .. } if size == dec!(0.5) && avg_px == dec!(40)), "{sold:?}");
        assert_eq!(client.position("HYPE").await.unwrap(), dec!(0));
        let fills = client.fills_since(since).await.unwrap();
        let fill = |i: usize, key: &str| fills[i][key].clone();
        assert_eq!((fills.len(), fill(0, "cloid"), fill(0, "side"), fill(0, "crossed")), (2, json!("0x03"), json!("B"), json!(true)));
        assert_eq!(parse(&fill(0, "fee")).unwrap(), dec!(0.5) * dec!(40.01) * dec!(0.00045));
        assert!(client.fills_since(wall_us() / 1_000 + 1).await.unwrap().is_empty(), "only the fills since startTime");
        let missed = client.place(&asset, true, dec!(30), dec!(0.5), Wire::Ioc, false, "0x05").await;
        assert!(matches!(&missed, Placed::Rejected(e) if e.starts_with("Order could not immediately match")), "{missed:?}");
        assert_eq!(client.order_status("0x05").await.unwrap(), ("iocCancelRejected".into(), dec!(0)));
        assert_eq!(client.order_status("0x03").await.unwrap(), ("filled".into(), dec!(0.5)));

        // Each upstream l2Book fills the connector's L2 slot, each bbo its BBO slot, with the
        // venue's time moved by the shift (so received about as fresh as sent).
        let cell = Arc::new(crate::hotpath::VenueBook::new());
        let tap = crate::connectors::Tap { book: Some(cell.clone()), reconnect: None, scale: None, qty_scale: crate::livebot::scale::HotQtyScale::Hedge };
        tokio::spawn(crate::connectors::hyperliquid::run_with_tap(crate::connectors::hyperliquid::ws_url(&base), "HYPE".into(), tap));
        let upstream = |channel: &str, data: Value| {
            let frame = super::super::feed::hyperliquid_frame(&json!({"channel": channel, "data": data}).to_string(), 300_000);
            super::super::feed::forward(frame.unwrap().unwrap(), &hubs[1], &inputs);
        };
        let level = |px: &str, sz: &str| json!({"px": px, "sz": sz, "n": 1});
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while cell.load().is_none() || cell.load_bbo().is_none() {
                let time = wall_us() / 1_000;
                upstream("l2Book", json!({"coin": "HYPE", "time": time, "levels": [[level("40", "5"), level("39.9", "7")], [level("40.01", "5")]]}));
                upstream("bbo", json!({"coin": "HYPE", "time": time, "bbo": [level("40.005", "1"), level("40.01", "5")]}));
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the connector gets both books");
        let (l2, bbo) = (cell.load().unwrap(), cell.load_bbo().unwrap());
        let top = |book: &crate::book::OrderBook| [book.bids[0], book.asks[0]].map(|l| (l.px, l.qty));
        assert_eq!((l2.bids.len(), top(&l2)), (2, [(dec!(40), dec!(5)), (dec!(40.01), dec!(5))]));
        assert_eq!(top(&bbo), [(dec!(40.005), dec!(1)), (dec!(40.01), dec!(5))]);
        let age_ms = (l2.local_recv_ts - l2.exch_ts).num_milliseconds();
        assert!((0..200).contains(&age_ms), "a shifted frame arrives as fresh as sent: {age_ms} ms");
        // The core takes the same frames (the bbo overlays the l2Book), as the bot's REST read sees.
        let http = reqwest::Client::new();
        let rest_top = || async {
            let book = crate::connectors::rest_book::fetch_hyperliquid_book(&http, &base, "HYPE").await.unwrap();
            (book.bids[0].px, book.asks[0].px)
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while rest_top().await != (dec!(40.005), dec!(40.01)) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the core follows the upstream frames");
    }
}

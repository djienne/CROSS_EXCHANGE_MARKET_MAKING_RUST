//! Aster user-data stream: the low-latency maker-fill signal. Manages the
//! listenKey lifecycle (POST create / PUT keepalive / DELETE only on graceful shutdown, with
//! the documented gotchas: no PUT right after POST; reuse-not-recreate on reconnect), connects
//! the WS, parses `ORDER_TRADE_UPDATE` fills into [`AsterFill`]s, and forwards them to the
//! strategy. [`FillDedup`](super::fills::FillDedup) and the strategy's cumulative quantity
//! coverage prevent repeated events from crediting the same maker quantity again.
//!
//! The position reconciler ([`super::reconcile`]) is the REST safeguard behind this stream: a
//! fill the stream somehow misses surfaces as a reported-vs-predicted position delta, which the
//! strategy's `recover_orphans` backstop then actively hedges/flattens (not merely freezes).
//! Sub-minimum fills can accumulate before hedging; unresolved attempts remain reserved.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tokio::time::interval_at;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::hotpath::clock::mono_now_ns;
use crate::types::{MarketId, Side};

use super::exec::aster::AsterRest;
use super::fills::AsterFill;

/// listenKey keepalive cadence (the key lives 60 min; refresh well inside that, and NOT
/// immediately after POST — the first tick is delayed a full interval).
const KEEPALIVE: Duration = Duration::from_secs(25 * 60);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const EXPIRED_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const LISTEN_KEY_BACKOFF_MAX: Duration = Duration::from_secs(60);
const USER_WS_PING: Duration = Duration::from_secs(2);
const USER_WS_IDLE_TIMEOUT: Duration = Duration::from_secs(8);
const USER_WS_IDLE_CHECK: Duration = Duration::from_secs(1);

#[derive(Deserialize)]
struct UserEventType<'a> {
    #[serde(borrow, default)]
    e: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum IntOrString<'a> {
    Int(i64),
    Str(&'a str),
}

impl<'a> IntOrString<'a> {
    fn into_string(self) -> String {
        match self {
            IntOrString::Int(i) => i.to_string(),
            IntOrString::Str(s) => s.to_string(),
        }
    }
}

#[derive(Deserialize)]
struct AsterTradeUpdate<'a> {
    #[serde(rename = "e", default)]
    event_type: &'a str,
    #[serde(rename = "E", default)]
    event_time_ms: i64,
    #[serde(rename = "o", borrow, default)]
    order: Option<AsterOrderInfo<'a>>,
}

#[derive(Deserialize)]
struct AsterOrderInfo<'a> {
    #[serde(rename = "T", default)]
    trade_time_ms: i64,
    #[serde(rename = "s", default)]
    symbol: &'a str,
    #[serde(rename = "c", default)]
    client_id: &'a str,
    #[serde(rename = "S", default)]
    side: &'a str,
    #[serde(rename = "x", default)]
    execution_type: &'a str,
    #[serde(rename = "i", borrow)]
    order_id: Option<IntOrString<'a>>,
    #[serde(rename = "l", default)]
    last_fill_qty: &'a str,
    #[serde(rename = "z", default)]
    cum_filled_qty: &'a str,
    #[serde(rename = "L", default)]
    last_fill_px: &'a str,
    #[serde(rename = "t", borrow)]
    trade_id: Option<IntOrString<'a>>,
    #[serde(rename = "R", default)]
    reduce_only: bool,
    #[serde(rename = "n", default)]
    commission: Option<&'a str>,
    #[serde(rename = "N", default)]
    commission_asset: Option<&'a str>,
}

#[derive(Debug, Clone)]
struct Backoff {
    cur: Duration,
    base: Duration,
    max: Duration,
}

impl Backoff {
    fn new(base: Duration, max: Duration) -> Self {
        Backoff { cur: base, base, max }
    }

    fn reset(&mut self) {
        self.cur = self.base;
    }

    fn next_delay(&mut self) -> Duration {
        let d = self.cur;
        self.cur = self.cur.saturating_mul(2).min(self.max);
        d
    }
}

/// Shared monotonic timestamp (ns) of the last user-stream message — the strategy/watchdog can
/// read this to gate quoting on stream liveness.
#[derive(Debug, Default)]
pub struct StreamLiveness {
    last_ns: AtomicI64,
}
impl StreamLiveness {
    pub fn touch(&self) {
        self.last_ns.store(mono_now_ns(), Ordering::Release);
    }
    pub fn age_ms(&self, now_ns: i64) -> i64 {
        let ts = self.last_ns.load(Ordering::Acquire);
        if ts == 0 {
            i64::MAX
        } else {
            now_ns.saturating_sub(ts) / 1_000_000
        }
    }
}

/// Run the Aster user stream until `shutdown`. Reconnects on drop; reuses the listenKey across
/// reconnects (recreating only when the server reports `listenKeyExpired`).
pub async fn run_aster_user_stream(
    aster: AsterRest,
    sym_to_market: HashMap<String, MarketId>,
    fill_tx: Sender<AsterFill>,
    liveness: Arc<StreamLiveness>,
    shutdown: CancellationToken,
) {
    // Arc so the periodic keepalive can run on its own task instead of blocking the
    // fill-read select (a keepalive REST round trip must never delay a fill->hedge).
    let aster = Arc::new(aster);
    let ws_root = aster.ws_root();
    info!("aster user stream starting");
    let mut listen_key: Option<String> = None;
    let mut listen_backoff = Backoff::new(RECONNECT_DELAY, LISTEN_KEY_BACKOFF_MAX);
    while !shutdown.is_cancelled() {
        // Obtain a key: reuse the existing one via keepalive (PUT) where possible — avoids the
        // race where a fresh POST returns a key the server is still invalidating.
        let key = match &listen_key {
            Some(k) => match aster.keepalive_listen_key().await {
                Ok(()) => {
                    listen_backoff.reset();
                    k.clone()
                }
                Err(_) => match aster.create_listen_key().await {
                    Ok(k) => {
                        listen_backoff.reset();
                        listen_key = Some(k.clone());
                        k
                    }
                    Err(e) => {
                        let d = listen_backoff.next_delay();
                        warn!("aster listenKey create failed: {e:#}; retrying in {:?}", d);
                        tokio::time::sleep(d).await;
                        continue;
                    }
                },
            },
            None => match aster.create_listen_key().await {
                Ok(k) => {
                    listen_backoff.reset();
                    listen_key = Some(k.clone());
                    k
                }
                Err(e) => {
                    let d = listen_backoff.next_delay();
                    warn!("aster listenKey create failed: {e:#}; retrying in {:?}", d);
                    tokio::time::sleep(d).await;
                    continue;
                }
            },
        };

        let url = format!("{ws_root}/ws/{key}");
        match connect_and_read(&url, &sym_to_market, &fill_tx, &aster, &liveness, &shutdown).await {
            Ok(expired) => {
                if expired {
                    // The key is dead — drop it so the next iteration creates a fresh one.
                    listen_key = None;
                    tokio::time::sleep(EXPIRED_RECONNECT_DELAY).await;
                }
            }
            Err(e) => warn!("aster user stream error: {e:#}"),
        }
        if !shutdown.is_cancelled() {
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }
    // Graceful shutdown: close the key (the only place we DELETE).
    if listen_key.is_some() {
        let _ = aster.close_listen_key().await;
    }
    info!("aster user stream stopped");
}

/// Connect + read until close/error/shutdown. Returns `Ok(true)` if the server pushed
/// `listenKeyExpired` (caller must recreate the key), `Ok(false)` for a plain disconnect.
async fn connect_and_read(
    url: &str,
    sym_to_market: &HashMap<String, MarketId>,
    fill_tx: &Sender<AsterFill>,
    aster: &Arc<AsterRest>,
    liveness: &StreamLiveness,
    shutdown: &CancellationToken,
) -> Result<bool> {
    let ws = crate::connectors::connect_guarded(url).await?;
    let (mut write, mut read) = ws.split();
    info!("aster user stream connected");
    liveness.touch();
    // First keepalive a full interval out (NOT right after POST — documented gotcha).
    let now = tokio::time::Instant::now();
    let mut keepalive = interval_at(now + KEEPALIVE, KEEPALIVE);
    let mut ws_ping = interval_at(now + USER_WS_PING, USER_WS_PING);
    let mut idle_check = interval_at(now + USER_WS_IDLE_CHECK, USER_WS_IDLE_CHECK);
    let mut last_frame = now;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(false),
            _ = ws_ping.tick() => {
                crate::connectors::send_guarded(&mut write, Message::Ping(Vec::new())).await?;
            }
            _ = idle_check.tick() => {
                if last_frame.elapsed() >= USER_WS_IDLE_TIMEOUT {
                    anyhow::bail!("aster user stream idle >{}s, forcing reconnect", USER_WS_IDLE_TIMEOUT.as_secs());
                }
            }
            _ = keepalive.tick() => {
                // Fire-and-forget on its own task: the PUT's round trip (50ms..5s
                // client timeout) must not stall the ORDER_TRADE_UPDATE reads below —
                // a fill landing during an inline keepalive would delay its hedge.
                // Failure handling is unchanged (warn only; the authoritative signal
                // is the venue's listenKeyExpired frame).
                let aster = aster.clone();
                tokio::spawn(async move {
                    if let Err(e) = aster.keepalive_listen_key().await {
                        warn!("aster listenKey keepalive failed: {e:#}");
                    }
                });
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        last_frame = tokio::time::Instant::now();
                        liveness.touch();
                        match classify_user_frame(&t) {
                            UserFrame::ListenKeyExpired => {
                                warn!("aster listenKeyExpired — reconnecting with a fresh key");
                                return Ok(true);
                            }
                            UserFrame::TradeUpdate(update) => {
                                if let Some(fill) = fill_from_update(update, sym_to_market) {
                                    // Bounded channel: if the strategy is briefly behind, block
                                    // rather than drop a fill (correctness over latency here).
                                    if fill_tx.send(fill).await.is_err() {
                                        return Ok(false); // strategy gone
                                    }
                                }
                            }
                            UserFrame::Other => {} // ACCOUNT_UPDATE etc. — positions come from the reconciler
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        last_frame = tokio::time::Instant::now();
                        liveness.touch();
                        crate::connectors::send_guarded(&mut write, Message::Pong(p)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_frame = tokio::time::Instant::now();
                        liveness.touch();
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(false),
                    Some(Ok(_)) => {
                        last_frame = tokio::time::Instant::now();
                        liveness.touch();
                    }
                    Some(Err(e)) => return Err(e.into()),
                }
            }
        }
    }
}

enum UserFrame<'a> {
    ListenKeyExpired,
    TradeUpdate(AsterTradeUpdate<'a>),
    Other,
}

/// Classify a user-stream frame with a single full parse. Frames whose fields clash with
/// [`AsterTradeUpdate`]'s types (foreign event shapes) fall back to a bare type-tag probe so
/// recognized `listenKeyExpired` frames still take the expiry path.
fn classify_user_frame(text: &str) -> UserFrame<'_> {
    match serde_json::from_str::<AsterTradeUpdate<'_>>(text) {
        Ok(update) => match update.event_type {
            "listenKeyExpired" => UserFrame::ListenKeyExpired,
            "ORDER_TRADE_UPDATE" => UserFrame::TradeUpdate(update),
            _ => UserFrame::Other,
        },
        Err(_) => match serde_json::from_str::<UserEventType<'_>>(text) {
            Ok(UserEventType {
                e: Some("listenKeyExpired"),
            }) => UserFrame::ListenKeyExpired,
            _ => UserFrame::Other,
        },
    }
}

/// Test entry point: the reader loop's exact frame -> fill path (classify, then convert).
#[cfg(test)]
fn parse_order_trade_update(text: &str, sym_to_market: &HashMap<String, MarketId>) -> Option<AsterFill> {
    match classify_user_frame(text) {
        UserFrame::TradeUpdate(update) => fill_from_update(update, sym_to_market),
        _ => None,
    }
}

/// Convert an already-parsed `ORDER_TRADE_UPDATE` into an [`AsterFill`] when it represents a
/// real fill increment (`x == "TRADE"` and last-filled-qty `l > 0`).
fn fill_from_update(update: AsterTradeUpdate<'_>, sym_to_market: &HashMap<String, MarketId>) -> Option<AsterFill> {
    if update.event_type != "ORDER_TRADE_UPDATE" {
        return None;
    }
    let o = update.order?;
    if o.execution_type != "TRADE" {
        return None;
    }
    let last_fill_qty: Decimal = o.last_fill_qty.parse().ok()?;
    if last_fill_qty <= Decimal::ZERO {
        return None;
    }
    let sym = o.symbol.to_ascii_uppercase();
    let market = sym_to_market.get(&sym)?.clone();
    // STRICT: a real fill must carry an explicit BUY/SELL side and a positive price. Defaulting
    // side→Buy or price→0 could hedge the WRONG direction or with a 0 notional — skip instead.
    let aster_side = match o.side {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => {
            warn!("aster fill on {} with bad/missing side {other:?}; skipping", o.symbol);
            return None;
        }
    };
    let last_fill_px: Decimal = match o.last_fill_px.parse() {
        Ok(p) if p > Decimal::ZERO => p,
        _ => {
            warn!("aster fill on {} with missing/non-positive price; skipping", o.symbol);
            return None;
        }
    };
    Some(AsterFill {
        market,
        aster_side,
        order_id: o.order_id.map(IntOrString::into_string).unwrap_or_default(),
        trade_id: o.trade_id.map(IntOrString::into_string).unwrap_or_default(),
        client_id: o.client_id.to_string(),
        last_fill_qty,
        last_fill_px,
        cum_filled_qty: o.cum_filled_qty.parse().unwrap_or(last_fill_qty),
        event_time_ms: if o.trade_time_ms > 0 { o.trade_time_ms } else { update.event_time_ms },
        reduce_only: o.reduce_only,
        // The venue documents `n`/`N` as "will not push if no commission": a fill without them
        // paid none. An unparseable `n` stays unknown.
        commission: o.commission.map_or(Some(Decimal::ZERO), |value| value.parse::<Decimal>().ok()),
        commission_asset: o.commission_asset.map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn map() -> HashMap<String, MarketId> {
        let mut m = HashMap::new();
        m.insert("HYPEUSDT".to_string(), MarketId("HYPE".into()));
        m
    }

    #[test]
    fn parses_maker_trade_fill() {
        let text = r#"{"e":"ORDER_TRADE_UPDATE","E":1700000000123,"o":{"s":"HYPEUSDT","c":"Xsess-HYPE-B-0","S":"BUY","x":"TRADE","X":"PARTIALLY_FILLED","i":2037568488,"l":"0.05","z":"0.05","L":"64.5","t":99887766,"m":true,"T":1700000000100,"n":"0.000645","N":"USDT"}}"#;
        let f = parse_order_trade_update(text, &map()).unwrap();
        assert_eq!(f.market, MarketId("HYPE".into()));
        assert_eq!(f.aster_side, Side::Buy);
        assert_eq!(f.order_id, "2037568488");
        assert_eq!(f.trade_id, "99887766");
        assert_eq!(f.client_id, "Xsess-HYPE-B-0");
        assert_eq!(f.last_fill_qty, dec!(0.05));
        assert_eq!(f.last_fill_px, dec!(64.5));
        assert_eq!(f.cum_filled_qty, dec!(0.05));
        assert_eq!(f.event_time_ms, 1700000000100);
        assert_eq!(f.commission, Some(dec!(0.000645)));
        assert_eq!(f.commission_asset.as_deref(), Some("USDT"));
    }

    #[test]
    fn ignores_non_trade_updates() {
        // A NEW ack (x=="NEW") is not a fill.
        let text = r#"{"e":"ORDER_TRADE_UPDATE","E":1,"o":{"s":"HYPEUSDT","S":"BUY","x":"NEW","X":"NEW","i":1,"l":"0","z":"0","t":0}}"#;
        assert!(parse_order_trade_update(text, &map()).is_none());
    }

    #[test]
    fn ignores_unknown_symbol() {
        let text = r#"{"e":"ORDER_TRADE_UPDATE","E":1,"o":{"s":"DOGEUSDT","S":"SELL","x":"TRADE","i":1,"l":"1","z":"1","L":"0.1","t":5}}"#;
        assert!(parse_order_trade_update(text, &map()).is_none());
    }

    #[test]
    fn parses_string_order_and_trade_ids() {
        let text = r#"{"e":"ORDER_TRADE_UPDATE","E":1700000000999,"o":{"s":"HYPEUSDT","c":"Xsess-HYPE-S-1","S":"SELL","x":"TRADE","i":"oid-abc","l":"0.02","z":"0.02","L":"65.1","t":"trade-xyz","R":true}}"#;
        let f = parse_order_trade_update(text, &map()).unwrap();
        assert_eq!(f.aster_side, Side::Sell);
        assert_eq!(f.order_id, "oid-abc");
        assert_eq!(f.trade_id, "trade-xyz");
        assert_eq!(f.event_time_ms, 1700000000999);
        assert!(f.reduce_only);
        assert_eq!(f.usd_fee(), Some(Decimal::ZERO), "no commission pushed: none paid");
    }

    #[test]
    fn classifies_listen_key_expired_via_main_parse() {
        let text = r#"{"e":"listenKeyExpired","E":1700000000123}"#;
        assert!(matches!(classify_user_frame(text), UserFrame::ListenKeyExpired));
    }

    #[test]
    fn classifies_listen_key_expired_via_fallback_probe_on_foreign_shape() {
        // "o" as a number clashes with AsterTradeUpdate's object field — the full parse
        // fails, but the type-tag probe must still catch the key expiry.
        let text = r#"{"e":"listenKeyExpired","o":123}"#;
        assert!(matches!(classify_user_frame(text), UserFrame::ListenKeyExpired));
    }

    #[test]
    fn classifies_trade_update_and_foreign_frames() {
        let trade = r#"{"e":"ORDER_TRADE_UPDATE","E":1,"o":{"s":"HYPEUSDT","S":"BUY","x":"TRADE","i":1,"l":"0.05","z":"0.05","L":"64.5","t":7}}"#;
        assert!(matches!(classify_user_frame(trade), UserFrame::TradeUpdate(_)));
        let account = r#"{"e":"ACCOUNT_UPDATE","E":1,"a":{"B":[]}}"#;
        assert!(matches!(classify_user_frame(account), UserFrame::Other));
        // Garbage shape without a recoverable type tag stays Other (frame dropped).
        assert!(matches!(classify_user_frame(r#"{"e":123}"#), UserFrame::Other));
    }
}

//! Lightweight Aster futures public book feed for the arb scanner hot path.
//!
//! The book is the latest `@depth20@100ms` snapshot with the newer `@bookTicker` top laid
//! over it, both from one combined-stream connection and ordered by Aster's shared update id
//! `u` (their `T` is the 50 ms batch, so time cannot order them). Depth alone lags the top by
//! up to 100 ms: the first dry-run trades (2026-09-26) priced Aster from such stale tops and
//! lost their edge. The book stays hot in memory, so the price decision never waits on REST
//! latency or rate limits; the trading loop loads the latest published [`OrderBook`] Arc
//! lock-free.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arc_swap::ArcSwapOption;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

use crate::connectors::aster::BookTickerMsg;
use crate::taker::book::OrderBook;
use crate::taker::decimal::parse_dec;

const RECONNECT_BASE: Duration = Duration::from_millis(250);
const RECONNECT_MAX: Duration = Duration::from_secs(10);
const READY_POLL: Duration = Duration::from_millis(20);
/// Depth snapshots come every ~100ms and tickers more often; silence this long means a
/// dead/half-open connection. Without this watchdog a NAT/LB silently dropping the
/// connection blocks `read.next()` forever, the book freezes, and the staleness gate
/// halts trading permanently with no reconnect.
const FRAME_TIMEOUT: Duration = Duration::from_secs(10);
/// Client keepalive ping cadence (keeps NAT/LB state alive between server pings).
const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Bound on sink writes so a wedged socket can never block the session task.
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct AsterBookFeed {
    symbol: String,
    state: Arc<AsterBookState>,
    reconnect: Arc<Notify>,
}

#[derive(Default)]
struct AsterBookState {
    book: ArcSwapOption<OrderBook>,
    scan_notify: ArcSwapOption<Notify>,
}
impl AsterBookState {
    fn publish(&self, book: Option<Arc<OrderBook>>) {
        self.book.store(book);
        if let Some(notify) = self.scan_notify.load().as_ref() { notify.notify_one(); }
    }
}

impl AsterBookFeed {
    pub fn spawn_from_rest_base(rest_base_url: &str, symbol_upper: &str) -> Self {
        let symbol = symbol_upper.to_ascii_uppercase();
        let url = futures_book_url(rest_base_url, symbol_upper);
        let state = Arc::new(AsterBookState::default());
        let reconnect = Arc::new(Notify::new());
        tokio::spawn(depth_loop(
            url,
            symbol.clone(),
            state.clone(),
            reconnect.clone(),
        ));
        Self {
            symbol,
            state,
            reconnect,
        }
    }

    pub fn set_scan_notify(&self, notify: Arc<Notify>) {
        self.state.scan_notify.store(Some(notify));
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.order_book().is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("Aster websocket depth not ready for {}", self.symbol);
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }

    pub fn order_book(&self) -> Result<OrderBook> {
        self.order_book_arc().map(|arc| (*arc).clone())
    }

    /// The published book Arc without a snapshot clone (lock-free ArcSwap load). Pointer
    /// identity doubles as the scan loop's change detector: every applied frame stores a
    /// fresh Arc and resets store None, so an old pointer can never alias new data.
    pub fn order_book_arc(&self) -> Result<Arc<OrderBook>> {
        self.state
            .book
            .load_full()
            .ok_or_else(|| anyhow!("Aster websocket depth not ready for {}", self.symbol))
    }

    pub fn request_reconnect(&self) {
        self.state.publish(None);
        self.reconnect.notify_one();
    }
}

async fn depth_loop(
    url: String,
    symbol: String,
    state: Arc<AsterBookState>,
    reconnect: Arc<Notify>,
) {
    let mut backoff = RECONNECT_BASE;
    loop {
        let started = tokio::time::Instant::now();
        match depth_session(&url, &symbol, state.clone(), reconnect.clone()).await {
            Ok(()) => {}
            Err(e) => tracing::warn!("Aster depth websocket disconnected: {e:#}"),
        }
        state.publish(None);
        if started.elapsed() >= Duration::from_secs(60) {
            backoff = RECONNECT_BASE; // the session was healthy: retry fast, as the Lighter streams do
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

async fn depth_session(
    url: &str,
    symbol: &str,
    state: Arc<AsterBookState>,
    reconnect: Arc<Notify>,
) -> Result<()> {
    let ws = crate::connectors::connect_guarded(url).await?;
    let (mut write, mut read) = ws.split();
    tracing::info!("Aster depth connected: symbol={} url={}", symbol, url);

    let mut ping_tick = tokio::time::interval(PING_INTERVAL);
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_tick.tick().await; // first tick fires immediately; skip it (just connected)

    let mut merged = Merged::default();
    loop {
        tokio::select! {
            _ = reconnect.notified() => {
                tracing::warn!("Aster depth reconnect requested: symbol={symbol}");
                return Ok(());
            }
            _ = ping_tick.tick() => {
                match tokio::time::timeout(WRITE_TIMEOUT, write.send(Message::Ping(Vec::new()))).await {
                    Ok(Ok(())) => {}
                    _ => anyhow::bail!("Aster depth keepalive ping failed/wedged: symbol={symbol}"),
                }
            }
            msg = tokio::time::timeout(FRAME_TIMEOUT, read.next()) => {
                let msg = match msg {
                    Ok(Some(msg)) => msg?,
                    Ok(None) => return Ok(()),
                    Err(_) => anyhow::bail!(
                        "Aster depth frame timeout ({}s of silence, half-open connection?): symbol={symbol}",
                        FRAME_TIMEOUT.as_secs()
                    ),
                };
                match msg {
                    Message::Text(text) => {
                        if let Some(book) = merged.apply(&text)? {
                            state.publish(Some(Arc::new(book)));
                        }
                    }
                    Message::Ping(payload) => {
                        // Do not depend on a future writer to flush tungstenite's auto-pong.
                        match tokio::time::timeout(WRITE_TIMEOUT, write.send(Message::Pong(payload))).await {
                            Ok(Ok(())) => {}
                            _ => anyhow::bail!("Aster depth pong write failed/wedged: symbol={symbol}"),
                        }
                    }
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

/// Combined-stream envelope probe: `{"stream":...,"data":{...}}`. The payload is borrowed as
/// a `RawValue` slice so it is re-parsed in place, never cloned.
#[derive(Deserialize)]
struct CombinedRef<'a> {
    #[serde(default)]
    stream: Option<&'a str>,
    #[serde(borrow, default)]
    data: Option<&'a serde_json::value::RawValue>,
}

#[derive(Debug, Deserialize)]
struct DepthMsgRef<'a> {
    #[serde(rename = "e", default)]
    event_type: Option<&'a str>,
    #[serde(rename = "E", default)]
    event_time_ms: i64,
    #[serde(rename = "T", default)]
    transaction_time_ms: i64,
    #[serde(default)]
    u: u64,
    #[serde(default, rename = "bids", alias = "b", borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(default, rename = "asks", alias = "a", borrow)]
    asks: Vec<[&'a str; 2]>,
}

/// A depth payload as `(u, book)`.
fn parse_depth(payload: &str) -> Result<Option<(u64, OrderBook)>> {
    // Borrow-parse: no Value tree, no payload clone, no per-level String allocations.
    // Ack/`result`/`id` frames deserialize as all-default (empty books) and fall out
    // through the emptiness check below. A malformed DEPTH frame still errors — the
    // caller tears the session down for a fresh connection (fail-closed, unchanged).
    let msg: DepthMsgRef<'_> = serde_json::from_str(payload)?;
    let bids = parse_levels(&msg.bids)?;
    let asks = parse_levels(&msg.asks)?;
    if bids.is_empty() || asks.is_empty() {
        if msg.event_type == Some("depthUpdate") {
            anyhow::bail!("Aster depth update is missing one or both book sides");
        }
        return Ok(None);
    }
    Ok(Some((msg.u, book_at(bids, asks, msg.event_time_ms, msg.transaction_time_ms)?)))
}

/// A bookTicker payload as `(u, one-level book)`; none for a crossed or empty top (ignored, as
/// the XEMM connector ignores it).
fn parse_top(payload: &str) -> Result<Option<(u64, OrderBook)>> {
    let t: BookTickerMsg<'_> = serde_json::from_str(payload)?;
    let level = |px: &str, qty: &str| -> Result<(Decimal, Decimal)> { Ok((parse_dec(px)?, parse_dec(qty)?)) };
    let (bid, ask) = (level(t.bid_px, t.bid_qty)?, level(t.ask_px, t.ask_qty)?);
    if bid.0 <= Decimal::ZERO || bid.1 <= Decimal::ZERO || ask.1 <= Decimal::ZERO || bid.0 >= ask.0 {
        return Ok(None);
    }
    Ok(Some((t.u, book_at([bid], [ask], t.event_time, t.trade_time)?)))
}

/// A book stamped with its frame's publish time (`E`, else `T`) and batch time (`T`).
fn book_at(
    bids: impl IntoIterator<Item = (Decimal, Decimal)>,
    asks: impl IntoIterator<Item = (Decimal, Decimal)>,
    event_ms: i64,
    transaction_ms: i64,
) -> Result<OrderBook> {
    let source_ms = if event_ms > 0 { event_ms } else { transaction_ms };
    if source_ms <= 0 { anyhow::bail!("Aster book update is missing its source timestamp"); }
    let mut book = OrderBook::from_levels(bids, asks, ms_to_dt(source_ms), Utc::now());
    book.engine_ts = (transaction_ms > 0).then(|| ms_to_dt(transaction_ms));
    Ok(book)
}

/// The latest depth snapshot and the ticker published after it, each with its update id `u`.
#[derive(Default)]
struct Merged {
    depth: Option<(u64, OrderBook)>,
    top: Option<(u64, OrderBook)>,
}

impl Merged {
    /// Applies one frame; returns the book to publish (none before the first snapshot).
    fn apply(&mut self, text: &str) -> Result<Option<OrderBook>> {
        // Combined frames re-parse only the `data` slice; bare frames parse the whole text.
        let (stream, payload) = match serde_json::from_str::<CombinedRef<'_>>(text) {
            Ok(CombinedRef { stream, data: Some(raw) }) => (stream.unwrap_or_default(), raw.get()),
            _ => ("", text),
        };
        if stream.ends_with("@bookTicker") {
            let Some((u, top)) = parse_top(payload)? else { return Ok(None) };
            if self.depth.as_ref().is_some_and(|(at, _)| u <= *at) {
                return Ok(None);
            }
            self.top = Some((u, top));
        } else {
            let Some((u, depth)) = parse_depth(payload)? else { return Ok(None) };
            self.top = self.top.take().filter(|(at, _)| *at > u);
            self.depth = Some((u, depth));
        }
        Ok(self.depth.as_ref().map(|(_, depth)| match &self.top {
            Some((_, top)) => overlay(depth, top),
            None => depth.clone(),
        }))
    }
}

/// `top`'s level, then `depth`'s levels behind it on each side. Those are as old as the last
/// snapshot (at most ~100 ms while the book moves).
fn overlay(depth: &OrderBook, top: &OrderBook) -> OrderBook {
    let (bid, ask) = (top.bids[0], top.asks[0]);
    let mut book = OrderBook::from_levels(
        std::iter::once(bid).chain(depth.bids.iter().copied().filter(|l| l.px < bid.px)).map(|l| (l.px, l.qty)),
        std::iter::once(ask).chain(depth.asks.iter().copied().filter(|l| l.px > ask.px)).map(|l| (l.px, l.qty)),
        top.exch_ts,
        top.local_recv_ts,
    );
    book.engine_ts = top.engine_ts;
    book
}

fn parse_levels(raw: &[[&str; 2]]) -> Result<Vec<(Decimal, Decimal)>> {
    let mut out = Vec::with_capacity(raw.len());
    for [px, qty] in raw {
        let px = parse_dec(px)?;
        let qty = parse_dec(qty)?;
        if px <= Decimal::ZERO || qty < Decimal::ZERO {
            anyhow::bail!("invalid Aster depth price or size");
        }
        if qty > Decimal::ZERO { out.push((px, qty)); }
    }
    Ok(out)
}

fn futures_book_url(rest_base_url: &str, symbol_upper: &str) -> String {
    let root = crate::connectors::aster::ws_root(rest_base_url);
    let s = symbol_upper.to_ascii_lowercase();
    format!("{root}/stream?streams={s}@depth20@100ms/{s}@bookTicker")
}

fn ms_to_dt(ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(ms.max(0)).unwrap_or(chrono::DateTime::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parses_raw_depth() {
        let raw = r#"{"e":"depthUpdate","E":1568014460893,"T":1568014460891,"s":"HYPEUSDT","bids":[["25.35","31.21"],["25.34","12.00"]],"asks":[["25.36","40.66"],["25.37","9.00"]]}"#;
        let book = parse_depth(raw).unwrap().unwrap().1;
        assert_eq!(book.best_bid().unwrap().px, dec!(25.35));
        assert_eq!(book.best_bid().unwrap().qty, dec!(31.21));
        assert_eq!(book.best_ask().unwrap().px, dec!(25.36));
        assert_eq!(book.best_ask().unwrap().qty, dec!(40.66));
        assert_eq!(book.bids[1].px, dec!(25.34));
        assert_eq!(book.asks[1].px, dec!(25.37));
    }

    #[test]
    fn parses_combined_depth() {
        let raw = r#"{"stream":"hypeusdt@depth20@100ms","data":{"e":"depthUpdate","E":1,"T":2,"b":[["10","1"]],"a":[["11","2"]]}}"#;
        let book = Merged::default().apply(raw).unwrap().unwrap();
        assert_eq!(book.best_bid().unwrap().px, dec!(10));
        assert_eq!(book.best_ask().unwrap().px, dec!(11));
    }

    #[test]
    fn a_newer_ticker_lies_over_the_snapshot_and_an_older_one_does_not() {
        let depth = |u: u64| format!(r#"{{"stream":"hypeusdt@depth20@100ms","data":{{"e":"depthUpdate","E":5,"T":5,"u":{u},"b":[["10","1"],["9.9","1"]],"a":[["10.1","1"],["10.2","1"]]}}}}"#);
        let ticker = |u: u64, bid: &str, ask: &str| format!(r#"{{"stream":"hypeusdt@bookTicker","data":{{"e":"bookTicker","u":{u},"b":"{bid}","B":"3","a":"{ask}","A":"4","T":5,"E":6}}}}"#);
        let prices = |book: OrderBook| (book.bids.iter().map(|l| l.px).collect::<Vec<_>>(), book.asks.iter().map(|l| l.px).collect::<Vec<_>>());
        let mut m = Merged::default();
        assert!(m.apply(&ticker(5, "10", "10.1")).unwrap().is_none(), "no book before a snapshot");
        let book = m.apply(&depth(10)).unwrap().unwrap();
        assert_eq!(prices(book), (vec![dec!(10), dec!(9.9)], vec![dec!(10.1), dec!(10.2)]), "the older ticker is dropped");
        // A newer top: its level first, the levels it crossed out gone, the deeper ones kept.
        let book = m.apply(&ticker(12, "10.05", "10.15")).unwrap().unwrap();
        assert_eq!((book.best_bid().unwrap().qty, book.best_ask().unwrap().qty), (dec!(3), dec!(4)));
        assert_eq!(prices(book), (vec![dec!(10.05), dec!(10), dec!(9.9)], vec![dec!(10.15), dec!(10.2)]));
        assert!(m.apply(&ticker(9, "9", "12")).unwrap().is_none(), "older than the snapshot");
        assert!(m.apply(&ticker(13, "10.1", "10.1")).unwrap().is_none(), "a crossed top is ignored");
        // A snapshot of the same 50 ms batch, published before the ticker, keeps it on top.
        assert_eq!(prices(m.apply(&depth(11)).unwrap().unwrap()).1, vec![dec!(10.15), dec!(10.2)]);
        assert_eq!(prices(m.apply(&depth(14)).unwrap().unwrap()).1, vec![dec!(10.1), dec!(10.2)]);
    }

    #[test]
    fn ack_frames_yield_no_book_and_malformed_frames_error() {
        // Subscription acks have no depth payload: all-default parse -> empty -> None.
        assert!(parse_depth(r#"{"result":null,"id":1}"#).unwrap().is_none());
        // An incomplete real depth update invalidates the old cached book immediately.
        assert!(parse_depth(r#"{"e":"depthUpdate","E":1,"T":2,"b":[],"a":[]}"#).is_err());
        // Malformed depth data must still ERROR -> session teardown (fail-closed).
        assert!(parse_depth(r#"{"b":[["10"]],"a":[["11","2"]]}"#).is_err());
        assert!(parse_depth("not json").is_err());
    }

    #[test]
    fn derives_the_book_streams_from_the_rest_base() {
        assert_eq!(
            futures_book_url("https://fapi.asterdex.com", "HYPEUSDT"),
            "wss://fstream.asterdex.com/stream?streams=hypeusdt@depth20@100ms/hypeusdt@bookTicker"
        );
        assert_eq!(
            futures_book_url("http://127.0.0.1:18081/", "HYPEUSDT"),
            "ws://127.0.0.1:18081/stream?streams=hypeusdt@depth20@100ms/hypeusdt@bookTicker"
        );
    }
}

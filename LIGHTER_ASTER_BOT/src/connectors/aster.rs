//! Aster (asterdex) futures market-data WebSocket connector: subscribes to the
//! `<sym>@depth20@100ms` partial-depth snapshot, `<sym>@bookTicker` top-of-book
//! assist, and `<sym>@aggTrade` streams via a combined-stream connection, and
//! publishes each book into the hot-path [`Tap`]. Trades only count as liveness.
//! Using partial-depth snapshots avoids diff/sequence (U/u/pu) maintenance entirely.
//! Responds to server pings; reconnects with capped backoff (24h server cap).

use anyhow::{Context, Result};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, info, warn};

use super::Tap;
use crate::book::PriceLevel;
use crate::decimal::parse_dec;

/// The websocket root that serves the same venue as the REST base `rest_base`. Mainnet streams
/// live on their own host; any other base (the dry-run venue on loopback) serves both on one.
/// Deriving it keeps one configured origin per venue, so no stream can fall back to mainnet.
pub fn ws_root(rest_base: &str) -> String {
    let base = rest_base.trim_end_matches('/');
    match base.strip_prefix("https://") {
        Some("fapi.asterdex.com") => "wss://fstream.asterdex.com".to_string(),
        Some(host) => format!("wss://{host}"),
        None => base.strip_prefix("http://").map_or_else(|| base.to_string(), |host| format!("ws://{host}")),
    }
}

#[derive(Deserialize)]
struct Combined<'a> {
    stream: &'a str,
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}

#[derive(Deserialize)]
struct DepthMsg<'a> {
    #[serde(rename = "E", default)]
    event_time: i64,
    #[serde(rename = "b", alias = "bids", borrow, default)]
    bids: Vec<[&'a str; 2]>,
    #[serde(rename = "a", alias = "asks", borrow, default)]
    asks: Vec<[&'a str; 2]>,
}

/// Also the taker's (`taker::aster::ws`), which orders it against depth by the update id `u`.
#[derive(Deserialize)]
pub(crate) struct BookTickerMsg<'a> {
    #[serde(rename = "E", default)]
    pub(crate) event_time: i64,
    #[serde(rename = "T", default)]
    pub(crate) trade_time: i64,
    #[serde(default)]
    pub(crate) u: u64,
    #[serde(rename = "b")]
    pub(crate) bid_px: &'a str,
    #[serde(rename = "B")]
    pub(crate) bid_qty: &'a str,
    #[serde(rename = "a")]
    pub(crate) ask_px: &'a str,
    #[serde(rename = "A")]
    pub(crate) ask_qty: &'a str,
}

impl<'a> BookTickerMsg<'a> {
    fn ts_ms(&self) -> i64 {
        if self.event_time != 0 { self.event_time } else { self.trade_time }
    }
}

/// Idle-reconnect threshold. `depth20@100ms` is *event-driven* — it only pushes when
/// the top-20 levels change, so a quiet/thin book can be legitimately silent for many
/// seconds while the socket is perfectly healthy (Aster keeps it alive with a server
/// ping every ~3–5 min). So we size this well above that ping cadence: long enough not
/// to tear down a healthy quiet feed, short enough to still catch a genuinely half-open
/// socket (no Close frame, no error) inside Aster's ~10–15 min pong-death window. Fast
/// liveness/freshness detection is owned elsewhere — the 60s stream watchdog and the
/// 30s REST book-check.
const IDLE_TIMEOUT: Duration = Duration::from_secs(360);
/// A connection that stayed up at least this long is "healthy"; reset the backoff
/// after it so a single long session isn't punished by a grown backoff.
const HEALTHY_AFTER: Duration = Duration::from_secs(60);

/// Run forever (until aborted), reconnecting on error: fans each book out to the
/// lock-free [`Tap`] and honors the watchdog's reconnect signal.
pub async fn run_with_tap(
    ws_root: String,
    symbol_lower: String,
    tap: Tap,
) {
    let mut backoff = 1u64;
    loop {
        let started = std::time::Instant::now();
        match stream_once(&ws_root, &symbol_lower, &tap).await {
            Ok(()) => info!("[ASTER {}] stream closed", symbol_lower),
            Err(e) => warn!("[ASTER {}] error: {e:#}", symbol_lower),
        }
        // Whether closed or errored, the stream is KNOWN down until the next connect's
        // full snapshot: flag the cell so the maker gate closes now, not at age expiry.
        tap.mark_stream_down();
        if started.elapsed() >= HEALTHY_AFTER {
            backoff = 1; // the connection was healthy; start the next retry fast
        }
        sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

async fn stream_once(
    ws_root: &str,
    symbol: &str,
    tap: &Tap,
) -> Result<()> {
    let url = format!("{ws_root}/stream?streams={symbol}@depth20@100ms/{symbol}@bookTicker/{symbol}@aggTrade");
    let ws = super::connect_guarded(&url).await.context("connect Aster ws")?;
    let (mut write, mut read) = ws.split();
    info!("[ASTER {}] subscribed depth20@100ms + bookTicker + aggTrade", symbol);

    // Race each read against (a) an idle deadline so a silent socket forces a
    // reconnect instead of hanging here forever, and (b) the watchdog's reconnect
    // signal. The idle deadline resets on every frame.
    loop {
        tokio::select! {
            msg = read.next() => {
                let Some(msg) = msg else { break };
                match msg? {
                    Message::Text(text) => handle(&text, tap).await,
                    Message::Ping(p) => super::send_guarded(&mut write, Message::Pong(p)).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
                tap.touch(); // liveness on every inbound frame
            }
            _ = sleep(IDLE_TIMEOUT) => {
                anyhow::bail!("idle >{}s, forcing reconnect", IDLE_TIMEOUT.as_secs());
            }
            _ = tap.wait_reconnect() => {
                anyhow::bail!("watchdog forced reconnect");
            }
        }
    }
    Ok(())
}

async fn handle(text: &str, tap: &Tap) {
    let Combined { stream, data } = match serde_json::from_str::<Combined<'_>>(text) {
        Ok(c) => c,
        Err(_) => return,
    };
    if stream.contains("@aggTrade") {
        // Trades are not consumed: every frame already refreshed liveness
        // (`tap.touch()` in `stream_once`), which keeps a quiet book's feed fresh.
    } else if stream.contains("@bookTicker") {
        if let Ok(t) = serde_json::from_str::<BookTickerMsg<'_>>(data.get()) {
            // Validate BEFORE any publish. The hot-only publish below sets the
            // bbo_hot_only_pending guard that only the raw publish clears; a
            // crossed/zero-qty frame that bailed between the two used to leave the
            // guard latched — reprice_market then early-returns, freezing placements
            // AND slow-path cancels until the next VALID bookTicker arrives.
            let (Ok(bp), Ok(bq), Ok(ap), Ok(aq)) = (
                parse_dec(t.bid_px),
                parse_dec(t.bid_qty),
                parse_dec(t.ask_px),
                parse_dec(t.ask_qty),
            ) else {
                return;
            };
            if bp <= Decimal::ZERO || bq <= Decimal::ZERO || ap <= Decimal::ZERO || aq <= Decimal::ZERO || bp >= ap {
                return;
            }
            let exch_ts = ms_to_dt(t.ts_ms());
            if crate::hot_types::source_age_at_receive_ms(t.ts_ms(), chrono::Utc::now().timestamp_millis()) == i64::MAX {
                tap.mark_stream_down();
                return;
            }
            let prebuilt_hot = tap.hot_book_from_raw(
                std::iter::once((t.bid_px, t.bid_qty)),
                std::iter::once((t.ask_px, t.ask_qty)),
                exch_ts,
            );
            if let Some((hot, _)) = prebuilt_hot.as_ref() {
                tap.publish_bbo_hot_only(*hot, exch_ts);
            }
            // Aster BBO *size* influences quote safety: the quote engine only trusts a
            // bookTicker touch as the effective touch when its visible quantity covers
            // the candidate order. Therefore size-only updates must wake the strategy
            // too; using the price-only coalescer here can leave a quote resting up to
            // the next cold tick after top size vanishes.
            tap.publish_bbo_prebuilt((bp, bq), (ap, aq), exch_ts, prebuilt_hot);
        }
    } else if stream.contains("@depth") {
        if let Ok(d) = serde_json::from_str::<DepthMsg<'_>>(data.get()) {
            let exch_ts = ms_to_dt(d.event_time);
            if crate::hot_types::source_age_at_receive_ms(d.event_time, chrono::Utc::now().timestamp_millis()) == i64::MAX {
                tap.mark_stream_down();
                return;
            }
            let prebuilt_hot = tap.hot_book_from_raw(
                d.bids.iter().map(|r| (r[0], r[1])),
                d.asks.iter().map(|r| (r[0], r[1])),
                exch_ts,
            );
            if let Some((hot, _)) = prebuilt_hot.as_ref() {
                tap.publish_hot_only(*hot, exch_ts);
            }
            let bids = to_levels(&d.bids);
            let asks = to_levels(&d.asks);
            tap.publish_prebuilt(&bids, &asks, exch_ts, prebuilt_hot);
        }
    } else {
        debug!("[ASTER] unhandled stream {stream}");
    }
}

fn to_levels(rows: &[[&str; 2]]) -> Vec<PriceLevel> {
    rows.iter()
        .filter_map(|r| match (parse_dec(r[0]), parse_dec(r[1])) {
            (Ok(p), Ok(q)) if p > Decimal::ZERO && q > Decimal::ZERO => Some((p, q)),
            _ => None,
        })
        .collect()
}

fn ms_to_dt(ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(ms.max(0)).unwrap_or(chrono::DateTime::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_root_follows_the_configured_rest_base() {
        for (rest, ws) in [
            ("https://fapi.asterdex.com", "wss://fstream.asterdex.com"),
            ("https://fapi.asterdex.com/", "wss://fstream.asterdex.com"),
            ("http://127.0.0.1:18081", "ws://127.0.0.1:18081"),
            ("http://127.0.0.1:18081/", "ws://127.0.0.1:18081"),
            ("https://fapi.example.test", "wss://fapi.example.test"),
            ("ws://127.0.0.1:9000", "ws://127.0.0.1:9000"),
        ] {
            assert_eq!(ws_root(rest), ws, "{rest}");
        }
    }

    #[tokio::test]
    async fn invalid_book_ticker_never_latches_the_hot_only_guard() {
        // A crossed/zero-qty bookTicker frame must be a complete no-op. Before the
        // validate-first reorder, the hot-only publish fired before validation and the
        // bail skipped the raw publish that clears bbo_hot_only_pending — leaving the
        // guard latched so reprice_market froze placements AND slow-path cancels until
        // the next valid frame.
        use crate::connectors::BookTap;
        use crate::hotpath::book_cell::VenueBook;
        use std::sync::Arc;

        let cell = Arc::new(VenueBook::new());
        let tap = Tap { book: Some(cell.clone() as Arc<dyn BookTap>), ..Tap::none() };
        let frame = |b: &str, bq: &str, a: &str, aq: &str| {
            format!(
                r#"{{"stream":"hypeusdt@bookTicker","data":{{"e":"bookTicker","u":1,"s":"HYPEUSDT","b":"{b}","B":"{bq}","a":"{a}","A":"{aq}","T":1,"E":2}}}}"#
            )
        };

        // Valid frame: BBO populated and the pending guard is cleared by the raw publish.
        handle(&frame("70.5", "10", "70.6", "12"), &tap).await;
        let before = cell.load_bbo().expect("valid frame populates the BBO slot");
        assert!(!cell.has_hot_only_update());

        // Crossed frame (bid >= ask): no publish at all, guard must stay clear.
        handle(&frame("70.7", "10", "70.6", "12"), &tap).await;
        assert!(
            !cell.has_hot_only_update(),
            "crossed bookTicker latched the hot-only guard"
        );
        // Zero-qty frame likewise.
        handle(&frame("70.5", "0", "70.6", "12"), &tap).await;
        assert!(
            !cell.has_hot_only_update(),
            "zero-qty bookTicker latched the hot-only guard"
        );
        // And the previously published BBO is untouched.
        let after = cell.load_bbo().expect("BBO still present");
        assert_eq!(
            before.best_bid().unwrap().px,
            after.best_bid().unwrap().px
        );
        assert_eq!(
            before.best_ask().unwrap().px,
            after.best_ask().unwrap().px
        );
    }

    #[test]
    fn parses_aster_book_ticker_wire_shape() {
        let raw = r#"{
            "stream":"hypeusdt@bookTicker",
            "data":{
                "e":"bookTicker",
                "u":481270967632,
                "s":"HYPEUSDT",
                "b":"70.61400",
                "B":"365.37",
                "a":"70.62800",
                "A":"220.16",
                "T":1781952197150,
                "E":1781952197185
            }
        }"#;
        let wrap: Combined<'_> = serde_json::from_str(raw).unwrap();
        assert_eq!(wrap.stream, "hypeusdt@bookTicker");
        let ticker: BookTickerMsg<'_> = serde_json::from_str(wrap.data.get()).unwrap();
        assert_eq!(ticker.event_time, 1781952197185);
        assert_eq!(ticker.trade_time, 1781952197150);
        assert_eq!(ticker.ts_ms(), 1781952197185);
        assert_eq!(ticker.bid_px, "70.61400");
        assert_eq!(ticker.bid_qty, "365.37");
        assert_eq!(ticker.ask_px, "70.62800");
        assert_eq!(ticker.ask_qty, "220.16");
    }
}

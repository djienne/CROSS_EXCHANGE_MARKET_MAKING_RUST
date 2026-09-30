//! Hyperliquid public books: fast `l2Book` supplies five levels, `bbo` supplies changed tops.
//! Each occupies its own hedge cell slot; a thin BBO requires fresh L2 depth.
//! Shared subscriptions are also used by the taker and dry run; cadence measurements are in the screener README.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::time::{interval, sleep, sleep_until};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::warn;

use super::Tap;
use crate::book::PriceLevel;

/// Hyperliquid closes a connection that sent nothing for 60 s; its ping is an app message.
const PING_EVERY: Duration = Duration::from_secs(20);
/// A pong is due every 20 s, so 30 s without any frame means a dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// A connection that stayed up this long resets the reconnect backoff.
const HEALTHY_AFTER: Duration = Duration::from_secs(60);

/// The websocket of the REST origin `rest_base`.
pub fn ws_url(rest_base: &str) -> String {
    format!("{}/ws", rest_base.trim_end_matches('/').replacen("http", "ws", 1))
}

#[derive(Deserialize)]
struct Level {
    px: Decimal,
    sz: Decimal,
}

#[derive(Deserialize)]
#[serde(tag = "channel", content = "data")]
enum Frame {
    #[serde(rename = "l2Book")]
    L2 { time: i64, levels: [Vec<Level>; 2] },
    #[serde(rename = "bbo")]
    Bbo { time: i64, bbo: [Option<Level>; 2] },
}

/// The market-data subscriptions for `coin`, `l2Book` in its fast mode.
pub fn subscriptions(coin: &str) -> [serde_json::Value; 2] {
    [serde_json::json!({"type": "l2Book", "coin": coin, "fast": true}), serde_json::json!({"type": "bbo", "coin": coin})]
}

/// Runs until aborted, reconnecting; honors the watchdog's reconnect signal.
pub async fn run_with_tap(ws_url: String, coin: String, tap: Tap) {
    let mut backoff = 1u64;
    loop {
        let started = Instant::now();
        if let Err(e) = stream_once(&ws_url, &coin, &tap).await {
            warn!("[HYPERLIQUID {coin}] {e:#}");
        }
        // Known down until the next connect's first book.
        tap.mark_stream_down();
        if started.elapsed() >= HEALTHY_AFTER {
            backoff = 1;
        }
        sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

async fn stream_once(url: &str, coin: &str, tap: &Tap) -> Result<()> {
    let ws = super::connect_guarded(url).await.context("connect Hyperliquid ws")?;
    let (mut write, mut read) = ws.split();
    for subscription in subscriptions(coin) {
        let subscribe = serde_json::json!({"method": "subscribe", "subscription": subscription});
        super::send_guarded(&mut write, Message::Text(subscribe.to_string())).await?;
    }
    let mut ping = interval(PING_EVERY);
    let mut deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
    loop {
        tokio::select! {
            msg = read.next() => {
                match msg.context("the server closed the stream")?? {
                    Message::Text(text) => handle(&text, tap),
                    Message::Ping(p) => super::send_guarded(&mut write, Message::Pong(p)).await?,
                    Message::Close(_) => bail!("the server closed the stream"),
                    _ => {}
                }
                deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
                tap.touch();
            }
            _ = ping.tick() => super::send_guarded(&mut write, Message::Text(r#"{"method":"ping"}"#.into())).await?,
            _ = sleep_until(deadline) => bail!("no frame for {} s", IDLE_TIMEOUT.as_secs()),
            _ = tap.wait_reconnect() => bail!("watchdog forced reconnect"),
        }
    }
}

fn handle(text: &str, tap: &Tap) {
    let (time, bids, asks, bbo) = match serde_json::from_str::<Frame>(text) {
        Ok(Frame::L2 { time, levels: [bids, asks] }) => (time, levels(bids), levels(asks), false),
        Ok(Frame::Bbo { time, bbo: [bid, ask] }) => (time, levels(bid), levels(ask), true),
        Err(_) => return, // pong, subscriptionResponse
    };
    let Some(exch_ts) = DateTime::<Utc>::from_timestamp_millis(time) else { return };
    if crate::hot_types::source_age_at_receive_ms(time, Utc::now().timestamp_millis()) == i64::MAX {
        tap.mark_stream_down();
        return;
    }
    // Checked before any publish: a hot-only publish latches a guard that only the full one clears.
    if bids.is_empty() || asks.is_empty() || bids[0].0 >= asks[0].0 {
        return;
    }
    let hot = tap.hot_book_from_levels(&bids, &asks, exch_ts);
    if bbo {
        if let Some((h, _)) = hot.as_ref() {
            tap.publish_bbo_hot_only(*h, exch_ts);
        }
        tap.publish_bbo_prebuilt(bids[0], asks[0], exch_ts, hot);
    } else {
        if let Some((h, _)) = hot.as_ref() {
            tap.publish_hot_only(*h, exch_ts);
        }
        tap.publish_prebuilt(&bids, &asks, exch_ts, hot);
    }
}

fn levels(rows: impl IntoIterator<Item = Level>) -> Vec<PriceLevel> {
    rows.into_iter().filter(|l| l.px > Decimal::ZERO && l.sz > Decimal::ZERO).map(|l| (l.px, l.sz)).collect()
}

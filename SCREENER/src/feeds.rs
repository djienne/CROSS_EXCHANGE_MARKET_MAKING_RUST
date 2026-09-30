//! The venues' public websockets, followed until stopped: connect and write timeouts, a silence
//! watchdog, keepalive pings, reconnect backoff capped at 5 s, and subscriptions
//! paced under Lighter's limit of 200 sent messages a minute.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

pub const ASTER_WS: &str = "wss://fstream.asterdex.com";
pub const LIGHTER_WS: &str = "wss://mainnet.zklighter.elliot.ai/stream";
pub const HYPERLIQUID_WS: &str = "wss://api.hyperliquid.xyz/ws";
/// Aster allows 200 streams per connection.
pub const ASTER_STREAMS_PER_CONNECTION: usize = 190;
/// Lighter allows 500 subscriptions per connection.
pub const LIGHTER_SUBSCRIPTIONS_PER_CONNECTION: usize = 400;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Dozens of markets update every second, so this long without a frame is a dead connection.
const SILENCE: Duration = Duration::from_secs(30);
const PING_EVERY: Duration = Duration::from_secs(20);
const SUBSCRIBE_EVERY: Duration = Duration::from_millis(350);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// What a connection delivers: a text frame, or the end of its session.
pub enum Wire<'a> {
    Opened,
    Text(&'a str),
    Closed,
}

/// Follows `url` until `stop`. After each connect it sends `subscribe` (one message each, paced),
/// then hands `on` every text frame, and `Wire::Closed` when a session ends.
pub async fn follow(url: &str, label: &str, subscribe: &[String], stop: &CancellationToken, mut on: impl FnMut(Wire<'_>)) {
    let mut backoff = Duration::from_millis(500);
    loop {
        let started = Instant::now();
        let result = tokio::select! {
            result = session(url, subscribe, &mut on) => result,
            _ = stop.cancelled() => return,
        };
        if let Err(e) = result {
            tracing::warn!("{label}: {e:#}");
        }
        on(Wire::Closed);
        if started.elapsed() >= Duration::from_secs(60) {
            backoff = Duration::from_millis(500);
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stop.cancelled() => return,
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn session(url: &str, subscribe: &[String], on: &mut impl FnMut(Wire<'_>)) -> Result<()> {
    let (ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url)).await.context("connect stalled")??;
    on(Wire::Opened);
    let (mut write, mut read) = ws.split();
    let mut pending = subscribe.iter();
    let mut next = pending.next();
    let mut pace = tokio::time::interval(SUBSCRIBE_EVERY);
    let mut ping = tokio::time::interval(PING_EVERY);
    let mut watchdog = tokio::time::interval(Duration::from_secs(5));
    let mut last_frame = Instant::now();
    loop {
        let msg = tokio::select! {
            msg = read.next() => match msg {
                Some(msg) => msg?,
                None => return Ok(()),
            },
            _ = pace.tick(), if next.is_some() => {
                send(&mut write, Message::Text(next.unwrap().clone())).await?;
                next = pending.next();
                continue;
            }
            _ = ping.tick() => {
                let ping = if url == HYPERLIQUID_WS { Message::Text(r#"{"method":"ping"}"#.into()) } else { Message::Ping(Vec::new()) };
                send(&mut write, ping).await?;
                continue;
            }
            _ = watchdog.tick() => {
                if last_frame.elapsed() > SILENCE {
                    bail!("no frame for {} s", SILENCE.as_secs());
                }
                continue;
            }
        };
        last_frame = Instant::now();
        match msg {
            // Lighter's application keepalive.
            Message::Text(text) if text.len() < 64 && text.contains(r#""ping""#) => send(&mut write, Message::Text(r#"{"type":"pong"}"#.into())).await?,
            Message::Text(text) => on(Wire::Text(&text)),
            Message::Ping(payload) => send(&mut write, Message::Pong(payload)).await?,
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

/// A send that cannot wedge the reader: a stalled write ends the session instead.
async fn send<S>(write: &mut S, msg: Message) -> Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::time::timeout(WRITE_TIMEOUT, write.send(msg)).await.context("write stalled")??;
    Ok(())
}

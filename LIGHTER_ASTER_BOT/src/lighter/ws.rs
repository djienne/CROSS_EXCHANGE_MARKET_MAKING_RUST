//! Lighter WebSocket subscription primitives — port of `ws_manager.py`.
//!
//! `subscribe_loop` connects, subscribes to channels (optionally with per-channel auth),
//! handles ping/pong + the `subscribed` confirmation, applies liveness/data watchdogs,
//! and reconnects with exponential backoff. Each decoded application message is handed to a
//! synchronous callback (the hot-path market-data task runs its book+signal update there;
//! cold-path account tasks enqueue to channels). A callback must not panic: a panic outside
//! the strategy thread aborts the process (`taker::abort_on_panic`).

use anyhow::Result;
use futures_util::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

/// The `/stream` websocket of the Lighter venue whose REST base is `base`: Lighter serves both
/// from one host, so every stream follows the configured origin (mainnet, or the dry-run venue).
pub fn stream_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if let Some(host) = base.strip_prefix("https://") {
        format!("wss://{host}/stream")
    } else if let Some(host) = base.strip_prefix("http://") {
        format!("ws://{host}/stream")
    } else if base.starts_with("ws://") || base.starts_with("wss://") {
        format!("{base}/stream")
    } else {
        format!("wss://{base}/stream")
    }
}

/// A routed application frame: the raw JSON text plus its already-extracted top-level
/// `type` tag. Handlers deserialize the raw text straight into typed structs
/// (`serde_json::from_str`) — no intermediate `Value` tree is ever built.
pub struct LighterFrame<'a> {
    /// Top-level `type` (absent on untyped frames). `Cow` because JSON escapes in the
    /// tag (legal, e.g. `"update\/order_book"`) force an owned unescape.
    pub msg_type: Option<std::borrow::Cow<'a, str>>,
    pub raw: &'a str,
}

/// Envelope probe: deserializes ONLY the top-level `type` tag for routing.
#[derive(serde::Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type", borrow, default)]
    msg_type: Option<std::borrow::Cow<'a, str>>,
}

/// Proactive client-ping interval. Lighter closes any connection that sends NO frame for 2
/// minutes (<https://apidocs.lighter.xyz/docs/websocket-reference>), so quiet streams (e.g.
/// account/user_stats) must emit a keepalive frame well under that window — matches Python's
/// `ping_interval=20`.
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Clone)]
pub struct SubscribeOptions {
    pub url: String,
    pub channels: Vec<String>,
    pub channel_auths: HashMap<String, String>,
    /// Reconnect when no real application message arrives within this many seconds.
    /// Quiet private streams may set this to `None`; socket liveness is still guarded by
    /// `frame_timeout`.
    pub data_timeout: Option<f64>,
    /// Reconnect when no websocket/control/application frame arrives within this many seconds.
    pub frame_timeout: f64,
    pub ping_interval: Duration,
    pub reconnect_base: f64,
    pub reconnect_max: f64,
    pub label: String,
}

impl SubscribeOptions {
    pub fn new(url: &str, label: &str, channels: Vec<String>) -> Self {
        Self {
            url: url.to_string(),
            channels,
            channel_auths: HashMap::new(),
            data_timeout: Some(30.0),
            frame_timeout: 90.0,
            ping_interval: WS_PING_INTERVAL,
            reconnect_base: 5.0,
            reconnect_max: 60.0,
            label: label.to_string(),
        }
    }
}

/// Deterministic jitter in [0, 0.2*base) without an RNG dependency (matches the spirit of
/// the Python `backoff*0.2*(monotonic()%1)`), seeded by wall-clock ms.
fn jitter(base: f64) -> f64 {
    let now_ms = chrono::Utc::now().timestamp_millis().unsigned_abs();
    let frac = (now_ms % 1000) as f64 / 1000.0;
    base * 0.2 * frac
}

fn next_reconnect_backoff(current: f64, base: f64, max: f64, elapsed: Duration) -> f64 {
    if elapsed >= Duration::from_secs(60) {
        base
    } else {
        (current * 2.0).min(max).max(base)
    }
}

fn reconnect_delay_after_session(current: f64, base: f64, elapsed: Duration) -> f64 {
    if elapsed >= Duration::from_secs(60) {
        base
    } else {
        current
    }
}

/// Sleep the jittered delay after a session that lasted `elapsed`; returns the next backoff base.
async fn pause_before_reconnect(opts: &SubscribeOptions, backoff: f64, elapsed: Duration) -> f64 {
    let delay = reconnect_delay_after_session(backoff, opts.reconnect_base, elapsed);
    let sleep_for = delay + jitter(delay);
    let next = next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed);
    tracing::info!(
        "{} reconnecting in {:.3}s after session {:.3}s (next_backoff_base={:.3}s)",
        opts.label,
        sleep_for,
        elapsed.as_secs_f64(),
        next,
    );
    sleep(Duration::from_secs_f64(sleep_for)).await;
    next
}

/// Run the subscription loop forever (reconnecting). `on_message` is called for each
/// decoded application message (NOT ping/subscribed). `reconnect` (if provided) forces a
/// fresh reconnect when notified (e.g. orderbook sanity divergence). `on_disconnect` runs
/// on every disconnect (e.g. clear the local book, flag the stream down).
pub async fn subscribe_loop<F, D>(
    opts: SubscribeOptions,
    reconnect: Option<std::sync::Arc<Notify>>,
    mut on_message: F,
    mut on_disconnect: D,
) where
    F: FnMut(&LighterFrame<'_>),
    D: FnMut(),
{
    let mut backoff = opts.reconnect_base;
    loop {
        let started = Instant::now();
        match session(&opts, reconnect.as_deref(), &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        on_disconnect();
        backoff = pause_before_reconnect(&opts, backoff, started.elapsed()).await;
    }
}

/// Like `subscribe_loop` but regenerates per-channel auth tokens before EACH connection
/// (private channels: account_orders / account_all / user_stats). The server token TTL is
/// ~10 min; on expiry the server drops the socket, the session ends, and we reconnect with a
/// fresh token. `auth_fn` returns the channel->token map for the upcoming connection.
pub async fn subscribe_loop_authed<F, A>(
    mut opts: SubscribeOptions,
    mut auth_fn: A,
    mut on_message: F,
) where
    F: FnMut(&LighterFrame<'_>),
    A: FnMut() -> std::collections::HashMap<String, String>,
{
    let mut backoff = opts.reconnect_base;
    loop {
        opts.channel_auths = auth_fn();
        if opts.channel_auths.is_empty() {
            tracing::warn!("{}: no auth token; retrying", opts.label);
            sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(opts.reconnect_max);
            continue;
        }
        let started = Instant::now();
        match session(&opts, None, &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        backoff = pause_before_reconnect(&opts, backoff, started.elapsed()).await;
    }
}

async fn session<F>(
    opts: &SubscribeOptions,
    reconnect: Option<&Notify>,
    on_message: &mut F,
) -> Result<()>
where
    F: FnMut(&LighterFrame<'_>),
{
    let ws_stream = crate::connectors::connect_guarded(&opts.url).await?;
    let (mut write, mut read) = ws_stream.split();
    tracing::info!("connected to {} for {}", opts.url, opts.label);

    for ch in &opts.channels {
        let mut sub = serde_json::json!({"type": "subscribe", "channel": ch});
        if let Some(auth) = opts.channel_auths.get(ch) {
            sub["auth"] = Value::String(auth.clone());
        }
        crate::connectors::send_guarded(&mut write, Message::Text(sub.to_string())).await?;
    }
    tracing::info!("{} subscribed to {:?}", opts.label, opts.channels);

    let data_to = opts.data_timeout.map(Duration::from_secs_f64);
    let frame_to = Duration::from_secs_f64(opts.frame_timeout);
    let mut ping_tick = tokio::time::interval(opts.ping_interval);
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_tick.tick().await; // consume the immediate first tick (just connected)
    let mut last_frame = Instant::now();
    let mut last_data = Instant::now();
    loop {
        // Race the read against the keepalive tick and the forced-reconnect Notify. On the tick
        // we send a proactive client Ping (so a quiet stream still satisfies Lighter's 2-min
        // "send a frame" rule). Socket liveness is based on ANY received frame; optional data
        // freshness is based only on real application messages. The Notify is a real arm, so a
        // request during a quiet stretch fires at once; recreating `notified()` per pass is
        // safe, as an unconsumed permit stays stored.
        let msg = tokio::select! {
            _ = async {
                match reconnect {
                    Some(rc) => rc.notified().await,
                    None => std::future::pending().await,
                }
            } => {
                tracing::info!("{} reconnect requested; dropping for fresh snapshot", opts.label);
                return Ok(());
            }
            _ = ping_tick.tick() => {
                if last_frame.elapsed() > frame_to {
                    tracing::warn!("{} watchdog: no frames for {}s", opts.label, opts.frame_timeout);
                    return Ok(());
                }
                if let Some(data_to) = data_to {
                    if last_data.elapsed() > data_to {
                        tracing::warn!(
                            "{} watchdog: no application data for {}s",
                            opts.label,
                            opts.data_timeout.unwrap_or_default()
                        );
                        return Ok(());
                    }
                }
                // Guarded: a wedged TCP write side otherwise blocks the reader task
                // inline, making this very watchdog (and the gap-resync Notify)
                // unreachable until the OS gives up on the socket.
                if crate::connectors::send_guarded(&mut write, Message::Ping(Vec::new()))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                continue;
            }
            res = read.next() => match res {
                Some(Ok(m)) => m,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            },
        };

        last_frame = Instant::now();
        match msg {
            Message::Text(t) => {
                // Envelope-only parse: route on the top-level `type` tag without ever
                // building a `Value` tree. Handlers typed-parse the raw text themselves.
                let env: Envelope<'_> = match serde_json::from_str(&t) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                match env.msg_type.as_deref() {
                    Some("ping") => {
                        // Server app-level keepalive — reply, but do NOT count as feed data.
                        // Guarded + fail-fast: a stalled/broken write side ends the session
                        // (normal reconnect path) instead of wedging the reader.
                        if crate::connectors::send_guarded(
                            &mut write,
                            Message::Text(r#"{"type":"pong"}"#.to_string()),
                        )
                        .await
                        .is_err()
                        {
                            return Ok(());
                        }
                    }
                    Some("connected") | Some("subscribed") => {}
                    _ => {
                        // Real application message (typed or untyped — untyped frames are
                        // still delivered) — the only thing that refreshes the data
                        // watchdog. Callbacks here are written to not panic.
                        last_data = Instant::now();
                        on_message(&LighterFrame {
                            msg_type: env.msg_type,
                            raw: &t,
                        });
                    }
                }
            }
            Message::Ping(p) => {
                if crate::connectors::send_guarded(&mut write, Message::Pong(p))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use futures_util::SinkExt;
    use tokio::time::{sleep, timeout};
    use tokio_tungstenite::accept_async;

    async fn local_ws_url() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    fn opts(url: String) -> SubscribeOptions {
        let mut opts = SubscribeOptions::new(&url, "test", vec!["test/channel".to_string()]);
        opts.reconnect_base = 0.01;
        opts.reconnect_max = 0.01;
        opts.ping_interval = Duration::from_millis(50);
        opts
    }

    #[test]
    fn stream_url_follows_the_configured_base() {
        for (base, url) in [
            ("https://mainnet.zklighter.elliot.ai", "wss://mainnet.zklighter.elliot.ai/stream"),
            ("https://testnet.zklighter.elliot.ai/", "wss://testnet.zklighter.elliot.ai/stream"),
            ("http://127.0.0.1:18082/", "ws://127.0.0.1:18082/stream"),
            ("wss://example.test/ws", "wss://example.test/ws/stream"),
            ("example.test", "wss://example.test/stream"),
        ] {
            assert_eq!(stream_url(base), url, "{base}");
        }
    }

    #[tokio::test]
    async fn app_ping_replies_pong_without_refreshing_application_data() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            loop {
                if ws.send(Message::Text(r#"{"type":"ping"}"#.into())).await.is_err() { return; }
                loop {
                    let msg = timeout(Duration::from_millis(100), ws.next()).await.unwrap();
                    match msg {
                        Some(Ok(Message::Text(raw))) if raw == r#"{"type":"pong"}"# => break,
                        Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await.unwrap(),
                        Some(Ok(_)) => {}
                        _ => return,
                    }
                }
                sleep(Duration::from_millis(40)).await;
            }
        });

        let mut opts = opts(url);
        opts.data_timeout = Some(0.12);
        opts.frame_timeout = 2.0;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut on_message = move |_frame: &LighterFrame<'_>| {
            calls_for_cb.fetch_add(1, Ordering::Relaxed);
        };

        timeout(Duration::from_millis(500), session(&opts, None, &mut on_message))
            .await
            .unwrap()
            .unwrap();
        server.abort();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn quiet_control_frames_keep_private_stream_alive_when_data_timeout_disabled() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            loop {
                ws.send(Message::Pong(Vec::new())).await.unwrap();
                sleep(Duration::from_millis(40)).await;
            }
        });

        let mut opts = opts(url);
        opts.data_timeout = None;
        opts.frame_timeout = 0.15;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut on_message = move |_frame: &LighterFrame<'_>| {
            calls_for_cb.fetch_add(1, Ordering::Relaxed);
        };

        let mut fut = Box::pin(session(&opts, None, &mut on_message));
        assert!(
            timeout(Duration::from_millis(350), &mut fut).await.is_err(),
            "quiet control frames should keep the private stream session alive"
        );
        drop(fut);
        server.abort();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn silent_socket_trips_frame_timeout_even_when_data_timeout_disabled() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            std::future::pending::<()>().await;
        });

        let mut opts = opts(url);
        opts.data_timeout = None;
        opts.frame_timeout = 0.16;
        let mut on_message = |_frame: &LighterFrame<'_>| {};

        timeout(Duration::from_millis(500), session(&opts, None, &mut on_message))
            .await
            .unwrap()
            .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn application_message_refreshes_data_timeout_and_invokes_callback() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            for n in 0..4 {
                ws.send(Message::Text(format!(r#"{{"type":"update","n":{n}}}"#)))
                    .await
                    .unwrap();
                sleep(Duration::from_millis(60)).await;
            }
            sleep(Duration::from_millis(200)).await;
        });

        let mut opts = opts(url);
        opts.data_timeout = Some(0.12);
        opts.frame_timeout = 2.0;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut on_message = move |_frame: &LighterFrame<'_>| {
            calls_for_cb.fetch_add(1, Ordering::Relaxed);
        };

        timeout(Duration::from_secs(2), session(&opts, None, &mut on_message))
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn subscription_message_includes_channel_auth_when_configured() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let Message::Text(raw) = ws.next().await.unwrap().unwrap() else {
                panic!("expected text subscribe frame");
            };
            let v: Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(v["type"], "subscribe");
            assert_eq!(v["channel"], "test/channel");
            assert_eq!(v["auth"], "secret-token");
            ws.close(None).await.unwrap();
        });
        let mut opts = opts(url);
        opts.channel_auths.insert("test/channel".to_string(), "secret-token".to_string());
        opts.frame_timeout = 1.0;
        let mut on_message = |_frame: &LighterFrame<'_>| {};
        timeout(Duration::from_secs(2), session(&opts, None, &mut on_message))
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn frames_without_a_type_tag_are_still_delivered() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(r#"{"n":1}"#.into())).await.unwrap();
            sleep(Duration::from_millis(200)).await;
        });
        let mut opts = opts(url);
        opts.data_timeout = Some(0.12);
        opts.frame_timeout = 2.0;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut on_message = move |frame: &LighterFrame<'_>| {
            assert_eq!(frame.raw, r#"{"n":1}"#);
            calls_for_cb.fetch_add(1, Ordering::Relaxed);
        };
        timeout(Duration::from_secs(2), session(&opts, None, &mut on_message))
            .await
            .unwrap()
            .unwrap();
        server.abort();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn reconnect_notify_fires_immediately_on_a_quiet_stream() {
        // A request during a quiet stretch (no frames, no ping tick) ends the session at once.
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            sleep(Duration::from_secs(5)).await;
        });
        let mut opts = opts(url);
        opts.data_timeout = None;
        opts.frame_timeout = 30.0;
        opts.ping_interval = Duration::from_secs(20);
        let reconnect = Arc::new(Notify::new());
        let rc = reconnect.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            rc.notify_one();
        });
        let mut on_message = |_frame: &LighterFrame<'_>| {};
        timeout(Duration::from_millis(500), session(&opts, Some(&reconnect), &mut on_message))
            .await
            .expect("session must return promptly on reconnect notify")
            .unwrap();
        server.abort();
    }
}

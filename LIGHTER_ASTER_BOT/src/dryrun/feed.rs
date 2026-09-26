//! The upstream feed. The simulator keeps its own public connections to the real venues; each
//! frame is applied to the matching core at its engine time + D, and re-served to every bot
//! connection at its publish time + D + that connection's feed latency, with each exchange
//! timestamp moved +D. The bot so sees Tokyo-fresh data by its own, unchanged clocks.
//!
//! The venues' own field names are parsed here (Aster `E`/`T`, Lighter `timestamp` in ms and
//! `last_updated_at` in µs); everything downstream works in µs.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, Notify};
use tokio_tungstenite::tungstenite::Message;

use super::book::{BookUpdate, Level};
use super::clock::{wall_us, Latency, Rng};
use super::matching::{FeedEvent, Venue};
use crate::decimal::parse_dec;
use crate::lighter::local_book::LocalBook;
use crate::lighter::messages::{classify_book_update, BookUpdateContiguity, OrderBookMsgRef};
use crate::lighter::ws::{subscribe_loop, LighterFrame, SubscribeOptions};
use crate::types::Side;

/// A Lighter order-book message's place in its sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seq {
    pub snapshot: bool,
    pub begin_nonce: Option<i64>,
    pub nonce: Option<i64>,
    pub offset: Option<u64>,
}

/// One upstream frame, parsed once and shared by every bot connection.
#[derive(Debug, Clone)]
pub struct Frame {
    pub venue: Venue,
    /// The core's market key: the Aster symbol (`HYPEUSDT`) or the Lighter market index (`24`).
    pub market: String,
    /// Aster stream (`hypeusdt@depth20@100ms`) or Lighter channel (`order_book/24`).
    pub stream: String,
    /// When the venue's engine made the change (µs, unshifted).
    pub engine_us: i64,
    /// When the venue published it (µs, unshifted).
    pub publish_us: i64,
    pub event: FeedEvent,
    /// What the bot receives, timestamps already moved +D: the stream's `data` (Aster) or the
    /// whole message (Lighter).
    pub text: Arc<str>,
    pub seq: Option<Seq>,
}

/// What the upstream hands the matching core.
#[derive(Debug, Clone, PartialEq)]
pub enum Input {
    Frame { venue: Venue, market: String, exch_us: i64, event: FeedEvent },
    /// The rate a settlement at `exch_us` charges, as a fraction (positive: longs pay).
    Funding { venue: Venue, market: String, exch_us: i64, rate: Decimal },
}

fn dec(v: &Value, key: &str) -> Result<Decimal> {
    parse_dec(v.get(key).and_then(Value::as_str).with_context(|| format!("field {key} missing"))?)
}

fn levels(rows: &Value) -> Result<Vec<Level>> {
    let rows = rows.as_array().context("levels are not an array")?;
    rows.iter()
        .map(|row| match row {
            Value::Array(pair) if pair.len() == 2 => Ok((
                parse_dec(pair[0].as_str().context("price")?)?,
                parse_dec(pair[1].as_str().context("size")?)?,
            )),
            _ => Ok((dec(row, "price")?, dec(row, "size")?)),
        })
        .collect()
}

/// Adds `by` to each integer field of `v` named in `keys` that is present.
fn shift_fields(v: &mut Value, keys: &[&str], by: i64) {
    for key in keys {
        if let Some(field) = v.get_mut(*key) {
            if let Some(t) = field.as_i64() {
                *field = json!(t + by);
            }
        }
    }
}

/// Parses one frame of the upstream Aster combined stream (`None` for acks and other streams).
pub fn aster_frame(text: &str, shift_us: i64) -> Result<Option<Frame>> {
    let mut msg: Value = serde_json::from_str(text).context("Aster frame is not JSON")?;
    let Some(stream) = msg.get("stream").and_then(Value::as_str).map(str::to_string) else {
        return Ok(None);
    };
    let data = msg.get_mut("data").context("Aster frame without data")?;
    let engine_ms = data.get("T").and_then(Value::as_i64).context("Aster frame without T")?;
    let publish_ms = data.get("E").and_then(Value::as_i64).filter(|&e| e > 0).unwrap_or(engine_ms);
    let market = data.get("s").and_then(Value::as_str).context("Aster frame without s")?.to_string();
    let event = if stream.ends_with("@aggTrade") {
        // `m`: the buyer is the maker, so the seller took.
        let taker = if data.get("m").and_then(Value::as_bool) == Some(true) { Side::Sell } else { Side::Buy };
        FeedEvent::Trade { price: dec(data, "p")?, qty: dec(data, "q")?, taker }
    } else if stream.ends_with("@bookTicker") {
        FeedEvent::Book(BookUpdate::Top { bid: (dec(data, "b")?, dec(data, "B")?), ask: (dec(data, "a")?, dec(data, "A")?) })
    } else if stream.contains("@depth") {
        FeedEvent::Book(BookUpdate::Replace { bids: levels(&data["b"])?, asks: levels(&data["a"])? })
    } else {
        return Ok(None);
    };
    shift_fields(data, &["E", "T"], shift_us / 1_000);
    Ok(Some(Frame {
        venue: Venue::Aster,
        market,
        stream,
        engine_us: engine_ms * 1_000,
        publish_us: publish_ms * 1_000,
        event,
        text: data.to_string().into(),
        seq: None,
    }))
}

/// Parses one upstream Lighter message (`None` unless it is an `order_book` snapshot or update).
pub fn lighter_frame(text: &str, shift_us: i64) -> Result<Option<Frame>> {
    let parsed: OrderBookMsgRef<'_> = match serde_json::from_str(text) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };
    if !parsed.msg_type.ends_with("order_book") {
        return Ok(None);
    }
    let mut msg: Value = serde_json::from_str(text)?;
    let channel = msg.get("channel").and_then(Value::as_str).context("Lighter order book without a channel")?;
    let market = channel.rsplit([':', '/']).next().unwrap_or_default().to_string();
    let engine_us = parsed.last_updated_at.or(parsed.order_book.last_updated_at).filter(|&t| t > 0);
    let publish_us = parsed.timestamp.filter(|&t| t > 0).map(|t| t * 1_000);
    let (engine_us, publish_us) = match (engine_us, publish_us) {
        (Some(engine), Some(publish)) => (engine, publish),
        (Some(one), None) | (None, Some(one)) => (one, one),
        (None, None) => anyhow::bail!("Lighter order book without a timestamp"),
    };
    let book = &msg["order_book"];
    let (bids, asks) = (levels(&book["bids"])?, levels(&book["asks"])?);
    let snapshot = parsed.is_snapshot();
    let event = FeedEvent::Book(if snapshot { BookUpdate::Replace { bids, asks } } else { BookUpdate::Delta { bids, asks } });
    let seq = Seq {
        snapshot,
        begin_nonce: parsed.order_book.begin_nonce,
        nonce: parsed.order_book.nonce,
        offset: parsed.effective_offset(),
    };
    shift_fields(&mut msg, &["timestamp"], shift_us / 1_000);
    shift_fields(&mut msg, &["last_updated_at"], shift_us);
    shift_fields(&mut msg["order_book"], &["last_updated_at"], shift_us);
    Ok(Some(Frame {
        venue: Venue::Lighter,
        market: market.clone(),
        stream: format!("order_book/{market}"),
        engine_us,
        publish_us,
        event,
        text: msg.to_string().into(),
        seq: Some(seq),
    }))
}

/// Aster `premiumIndex`: the rate its next settlement will charge, as currently estimated.
pub fn aster_funding(text: &str) -> Result<(String, i64, Decimal)> {
    let v: Value = serde_json::from_str(text).context("premiumIndex is not JSON")?;
    let symbol = v.get("symbol").and_then(Value::as_str).context("premiumIndex without symbol")?;
    let at_ms = v.get("nextFundingTime").and_then(Value::as_i64).filter(|&t| t > 0).context("premiumIndex without nextFundingTime")?;
    Ok((symbol.to_string(), at_ms * 1_000, dec(&v, "lastFundingRate")?))
}

/// Lighter `market_stats`: the last settlement and its signed rate, which the venue quotes in
/// percent per hour (its funding history agrees: the same settlement shows as that rate with
/// the paying side).
pub fn lighter_funding(text: &str) -> Option<(String, i64, Decimal)> {
    let msg: Value = serde_json::from_str(text).ok()?;
    let stats = msg.get("market_stats")?;
    let market = stats.get("market_id")?.as_i64()?.to_string();
    let at_ms = stats.get("funding_timestamp")?.as_i64().filter(|&t| t > 0)?;
    let rate = parse_dec(stats.get("funding_rate")?.as_str()?).ok()? / Decimal::ONE_HUNDRED;
    Some((market, at_ms * 1_000, rate))
}

/// A Lighter order-book sequence, followed exactly as the bot follows its own.
#[derive(Debug, Clone, Default)]
pub struct Sequence {
    nonce: Option<i64>,
    offset: Option<u64>,
    started: bool,
}

impl Sequence {
    /// Where `seq` stands: a snapshot restarts the sequence; `Gap` means resubscribe.
    pub fn follow(&mut self, seq: &Seq) -> BookUpdateContiguity {
        if seq.snapshot {
            *self = Self { nonce: seq.nonce, offset: seq.offset, started: true };
            return BookUpdateContiguity::Apply;
        }
        if !self.started {
            return BookUpdateContiguity::Gap;
        }
        let verdict = classify_book_update(seq.begin_nonce, seq.nonce, seq.offset, self.nonce, self.offset);
        if verdict == BookUpdateContiguity::Apply {
            self.nonce = seq.nonce.or(self.nonce);
            self.offset = seq.offset.or(self.offset);
        }
        verdict
    }
}

/// A Lighter market's book as the venue has published it up to the release frontier. A bot
/// connection subscribing now gets it as its snapshot, positioned so the deltas that follow
/// continue its sequence.
#[derive(Debug, Clone, Default)]
pub struct ServedBook {
    book: LocalBook,
    sequence: Sequence,
    /// The last upstream snapshot message: the shape a synthesized one copies.
    template: Option<Value>,
    last_updated_us: Option<i64>,
}

impl ServedBook {
    /// Applies a released frame (frames arrive in order: the upstream already dropped stale ones).
    pub fn apply(&mut self, frame: &Frame) {
        let (Some(seq), FeedEvent::Book(update)) = (&frame.seq, &frame.event) else { return };
        self.sequence.follow(seq);
        match update {
            BookUpdate::Replace { bids, asks } => {
                self.book.apply_snapshot(bids.clone(), asks.clone());
                self.template = serde_json::from_str(&frame.text).ok();
            }
            BookUpdate::Delta { bids, asks } => self.book.apply_delta(bids, asks),
            BookUpdate::Top { .. } => {}
        }
        self.last_updated_us = Some(frame.engine_us);
    }

    /// The snapshot for a subscription made at `now_ms` on the shifted clock; `shift_us` moves
    /// the engine time as every served frame's. `None` until a snapshot has been released.
    pub fn snapshot(&self, now_ms: i64, shift_us: i64) -> Option<String> {
        let mut msg = self.template.clone()?;
        let side = |levels: &mut dyn Iterator<Item = Level>| -> Value {
            levels.map(|(price, size)| json!({"price": price.to_string(), "size": size.to_string()})).collect()
        };
        let book = &mut msg["order_book"];
        book["bids"] = side(&mut self.book.bids.top_descending(usize::MAX));
        book["asks"] = side(&mut self.book.asks.top_ascending(usize::MAX));
        // `begin_nonce` stays as the venue sends it on a snapshot (0).
        for (key, value) in [("nonce", self.sequence.nonce.map(Value::from)), ("offset", self.sequence.offset.map(Value::from))] {
            if let (Some(value), true) = (value, book.get(key).is_some()) {
                book[key] = value;
            }
        }
        if msg.get("offset").is_some() {
            msg["offset"] = self.sequence.offset.map_or(Value::Null, Value::from);
        }
        msg["timestamp"] = json!(now_ms);
        if let Some(engine) = self.last_updated_us.map(|t| t + shift_us) {
            if msg.get("last_updated_at").is_some() {
                msg["last_updated_at"] = json!(engine);
            }
            if msg["order_book"].get("last_updated_at").is_some() {
                msg["order_book"]["last_updated_at"] = json!(engine);
            }
        }
        Some(msg.to_string())
    }
}

/// One bot connection's frames in flight. Each is released at its shifted publish time plus a
/// latency draw, and never before the frame ahead of it: one connection delivers in order.
#[derive(Debug)]
pub struct Outbox {
    rng: Rng,
    latency: Latency,
    last_us: i64,
    queue: VecDeque<(i64, Arc<str>)>,
}

impl Outbox {
    pub fn new(seed: u64, latency: Latency) -> Self {
        Self { rng: Rng::new(seed), latency, last_us: i64::MIN, queue: VecDeque::new() }
    }

    /// Queues `text`, published at `at_us` on the shifted clock.
    pub fn push(&mut self, at_us: i64, text: Arc<str>) {
        let release = (at_us + self.latency.sample_us(&mut self.rng)).max(self.last_us);
        self.last_us = release;
        self.queue.push_back((release, text));
    }

    pub fn next_due(&self) -> Option<i64> {
        self.queue.front().map(|(at, _)| *at)
    }

    pub fn pop_due(&mut self, now_us: i64) -> Option<Arc<str>> {
        if self.next_due()? <= now_us {
            self.queue.pop_front().map(|(_, text)| text)
        } else {
            None
        }
    }
}

/// What the hub passes to the connections that follow it.
#[derive(Debug, Clone)]
enum Relay {
    /// A frame, numbered in arrival order.
    Frame(u64, Arc<Frame>),
    /// The upstream broke.
    Gap,
}

/// Frames a follower may fall behind by before it is dropped (a stuck connection).
const FANOUT: usize = 4_096;

/// One venue's public frames between their upstream arrival and their publication on the
/// shifted clock. Bot connections follow it through a [`Follower`]; Lighter books are also
/// kept as published, so a subscriber joining late gets a snapshot the deltas continue.
pub struct Hub {
    shift_us: i64,
    /// The streams the upstream carries; a bot asking for another gets nothing.
    streams: BTreeSet<String>,
    last_id: u64,
    pending: VecDeque<(u64, Arc<Frame>)>,
    books: BTreeMap<String, ServedBook>,
    tx: broadcast::Sender<Relay>,
}

impl Hub {
    pub fn new(shift_us: i64, streams: impl IntoIterator<Item = String>) -> Self {
        Self {
            shift_us,
            streams: streams.into_iter().collect(),
            last_id: 0,
            pending: VecDeque::new(),
            books: BTreeMap::new(),
            tx: broadcast::channel(FANOUT).0,
        }
    }

    pub fn carries(&self, stream: &str) -> bool {
        self.streams.contains(stream)
    }

    /// Publishes what is due by `now_us` into the served books.
    fn advance(&mut self, now_us: i64) {
        while self.pending.front().is_some_and(|(_, f)| f.publish_us + self.shift_us <= now_us) {
            let (_, frame) = self.pending.pop_front().expect("front exists");
            if frame.seq.is_some() {
                self.books.entry(frame.stream.clone()).or_default().apply(&frame);
            }
        }
    }

    /// An upstream frame arrived at `now_us`.
    pub fn arrive(&mut self, now_us: i64, frame: Frame) {
        self.last_id += 1;
        let frame = Arc::new(frame);
        self.pending.push_back((self.last_id, frame.clone()));
        self.advance(now_us);
        let _ = self.tx.send(Relay::Frame(self.last_id, frame));
    }

    /// The upstream broke: the following connections close once what is in flight reaches them
    /// (the core applies it too), as a real disconnect forces the bot to resubscribe. A later
    /// subscriber starts after the gap.
    pub fn gap(&mut self) {
        self.pending.clear();
        self.books.clear();
        let _ = self.tx.send(Relay::Gap);
    }

    /// What a connection subscribing to `stream` at `now_us` gets first (the book as published,
    /// for a Lighter book, then the stream's frames still in flight, each with its publish
    /// time), and the id of the last frame that covers.
    fn subscribe(&mut self, stream: &str, now_us: i64) -> (Vec<(i64, Arc<str>)>, u64) {
        self.advance(now_us);
        let snapshot = self.books.get(stream).and_then(|book| book.snapshot(now_us / 1_000, self.shift_us));
        let first = snapshot.map(|text| (now_us, Arc::from(text))).into_iter()
            .chain(self.pending.iter()
                .filter(|(_, f)| f.stream == stream)
                .map(|(_, f)| (f.publish_us + self.shift_us, f.text.clone())))
            .collect();
        (first, self.last_id)
    }
}

/// One bot connection's public streams: its subscriptions and its frames in flight.
pub struct Follower {
    hub: Arc<Mutex<Hub>>,
    shift_us: i64,
    /// Made at the first subscription, so an idle connection holds no frames.
    rx: Option<broadcast::Receiver<Relay>>,
    /// Subscribed streams, each with the last frame id its subscription already covered.
    streams: HashMap<String, u64>,
    outbox: Outbox,
    /// Aster combined streams wrap each frame as `{"stream","data"}`.
    combined: bool,
    /// The upstream broke: close once the outbox is empty.
    closing: bool,
}

impl Follower {
    pub fn new(hub: Arc<Mutex<Hub>>, latency: Latency, seed: u64, combined: bool) -> Self {
        let shift_us = hub.lock().expect("feed hub poisoned").shift_us;
        Self { hub, shift_us, rx: None, streams: HashMap::new(), outbox: Outbox::new(seed, latency), combined, closing: false }
    }

    /// Follows `stream` from now on; false if the upstream does not carry it.
    pub fn subscribe(&mut self, stream: &str) -> bool {
        let mut hub = self.hub.lock().expect("feed hub poisoned");
        if !hub.carries(stream) {
            return false;
        }
        if self.rx.is_none() {
            self.rx = Some(hub.tx.subscribe());
        }
        let (first, cut) = hub.subscribe(stream, wall_us());
        drop(hub);
        for (at, text) in first {
            self.push(stream, at, text);
        }
        self.streams.insert(stream.to_string(), cut);
        true
    }

    pub fn unsubscribe(&mut self, stream: &str) {
        self.streams.remove(stream);
    }

    fn push(&mut self, stream: &str, at_us: i64, text: Arc<str>) {
        let text = if self.combined { Arc::from(format!(r#"{{"stream":"{stream}","data":{text}}}"#)) } else { text };
        self.outbox.push(at_us, text);
    }

    /// The next frame due on this connection; `None` once it must close (the upstream broke
    /// and its last frames are out, or the connection fell too far behind).
    pub async fn next(&mut self) -> Option<Arc<str>> {
        loop {
            let now = wall_us();
            if let Some(text) = self.outbox.pop_due(now) {
                return Some(text);
            }
            if self.closing && self.outbox.next_due().is_none() {
                return None;
            }
            let wait = self.outbox.next_due().map(|due| Duration::from_micros((due - now) as u64));
            let relay = match (&mut self.rx, wait) {
                (Some(rx), Some(wait)) => tokio::select! {
                    relay = rx.recv() => Some(relay),
                    _ = tokio::time::sleep(wait) => None,
                },
                (Some(rx), None) => Some(rx.recv().await),
                (None, Some(wait)) => {
                    tokio::time::sleep(wait).await;
                    None
                }
                (None, None) => std::future::pending().await,
            };
            match relay {
                None => {}
                Some(Ok(Relay::Frame(id, frame))) => {
                    if self.streams.get(&frame.stream).is_some_and(|&cut| id > cut) {
                        self.push(&frame.stream, frame.publish_us + self.shift_us, frame.text.clone());
                    }
                }
                Some(Ok(Relay::Gap)) => (self.rx, self.closing) = (None, true),
                Some(Err(_)) => return None,
            }
        }
    }
}

/// Hands one upstream frame to the core and to the bot connections.
pub(super) fn forward(frame: Frame, hub: &Mutex<Hub>, inputs: &mpsc::UnboundedSender<Input>) {
    let input = Input::Frame { venue: frame.venue, market: frame.market.clone(), exch_us: frame.engine_us, event: frame.event.clone() };
    let _ = inputs.send(input);
    hub.lock().expect("feed hub poisoned").arrive(wall_us(), frame);
}

/// The upstream broke after the frame stamped `last_us`: the core stops matching `markets`
/// until their next snapshot, and the bot's streams close.
fn gap(venue: Venue, markets: &[String], last_us: i64, hub: &Mutex<Hub>, inputs: &mpsc::UnboundedSender<Input>) {
    for market in markets {
        let _ = inputs.send(Input::Frame { venue, market: market.clone(), exch_us: last_us + 1, event: FeedEvent::Gap });
    }
    hub.lock().expect("feed hub poisoned").gap();
}

/// The Aster streams the core matches against, for one symbol.
pub fn aster_streams(symbol: &str) -> [String; 3] {
    let s = symbol.to_lowercase();
    [format!("{s}@depth20@100ms"), format!("{s}@bookTicker"), format!("{s}@aggTrade")]
}

/// The Aster combined-stream URL of `symbols`' streams under `ws_root`.
pub fn aster_url(ws_root: &str, symbols: &[String]) -> String {
    let streams: Vec<String> = symbols.iter().flat_map(|s| aster_streams(s)).collect();
    format!("{ws_root}/stream?streams={}", streams.join("/"))
}

/// A silent upstream is presumed dead after this (depth20 only pushes on change).
const UPSTREAM_IDLE: Duration = Duration::from_secs(60);
/// The longest wait between reconnects to a public feed: an attempt is cheap, and every second
/// waited after the network is back is market data lost.
pub const UPSTREAM_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// What an upstream connection delivers: a text frame, or the end of its session.
pub enum Wire<'a> {
    Text(&'a str),
    Closed,
}

/// Follows the Aster combined stream at `url`, reconnecting forever; `on` gets every text
/// frame and the end of every session. Shared by the simulator and the tape recorder.
pub async fn aster_stream(url: String, label: &str, mut on: impl FnMut(Wire<'_>)) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let started = Instant::now();
        if let Err(e) = aster_session(&url, &mut on).await {
            tracing::warn!("{label}: {e:#}");
        }
        on(Wire::Closed);
        if started.elapsed() >= Duration::from_secs(60) {
            backoff = Duration::from_secs(1);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(UPSTREAM_BACKOFF_MAX);
    }
}

async fn aster_session(url: &str, on: &mut impl FnMut(Wire<'_>)) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(url).await.context("connect")?;
    let (mut write, mut read) = ws.split();
    loop {
        let Some(msg) = tokio::time::timeout(UPSTREAM_IDLE, read.next()).await.context("silent upstream")? else {
            return Ok(());
        };
        match msg? {
            Message::Text(text) => on(Wire::Text(&text)),
            Message::Ping(p) => crate::connectors::send_guarded(&mut write, Message::Pong(p)).await?,
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

/// Follows the real Aster streams of `symbols` from `ws_root` into the core and the bot's streams.
pub async fn aster_upstream(ws_root: String, symbols: Vec<String>, shift_us: i64, hub: Arc<Mutex<Hub>>, inputs: mpsc::UnboundedSender<Input>) {
    let markets: Vec<String> = symbols.iter().map(|s| s.to_uppercase()).collect();
    let mut last_us = None;
    aster_stream(aster_url(&ws_root, &symbols), "dry-run upstream Aster", |wire| match wire {
        Wire::Text(text) => match aster_frame(text, shift_us) {
            Ok(Some(frame)) => {
                // The latest, not the last: an aggTrade's stamp runs ~190 ms behind the books'.
                last_us = last_us.max(Some(frame.engine_us));
                forward(frame, &hub, &inputs);
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("dry-run upstream Aster: unreadable frame: {e:#}"),
        },
        Wire::Closed => {
            if let Some(last_us) = last_us.take() {
                gap(Venue::Aster, &markets, last_us, &hub, &inputs);
            }
        }
    })
    .await
}

/// Follows the real Lighter order books of `markets` (indices) and their funding, reconnecting
/// forever. A sequence gap drops the connection for a fresh snapshot, as the bot's own client does.
pub async fn lighter_upstream(url: String, markets: Vec<String>, shift_us: i64, hub: Arc<Mutex<Hub>>, inputs: mpsc::UnboundedSender<Input>) {
    #[derive(Default)]
    struct Session {
        sequences: HashMap<String, Sequence>,
        last_us: Option<i64>,
        settled: HashMap<String, i64>,
    }
    let channels = markets.iter().flat_map(|m| [format!("order_book/{m}"), format!("market_stats/{m}")]).collect();
    let mut opts = SubscribeOptions::new(&url, "dry-run upstream Lighter", channels);
    opts.reconnect_base = 0.5;
    opts.reconnect_max = UPSTREAM_BACKOFF_MAX.as_secs_f64();
    let reconnect = Arc::new(Notify::new());
    let session = Arc::new(Mutex::new(Session::default()));
    let (on_gap, closing) = (reconnect.clone(), session.clone());
    let (hub_on_close, inputs_on_close) = (hub.clone(), inputs.clone());
    subscribe_loop(
        opts,
        Some(reconnect),
        move |msg: &LighterFrame<'_>| {
            let mut st = session.lock().expect("upstream state poisoned");
            if msg.msg_type.as_deref().is_some_and(|t| t.ends_with("market_stats")) {
                if let Some((market, exch_us, rate)) = lighter_funding(msg.raw) {
                    if st.settled.insert(market.clone(), exch_us) != Some(exch_us) {
                        let _ = inputs.send(Input::Funding { venue: Venue::Lighter, market, exch_us, rate });
                    }
                }
                return;
            }
            let frame = match lighter_frame(msg.raw, shift_us) {
                Ok(Some(frame)) => frame,
                Ok(None) => return,
                Err(e) => {
                    tracing::warn!("dry-run upstream Lighter: unreadable frame: {e:#}");
                    return;
                }
            };
            let seq = frame.seq.expect("Lighter frames carry their sequence");
            match st.sequences.entry(frame.market.clone()).or_default().follow(&seq) {
                BookUpdateContiguity::Apply => {
                    st.last_us = Some(frame.engine_us);
                    forward(frame, &hub, &inputs);
                }
                BookUpdateContiguity::SkipStale => {}
                BookUpdateContiguity::Gap => {
                    tracing::warn!("dry-run upstream Lighter: sequence gap on {}; resubscribing", frame.stream);
                    on_gap.notify_one();
                }
            }
        },
        move || {
            let mut st = closing.lock().expect("upstream state poisoned");
            if let Some(last_us) = st.last_us.take() {
                gap(Venue::Lighter, &markets, last_us, &hub_on_close, &inputs_on_close);
            }
            st.sequences.clear();
        },
    )
    .await;
}

/// How often the Aster funding estimate is polled: the last poll before a settlement sets its rate.
const FUNDING_POLL: Duration = Duration::from_secs(60);

/// Polls each Aster symbol's `premiumIndex` from `rest_base`, forever; `on_body` gets each
/// response. Shared by the simulator and the tape recorder.
pub async fn aster_premium_poll(rest_base: String, symbols: Vec<String>, label: &str, mut on_body: impl FnMut(&str)) {
    let client = reqwest::Client::new();
    let mut tick = tokio::time::interval(FUNDING_POLL);
    loop {
        tick.tick().await;
        for symbol in &symbols {
            let url = format!("{rest_base}/fapi/v1/premiumIndex?symbol={symbol}");
            let body = async { client.get(&url).timeout(Duration::from_secs(10)).send().await?.error_for_status()?.text().await };
            match body.await {
                Ok(body) => on_body(&body),
                Err(e) => tracing::warn!("{label} for {symbol}: {e:#}"),
            }
        }
    }
}

/// Hands the core each Aster symbol's next settlement and its estimated rate, forever.
pub async fn aster_funding_poll(rest_base: String, symbols: Vec<String>, inputs: mpsc::UnboundedSender<Input>) {
    aster_premium_poll(rest_base, symbols, "dry-run funding poll", |body| match aster_funding(body) {
        Ok((market, exch_us, rate)) => {
            let _ = inputs.send(Input::Funding { venue: Venue::Aster, market, exch_us, rate });
        }
        Err(e) => tracing::warn!("dry-run funding poll: {e:#}"),
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use rust_decimal_macros::dec;

    const D: i64 = 500_000;

    // Frames as the venues sent them on 2026-09-24 (depth cut to two levels).
    const ASTER_DEPTH: &str = r#"{"stream":"hypeusdt@depth20@100ms","data":{"e":"depthUpdate","E":1790235498326,"T":1790235498300,"s":"HYPEUSDT","U":562456607908,"u":562456610292,"pu":562456607469,"b":[["94.67300","31.00"],["94.67200","135.67"]],"a":[["94.69300","227.81"],["94.69800","116.31"]]}}"#;
    const ASTER_TICKER: &str = r#"{"stream":"hypeusdt@bookTicker","data":{"e":"bookTicker","u":562456609360,"s":"HYPEUSDT","b":"94.67200","B":"136.45","a":"94.69800","A":"116.31","T":1790235498250,"E":1790235498273}}"#;
    const ASTER_TRADE: &str = r#"{"stream":"hypeusdt@aggTrade","data":{"e":"aggTrade","E":1790235498368,"a":18581250,"s":"HYPEUSDT","p":"94.68500","q":"331.46","f":22198912,"l":22198912,"T":1790235498150,"m":true}}"#;
    const ASTER_PREMIUM: &str = r#"{"symbol":"HYPEUSDT","markPrice":"94.74841428","indexPrice":"94.77735714","estimatedSettlePrice":"92.82765244","lastFundingRate":"0.00003154","interestRate":"0.00010000","nextFundingTime":1790236800000,"time":1790235499000}"#;
    const LIGHTER_STATS: &str = r#"{"channel":"market_stats:24","market_stats":{"symbol":"HYPE","market_id":24,"index_price":"94.4291","mark_price":"94.3802","current_funding_rate":"0.0006","funding_rate":"-0.0006","funding_timestamp":1790233200002},"timestamp":1790235633602,"type":"update/market_stats"}"#;

    #[test]
    fn aster_frames_carry_engine_and_publish_times_and_reach_the_bot_shifted() {
        let f = aster_frame(ASTER_DEPTH, D).unwrap().unwrap();
        assert_eq!((f.market.as_str(), f.engine_us, f.publish_us), ("HYPEUSDT", 1_790_235_498_300_000, 1_790_235_498_326_000));
        let bids = vec![(dec!(94.673), dec!(31)), (dec!(94.672), dec!(135.67))];
        let asks = vec![(dec!(94.693), dec!(227.81)), (dec!(94.698), dec!(116.31))];
        assert_eq!(f.event, FeedEvent::Book(BookUpdate::Replace { bids, asks }));
        let served: Value = serde_json::from_str(&f.text).unwrap();
        assert_eq!((served["E"].as_i64(), served["T"].as_i64()), (Some(1_790_235_498_826), Some(1_790_235_498_800)));
        assert_eq!(served["b"][0][0].as_str(), Some("94.67300"), "prices go out exactly as the venue wrote them");

        let f = aster_frame(ASTER_TICKER, D).unwrap().unwrap();
        assert_eq!(f.event, FeedEvent::Book(BookUpdate::Top { bid: (dec!(94.672), dec!(136.45)), ask: (dec!(94.698), dec!(116.31)) }));

        // A print ~220 ms older than its publication: the core applies it at its trade time.
        let f = aster_frame(ASTER_TRADE, D).unwrap().unwrap();
        assert_eq!(f.event, FeedEvent::Trade { price: dec!(94.685), qty: dec!(331.46), taker: Side::Sell });
        assert_eq!((f.engine_us, f.publish_us), (1_790_235_498_150_000, 1_790_235_498_368_000));
        assert!(aster_frame(r#"{"result":null,"id":1}"#, D).unwrap().is_none());

        assert_eq!(aster_funding(ASTER_PREMIUM).unwrap(), ("HYPEUSDT".into(), 1_790_236_800_000_000, dec!(0.00003154)));
        assert_eq!(lighter_funding(LIGHTER_STATS), Some(("24".into(), 1_790_233_200_002_000, dec!(-0.000006))));
    }

    /// A Lighter order-book message in the venue's shape (a snapshot's `begin_nonce` is 0).
    fn lighter(kind: &str, begin: i64, nonce: i64, publish_ms: i64, bids: &str, asks: &str) -> String {
        let offset = nonce * 10;
        let engine = publish_ms * 1_000 - 11_400;
        format!(
            r#"{{"channel":"order_book:24","last_updated_at":{engine},"offset":{offset},"order_book":{{"code":0,"asks":{asks},"bids":{bids},"offset":{offset},"nonce":{nonce},"last_updated_at":{engine},"begin_nonce":{begin}}},"timestamp":{publish_ms},"type":"{kind}/order_book"}}"#
        )
    }

    #[test]
    fn lighter_frames_are_parsed_shifted_and_checked_for_gaps() {
        let snap = lighter("subscribed", 0, 10, 5_000, r#"[{"price":"40.1","size":"2"}]"#, r#"[{"price":"40.3","size":"1"}]"#);
        let f = lighter_frame(&snap, D).unwrap().unwrap();
        assert_eq!((f.market.as_str(), f.stream.as_str(), f.engine_us, f.publish_us), ("24", "order_book/24", 4_988_600, 5_000_000));
        let served: Value = serde_json::from_str(&f.text).unwrap();
        assert_eq!((served["timestamp"].as_i64(), served["last_updated_at"].as_i64()), (Some(5_500), Some(5_488_600)));
        assert_eq!(served["order_book"]["last_updated_at"].as_i64(), Some(5_488_600));

        let mut seq = Sequence::default();
        let delta = |begin, nonce| lighter_frame(&lighter("update", begin, nonce, 5_050, "[]", "[]"), D).unwrap().unwrap().seq.unwrap();
        assert_eq!(seq.follow(&delta(10, 11)), BookUpdateContiguity::Gap, "a delta before any snapshot");
        assert_eq!(seq.follow(&f.seq.unwrap()), BookUpdateContiguity::Apply);
        assert_eq!(seq.follow(&delta(10, 11)), BookUpdateContiguity::Apply);
        assert_eq!(seq.follow(&delta(9, 10)), BookUpdateContiguity::SkipStale);
        assert_eq!(seq.follow(&delta(12, 13)), BookUpdateContiguity::Gap);
        assert!(lighter_frame(r#"{"type":"pong"}"#, D).unwrap().is_none());
        assert!(lighter_frame(LIGHTER_STATS, D).unwrap().is_none());
    }

    #[test]
    fn a_synthesized_snapshot_is_what_the_bot_accepts_and_continues_from() {
        let mut served = ServedBook::default();
        assert!(served.snapshot(9_000, D).is_none());
        let snap = lighter("subscribed", 0, 10, 5_000, r#"[{"price":"40.1","size":"2"}]"#, r#"[{"price":"40.3","size":"1"}]"#);
        let delta = lighter("update", 10, 11, 5_050, r#"[{"price":"40.2","size":"4"}]"#, r#"[{"price":"40.3","size":"0"}]"#);
        let next = lighter("update", 11, 12, 5_100, "[]", r#"[{"price":"40.4","size":"1"}]"#);
        for text in [&snap, &delta] {
            served.apply(&lighter_frame(text, D).unwrap().unwrap());
        }
        let synthesized = served.snapshot(9_000, D).unwrap();
        let msg: OrderBookMsgRef<'_> = serde_json::from_str(&synthesized).unwrap();
        assert!(msg.is_snapshot());
        assert_eq!((msg.timestamp, msg.order_book.begin_nonce, msg.order_book.nonce), (Some(9_000), Some(0), Some(11)));
        let bids: Vec<_> = msg.order_book.bids.iter().map(|l| (l.price.to_string(), l.size.to_string())).collect();
        assert_eq!(bids, [("40.2".into(), "4".into()), ("40.1".into(), "2".into())]);
        assert!(msg.order_book.asks.is_empty(), "the delta emptied the only ask");
        // The next upstream delta continues the served sequence, as the bot checks it.
        let next: OrderBookMsgRef<'_> = serde_json::from_str(&next).unwrap();
        assert_eq!(next.contiguity(msg.order_book.nonce, msg.effective_offset()), BookUpdateContiguity::Apply);
    }

    #[test]
    fn a_connection_releases_in_order_after_its_latency() {
        let mut out = Outbox::new(1, Latency::try_from([5.0, 30.0]).unwrap());
        for (i, at) in [1_000_000, 1_000_100, 1_000_200].into_iter().enumerate() {
            out.push(at, Arc::from(i.to_string()));
        }
        assert!(out.next_due().unwrap() > 1_000_000);
        assert!(out.pop_due(1_000_000).is_none());
        let mut seen = Vec::new();
        let mut last = i64::MIN;
        while let Some(due) = out.next_due() {
            assert!(due >= last);
            last = due;
            seen.push(out.pop_due(due).unwrap().to_string());
        }
        assert_eq!(seen, ["0", "1", "2"]);
    }

    /// Reads a served Lighter message: (type, nonce, begin_nonce, timestamp ms).
    fn served(text: &str) -> (String, i64, i64, i64) {
        let v: Value = serde_json::from_str(text).unwrap();
        let book = &v["order_book"];
        (v["type"].as_str().unwrap().into(), book["nonce"].as_i64().unwrap(), book["begin_nonce"].as_i64().unwrap(), v["timestamp"].as_i64().unwrap())
    }

    /// Frames published at `publish_ms` for a hub shifted by `shift_ms`, arriving now.
    fn at(shift_ms: i64, frames: &[(&str, i64, i64, i64)]) -> Vec<Frame> {
        frames.iter()
            .map(|&(kind, begin, nonce, publish_ms)| lighter_frame(&lighter(kind, begin, nonce, publish_ms, "[]", "[]"), shift_ms * 1_000).unwrap().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn a_late_subscriber_gets_the_book_as_published_then_the_frames_in_flight() {
        const SHIFT_MS: i64 = 60;
        let stream = "order_book/24";
        let hub = Arc::new(Mutex::new(Hub::new(SHIFT_MS * 1_000, [stream.to_string()])));
        let now_ms = wall_us() / 1_000;
        // Published 100 ms ago (already out on the shifted clock), 20 ms ago and now (in flight).
        let frames = at(SHIFT_MS, &[("subscribed", 0, 10, now_ms - 100), ("update", 10, 11, now_ms - 20), ("update", 11, 12, now_ms)]);
        let mut frames = frames.into_iter();
        for frame in frames.by_ref().take(2) {
            hub.lock().unwrap().arrive(wall_us(), frame);
        }
        let mut bot = Follower::new(hub.clone(), Latency::ZERO, 1, false);
        assert!(bot.subscribe(stream));
        assert!(!Follower::new(hub.clone(), Latency::ZERO, 2, false).subscribe("order_book/1"), "the upstream does not carry it");
        hub.lock().unwrap().arrive(wall_us(), frames.next().unwrap());

        let (kind, nonce, begin, ts) = served(&bot.next().await.unwrap());
        assert_eq!((kind.as_str(), nonce, begin), ("subscribed/order_book", 10, 0), "the book as published");
        assert!(ts >= now_ms && ts <= wall_us() / 1_000);
        let mut last = nonce;
        for publish_ms in [now_ms - 20, now_ms] {
            let (kind, nonce, begin, ts) = served(&bot.next().await.unwrap());
            assert_eq!((kind.as_str(), begin), ("update/order_book", last), "each delta continues the last");
            assert_eq!(ts, publish_ms + SHIFT_MS, "stamped on the shifted clock");
            assert!(wall_us() / 1_000 >= ts, "never released before its shifted publish time");
            last = nonce;
        }
        // A frame in flight when the upstream breaks still arrives (the core applies it too).
        hub.lock().unwrap().arrive(wall_us(), at(SHIFT_MS, &[("update", 12, 13, wall_us() / 1_000)]).pop().unwrap());
        hub.lock().unwrap().gap();
        assert_eq!(served(&bot.next().await.unwrap()).1, 13, "what was in flight arrives");
        assert!(bot.next().await.is_none(), "then the upstream gap closes the stream");
    }

    #[tokio::test]
    async fn combined_streams_wrap_each_frame_with_its_stream() {
        let streams = aster_streams("HYPEUSDT");
        let hub = Arc::new(Mutex::new(Hub::new(0, streams.clone())));
        let mut bot = Follower::new(hub.clone(), Latency::ZERO, 1, true);
        assert!(bot.subscribe(&streams[2]));
        for text in [ASTER_DEPTH, ASTER_TRADE] {
            hub.lock().unwrap().arrive(wall_us(), aster_frame(text, 0).unwrap().unwrap());
        }
        let got: Value = serde_json::from_str(&bot.next().await.unwrap()).unwrap();
        assert_eq!(got["stream"].as_str(), Some("hypeusdt@aggTrade"), "only the subscribed stream");
        assert_eq!(got["data"]["q"].as_str(), Some("331.46"));
    }

    /// A Lighter venue on loopback: plays `before` once the client subscribed, then `after` when
    /// told to.
    async fn fake_lighter(before: Vec<String>, after: Vec<String>, go: tokio::sync::oneshot::Receiver<()>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/stream", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 0..2 {
                ws.next().await;
            }
            for text in before {
                ws.send(Message::Text(text)).await.unwrap();
            }
            let _ = go.await;
            for text in after {
                ws.send(Message::Text(text)).await.unwrap();
            }
            while ws.next().await.is_some() {}
        });
        url
    }

    #[tokio::test]
    async fn the_upstream_feeds_core_and_bot_and_a_sequence_gap_closes_the_bot_stream() {
        const SHIFT_MS: i64 = 40;
        let now_ms = wall_us() / 1_000;
        let snap = lighter("subscribed", 0, 10, now_ms - 100, r#"[{"price":"40.1","size":"2"}]"#, r#"[{"price":"40.3","size":"1"}]"#);
        let delta = lighter("update", 10, 11, now_ms - 10, r#"[{"price":"40.2","size":"4"}]"#, "[]");
        let gapped = lighter("update", 12, 13, now_ms, "[]", "[]");
        let (go, wait) = tokio::sync::oneshot::channel();
        let url = fake_lighter(vec![snap, delta, LIGHTER_STATS.into(), LIGHTER_STATS.into()], vec![gapped], wait).await;
        let hub = Arc::new(Mutex::new(Hub::new(SHIFT_MS * 1_000, ["order_book/24".to_string()])));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(lighter_upstream(url, vec!["24".into()], SHIFT_MS * 1_000, hub.clone(), tx));

        let mut exch = Vec::new();
        while exch.len() < 2 {
            match rx.recv().await.unwrap() {
                Input::Frame { venue: Venue::Lighter, market, exch_us, event: FeedEvent::Book(_) } if market == "24" => exch.push(exch_us),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(exch, [(now_ms - 100) * 1_000 - 11_400, (now_ms - 10) * 1_000 - 11_400], "the core gets engine times");
        let funding = rx.recv().await.unwrap();
        assert_eq!(funding, Input::Funding { venue: Venue::Lighter, market: "24".into(), exch_us: 1_790_233_200_002_000, rate: dec!(-0.000006) });

        let mut bot = Follower::new(hub.clone(), Latency::ZERO, 1, false);
        assert!(bot.subscribe("order_book/24"));
        let (kind, nonce, ..) = served(&bot.next().await.unwrap());
        assert_eq!(kind, "subscribed/order_book");
        if nonce == 10 {
            assert_eq!(served(&bot.next().await.unwrap()).2, 10, "the delta in flight follows");
        }
        go.send(()).unwrap();
        assert!(bot.next().await.is_none(), "the gap closed the bot's stream");
        let closed = rx.recv().await.unwrap();
        assert_eq!(closed, Input::Frame { venue: Venue::Lighter, market: "24".into(), exch_us: exch[1] + 1, event: FeedEvent::Gap }, "the repeated funding was not re-sent");
        task.abort();
    }
}

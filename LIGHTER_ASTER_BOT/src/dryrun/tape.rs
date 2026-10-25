//! The market-data tape: one market's raw public feeds, recorded by `record` (on the Tokyo VPS since
//! 2026-10-10: VPS.md) and replayed by `replay` (a backtest: [`Replay`]). A file per UTC day and per
//! recorder start, `<dir>/<MARKET>/<YYYY-MM-DD>T<HHMMSS>Z.tape.zst` after its first line's arrival
//! (UTC), of tab-separated lines `<arrival µs since the epoch>\t<kind>\t<payload>`:
//!
//! | kind | payload |
//! |---|---|
//! | `A` | an Aster combined-stream frame, raw: `depth20@100ms` (20 levels), `bookTicker`, `aggTrade` |
//! | `A-` | the Aster connection ended, or a reconnect failed (empty); later frames follow a reconnect |
//! | `L` | a Lighter frame, raw: `order_book` (snapshot, then deltas: the whole book; a new day's file carries on the book of the file before), `trade`, `market_stats` (funding) |
//! | `L-` | the Lighter connection ended, or a reconnect failed (empty) |
//! | `H` | a Hyperliquid frame, raw: fast `l2Book` (5 levels, ~0.5 s), `bbo`, `trades`, `activeAssetCtx` (funding, mark), and the venue's acks and pongs |
//! | `HS` | a Hyperliquid frame of the slow `l2Book` connection (20 levels, ~5 s) |
//! | `H-`, `HS-` | that Hyperliquid connection ended, or a reconnect failed (empty) |
//! | `F` | an Aster `premiumIndex` response (funding), every minute |
//! | `X`, `O` | the Aster `exchangeInfo` and Lighter `orderBooks` responses (filters), hourly |
//! | `DA`, `DL` | the Aster `depth` (500 levels) and Lighter `orderBookOrders` (250 orders) responses, every minute: the books past the streams' 20 levels, which reach ~5 bps on Aster (on 2026-10-10/11, Aster's 100 levels reached 20-31 bps, 500 ~300 bps; Lighter's 100 orders 20-26 bps, 250 ~180-230 bps) |
//!
//! `replay` reads the Aster and Lighter kinds only (tapes before 2026-10-10 have no `H`).
//! Lines are in the order they arrived, which a reader keeps. Their times are the recording host's
//! wall clock, which clock corrections step back (2026-09-26, on this PC: 71 steps over 1 ms, most
//! of a few ms, the largest 0.37, 0.50 and 0.70 s), so a reader lets no time pass across a step.
//!
//! A file is concatenated zstd frames, flushed every 10 s; an unclean stop can lose queued or
//! unflushed data. `zstd -dc` reads complete frames. A truncated frame can only end a
//! file, since no later start appends to it. Recording never waits: the network tasks hand each
//! line to a writer thread, and drop it if the writer is a whole queue behind, counted in the log
//! only: a Lighter book read across a drop is incomplete until the next snapshot.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::clock::wall_us;
use super::feed::{self, Input, Upstream, Wire};
use crate::lighter::messages::OrderBooksResponse;
use crate::lighter::ws::{stream_url, subscribe_loop, SubscribeOptions};

/// Lines the writer may fall behind by: minutes of HYPE (~60 lines/s of Aster and Lighter, plus Hyperliquid).
const QUEUE: usize = 20_000;
const FLUSH: Duration = Duration::from_secs(10);
/// A flush also happens once this much is buffered.
const MAX_BUFFER: usize = 8 << 20;
const LEVEL: i32 = 9;
const SPECS_EVERY: Duration = Duration::from_secs(3_600);
const DEPTH_EVERY: Duration = Duration::from_secs(60);

enum Msg {
    Line(i64, &'static str, String),
    Stop,
}

/// A handle on the writer; clones share it.
#[derive(Clone)]
pub struct Tape {
    tx: SyncSender<Msg>,
    dropped: Arc<AtomicU64>,
}

impl Tape {
    /// Starts the writer thread for `<dir>/<market>/`.
    pub fn open(dir: &Path, market: &str) -> Result<(Tape, std::thread::JoinHandle<Result<()>>)> {
        let dir = dir.join(market);
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let (tx, rx) = sync_channel(QUEUE);
        let writer = std::thread::Builder::new().name("tape".into()).spawn(move || write(&dir, rx))?;
        Ok((Tape { tx, dropped: Arc::default() }, writer))
    }

    /// Records `payload` as arriving now.
    pub fn record(&self, kind: &'static str, payload: &str) {
        self.record_at(wall_us(), kind, payload);
    }

    fn record_at(&self, arrival_us: i64, kind: &'static str, payload: &str) {
        if let Err(TrySendError::Full(_)) = self.tx.try_send(Msg::Line(arrival_us, kind, payload.to_string())) {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped.is_power_of_two() {
                tracing::warn!("tape: the writer is behind; {dropped} lines dropped so far");
            }
        }
    }

    /// Writes what is queued, then stops the writer.
    pub fn stop(&self) {
        let _ = self.tx.send(Msg::Stop);
    }
}

/// `<YYYY-MM-DD>T<HHMMSS>Z`: the name of a file whose first line arrived at `arrival_us`.
fn file_name(arrival_us: i64) -> String {
    chrono::DateTime::from_timestamp_micros(arrival_us).map_or_else(|| "invalid-time".into(), |t| t.format("%Y-%m-%dT%H%M%SZ").to_string())
}

fn write(dir: &Path, rx: Receiver<Msg>) -> Result<()> {
    let mut buffer = Vec::new();
    let mut file: Option<String> = None;
    let mut due = Instant::now() + FLUSH;
    loop {
        match rx.recv_timeout(due.saturating_duration_since(Instant::now())) {
            Ok(Msg::Line(arrival_us, kind, payload)) => {
                let name = file_name(arrival_us);
                // The first 10 characters are the UTC day.
                if file.as_ref().map(|f| &f[..10]) != Some(&name[..10]) {
                    flush(dir, file.as_deref(), &mut buffer)?;
                    file = Some(name);
                }
                write!(buffer, "{arrival_us}\t{kind}\t")?;
                // The venues send single-line JSON; a stray newline would split the record.
                buffer.extend(payload.bytes().map(|b| if b == b'\n' || b == b'\r' { b' ' } else { b }));
                buffer.push(b'\n');
                if buffer.len() >= MAX_BUFFER {
                    flush(dir, file.as_deref(), &mut buffer)?;
                }
            }
            Ok(Msg::Stop) | Err(RecvTimeoutError::Disconnected) => return flush(dir, file.as_deref(), &mut buffer),
            Err(RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= due {
            flush(dir, file.as_deref(), &mut buffer)?;
            due = Instant::now() + FLUSH;
        }
    }
}

/// Appends `buffer` to `file` as one zstd frame.
fn flush(dir: &Path, file: Option<&str>, buffer: &mut Vec<u8>) -> Result<()> {
    let Some(file) = file.filter(|_| !buffer.is_empty()) else { return Ok(()) };
    let path = dir.join(format!("{file}.tape.zst"));
    let frame = zstd::bulk::compress(buffer, LEVEL)?;
    let mut file = OpenOptions::new().create(true).append(true).open(&path).with_context(|| format!("opening {}", path.display()))?;
    file.write_all(&frame)?;
    file.sync_data()?;
    buffer.clear();
    Ok(())
}

/// A tape file's lines as (arrival µs, kind, payload). A frame cut short ends it.
pub fn lines(path: &Path) -> Result<impl Iterator<Item = Result<(i64, String, String)>>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let path = path.to_path_buf();
    let lines = BufReader::new(zstd::stream::read::Decoder::new(file)?).lines().map_while(Result::ok);
    Ok(lines.enumerate().map(move |(n, line)| {
        let mut parts = line.splitn(3, '\t');
        let (Some(at), Some(kind), Some(payload)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(anyhow!("{}: malformed line {}", path.display(), n + 1));
        };
        Ok((at.parse()?, kind.to_string(), payload.to_string()))
    }))
}

/// How far ahead of its arrival on the shifted clock a replay feeds each line: more than the worst
/// feed lag the tape recorded (2026-10-10: Aster 75 s, Lighter 59 s), so a frame reaches the
/// simulator before its venue time and this host's stalls vanish.
pub const LEAD_US: i64 = 120_000_000;
/// Nothing on the tape for this long (the recorder down, or a longer stall) freezes the bot's feeds.
const SILENCE_US: i64 = 30_000_000;
/// A Lighter connection starts with its book whole.
const SNAPSHOT: &str = "\"subscribed/order_book\"";

/// When a tape file starts (µs), from its name.
fn start_of(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?.strip_suffix(".tape.zst")?;
    Some(chrono::NaiveDateTime::parse_from_str(name, "%Y-%m-%dT%H%M%SZ").ok()?.and_utc().timestamp_micros())
}

/// `market`'s tape under `root` for a replay from `from_us`: the Tokyo VPS's (`data-tokyo`, pulled
/// by `vps.py pull`) once one of its files starts by then, else this PC's own (`data`, recorded
/// until 2026-10-10). Never a mix: the two overlap in time and a replay reads its files in order.
pub fn default_dir(root: &Path, market: &str, from_us: i64) -> PathBuf {
    let tokyo = root.join("data-tokyo").join(market);
    let started = std::fs::read_dir(&tokyo).into_iter().flatten().filter_map(|e| start_of(&e.ok()?.path())).any(|start| start <= from_us);
    if started { tokyo } else { root.join("data").join(market) }
}

/// A replay of a market's tape from `from_us` to `until_us` (µs, venue time): a dry run whose
/// world is the tape's, shifted by the days since. Ponytail: it runs at the market's own pace;
/// a faster one would need every clock the bot reads to follow one virtual clock.
#[derive(Debug, Clone)]
pub struct Replay {
    /// The files to read, in order: the first from the line of the last Lighter snapshot at or
    /// before `from_us`.
    pub files: Vec<PathBuf>,
    first_line: usize,
    pub from_us: i64,
    pub until_us: i64,
    pub lead_us: i64,
    /// The Aster `exchangeInfo` and Lighter `orderBooks` recorded last by `from_us`.
    pub specs: [String; 2],
    /// Cancelled once the bot's clock passes `until_us` or the tape ends: the bot drains.
    pub stop: CancellationToken,
}

impl Replay {
    /// Finds where in `dir` (`data-tokyo/<MARKET>` or `data/<MARKET>`) a replay from `from_us` starts.
    pub fn open(dir: &Path, from_us: i64, until_us: i64, lead_us: i64, stop: CancellationToken) -> Result<Self> {
        ensure!(from_us < until_us, "--from must come before --until");
        let mut files: Vec<(i64, PathBuf)> = std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter_map(|path| Some((start_of(&path)?, path)))
            // A file is named after its first line: one started after `until_us` is not needed.
            .filter(|(start, _)| *start <= until_us)
            .collect();
        files.sort();
        let last = files.iter().rposition(|(start, _)| *start <= from_us).with_context(|| format!("no tape in {} starts by --from", dir.display()))?;
        let (mut snapshot, mut specs) = (None, [None, None]);
        for i in (0..=last).rev() {
            let (mut found, mut listed) = (None, [None, None]);
            for (n, line) in lines(&files[i].1)?.enumerate() {
                let (at, kind, payload) = line?;
                if at > from_us {
                    break;
                }
                match kind.as_str() {
                    "L" if payload.contains(SNAPSHOT) => found = Some((i, n)),
                    "X" => listed[0] = Some(payload),
                    "O" => listed[1] = Some(payload),
                    _ => {}
                }
            }
            snapshot = snapshot.or(found);
            for (spec, line) in specs.iter_mut().zip(listed) {
                *spec = spec.take().or(line);
            }
            if snapshot.is_some() && specs.iter().all(Option::is_some) {
                break;
            }
        }
        let (Some((first, first_line)), [Some(aster), Some(lighter)]) = (snapshot, specs) else {
            bail!("the tape in {} has no Lighter snapshot or no listings by --from", dir.display());
        };
        let files = files.into_iter().skip(first).map(|(_, path)| path).collect();
        Ok(Self { files, first_line, from_us, until_us, lead_us, specs: [aster, lighter], stop })
    }

    /// Feeds the tape to the venues' upstreams on a thread of its own: the Lighter book from its
    /// snapshot at once, then each line `lead_us` before its arrival on the clock shifted by
    /// `shift_us`. The receiver resolves when the tape reaches `from_us`, before any bot
    /// connection follows the feed (the catch-up burst would overflow it). The feed goes on after
    /// `stop`, so the drain still meets a market.
    pub fn spawn(&self, shift_us: i64, mut aster: Upstream, mut lighter: Upstream, inputs: mpsc::UnboundedSender<Input>) -> Result<oneshot::Receiver<()>> {
        let (caught_up, rx) = oneshot::channel();
        let replay = self.clone();
        std::thread::Builder::new().name("replay".into()).spawn(move || {
            if let Err(e) = replay.run(shift_us, &mut aster, &mut lighter, &inputs, caught_up) {
                tracing::error!("replay: {e:#}");
            }
            replay.stop.cancel();
        })?;
        Ok(rx)
    }

    fn run(&self, shift_us: i64, aster: &mut Upstream, lighter: &mut Upstream, inputs: &mpsc::UnboundedSender<Input>, caught_up: oneshot::Sender<()>) -> Result<()> {
        let (mut caught_up, mut last) = (Some(caught_up), self.from_us);
        for (i, file) in self.files.iter().enumerate() {
            for line in lines(file)?.skip(if i == 0 { self.first_line } else { 0 }) {
                let (at, kind, payload) = line?;
                if inputs.is_closed() {
                    return Ok(());
                }
                if at >= self.from_us {
                    if let Some(tx) = caught_up.take() {
                        let _ = tx.send(());
                    }
                    if at - last > SILENCE_US {
                        tracing::warn!("replay: nothing on the tape for {} s before {}", (at - last) / 1_000_000, time(at));
                    }
                    last = last.max(at);
                    let wait = at + shift_us - self.lead_us - wall_us();
                    if wait > 0 {
                        std::thread::sleep(Duration::from_micros(wait as u64));
                    }
                    if at - self.lead_us >= self.until_us && !self.stop.is_cancelled() {
                        tracing::info!("replay: the bot's clock passed --until; draining");
                        self.stop.cancel();
                    }
                } else if !kind.starts_with('L') {
                    // Before `from_us` only the Lighter book matters: Aster's streams are whole.
                    continue;
                }
                match kind.as_str() {
                    "A" => aster.aster(&payload),
                    "A-" => aster.closed(),
                    "L" => {
                        // A snapshot starts a connection: a recorder restart ends the last one unmarked.
                        if payload.contains(SNAPSHOT) {
                            lighter.closed();
                        }
                        if !lighter.lighter(&payload) {
                            lighter.closed();
                        }
                    }
                    "L-" => lighter.closed(),
                    "F" => feed::aster_premium(&payload, inputs),
                    _ => {}
                }
            }
        }
        ensure!(caught_up.is_none(), "the tape ends before --from");
        tracing::warn!("replay: the tape ends at {}", time(last));
        Ok(())
    }
}

fn time(us: i64) -> String {
    chrono::DateTime::from_timestamp_micros(us).map_or_else(|| us.to_string(), |t| t.to_rfc3339())
}

/// `record`: tapes `market`'s public feeds from the venues in `config` into `dir` until a stop
/// signal. It needs no credentials and sends no orders.
pub async fn record(config: &Path, market: &str, dir: PathBuf, stop: CancellationToken) -> Result<()> {
    let cfg = crate::controller::BotConfig::load(config)?;
    let (taker_markets, _) = cfg.select(&market.to_ascii_uppercase())?;
    let market = &taker_markets[0];
    let aster_base = cfg.maker.live.aster.base_url.trim_end_matches('/').to_string();
    let lighter_base = cfg.maker.live.lighter.base_url.trim_end_matches('/').to_string();
    let symbol = market.aster_symbol.to_ascii_uppercase();
    let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let get = move |url: String| {
        let request = http.get(url);
        async move { anyhow::Ok(request.send().await?.error_for_status()?.text().await?) }
    };
    // A boot can come before the network. Waiting here resumes within seconds of its return,
    // where Docker's restart backoff grows to a minute of lost data.
    let order_books = loop {
        match get(format!("{lighter_base}/api/v1/orderBooks")).await {
            Ok(body) => break body,
            Err(e) => tracing::warn!("recorder: fetching Lighter orderBooks: {e:#}"),
        }
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = tokio::time::sleep(feed::UPSTREAM_BACKOFF_MAX) => {}
        }
    };
    let index = serde_json::from_str::<OrderBooksResponse>(&order_books)?
        .order_books
        .into_iter()
        .find(|b| b.symbol.eq_ignore_ascii_case(&market.lighter_symbol))
        .with_context(|| format!("Lighter orderBooks has no {}", market.lighter_symbol))?
        .market_id;

    let (tape, writer) = Tape::open(&dir, &market.id().0)?;
    let polls = [
        (SPECS_EVERY, [("X", format!("{aster_base}/fapi/v3/exchangeInfo")), ("O", format!("{lighter_base}/api/v1/orderBooks"))]),
        (DEPTH_EVERY, [("DA", format!("{aster_base}/fapi/v1/depth?symbol={symbol}&limit=500")),
            ("DL", format!("{lighter_base}/api/v1/orderBookOrders?market_id={index}&limit=250"))]),
    ];
    for (every, specs) in polls {
        let (tape, get) = (tape.clone(), get.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                for &(kind, ref url) in &specs {
                    match get(url.clone()).await {
                        Ok(body) => tape.record(kind, &body),
                        Err(e) => tracing::warn!("recorder: {url}: {e:#}"),
                    }
                }
            }
        });
    }
    let aster_url = feed::aster_url(&crate::connectors::aster::ws_root(&aster_base), &[symbol.clone()]);
    tokio::spawn({
        let tape = tape.clone();
        async move {
            feed::aster_stream(aster_url, "recorder Aster", |wire| match wire {
                Wire::Text(text) => tape.record("A", text),
                Wire::Closed => tape.record("A-", ""),
            })
            .await
        }
    });
    let channels = ["order_book", "trade", "market_stats"].map(|c| format!("{c}/{index}")).to_vec();
    let mut opts = SubscribeOptions::new(&stream_url(&lighter_base), "recorder Lighter", channels);
    opts.reconnect_base = 0.5;
    opts.reconnect_max = feed::UPSTREAM_BACKOFF_MAX.as_secs_f64();
    let (on_frame, on_close) = (tape.clone(), tape.clone());
    tokio::spawn(subscribe_loop(opts, None, move |frame| on_frame.record("L", frame.raw), move || on_close.record("L-", "")));
    let on_body = tape.clone();
    tokio::spawn(async move { feed::aster_premium_poll(aster_base, vec![symbol], "recorder premiumIndex", |body| on_body.record("F", body)).await });
    // Ponytail: Hyperliquid's coin is the hedge coin's name (`lighter_symbol`), as in the bot's
    // configs; a coin spelled differently there would need its own field.
    let coin = market.lighter_symbol.to_ascii_uppercase();
    let hl_url = crate::connectors::hyperliquid::ws_url(&cfg.maker.live.hyperliquid.base_url);
    let mut fast = crate::connectors::hyperliquid::subscriptions(&coin).to_vec();
    fast.extend([json!({"type": "trades", "coin": coin}), json!({"type": "activeAssetCtx", "coin": coin})]);
    // The 20-level book on a connection of its own: its frames do not say which mode they are.
    let slow = vec![json!({"type": "l2Book", "coin": coin})];
    for (subscriptions, kinds, label) in [(fast, ("H", "H-"), "recorder Hyperliquid"), (slow, ("HS", "HS-"), "recorder Hyperliquid slow l2Book")] {
        let (tape, url) = (tape.clone(), hl_url.clone());
        tokio::spawn(async move {
            feed::hyperliquid_stream(url, subscriptions, label, |wire| match wire {
                Wire::Text(text) => tape.record(kinds.0, text),
                Wire::Closed => tape.record(kinds.1, ""),
            })
            .await
        });
    }
    tracing::info!("recording {} (Aster {}, Lighter market {index}, Hyperliquid {coin}) into {}", market.id().0, market.aster_symbol, dir.display());

    // A failed writer (a full disk) stops the recorder too, so the restart policy shows it.
    let watch = {
        let stop = stop.clone();
        tokio::task::spawn_blocking(move || {
            let result = writer.join();
            stop.cancel();
            result
        })
    };
    stop.cancelled().await;
    tape.stop();
    watch.await?.map_err(|_| anyhow!("the tape writer panicked"))?
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::dryrun::feed::{tests::lighter, Hub};
    use crate::dryrun::matching::{FeedEvent, Venue};

    /// An Aster depth20 frame arriving at `at_ms`, 20 ms after its engine stamp.
    pub(crate) fn aster_depth(at_ms: i64, bid: &str, ask: &str) -> String {
        let t = at_ms - 20;
        format!(r#"{{"stream":"hypeusdt@depth20@100ms","data":{{"e":"depthUpdate","E":{t},"T":{t},"s":"HYPEUSDT","U":1,"u":2,"pu":0,"b":[["{bid}","5"]],"a":[["{ask}","5"]]}}}}"#)
    }

    /// Tapes `lines` (arrival ms, kind, payload) into `dir/HYPE` as the recorder does.
    pub(crate) fn write_tape(dir: &Path, lines: &[(i64, &'static str, String)]) {
        let (tape, writer) = Tape::open(dir, "HYPE").unwrap();
        for (at_ms, kind, payload) in lines {
            tape.record_at(at_ms * 1_000, kind, payload);
        }
        tape.stop();
        writer.join().unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_replay_starts_at_the_lighter_snapshot_paces_the_tape_ahead_and_stops_past_until() {
        let dir = crate::dryrun::tests::temp_dir("replay");
        let t0 = wall_us() / 1_000 - 60_000;
        let (top, delta) = (|at| lighter("subscribed", 0, 10, at, r#"[{"price":"99","size":"5"}]"#, r#"[{"price":"101","size":"5"}]"#), |n, at| lighter("update", n, n + 1, at, "[]", "[]"));
        // Hyperliquid and REST depth lines (2026-10-10 on) leave an Aster-Lighter replay as it was.
        let hl = |at: i64| format!(r#"{{"channel":"l2Book","data":{{"coin":"HYPE","time":{at},"levels":[[{{"px":"99","sz":"1","n":1}}],[{{"px":"101","sz":"1","n":1}}]]}}}}"#);
        write_tape(&dir, &[
            (t0 - 5_000, "X", "{}".into()),
            (t0 - 4_000, "O", "{}".into()),
            (t0 - 3_000, "L", top(t0 - 3_000)),
            (t0 - 2_000, "L", top(t0 - 2_000)),
            (t0 - 1_000, "L", delta(10, t0 - 1_000)),
            (t0 - 700, "H", hl(t0 - 700)),
            (t0 - 650, "DA", r#"{"bids":[["99","5"]],"asks":[["101","5"]]}"#.into()),
            (t0 - 600, "HS", hl(t0 - 600)),
            (t0 - 500, "A", aster_depth(t0 - 500, "99", "101")),
            (t0 + 100, "A", aster_depth(t0 + 100, "99", "101")),
            (t0 + 150, "H", hl(t0 + 150)),
            (t0 + 160, "HS-", String::new()),
            (t0 + 170, "H-", String::new()),
            (t0 + 200, "L", delta(12, t0 + 200)),
            (t0 + 300, "L", delta(13, t0 + 300)),
            (t0 + 400, "L", top(t0 + 400)),
            (t0 + 900, "A", aster_depth(t0 + 900, "99", "101")),
            (t0 + 2_000, "A-", String::new()),
        ]);
        let (stop, lead) = (CancellationToken::new(), 300_000);
        let replay = Replay::open(&dir.join("HYPE"), t0 * 1_000, (t0 + 500) * 1_000, lead, stop.clone()).unwrap();
        let shift = wall_us() - t0 * 1_000 + lead;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let up = |venue, market: &str, stream: &str| Upstream::new(venue, vec![market.into()], shift, Arc::new(Mutex::new(Hub::new(shift, [stream.to_string()]))), tx.clone());
        let (aster, book) = (up(Venue::Aster, "HYPEUSDT", "hypeusdt@depth20@100ms"), up(Venue::Lighter, "24", "order_book/24"));
        replay.spawn(shift, aster, book, tx).unwrap().await.expect("caught up");
        // The thread drops the senders at the end of the tape.
        let mut seen = Vec::new();
        while let Some(Input::Frame { venue, exch_us, event, .. }) = rx.recv().await {
            // Ahead of its time on the shifted clock, for a frame from `from` on (a gap carries the time of the frame before it).
            let gap = matches!(event, FeedEvent::Gap);
            let ahead = gap || exch_us < t0 * 1_000 || wall_us() < exch_us + shift;
            seen.push((venue, exch_us, gap, ahead, stop.is_cancelled()));
        }
        let (lighter_us, aster_us) = (|at: i64| at * 1_000 - 11_400, |at: i64| (at - 20) * 1_000);
        assert_eq!(seen, [
            // The last snapshot by `from` and what follows it, at once; no Aster before `from`.
            (Venue::Lighter, lighter_us(t0 - 2_000), false, true, false),
            (Venue::Lighter, lighter_us(t0 - 1_000), false, true, false),
            // Then each frame ahead of its time.
            (Venue::Aster, aster_us(t0 + 100), false, true, false),
            // A break in the sequence closes the book once, until the next snapshot.
            (Venue::Lighter, lighter_us(t0 - 1_000) + 1, true, true, false),
            (Venue::Lighter, lighter_us(t0 + 400), false, true, false),
            // The bot's clock (a lead behind) passed --until: the bot drains, the feed goes on.
            (Venue::Aster, aster_us(t0 + 900), false, true, true),
            (Venue::Aster, aster_us(t0 + 900) + 1, true, true, true),
        ]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_replay_defaults_to_the_tokyo_tape_once_it_covers_from() {
        let root = crate::dryrun::tests::temp_dir("tapes");
        let tokyo = root.join("data-tokyo").join("HYPE");
        std::fs::create_dir_all(&tokyo).unwrap();
        std::fs::write(tokyo.join("2026-10-10T150000Z.tape.zst"), b"").unwrap();
        let at = |s: &str| chrono::DateTime::parse_from_rfc3339(s).unwrap().timestamp_micros();
        assert_eq!(default_dir(&root, "HYPE", at("2026-10-10T14:59:59Z")), root.join("data").join("HYPE"));
        assert_eq!(default_dir(&root, "HYPE", at("2026-10-10T15:00:00Z")), tokyo);
        assert_eq!(default_dir(&root, "BTC", at("2026-10-11T00:00:00Z")), root.join("data").join("BTC"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_new_utc_day_or_a_restart_starts_a_file() {
        let dir = crate::dryrun::tests::temp_dir("tape");
        let day = 1_790_294_400_000_000; // 2026-09-25T00:00:00Z
        let (tape, writer) = Tape::open(&dir, "HYPE").unwrap();
        tape.record_at(day - 1, "A", r#"{"stream":"x"}"#);
        tape.record_at(day, "L", "{\"a\":\n1}");
        tape.record_at(day + 1, "L-", "");
        tape.stop();
        writer.join().unwrap().unwrap();
        let (tape, writer) = Tape::open(&dir, "HYPE").unwrap();
        tape.record_at(day + 61_000_000, "A-", "");
        tape.stop();
        writer.join().unwrap().unwrap();

        let files = dir.join("HYPE");
        let mut names: Vec<_> = std::fs::read_dir(&files).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        names.sort();
        assert_eq!(names, ["2026-09-24T235959Z.tape.zst", "2026-09-25T000000Z.tape.zst", "2026-09-25T000101Z.tape.zst"]);
        let read = |name: &str| lines(&files.join(name)).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(read(&names[0]), vec![(day - 1, "A".into(), r#"{"stream":"x"}"#.into())]);
        assert_eq!(read(&names[1]), vec![(day, "L".into(), "{\"a\": 1}".into()), (day + 1, "L-".into(), String::new())]);
        // A frame cut short ends its file.
        let path = files.join(&names[2]);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&zstd::bulk::compress(b"1\tA\t{}\n", LEVEL).unwrap()[..9]);
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(read(&names[2]), vec![(day + 61_000_000, "A-".into(), String::new())]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

//! Dry-run mode: the bot trades its selected route against two in-process simulated venues.
//!
//! The simulated world is the real one delayed by a constant shift D: real feed events are
//! replayed D late to absorb host feed lag; configured request/feed delays model a Tokyo scenario.
//! `matching` is the venues' deterministic core; `book` holds
//! the replica each market trades against; `account` settles the money; `clock` draws the
//! latencies; `feed` brings the real market in; `server` speaks HTTP and WebSocket on
//! loopback, and `aster`, `lighter` and `hyperliquid` speak each venue's protocol on top of
//! it. `tape` records the same feeds to disk for backtests (`record`).

pub mod account;
pub mod aster;
pub mod book;
pub mod clock;
pub mod feed;
pub mod hyperliquid;
pub mod lighter;
pub mod matching;
pub mod server;
pub mod tape;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, oneshot};

use self::account::AccountView;
use self::clock::{wall_us, Latency};
use self::feed::{Follower, Hub, Input};
use self::matching::{
    quantiles, Envelope, Event, Exchange, Fees, Filters, Order, Output, Reject, Reply, SimParams, Venue, VenueState, LOOKAHEAD_US,
};
use crate::config::{FirstVenue, HedgeVenue};
use crate::controller::BotConfig;
use crate::decimal::parse_dec;
use crate::lighter::messages::{OrderBookDetail, OrderBooksResponse};

/// `[dry_run]` in bot.toml, where each value cites its source.
#[derive(Debug, Clone, Deserialize)]
pub struct DryRunCfg {
    pub shift_ms: i64,
    pub seed: u64,
    pub aster_port: u16,
    pub lighter_port: u16,
    pub hyperliquid_port: u16,
    pub aster_balance_usdt: Decimal,
    pub lighter_balance_usdc: Decimal,
    pub effect_fraction: f64,
    pub aster_rest_rtt_ms: Latency,
    pub lighter_rtt_ms: Latency,
    pub lighter_taker_delay_ms: i64,
    pub aster_feed_ms: Latency,
    pub lighter_feed_ms: Latency,
    pub aster_user_stream_ms: Latency,
    pub lighter_account_ms: Latency,
    pub hyperliquid_rtt_ms: Latency,
    pub hyperliquid_feed_ms: Latency,
    pub hidden_queue_multiplier: Decimal,
}

impl DryRunCfg {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.shift_ms * 1_000 > LOOKAHEAD_US,
            "[dry_run] shift_ms must exceed the {} ms matching lookahead plus this host's feed lag",
            LOOKAHEAD_US / 1_000
        );
        ensure!((0.0..=1.0).contains(&self.effect_fraction), "[dry_run] effect_fraction must be within [0, 1]");
        ensure!(self.lighter_taker_delay_ms >= 0, "[dry_run] lighter_taker_delay_ms must be >= 0");
        ensure!(self.hidden_queue_multiplier >= Decimal::ZERO, "[dry_run] hidden_queue_multiplier must be >= 0");
        ensure!(
            self.aster_balance_usdt > Decimal::ZERO && self.lighter_balance_usdc > Decimal::ZERO,
            "[dry_run] starting balances must be positive"
        );
        let ports = [self.aster_port, self.lighter_port, self.hyperliquid_port];
        ensure!(ports.iter().enumerate().all(|(i, p)| *p == 0 || !ports[..i].contains(p)), "[dry_run] the venues need different ports");
        Ok(())
    }

    /// The core's parameters for `venues`; `fees` (in that order) and `leverage` come from the
    /// bot's own keys, so the simulation charges what the strategy assumes. Hyperliquid's private
    /// stream and starting balance are Lighter's.
    pub fn sim_params(&self, fees: [Fees; 2], leverage: Decimal, venues: [Venue; 2]) -> SimParams {
        let [a, b] = venues.map(|venue| match venue {
            Venue::Aster => (self.aster_rest_rtt_ms, self.aster_user_stream_ms, self.aster_balance_usdt),
            Venue::Lighter => (self.lighter_rtt_ms, self.lighter_account_ms, self.lighter_balance_usdc),
            Venue::Hyperliquid => (self.hyperliquid_rtt_ms, self.lighter_account_ms, self.lighter_balance_usdc),
        });
        SimParams {
            shift_us: self.shift_ms * 1_000,
            seed: self.seed,
            effect_fraction: self.effect_fraction,
            rtt: [a.0, b.0],
            private: [a.1, b.1],
            lighter_taker_delay_us: self.lighter_taker_delay_ms * 1_000,
            hidden_queue_multiplier: self.hidden_queue_multiplier,
            fees,
            leverage,
            balances: [a.2, b.2],
            venues,
        }
    }
}

/// Aster's stream carries 20 levels a side, Hyperliquid's fast l2Book 5; Lighter's book is whole.
const DEPTH: usize = 20;
/// How long the live market data may take to give both replicas a book.
const WARM_TIMEOUT: Duration = Duration::from_secs(60);

/// One simulated venue as its real API lists it: what the core, the feed and the venue's
/// protocol server need.
struct Listing {
    venue: Venue,
    /// The real API, whose market data the venue replays.
    base: String,
    /// Its market's key in the core: Aster's symbol, Lighter's market id, Hyperliquid's coin.
    market: String,
    filters: Filters,
    fees: Fees,
    /// What its protocol server lists: exchangeInfo, orderBooks or meta, whole.
    meta: String,
    lighter: Option<OrderBookDetail>,
}

impl Listing {
    /// Fetches `symbol`'s listing on `venue` from the real API `cfg` points at. Its fees are the
    /// bot's own keys (circular by construction); the bot makes only on Aster.
    async fn fetch(venue: Venue, symbol: &str, cfg: &BotConfig, http: &reqwest::Client) -> Result<Self> {
        let (live, edge, arb) = (&cfg.maker.live, &cfg.maker.edge, &cfg.taker.arb);
        let rate = |bps: Decimal| bps / Decimal::from(10_000);
        let get = |url: String| {
            let request = http.get(url);
            async move { request.send().await?.error_for_status()?.text().await }
        };
        match venue {
            Venue::Aster => {
                let base = live.aster.base_url.trim_end_matches('/').to_string();
                let meta = get(format!("{base}/fapi/v3/exchangeInfo")).await.context("fetching Aster exchangeInfo")?;
                let market = symbol.to_ascii_uppercase();
                let aster = crate::connectors::rest_specs::parse_aster_exchange_info(&meta)?
                    .remove(&market)
                    .with_context(|| format!("Aster exchangeInfo has no {market}"))?;
                let filters = Filters {
                    tick: aster.tick,
                    step: aster.step,
                    min_qty: aster.min_qty,
                    min_notional: aster.min_notional,
                    percent_price: aster.percent_price,
                    sig_figs: None,
                };
                let fees = Fees { maker: rate(edge.aster_maker_fee_bps), taker: rate(arb.aster_taker_fee_bps) };
                Ok(Self { venue, base, market, filters, fees, meta, lighter: None })
            }
            Venue::Lighter => {
                let base = live.lighter.base_url.trim_end_matches('/').to_string();
                let meta = get(format!("{base}/api/v1/orderBooks")).await.context("fetching Lighter orderBooks")?;
                let detail = serde_json::from_str::<OrderBooksResponse>(&meta)
                    .context("parsing Lighter orderBooks")?
                    .order_books
                    .into_iter()
                    .find(|b| b.symbol.eq_ignore_ascii_case(symbol))
                    .with_context(|| format!("Lighter orderBooks has no {symbol}"))?;
                let filters = Filters {
                    tick: Decimal::new(1, detail.supported_price_decimals),
                    step: Decimal::new(1, detail.supported_size_decimals),
                    min_qty: parse_dec(&detail.min_base_amount)?,
                    min_notional: parse_dec(&detail.min_quote_amount)?,
                    percent_price: None,
                    sig_figs: None,
                };
                // Both engines must agree on the Lighter fee they share.
                ensure!(
                    edge.taker_fee_bps == arb.lighter_taker_fee_bps,
                    "[maker.edge] taker_fee_bps and [taker.arb] lighter_taker_fee_bps are the same Lighter fee but differ"
                );
                let fees = Fees { maker: Decimal::ZERO, taker: rate(arb.lighter_taker_fee_bps) };
                Ok(Self { venue, base, market: detail.market_id.to_string(), filters, fees, meta, lighter: Some(detail) })
            }
            Venue::Hyperliquid => {
                let base = live.hyperliquid.base_url.trim_end_matches('/').to_string();
                let meta = http.post(format!("{base}/info")).json(&json!({"type": "meta"})).send().await
                    .and_then(|r| r.error_for_status()).context("fetching Hyperliquid meta")?.text().await?;
                let asset = crate::hyperliquid::client::asset_in(&serde_json::from_str(&meta).context("parsing Hyperliquid meta")?, symbol)?;
                // At most 6 - szDecimals decimals and 5 significant figures; $10 at the limit price;
                // no price over 80% from the reference.
                let filters = Filters {
                    tick: Decimal::new(1, 6u32.saturating_sub(asset.sz_decimals)),
                    step: Decimal::new(1, asset.sz_decimals),
                    min_qty: Decimal::new(1, asset.sz_decimals),
                    min_notional: Decimal::TEN,
                    percent_price: Some((Decimal::new(2, 1), Decimal::new(18, 1))),
                    sig_figs: Some(5),
                };
                let fee = edge.hyperliquid_taker_fee_bps.context("trading on Hyperliquid needs [maker.edge] hyperliquid_taker_fee_bps")?;
                ensure!(
                    arb.hyperliquid_taker_fee_bps == Some(fee),
                    "[maker.edge] and [taker.arb] hyperliquid_taker_fee_bps are the same Hyperliquid fee but differ"
                );
                let fees = Fees { maker: Decimal::ZERO, taker: rate(fee) };
                Ok(Self { venue, base, market: asset.coin, filters, fees, meta, lighter: None })
            }
        }
    }
}

/// Starts the simulated venues of `market`'s two legs on loopback, fed by the live market data
/// of the venues `cfg` points at, then points both engines at them: every venue URL (one not
/// simulated resolves nowhere), the dry-run identity, and the taker's files under `runs_dir`.
/// The venues take up the state a previous run saved there. Returns once both replicas hold a
/// book.
pub async fn start(dry: &DryRunCfg, cfg: &mut BotConfig, market: &crate::taker::config::MarketCfg, runs_dir: &Path) -> Result<Venues> {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let hedge = match market.hedge_venue {
        HedgeVenue::Lighter => Venue::Lighter,
        HedgeVenue::Hyperliquid => Venue::Hyperliquid,
    };
    let (first, symbol) = match market.first_venue {
        FirstVenue::Aster => (Venue::Aster, &market.aster_symbol),
        FirstVenue::Lighter => (Venue::Lighter, &market.lighter_symbol),
    };
    let legs = [
        Listing::fetch(first, symbol, cfg, &http).await?,
        Listing::fetch(hedge, &market.lighter_symbol, cfg, &http).await?,
    ];
    let (pair, keys) = (legs.each_ref().map(|leg| leg.venue), legs.each_ref().map(|leg| leg.market.clone()));
    let leverage = cfg.maker.capital.leverage;
    let mut core = Exchange::new(dry.sim_params(legs.each_ref().map(|leg| leg.fees.clone()), leverage, pair), wall_us());
    for leg in &legs {
        core.add_market(leg.venue, &leg.market, (leg.venue != Venue::Lighter).then_some(DEPTH), leg.filters.clone());
    }
    let files = SimFiles::new(runs_dir, &market.id().0);
    if let Some(saved) = files.load()? {
        let [a, b] = [0, 1].map(|i| format!("{:?} {}", pair[i], saved[i].account.balance));
        tracing::info!("dry run: resuming the simulated accounts of {} ({a}, {b})", files.state.display());
        core.restore(saved);
    }
    let shift = dry.shift_ms * 1_000;
    let hubs = legs.each_ref().map(|leg| {
        let streams = match leg.venue {
            Venue::Aster => feed::aster_streams(&leg.market).to_vec(),
            Venue::Lighter => vec![format!("order_book/{}", leg.market)],
            Venue::Hyperliquid => vec![format!("l2Book/{}", leg.market), format!("bbo/{}", leg.market)],
        };
        Arc::new(Mutex::new(Hub::new(shift, streams)))
    });
    let feed_ms = pair.map(|venue| match venue {
        Venue::Aster => dry.aster_feed_ms,
        Venue::Lighter => dry.lighter_feed_ms,
        Venue::Hyperliquid => dry.hyperliquid_feed_ms,
    });
    let (inputs, feed_rx) = mpsc::unbounded_channel();
    let venues = Venues::start(core, feed_rx, hubs.clone(), feed_ms, dry.seed, Some(files));
    let mut urls = HashMap::new();
    for (leg, hub) in legs.into_iter().zip(hubs) {
        let (base, markets, inputs) = (leg.base, vec![leg.market], inputs.clone());
        let url = match leg.venue {
            Venue::Aster => {
                tokio::spawn(feed::aster_upstream(crate::connectors::aster::ws_root(&base), markets.clone(), shift, hub, inputs.clone()));
                tokio::spawn(feed::aster_funding_poll(base, markets.clone(), inputs));
                serve(dry.aster_port, aster::Aster::new(venues.clone(), leg.meta, markets, leverage)).await?
            }
            Venue::Lighter => {
                tokio::spawn(feed::lighter_upstream(crate::lighter::ws::stream_url(&base), markets, shift, hub, inputs));
                let fees = [leg.fees.maker, leg.fees.taker];
                serve(dry.lighter_port, lighter::Lighter::new(venues.clone(), leg.meta, leg.lighter.into_iter().collect(), fees, leverage)).await?
            }
            Venue::Hyperliquid => {
                tokio::spawn(feed::hyperliquid_upstream(crate::connectors::hyperliquid::ws_url(&base), markets, shift, hub, inputs));
                serve(dry.hyperliquid_port, hyperliquid::Hyperliquid::new(venues.clone(), &leg.meta, leverage)?).await?
            }
        };
        urls.insert(leg.venue, url);
    }
    // RFC 2606: `.invalid` never resolves.
    let url = |venue| urls.get(&venue).cloned().unwrap_or_else(|| "http://unsimulated.invalid".to_string());
    let (live, taker) = (&mut cfg.maker.live, &mut cfg.taker.venues);
    (live.aster.base_url, taker.aster_base_url) = (url(Venue::Aster), url(Venue::Aster));
    (live.lighter.base_url, taker.lighter_base_url) = (url(Venue::Lighter), url(Venue::Lighter));
    (live.hyperliquid.base_url, taker.hyperliquid_base_url) = (url(Venue::Hyperliquid), url(Venue::Hyperliquid));
    (live.dry_run, taker.dry_run) = (true, true);
    cfg.taker.pnl.persist_dir = runs_dir.to_string_lossy().into_owned();

    let markets = [(pair[0], keys[0].as_str()), (pair[1], keys[1].as_str())];
    tokio::time::timeout(WARM_TIMEOUT, venues.warm(&markets)).await.context("the live market data gave no book within 60 s")?;
    // The first books have filled what the market crossed while the venues were down; now
    // the deadmen that ran out meanwhile cancel the rest.
    venues.resume();
    tracing::info!("dry run: simulated venues {urls:?}; the world is shifted {} ms", dry.shift_ms);
    Ok(venues)
}

/// Save cadence for changed venue state. An unclean stop loses changes since the last
/// successful save; scheduling and storage latency can lengthen that interval.
const SAVE_EVERY: Duration = Duration::from_secs(1);
/// Diagnostic reporting cadence.
const REPORT_EVERY: Duration = Duration::from_secs(60);

/// A dry run's own files in its runs directory: the simulated venues' state, which the next
/// start takes up, and one diagnostics row per window. Ponytail: the diagnostics file grows by
/// about 2 MB a day, with no rotation.
pub struct SimFiles {
    state: PathBuf,
    diag: PathBuf,
    /// The state as last written: an unchanged state is not rewritten.
    written: String,
    window_start_us: i64,
}

impl SimFiles {
    pub fn new(runs_dir: &Path, market: &str) -> Self {
        let market = market.to_ascii_uppercase();
        Self {
            state: runs_dir.join(format!("sim-{market}.state.json")),
            diag: runs_dir.join(format!("sim-{market}.diag.jsonl")),
            written: String::new(),
            window_start_us: wall_us(),
        }
    }

    /// The state a previous run saved, if any.
    fn load(&self) -> Result<Option<[VenueState; 2]>> {
        let text = match std::fs::read_to_string(&self.state) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading {}", self.state.display())),
        };
        let saved = serde_json::from_str(&text)
            .with_context(|| format!("{} is unreadable; move it away to start the simulated accounts afresh", self.state.display()))?;
        Ok(Some(saved))
    }

    /// Writes the state if it changed, synced before it replaces the last one: a crash leaves
    /// one whole state or the other, never a torn file the next start refuses. Ponytail: the
    /// sync runs on the venues' loop (the state changes about once a minute when idle); if
    /// `lateness_ms` grows with order flow, move the write to a thread.
    fn save(&mut self, core: &Exchange) -> Result<()> {
        let json = serde_json::to_string(core.state())?;
        if json != self.written {
            let raw = serde_json::value::RawValue::from_string(json.clone())?;
            crate::taker::pnl::write_json_atomic(&self.state, &raw, true)?;
            self.written = json;
        }
        Ok(())
    }

    /// Appends the window's diagnostics, logs their gist, and starts the next window.
    fn report(&mut self, core: &mut Exchange) -> Result<()> {
        let now = wall_us();
        let window_s = (now - self.window_start_us) as f64 / 1e6;
        let lateness = std::mem::take(&mut core.lateness_us);
        let lateness = quantiles(lateness.into_iter().map(|us| us as f64 / 1e3).collect());
        let mut gist = vec![format!("scheduler lateness p99 {} ms", lateness["p99"])];
        let mut row = json!({"ts_ms": now / 1_000, "window_s": window_s, "lateness_ms": lateness});
        for (v, venue) in core.venues().into_iter().enumerate() {
            let name = format!("{venue:?}").to_lowercase();
            let diag = std::mem::take(&mut core.diag[v]);
            let (view, _) = core.peek(venue);
            let account = &core.state()[v].account;
            let mut readout = diag.report();
            let lag: Vec<String> = diag.lag_us.keys().map(|s| format!("{s} {}", readout["lag_ms"][*s]["p99"])).collect();
            gist.push(format!(
                "{name}: {} frames ({} late; lag p99 ms: {}), {} orders, {}/{} maker/taker fills, {} rejects, equity {}",
                diag.frames,
                diag.late_frames,
                lag.join(", "),
                diag.orders,
                diag.maker_fills,
                diag.taker_fills,
                diag.rejects.values().sum::<u64>(),
                view.equity.round_dp(2),
            ));
            readout["account"] = json!({
                "balance": view.balance,
                "unrealized": view.unrealized,
                "equity": view.equity,
                "realized": account.realized,
                "fees": account.fees,
                "funding": account.funding,
                "positions": view.positions,
                "maintenance_breach": view.maintenance_breach,
            });
            row[&name] = readout;
        }
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&self.diag)
            .with_context(|| format!("opening {}", self.diag.display()))?;
        std::io::Write::write_all(&mut file, format!("{row}\n").as_bytes())?;
        tracing::info!("dry run, last {window_s:.0} s: {}", gist.join(" | "));
        self.window_start_us = now;
        Ok(())
    }
}

/// Serves `handler` on loopback `port` (0: any free port) and returns its URL.
async fn serve<H: server::Handler>(port: u16, handler: H) -> Result<String> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await.with_context(|| format!("binding 127.0.0.1:{port}"))?;
    let url = format!("http://{}", listener.local_addr()?);
    tokio::spawn(server::serve(listener, Arc::new(handler)));
    Ok(url)
}

enum Command {
    Call(Envelope, oneshot::Sender<Reply>),
    Peek(Venue, oneshot::Sender<(AccountView, Vec<Order>)>),
    Warm(Venue, String, oneshot::Sender<bool>),
    Resume,
    Save(oneshot::Sender<Result<()>>),
}

/// Private events a connection may fall behind by before it is dropped.
const EVENTS: usize = 1_024;

/// The simulated venues as their protocol servers reach them: requests go through the matching
/// core, private events come out of it, public frames come from the feed hubs.
#[derive(Clone)]
pub struct Venues {
    commands: mpsc::UnboundedSender<Command>,
    /// The core's venue pair, whose slots the pairs here follow.
    pair: [Venue; 2],
    events: [broadcast::Sender<Arc<Event>>; 2],
    hubs: [Arc<Mutex<Hub>>; 2],
    feed_latency: [Latency; 2],
    seed: u64,
}

impl Venues {
    /// Runs `core` on this host's wall clock, fed by `feed`, until every handle is dropped,
    /// saving it and reporting its diagnostics to `files`.
    pub fn start(
        core: Exchange,
        feed: mpsc::UnboundedReceiver<Input>,
        hubs: [Arc<Mutex<Hub>>; 2],
        feed_latency: [Latency; 2],
        seed: u64,
        files: Option<SimFiles>,
    ) -> Self {
        let ((commands, rx), pair) = (mpsc::unbounded_channel(), core.venues());
        let events = [broadcast::channel(EVENTS).0, broadcast::channel(EVENTS).0];
        tokio::spawn(drive(core, feed, rx, events.clone(), files));
        Self { commands, pair, events, hubs, feed_latency, seed }
    }

    /// Runs the deadmen a restored state armed; call once the books are warm.
    pub fn resume(&self) {
        let _ = self.commands.send(Command::Resume);
    }

    /// Saves the venues' state now, durably, and appends the diagnostics of the last window.
    pub async fn save(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.commands.send(Command::Save(tx));
        rx.await.context("the simulated venues stopped")?
    }

    /// Sends one request now; its reply comes back after the venue's round trip.
    pub async fn call(&self, envelope: Envelope) -> Reply {
        let (tx, rx) = oneshot::channel();
        let _ = self.commands.send(Command::Call(envelope, tx));
        rx.await.unwrap_or(Reply::Reject(Reject::Unavailable))
    }

    /// The venue's account and open orders now: what a private stream sends on subscription.
    pub async fn peek(&self, venue: Venue) -> Option<(AccountView, Vec<Order>)> {
        let (tx, rx) = oneshot::channel();
        let _ = self.commands.send(Command::Peek(venue, tx));
        rx.await.ok()
    }

    /// The venue's private events from now on.
    pub fn events(&self, venue: Venue) -> broadcast::Receiver<Arc<Event>> {
        self.events[venue.ix(self.pair)].subscribe()
    }

    /// Connection `lane`'s view of the venue's public streams.
    pub fn follower(&self, venue: Venue, lane: u64, combined: bool) -> Follower {
        let v = venue.ix(self.pair);
        Follower::new(self.hubs[v].clone(), self.feed_latency[v], self.seed ^ lane.rotate_left(32), combined)
    }

    /// Waits until each market's replica holds a book (the feed and the requests reach the core
    /// on separate channels). Asks the core directly: a simulated request would count in the
    /// diagnostics as the bot's.
    pub async fn warm(&self, markets: &[(Venue, &str)]) {
        for &(venue, market) in markets {
            loop {
                let (tx, rx) = oneshot::channel();
                let _ = self.commands.send(Command::Warm(venue, market.to_string(), tx));
                if rx.await.unwrap_or(false) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn drive(
    mut core: Exchange,
    mut feed: mpsc::UnboundedReceiver<Input>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: [broadcast::Sender<Arc<Event>>; 2],
    mut files: Option<SimFiles>,
) {
    let mut waiting: HashMap<u64, oneshot::Sender<Reply>> = HashMap::new();
    let mut out = Vec::new();
    let mut feeding = true;
    let every = |period| tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    let (mut save, mut report) = (every(SAVE_EVERY), every(REPORT_EVERY));
    loop {
        let wait = core.next_due().map(|due| Duration::from_micros((due - wall_us()).max(0) as u64));
        tokio::select! {
            input = feed.recv(), if feeding => match input {
                Some(input) => {
                    core.advance(wall_us(), &mut out);
                    match input {
                        Input::Frame { venue, market, exch_us, event } => core.ingest(venue, &market, exch_us, event),
                        Input::Funding { venue, market, exch_us, rate } => core.funding(venue, &market, exch_us, rate),
                    }
                }
                None => feeding = false,
            },
            command = commands.recv() => {
                let Some(command) = command else { return };
                core.advance(wall_us(), &mut out);
                match command {
                    Command::Call(envelope, reply) => {
                        waiting.insert(core.submit(envelope), reply);
                    }
                    Command::Peek(venue, reply) => {
                        let _ = reply.send(core.peek(venue));
                    }
                    Command::Warm(venue, market, reply) => {
                        let _ = reply.send(core.replica(venue, &market).is_some_and(|book| book.warm()));
                    }
                    Command::Resume => core.resume(),
                    Command::Save(reply) => {
                        let _ = reply.send(files.as_mut().map_or(Ok(()), |files| files.save(&core).and_then(|()| files.report(&mut core))));
                    }
                }
            }
            _ = tokio::time::sleep(wait.unwrap_or_default()), if wait.is_some() => {}
            _ = save.tick() => {
                if let Some(Err(error)) = files.as_mut().map(|files| files.save(&core)) {
                    tracing::warn!("dry run: saving the simulated venues: {error:#}");
                }
            }
            _ = report.tick() => {
                if let Some(Err(error)) = files.as_mut().map(|files| files.report(&mut core)) {
                    tracing::warn!("dry run: writing the diagnostics: {error:#}");
                }
            }
        }
        core.advance(wall_us(), &mut out);
        for output in out.drain(..) {
            match output {
                Output::Reply { ticket, reply } => {
                    if let Some(tx) = waiting.remove(&ticket) {
                        let _ = tx.send(reply);
                    }
                }
                Output::Event { venue, event } => {
                    let _ = events[venue.ix(core.venues())].send(Arc::new(event));
                }
            }
        }
    }
}

/// The venues answering the bot's own clients on loopback.
#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Instant;

    use futures_util::StreamExt;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::Value;
    use tokio_util::sync::CancellationToken;

    use super::aster::Aster;
    use super::feed::{aster_frame, aster_streams, forward, hyperliquid_frame, lighter_frame, Frame};
    use super::lighter::Lighter;
    use super::book::BookUpdate;
    use super::matching::{Fees, FeedEvent, Filters, SimParams};
    use super::*;
    use crate::connectors::rest_book::fetch_lighter_book_from_base;
    use crate::connectors::rest_specs::fetch_lighter_meta_from_base;
    use crate::hotpath::clock::mono_now_ns;
    use crate::lighter::messages::{OrderBooksResponse, TradePayload};
    use crate::lighter::rest::RestClient;
    use crate::lighter::signer::{self as native, Signer};
    use crate::lighter::tx_ws::TxWebSocket;
    use crate::lighter::ws::{stream_url, subscribe_loop, SubscribeOptions};
    use crate::livebot::exec::aster::{AsterRest, CancelOutcome};
    use crate::livebot::exec::creds::{AsterCreds, LighterCreds};
    use crate::livebot::exec::sign::{AsterSigner, EvmAsterSigner};
    use crate::livebot::exec::command::ExecEvent;
    use crate::livebot::scale::MarketScale;
    use crate::livebot::userstream::{run_aster_user_stream, StreamLiveness};
    use crate::types::{MarketId, Side, TxSendStatus};

    /// Frames are stamped one shift in the past, so the core applies them at once.
    const SHIFT_MS: i64 = 50;
    /// Lighter's `orderBooks`, cut to HYPE (minimum sizes as live on 2026-09-24).
    const ORDER_BOOKS: &str = r#"{"code":200,"order_books":[{"symbol":"HYPE","market_id":24,"status":"active","taker_fee":"0.0000","maker_fee":"0.0000","min_base_amount":"0.07","min_quote_amount":"10.000000","supported_size_decimals":2,"supported_price_decimals":4,"supported_quote_decimals":6}]}"#;
    /// Hyperliquid's `meta`, cut to BTC and HYPE (HYPE is asset 159 live).
    const HYPERLIQUID_META: &str = r#"{"universe":[{"name":"BTC","szDecimals":5},{"name":"HYPE","szDecimals":2}]}"#;
    /// Aster's `exchangeInfo` in the documented v3 shape, cut to HYPEUSDT.
    const ASTER_EXCHANGE_INFO: &str = r#"{"timezone":"UTC","serverTime":1790235498000,"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400},{"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1200}],"assets":[],"symbols":[{"symbol":"HYPEUSDT","pair":"HYPEUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"HYPE","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":3,"quantityPrecision":2,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.001","maxPrice":"100000","tickSize":"0.001"},{"filterType":"LOT_SIZE","stepSize":"0.01","maxQty":"100000","minQty":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"},{"filterType":"PERCENT_PRICE","multiplierUp":"1.0500","multiplierDown":"0.9500","multiplierDecimal":4}],"orderTypes":["LIMIT","MARKET"],"timeInForce":["GTC","IOC","FOK","GTX","HIDDEN"]}]}"#;

    /// Two simulated venues on loopback standing in for mainnet: HYPE quoted 99 / 101 with 5 on
    /// each, 1000 of collateral on each, a few milliseconds away.
    pub(crate) struct World {
        /// The first venue's URL: Aster's, or Lighter's against Hyperliquid.
        pub(crate) aster: String,
        /// The second venue's URL: Lighter's, or Hyperliquid's.
        pub(crate) hedge: String,
        pair: [Venue; 2],
        venues: Venues,
        inputs: mpsc::UnboundedSender<Input>,
        hubs: [Arc<Mutex<Hub>>; 2],
        /// The Aster and Hyperliquid tops [`World::keep_fresh`] republishes.
        aster_top: Mutex<(Decimal, Decimal)>,
        hyperliquid_top: Mutex<(Decimal, Decimal)>,
        /// The Lighter book's last nonce.
        lighter_nonce: AtomicI64,
    }

    impl World {
        pub(crate) async fn start() -> Self {
            Self::pair([Venue::Aster, Venue::Lighter]).await
        }

        pub(crate) async fn hedged_on(hedge_venue: Venue) -> Self {
            Self::pair([Venue::Aster, hedge_venue]).await
        }

        pub(crate) async fn pair(pair: [Venue; 2]) -> Self {
            let fixed = |ms: f64| Latency::try_from([ms, ms]).unwrap();
            let params = SimParams {
                shift_us: SHIFT_MS * 1_000,
                seed: 1,
                effect_fraction: 0.9,
                rtt: [fixed(4.0), fixed(2.0)],
                private: [fixed(1.0), fixed(1.0)],
                lighter_taker_delay_us: 300_000,
                hidden_queue_multiplier: dec!(0.5),
                fees: [Fees { maker: dec!(0), taker: dec!(0.0004) }, Fees { maker: dec!(0), taker: dec!(0) }],
                leverage: dec!(1),
                balances: [dec!(1000), dec!(1000)],
                venues: pair,
            };
            let mut core = Exchange::new(params, wall_us());
            core.trust_feed();
            let band = Some((dec!(0.95), dec!(1.05)));
            let aster = Filters { tick: dec!(0.001), step: dec!(0.01), min_qty: dec!(0.01), min_notional: dec!(5), percent_price: band, sig_figs: None };
            let other = Filters { tick: dec!(0.0001), step: dec!(0.01), min_qty: dec!(0.07), min_notional: dec!(10), percent_price: band, sig_figs: None };
            // Each venue's market key, streams and filters.
            let listing = |venue| match venue {
                Venue::Aster => ("HYPEUSDT", aster_streams("HYPEUSDT").to_vec(), aster.clone(), Some(20)),
                Venue::Lighter => ("24", vec!["order_book/24".to_string()], other.clone(), None),
                Venue::Hyperliquid => ("HYPE", vec!["l2Book/HYPE".to_string(), "bbo/HYPE".to_string()], other.clone(), None),
            };
            let listings = pair.map(listing);
            for (venue, (market, _, filters, depth)) in pair.into_iter().zip(&listings) {
                core.add_market(venue, market, *depth, filters.clone());
            }
            let shift = SHIFT_MS * 1_000;
            let hubs = listings.each_ref().map(|(_, streams, ..)| Arc::new(Mutex::new(Hub::new(shift, streams.clone()))));
            let (inputs, feed) = mpsc::unbounded_channel();
            let venues = Venues::start(core, feed, hubs.clone(), [Latency::ZERO; 2], 1, None);
            let mut urls = Vec::new();
            for venue in pair {
                urls.push(match venue {
                    Venue::Aster => serve(0, Aster::new(venues.clone(), ASTER_EXCHANGE_INFO.into(), vec!["HYPEUSDT".into()], dec!(1))).await.unwrap(),
                    Venue::Hyperliquid => serve(0, super::hyperliquid::Hyperliquid::new(venues.clone(), HYPERLIQUID_META, dec!(1)).unwrap()).await.unwrap(),
                    Venue::Lighter => {
                        let markets = serde_json::from_str::<OrderBooksResponse>(ORDER_BOOKS).unwrap().order_books;
                        serve(0, Lighter::new(venues.clone(), ORDER_BOOKS.into(), markets, [dec!(0); 2], dec!(1))).await.unwrap()
                    }
                });
            }
            let [aster, hedge] = <[String; 2]>::try_from(urls).unwrap();
            let top = || Mutex::new((dec!(99), dec!(101)));
            let world = Self { aster, hedge, pair, venues, inputs, hubs, aster_top: top(), hyperliquid_top: top(), lighter_nonce: AtomicI64::new(1) };
            for venue in pair {
                match venue {
                    Venue::Aster => world.aster_book(dec!(99), dec!(101)),
                    Venue::Lighter => world.lighter_book(dec!(99), dec!(101)),
                    Venue::Hyperliquid => world.hyperliquid_book(),
                }
            }
            world.venues.warm(&[(pair[0], listings[0].0), (pair[1], listings[1].0)]).await;
            world
        }

        /// Hyperliquid's l2Book and bbo at its top, 5 on each side, as the venue pushes them.
        fn hyperliquid_book(&self) {
            let (time, (bid, ask)) = (Self::due_ms(), *self.hyperliquid_top.lock().unwrap());
            let level = |px: Decimal| json!({"px": px.to_string(), "sz": "5", "n": 1});
            for (channel, data) in [
                ("l2Book", json!({"coin": "HYPE", "time": time, "levels": [[level(bid)], [level(ask)]]})),
                ("bbo", json!({"coin": "HYPE", "time": time, "bbo": [level(bid), level(ask)]})),
            ] {
                let text = json!({"channel": channel, "data": data}).to_string();
                self.publish(hyperliquid_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
            }
        }

        /// Moves the Hyperliquid top; [`World::keep_fresh`] repeats it.
        pub(crate) fn set_hyperliquid(&self, bid: Decimal, ask: Decimal) {
            *self.hyperliquid_top.lock().unwrap() = (bid, ask);
            self.hyperliquid_book();
        }

        /// As the upstream does: to the core and to the bot's streams.
        fn publish(&self, frame: Frame) {
            let hub = &self.hubs[frame.venue.ix(self.pair)];
            forward(frame, hub, &self.inputs);
        }

        /// One shift ago in venue milliseconds: due now.
        fn due_ms() -> i64 {
            wall_us() / 1_000 - SHIFT_MS
        }

        pub(crate) fn aster_book(&self, bid: Decimal, ask: Decimal) {
            let t = Self::due_ms();
            let text = format!(r#"{{"stream":"hypeusdt@depth20@100ms","data":{{"e":"depthUpdate","E":{t},"T":{t},"s":"HYPEUSDT","U":1,"u":2,"pu":0,"b":[["{bid}","5"]],"a":[["{ask}","5"]]}}}}"#);
            self.publish(aster_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
        }

        /// A print whose aggressor was on the `taker` side.
        pub(crate) fn aster_print(&self, price: Decimal, qty: Decimal, taker: Side) {
            let (t, buyer_made) = (Self::due_ms(), taker == Side::Sell);
            let text = format!(r#"{{"stream":"hypeusdt@aggTrade","data":{{"e":"aggTrade","E":{t},"a":1,"s":"HYPEUSDT","p":"{price}","q":"{qty}","f":1,"l":1,"T":{t},"m":{buyer_made}}}}}"#);
            self.publish(aster_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
        }

        fn lighter_book(&self, bid: Decimal, ask: Decimal) {
            let t = Self::due_ms();
            let us = t * 1_000;
            let text = format!(r#"{{"channel":"order_book:24","last_updated_at":{us},"offset":10,"order_book":{{"code":0,"asks":[{{"price":"{ask}","size":"5"}}],"bids":[{{"price":"{bid}","size":"5"}}],"offset":10,"nonce":1,"last_updated_at":{us},"begin_nonce":0}},"timestamp":{t},"type":"subscribed/order_book"}}"#);
            self.publish(lighter_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
        }

        /// A Lighter update that moves no level: it continues the nonce chain and the clock.
        fn lighter_tick(&self) {
            let begin = self.lighter_nonce.fetch_add(1, Ordering::Relaxed);
            let (nonce, t) = (begin + 1, Self::due_ms());
            let (us, offset) = (t * 1_000, nonce * 10);
            let text = format!(r#"{{"channel":"order_book:24","last_updated_at":{us},"offset":{offset},"order_book":{{"code":0,"asks":[],"bids":[],"offset":{offset},"nonce":{nonce},"last_updated_at":{us},"begin_nonce":{begin}}},"timestamp":{t},"type":"update/order_book"}}"#);
            self.publish(lighter_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
        }

        /// Moves the Aster top; [`World::keep_fresh`] repeats it.
        pub(crate) fn set_aster(&self, bid: Decimal, ask: Decimal) {
            *self.aster_top.lock().unwrap() = (bid, ask);
            self.aster_book(bid, ask);
        }

        /// Publishes both books every 100 ms, as the venues' streams tick: the bot's staleness
        /// guards expect it.
        pub(crate) fn keep_fresh(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
            let world = self.clone();
            tokio::spawn(async move {
                loop {
                    for venue in world.pair {
                        match venue {
                            Venue::Aster => {
                                let (bid, ask) = *world.aster_top.lock().unwrap();
                                world.aster_book(bid, ask);
                            }
                            Venue::Lighter => world.lighter_tick(),
                            Venue::Hyperliquid => world.hyperliquid_book(),
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        }
    }

    /// The shipped bot.toml with `market` standing in for mainnet, and fast fixed latencies.
    pub(crate) fn shipped_config(market: &World, dir: &Path) -> BotConfig {
        std::fs::write(dir.join("bot.toml"), include_str!("../../bot.toml")).unwrap();
        let mut cfg = BotConfig::load(&dir.join("bot.toml")).unwrap();
        let live = &mut cfg.maker.live;
        for (venue, url) in market.pair.into_iter().zip([&market.aster, &market.hedge]) {
            match venue {
                Venue::Aster => live.aster.base_url = url.clone(),
                Venue::Lighter => live.lighter.base_url = url.clone(),
                Venue::Hyperliquid => live.hyperliquid.base_url = url.clone(),
            }
        }
        let fixed = |ms: f64| Latency::try_from([ms, ms]).unwrap();
        cfg.dry_run = Some(DryRunCfg {
            shift_ms: 300,
            seed: 1,
            aster_port: 0,
            lighter_port: 0,
            hyperliquid_port: 0,
            aster_balance_usdt: dec!(200),
            lighter_balance_usdc: dec!(200),
            effect_fraction: 0.9,
            aster_rest_rtt_ms: fixed(5.0),
            lighter_rtt_ms: fixed(3.0),
            lighter_taker_delay_ms: 300,
            aster_feed_ms: fixed(1.0),
            lighter_feed_ms: fixed(1.0),
            aster_user_stream_ms: fixed(2.0),
            lighter_account_ms: fixed(2.0),
            hyperliquid_rtt_ms: fixed(3.0),
            hyperliquid_feed_ms: fixed(1.0),
            hidden_queue_multiplier: dec!(0.5),
        });
        cfg
    }

    pub(crate) fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_leaves_both_engines_pointed_at_the_simulated_venues_only() {
        let market = Arc::new(World::start().await);
        let fresh = market.keep_fresh();
        let dir = temp_dir("dry-run-start");
        let mut cfg = shipped_config(&market, &dir);
        let (dry, runs) = (cfg.dry_run.clone().unwrap(), dir.join("dry-run"));
        let (taker_markets, _) = cfg.select("HYPE").unwrap();
        start(&dry, &mut cfg, &taker_markets[0], &runs).await.unwrap();
        fresh.abort();
        let engines = format!("{:?} {:?}", cfg.maker, cfg.taker);
        for venue in ["asterdex", "zklighter", "hyperliquid.xyz", &format!("\"{}\"", market.aster), &format!("\"{}\"", market.hedge)] {
            assert!(!engines.contains(venue), "{venue} is still in the engines' config");
        }
        assert!(cfg.maker.live.dry_run && cfg.taker.venues.dry_run, "both engines sign with the dry-run identity");
        assert_eq!(Path::new(&cfg.taker.pnl.persist_dir), runs);
        let lighter = fetch_lighter_meta_from_base(&reqwest::Client::new(), &cfg.taker.venues.lighter_base_url).await.unwrap();
        assert_eq!(lighter["HYPE"].market_id, 24, "the simulated venue lists the real instruments");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_dry_run_saves_its_venues_and_reports_each_window() {
        let dir = temp_dir("dry-run-files");
        std::fs::write(dir.join("bot.toml"), include_str!("../../bot.toml")).unwrap();
        let dry = BotConfig::load(&dir.join("bot.toml")).unwrap().dry_run.unwrap();
        let mut files = SimFiles::new(&dir, "hype");
        assert!(files.load().unwrap().is_none(), "a first run starts afresh");
        let fees = [Fees { maker: dec!(0), taker: dec!(0.0004) }, Fees { maker: dec!(0), taker: dec!(0) }];
        let mut core = Exchange::new(dry.sim_params(fees, dec!(1), [Venue::Aster, Venue::Lighter]), wall_us());
        core.add_market(Venue::Aster, "HYPEUSDT", Some(20), Filters::default());
        let top = BookUpdate::Top { bid: (dec!(99), dec!(1)), ask: (dec!(101), dec!(1)) };
        core.ingest(Venue::Aster, "HYPEUSDT", core.now() - 30_000, FeedEvent::Book(top));
        files.save(&core).unwrap();
        let saved = files.load().unwrap().expect("the saved state");
        assert_eq!(serde_json::to_value(&saved).unwrap(), serde_json::to_value(core.state()).unwrap());
        files.report(&mut core).unwrap();
        let text = std::fs::read_to_string(dir.join("sim-HYPE.diag.jsonl")).unwrap();
        let row: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(row["aster"]["lag_ms"]["top"], json!({"n": 1, "p50": 30.0, "p90": 30.0, "p99": 30.0, "max": 30.0}));
        assert_eq!(row["lighter"]["account"]["balance"], "200");
        assert_eq!(core.diag[0].frames, 0, "each report starts a new window");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The XEMM engine end to end on the dry-run venues: its resting Aster bid is filled by a sweep
    /// and hedged by a Lighter IOC, and the journal reports one complete trade.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn xemm_quotes_is_filled_and_hedges_on_the_dry_run_venues() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let market = Arc::new(World::start().await);
        let fresh = market.keep_fresh();
        let dir = temp_dir("dry-run-xemm");
        let mut cfg = shipped_config(&market, &dir);
        let (dry, runs) = (cfg.dry_run.clone().unwrap(), dir.join("dry-run"));
        let (taker_markets, maker_markets) = cfg.select("HYPE").unwrap();
        start(&dry, &mut cfg, &taker_markets[0], &runs).await.unwrap();
        let (stop, stem) = (CancellationToken::new(), runs.join("bot-HYPE"));
        let xemm = tokio::spawn({
            let (cfg, stop, stem) = (cfg.maker.clone(), stop.clone(), stem.clone());
            async move { crate::livebot::run(&cfg, maker_markets, stem, Default::default(), None, Default::default(), stop).await }
        });
        // A taker waiting for a lease runs beside XEMM on the same account, as under `run`.
        let (_no_lease, lease) = tokio::sync::watch::channel(None);
        let (observer_stop, options) = (CancellationToken::new(), crate::taker::arb::RunOptions { lease: Some(lease), ..Default::default() });
        let observer = tokio::spawn(crate::taker::arb::run(cfg.taker.clone(), taker_markets.clone(), options, observer_stop.clone()));
        let journal = crate::live_report::inferred_journal_path(&stem);
        // Sells print through any bid the bot can quote (its edge keeps it under 99) until one fills.
        let trade = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                market.aster_print(dec!(97.5), dec!(1), Side::Sell);
                tokio::time::sleep(Duration::from_millis(200)).await;
                let trades = crate::live_report::summarize_path(&journal, None, None).map(|s| s.trades).unwrap_or_default();
                if let Some(trade) = trades.into_iter().find(|t| t.lighter_qty > Decimal::ZERO) {
                    break trade;
                }
            }
        })
        .await
        .expect("no hedged maker fill within 60 s");
        assert_eq!((trade.economic_status, trade.hedge_side), ("confirmed", Some(Side::Sell)), "{trade:?}");
        assert!(trade.qty > Decimal::ZERO && trade.lighter_qty == trade.qty && trade.residual_qty.is_zero(), "{trade:?}");
        assert!(trade.aster_px.is_some_and(|px| px < dec!(99)), "the maker fill is at the bot's own bid: {trade:?}");
        assert_eq!(trade.lighter_px, Some(dec!(99)), "the hedge took the Lighter bid: {trade:?}");
        assert!(trade.last_mono_ns - trade.first_mono_ns >= 300_000_000, "the hedge waited out the taker delay: {trade:?}");
        // The waiting taker stops first, while XEMM still quotes: having sent nothing, it stops
        // cleanly whatever rests on the account.
        let rest = xemm_aster(&cfg.maker.live.aster.base_url);
        tokio::time::timeout(Duration::from_secs(10), async {
            while rest.open_orders(None).await.unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("XEMM quotes again");
        observer_stop.cancel();
        tokio::time::timeout(Duration::from_secs(60), observer).await.expect("the standby hung").unwrap().expect("a clean standby stop");
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(60), xemm).await.expect("the drain hung").unwrap().expect("a clean stop");
        fresh.abort();
        let summary = crate::live_report::summarize_path(&journal, None, None).unwrap();
        assert_eq!((summary.trades.len(), summary.unmatched_fills, summary.qty_mismatches), (1, 0, 0), "{:?}", summary.trades);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The same with the hedge on Hyperliquid: the Aster fill is hedged by an IOC at the
    /// Hyperliquid bid, whose fee and trade id reach the journal from the venue's fills.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn xemm_quotes_is_filled_and_hedges_on_hyperliquid() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let market = Arc::new(World::hedged_on(Venue::Hyperliquid).await);
        let fresh = market.keep_fresh();
        let dir = temp_dir("dry-run-xemm-hl");
        let mut cfg = shipped_config(&market, &dir);
        let (dry, runs) = (cfg.dry_run.clone().unwrap(), dir.join("dry-run"));
        let (taker_markets, maker_markets) = cfg.select("HYPE-HL").unwrap();
        start(&dry, &mut cfg, &taker_markets[0], &runs).await.unwrap();
        assert_ne!(cfg.maker.live.hyperliquid.base_url, market.hedge, "the engine trades on the simulation, not its upstream");
        let (stop, stem, account) = (CancellationToken::new(), runs.join("bot-HYPE-HL"), crate::livebot::account::AccountState::default());
        let xemm = tokio::spawn({
            let (cfg, stop, stem, account) = (cfg.maker.clone(), stop.clone(), stem.clone(), account.clone());
            async move { crate::livebot::run(&cfg, maker_markets, stem, Default::default(), None, account, stop).await }
        });
        let journal = crate::live_report::inferred_journal_path(&stem);
        let trade = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                market.aster_print(dec!(97.5), dec!(1), Side::Sell);
                tokio::time::sleep(Duration::from_millis(200)).await;
                let trades = crate::live_report::summarize_path(&journal, None, None).map(|s| s.trades).unwrap_or_default();
                if let Some(trade) = trades.into_iter().find(|t| t.lighter_qty > Decimal::ZERO) {
                    break trade;
                }
            }
        })
        .await
        .expect("no hedged maker fill within 60 s");
        assert_eq!((trade.economic_status, trade.hedge_side), ("confirmed", Some(Side::Sell)), "{trade:?}");
        assert!(trade.qty > Decimal::ZERO && trade.lighter_qty == trade.qty && trade.residual_qty.is_zero(), "{trade:?}");
        assert_eq!(trade.lighter_px, Some(dec!(99)), "the hedge took the Hyperliquid bid: {trade:?}");
        // 18.5 bps of edge and the 4.5 bps Hyperliquid fee under the 99 bid: 98.77, where the Lighter
        // fee (0) would allow 98.82.
        assert!(trade.aster_px.is_some_and(|px| px < dec!(98.79)), "the quote pays for the hedge fee: {trade:?}");
        // Hyperliquid's accountValue already carries the hedge's uPnL: none is added on top.
        let snapshot = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = account.load();
                if !snapshot.hl_positions.is_empty() {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the reconciler sees the hedge");
        assert!(snapshot.hl_upnl_marked && snapshot.hl_unrealized_usd.is_zero(), "{snapshot:?}");
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(60), xemm).await.expect("the drain hung").unwrap().expect("a clean stop");
        fresh.abort();
        let summary = crate::live_report::summarize_path(&journal, None, None).unwrap();
        assert_eq!((summary.trades.len(), summary.unmatched_fills, summary.qty_mismatches), (1, 0, 0), "{:?}", summary.trades);
        // The hedge's own trade row, identified by the venue's trade id and priced with its fee.
        let text = std::fs::read_to_string(&journal).unwrap();
        let statuses: Vec<_> = text.lines().filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|row| row["kind"] == "execution_trade").map(|row| row["economic_status"].clone()).collect();
        assert!(!statuses.is_empty() && statuses.iter().all(|s| s == "confirmed"), "{statuses:?}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    pub(crate) fn aster_signer() -> Arc<dyn AsterSigner> {
        let creds = AsterCreds::dry_run();
        Arc::new(EvmAsterSigner::new(creds.user, creds.signer, creds.key).unwrap())
    }

    fn xemm_aster(url: &str) -> AsterRest {
        let scale = MarketScale { tick: dec!(0.001), step: dec!(0.01), hl_qty_step: dec!(0.01) };
        let markets = HashMap::from([(MarketId::from("HYPE"), (scale, "HYPEUSDT".to_string()))]);
        AsterRest::new(url.to_string(), aster_signer(), markets, 30_000, 1_000, 1_000).unwrap()
    }

    /// A subscription's frames, taken by type in whatever order they came.
    struct Frames(mpsc::UnboundedReceiver<String>, Vec<Value>);

    impl Frames {
        async fn take(&mut self, kind: &str) -> Value {
            loop {
                if let Some(i) = self.1.iter().position(|v| v["type"] == kind) {
                    return self.1.remove(i);
                }
                let text = tokio::time::timeout(Duration::from_secs(5), self.0.recv()).await
                    .unwrap_or_else(|_| panic!("no {kind} in time; got {:?}", self.1))
                    .expect("the subscription ended");
                self.1.push(serde_json::from_str(&text).unwrap());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aster_answers_the_bots_own_clients() {
        let world = World::start().await;
        let hype = MarketId::from("HYPE");
        let rest = xemm_aster(&world.aster);
        // The live startup gates.
        assert!(rest.is_one_way().await.unwrap());
        assert_eq!(rest.get_leverage(&hype).await.unwrap(), 1);
        assert_eq!(rest.account_available_balance().await.unwrap(), dec!(1000));
        assert!(rest.open_orders(None).await.unwrap().is_empty());

        // A quote rests, shows and cancels once; a post-only order that would take expires.
        let placed = rest.place(&hype, Side::Buy, 98_500, 10, "q1", false).await;
        assert!(matches!(&placed, ExecEvent::PlaceAck { client_id, .. } if client_id == "q1"), "{placed:?}");
        assert_eq!(rest.open_orders(Some(&hype)).await.unwrap().len(), 1);
        assert_eq!(rest.query_order(&hype, "q1").await.unwrap()["status"], "NEW");
        assert_eq!(rest.cancel_order(&hype, "q1").await.unwrap(), CancelOutcome::Canceled);
        assert_eq!(rest.cancel_order(&hype, "q1").await.unwrap(), CancelOutcome::AlreadyGone);
        let crossing = rest.place(&hype, Side::Buy, 101_000, 10, "q2", false).await;
        assert!(matches!(&crossing, ExecEvent::PlaceReject { reason, .. } if reason.contains("EXPIRED")), "{crossing:?}");

        // A print through a resting quote fills it, and the user stream reports the fill.
        let (fills_tx, mut fills) = tokio::sync::mpsc::channel(8);
        let liveness = Arc::new(StreamLiveness::default());
        let shutdown = CancellationToken::new();
        let symbols = HashMap::from([("HYPEUSDT".to_string(), hype.clone())]);
        tokio::spawn(run_aster_user_stream(xemm_aster(&world.aster), symbols, fills_tx, liveness.clone(), shutdown.clone()));
        while liveness.age_ms(mono_now_ns()) == i64::MAX {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let placed = rest.place(&hype, Side::Buy, 99_000, 10, "q3", false).await;
        assert!(matches!(placed, ExecEvent::PlaceAck { .. }), "{placed:?}");
        world.aster_print(dec!(98.9), dec!(20), Side::Sell);
        let fill = tokio::time::timeout(Duration::from_secs(5), fills.recv()).await.unwrap().unwrap();
        assert_eq!((fill.client_id.as_str(), fill.aster_side, fill.last_fill_qty, fill.last_fill_px), ("q3", Side::Buy, dec!(0.1), dec!(99)));
        assert_eq!(fill.usd_fee(), Some(Decimal::ZERO), "a free maker fill carries no commission fields: none paid");
        shutdown.cancel();

        // Public streams, combined and raw, with the venue's timestamps moved by the shift.
        for path in ["stream?streams=hypeusdt@depth20@100ms", "ws/hypeusdt@depth20@100ms"] {
            let url = format!("{}/{path}", world.aster.replacen("http", "ws", 1));
            let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
            // Only frames published after the subscription reaches the venue come through.
            let (text, due) = loop {
                let due = World::due_ms();
                world.aster_book(dec!(99), dec!(101));
                if let Ok(Some(Ok(message))) = tokio::time::timeout(Duration::from_millis(50), ws.next()).await {
                    break (message.into_text().unwrap(), due);
                }
            };
            let frame: Value = serde_json::from_str(&text).unwrap();
            let data = if path.starts_with("stream") { &frame["data"] } else { &frame };
            if path.starts_with("stream") {
                assert_eq!(frame["stream"], "hypeusdt@depth20@100ms");
            }
            assert_eq!((data["b"][0][0].as_str(), data["a"][0][0].as_str()), (Some("99"), Some("101")));
            assert!(data["T"].as_i64().unwrap() >= due + SHIFT_MS, "{data}");
        }
    }

    // One worker: the library's blocking key check must leave the runtime free to answer it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn lighter_answers_the_bots_own_clients() {
        let dir = Path::new("signers");
        if !dir.join(native::signer_filename()).exists() {
            eprintln!("skipped: no signer library for this platform");
            return;
        }
        let _native = native::NATIVE.lock().unwrap_or_else(|e| e.into_inner());
        let world = World::start().await;
        let creds = LighterCreds::dry_run();
        let (account, key) = (creds.account_index, creds.api_key_index);
        let signer = Signer::load(dir, &world.hedge, &creds.api_private_key, key, account).unwrap();
        // The library's own key check, as at live startup: on the only worker, as an engine
        // task, so the simulated venue can answer only if the check leaves the worker free.
        let signer = tokio::spawn(async move { signer.check_client(key).map(|()| signer) }).await.unwrap().unwrap();

        let rest = RestClient::new(&world.hedge, 0).unwrap();
        let http = reqwest::Client::new();
        let meta = &fetch_lighter_meta_from_base(&http, &world.hedge).await.unwrap()["HYPE"];
        assert_eq!((meta.market_id, meta.size_decimals, meta.price_decimals), (24, 2, 4));
        assert_eq!(rest.next_nonce(account, key).await.unwrap(), 0);
        let position = |raw: Value| {
            let row = raw["accounts"][0]["positions"].as_array().unwrap().iter().find(|p| p["market_id"] == 24).unwrap().clone();
            row["position"].as_str().unwrap().parse::<Decimal>().unwrap() * Decimal::from(row["sign"].as_i64().unwrap())
        };
        assert_eq!(position(rest.account_raw(account).await.unwrap()), Decimal::ZERO);
        let top = fetch_lighter_book_from_base(&http, &world.hedge, 24, 1).await.unwrap();
        assert_eq!((top.bids[0].px, top.asks[0].px), (dec!(99), dec!(101)));

        let url = stream_url(&world.hedge);
        let (frames_tx, frames_rx) = mpsc::unbounded_channel();
        let channels = vec!["order_book/24".to_string(), format!("account_all/{account}"), format!("account_all_orders/{account}")];
        let mut options = SubscribeOptions::new(&url, "dry-run test", channels);
        options.channel_auths = options.channels.iter().map(|c| (c.clone(), "token".to_string())).collect();
        let stream = tokio::spawn(subscribe_loop(options, None, move |f| {
            let _ = frames_tx.send(f.raw.to_string());
        }, || {}));
        let mut frames = Frames(frames_rx, Vec::new());
        let book = frames.take("subscribed/order_book").await;
        assert_eq!(book["order_book"]["bids"][0]["price"], "99");
        frames.take("subscribed/account_all_orders").await;

        let tx = TxWebSocket::new(&url);
        tx.connect().await.unwrap();
        let order = |client: i64, price: i32, is_ask: bool, tif: i32, nonce: i64| {
            let expiry = if tif == native::TIF_IMMEDIATE_OR_CANCEL { native::DEFAULT_IOC_EXPIRY } else { native::DEFAULT_28_DAY_ORDER_EXPIRY };
            let signed = signer
                .sign_create_order(24, client, 50, price, is_ask, native::ORDER_TYPE_LIMIT, tif, false,
                    native::NIL_TRIGGER_PRICE, expiry, nonce, key)
                .unwrap();
            ([signed.tx_type], [signed.tx_info])
        };
        // A quote: the sequencer accepts it, then the account stream and REST show it resting.
        let (types, infos) = order(7, 985_000, false, native::TIF_POST_ONLY, 0);
        let sent = tx.send_batch(&types, &infos).await;
        assert_eq!((sent.status, sent.code), (TxSendStatus::Ok, 0), "{}", sent.message);
        let orders = frames.take("update/account_all_orders").await;
        assert_eq!(orders["orders"]["24"][0]["client_order_index"], 7);
        let active = rest.account_active_orders(account, 24, "token").await.unwrap();
        assert_eq!(active.iter().map(|o| o.client_order_index).collect::<Vec<_>>(), [Some(7)]);
        // A reused nonce is refused before anything executes.
        let (types, infos) = order(8, 985_000, false, native::TIF_POST_ONLY, 0);
        let refused = tx.send_batch(&types, &infos).await;
        assert_eq!((refused.status, refused.code), (TxSendStatus::Rejected, 21104));
        // A taker order executes after the Standard account's delay; its trade reaches the stream.
        let (types, infos) = order(9, 980_000, true, native::TIF_IMMEDIATE_OR_CANCEL, 1);
        let sent_at = Instant::now();
        assert_eq!(tx.send_batch(&types, &infos).await.status, TxSendStatus::Ok);
        let update = frames.take("update/account_all").await;
        assert!(sent_at.elapsed() >= Duration::from_millis(300), "filled {:?} after sending", sent_at.elapsed());
        let trades: Vec<TradePayload> = serde_json::from_value(update["trades"]["24"].clone()).unwrap();
        assert_eq!((trades[0].ask_client_id, trades[0].ask_account_id, trades[0].is_maker_ask), (Some(9), Some(account), Some(false)));
        assert_eq!(trades[0].size.as_deref().map(|s| s.parse::<Decimal>().unwrap()), Some(dec!(0.5)));
        assert_eq!(position(rest.account_raw(account).await.unwrap()), dec!(-0.5));
        stream.abort();
    }
}

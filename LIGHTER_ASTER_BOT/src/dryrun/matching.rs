//! The simulated venues: a deterministic discrete-event machine on this host's wall clock (µs).
//!
//! Inputs are feed events stamped with exchange time T, applied at T + D (the shift), and
//! requests sent now, which reach the venue at now + f·RTT and are answered at now + RTT.
//! Outputs (replies and private-stream events) come out of `advance` when due. Nothing here
//! reads a clock or does I/O: a run is a pure function of its inputs and seed.
//!
//! Matching, pessimistic where the data cannot decide:
//! - A taker order fills against the worse (per level) of the two book states around its
//!   effect time, less what we took before. The later state is in hand because matching runs
//!   D behind reality ([`LOOKAHEAD_US`]).
//! - A post-only order expires only if it crosses the book it arrives at. If the book crosses it
//!   later, it rests and the crossing size trades against it (the adverse outcome; expiring
//!   it instead would be the kind one).
//! - A resting order has the visible size at its price ahead of it, and h × that size of hidden
//!   orders. A print at its price eats the visible queue, then the hidden one, then fills it; a
//!   book update only shortens the visible queue, to what is left on the level. A print through
//!   its price proves the level emptied (hidden orders too), so it fills it at once. A print at
//!   a better price is another level's business. Our resting orders share each print's size.
//! - If the book crosses a resting order (the market moved through it, or a feed gap hid the
//!   prints), the crossing size fills it at its price.
//! - A venue trades only on a market its feed has shown up to that time. An order, a cancel or
//!   a deadman that falls due first waits, then runs after the frame that catches up: a host or
//!   network stall delivers frames late, and running ahead of them would let a cancel beat the
//!   prints that filled its order. A request still waiting after [`HOLD_US`] is answered as
//!   unavailable. Reads never wait.

use std::collections::{BTreeMap, HashMap, VecDeque};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::account::{Account, AccountView, Working};
use super::book::{BookUpdate, Level, Replica};
use super::clock::{Latency, Rng};
use crate::types::Side;

/// How far past an effect time matching looks for the next book state. The shift minus the
/// feed lag must exceed it for that state to have arrived (the diagnostics report the lag).
pub const LOOKAHEAD_US: i64 = 250_000;

/// How long an order or cancel may wait for its venue's feed before the venue answers it as
/// unavailable (the bot's own timeouts are 5 s and more). The feed has then been silent for the
/// shift minus its lag plus this, which a quiet market does not explain.
pub const HOLD_US: i64 = 2_000_000;

/// Closed orders and fills kept for queries.
const KEEP: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Venue {
    Aster,
    Lighter,
    Hyperliquid,
}

impl Venue {
    /// Its slot in a simulation's per-venue pairs (`SimParams::venues`); only the venues of the
    /// pair are served, so any other is a bug.
    pub fn ix(self, venues: [Venue; 2]) -> usize {
        venues.iter().position(|&v| v == self).unwrap_or_else(|| panic!("{self:?} is not simulated here ({venues:?})"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fees {
    pub maker: Decimal,
    pub taker: Decimal,
}

#[derive(Debug, Clone)]
pub struct SimParams {
    pub shift_us: i64,
    pub seed: u64,
    /// Fraction of a round trip that passes before a request takes effect.
    pub effect_fraction: f64,
    /// Request round trip, per venue (in `venues` order, as every pair below).
    pub rtt: [Latency; 2],
    /// Private-stream delay after an effect.
    pub private: [Latency; 2],
    /// Lighter's speed bump for orders that may take liquidity.
    pub lighter_taker_delay_us: i64,
    /// Hidden size assumed ahead of us, as a multiple of the visible size at our price.
    pub hidden_queue_multiplier: Decimal,
    pub fees: [Fees; 2],
    pub leverage: Decimal,
    pub balances: [Decimal; 2],
    /// The two venues, e.g. `[Aster, Lighter]`.
    pub venues: [Venue; 2],
}

#[derive(Debug, Clone, Default)]
pub struct Filters {
    pub tick: Decimal,
    pub step: Decimal,
    pub min_qty: Decimal,
    pub min_notional: Decimal,
    /// PERCENT_PRICE `(down, up)` multipliers: buys at most mark × up, sells at least
    /// mark × down.
    pub percent_price: Option<(Decimal, Decimal)>,
    /// Hyperliquid: at most this many significant figures in a price, unless it is an integer.
    pub sig_figs: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tif {
    Gtc,
    Ioc,
    PostOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reject {
    /// The replica is cold (upstream gap): the venue cannot match.
    Unavailable,
    RateLimited,
    BadNonce,
    UnknownMarket,
    UnknownOrder,
    TickSize,
    StepSize,
    MinQty,
    MinNotional,
    PriceBand,
    ReduceOnly,
    Margin,
    /// An amend to a price that would take: the order rests unchanged.
    WouldCross,
}

/// Why an order stopped working.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum End {
    Canceled,
    Deadman,
    /// The unfilled rest of an immediate-or-cancel order.
    Ioc,
    PostOnly,
    /// A reduce-only order with no position left to reduce.
    ReduceOnly,
    /// A Lighter transaction the sequencer accepted but execution refused.
    Rejected(Reject),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    New,
    PartiallyFilled,
    Filled,
    Done(End),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub client_id: String,
    pub market: String,
    pub side: Side,
    /// `None` for a market order.
    pub price: Option<Decimal>,
    pub qty: Decimal,
    pub tif: Tif,
    pub reduce_only: bool,
    pub filled: Decimal,
    pub filled_quote: Decimal,
    pub fee: Decimal,
    pub status: Status,
    pub created_us: i64,
    pub updated_us: i64,
    /// Visible queue ahead at our price; `None` while the feed's depth cut hides our level.
    pub ahead: Option<Decimal>,
    /// Hidden size assumed ahead of us besides: only prints take it.
    #[serde(default)]
    pub hidden: Decimal,
}

impl Order {
    pub fn remaining(&self) -> Decimal {
        self.qty - self.filled
    }

    pub fn working(&self) -> bool {
        matches!(self.status, Status::New | Status::PartiallyFilled)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub id: u64,
    pub order_id: u64,
    pub client_id: String,
    pub market: String,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    pub fee: Decimal,
    pub maker: bool,
    pub realized: Decimal,
    pub at_us: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderRef {
    Id(u64),
    Client(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderSpec {
    pub market: String,
    pub client_id: String,
    pub side: Side,
    pub qty: Decimal,
    /// `None` for a market order.
    pub price: Option<Decimal>,
    pub tif: Tif,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    Place(OrderSpec),
    /// Aster's modify of a resting order: its new price and total qty.
    Amend { market: String, order: OrderRef, qty: Decimal, price: Decimal },
    Cancel { market: String, order: OrderRef },
    CancelAll { market: Option<String> },
    /// Aster `countdownCancelAll`: cancels the market's orders unless re-armed in time; 0 disarms.
    Deadman { market: String, countdown_ms: i64 },
    Account,
    OpenOrders { market: Option<String> },
    /// Finished orders, newest first.
    ClosedOrders { market: Option<String> },
    Order { order: OrderRef },
    Fills { market: Option<String> },
    Book { market: String },
    NextNonce { key: u64 },
    /// Accepted without changing anything here (e.g. a leverage update).
    Noop,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub venue: Venue,
    /// The connection it travels on: one connection delivers in order.
    pub lane: u64,
    /// Aster request weight, or 1 per Lighter REST call.
    pub weight: u32,
    /// Aster order count, or 1 per Lighter transaction.
    pub orders: u32,
    /// A Lighter transaction's `(api key, nonce)`. The sequencer answers a transaction when it
    /// accepts it; execution follows, and its outcome only shows on the private streams.
    pub nonce: Option<(u64, i64)>,
    pub request: Request,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Ok,
    Order(Order),
    Orders(Vec<Order>),
    Fills(Vec<Fill>),
    Account(AccountView),
    /// The visible book, read at `at_us`.
    Book { bids: Vec<Level>, asks: Vec<Level>, at_us: i64 },
    Nonce(i64),
    Reject(Reject),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// An order was accepted, filled (with the fill) or finished.
    Order { order: Order, fill: Option<Fill>, account: AccountView },
    Funding { market: String, amount: Decimal, account: AccountView },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Reply { ticket: u64, reply: Reply },
    Event { venue: Venue, event: Event },
}

#[derive(Debug, Clone, PartialEq)]
pub enum FeedEvent {
    Book(BookUpdate),
    /// A print; `taker` is the aggressor's side.
    Trade { price: Decimal, qty: Decimal, taker: Side },
    /// The upstream stream broke: the replica is unusable until its next snapshot.
    Gap,
}

/// Per-venue readouts for the diagnostics log, over one report window.
#[derive(Debug, Clone, Default)]
pub struct Diag {
    pub frames: u64,
    /// Frames that arrived after their shifted time (applied on arrival instead).
    pub late_frames: u64,
    pub stale_frames: u64,
    pub gaps: u64,
    /// Actions that had to wait for the feed to reach their time.
    pub held: u64,
    /// Arrival minus exchange time per stream (`book`, `top`, `trade`), µs: the feed lag the
    /// shift must cover (clock skew shows here too).
    pub lag_us: BTreeMap<&'static str, Vec<i64>>,
    pub requests: u64,
    pub orders: u64,
    pub rejects: BTreeMap<String, u64>,
    /// The round trips and private-stream delays drawn, µs.
    pub rtt_us: Vec<i64>,
    pub private_us: Vec<i64>,
    pub maker_fills: u64,
    pub taker_fills: u64,
    /// The queue ahead of each order that came to rest, and each maker fill's wait since
    /// its order was placed (µs).
    pub queue_ahead: Vec<Decimal>,
    pub maker_wait_us: Vec<i64>,
    pub prints: u64,
    /// Prints the visible book cannot explain: inside the spread, or bigger than the level
    /// they hit. Hidden orders (or a stale book); they calibrate `hidden_queue_multiplier`.
    pub prints_inside_spread: u64,
    pub prints_over_visible: u64,
}

impl Diag {
    /// The window's counts, and quantiles of its samples (times in ms).
    pub fn report(&self) -> Value {
        let ms = |samples: &[i64]| quantiles(samples.iter().map(|&us| us as f64 / 1e3).collect());
        let lag: serde_json::Map<String, Value> = self.lag_us.iter().map(|(stream, s)| (stream.to_string(), ms(s))).collect();
        json!({
            "frames": self.frames,
            "late_frames": self.late_frames,
            "stale_frames": self.stale_frames,
            "gaps": self.gaps,
            "held": self.held,
            "lag_ms": lag,
            "requests": self.requests,
            "orders": self.orders,
            "rejects": self.rejects,
            "rtt_ms": ms(&self.rtt_us),
            "private_ms": ms(&self.private_us),
            "maker_fills": self.maker_fills,
            "taker_fills": self.taker_fills,
            "queue_ahead": quantiles(self.queue_ahead.iter().filter_map(|q| q.to_f64()).collect()),
            "maker_wait_ms": ms(&self.maker_wait_us),
            "prints": self.prints,
            "prints_inside_spread": self.prints_inside_spread,
            "prints_over_visible": self.prints_over_visible,
        })
    }
}

/// `{n, p50, p90, p99, max}` of `samples` by nearest rank, or `{n: 0}`.
pub fn quantiles(mut samples: Vec<f64>) -> Value {
    if samples.is_empty() {
        return json!({"n": 0});
    }
    samples.sort_by(f64::total_cmp);
    let n = samples.len();
    let rank = |q: f64| samples[((q * n as f64).ceil() as usize).clamp(1, n) - 1];
    let round = |x: f64| (x * 100.0).round() / 100.0;
    json!({"n": n, "p50": round(rank(0.5)), "p90": round(rank(0.9)), "p99": round(rank(0.99)), "max": round(samples[n - 1])})
}

/// A request limit over a sliding window (the venues' own windows may be fixed; sliding
/// never admits more).
#[derive(Debug, Clone)]
struct Window {
    span_us: i64,
    cap: u32,
    counts_orders: bool,
    hits: VecDeque<(i64, u32)>,
    used: u32,
}

impl Window {
    fn new(span_s: i64, cap: u32, counts_orders: bool) -> Self {
        Self { span_us: span_s * 1_000_000, cap, counts_orders, hits: VecDeque::new(), used: 0 }
    }
}

fn venue_limits(venue: Venue) -> Vec<Window> {
    match venue {
        // Aster exchangeInfo: REQUEST_WEIGHT 2400/min; ORDERS 1200/min and 300/10 s.
        Venue::Aster => vec![Window::new(60, 2_400, false), Window::new(60, 1_200, true), Window::new(10, 300, true)],
        // Lighter docs, Standard account: 60 REST requests and 60 transactions per minute.
        Venue::Lighter => vec![Window::new(60, 60, false), Window::new(60, 60, true)],
        // Hyperliquid docs: REST weight 1200/min per IP (the per-address action budget, one
        // per USDC traded, is not modelled).
        Venue::Hyperliquid => vec![Window::new(60, 1_200, false)],
    }
}

/// Takes `weight`/`orders` from every window, or from none if any would overflow.
fn admit(windows: &mut [Window], now: i64, weight: u32, orders: u32) -> bool {
    let cost = |w: &Window| if w.counts_orders { orders } else { weight };
    for w in windows.iter_mut() {
        while w.hits.front().is_some_and(|&(at, _)| at <= now - w.span_us) {
            w.used -= w.hits.pop_front().unwrap().1;
        }
    }
    if windows.iter().any(|w| w.used + cost(w) > w.cap) {
        return false;
    }
    for w in windows.iter_mut() {
        let n = cost(w);
        if n > 0 {
            w.hits.push_back((now, n));
            w.used += n;
        }
    }
    true
}

/// One venue's account state: what a restart must keep (the rest rebuilds from the feed).
/// Finished orders and fills are not kept: the bot restarts with the venues and never asks for
/// older history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueState {
    pub account: Account,
    pub open: BTreeMap<u64, Order>,
    #[serde(skip)]
    pub closed: VecDeque<Order>,
    #[serde(skip)]
    pub fills: VecDeque<Fill>,
    pub last_id: u64,
    /// Next expected Lighter nonce per API key.
    pub nonces: BTreeMap<u64, i64>,
    /// Armed Aster deadman per market: its deadline (µs).
    pub deadman: BTreeMap<String, i64>,
    /// Funding rate per market and settlement time (exchange µs); the latest poll wins.
    pub funding: BTreeMap<String, BTreeMap<i64, Decimal>>,
    /// The last settlement applied per market: a rate reported again is not charged twice.
    #[serde(default)]
    pub settled: BTreeMap<String, i64>,
    #[serde(skip)]
    books: BTreeMap<String, Replica>,
    #[serde(skip)]
    filters: BTreeMap<String, Filters>,
    #[serde(skip)]
    marks: BTreeMap<String, Decimal>,
    #[serde(skip)]
    limits: Vec<Window>,
}

impl VenueState {
    fn new(balance: Decimal) -> Self {
        Self {
            account: Account::new(balance),
            open: BTreeMap::new(),
            closed: VecDeque::new(),
            fills: VecDeque::new(),
            last_id: 0,
            nonces: BTreeMap::new(),
            deadman: BTreeMap::new(),
            funding: BTreeMap::new(),
            settled: BTreeMap::new(),
            books: BTreeMap::new(),
            filters: BTreeMap::new(),
            marks: BTreeMap::new(),
            limits: Vec::new(),
        }
    }

    fn next_id(&mut self) -> u64 {
        self.last_id += 1;
        self.last_id
    }

    fn working(&self) -> BTreeMap<String, Working> {
        let mut working: BTreeMap<String, Working> = BTreeMap::new();
        for order in self.open.values() {
            let w = working.entry(order.market.clone()).or_default();
            match order.side {
                Side::Buy => w.buys += order.remaining(),
                Side::Sell => w.sells += order.remaining(),
            }
        }
        working
    }

    fn view(&self, working: &BTreeMap<String, Working>, leverage: Decimal) -> AccountView {
        self.account.view(&|market: &str| self.marks.get(market).copied(), working, leverage)
    }

    /// Whether the margin covers the open orders plus `qty` more working on `side`.
    fn affords(&self, market: &str, side: Side, qty: Decimal, leverage: Decimal) -> bool {
        let mut working = self.working();
        let w = working.entry(market.to_string()).or_default();
        match side {
            Side::Buy => w.buys += qty,
            Side::Sell => w.sells += qty,
        }
        self.view(&working, leverage).available >= Decimal::ZERO
    }
}

/// When a pending event is due: `(at, rank, seq)`.
type Key = (i64, u8, u64);

enum Pending {
    Feed { venue: Venue, market: String, exch_us: i64, event: FeedEvent },
    Funding { venue: Venue, market: String, exch_us: i64 },
    /// A request reaching the venue at `due`.
    Gateway { ticket: u64, due: i64, reply_at: i64, envelope: Envelope },
    /// A Lighter transaction coming out of the speed bump at `due`.
    Execute { venue: Venue, due: i64, request: Request },
    Deadman { venue: Venue, market: String, deadline: i64 },
    /// The end of a held request's wait, if it is still held.
    Expire { venue: Venue, key: Key },
    Deliver(Output),
}

impl Pending {
    /// The venue whose market a trading action must have seen up to its time, and that time.
    fn acts_at(&self) -> Option<(Venue, i64)> {
        match *self {
            Pending::Gateway { due, ref envelope, .. }
                if matches!(envelope.request, Request::Place(_) | Request::Amend { .. } | Request::Cancel { .. } | Request::CancelAll { .. }) =>
            {
                Some((envelope.venue, due))
            }
            Pending::Execute { venue, due, .. } => Some((venue, due)),
            Pending::Deadman { venue, deadline, .. } => Some((venue, deadline)),
            _ => None,
        }
    }
}

// Same-µs order: a print before the book change it causes, book frames as they arrived (Aster
// stamps its 50 ms batch: a depth snapshot and the tickers published after it share one time),
// market before our actions (pessimistic), deliveries last.
const RANK_TRADE: u8 = 0;
const RANK_BOOK: u8 = 2;
const RANK_FUNDING: u8 = 3;
const RANK_ACTION: u8 = 4;
const RANK_DELIVER: u8 = 5;

/// Is `a` a better price than `b` for orders resting on `side`?
fn better(side: Side, a: Decimal, b: Decimal) -> bool {
    match side {
        Side::Buy => a > b,
        Side::Sell => a < b,
    }
}

/// How much a `side` order can reduce `position` by.
fn reducible(position: Decimal, side: Side) -> Decimal {
    match side {
        Side::Buy if position < Decimal::ZERO => -position,
        Side::Sell if position > Decimal::ZERO => position,
        _ => Decimal::ZERO,
    }
}

fn check_filters(f: &Filters, spec: &OrderSpec, mark: Option<Decimal>) -> Result<(), Reject> {
    let off = |value: Decimal, step: Decimal| !step.is_zero() && !(value % step).is_zero();
    if let Some(price) = spec.price {
        if price <= Decimal::ZERO || off(price, f.tick) {
            return Err(Reject::TickSize);
        }
        let figures = |p: Decimal| p.normalize().mantissa().unsigned_abs().to_string().len();
        if f.sig_figs.is_some_and(|n| !price.fract().is_zero() && figures(price) > n as usize) {
            return Err(Reject::TickSize);
        }
        if let (Some((down, up)), Some(mark)) = (f.percent_price, mark) {
            let outside = match spec.side {
                Side::Buy => price > mark * up,
                Side::Sell => price < mark * down,
            };
            if outside {
                return Err(Reject::PriceBand);
            }
        }
    }
    if spec.qty <= Decimal::ZERO || off(spec.qty, f.step) {
        return Err(Reject::StepSize);
    }
    // A reduce-only order is exempt from both minimums (measured live 2026-09-28: Aster and
    // Lighter fill one of 0.01 HYPE); Hyperliquid's exception is in `place`.
    if !spec.reduce_only && spec.qty < f.min_qty {
        return Err(Reject::MinQty);
    }
    let notional = spec.qty * spec.price.or(mark).unwrap_or_default();
    if !spec.reduce_only && notional < f.min_notional {
        return Err(Reject::MinNotional);
    }
    Ok(())
}

pub struct Exchange {
    p: SimParams,
    rng: Rng,
    now: i64,
    seq: u64,
    tickets: u64,
    pending: BTreeMap<Key, Pending>,
    /// Per venue: how far its feed has shown the market (the latest frame's shifted time), and
    /// the actions waiting for it to reach their time.
    known: [i64; 2],
    held: [BTreeMap<Key, Pending>; 2],
    venues: [VenueState; 2],
    /// Per connection: when its last request reached the venue and when it was answered.
    lanes: HashMap<u64, (i64, i64)>,
    /// Per venue: when the private stream last delivered (it delivers in order).
    streams: [i64; 2],
    pub diag: [Diag; 2],
    /// How late due events ran on this host (µs): scheduling, and CPU throttling.
    pub lateness_us: Vec<i64>,
}

impl Exchange {
    pub fn new(p: SimParams, start_us: i64) -> Self {
        let mut venues = [VenueState::new(p.balances[0]), VenueState::new(p.balances[1])];
        venues[0].limits = venue_limits(p.venues[0]);
        venues[1].limits = venue_limits(p.venues[1]);
        Self {
            rng: Rng::new(p.seed),
            p,
            now: start_us,
            seq: 0,
            tickets: 0,
            pending: BTreeMap::new(),
            known: [i64::MIN; 2],
            held: Default::default(),
            venues,
            lanes: HashMap::new(),
            streams: [i64::MIN; 2],
            diag: Default::default(),
            lateness_us: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn now(&self) -> i64 {
        self.now
    }

    pub fn venues(&self) -> [Venue; 2] {
        self.p.venues
    }

    fn ix(&self, venue: Venue) -> usize {
        venue.ix(self.p.venues)
    }

    /// For scripted feeds, which publish only when told: nothing waits for them.
    #[cfg(test)]
    pub fn trust_feed(&mut self) {
        self.known = [i64::MAX; 2];
    }

    /// What a restart must keep, per venue.
    pub fn state(&self) -> &[VenueState; 2] {
        &self.venues
    }

    /// Takes up a saved state: accounts, working orders, ids, nonces, deadmen and funding
    /// rates. The first warm book then fills whatever the market crossed while the venues
    /// were down; [`Exchange::resume`] runs the deadmen afterwards.
    pub fn restore(&mut self, saved: [VenueState; 2]) {
        for (st, saved) in self.venues.iter_mut().zip(saved) {
            *st = VenueState {
                books: std::mem::take(&mut st.books),
                filters: std::mem::take(&mut st.filters),
                marks: std::mem::take(&mut st.marks),
                limits: std::mem::take(&mut st.limits),
                ..saved
            };
        }
        for venue in self.p.venues {
            let due: Vec<(String, i64)> = self.venues[self.ix(venue)].funding.iter()
                .flat_map(|(market, rates)| rates.keys().map(move |&exch_us| (market.clone(), exch_us)))
                .collect();
            for (market, exch_us) in due {
                self.schedule(exch_us + self.p.shift_us, RANK_FUNDING, Pending::Funding { venue, market, exch_us });
            }
        }
    }

    /// Re-arms the restored deadmen: one that ran out while the venues were down cancels its
    /// market's orders now.
    pub fn resume(&mut self) {
        for venue in self.p.venues {
            let armed: Vec<(String, i64)> = self.venues[self.ix(venue)].deadman.iter().map(|(m, &d)| (m.clone(), d)).collect();
            for (market, deadline) in armed {
                self.schedule(deadline, RANK_ACTION, Pending::Deadman { venue, market, deadline });
            }
        }
    }

    /// Adds a market; `depth` is how many levels per side its feed shows (None = full book).
    pub fn add_market(&mut self, venue: Venue, market: &str, depth: Option<usize>, filters: Filters) {
        let st = &mut self.venues[self.ix(venue)];
        st.books.entry(market.to_string()).or_insert_with(|| Replica::new(depth));
        st.filters.insert(market.to_string(), filters);
    }

    pub fn replica(&self, venue: Venue, market: &str) -> Option<&Replica> {
        self.venues[self.ix(venue)].books.get(market)
    }

    /// Queues a feed event, arriving now, for its shifted time, or for now if it arrived too
    /// late for that.
    pub fn ingest(&mut self, venue: Venue, market: &str, exch_us: i64, event: FeedEvent) {
        let at = exch_us + self.p.shift_us;
        let diag = &mut self.diag[self.ix(venue)];
        diag.frames += 1;
        if at < self.now {
            diag.late_frames += 1;
        }
        let (rank, stream) = match event {
            FeedEvent::Trade { .. } => (RANK_TRADE, Some("trade")),
            FeedEvent::Book(BookUpdate::Top { .. }) => (RANK_BOOK, Some("top")),
            FeedEvent::Book(_) => (RANK_BOOK, Some("book")),
            FeedEvent::Gap => (RANK_BOOK, None),
        };
        if let Some(stream) = stream {
            diag.lag_us.entry(stream).or_default().push(self.now - exch_us);
        }
        self.schedule(at, rank, Pending::Feed { venue, market: market.to_string(), exch_us, event });
        // The feed has shown the market up to `at`: what waited for that runs after this frame.
        let v = self.ix(venue);
        if stream.is_some() && at > self.known[v] {
            self.known[v] = at;
            let waiting = self.held[v].split_off(&(at + 1, 0, 0));
            for ((_, rank, _), action) in std::mem::replace(&mut self.held[v], waiting) {
                self.schedule(self.now, rank, action);
            }
        }
    }

    /// Records the funding rate a settlement at `exch_us` will use (re-polls update it). A rate
    /// arriving after its settlement's shifted time is charged on arrival.
    pub fn funding(&mut self, venue: Venue, market: &str, exch_us: i64, rate: Decimal) {
        let st = &mut self.venues[self.ix(venue)];
        if st.settled.get(market).is_some_and(|&last| exch_us <= last) {
            return;
        }
        let rates = st.funding.entry(market.to_string()).or_default();
        if rates.insert(exch_us, rate).is_none() {
            let at = exch_us + self.p.shift_us;
            self.schedule(at, RANK_FUNDING, Pending::Funding { venue, market: market.to_string(), exch_us });
        }
    }

    /// Sends a request now; its reply comes out of `advance` as `Output::Reply` with this ticket.
    pub fn submit(&mut self, envelope: Envelope) -> u64 {
        self.tickets += 1;
        let rtt = self.p.rtt[self.ix(envelope.venue)].sample_us(&mut self.rng);
        self.diag[self.ix(envelope.venue)].rtt_us.push(rtt);
        let effect = self.now + (rtt as f64 * self.p.effect_fraction).round() as i64;
        // A lane answered by now holds nothing back: forget closed connections' lanes.
        if self.lanes.len() > 1_024 {
            let now = self.now;
            self.lanes.retain(|_, lane| lane.1 > now);
        }
        let lane = self.lanes.entry(envelope.lane).or_insert((i64::MIN, i64::MIN));
        let gateway = effect.max(lane.0);
        let reply_at = (self.now + rtt).max(gateway).max(lane.1);
        *lane = (gateway, reply_at);
        let ticket = self.tickets;
        self.schedule(gateway, RANK_ACTION, Pending::Gateway { ticket, due: gateway, reply_at, envelope });
        ticket
    }

    /// The venue's account and open orders as of now, outside any request: what a private
    /// stream sends on subscription.
    pub fn peek(&self, venue: Venue) -> (AccountView, Vec<Order>) {
        (self.account_view(venue), self.venues[self.ix(venue)].open.values().cloned().collect())
    }

    pub fn next_due(&self) -> Option<i64> {
        self.pending.first_key_value().map(|(key, _)| key.0)
    }

    /// Runs every event due by `to`, appending what the bot receives to `out`.
    pub fn advance(&mut self, to: i64, out: &mut Vec<Output>) {
        while let Some(entry) = self.pending.first_entry() {
            if entry.key().0 > to {
                break;
            }
            let (key, pending) = entry.remove_entry();
            self.lateness_us.push(to - key.0);
            self.now = self.now.max(key.0);
            if let Some((venue, due)) = pending.acts_at() {
                if due > self.known[self.ix(venue)] {
                    self.hold(venue, key, pending);
                    continue;
                }
            }
            match pending {
                Pending::Feed { venue, market, exch_us, event } => self.on_feed(venue, &market, exch_us, event),
                Pending::Funding { venue, market, exch_us } => self.on_funding(venue, market, exch_us),
                Pending::Gateway { ticket, reply_at, envelope, .. } => self.on_gateway(ticket, reply_at, envelope),
                Pending::Execute { venue, request, .. } => self.execute_tx(venue, request),
                Pending::Deadman { venue, market, deadline } => {
                    if self.venues[self.ix(venue)].deadman.get(&market) == Some(&deadline) {
                        self.venues[self.ix(venue)].deadman.remove(&market);
                        self.cancel_all(venue, Some(&market), End::Deadman);
                    }
                }
                Pending::Expire { venue, key } => {
                    // Only requests expire (`hold`).
                    if let Some(Pending::Gateway { ticket, reply_at, .. }) = self.held[self.ix(venue)].remove(&key) {
                        self.diag[self.ix(venue)].requests += 1;
                        let reply = self.reject(venue, Reject::Unavailable);
                        self.schedule(reply_at, RANK_DELIVER, Pending::Deliver(Output::Reply { ticket, reply }));
                    }
                }
                Pending::Deliver(output) => out.push(output),
            }
        }
        self.now = self.now.max(to);
    }

    /// Parks an action until the venue's feed reaches its time; a request gives up after
    /// [`HOLD_US`].
    fn hold(&mut self, venue: Venue, key: Key, action: Pending) {
        self.diag[self.ix(venue)].held += 1;
        if let Pending::Gateway { due, .. } = action {
            self.schedule(due + HOLD_US, RANK_ACTION, Pending::Expire { venue, key });
        }
        self.held[self.ix(venue)].insert(key, action);
    }

    fn schedule(&mut self, at: i64, rank: u8, pending: Pending) {
        self.seq += 1;
        self.pending.insert((at.max(self.now), rank, self.seq), pending);
    }

    fn reject(&mut self, venue: Venue, reject: Reject) -> Reply {
        *self.diag[self.ix(venue)].rejects.entry(format!("{reject:?}")).or_default() += 1;
        Reply::Reject(reject)
    }

    fn account_view(&self, venue: Venue) -> AccountView {
        let st = &self.venues[self.ix(venue)];
        st.view(&st.working(), self.p.leverage)
    }

    fn push_event(&mut self, venue: Venue, event: Event) {
        let v = self.ix(venue);
        let delay = self.p.private[v].sample_us(&mut self.rng);
        self.diag[v].private_us.push(delay);
        let at = (self.now + delay).max(self.streams[v]);
        self.streams[v] = at;
        self.schedule(at, RANK_DELIVER, Pending::Deliver(Output::Event { venue, event }));
    }

    fn emit(&mut self, venue: Venue, order: &Order, fill: Option<Fill>) {
        let account = self.account_view(venue);
        self.push_event(venue, Event::Order { order: order.clone(), fill, account });
    }

    fn on_gateway(&mut self, ticket: u64, reply_at: i64, envelope: Envelope) {
        let venue = envelope.venue;
        let v = self.ix(venue);
        self.diag[v].requests += 1;
        let reply = if !admit(&mut self.venues[v].limits, self.now, envelope.weight, envelope.orders) {
            self.reject(venue, Reject::RateLimited)
        } else if let Some((key, nonce)) = envelope.nonce {
            let expected = self.venues[v].nonces.get(&key).copied().unwrap_or(0);
            if nonce != expected {
                self.reject(venue, Reject::BadNonce)
            } else {
                self.venues[v].nonces.insert(key, expected + 1);
                let bumped = matches!(&envelope.request, Request::Place(spec) if spec.tif != Tif::PostOnly);
                if venue == Venue::Lighter && bumped {
                    let at = self.now + self.p.lighter_taker_delay_us;
                    self.schedule(at, RANK_ACTION, Pending::Execute { venue, due: at, request: envelope.request });
                } else {
                    self.execute_tx(venue, envelope.request);
                }
                Reply::Ok
            }
        } else {
            self.execute(venue, envelope.request)
        };
        self.schedule(reply_at, RANK_DELIVER, Pending::Deliver(Output::Reply { ticket, reply }));
    }

    /// A transaction's outcome goes to the private stream only: a refused placement becomes
    /// an order that ends `Rejected`.
    fn execute_tx(&mut self, venue: Venue, request: Request) {
        match request {
            Request::Place(spec) => {
                if let Err(reject) = self.place(venue, &spec) {
                    self.reject(venue, reject);
                    let id = self.venues[self.ix(venue)].next_id();
                    let order = Order {
                        id,
                        client_id: spec.client_id,
                        market: spec.market,
                        side: spec.side,
                        price: spec.price,
                        qty: spec.qty,
                        tif: spec.tif,
                        reduce_only: spec.reduce_only,
                        filled: Decimal::ZERO,
                        filled_quote: Decimal::ZERO,
                        fee: Decimal::ZERO,
                        status: Status::Done(End::Rejected(reject)),
                        created_us: self.now,
                        updated_us: self.now,
                        ahead: None,
                        hidden: Decimal::ZERO,
                    };
                    self.emit(venue, &order, None);
                    self.close(venue, order);
                }
            }
            other => {
                self.execute(venue, other);
            }
        }
    }

    fn execute(&mut self, venue: Venue, request: Request) -> Reply {
        let v = self.ix(venue);
        match request {
            Request::Place(spec) => match self.place(venue, &spec) {
                Ok(order) => Reply::Order(order),
                Err(reject) => self.reject(venue, reject),
            },
            Request::Amend { market, order: which, qty, price } => match self.amend(venue, &market, &which, qty, price) {
                Ok(order) => Reply::Order(order),
                Err(reject) => self.reject(venue, reject),
            },
            Request::Cancel { market, order: which } => {
                let Some(id) = self.find_open(v, &market, &which) else { return self.reject(venue, Reject::UnknownOrder) };
                let mut order = self.venues[v].open.remove(&id).unwrap();
                self.finish(venue, &mut order, End::Canceled);
                Reply::Order(order)
            }
            Request::CancelAll { market } => {
                self.cancel_all(venue, market.as_deref(), End::Canceled);
                Reply::Ok
            }
            Request::Deadman { market, countdown_ms } => {
                if countdown_ms <= 0 {
                    self.venues[v].deadman.remove(&market);
                } else {
                    let deadline = self.now + countdown_ms * 1_000;
                    self.venues[v].deadman.insert(market.clone(), deadline);
                    self.schedule(deadline, RANK_ACTION, Pending::Deadman { venue, market, deadline });
                }
                Reply::Ok
            }
            Request::Account => Reply::Account(self.account_view(venue)),
            Request::OpenOrders { market } => Reply::Orders(
                self.venues[v].open.values()
                    .filter(|o| market.as_ref().is_none_or(|m| &o.market == m))
                    .cloned()
                    .collect(),
            ),
            Request::ClosedOrders { market } => Reply::Orders(
                self.venues[v].closed.iter().rev()
                    .filter(|o| market.as_ref().is_none_or(|m| &o.market == m))
                    .cloned()
                    .collect(),
            ),
            Request::Order { order: which } => {
                let st = &self.venues[v];
                let matches = |o: &&Order| match &which {
                    OrderRef::Id(id) => o.id == *id,
                    OrderRef::Client(client_id) => &o.client_id == client_id,
                };
                let found = st.open.values().find(matches).or_else(|| st.closed.iter().rev().find(matches)).cloned();
                match found {
                    Some(order) => Reply::Order(order),
                    None => self.reject(venue, Reject::UnknownOrder),
                }
            }
            Request::Fills { market } => Reply::Fills(
                self.venues[v].fills.iter()
                    .filter(|f| market.as_ref().is_none_or(|m| &f.market == m))
                    .cloned()
                    .collect(),
            ),
            Request::Book { market } => {
                let book = self.venues[v].books.get(&market)
                    .map(|b| b.warm().then(|| (b.levels(Side::Buy).collect(), b.levels(Side::Sell).collect())));
                match book {
                    Some(Some((bids, asks))) => Reply::Book { bids, asks, at_us: self.now },
                    Some(None) => self.reject(venue, Reject::Unavailable),
                    None => self.reject(venue, Reject::UnknownMarket),
                }
            }
            Request::NextNonce { key } => Reply::Nonce(self.venues[v].nonces.get(&key).copied().unwrap_or(0)),
            Request::Noop => Reply::Ok,
        }
    }

    /// The book state after the next update due within the lookahead, if any.
    fn next_state(&self, venue: Venue, market: &str) -> Option<Replica> {
        let current = self.venues[self.ix(venue)].books.get(market)?;
        let window = (self.now, 0, 0)..=(self.now + LOOKAHEAD_US, u8::MAX, u64::MAX);
        self.pending.range(window).find_map(|(_, pending)| match pending {
            Pending::Feed { venue: v, market: m, exch_us, event: FeedEvent::Book(update) }
                if *v == venue && m == market =>
            {
                let mut next = current.clone();
                next.apply(*exch_us, update);
                Some(next)
            }
            _ => None,
        })
    }

    fn place(&mut self, venue: Venue, spec: &OrderSpec) -> Result<Order, Reject> {
        let v = self.ix(venue);
        self.diag[v].orders += 1;
        if spec.tif == Tif::PostOnly && spec.price.is_none() {
            return Err(Reject::TickSize);
        }
        let st = &self.venues[v];
        let (Some(book), Some(filters)) = (st.books.get(&spec.market), st.filters.get(&spec.market)) else {
            return Err(Reject::UnknownMarket);
        };
        if !book.warm() {
            return Err(Reject::Unavailable);
        }
        let mark = st.marks.get(&spec.market).copied();
        check_filters(filters, spec, mark)?;
        let mut qty = spec.qty;
        if spec.reduce_only {
            let room = reducible(st.account.position(&spec.market).qty, spec.side);
            if room.is_zero() {
                return Err(Reject::ReduceOnly);
            }
            // Hyperliquid refuses a reduce-only order under its minimum unless it closes the whole
            // position (measured live 2026-09-28).
            if venue == Venue::Hyperliquid && qty < room && qty * spec.price.or(mark).unwrap_or_default() < filters.min_notional {
                return Err(Reject::MinNotional);
            }
            qty = qty.min(room);
        } else if !st.affords(&spec.market, spec.side, qty, self.p.leverage) {
            return Err(Reject::Margin);
        }
        let next = self.next_state(venue, &spec.market);
        let id = self.venues[v].next_id();
        let mut order = Order {
            id,
            client_id: spec.client_id.clone(),
            market: spec.market.clone(),
            side: spec.side,
            price: spec.price,
            qty,
            tif: spec.tif,
            reduce_only: spec.reduce_only,
            filled: Decimal::ZERO,
            filled_quote: Decimal::ZERO,
            fee: Decimal::ZERO,
            status: Status::New,
            created_us: self.now,
            updated_us: self.now,
            ahead: None,
            hidden: Decimal::ZERO,
        };
        if let (Tif::PostOnly, Some(price)) = (spec.tif, spec.price) {
            if self.venues[v].books[&spec.market].crosses(spec.side, price) {
                self.finish(venue, &mut order, End::PostOnly);
                return Ok(order);
            }
        }
        self.emit(venue, &order, None);
        if spec.tif != Tif::PostOnly {
            let levels = self.venues[v].books[&spec.market].takeable(next.as_ref(), spec.side, spec.price);
            for (level, free) in levels {
                let qty = self.fillable(venue, &order).min(free);
                if qty <= Decimal::ZERO {
                    break;
                }
                self.venues[v].books.get_mut(&spec.market).unwrap().consume(spec.side.opposite(), level, qty);
                self.fill(venue, &mut order, level, qty, false);
            }
        }
        if order.working() {
            if spec.tif == Tif::Ioc || spec.price.is_none() {
                self.finish(venue, &mut order, End::Ioc);
                return Ok(order);
            }
            self.join_queue(venue, &mut order, next.as_ref());
        }
        let snapshot = order.clone();
        self.settle(venue, order);
        Ok(snapshot)
    }

    /// Aster's modify: a resting order takes a new price and total qty and keeps its ids, at
    /// the back of the queue. One that would take is refused and rests unchanged (live
    /// 2026-09-28, -2036); one whose new qty is already filled ends Canceled. Live Aster also
    /// streams an AMENDMENT update, which the bot does not read; none is sent here.
    fn amend(&mut self, venue: Venue, market: &str, which: &OrderRef, qty: Decimal, price: Decimal) -> Result<Order, Reject> {
        let v = self.ix(venue);
        self.diag[v].orders += 1;
        let Some(id) = self.find_open(v, market, which) else { return Err(Reject::UnknownOrder) };
        let mut order = self.venues[v].open.remove(&id).unwrap();
        if let Err(reject) = self.amend_allowed(venue, &order, qty, price) {
            self.venues[v].open.insert(id, order);
            return Err(reject);
        }
        if qty <= order.filled {
            self.finish(venue, &mut order, End::Canceled);
            return Ok(order);
        }
        (order.qty, order.price, order.ahead, order.hidden, order.updated_us) = (qty, Some(price), None, Decimal::ZERO, self.now);
        let next = self.next_state(venue, market);
        self.join_queue(venue, &mut order, next.as_ref());
        let snapshot = order.clone();
        self.settle(venue, order);
        Ok(snapshot)
    }

    /// An amend's checks, against the book and the account without the order it amends.
    fn amend_allowed(&self, venue: Venue, order: &Order, qty: Decimal, price: Decimal) -> Result<(), Reject> {
        let st = &self.venues[self.ix(venue)];
        let (Some(book), Some(filters)) = (st.books.get(&order.market), st.filters.get(&order.market)) else {
            return Err(Reject::UnknownMarket);
        };
        if !book.warm() {
            return Err(Reject::Unavailable);
        }
        let spec = OrderSpec {
            market: order.market.clone(),
            client_id: order.client_id.clone(),
            side: order.side,
            qty,
            price: Some(price),
            tif: order.tif,
            reduce_only: order.reduce_only,
        };
        check_filters(filters, &spec, st.marks.get(&order.market).copied())?;
        if book.crosses(order.side, price) {
            return Err(Reject::WouldCross);
        }
        let rest = (qty - order.filled).max(Decimal::ZERO);
        if !order.reduce_only && !st.affords(&order.market, order.side, rest, self.p.leverage) {
            return Err(Reject::Margin);
        }
        Ok(())
    }

    /// Puts a resting order at the back of its level: behind the visible size there now or in
    /// the update due next, whichever is larger, and the hidden size assumed with it.
    fn join_queue(&mut self, venue: Venue, order: &mut Order, next: Option<&Replica>) {
        let v = self.ix(venue);
        let Some(price) = order.price else { return };
        if let Some(size) = self.venues[v].books[&order.market].visible(order.side, price) {
            let later = next.and_then(|n| n.visible(order.side, price)).unwrap_or_default();
            let visible = size.max(later);
            (order.ahead, order.hidden) = (Some(visible), visible * self.p.hidden_queue_multiplier);
            self.diag[v].queue_ahead.push(visible + order.hidden);
        }
    }

    fn find_open(&self, v: usize, market: &str, which: &OrderRef) -> Option<u64> {
        self.venues[v].open.values()
            .find(|o| o.market == market && match which {
                OrderRef::Id(id) => o.id == *id,
                OrderRef::Client(client_id) => &o.client_id == client_id,
            })
            .map(|o| o.id)
    }

    /// What a resting or taking order can still fill: its remainder, capped for reduce-only
    /// orders by the position left to reduce.
    fn fillable(&self, venue: Venue, order: &Order) -> Decimal {
        let remaining = order.remaining();
        if !order.reduce_only {
            return remaining;
        }
        remaining.min(reducible(self.venues[self.ix(venue)].account.position(&order.market).qty, order.side))
    }

    fn fill(&mut self, venue: Venue, order: &mut Order, price: Decimal, qty: Decimal, maker: bool) {
        let v = self.ix(venue);
        let rates = &self.p.fees[v];
        let fee = price * qty * if maker { rates.maker } else { rates.taker };
        let st = &mut self.venues[v];
        let realized = st.account.fill(&order.market, order.side, qty, price, fee);
        order.filled += qty;
        order.filled_quote += price * qty;
        order.fee += fee;
        order.updated_us = self.now;
        order.status = if order.remaining().is_zero() { Status::Filled } else { Status::PartiallyFilled };
        let fill = Fill {
            id: st.next_id(),
            order_id: order.id,
            client_id: order.client_id.clone(),
            market: order.market.clone(),
            side: order.side,
            price,
            qty,
            fee,
            maker,
            realized,
            at_us: self.now,
        };
        st.fills.push_back(fill.clone());
        if st.fills.len() > KEEP {
            st.fills.pop_front();
        }
        if maker {
            self.diag[v].maker_fills += 1;
            self.diag[v].maker_wait_us.push(self.now - order.created_us);
        } else {
            self.diag[v].taker_fills += 1;
        }
        self.emit(venue, order, Some(fill));
        // Resting reduce-only orders die with the position they were reducing.
        let dead: Vec<u64> = self.venues[v].open.values()
            .filter(|o| o.market == order.market && o.reduce_only && self.fillable(venue, o).is_zero())
            .map(|o| o.id)
            .collect();
        for id in dead {
            let mut stale = self.venues[v].open.remove(&id).unwrap();
            self.finish(venue, &mut stale, End::ReduceOnly);
        }
    }

    fn finish(&mut self, venue: Venue, order: &mut Order, end: End) {
        order.status = Status::Done(end);
        order.updated_us = self.now;
        self.emit(venue, order, None);
        self.close(venue, order.clone());
    }

    fn close(&mut self, venue: Venue, order: Order) {
        let closed = &mut self.venues[self.ix(venue)].closed;
        closed.push_back(order);
        if closed.len() > KEEP {
            closed.pop_front();
        }
    }

    /// Puts a still-working order back on the book, or closes it.
    fn settle(&mut self, venue: Venue, mut order: Order) {
        if order.working() && order.reduce_only && self.fillable(venue, &order).is_zero() {
            self.finish(venue, &mut order, End::ReduceOnly);
        } else if order.working() {
            self.venues[self.ix(venue)].open.insert(order.id, order);
        } else {
            self.close(venue, order);
        }
    }

    fn cancel_all(&mut self, venue: Venue, market: Option<&str>, end: End) {
        let v = self.ix(venue);
        let ids: Vec<u64> = self.venues[v].open.values()
            .filter(|o| market.is_none_or(|m| o.market == m))
            .map(|o| o.id)
            .collect();
        for id in ids {
            let mut order = self.venues[v].open.remove(&id).unwrap();
            self.finish(venue, &mut order, end);
        }
    }

    /// Our resting orders in `market` (on `side`, if given), each side best price first, then
    /// oldest first.
    fn resting(&self, venue: Venue, market: &str, side: Option<Side>) -> Vec<u64> {
        let mut orders: Vec<&Order> = self.venues[self.ix(venue)].open.values()
            .filter(|o| o.market == market && side.is_none_or(|s| o.side == s))
            .collect();
        orders.sort_by(|a, b| {
            let (pa, pb) = (a.price.unwrap_or_default(), b.price.unwrap_or_default());
            (a.side == Side::Sell).cmp(&(b.side == Side::Sell))
                .then(match a.side {
                    Side::Buy => pb.cmp(&pa),
                    Side::Sell => pa.cmp(&pb),
                })
                .then(a.id.cmp(&b.id))
        });
        orders.iter().map(|o| o.id).collect()
    }

    fn on_feed(&mut self, venue: Venue, market: &str, exch_us: i64, event: FeedEvent) {
        let v = self.ix(venue);
        let Some(book) = self.venues[v].books.get_mut(market) else { return };
        match event {
            FeedEvent::Book(update) => {
                if !book.apply(exch_us, &update) {
                    self.diag[v].stale_frames += 1;
                } else if book.warm() {
                    if let Some(mid) = book.mid() {
                        self.venues[v].marks.insert(market.to_string(), mid);
                    }
                    self.on_book(venue, market);
                }
            }
            FeedEvent::Trade { price, qty, taker } => {
                if book.warm() {
                    self.on_trade(venue, market, price, qty, taker);
                }
            }
            FeedEvent::Gap => {
                book.invalidate();
                self.diag[v].gaps += 1;
            }
        }
    }

    /// After a book update: cut each resting order's visible queue to what is left on its level
    /// (the only cancel credit), then fill whatever the book now crosses.
    fn on_book(&mut self, venue: Venue, market: &str) {
        let v = self.ix(venue);
        let h = self.p.hidden_queue_multiplier;
        for id in self.resting(venue, market, None) {
            // A fill above may have closed it (reduce-only orders die with their position).
            let Some(mut order) = self.venues[v].open.remove(&id) else { continue };
            let price = order.price.expect("resting orders have a price");
            let book = &self.venues[v].books[market];
            if let Some(size) = book.visible(order.side, price) {
                match order.ahead {
                    Some(ahead) => order.ahead = Some(ahead.min(size)),
                    None => (order.ahead, order.hidden) = (Some(size), size * h),
                }
            }
            for (level, free) in book.takeable(None, order.side, Some(price)) {
                let qty = self.fillable(venue, &order).min(free);
                if qty <= Decimal::ZERO {
                    break;
                }
                self.venues[v].books.get_mut(market).unwrap().consume(order.side.opposite(), level, qty);
                self.fill(venue, &mut order, price, qty, true);
            }
            self.settle(venue, order);
        }
    }

    fn on_trade(&mut self, venue: Venue, market: &str, price: Decimal, qty: Decimal, taker: Side) {
        let v = self.ix(venue);
        let passive = taker.opposite();
        let book = &self.venues[v].books[market];
        let diag = &mut self.diag[v];
        diag.prints += 1;
        if book.best(passive).is_some_and(|(best, _)| better(passive, price, best)) {
            diag.prints_inside_spread += 1;
        } else if book.visible(passive, price).is_some_and(|size| qty > size) {
            diag.prints_over_visible += 1;
        }
        let h = self.p.hidden_queue_multiplier;
        let mut left = qty;
        for id in self.resting(venue, market, Some(passive)) {
            if left <= Decimal::ZERO {
                break;
            }
            let Some(ours) = self.venues[v].open.get(&id).map(|o| o.price.expect("resting orders have a price")) else {
                continue;
            };
            if better(passive, price, ours) {
                break;
            }
            let mut order = self.venues[v].open.remove(&id).unwrap();
            if price != ours {
                (order.ahead, order.hidden) = (Some(Decimal::ZERO), Decimal::ZERO);
            } else if order.ahead.is_none() {
                if let Some(size) = self.venues[v].books[market].visible(passive, ours) {
                    (order.ahead, order.hidden) = (Some(size), size * h);
                }
            }
            if let Some(ahead) = order.ahead {
                // The visible queue goes first: the book update that follows shows it gone.
                let visible = ahead.min(left);
                let hidden = order.hidden.min(left - visible);
                (order.ahead, order.hidden) = (Some(ahead - visible), order.hidden - hidden);
                left -= visible + hidden;
                let qty = self.fillable(venue, &order).min(left);
                if qty > Decimal::ZERO {
                    left -= qty;
                    self.fill(venue, &mut order, ours, qty, true);
                }
            }
            self.settle(venue, order);
        }
    }

    fn on_funding(&mut self, venue: Venue, market: String, exch_us: i64) {
        let v = self.ix(venue);
        // No mark before the first book (a restart): settle once there is one.
        let Some(mark) = self.venues[v].marks.get(&market).copied() else {
            self.schedule(self.now + 1_000_000, RANK_FUNDING, Pending::Funding { venue, market, exch_us });
            return;
        };
        let st = &mut self.venues[v];
        let Some(rate) = st.funding.get_mut(&market).and_then(|rates| rates.remove(&exch_us)) else { return };
        st.settled.insert(market.clone(), exch_us);
        let qty = st.account.position(&market).qty;
        if qty.is_zero() {
            return;
        }
        // Longs pay a positive rate.
        let amount = -qty * mark * rate;
        st.account.fund(amount);
        let account = self.account_view(venue);
        self.push_event(venue, Event::Funding { market, amount, account });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const MS: i64 = 1_000;
    const HYPE: &str = "HYPE";

    fn fixed(ms: f64) -> Latency {
        if ms == 0.0 { Latency::ZERO } else { Latency::try_from([ms, ms]).unwrap() }
    }

    fn params(shift_ms: i64) -> SimParams {
        SimParams {
            shift_us: shift_ms * MS,
            seed: 7,
            effect_fraction: 0.9,
            rtt: [fixed(100.0), fixed(10.0)],
            private: [fixed(10.0), fixed(5.0)],
            lighter_taker_delay_us: 300 * MS,
            hidden_queue_multiplier: dec!(0.5),
            fees: [Fees { maker: dec!(0), taker: dec!(0.0004) }, Fees { maker: dec!(0), taker: dec!(0) }],
            leverage: dec!(1),
            balances: [dec!(1000), dec!(1000)],
            venues: [Venue::Aster, Venue::Lighter],
        }
    }

    fn levels(v: &[(Decimal, Decimal)]) -> Vec<Level> {
        v.to_vec()
    }

    /// A market around 100 on both venues, a fixed timeline in exchange ms, and everything the
    /// bot received.
    struct Sim {
        ex: Exchange,
        shift: i64,
        out: Vec<Output>,
        nonce: i64,
    }

    impl Sim {
        fn new() -> Self {
            Self::with(params(500))
        }

        fn with(p: SimParams) -> Self {
            let mut sim = Self::cold(p, 0);
            for venue in sim.ex.venues() {
                sim.book(venue, 0, &[(dec!(99), dec!(5)), (dec!(98), dec!(5))], &[(dec!(101), dec!(5)), (dec!(102), dec!(5))]);
            }
            sim.at(0);
            sim
        }

        /// The venues before any book, at exchange time `ms`.
        fn cold(p: SimParams, ms: i64) -> Self {
            let (shift, venues) = (p.shift_us, p.venues);
            let mut ex = Exchange::new(p, ms * MS + shift);
            let filters = Filters {
                tick: dec!(0.01),
                step: dec!(0.01),
                min_qty: dec!(0.01),
                min_notional: dec!(5),
                percent_price: Some((dec!(0.98), dec!(1.02))),
                sig_figs: None,
            };
            for venue in venues {
                ex.add_market(venue, HYPE, (venue != Venue::Lighter).then_some(20), filters.clone());
            }
            ex.trust_feed();
            Self { ex, shift, out: Vec::new(), nonce: 0 }
        }

        fn feed(&mut self, venue: Venue, ms: i64, event: FeedEvent) {
            self.ex.ingest(venue, HYPE, ms * MS, event);
        }

        fn book(&mut self, venue: Venue, ms: i64, bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) {
            let update = BookUpdate::Replace { bids: levels(bids), asks: levels(asks) };
            self.feed(venue, ms, FeedEvent::Book(update));
        }

        fn delta(&mut self, venue: Venue, ms: i64, bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) {
            let update = BookUpdate::Delta { bids: levels(bids), asks: levels(asks) };
            self.feed(venue, ms, FeedEvent::Book(update));
        }

        fn print(&mut self, venue: Venue, ms: i64, price: Decimal, qty: Decimal, taker: Side) {
            self.feed(venue, ms, FeedEvent::Trade { price, qty, taker });
        }

        /// Advances to the wall time at which the venue is at exchange time `ms`.
        fn at(&mut self, ms: i64) {
            self.ex.advance(ms * MS + self.shift, &mut self.out);
        }

        fn send(&mut self, venue: Venue, request: Request) -> u64 {
            let orders = u32::from(matches!(request, Request::Place(_) | Request::Amend { .. }));
            self.ex.submit(Envelope { venue, lane: 1, weight: 1, orders, nonce: None, request })
        }

        fn tx(&mut self, request: Request) -> u64 {
            self.nonce += 1;
            let nonce = Some((9, self.nonce - 1));
            self.ex.submit(Envelope { venue: Venue::Lighter, lane: 2, weight: 0, orders: 1, nonce, request })
        }

        fn reply(&self, ticket: u64) -> Option<&Reply> {
            self.out.iter().find_map(|o| match o {
                Output::Reply { ticket: t, reply } if *t == ticket => Some(reply),
                _ => None,
            })
        }

        fn fills(&self, venue: Venue) -> Vec<(Decimal, Decimal, bool)> {
            self.out.iter().filter_map(|o| match o {
                Output::Event { venue: v, event: Event::Order { fill: Some(f), .. } } if *v == venue => {
                    Some((f.price, f.qty, f.maker))
                }
                _ => None,
            }).collect()
        }

        fn last_status(&self, venue: Venue, id: u64) -> Option<Status> {
            self.out.iter().rev().find_map(|o| match o {
                Output::Event { venue: v, event: Event::Order { order, .. } } if *v == venue && order.id == id => {
                    Some(order.status)
                }
                _ => None,
            })
        }

        fn open(&self, venue: Venue) -> Vec<&Order> {
            self.ex.venues[self.ex.ix(venue)].open.values().collect()
        }
    }

    fn limit(side: Side, qty: Decimal, price: Decimal, tif: Tif) -> Request {
        Request::Place(OrderSpec {
            market: HYPE.into(),
            client_id: "c".into(),
            side,
            qty,
            price: Some(price),
            tif,
            reduce_only: false,
        })
    }

    fn reduce_only(side: Side, qty: Decimal, price: Decimal, tif: Tif) -> Request {
        match limit(side, qty, price, tif) {
            Request::Place(spec) => Request::Place(OrderSpec { reduce_only: true, ..spec }),
            _ => unreachable!(),
        }
    }

    fn placed(sim: &Sim, ticket: u64) -> Order {
        match sim.reply(ticket) {
            Some(Reply::Order(order)) => order.clone(),
            other => panic!("expected an order reply, got {other:?}"),
        }
    }

    #[test]
    fn hyperliquid_prices_keep_five_significant_figures_unless_whole() {
        let f = Filters { tick: dec!(0.0001), step: dec!(0.01), sig_figs: Some(5), ..Filters::default() };
        let spec = |price| OrderSpec {
            market: HYPE.into(), client_id: "c".into(), side: Side::Buy, qty: dec!(1), price: Some(price), tif: Tif::Gtc, reduce_only: false,
        };
        for (price, valid) in [(dec!(40.012), true), (dec!(40.0125), false), (dec!(0.0012), true), (dec!(123456), true), (dec!(12345.5), false)] {
            assert_eq!(check_filters(&f, &spec(price), None).is_ok(), valid, "{price}");
        }
    }

    // --- The maker queue (ported from the retired fill_sweep, with two corrections: better-
    // priced prints no longer advance us, and a print through our price empties the level). ---

    #[test]
    fn prints_at_our_level_eat_the_visible_queue_then_fill_us() {
        let mut sim = Sim::new();
        let ticket = sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(99), Tif::PostOnly));
        sim.at(100);
        let order = placed(&sim, ticket);
        assert_eq!((order.ahead, order.hidden), (Some(dec!(5)), dec!(2.5)));
        sim.print(Venue::Aster, 200, dec!(99), dec!(6), Side::Sell);
        sim.print(Venue::Aster, 300, dec!(99), dec!(3), Side::Sell);
        sim.print(Venue::Aster, 400, dec!(99), dec!(1), Side::Sell);
        sim.at(500);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99), dec!(1.5), true), (dec!(99), dec!(0.5), true)]);
        assert!(sim.open(Venue::Aster).is_empty());
    }

    #[test]
    fn an_amend_keeps_the_ids_rejoins_the_queue_and_never_takes() {
        let mut sim = Sim::new();
        let ticket = sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(98), Tif::PostOnly));
        sim.print(Venue::Aster, 200, dec!(98), dec!(3), Side::Sell);
        sim.at(300);
        let order = placed(&sim, ticket);
        let amend = |qty, price| Request::Amend { market: HYPE.into(), order: OrderRef::Client("c".into()), qty, price };
        // Onto the ask: refused, and the order rests as it was, 2 behind at 98.
        let cross = sim.send(Venue::Aster, amend(dec!(2), dec!(101)));
        sim.at(400);
        assert_eq!(sim.reply(cross), Some(&Reply::Reject(Reject::WouldCross)));
        let rest = sim.open(Venue::Aster)[0];
        assert_eq!((rest.price, rest.qty, rest.ahead), (Some(dec!(98)), dec!(2), Some(dec!(2))));
        // Up to 99 for 3: the same order, at the back of the 99 queue.
        let moved = sim.send(Venue::Aster, amend(dec!(3), dec!(99)));
        sim.at(500);
        let now = placed(&sim, moved);
        assert_eq!((now.id, now.price, now.qty, now.status), (order.id, Some(dec!(99)), dec!(3), Status::New));
        assert_eq!((now.ahead, now.hidden), (Some(dec!(5)), dec!(2.5)));
        sim.print(Venue::Aster, 600, dec!(99), dec!(8.5), Side::Sell);
        sim.at(700);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99), dec!(1), true)]);
        // Down to what already filled: it ends.
        let shrink = sim.send(Venue::Aster, amend(dec!(1), dec!(99)));
        sim.at(800);
        assert_eq!(placed(&sim, shrink).status, Status::Done(End::Canceled));
        assert!(sim.open(Venue::Aster).is_empty());
        let gone = sim.send(Venue::Aster, amend(dec!(2), dec!(99)));
        sim.at(900);
        assert_eq!(sim.reply(gone), Some(&Reply::Reject(Reject::UnknownOrder)));
    }

    #[test]
    fn better_priced_prints_leave_our_queue_alone() {
        let mut sim = Sim::new();
        sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(98), Tif::PostOnly));
        sim.print(Venue::Aster, 200, dec!(99), dec!(10), Side::Sell);
        sim.print(Venue::Aster, 300, dec!(98), dec!(1), Side::Sell);
        sim.at(400);
        let order = sim.open(Venue::Aster)[0];
        assert_eq!((order.ahead, order.hidden), (Some(dec!(4)), dec!(2.5)));
        assert!(sim.fills(Venue::Aster).is_empty());
    }

    #[test]
    fn a_print_through_our_price_fills_us_despite_the_queue() {
        let mut sim = Sim::new();
        sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(99), Tif::PostOnly));
        sim.print(Venue::Aster, 200, dec!(98.5), dec!(3), Side::Sell);
        sim.at(300);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99), dec!(2), true)]);
    }

    #[test]
    fn prints_from_the_wrong_side_or_before_the_order_exists_never_fill() {
        let mut sim = Sim::new();
        sim.send(Venue::Aster, limit(Side::Sell, dec!(1), dec!(100.5), Tif::PostOnly));
        // The order takes effect at 90 ms: an earlier print cannot reach it.
        sim.print(Venue::Aster, 50, dec!(100.6), dec!(9), Side::Buy);
        sim.print(Venue::Aster, 150, dec!(100.4), dec!(9), Side::Sell);
        sim.at(200);
        assert!(sim.fills(Venue::Aster).is_empty());
        // Our ask is lifted by a market buy through it.
        sim.print(Venue::Aster, 250, dec!(100.6), dec!(9), Side::Buy);
        sim.at(300);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(100.5), dec!(1), true)]);
    }

    #[test]
    fn one_print_is_shared_across_our_orders() {
        let mut sim = Sim::new();
        sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(99.5), Tif::PostOnly));
        sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(99.4), Tif::PostOnly));
        sim.print(Venue::Aster, 200, dec!(99.4), dec!(3), Side::Sell);
        sim.at(300);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99.5), dec!(2), true), (dec!(99.4), dec!(1), true)]);
    }

    #[test]
    fn a_crossed_book_fills_resting_orders_once_even_after_a_gap() {
        let mut sim = Sim::new();
        sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(99.5), Tif::PostOnly));
        sim.feed(Venue::Aster, 400, FeedEvent::Gap);
        let asks = [(dec!(99.4), dec!(1)), (dec!(99.6), dec!(5))];
        sim.book(Venue::Aster, 500, &[(dec!(99), dec!(5))], &asks);
        sim.book(Venue::Aster, 600, &[(dec!(99), dec!(5))], &asks);
        sim.at(700);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99.5), dec!(1), true)]);
        // The same ask growing behind what we took fills the rest.
        sim.book(Venue::Aster, 800, &[(dec!(99), dec!(5))], &[(dec!(99.4), dec!(4))]);
        sim.at(900);
        assert_eq!(sim.fills(Venue::Aster)[1], (dec!(99.5), dec!(1), true));
    }

    #[test]
    fn the_visible_queue_shrinks_with_its_level_and_the_hidden_one_only_by_prints() {
        let mut sim = Sim::new();
        sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(99), Tif::PostOnly));
        sim.book(Venue::Aster, 200, &[(dec!(99), dec!(2))], &[(dec!(101), dec!(5))]);
        sim.book(Venue::Aster, 300, &[(dec!(99), dec!(9))], &[(dec!(101), dec!(5))]);
        sim.at(400);
        let order = sim.open(Venue::Aster)[0];
        assert_eq!((order.ahead, order.hidden), (Some(dec!(2)), dec!(2.5)));
        // 4 takes the 2 visible and 2 hidden; of the next 1, half is hidden and half fills us.
        sim.print(Venue::Aster, 500, dec!(99), dec!(4), Side::Sell);
        sim.print(Venue::Aster, 600, dec!(99), dec!(1), Side::Sell);
        sim.at(700);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99), dec!(0.5), true)]);
    }

    // --- A stalled feed. ---

    #[test]
    fn a_cancel_waits_for_a_stalled_feed_and_the_prints_it_missed_fill_first() {
        let mut sim = Sim::new();
        let place = sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(99.5), Tif::PostOnly));
        sim.at(100);
        let id = placed(&sim, place).id;
        // The Aster feed has shown the market up to 100 ms, then stalls; the cancel lands at 190.
        sim.ex.known[0] = 100 * MS + sim.shift;
        let cancel = sim.send(Venue::Aster, Request::Cancel { market: HYPE.into(), order: OrderRef::Id(id) });
        let account = sim.send(Venue::Aster, Request::Account);
        sim.at(1_000);
        assert!(sim.reply(cancel).is_none(), "the cancel waits for the market");
        assert!(matches!(sim.reply(account), Some(Reply::Account(_))), "reads never wait");
        // The stalled frames arrive late: a print at 150 ms hit the bid before the cancel landed.
        sim.print(Venue::Aster, 150, dec!(99.5), dec!(1), Side::Sell);
        sim.print(Venue::Aster, 250, dec!(101), dec!(1), Side::Buy);
        sim.at(1_100);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99.5), dec!(1), true)]);
        let canceled = placed(&sim, cancel);
        assert_eq!((canceled.status, canceled.filled), (Status::Done(End::Canceled), dec!(1)));
        assert_eq!(sim.ex.diag[0].held, 1);
    }

    #[test]
    fn an_order_the_feed_never_catches_up_with_is_refused_as_unavailable() {
        let mut sim = Sim::new();
        sim.ex.known[1] = sim.ex.now();
        let order = sim.tx(limit(Side::Buy, dec!(1), dec!(99), Tif::PostOnly));
        sim.at(2_000);
        assert!(sim.reply(order).is_none());
        sim.at(2_010);
        assert_eq!(sim.reply(order), Some(&Reply::Reject(Reject::Unavailable)));
        // The nonce was not used: the next transaction may take it.
        let next = sim.ex.submit(Envelope { venue: Venue::Lighter, lane: 3, weight: 1, orders: 0, nonce: None, request: Request::NextNonce { key: 9 } });
        sim.at(2_100);
        assert_eq!(sim.reply(next), Some(&Reply::Nonce(0)));
    }

    // --- Taking liquidity. ---

    #[test]
    fn takers_fill_against_the_worse_of_the_bracketing_states() {
        let mut sim = Sim::new();
        // Effect at 90 ms; the next top (at 120 ms) shows 101 almost gone; one far past the
        // lookahead is ignored.
        sim.feed(Venue::Aster, 120, FeedEvent::Book(BookUpdate::Top { bid: (dec!(99), dec!(5)), ask: (dec!(101), dec!(1)) }));
        sim.feed(Venue::Aster, 900, FeedEvent::Book(BookUpdate::Top { bid: (dec!(99), dec!(5)), ask: (dec!(102), dec!(1)) }));
        let ticket = sim.send(Venue::Aster, limit(Side::Buy, dec!(4), dec!(102), Tif::Ioc));
        sim.at(200);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(101), dec!(1), false), (dec!(102), dec!(3), false)]);
        let order = placed(&sim, ticket);
        assert_eq!((order.status, order.fee), (Status::Filled, dec!(0.1628)));
        // What we took stays taken: the next taker finds 102 short by our 3.
        sim.send(Venue::Aster, limit(Side::Buy, dec!(4), dec!(102), Tif::Ioc));
        sim.at(400);
        assert_eq!(sim.fills(Venue::Aster)[2..], [(dec!(102), dec!(2), false)]);
    }

    #[test]
    fn a_ticker_published_after_the_snapshot_of_its_batch_stays_on_top() {
        let mut sim = Sim::new();
        sim.book(Venue::Aster, 50, &[(dec!(99), dec!(5))], &[(dec!(101), dec!(5))]);
        sim.feed(Venue::Aster, 50, FeedEvent::Book(BookUpdate::Top { bid: (dec!(99), dec!(5)), ask: (dec!(100.5), dec!(2)) }));
        sim.at(60);
        assert_eq!(sim.ex.replica(Venue::Aster, HYPE).unwrap().best(Side::Sell), Some((dec!(100.5), dec!(2))));
    }

    #[test]
    fn post_only_expires_if_it_crosses_on_arrival_and_is_hit_if_the_book_crosses_later() {
        let mut sim = Sim::new();
        let crossing = sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(101), Tif::PostOnly));
        // Lands at 90 ms; an ask at its price shows up at 150 ms and trades against it.
        let resting = sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(100.5), Tif::PostOnly));
        sim.feed(Venue::Aster, 150, FeedEvent::Book(BookUpdate::Top { bid: (dec!(99), dec!(5)), ask: (dec!(100.5), dec!(3)) }));
        sim.at(200);
        assert_eq!(placed(&sim, crossing).status, Status::Done(End::PostOnly));
        assert_eq!(placed(&sim, resting).status, Status::New);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(100.5), dec!(1), true)]);
    }

    #[test]
    fn fills_that_beat_a_cancel_stand() {
        let mut sim = Sim::new();
        let place = sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(99.5), Tif::PostOnly));
        sim.at(100);
        let id = placed(&sim, place).id;
        let cancel = sim.send(Venue::Aster, Request::Cancel { market: HYPE.into(), order: OrderRef::Id(id) });
        // The cancel lands at 190 ms; a print at 150 ms gets there first.
        sim.print(Venue::Aster, 150, dec!(99.5), dec!(1), Side::Sell);
        sim.at(300);
        assert_eq!(sim.fills(Venue::Aster), vec![(dec!(99.5), dec!(1), true)]);
        let canceled = placed(&sim, cancel);
        assert_eq!((canceled.status, canceled.filled), (Status::Done(End::Canceled), dec!(1)));
        let again = sim.send(Venue::Aster, Request::Cancel { market: HYPE.into(), order: OrderRef::Id(id) });
        sim.at(500);
        assert_eq!(sim.reply(again), Some(&Reply::Reject(Reject::UnknownOrder)));
    }

    // --- Venue rules. ---

    #[test]
    fn lighter_taker_orders_wait_out_the_speed_bump() {
        let mut sim = Sim::new();
        // Gateway at 9 ms, execution at 309 ms, private event at 314 ms.
        let ticket = sim.tx(limit(Side::Buy, dec!(1), dec!(102), Tif::Ioc));
        sim.delta(Venue::Lighter, 200, &[], &[(dec!(101), dec!(0))]);
        sim.at(100);
        assert_eq!(sim.reply(ticket), Some(&Reply::Ok));
        sim.at(313);
        assert!(sim.fills(Venue::Lighter).is_empty());
        sim.at(314);
        assert_eq!(sim.fills(Venue::Lighter), vec![(dec!(102), dec!(1), false)]);
        // Post-only orders and cancels skip the bump.
        sim.tx(limit(Side::Buy, dec!(1), dec!(99), Tif::PostOnly));
        sim.at(330);
        assert_eq!(sim.open(Venue::Lighter).len(), 1);
    }

    #[test]
    fn lighter_nonces_must_follow_on_exactly() {
        let mut sim = Sim::new();
        let key = 9;
        let tx = |nonce| Envelope { venue: Venue::Lighter, lane: 2, weight: 0, orders: 1, nonce: Some((key, nonce)), request: Request::Noop };
        let tickets: Vec<u64> = [0, 0, 2, 1].into_iter().map(|n| sim.ex.submit(tx(n))).collect();
        let next = sim.ex.submit(Envelope { venue: Venue::Lighter, lane: 3, weight: 1, orders: 0, nonce: None, request: Request::NextNonce { key } });
        sim.at(100);
        let replies: Vec<_> = tickets.iter().map(|&t| sim.reply(t).cloned().unwrap()).collect();
        let bad = Reply::Reject(Reject::BadNonce);
        assert_eq!(replies, vec![Reply::Ok, bad.clone(), bad, Reply::Ok]);
        assert_eq!(sim.reply(next), Some(&Reply::Nonce(2)));
    }

    #[test]
    fn one_connection_keeps_its_order_under_random_latency() {
        let jittery = || {
            let mut p = params(500);
            p.rtt[1] = Latency::try_from([5.0, 100.0]).unwrap();
            Sim::with(p)
        };
        let tx = |lane: u64, nonce: i64| Envelope {
            venue: Venue::Lighter, lane, weight: 0, orders: 0, nonce: Some((1, nonce)), request: Request::Noop,
        };
        let mut sim = jittery();
        let tickets: Vec<u64> = (0..50).map(|n| sim.ex.submit(tx(7, n))).collect();
        sim.at(1_000);
        assert!(tickets.iter().all(|&t| sim.reply(t) == Some(&Reply::Ok)));
        // The same transactions spread over separate connections overtake each other.
        let mut sim = jittery();
        let tickets: Vec<u64> = (0..50).map(|n| sim.ex.submit(tx(100 + n as u64, n))).collect();
        sim.at(1_000);
        assert!(tickets.iter().any(|&t| sim.reply(t) == Some(&Reply::Reject(Reject::BadNonce))));
    }

    #[test]
    fn reduce_only_refuses_to_add_and_dies_with_the_position() {
        let mut sim = Sim::new();
        let flat = sim.send(Venue::Aster, reduce_only(Side::Sell, dec!(1), dec!(99), Tif::Ioc));
        sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(101), Tif::Ioc));
        sim.at(200);
        assert_eq!(sim.reply(flat), Some(&Reply::Reject(Reject::ReduceOnly)));
        // A resting reduce-only ask, then an IOC that closes the long first.
        let resting = sim.send(Venue::Aster, reduce_only(Side::Sell, dec!(3), dec!(100.5), Tif::PostOnly));
        sim.at(400);
        let resting = placed(&sim, resting);
        assert_eq!(resting.qty, dec!(1));
        sim.send(Venue::Aster, reduce_only(Side::Sell, dec!(5), dec!(98), Tif::Ioc));
        sim.at(600);
        assert_eq!(sim.fills(Venue::Aster)[1], (dec!(99), dec!(1), false));
        assert_eq!(sim.last_status(Venue::Aster, resting.id), Some(Status::Done(End::ReduceOnly)));
        assert_eq!(sim.ex.venues[0].account.position(HYPE).qty, dec!(0));
    }

    #[test]
    fn hyperliquid_takes_a_reduce_only_order_under_its_minimum_only_as_a_full_close() {
        let mut p = params(500);
        p.venues = [Venue::Aster, Venue::Hyperliquid];
        let mut sim = Sim::with(p);
        let both = [Venue::Aster, Venue::Hyperliquid];
        for venue in both {
            sim.send(venue, limit(Side::Buy, dec!(0.1), dec!(102), Tif::Ioc));
        }
        sim.at(200);
        for venue in both {
            sim.send(venue, limit(Side::Sell, dec!(0.06), dec!(98), Tif::Ioc));
        }
        sim.at(400);
        // Under the $5 minimum, a size minimum (Lighter's HYPE 0.07) and Hyperliquid's 0.04
        // position: Aster takes it, Hyperliquid not.
        sim.ex.venues[0].filters.get_mut(HYPE).unwrap().min_qty = dec!(0.07);
        let partial = both.map(|venue| sim.send(venue, reduce_only(Side::Sell, dec!(0.01), dec!(98), Tif::Ioc)));
        sim.at(600);
        assert!(matches!(sim.reply(partial[0]), Some(Reply::Order(_))), "{:?}", sim.reply(partial[0]));
        assert_eq!(sim.reply(partial[1]), Some(&Reply::Reject(Reject::MinNotional)));
        let close = sim.send(Venue::Hyperliquid, reduce_only(Side::Sell, dec!(0.04), dec!(98), Tif::Ioc));
        sim.at(800);
        assert!(matches!(sim.reply(close), Some(Reply::Order(_))), "{:?}", sim.reply(close));
        assert_eq!([0, 1].map(|v| sim.ex.venues[v].account.position(HYPE).qty), [dec!(0.03), dec!(0)]);
    }

    #[test]
    fn filters_and_margin_refuse_bad_orders() {
        let mut sim = Sim::new();
        let cases = [
            (limit(Side::Buy, dec!(1), dec!(99.001), Tif::PostOnly), Reject::TickSize),
            (limit(Side::Buy, dec!(1.001), dec!(99), Tif::PostOnly), Reject::StepSize),
            (limit(Side::Buy, dec!(0.05), dec!(99), Tif::PostOnly), Reject::MinNotional),
            (limit(Side::Buy, dec!(1), dec!(102.5), Tif::Ioc), Reject::PriceBand),
            (limit(Side::Buy, dec!(11), dec!(99), Tif::PostOnly), Reject::Margin),
        ];
        let tickets: Vec<(u64, Reject)> = cases.into_iter().map(|(r, want)| (sim.send(Venue::Aster, r), want)).collect();
        sim.at(1_000);
        for (ticket, want) in tickets {
            assert_eq!(sim.reply(ticket), Some(&Reply::Reject(want)));
        }
        // On Lighter the sequencer accepts the transaction; execution refuses the order.
        let ticket = sim.tx(limit(Side::Buy, dec!(11), dec!(99), Tif::PostOnly));
        sim.at(1_100);
        assert_eq!(sim.reply(ticket), Some(&Reply::Ok));
        assert_eq!(sim.last_status(Venue::Lighter, 1), Some(Status::Done(End::Rejected(Reject::Margin))));
    }

    #[test]
    fn rate_limits_answer_429_until_the_window_slides() {
        let mut sim = Sim::new();
        let rest = |sim: &mut Sim| sim.ex.submit(Envelope { venue: Venue::Lighter, lane: 3, weight: 1, orders: 0, nonce: None, request: Request::Account });
        let tickets: Vec<u64> = (0..61).map(|_| rest(&mut sim)).collect();
        sim.at(100);
        assert!(tickets[..60].iter().all(|&t| matches!(sim.reply(t), Some(Reply::Account(_)))));
        assert_eq!(sim.reply(tickets[60]), Some(&Reply::Reject(Reject::RateLimited)));
        sim.at(60_100);
        let later = rest(&mut sim);
        sim.at(60_200);
        assert!(matches!(sim.reply(later), Some(Reply::Account(_))));
    }

    #[test]
    fn the_deadman_cancels_unless_re_armed() {
        let mut sim = Sim::new();
        let arm = |countdown_ms| Request::Deadman { market: HYPE.into(), countdown_ms };
        sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(99), Tif::PostOnly));
        sim.send(Venue::Aster, arm(1_000));
        sim.at(500);
        sim.send(Venue::Aster, arm(1_000));
        sim.at(1_200);
        assert_eq!(sim.open(Venue::Aster).len(), 1);
        sim.at(1_700);
        assert!(sim.open(Venue::Aster).is_empty());
        assert_eq!(sim.last_status(Venue::Aster, 1), Some(Status::Done(End::Deadman)));
    }

    #[test]
    fn funding_settles_on_the_position_and_the_books_close() {
        let mut sim = Sim::new();
        sim.send(Venue::Aster, limit(Side::Buy, dec!(2), dec!(102), Tif::Ioc));
        sim.ex.funding(Venue::Aster, HYPE, 1_000 * MS, dec!(0.0001));
        sim.ex.funding(Venue::Aster, HYPE, 1_000 * MS, dec!(0.0002));
        sim.at(2_000);
        // Lighter re-reports its last settlement with every stats frame: charged once.
        sim.ex.funding(Venue::Aster, HYPE, 1_000 * MS, dec!(0.0002));
        sim.at(3_000);
        let account = &sim.ex.venues[0].account;
        // Long 2 at mid 100 pays 0.0002 × 200.
        assert_eq!(account.funding, dec!(-0.04));
        assert_eq!(account.balance, account.initial + account.realized - account.fees + account.funding);
        let fills: Decimal = sim.fills(Venue::Aster).iter().map(|f| f.1).sum();
        assert_eq!(account.position(HYPE).qty, fills);
    }

    #[test]
    fn a_restart_keeps_the_accounts_and_settles_what_happened_while_down() {
        let mut sim = Sim::new();
        // Long 1 at 101; bids at 99.5 and 98.5 under a 1 s deadman; Lighter nonce 0 used; a
        // funding settlement due at 5 s.
        sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(102), Tif::Ioc));
        let crossed = sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(99.5), Tif::PostOnly));
        let expired = sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(98.5), Tif::PostOnly));
        sim.send(Venue::Aster, Request::Deadman { market: HYPE.into(), countdown_ms: 1_000 });
        sim.tx(Request::Noop);
        sim.ex.funding(Venue::Aster, HYPE, 5_000 * MS, dec!(0.0001));
        sim.at(200);
        let (crossed, expired) = (placed(&sim, crossed).id, placed(&sim, expired).id);
        let saved = serde_json::to_string(sim.ex.state()).unwrap();

        // Back at 10 s: the settlement waits for a mark, and the first book, at 10.2 s, shows an
        // ask through the 99.5 bid.
        let mut back = Sim::cold(params(500), 10_000);
        back.ex.restore(serde_json::from_str(&saved).unwrap());
        back.book(Venue::Aster, 10_200, &[(dec!(99), dec!(5))], &[(dec!(99.4), dec!(1)), (dec!(100), dec!(5))]);
        back.at(10_300);
        back.ex.resume();
        let next = back.ex.submit(Envelope { venue: Venue::Lighter, lane: 3, weight: 1, orders: 0, nonce: None, request: Request::NextNonce { key: 9 } });
        let order = back.send(Venue::Aster, limit(Side::Sell, dec!(1), dec!(100.5), Tif::PostOnly));
        back.at(12_000);
        assert_eq!(back.fills(Venue::Aster), vec![(dec!(99.5), dec!(1), true)], "the market crossed the bid while down");
        assert_eq!(back.last_status(Venue::Aster, expired), Some(Status::Done(End::Deadman)));
        assert_eq!(back.last_status(Venue::Aster, crossed), Some(Status::Filled));
        assert_eq!(back.reply(next), Some(&Reply::Nonce(1)));
        assert!(placed(&back, order).id > sim.ex.state()[0].last_id, "ids never repeat");
        let account = &back.ex.venues[0].account;
        // Long 2 at the first mark (99.2) pays 0.0001 of its value, once.
        assert_eq!((account.position(HYPE).qty, account.funding), (dec!(2), dec!(-0.01984)));
        assert_eq!(account.balance, account.initial + account.realized - account.fees + account.funding);
    }

    #[test]
    fn a_lighter_and_hyperliquid_pair_trades_and_restores_in_its_own_slots() {
        let mut p = params(500);
        p.venues = [Venue::Lighter, Venue::Hyperliquid];
        p.fees = [Fees { maker: dec!(0), taker: dec!(0) }, Fees { maker: dec!(0), taker: dec!(0.00045) }];
        let mut sim = Sim::with(p.clone());
        sim.tx(limit(Side::Buy, dec!(1), dec!(102), Tif::Ioc));
        sim.send(Venue::Hyperliquid, limit(Side::Sell, dec!(1), dec!(98), Tif::Ioc));
        sim.at(200);
        // Lighter's speed bump holds its taker, in the first slot as in the second.
        assert_eq!((sim.fills(Venue::Lighter), sim.fills(Venue::Hyperliquid)), (vec![], vec![(dec!(99), dec!(1), false)]));
        sim.at(1_000);
        assert_eq!(sim.fills(Venue::Lighter), vec![(dec!(101), dec!(1), false)]);
        let saved = serde_json::to_string(sim.ex.state()).unwrap();
        let mut back = Sim::cold(p, 2_000);
        back.ex.restore(serde_json::from_str(&saved).unwrap());
        let [lighter, hyperliquid] = back.ex.state().clone().map(|st| st.account);
        assert_eq!((lighter.position(HYPE).qty, lighter.fees), (dec!(1), dec!(0)));
        assert_eq!((hyperliquid.position(HYPE).qty, hyperliquid.fees), (dec!(-1), dec!(0.04455)), "Hyperliquid's own fee");
        assert_eq!(back.ex.peek(Venue::Hyperliquid).0.positions[0].qty, dec!(-1));
    }

    #[test]
    fn quantiles_take_the_nearest_rank() {
        let samples: Vec<f64> = (1..=200).map(f64::from).collect();
        assert_eq!(quantiles(samples), json!({"n": 200, "p50": 100.0, "p90": 180.0, "p99": 198.0, "max": 200.0}));
        assert_eq!(quantiles(vec![]), json!({"n": 0}));
    }

    #[test]
    fn results_do_not_depend_on_the_shift() {
        // The same market and the same orders, sent at the same shifted times, under two shifts.
        fn session(shift_ms: i64) -> (Vec<Output>, Vec<Account>) {
            let mut p = params(shift_ms);
            p.rtt = [Latency::try_from([100.0, 164.0]).unwrap(), Latency::try_from([8.0, 36.0]).unwrap()];
            let mut sim = Sim::with(p);
            for i in 0..20 {
                let ms = 100 * i;
                let px = dec!(99) + Decimal::from(i % 3) / dec!(10);
                let top = BookUpdate::Top { bid: (px, dec!(3)), ask: (px + dec!(1.5), dec!(2)) };
                sim.feed(Venue::Aster, ms + 5, FeedEvent::Book(top));
                sim.print(Venue::Aster, ms + 7, px, dec!(4), Side::Sell);
                sim.delta(Venue::Lighter, ms + 9, &[(px, dec!(4))], &[(px + dec!(1), dec!(3))]);
            }
            for i in 0..8 {
                sim.at(200 * i);
                sim.send(Venue::Aster, limit(Side::Buy, dec!(1), dec!(99.1), Tif::PostOnly));
                sim.tx(limit(Side::Buy, dec!(0.5), dec!(101), Tif::Ioc));
            }
            sim.at(3_000);
            let shift = shift_ms * MS;
            let unshift = |mut order: Order| {
                order.created_us -= shift;
                order.updated_us -= shift;
                order
            };
            let outputs = sim.out.into_iter().map(|o| match o {
                Output::Event { venue, event: Event::Order { order, fill, account } } => {
                    let fill = fill.map(|f| Fill { at_us: f.at_us - shift, ..f });
                    Output::Event { venue, event: Event::Order { order: unshift(order), fill, account } }
                }
                Output::Reply { ticket, reply: Reply::Order(order) } => {
                    Output::Reply { ticket, reply: Reply::Order(unshift(order)) }
                }
                other => other,
            }).collect();
            (outputs, sim.ex.venues.iter().map(|st| st.account.clone()).collect())
        }
        let (a, accounts_a) = session(300);
        let (b, accounts_b) = session(900);
        let fills = a.iter().filter(|o| matches!(o, Output::Event { event: Event::Order { fill: Some(_), .. }, .. })).count();
        assert!(fills >= 8, "the session should trade on both venues, got {fills} fills");
        assert_eq!(a, b);
        assert_eq!(accounts_a, accounts_b);
    }
}

//! `collect`: follows every pair's best bid/offer and trades on both venues, and records only the
//! moments the report can trade on, plus 5-minute summaries.
//!
//! What is recorded follows from `[report]` at its loosest (a `Recording`, in each file's header),
//! so the report can score the data with any settings that ask no less: the other Lighter tier,
//! other latencies, a higher margin, percentile or XEMM edge. The report checks this per file.
//!
//! - **Taker.** The bot trades when the edge reaches its gate: the 90th percentile of its samples
//!   (the edge, at most once a second, when it clears `required`) over the last 72 h. The summaries
//!   count the samples by edge from the pair's lowest `required`, and a state is recorded when its
//!   edge reaches the gate over the windows before. That is never above the report's gate, whose
//!   samples are the top of the same ones.
//! - **XEMM.** A trade is recorded, with the states of the second before it (what a resting quote
//!   was priced from), when it could have filled a quote priced from one of them (`could_fill`).
//! - After either, every state for two seconds: where latency-delayed orders fill.
//!
//! Lines, tab-separated, times in ms since the epoch at arrival on this host (never decreasing),
//! Right-leg prices and sizes in left-leg units (divided and multiplied by the pair's scale):
//!
//! | kind | fields |
//! |---|---|
//! | `P` | JSON: version 2, the `[collect]` settings, the `Recording`, and the constants below |
//! | `U` | JSON: the pairs (`universe::Pair`) |
//! | `B` | time, qualified pair, left bid, ask, right bid, ask (0 = unknown), `depth_flags` |
//! | `T` | time, pair, leg `0`/`1` (legacy `A`/`L`), price, size, aggressor `B`/`S`: a trade that could fill a quote |
//! | `G` | time, leg `0`/`1`: the run ended (a connection lost mid-run shows as unknown books) |
//! | `S` | JSON: one pair's 5-minute summary (`Summary::line`), with its gate samples |
//!
//! `B` lines come in time order, except a pre-roll written after later states: sort them by time.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, LighterTier, Report};
use crate::feeds::{self, Wire};
use crate::store::{self, Store};
use crate::universe::{self, Market, Pair, Venue};

pub const PREROLL_MS: i64 = 1_000;
pub const TAIL_MS: i64 = 2_000;
pub const WINDOW_MS: i64 = 300_000;
const DAY_MS: i64 = 86_400_000;
/// Gate samples are counted per edge bin this wide (the report's gate is exact to a bin).
pub const EDGE_BIN_BPS: f64 = 0.25;
const QUEUE: usize = 50_000;

pub fn wall_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A best bid and offer; zeros mean unknown.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Bbo {
    pub bid: f64,
    pub bid_size: f64,
    pub ask: f64,
    pub ask_size: f64,
}

impl Bbo {
    pub fn known(&self) -> bool {
        self.bid.is_finite() && self.ask.is_finite() && self.bid > 0.0 && self.ask >= self.bid
    }

    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }

    /// The smaller of the two sides' notional.
    pub fn top_usd(&self) -> f64 {
        (self.bid * self.bid_size).min(self.ask * self.ask_size)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Position within a pair, independent of the exchange. `a`/`l` book fields below mean left/right.
pub enum Leg {
    Left,
    Right,
}

impl Leg {
    pub fn code(self) -> &'static str {
        match self {
            Leg::Left => "0",
            Leg::Right => "1",
        }
    }
}

/// Which sides hold `depth_usd`, as bits: 1 left bid, 2 left ask, 4 right bid, 8 right ask.
/// That is all the report asks of book sizes (`best_edge`, the XEMM hedge), at the recorded depth
/// (`Recording::check`), in 40% less disk than the sizes.
pub fn depth_flags(a: &Bbo, l: &Bbo, depth_usd: f64) -> u8 {
    let sides = [a.bid * a.bid_size, a.ask * a.ask_size, l.bid * l.bid_size, l.ask * l.ask_size];
    sides.iter().enumerate().map(|(i, &usd)| ((usd >= depth_usd) as u8) << i).sum()
}

/// The better direction's executable taker edge, bps of the left mid, as the bot's taker computes
/// it (over its depth; here at the top of book, where both sides of a direction must hold
/// `depth_usd`): (buy left and sell right?, edge).
pub fn best_edge(a: &Bbo, l: &Bbo, depth_usd: f64) -> Option<(bool, f64)> {
    if !a.known() || !l.known() {
        return None;
    }
    let mid = a.mid();
    let deep = |px: f64, size: f64| px * size >= depth_usd;
    let buy_left = (deep(a.ask, a.ask_size) && deep(l.bid, l.bid_size)).then(|| (true, (l.bid - a.ask) / mid * 1e4));
    let sell_aster = (deep(a.bid, a.bid_size) && deep(l.ask, l.ask_size)).then(|| (false, (a.bid - l.ask) / mid * 1e4));
    match (buy_left, sell_aster) {
        (Some(b), Some(s)) => Some(if b.1 >= s.1 { b } else { s }),
        (b, s) => b.or(s),
    }
}

/// Could this trade on `venue` have filled a maker quote there priced from `(a, l)`, as the report
/// prices one (`simulate_xemm`, fees ≥ 0, required edge ≥ `min_bps`)? Only by printing strictly
/// through the venue's own touch (a quote never betters it) and `min_bps` of the mean mid through
/// the other venue's price, whose side holds `depth_usd` for the hedge.
pub fn could_fill(venue: Leg, buy: bool, price: f64, a: &Bbo, l: &Bbo, min_bps: f64, depth_usd: f64) -> bool {
    let (own, other) = match venue {
        Leg::Left => (a, l),
        Leg::Right => (l, a),
    };
    if !(own.known() && other.known()) {
        return false;
    }
    let reference = (a.mid() + l.mid()) / 2.0;
    let min = min_bps / 1e4 * reference;
    if buy {
        price > own.ask && price > other.ask + min && other.ask * other.ask_size >= depth_usd
    } else {
        price < own.bid && price < other.bid - min && other.bid * other.bid_size >= depth_usd
    }
}

pub fn edge_bin(bps: f64) -> i32 {
    (bps / EDGE_BIN_BPS).floor() as i32
}

/// Gate samples counted per edge bin (`edge_bin`).
pub type Samples = BTreeMap<i32, u64>;

/// The bot's entry gate over the samples of whole windows.
#[derive(Debug, Default)]
pub struct Gate {
    windows: VecDeque<(i64, Samples)>,
    total: Samples,
}

impl Gate {
    /// Adds a window's samples; windows come in time order.
    pub fn push(&mut self, start: i64, samples: Samples) {
        for (&bin, &n) in &samples {
            *self.total.entry(bin).or_default() += n;
        }
        self.windows.push_back((start, samples));
    }

    /// Forgets the windows that started before `since`.
    pub fn expire(&mut self, since: i64) {
        while self.windows.front().is_some_and(|(start, _)| *start < since) {
            let (_, samples) = self.windows.pop_front().expect("checked");
            for (bin, n) in samples {
                let total = self.total.get_mut(&bin).expect("counted when pushed");
                *total -= n;
                if *total == 0 {
                    self.total.remove(&bin);
                }
            }
        }
    }

    /// The `percentile` (nearest rank, as the bot's) of the samples from `floor`'s bin up, as its
    /// bin's lower edge; None under `min_samples`.
    pub fn level(&self, floor: f64, percentile: f64, min_samples: usize) -> Option<f64> {
        let above = || self.total.range(edge_bin(floor)..);
        let n: u64 = above().map(|(_, n)| n).sum();
        if n == 0 || n < min_samples as u64 {
            return None;
        }
        let rank = ((percentile / 100.0 * n as f64).ceil() as u64).clamp(1, n);
        let mut seen = 0;
        above().find(|&(_, &k)| {
            seen += k;
            seen >= rank
        })
        .map(|(&bin, _)| bin as f64 * EDGE_BIN_BPS)
    }
}

/// What the data holds: `[report]` at its loosest. Written in each file's `P` line; the report
/// refuses settings that ask for more (`check`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recording {
    /// Missing in v1: those files have a one-second tail.
    #[serde(default = "legacy_span")]
    pub tail_ms: i64,
    #[serde(default = "legacy_span")]
    pub preroll_ms: i64,
    /// A taker side, or an XEMM hedge, counts with this much at the top (10 x the clip).
    pub depth_usd: f64,
    pub gate_percentile: f64,
    pub gate_window_hours: f64,
    pub gate_min_samples: usize,
    pub sample_interval_ms: i64,
    /// The lowest XEMM required edge scored.
    pub xemm_min_bps: f64,
    /// Per pair, the lowest taker `required` (at the cheaper Lighter tier): the samples start there.
    pub floors: BTreeMap<String, f64>,
}

fn legacy_span() -> i64 { 1_000 }

impl Recording {
    pub fn new(cfg: &Report, pairs: &[Pair]) -> Recording {
        let (standard, premium) = (&cfg.lighter.standard, &cfg.lighter.premium);
        let cheaper = if standard.taker_bps <= premium.taker_bps { standard } else { premium };
        let t = &cfg.taker;
        Recording {
            tail_ms: TAIL_MS,
            preroll_ms: PREROLL_MS,
            depth_usd: cfg.depth_usd(),
            gate_percentile: t.gate_percentile,
            gate_window_hours: t.gate_window_hours,
            gate_min_samples: t.gate_min_samples,
            sample_interval_ms: t.sample_interval_ms,
            xemm_min_bps: cfg.xemm_min_bps(),
            floors: pairs.iter().map(|p| (p.name.clone(), cfg.taker_required_bps(p, cheaper))).collect(),
        }
    }

    pub fn check_latency(&self, cfg: &Report, pair: &Pair, latency: f64) -> Result<()> {
        let tier = cfg.lighter()?;
        let a = cfg.costs(&pair.left, tier);
        let l = cfg.costs(&pair.right, tier);
        let tail = a.taker_ms.max(l.taker_ms).max(a.notice_ms + l.taker_ms).max(l.notice_ms + a.taker_ms);
        let before = (a.quote_age_ms * latency + a.print_delay_ms).max(l.quote_age_ms * latency + l.print_delay_ms);
        ensure!(tail * latency <= self.tail_ms as f64 && before <= self.preroll_ms as f64,
            "{}: latency x{latency} exceeds recorded {} ms before / {} ms after; lower --latency or score newer data", pair.name, self.preroll_ms, self.tail_ms);
        Ok(())
    }

    pub fn span_ms(&self) -> i64 {
        (self.gate_window_hours * 3.6e6) as i64
    }

    /// Fails if `cfg`, at the `lighter` tier, would trade on moments this data did not record.
    pub fn check(&self, cfg: &Report, lighter: &LighterTier, pairs: &BTreeMap<String, Pair>) -> Result<()> {
        let t = &cfg.taker;
        ensure!(cfg.depth_usd() == self.depth_usd, "recorded for a depth of ${}, not ${}", self.depth_usd, cfg.depth_usd());
        ensure!(
            t.gate_window_hours == self.gate_window_hours && t.sample_interval_ms == self.sample_interval_ms,
            "recorded for a gate over {} h of samples every {} ms",
            self.gate_window_hours,
            self.sample_interval_ms
        );
        ensure!(t.gate_percentile >= self.gate_percentile, "recorded from the gate's P{} up, not P{}", self.gate_percentile, t.gate_percentile);
        ensure!(cfg.xemm_min_bps() >= self.xemm_min_bps, "recorded for XEMM edges from {} bps, not {}", self.xemm_min_bps, cfg.xemm_min_bps());
        for (name, &floor) in &self.floors {
            if let Some(pair) = pairs.get(name) {
                let required = cfg.taker_required_bps(pair, lighter);
                ensure!(required >= floor, "{name}: recorded for a taker required edge from {floor} bps, not {required}");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Book { pair: usize, venue: Leg, bbo: Bbo },
    Trade { pair: usize, venue: Leg, price: f64, size: f64, buy: bool },
}

/// Time-weighted over the seconds both venues' states were known ("up").
#[derive(Debug, Default)]
struct Summary {
    up: f64,
    down: f64,
    spread_a: f64,
    spread_l: f64,
    top_a: f64,
    top_l: f64,
    basis: f64,
    basis_min: f64,
    basis_max: f64,
    thin: f64,
    a_trades: u64,
    a_usd: f64,
    l_trades: u64,
    l_usd: f64,
    rows: u64,
    samples: Samples,
}

impl Summary {
    fn line(&self, start_ms: i64, pair: &str) -> String {
        let avg = |sum: f64| (self.up > 0.0).then(|| round(sum / self.up));
        let json = json!({
            "t": start_ms, "p": pair, "up": round(self.up), "down": round(self.down),
            "spread_a": avg(self.spread_a), "spread_l": avg(self.spread_l),
            "top_a": avg(self.top_a), "top_l": avg(self.top_l),
            "basis": avg(self.basis), "basis_min": (self.up > 0.0).then(|| round(self.basis_min)),
            "basis_max": (self.up > 0.0).then(|| round(self.basis_max)), "thin": avg(self.thin),
            "a_trades": self.a_trades, "a_usd": round(self.a_usd), "l_trades": self.l_trades, "l_usd": round(self.l_usd),
            "rows": self.rows, "samples": self.samples,
        });
        format!("S\t{json}")
    }
}

fn round(x: f64) -> f64 {
    (x * 1e3).round() / 1e3
}

struct PairState {
    name: String,
    /// Where the gate samples start (`Recording::floors`).
    floor: f64,
    a: Bbo,
    l: Bbo,
    /// States (time, left, right, written?) from the one in force `PREROLL_MS` ago to now.
    ring: VecDeque<(i64, Bbo, Bbo, bool)>,
    /// States are written until then.
    hot_until: i64,
    last_t: i64,
    last_sample: i64,
    gate: Gate,
    /// Samples an earlier run counted in this window (a restart), for the gate when it closes.
    earlier: Samples,
    /// A state whose edge reaches this is recorded: the gate at the floor, or the floor in warmup.
    trigger: f64,
    summary: Summary,
}

pub struct Collector {
    rec: Recording,
    pairs: Vec<PairState>,
    window_start: i64,
    now: i64,
}

impl Collector {
    /// `history`: each pair's gate before `now`'s window and the samples already counted in it
    /// (`load_gates`).
    pub fn new(rec: &Recording, pairs: &[Pair], mut history: HashMap<String, (Gate, Samples)>, now: i64) -> Collector {
        let pairs = pairs
            .iter()
            .map(|p| {
                let floor = rec.floors[&p.name];
                let (gate, earlier) = history.remove(&p.name).unwrap_or_default();
                let trigger = gate.level(floor, rec.gate_percentile, rec.gate_min_samples).unwrap_or(floor);
                PairState {
                    name: p.name.clone(),
                    floor,
                    a: Bbo::default(),
                    l: Bbo::default(),
                    // Unknown since the start: a pre-roll reaching back before it says so.
                    ring: VecDeque::from([(now, Bbo::default(), Bbo::default(), false)]),
                    hot_until: i64::MIN,
                    last_t: now,
                    last_sample: i64::MIN / 2,
                    gate,
                    earlier,
                    trigger,
                    summary: Summary::default(),
                }
            })
            .collect();
        Collector { rec: rec.clone(), pairs, window_start: now - now.rem_euclid(WINDOW_MS), now }
    }

    pub fn on_event(&mut self, t: i64, event: Event, out: &mut impl FnMut(String)) {
        let t = self.tick(t, out);
        match event {
            Event::Book { pair, venue, bbo } => {
                self.advance(pair, t);
                let p = &mut self.pairs[pair];
                match venue {
                    Leg::Left => p.a = bbo,
                    Leg::Right => p.l = bbo,
                }
                self.changed(pair, t, out);
            }
            Event::Trade { pair, venue, price, size, buy } => {
                let (min_bps, depth_usd) = (self.rec.xemm_min_bps, self.rec.depth_usd);
                let p = &mut self.pairs[pair];
                let (trades, usd) = match venue {
                    Leg::Left => (&mut p.summary.a_trades, &mut p.summary.a_usd),
                    Leg::Right => (&mut p.summary.l_trades, &mut p.summary.l_usd),
                };
                *trades += 1;
                *usd += price * size;
                if p.ring.iter().any(|(_, a, l, _)| could_fill(venue, buy, price, a, l, min_bps, depth_usd)) {
                    self.write_since(pair, t - PREROLL_MS, out);
                    let p = &mut self.pairs[pair];
                    p.hot_until = t + TAIL_MS;
                    let side = if buy { "B" } else { "S" };
                    out(format!("T\t{t}\t{}\t{}\t{price}\t{size}\t{side}", p.name, venue.code()));
                }
            }
        }
    }

    /// `venue`'s connection ended: its states are unknown until they update again.
    pub fn closed(&mut self, venue: Leg, t: i64, out: &mut impl FnMut(String)) {
        let t = self.tick(t, out);
        out(format!("G\t{t}\t{}", venue.code()));
        for pair in 0..self.pairs.len() {
            self.advance(pair, t);
            let p = &mut self.pairs[pair];
            match venue {
                Leg::Left => p.a = Bbo::default(),
                Leg::Right => p.l = Bbo::default(),
            }
            self.changed(pair, t, out);
        }
    }

    /// Moves the clock to `t` (never back: stamps from two connections can cross) and writes the
    /// summaries of every window that ended by then. Returns the clock.
    pub fn tick(&mut self, t: i64, out: &mut impl FnMut(String)) -> i64 {
        self.now = self.now.max(t);
        while self.now >= self.window_start + WINDOW_MS {
            self.close_window(self.window_start + WINDOW_MS, out);
        }
        self.now
    }

    /// Ends the run at `t`: both connections close (a tail cut short says so), and the window
    /// running is summarized, cut short there.
    pub fn finish(&mut self, t: i64, out: &mut impl FnMut(String)) {
        // The closing mark must also survive compression, even if the last market move was cold.
        for pair in 0..self.pairs.len() { self.write_since(pair, t, out); }
        self.closed(Leg::Left, t, out);
        self.closed(Leg::Right, t, out);
        let t = self.tick(t, out);
        self.close_window(t, out);
    }

    /// Writes the window's summaries, adds its samples to the gates and moves the triggers.
    fn close_window(&mut self, end: i64, out: &mut impl FnMut(String)) {
        let (span, percentile, min_samples) = (self.rec.span_ms(), self.rec.gate_percentile, self.rec.gate_min_samples);
        for pair in 0..self.pairs.len() {
            self.advance(pair, end);
            let p = &mut self.pairs[pair];
            out(p.summary.line(self.window_start, &p.name));
            let mut samples = std::mem::take(&mut p.summary.samples);
            for (bin, n) in std::mem::take(&mut p.earlier) {
                *samples.entry(bin).or_default() += n;
            }
            p.gate.push(self.window_start, samples);
            p.gate.expire(end - span);
            p.trigger = p.gate.level(p.floor, percentile, min_samples).unwrap_or(p.floor);
            p.summary = Summary::default();
        }
        self.window_start = end;
    }

    /// Accumulates the summary over the time since the pair's last change.
    fn advance(&mut self, pair: usize, t: i64) {
        let depth_usd = self.rec.depth_usd;
        let p = &mut self.pairs[pair];
        let dt = (t - p.last_t).max(0) as f64 / 1e3;
        p.last_t = p.last_t.max(t);
        let s = &mut p.summary;
        if dt == 0.0 {
            return;
        }
        let (a, l) = (&p.a, &p.l);
        if !(a.known() && l.known()) {
            s.down += dt;
            return;
        }
        let basis = (l.mid() - a.mid()) / a.mid() * 1e4;
        if s.up == 0.0 {
            (s.basis_min, s.basis_max) = (basis, basis);
        }
        s.up += dt;
        s.spread_a += dt * (a.ask - a.bid) / a.mid() * 1e4;
        s.spread_l += dt * (l.ask - l.bid) / l.mid() * 1e4;
        s.top_a += dt * a.top_usd();
        s.top_l += dt * l.top_usd();
        s.basis += dt * basis;
        s.basis_min = s.basis_min.min(basis);
        s.basis_max = s.basis_max.max(basis);
        if a.top_usd().min(l.top_usd()) < depth_usd {
            s.thin += dt;
        }
    }

    /// The pair's state changed at `t`: keep it for the pre-roll, sample it for the gate as the bot
    /// does, and record it if it reaches the trigger or falls in a tail.
    fn changed(&mut self, pair: usize, t: i64, out: &mut impl FnMut(String)) {
        let (depth_usd, interval) = (self.rec.depth_usd, self.rec.sample_interval_ms);
        let p = &mut self.pairs[pair];
        p.ring.push_back((t, p.a, p.l, false));
        while p.ring.len() >= 2 && p.ring[1].0 <= t - PREROLL_MS {
            p.ring.pop_front();
        }
        let edge = best_edge(&p.a, &p.l, depth_usd).map(|(_, e)| e);
        if let Some(e) = edge.filter(|&e| e >= p.floor && t - p.last_sample >= interval) {
            *p.summary.samples.entry(edge_bin(e)).or_default() += 1;
            p.last_sample = t;
        }
        let trigger = edge.is_some_and(|e| e >= p.trigger);
        if trigger || t <= p.hot_until {
            self.write_since(pair, t, out);
        }
        if trigger {
            self.pairs[pair].hot_until = t + TAIL_MS;
        }
    }

    /// Writes the pair's states in force at `from` or later that are not written yet. A state
    /// replaced within its millisecond is skipped: no lookup by time can return it.
    fn write_since(&mut self, pair: usize, from: i64, out: &mut impl FnMut(String)) {
        let depth_usd = self.rec.depth_usd;
        let p = &mut self.pairs[pair];
        let start = p.ring.iter().rposition(|s| s.0 <= from).unwrap_or(0);
        for i in start..p.ring.len() {
            let replaced_at_once = p.ring.get(i + 1).is_some_and(|next| next.0 == p.ring[i].0);
            let (at, a, l, written) = &mut p.ring[i];
            if !*written && !replaced_at_once {
                out(state_line(*at, &p.name, a, l, depth_usd));
                *written = true;
                p.summary.rows += 1;
            }
        }
    }
}

fn state_line(t: i64, pair: &str, a: &Bbo, l: &Bbo, depth_usd: f64) -> String {
    format!("B\t{t}\t{pair}\t{}\t{}\t{}\t{}\t{}", a.bid, a.ask, l.bid, l.ask, depth_flags(a, l, depth_usd))
}

/// The gate history for a run starting at `now`, from the summaries in `dir`: per pair, its gate
/// over the windows before `now`'s, and the samples an earlier run already counted in `now`'s. A
/// file that cannot be read is skipped (the gate then starts lower, which records more).
pub fn load_gates(dir: &Path, span_ms: i64, now: i64) -> HashMap<String, (Gate, Samples)> {
    let since = now - now.rem_euclid(WINDOW_MS) - span_ms;
    let first_day = chrono::DateTime::from_timestamp_millis(since).map(|d| d.format("%Y-%m-%d").to_string());
    let mut gates = HashMap::new();
    for file in store::files(dir, first_day.as_deref(), None).unwrap_or_default() {
        match store::read(&file) {
            Ok(lines) => add_gates(&mut gates, lines, span_ms, now),
            Err(e) => tracing::warn!("gate history: skipping {}: {e:#}", file.display()),
        }
    }
    gates
}

/// Adds the summaries among one file's `lines` to `gates` (`load_gates`).
pub fn add_gates(gates: &mut HashMap<String, (Gate, Samples)>, lines: impl Iterator<Item = String>, span_ms: i64, now: i64) {
    #[derive(Deserialize)]
    struct Window {
        t: i64,
        p: String,
        samples: Samples,
    }
    let current = now - now.rem_euclid(WINDOW_MS);
    for line in lines {
        let Some(w) = line.strip_prefix("S\t").and_then(|json| serde_json::from_str::<Window>(json).ok()) else { continue };
        let (gate, earlier) = gates.entry(universe::qualified(&w.p)).or_default();
        if w.t >= current {
            for (bin, n) in w.samples {
                *earlier.entry(bin).or_default() += n;
            }
        } else if w.t >= current - span_ms {
            gate.push(w.t, w.samples);
        }
    }
}

#[derive(Deserialize)]
struct AsterFrame {
    data: AsterData,
}

#[derive(Deserialize)]
#[serde(tag = "e")]
enum AsterData {
    #[serde(rename = "bookTicker")]
    Book {
        s: String,
        b: String,
        #[serde(rename = "B")]
        bid_size: String,
        a: String,
        #[serde(rename = "A")]
        ask_size: String,
    },
    #[serde(rename = "aggTrade")]
    Trade { s: String, p: String, q: String, m: bool },
}

/// An Aster combined-stream frame for one of `index`'s symbols.
pub fn aster_event(text: &str, index: &HashMap<String, usize>) -> Result<Option<Event>> {
    let frame: AsterFrame = serde_json::from_str(text)?;
    Ok(match frame.data {
        AsterData::Book { s, b, bid_size, a, ask_size } => index.get(&s).map(|&pair| -> Result<Event> {
            let bbo = Bbo { bid: b.parse()?, bid_size: bid_size.parse()?, ask: a.parse()?, ask_size: ask_size.parse()? };
            Ok(Event::Book { pair, venue: Leg::Left, bbo })
        }),
        // `m`: the buyer was the maker, so the aggressor sold.
        AsterData::Trade { s, p, q, m } => {
            index.get(&s).map(|&pair| -> Result<Event> { Ok(Event::Trade { pair, venue: Leg::Left, price: p.parse()?, size: q.parse()?, buy: !m }) })
        }
    }
    .transpose()?)
}

#[derive(Deserialize)]
struct LighterFrame {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    channel: String,
    ticker: Option<LighterTicker>,
    #[serde(default)]
    trades: Vec<LighterTrade>,
    #[serde(default)]
    liquidation_trades: Vec<LighterTrade>,
}

#[derive(Deserialize)]
struct LighterTicker {
    a: LighterLevel,
    b: LighterLevel,
}

#[derive(Deserialize)]
struct LighterLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct LighterTrade {
    price: String,
    size: String,
    is_maker_ask: bool,
}

/// A Lighter frame's events for `index`'s markets (id -> market). The trades
/// sent on subscribing are history and are skipped; liquidations fill resting orders like any trade.
pub fn lighter_events(text: &str, index: &HashMap<u32, usize>) -> Result<Vec<Event>> {
    let frame: LighterFrame = serde_json::from_str(text)?;
    let Some(&pair) = frame.channel.split_once(':').and_then(|(_, id)| id.parse().ok()).and_then(|id: u32| index.get(&id)) else {
        return Ok(Vec::new());
    };
    let mut events = Vec::new();
    match frame.kind.as_str() {
        "subscribed/ticker" | "update/ticker" => {
            if let Some(t) = frame.ticker {
                let bbo = Bbo {
                    bid: t.b.price.parse::<f64>()?,
                    bid_size: t.b.size.parse::<f64>()?,
                    ask: t.a.price.parse::<f64>()?,
                    ask_size: t.a.size.parse::<f64>()?,
                };
                events.push(Event::Book { pair, venue: Leg::Right, bbo });
            }
        }
        "update/trade" => {
            for t in frame.trades.iter().chain(&frame.liquidation_trades) {
                let (price, size) = (t.price.parse::<f64>()?, t.size.parse::<f64>()?);
                // The maker was the ask, so the aggressor bought.
                events.push(Event::Trade { pair, venue: Leg::Right, price, size, buy: t.is_maker_ask });
            }
        }
        _ => {}
    }
    Ok(events)
}

/// Records until `stop`: each UTC day is one run (a new file) with that day's pairs.
pub async fn run(cfg: Config, dir: PathBuf, stop: CancellationToken) -> Result<()> {
    while !stop.is_cancelled() {
        // A boot can come before the network: waiting here resumes within seconds of its return.
        let (pairs, mismatched) = loop {
            match universe::discover(&cfg.collect).await {
                Ok(found) => break found,
                Err(e) => tracing::warn!("discovering the pairs: {e:#}"),
            }
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        };
        let names: Vec<&str> = pairs.iter().map(|p| p.name.as_str()).collect();
        tracing::info!("{} pairs: {}", pairs.len(), names.join(" "));
        if !mismatched.is_empty() {
            tracing::info!("same name, different price (skipped): {mismatched:?}");
        }
        let rec = Recording::new(&cfg.report, &pairs);
        let now = wall_ms();
        let gates = tokio::task::block_in_place(|| load_gates(&dir, rec.span_ms(), now));
        tracing::info!("gate history: {} pairs, loaded in {:.1} s", gates.len(), (wall_ms() - now) as f64 / 1e3);
        let params = json!({ "version": 2, "collect": &cfg.collect, "recording": &rec, "preroll_ms": PREROLL_MS, "tail_ms": TAIL_MS, "window_ms": WINDOW_MS, "edge_bin_bps": EDGE_BIN_BPS });
        let header = vec![format!("P\t{params}"), format!("U\t{}", serde_json::to_string(&pairs)?)];
        let (store, writer) = Store::open(&dir, header)?;
        let day_end = now - now.rem_euclid(DAY_MS) + DAY_MS;
        let run = stop.child_token();
        // A venue down at discovery (a boot before its network, a blip at midnight) rejoins within
        // minutes of answering again, in a new run, rather than at the next UTC day.
        if venues(&pairs) < 3 {
            let (collect, run) = (cfg.collect.clone(), run.clone());
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = run.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(300)) => {}
                    }
                    if universe::discover(&collect).await.is_ok_and(|(pairs, _)| venues(&pairs) == 3) {
                        tracing::info!("every venue answers again: a new run adds its routes");
                        return run.cancel();
                    }
                }
            });
        }
        run_day(Collector::new(&rec, &pairs, gates, now), &pairs, &store, day_end, &run).await;
        run.cancel();
        store.stop();
        tokio::task::spawn_blocking(move || writer.join()).await?.map_err(|_| anyhow::anyhow!("the store writer panicked"))??;
    }
    Ok(())
}

fn venues(pairs: &[Pair]) -> usize {
    pairs.iter().flat_map(|p| [p.left.venue, p.right.venue]).collect::<std::collections::HashSet<_>>().len()
}

/// Unique venue instruments and their destinations. One subscription supplies several routes.
fn routing(pairs: &[Pair]) -> (Vec<Market>, Vec<Vec<(usize, Leg, f64)>>) {
    let mut markets = Vec::new();
    let mut destinations: Vec<Vec<(usize, Leg, f64)>> = Vec::new();
    let mut index = HashMap::new();
    for (pair, p) in pairs.iter().enumerate() {
        for (market, leg, scale) in [(&p.left, Leg::Left, 1.0), (&p.right, Leg::Right, p.scale)] {
            let i = *index.entry((market.venue, market.symbol.clone())).or_insert_with(|| {
                markets.push(market.clone()); destinations.push(Vec::new()); markets.len() - 1
            });
            destinations[i].push((pair, leg, scale));
        }
    }
    (markets, destinations)
}

fn deliver(collector: &mut Collector, t: i64, event: Event, destinations: &[Vec<(usize, Leg, f64)>], out: &mut impl FnMut(String)) {
    let market = match event { Event::Book { pair, .. } | Event::Trade { pair, .. } => pair };
    for &(pair, venue, scale) in &destinations[market] {
        let event = match event {
            Event::Book { bbo, .. } => Event::Book { pair, venue, bbo: Bbo {
                bid: bbo.bid / scale, ask: bbo.ask / scale, bid_size: bbo.bid_size * scale, ask_size: bbo.ask_size * scale,
            } },
            Event::Trade { price, size, buy, .. } => Event::Trade { pair, venue, price: price / scale, size: size * scale, buy },
        };
        collector.on_event(t, event, out);
    }
}

async fn run_day(mut collector: Collector, pairs: &[Pair], store: &Store, day_end: i64, stop: &CancellationToken) {
    enum Incoming { Opened, Closed, Text(String) }
    let day = stop.child_token();
    let (markets, destinations) = routing(pairs);
    let (tx, mut rx) = mpsc::channel::<(usize, i64, Incoming)>(QUEUE);
    let mut tasks = tokio::task::JoinSet::new();
    let mut connections = Vec::new();
    for venue in [Venue::Aster, Venue::Lighter, Venue::Hyperliquid] {
        let ids: Vec<usize> = markets.iter().enumerate().filter(|(_, m)| m.venue == venue).map(|(i, _)| i).collect();
        let chunk = match venue { Venue::Aster => feeds::ASTER_STREAMS_PER_CONNECTION / 2, Venue::Lighter => feeds::LIGHTER_SUBSCRIPTIONS_PER_CONNECTION / 2, Venue::Hyperliquid => 300 };
        for ids in ids.chunks(chunk) {
            let (url, subscribe) = match venue {
                Venue::Aster => (format!("{}/stream?streams={}", feeds::ASTER_WS, ids.iter().flat_map(|&i| ["bookTicker", "aggTrade"].map(|s| format!("{}@{s}", markets[i].symbol.to_lowercase()))).collect::<Vec<_>>().join("/")), Vec::new()),
                Venue::Lighter => (feeds::LIGHTER_WS.to_string(), ids.iter().flat_map(|&i| ["ticker", "trade"].map(|c| json!({"type":"subscribe", "channel":format!("{c}/{}", markets[i].id.expect("Lighter market id"))}).to_string())).collect()),
                Venue::Hyperliquid => (feeds::HYPERLIQUID_WS.to_string(), ids.iter().flat_map(|&i| [json!({"type":"bbo","coin":markets[i].symbol}), json!({"type":"l2Book","coin":markets[i].symbol,"fast":true}), json!({"type":"trades","coin":markets[i].symbol})].map(|subscription| json!({"method":"subscribe", "subscription":subscription}).to_string())).collect()),
            };
            let connection = connections.len();
            connections.push((venue, ids.to_vec()));
            let (tx, day) = (tx.clone(), day.clone());
            tasks.spawn(async move {
                let mut dropped = 0u64;
                feeds::follow(&url, &format!("{venue:?}"), &subscribe, &day, |wire| {
                    let message = match wire { Wire::Opened => Incoming::Opened, Wire::Closed => Incoming::Closed, Wire::Text(text) => Incoming::Text(text.to_string()) };
                    if tx.try_send((connection, wall_ms(), message)).is_err() {
                        dropped += 1;
                        if dropped.is_power_of_two() { tracing::warn!("{venue:?}: collector behind; {dropped} frames dropped"); }
                    }
                }).await
            });
        }
    }
    drop(tx);
    let aster_index: HashMap<String, usize> = markets.iter().enumerate().filter(|(_, m)| m.venue == Venue::Aster).map(|(i, m)| (m.symbol.clone(), i)).collect();
    let lighter_index: HashMap<u32, usize> = markets.iter().enumerate().filter(|(_, m)| m.venue == Venue::Lighter).map(|(i, m)| (m.id.unwrap(), i)).collect();
    let hl_index: HashMap<String, usize> = markets.iter().enumerate().filter(|(_, m)| m.venue == Venue::Hyperliquid).map(|(i, m)| (m.symbol.clone(), i)).collect();
    let mut hl = crate::hyperliquid::Feed::default();
    let mut out = |line: String| store.line(line);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut traffic = [(0u64, 0u64); 3];
    let mut next_log = wall_ms() + WINDOW_MS;
    loop {
        tokio::select! {
            item = rx.recv() => {
                let Some((connection, t, message)) = item else { break };
                let (venue, ids) = &connections[connection];
                let events = match message {
                    Incoming::Opened | Incoming::Closed => None,
                    Incoming::Text(text) => {
                        let counter = &mut traffic[*venue as usize];
                        *counter = (counter.0 + 1, counter.1 + text.len() as u64);
                        let events = match venue {
                            Venue::Aster => aster_event(&text, &aster_index).map(|e| e.into_iter().collect()),
                            Venue::Lighter => lighter_events(&text, &lighter_index),
                            Venue::Hyperliquid => hl.events(&text, &hl_index, t),
                        };
                        events.inspect_err(|e| tracing::warn!("{venue:?}: unreadable frame; invalidating affected books: {e:#}")).ok()
                    }
                };
                match events {
                    Some(events) => events.into_iter().for_each(|e| deliver(&mut collector, t, e, &destinations, &mut out)),
                    // A connection opened or closed, or a frame not understood: its books are unknown.
                    None => {
                        if *venue == Venue::Hyperliquid { hl.reset(); }
                        for &pair in ids {
                            deliver(&mut collector, t, Event::Book { pair, venue: Leg::Left, bbo: Bbo::default() }, &destinations, &mut out);
                        }
                    }
                }
            }
            _ = tick.tick() => {
                let now = wall_ms();
                if now >= day_end { break; }
                for event in hl.expire(now) { deliver(&mut collector, now, event, &destinations, &mut out); }
                collector.tick(now, &mut out);
                if now >= next_log {
                    for (venue, (frames, bytes)) in [Venue::Aster, Venue::Lighter, Venue::Hyperliquid].into_iter().zip(traffic) {
                        tracing::info!("last 5 min: {venue:?} {:.0} frames/s {:.1} KB/s", frames as f64 / 300.0, bytes as f64 / 300_000.0);
                    }
                    tracing::info!("Hyperliquid ignored: {} historical trades, {} duplicates", hl.historical, hl.duplicates);
                    // The memory history, kept by the rotated container logs: a leak is steady growth.
                    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
                    let kb = |key: &str| status.lines().find_map(|l| l.strip_prefix(key)).map_or("?", str::trim);
                    tracing::info!("memory: RSS {} (heap {}, peak {})", kb("VmRSS:"), kb("RssAnon:"), kb("VmHWM:"));
                    traffic = [(0, 0); 3];
                    next_log = now + WINDOW_MS;
                }
            }
            _ = day.cancelled() => break,
        }
    }
    collector.finish(wall_ms().min(day_end), &mut out);
    day.cancel();
    while tasks.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(name: &str) -> Pair {
        Pair {
            name: name.into(), scale: 1.0,
            left: Market { venue: Venue::Aster, symbol: format!("{name}USDT"), id: None, subtypes: vec![], volume_usd: 0.0 },
            right: Market { venue: Venue::Lighter, symbol: name.into(), id: Some(1), subtypes: vec![], volume_usd: 0.0 },
        }
    }

    /// Depth $130, taker samples from 6 bps (a crypto pair: Aster 4 + Lighter Standard 0 + 2), XEMM
    /// from 5 bps, a P90 gate with `min_samples`.
    fn rec(min_samples: usize) -> Recording {
        Recording {
            tail_ms: TAIL_MS, preroll_ms: PREROLL_MS,
            depth_usd: 130.0,
            gate_percentile: 90.0,
            gate_window_hours: 72.0,
            gate_min_samples: min_samples,
            sample_interval_ms: 1000,
            xemm_min_bps: 5.0,
            floors: BTreeMap::from([("X".into(), 6.0)]),
        }
    }

    fn bbo(bid: f64, ask: f64) -> Bbo {
        Bbo { bid, bid_size: 10.0, ask, ask_size: 10.0 }
    }

    fn book(venue: Leg, bid: f64, ask: f64) -> Event {
        Event::Book { pair: 0, venue, bbo: bbo(bid, ask) }
    }

    fn times(lines: &[String]) -> Vec<&str> {
        lines.iter().map(|l| l.split('\t').nth(1).unwrap()).collect()
    }

    #[test]
    fn edges_are_bps_of_the_aster_mid_and_need_the_depth() {
        // Aster 100.00/100.02 (mid 100.01), Lighter 100.08/100.10: buying Aster at 100.02 and
        // selling Lighter at 100.08 earns 6 cents; the other way loses 10.
        let (a, l) = (bbo(100.0, 100.02), bbo(100.08, 100.10));
        let (buy_left, edge) = best_edge(&a, &l, 130.0).unwrap();
        assert!(buy_left && (edge - 0.06 / 100.01 * 1e4).abs() < 1e-9);
        // $1,000 at each top: short of a $2,000 depth, neither direction counts.
        assert_eq!(best_edge(&a, &l, 2_000.0), None);
        assert_eq!(best_edge(&Bbo::default(), &l, 130.0), None);
        // Only the Lighter bid ($100.08) is short of a $130 depth: flag 4 is off.
        assert_eq!(depth_flags(&a, &Bbo { bid_size: 1.0, ..l }, 130.0), 0b1011);

        // A 5 bps quote on Aster, priced from these books, bids at most 100.08 - 5 bps of 100.045
        // (the mean mid) = 100.0300 and never above Aster's 100.00 bid: only a sale below 100.00
        // could fill it.
        assert!(!could_fill(Leg::Left, false, 100.0, &a, &l, 5.0, 130.0));
        assert!(could_fill(Leg::Left, false, 99.99, &a, &l, 5.0, 130.0));
        // A 10 bps quote bids at most 99.98: a sale at 99.99 is not through it.
        assert!(!could_fill(Leg::Left, false, 99.99, &a, &l, 10.0, 130.0));
        // On Lighter, a buy must lift its 100.10 ask and clear Aster's 100.02 ask by 5 bps.
        assert!(could_fill(Leg::Right, true, 100.11, &a, &l, 5.0, 130.0));
        assert!(!could_fill(Leg::Right, true, 100.06, &a, &l, 1.0, 130.0));
    }

    #[test]
    fn venue_frames_parse_and_lighter_history_is_skipped() {
        let index = HashMap::from([(7u32, 0usize)]);
        let ticker = r#"{"channel":"ticker:7","ticker":{"s":"1000NOT","a":{"price":"2.5","size":"5"},"b":{"price":"2.25","size":"4"}},"type":"update/ticker"}"#;
        assert_eq!(
            lighter_events(ticker, &index).unwrap(),
            [Event::Book { pair: 0, venue: Leg::Right, bbo: Bbo { bid: 2.25, bid_size: 4.0, ask: 2.5, ask_size: 5.0 } }]
        );
        let trade = |kind: &str| format!(r#"{{"channel":"trade:7","trades":[{{"price":"2.25","size":"3","is_maker_ask":true}}],"type":"{kind}"}}"#);
        assert_eq!(lighter_events(&trade("subscribed/trade"), &index).unwrap(), []);
        assert_eq!(
            lighter_events(&trade("update/trade"), &index).unwrap(),
            [Event::Trade { pair: 0, venue: Leg::Right, price: 2.25, size: 3.0, buy: true }]
        );
        let aster = HashMap::from([("NOTUSDT".to_string(), 0usize)]);
        let agg = r#"{"stream":"notusdt@aggTrade","data":{"e":"aggTrade","s":"NOTUSDT","p":"0.0021","q":"100","T":1,"m":true}}"#;
        assert_eq!(aster_event(agg, &aster).unwrap(), Some(Event::Trade { pair: 0, venue: Leg::Left, price: 0.0021, size: 100.0, buy: false }));
    }

    #[test]
    fn a_taker_moment_records_itself_and_its_tail_and_an_xemm_one_its_preroll() {
        let mut c = Collector::new(&rec(50), &[pair("X")], HashMap::new(), 0);
        let mut lines = Vec::new();
        let mut out = |l: String| lines.push(l);
        c.on_event(0, book(Leg::Left, 100.0, 100.01), &mut out); // cold
        c.on_event(1_000, book(Leg::Right, 100.0, 100.01), &mut out); // cold
        c.on_event(1_500, book(Leg::Left, 99.99, 100.0), &mut out); // cold
        c.on_event(2_000, book(Leg::Right, 100.07, 100.08), &mut out); // edge 7 bps: in warmup the trigger is the 6 bps floor
        c.on_event(2_400, book(Leg::Right, 100.0, 100.01), &mut out); // in the tail
        // Cold again: two states within a millisecond; the first never is the state in force.
        c.on_event(4_001, book(Leg::Right, 100.0, 100.03), &mut out);
        c.on_event(4_001, book(Leg::Right, 100.0, 100.02), &mut out);
        // An Aster sale at 99.98, under its 99.99 bid and over 5 bps (of the mean mid) under the
        // 100.05 Lighter bid of 4.5 s: it could fill a quote. The pre-roll (from 3.6 s) writes the
        // states in force since then not yet written (the second of 4.001 s, and 4.5 s; 2.4 s is),
        // and the tail runs to 6.6 s.
        c.on_event(4_500, book(Leg::Right, 100.05, 100.06), &mut out);
        c.on_event(4_600, Event::Trade { pair: 0, venue: Leg::Left, price: 99.98, size: 1.0, buy: false }, &mut out);
        c.on_event(6_600, book(Leg::Right, 100.0, 100.01), &mut out);
        c.on_event(6_601, book(Leg::Right, 100.0, 100.02), &mut out);
        assert_eq!(times(&lines), ["2000", "2400", "4001", "4500", "4600", "6600"]);
        assert!(lines[2].ends_with("100.02\t15"));
        assert_eq!(lines[0], "B\t2000\tX\t99.99\t100\t100.07\t100.08\t15");
        assert_eq!(lines[4], "T\t4600\tX\t0\t99.98\t1\tS");
    }

    #[test]
    fn after_its_warmup_the_trigger_is_the_gate_over_the_windows_before() {
        let mut c = Collector::new(&rec(5), &[pair("X")], HashMap::new(), 0);
        let mut lines = Vec::new();
        c.on_event(0, book(Leg::Left, 100.0, 100.0), &mut |l| lines.push(l));
        // Ten samples in the first window, a second apart: edges 7.1..=16.1 bps. A 20 bps edge half
        // a second after each is no sample (one a second at most). In the warmup (under 5 samples)
        // and after, until the window closes, the trigger is the 6 bps floor.
        for i in 0..10 {
            c.on_event(1_000 * (i + 1), book(Leg::Right, 100.071 + i as f64 * 0.01, 100.2), &mut |l| lines.push(l));
            c.on_event(1_000 * (i + 1) + 500, book(Leg::Right, 100.2, 100.21), &mut |l| lines.push(l));
        }
        // The next window's trigger is the 9th of ten samples (P90), 15.1 bps, as its bin's lower
        // edge: 15 bps. A 14.8 bps edge is not recorded; a 15.2 bps one is.
        let before = lines.len();
        c.on_event(WINDOW_MS + 10_000, book(Leg::Right, 100.148, 100.2), &mut |l| lines.push(l));
        let s: serde_json::Value = serde_json::from_str(lines[before].strip_prefix("S\t").unwrap()).unwrap();
        assert_eq!(s["samples"], json!({"28": 1, "32": 1, "36": 1, "40": 1, "44": 1, "48": 1, "52": 1, "56": 1, "60": 1, "64": 1}));
        assert_eq!(lines.len(), before + 1);
        c.on_event(WINDOW_MS + 12_000, book(Leg::Right, 100.152, 100.2), &mut |l| lines.push(l));
        assert_eq!(lines.len(), before + 2);
    }

    #[test]
    fn memory_is_bounded_the_gate_forgets_windows_past_its_span_and_the_ring_its_second() {
        let rec = Recording { gate_window_hours: 1.0, ..rec(1) };
        let mut c = Collector::new(&rec, &[pair("X")], HashMap::new(), 0);
        let mut out = |_| {};
        c.on_event(0, book(Leg::Left, 100.0, 100.0), &mut out);
        // Three hours of 10 updates a second, each window's edges in a bin of their own.
        for t in (100..3 * 3_600_000).step_by(100) {
            let bid = 100.0 * (1.0 + (7.1 + (t / WINDOW_MS) as f64 * 0.25) / 1e4);
            c.on_event(t, book(Leg::Right, bid, bid + 0.01), &mut out);
        }
        // 35 windows closed; the gate keeps the last hour's 12, and only their bins.
        let p = &c.pairs[0];
        assert_eq!((p.gate.windows.len(), p.gate.total.len()), (12, 12));
        assert!(p.ring.len() <= 12, "{} states in the ring", p.ring.len());
    }

    #[test]
    fn summaries_weight_by_time_and_close_on_window_bounds() {
        let mut c = Collector::new(&rec(50), &[pair("X")], HashMap::new(), 0);
        let mut lines = Vec::new();
        let mut out = |l: String| lines.push(l);
        c.on_event(0, book(Leg::Left, 100.0, 100.02), &mut out);
        c.on_event(0, book(Leg::Right, 100.0, 100.02), &mut out); // basis 0 for 100 s
        c.on_event(100_000, book(Leg::Right, 100.02, 100.04), &mut out); // basis 2 bps for 200 s
        c.tick(WINDOW_MS, &mut out);
        let s: serde_json::Value = serde_json::from_str(lines.last().unwrap().strip_prefix("S\t").unwrap()).unwrap();
        assert_eq!(s["up"], 300.0);
        assert_eq!(s["basis"], round(0.02 / 100.01 * 1e4 * 200.0 / 300.0));
        assert_eq!(s["basis_min"], 0.0);
        // Top of book: $1,000 per side, above the $130 depth: never thin.
        assert_eq!(s["thin"], 0.0);
    }

    #[test]
    fn a_restart_reloads_the_gate_from_the_summaries_and_keeps_its_window_apart() {
        let dir = std::env::temp_dir().join(format!("screener-gates-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let now = wall_ms();
        let current = now - now.rem_euclid(WINDOW_MS);
        let span = 72 * 3_600_000;
        let (store, writer) = Store::open(&dir, vec![]).unwrap();
        for (t, samples) in [(current - span - WINDOW_MS, r#"{"30":7}"#), (current - WINDOW_MS, r#"{"30":2,"40":1}"#), (current, r#"{"40":5}"#)] {
            store.line(format!("S\t{{\"t\":{t},\"p\":\"X\",\"samples\":{samples}}}"));
        }
        store.stop();
        writer.join().unwrap().unwrap();
        let gates = load_gates(&dir, span, now);
        // The window older than 72 h is dropped; the current one is kept apart for its close.
        let (gate, earlier) = &gates["A-L:X"];
        assert_eq!(gate.total, Samples::from([(30, 2), (40, 1)]));
        assert_eq!(earlier, &Samples::from([(40, 5)]));
        std::fs::remove_dir_all(&dir).unwrap();

        // Three samples: in warmup, the trigger is the floor. When the window closes, the gate adds
        // the earlier run's five: P90 of 8 is the 10 bps bin (40).
        let mut gates = gates;
        let old = gates.remove("A-L:X").unwrap();
        gates.insert("X".into(), old);
        let mut c = Collector::new(&rec(5), &[pair("X")], gates, now);
        assert_eq!(c.pairs[0].trigger, 6.0);
        c.tick(current + WINDOW_MS, &mut |_| {});
        assert_eq!(c.pairs[0].trigger, 10.0);
    }

    #[test]
    fn a_run_ends_its_tails_unknown_and_the_next_starts_unknown() {
        let unknown = |t: i64| format!("B\t{t}\tX\t0\t0\t0\t0\t0");
        let mut c = Collector::new(&rec(50), &[pair("X")], HashMap::new(), 0);
        let mut lines = Vec::new();
        c.on_event(0, book(Leg::Left, 99.99, 100.0), &mut |l| lines.push(l));
        c.on_event(100, book(Leg::Right, 100.07, 100.08), &mut |l| lines.push(l)); // 7 bps: a tail to 1.1 s
        c.finish(500, &mut |l| lines.push(l));
        assert_eq!(lines.iter().filter(|l| l.starts_with('B')).last(), Some(&unknown(500)));

        // A pre-roll reaching back before the start says the books were unknown then.
        let mut c = Collector::new(&rec(50), &[pair("X")], HashMap::new(), 1_000);
        let mut lines = Vec::new();
        c.on_event(1_100, book(Leg::Left, 99.99, 100.0), &mut |l| lines.push(l));
        c.on_event(1_200, book(Leg::Right, 100.05, 100.06), &mut |l| lines.push(l));
        c.on_event(1_300, Event::Trade { pair: 0, venue: Leg::Left, price: 99.98, size: 1.0, buy: false }, &mut |l| lines.push(l));
        assert_eq!(lines[0], unknown(1_000));
    }

    #[test]
    fn one_feed_fans_out_with_notional_preserved_and_gaps_are_local() {
        let mut al = pair("A-L:X");
        al.scale = 1000.0;
        let h = Market { venue: Venue::Hyperliquid, symbol: "kX".into(), id: None, subtypes: vec![], volume_usd: 0.0 };
        let ah = Pair { name: "A-H:kX".into(), left: al.left.clone(), right: h.clone(), scale: 1000.0 };
        let lh = Pair { name: "L-H:kX".into(), left: al.right.clone(), right: h, scale: 1.0 };
        let pairs = [al, ah, lh];
        let (markets, destinations) = routing(&pairs);
        assert_eq!(markets.len(), 3);
        let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/screener.toml"))).unwrap();
        let rec = Recording::new(&cfg.report, &pairs);
        let mut c = Collector::new(&rec, &pairs, HashMap::new(), 0);
        for (i, m) in markets.iter().enumerate() {
            let px = if m.venue == Venue::Aster { 0.1 } else { 100.0 };
            deliver(&mut c, 1, Event::Book { pair: i, venue: Leg::Left, bbo: bbo(px, px) }, &destinations, &mut |_| {});
        }
        assert_eq!(c.pairs[0].l.bid, 0.1);
        assert_eq!(c.pairs[1].l.bid, 0.1);
        assert_eq!(c.pairs[2].l.bid, 100.0);
        assert_eq!(c.pairs[1].l.top_usd(), c.pairs[2].l.top_usd());
        let hi = markets.iter().position(|m| m.venue == Venue::Hyperliquid).unwrap();
        deliver(&mut c, 2, Event::Trade { pair: hi, venue: Leg::Left, price: 100.0, size: 2.0, buy: true }, &destinations, &mut |_| {});
        assert_eq!((c.pairs[1].summary.l_usd, c.pairs[2].summary.l_usd), (200.0, 200.0));
        deliver(&mut c, 3, Event::Book { pair: hi, venue: Leg::Left, bbo: Bbo::default() }, &destinations, &mut |_| {});
        assert!(c.pairs[0].a.known() && c.pairs[0].l.known());
        assert!(!c.pairs[1].l.known() && !c.pairs[2].l.known());
    }
}

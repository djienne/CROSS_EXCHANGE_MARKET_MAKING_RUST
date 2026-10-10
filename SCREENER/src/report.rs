//! `report`: ranks pairs using a simplified replay model derived from the bot's rules.
//! Top-of-book depth, queue-free fills, inventory reduction and reverse-maker routes are model
//! assumptions, not identical bot execution paths (see README.md, Known limits).
//!
//! - **Taker-taker**, as `taker/arb.rs` and `entry_gate.rs` decide: the better direction's edge at
//!   the top of book (skipped where a side holds less than the bot's depth); the percentile gate
//!   over the samples the summaries counted in the windows before, once it has its minimum; the
//!   cooldown and the inventory cap. Each leg fills at the recorded state after its latency. PnL is
//!   the trades' cash plus the leftover hedged inventory closed at the last window's mean basis.
//! - **XEMM**, maker on one venue and a taker hedge on the other, as `quote_engine.rs` prices it:
//!   the quote is priced from the state `quote_age` before the trade (an Aster print is moved back
//!   to the book change it made); the prints of one ms *through* it fill min(their sum, clip)
//!   (queue position ignored); a fill pauses the market for the cooldown, and with inventory only
//!   the side that reduces it is quoted; the hedge fills at the recorded state after the fill notice
//!   and the hedge latency, and a fill below the hedge's minimum, held then corrected by the bot, at
//!   the same time (from flat, flattened on the maker venue); the leftover inventory closes as the
//!   taker's. Run in both directions with the bot's settings (required edge, distance gate, skipped
//!   behind a thin maker top), and as a sweep of the required edge without the gate.
//! - **Timing.** By default the states and prints replay at their arrival here, as recorded.
//!   `--scenario` rebuilds them on the venues' clocks (`timeline`): decisions and quotes on the bot's
//!   view (each venue a constant delay after its stamp), fills and hedges on the venues' books.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use chrono::{Days, NaiveDate};
use serde::Serialize;
use serde_json::Value;

use crate::collect::{best_edge, edge_bin, Bbo, Gate, Recording, Samples, Leg, PREROLL_MS, STALL_MS, WINDOW_MS};
use crate::config::{Report, Scenario};
use crate::universe::{self, Pair};

#[derive(clap::Args)]
pub struct Args {
    #[arg(long, default_value = "data")]
    data: PathBuf,
    /// First UTC day scored (YYYY-MM-DD); the gate also reads the days before it.
    #[arg(long)]
    since: Option<String>,
    /// Last UTC day scored (YYYY-MM-DD).
    #[arg(long)]
    until: Option<String>,
    /// Lighter account tier, standard or premium (default: screener.toml).
    #[arg(long)]
    lighter: Option<String>,
    /// Multiplies every latency: a sensitivity check (0 = none).
    #[arg(long, default_value_t = 1.0)]
    latency: f64,
    /// Replays the books on the venues' clocks with a scenario's feed delays (`[report.scenario.*]`),
    /// or `host` (the delays here, rebuilt from the stamps: the arrival result, a check). Default:
    /// arrival times here, as recorded.
    #[arg(long)]
    scenario: Option<String>,
    /// JSON instead of a table.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct State {
    pub t: i64,
    pub a: Bbo,
    pub l: Bbo,
    /// Each leg's age against its venue's stamp (`t` minus the stamp; v3, else 0).
    pub age: [i32; 2],
}

#[derive(Debug, Clone, Copy)]
pub struct Trade {
    pub t: i64,
    pub venue: Leg,
    pub price: f64,
    pub size: f64,
    pub buy: bool,
    /// As `State::age`.
    pub age: i32,
}

#[derive(Debug, Default)]
struct Series {
    states: Vec<State>,
    trades: Vec<Trade>,
    /// `R` marks: (time, leg, age).
    marks: Vec<(i64, Leg, i32)>,
    summaries: Vec<Value>,
    /// Each window's gate samples, by window start.
    windows: Vec<(i64, Samples)>,
}

/// The last recorded state at or before `t`. Callers only ask within a recorded moment (at most
/// the pre-roll before, or the tail after, a trigger), where every change was recorded.
pub fn state_at(states: &[State], t: i64) -> Option<&State> {
    states.partition_point(|s| s.t <= t).checked_sub(1).map(|i| &states[i])
}

fn day(t: i64) -> i64 {
    t.div_euclid(86_400_000)
}

/// Where the replay places the books and prints (`--scenario`).
#[derive(Clone, Copy)]
enum Timing<'a> {
    /// As recorded, at arrival here.
    Arrival,
    /// Rebuilt from the venue stamps at arrival here: the same books (a check of the rebuild).
    Host,
    /// Rebuilt at the venue stamps plus a scenario's feed delays.
    Delays(&'a Scenario),
}

/// The pair's books rebuilt from the venue stamps: as the bot sees them (`view`: each leg at its
/// stamp plus its `delays` ms, or with None at its arrival here) and as they stood on the venues
/// (`venue`: at the stamps; with None at arrival too), its prints at their stamps (with None at
/// arrival), and the ms left out around stalls. From an `R` mark (ignored with None) a leg is
/// unrecorded (NaN) until its next state.
fn timeline(s: &Series, delays: Option<[i64; 2]>) -> (Vec<State>, Vec<State>, Vec<Trade>, i64) {
    let unrecorded = Bbo { bid: f64::NAN, ask: f64::NAN, bid_size: 0.0, ask_size: 0.0 };
    // One leg's entries (time, arrival, line, book) in time order. On a tie a mark comes first: a
    // state of its stamp and arrival is the one it marked, written later.
    let leg = |k: usize, at: &dyn Fn(i64, i64) -> i64| {
        let marks = s.marks.iter().filter(|m| delays.is_some() && m.1 as usize == k).map(|&(t, _, age)| (at(t - age as i64, t), t, usize::MAX, unrecorded));
        let states = s.states.iter().enumerate().map(|(i, st)| (at(st.t - st.age[k] as i64, st.t), st.t, i, if k == 0 { st.a } else { st.l }));
        let mut entries: Vec<_> = marks.chain(states).collect();
        entries.sort_by_key(|e| (e.0, e.1));
        entries
    };
    // Both legs in time order; a line's two legs at one time make one state.
    let merge = |legs: [Vec<(i64, i64, usize, Bbo)>; 2]| {
        let (mut next, mut books, mut out) = ([0, 0], [Bbo::default(); 2], Vec::new());
        while let Some(k) = (0..2).filter(|&k| next[k] < legs[k].len()).min_by_key(|&k| { let e = &legs[k][next[k]]; (e.0, e.1, e.2) }) {
            let (t, _, line, book) = legs[k][next[k]];
            (books[k], next[k]) = (book, next[k] + 1);
            if let Some(&(_, _, _, other)) = legs[1 - k].get(next[1 - k]).filter(|e| e.0 == t && e.2 == line) {
                (books[1 - k], next[1 - k]) = (other, next[1 - k] + 1);
            }
            out.push(State { t, a: books[0], l: books[1], age: [0, 0] });
        }
        out
    };
    let mut trades: Vec<Trade> = s.trades.iter().map(|tr| Trade { t: tr.t - if delays.is_some() { tr.age as i64 } else { 0 }, ..*tr }).collect();
    trades.sort_by_key(|tr| tr.t);
    let Some(d) = delays else {
        let view = merge([0, 1].map(|k| leg(k, &|_, t| t)));
        return (view.clone(), view, trades, 0);
    };
    let mut view = merge([0, 1].map(|k| leg(k, &|stamp, _| stamp + d[k])));
    let venue = merge([0, 1].map(|k| leg(k, &|stamp, _| stamp)));
    // The collector pairs each leg's state with the other's of the pre-roll before (`changed`), which
    // covers the scenario while the feeds here lag less than `STALL_MS` (and its delays differ by
    // at most the rest of the pre-roll, `run`). Around a stall (`Summary::stalls`) it may have
    // missed what the scenario trades: those spans, widened by the pre-roll, are left out
    // (unrecorded view, no prints).
    let mut stalls: Vec<(i64, i64)> = s.summaries.iter().filter_map(|w| w["stalls"].as_array()).flatten()
        .filter_map(|r| Some((r[0].as_i64()? - PREROLL_MS, r[1].as_i64()? + PREROLL_MS))).collect();
    stalls.sort_by_key(|r| r.0);
    let mut spans: Vec<(i64, i64)> = Vec::new();
    for r in stalls {
        match spans.last_mut() {
            Some(last) if r.0 <= last.1 => last.1 = last.1.max(r.1),
            _ => spans.push(r),
        }
    }
    let uncovered = spans.iter().map(|r| r.1 - r.0).sum();
    let out = |t: i64| spans.partition_point(|r| r.0 <= t).checked_sub(1).is_some_and(|i| t < spans[i].1);
    for st in view.iter_mut().filter(|st| out(st.t)) {
        (st.a, st.l) = (unrecorded, unrecorded);
    }
    trades.retain(|tr| !out(tr.t));
    (view, venue, trades, uncovered)
}

/// Everything loaded: the pairs (as last listed), each one's series, and each file's recording.
#[derive(Default)]
struct Data {
    pairs: BTreeMap<String, Pair>,
    series: BTreeMap<String, Series>,
    /// (file, recording, history?, format version)
    recordings: Vec<(String, Recording, bool, u64)>,
}

impl Data {
    /// Adds one line of `file`. A history file (a day before `--since`) gives only its samples,
    /// for the gate.
    fn add(&mut self, file: &str, line: &str, history: bool) -> Result<()> {
        let f: Vec<&str> = line.split('\t').collect();
        match f[0] {
            "P" => {
                let params: Value = serde_json::from_str(f[1])?;
                let version = params["version"].as_u64().unwrap_or(1);
                ensure!(version <= 4, "{file}: unsupported screener format");
                let mut rec: Recording = serde_json::from_value(params["recording"].clone()).with_context(|| format!("{file}: not written by this screener version"))?;
                rec.floors = rec.floors.into_iter().map(|(k, v)| (universe::qualified(&k), v)).collect();
                self.recordings.push((file.to_string(), rec, history, version));
            }
            "U" => {
                for mut p in universe::read_pairs(f[1])? {
                    p.name = universe::qualified(&p.name);
                    self.pairs.insert(p.name.clone(), p);
                }
            }
            "B" if !history => {
                // v3 adds the legs' ages.
                ensure!(f.len() == 8 || f.len() == 10, "{file}: a B line of {} fields, not 8 or 10 (sizes, not depth flags?)", f.len());
                let n: Vec<f64> = f[3..7].iter().map(|x| x.parse()).collect::<Result<_, _>>()?;
                let flags: u8 = f[7].parse()?;
                // A side holding the recorded depth (`depth_flags`) gets an infinite size, the
                // others none: `px * size >= depth_usd` answers as it did on the live book.
                let size = |bit: u8| if flags >> bit & 1 == 1 { f64::INFINITY } else { 0.0 };
                let a = Bbo { bid: n[0], bid_size: size(0), ask: n[1], ask_size: size(1) };
                let l = Bbo { bid: n[2], bid_size: size(2), ask: n[3], ask_size: size(3) };
                let age = if f.len() == 10 { [f[8].parse()?, f[9].parse()?] } else { [0, 0] };
                self.series.entry(universe::qualified(f[2])).or_default().states.push(State { t: f[1].parse()?, a, l, age });
            }
            "T" if !history => {
                ensure!(f.len() == 7 || f.len() == 8, "{file}: a T line of {} fields, not 7 or 8", f.len());
                let age = if f.len() == 8 { f[7].parse()? } else { 0 };
                let trade = Trade { t: f[1].parse()?, venue: leg(file, f[3])?, price: f[4].parse()?, size: f[5].parse()?, buy: f[6] == "B", age };
                self.series.entry(universe::qualified(f[2])).or_default().trades.push(trade);
            }
            "R" if !history => {
                ensure!(f.len() == 5, "{file}: an R line of {} fields, not 5", f.len());
                self.series.entry(universe::qualified(f[2])).or_default().marks.push((f[1].parse()?, leg(file, f[3])?, f[4].parse()?));
            }
            "S" => {
                let mut s: Value = serde_json::from_str(f[1])?;
                s["p"] = Value::String(universe::qualified(s["p"].as_str().context("summary without pair")?));
                let series = self.series.entry(s["p"].as_str().unwrap_or_default().to_string()).or_default();
                series.windows.push((s["t"].as_i64().context("a summary without its time")?, serde_json::from_value(s["samples"].clone())?));
                if !history {
                    series.summaries.push(s);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Puts each series in time order (a pre-roll can be written after later states).
    fn sorted(mut self) -> Data {
        for s in self.series.values_mut() {
            s.states.sort_by_key(|s| s.t);
            s.trades.sort_by_key(|t| t.t);
            s.marks.sort_by_key(|m| m.0);
            s.windows.sort_by_key(|w| w.0);
        }
        self
    }
}

fn leg(file: &str, code: &str) -> Result<Leg> {
    Ok(match code { "A" | "0" => Leg::Left, "L" | "1" => Leg::Right, _ => bail!("{file}: invalid leg {code}") })
}

/// The data of the days `since..=until`, plus the samples of the `history_days` before `since`.
fn load(dir: &Path, since: Option<&str>, until: Option<&str>, history_days: u64) -> Result<Data> {
    let first = since.map(|s| -> Result<String> { Ok((NaiveDate::parse_from_str(s, "%Y-%m-%d")? - Days::new(history_days)).to_string()) }).transpose()?;
    let mut data = Data::default();
    for file in crate::store::files(dir, first.as_deref(), until)? {
        let name = file.file_name().unwrap_or_default().to_string_lossy().to_string();
        // Names start with the day: "2026-09-25T..." sorts before "2026-09-26".
        let history = since.is_some_and(|s| name.as_str() < s);
        for line in crate::store::read(&file)? {
            data.add(&name, &line, history)?;
        }
    }
    Ok(data.sorted())
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct TakerResult {
    pub trades: usize,
    /// Trades whose fill met an unknown book (a connection gap) or an unrecorded one: left out.
    pub unresolved: usize,
    /// Gate samples at `required` or more, over all the windows.
    pub samples: u64,
    /// Too few samples for the gate ever to open.
    pub warmup: bool,
    pub expected_bps: f64,
    pub realized_bps: f64,
    pub pnl_usd: f64,
    pub inventory_clips: f64,
    #[serde(skip)]
    pub by_day: BTreeMap<i64, f64>,
}

pub struct TakerSim<'a> {
    pub rules: &'a crate::config::Taker,
    pub required_bps: f64,
    pub clip_usd: f64,
    pub depth_usd: f64,
    pub fee_a: f64,
    pub fee_l: f64,
    pub lag_a: i64,
    pub lag_l: i64,
    /// Where the leftover inventory is closed: right mid vs left mid, bps.
    pub close_basis_bps: Option<f64>,
}

/// The bot's gated taker deciding on `states` (its view) and filling on `fills` (the venues' books;
/// the same in arrival time), its gate reading `windows` (by start) as the collector did: at a
/// state, the windows of the `gate_window_hours` before the state's own.
pub fn simulate_taker(states: &[State], fills: &[State], windows: &[(i64, Samples)], sim: &TakerSim) -> TakerResult {
    let rules = sim.rules;
    let span = (rules.gate_window_hours * 3.6e6) as i64;
    let samples = windows.iter().flat_map(|(_, s)| s.range(edge_bin(sim.required_bps)..)).map(|(_, n)| n).sum();
    let mut r = TakerResult { samples, warmup: samples < rules.gate_min_samples as u64, ..Default::default() };
    let (mut gate, mut pushed, mut window, mut threshold) = (Gate::default(), 0, i64::MIN, None);
    let mut last_trade = i64::MIN / 2;
    // Left base position (long: bought left, sold right) and cash.
    let (mut position, mut cash, mut expected, mut realized) = (0.0f64, 0.0f64, 0.0, 0.0);
    for s in states {
        let Some((buy_left, edge)) = best_edge(&s.a, &s.l, sim.depth_usd) else { continue };
        if edge < sim.required_bps {
            continue;
        }
        let w = s.t - s.t.rem_euclid(WINDOW_MS);
        if w != window {
            while pushed < windows.len() && windows[pushed].0 < w {
                gate.push(windows[pushed].0, windows[pushed].1.clone());
                pushed += 1;
            }
            gate.expire(w - span);
            threshold = gate.level(sim.required_bps, rules.gate_percentile, rules.gate_min_samples).map(|g| g.max(sim.required_bps + rules.gate_extra_bps));
            window = w;
        }
        let Some(threshold) = threshold else { continue };
        if edge < threshold || s.t - last_trade < rules.cooldown_ms {
            continue;
        }
        let mid = s.a.mid();
        let qty = sim.clip_usd / mid;
        let signed = if buy_left { qty } else { -qty };
        let after = ((position + signed) * mid).abs();
        if after > rules.max_position_usd + 1e-9 && after > (position * mid).abs() {
            continue;
        }
        let fill_a = state_at(fills, s.t + sim.lag_a).map(|f| f.a).filter(Bbo::known);
        let fill_l = state_at(fills, s.t + sim.lag_l).map(|f| f.l).filter(Bbo::known);
        let (Some(fa), Some(fl)) = (fill_a, fill_l) else {
            r.unresolved += 1;
            continue;
        };
        let (pa, pl) = if buy_left { (fa.ask, fl.bid) } else { (fa.bid, fl.ask) };
        let gross = if buy_left { pl - pa } else { pa - pl };
        let net = qty * gross - qty * pa * sim.fee_a - qty * pl * sim.fee_l;
        cash += net;
        position += signed;
        expected += edge;
        realized += gross / mid * 1e4;
        r.trades += 1;
        *r.by_day.entry(day(s.t)).or_default() += net;
        last_trade = s.t;
    }
    if r.trades > 0 {
        (r.expected_bps, r.realized_bps) = (expected / r.trades as f64, realized / r.trades as f64);
    }
    let (clips, cost) = close(position, states, sim.close_basis_bps, sim.clip_usd, &mut r.by_day);
    (r.inventory_clips, r.pnl_usd) = (clips, cash - cost);
    r
}

/// A hedged inventory of `position` left base (long: bought left, sold right), in clips, and
/// the cost of closing it: its left leg at the left mid, its right leg at the right mid,
/// `basis_bps` apart, free of fees, booked on the last day both books were known.
fn close(position: f64, states: &[State], basis_bps: Option<f64>, clip_usd: f64, by_day: &mut BTreeMap<i64, f64>) -> (f64, f64) {
    let mid = states.iter().rev().find(|s| s.a.known()).map_or(0.0, |s| s.a.mid());
    let cost = position * mid * basis_bps.unwrap_or(0.0) / 1e4;
    if let Some(last) = states.iter().rev().find(|s| s.a.known() && s.l.known()).filter(|_| cost != 0.0) {
        *by_day.entry(day(last.t)).or_default() -= cost;
    }
    (position * mid / clip_usd, cost)
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct XemmResult {
    pub required_bps: f64,
    pub fills: usize,
    /// Fills whose quote met an unrecorded book, or whose hedge an unknown one (a connection gap)
    /// or an unrecorded one: left out.
    pub unresolved: usize,
    /// Mean hedge (or flatten) price minus quote price, bps of the reference (before fees).
    pub edge_bps: f64,
    pub pnl_usd: f64,
    pub inventory_clips: f64,
    #[serde(skip)]
    pub by_day: BTreeMap<i64, f64>,
}

pub struct XemmSim {
    pub maker: Leg,
    pub required_bps: f64,
    /// The bot's distance gate (min, max bps behind the maker touch); None to quote anywhere.
    pub distance: Option<(f64, f64)>,
    pub fee_maker: f64,
    pub fee_hedge: f64,
    /// The maker venue's taker fee, paid to flatten a fill too small to hedge.
    pub fee_flatten: f64,
    pub clip_usd: f64,
    pub depth_usd: f64,
    /// The hedge venue's minimum order: a smaller fill is held `pending_age`, then corrected.
    pub min_hedge_usd: f64,
    pub pending_age: i64,
    pub quote_age: i64,
    /// A maker-venue print arrives this much after the book change it made: its time is moved back.
    pub print_delay: i64,
    pub notice: i64,
    pub hedge_lag: i64,
    pub cooldown: i64,
    /// As in `TakerSim`.
    pub close_basis_bps: Option<f64>,
}

/// As `simulate_taker`: quotes priced from `states`, hedges filled on `fills`.
pub fn simulate_xemm(states: &[State], fills: &[State], trades: &[Trade], sim: &XemmSim) -> XemmResult {
    let mut r = XemmResult { required_bps: sim.required_bps, ..Default::default() };
    let (mut last_fill, mut edge_sum, mut position) = (i64::MIN / 2, 0.0, 0.0f64);
    let books = |s: &State| match sim.maker {
        Leg::Left => (s.a, s.l),
        Leg::Right => (s.l, s.a),
    };
    let trades: Vec<&Trade> = trades.iter().filter(|t| t.venue == sim.maker).collect();
    for (i, trade) in trades.iter().enumerate() {
        let t = trade.t - sim.print_delay;
        // A buyer lifts our ask: we sell on the maker venue and buy the hedge. Our left leg:
        let signed = if trade.buy == (sim.maker == Leg::Left) { -1.0 } else { 1.0 };
        // As the bot: a fill pauses the market ([maker.live] cooldown_scope), and with inventory
        // only the side that reduces it is quoted ([maker.live.quote] reduce_position_only).
        if t - last_fill < sim.cooldown || position * signed > 0.0 {
            continue;
        }
        let Some(quoted) = state_at(states, t - sim.quote_age) else { continue };
        let (maker, hedge) = books(quoted);
        if maker.bid.is_nan() || hedge.bid.is_nan() {
            r.unresolved += 1;
            continue;
        }
        if !(maker.known() && hedge.known()) {
            continue;
        }
        let reference = (quoted.a.mid() + quoted.l.mid()) / 2.0;
        let required = sim.required_bps / 1e4 * reference;
        // The bot's price, joining the touch at best (post-only).
        let price = if trade.buy {
            ((hedge.ask * (1.0 + sim.fee_hedge) + required) / (1.0 - sim.fee_maker)).max(maker.ask)
        } else {
            ((hedge.bid * (1.0 - sim.fee_hedge) - required) / (1.0 + sim.fee_maker)).min(maker.bid)
        };
        let hedge_depth = if trade.buy { hedge.ask * hedge.ask_size } else { hedge.bid * hedge.bid_size };
        if hedge_depth < sim.depth_usd {
            continue;
        }
        if let Some((min_bps, max_bps)) = sim.distance {
            // The bot measures from where the maker side's depth reaches the bot's: unknown behind a thin top.
            let (touch, touch_usd) = if trade.buy { (maker.ask, maker.ask * maker.ask_size) } else { (maker.bid, maker.bid * maker.bid_size) };
            let distance = (price - touch).abs() / reference * 1e4;
            if touch_usd < sim.depth_usd || distance < min_bps || distance > max_bps {
                continue;
            }
        }
        // A taker sweeping levels prints once per level, in one ms: we fill from all that pass our price.
        let through: f64 = trades[i..].iter().take_while(|u| u.t == trade.t)
            .filter(|u| u.buy == trade.buy && if u.buy { u.price > price } else { u.price < price }).map(|u| u.size).sum();
        if through == 0.0 {
            continue;
        }
        // A clip when flat, else at most what flattens.
        let qty = through.min(if position == 0.0 { sim.clip_usd / reference } else { position.abs() });
        // Below the hedge venue's minimum the bot holds the fill ([maker.live.partials]); `pending_age`
        // later it freezes and corrects it by a taker order on the leg the fill grew: the maker's from
        // flat, else the hedge's. Priced at the hedge's time: the recording ends 2 s after a print.
        let small = qty * reference < sim.min_hedge_usd;
        let flatten = small && position == 0.0;
        let exit = |s: &State| if flatten { books(s).0 } else { books(s).1 };
        let Some(hedged) = state_at(fills, t + sim.notice + sim.hedge_lag).map(exit).filter(Bbo::known) else {
            r.unresolved += 1;
            continue;
        };
        let hedge_price = if trade.buy { hedged.ask } else { hedged.bid };
        let fee_exit = if flatten { sim.fee_flatten } else { sim.fee_hedge };
        let gross = if trade.buy { price - hedge_price } else { hedge_price - price };
        let net = qty * gross - qty * price * sim.fee_maker - qty * hedge_price * fee_exit;
        r.fills += 1;
        r.pnl_usd += net;
        edge_sum += gross / reference * 1e4;
        *r.by_day.entry(day(t)).or_default() += net;
        // Frozen until the correction, then the cooldown stands in for the reconciliation that unfreezes it.
        last_fill = if small { t + sim.pending_age } else { t };
        if !flatten {
            position += signed * qty;
        }
    }
    if r.fills > 0 {
        r.edge_bps = edge_sum / r.fills as f64;
    }
    let (clips, cost) = close(position, states, sim.close_basis_bps, sim.clip_usd, &mut r.by_day);
    (r.inventory_clips, r.pnl_usd) = (clips, r.pnl_usd - cost);
    r
}

#[derive(Debug, Serialize)]
pub struct Score {
    pub pair: String,
    pub left_venue: universe::Venue,
    pub right_venue: universe::Venue,
    pub left_costs: crate::config::Costs,
    pub right_costs: crate::config::Costs,
    pub down_days: f64,
    /// Days both venues were followed.
    pub days: f64,
    pub left_usd_day: f64,
    pub right_usd_day: f64,
    pub spread_a_bps: Option<f64>,
    pub spread_l_bps: Option<f64>,
    /// Share of the time either top of book held less than the bot's depth.
    pub thin: Option<f64>,
    pub basis_bps: Option<f64>,
    pub taker_gated: TakerResult,
    /// Left maker, right hedge, with the bot's required edge and distance gate.
    pub xemm_fixed: XemmResult,
    pub xemm_reverse_fixed: XemmResult,
    /// Left maker, right hedge, best required edge of the sweep, no gate.
    pub xemm_best: XemmResult,
    /// Right maker, left hedge, best required edge of the sweep, no gate.
    pub xemm_reverse_best: XemmResult,
    /// The best strategy's PnL per day, and on how many days it was positive.
    pub best_usd_day: f64,
    pub best: &'static str,
    pub best_days_positive: usize,
    pub best_by_day: BTreeMap<i64, f64>,
    /// With a scenario, the days left out around stalls here (`timeline`).
    pub uncovered_days: f64,
}

fn sum(summaries: &[Value], key: &str) -> f64 {
    summaries.iter().filter_map(|s| s[key].as_f64()).sum()
}

/// Mean of `key` weighted by each summary's seconds up.
fn weighted(summaries: &[Value], key: &str) -> Option<f64> {
    let (w, x) = summaries.iter().fold((0.0, 0.0), |(w, x), s| match (s["up"].as_f64(), s[key].as_f64()) {
        (Some(up), Some(v)) => (w + up, x + up * v),
        _ => (w, x),
    });
    (w > 0.0).then(|| x / w)
}

fn score(cfg: &Report, latency: f64, pair: &Pair, series: &Series, timing: Timing) -> Result<Score> {
    let lighter = cfg.lighter()?;
    let built;
    let (states, fills, trades, uncovered_days) = match timing {
        Timing::Arrival => (&series.states[..], &series.states[..], &series.trades[..], 0.0),
        Timing::Host | Timing::Delays(_) => {
            let delays = match timing { Timing::Delays(s) => Some([&pair.left, &pair.right].map(|m| s.ms(m.venue).round() as i64)), _ => None };
            built = timeline(series, delays);
            (&built.0[..], &built.1[..], &built.2[..], built.3 as f64 / 86_400_000.0)
        }
    };
    let ms = |ms: f64| (ms * latency).round() as i64;
    let left = cfg.costs(&pair.left, lighter);
    let right = cfg.costs(&pair.right, lighter);
    let (fee_a, fee_l) = (left.taker_bps / 1e4, right.taker_bps / 1e4);
    let depth_usd = cfg.depth_usd();
    let days = sum(&series.summaries, "up") / 86_400.0;
    let per_day = |x: f64| if days > 0.0 { x / days } else { 0.0 };
    let close_basis_bps = series.summaries.iter().rev().find_map(|s| s["basis"].as_f64());

    let taker = simulate_taker(
        states,
        fills,
        &series.windows,
        &TakerSim {
            rules: &cfg.taker,
            required_bps: cfg.taker_required_bps(pair, lighter),
            clip_usd: cfg.clip_usd,
            depth_usd,
            fee_a,
            fee_l,
            lag_a: ms(left.taker_ms),
            lag_l: ms(right.taker_ms),
            close_basis_bps,
        },
    );
    let xemm = |maker: Leg, required_bps: f64, distance: Option<(f64, f64)>| {
        let (own, hedge) = match maker { Leg::Left => (left, right), Leg::Right => (right, left) };
        let sim = XemmSim {
            maker,
            required_bps,
            distance,
            fee_maker: own.maker_bps / 1e4,
            fee_hedge: hedge.taker_bps / 1e4,
            fee_flatten: own.taker_bps / 1e4,
            clip_usd: cfg.clip_usd,
            depth_usd,
            min_hedge_usd: cfg.xemm.min_hedge_usd,
            pending_age: cfg.xemm.pending_age_ms,
            quote_age: ms(own.quote_age_ms),
            // On venue time a print is at its own stamp.
            print_delay: if matches!(timing, Timing::Delays(_)) { 0 } else { own.print_delay_ms.round() as i64 },
            notice: ms(own.notice_ms),
            hedge_lag: ms(hedge.taker_ms),
            cooldown: cfg.xemm.cooldown_ms,
            close_basis_bps,
        };
        simulate_xemm(states, fills, trades, &sim)
    };
    let best_of = |maker: Leg| {
        cfg.xemm.sweep_bps.iter().map(|&req| xemm(maker, req, None)).max_by(|x, y| x.pnl_usd.total_cmp(&y.pnl_usd)).unwrap_or_default()
    };
    let x = &cfg.xemm;
    let mut s = Score {
        pair: pair.name.clone(),
        left_venue: pair.left.venue, right_venue: pair.right.venue,
        left_costs: left, right_costs: right,
        down_days: sum(&series.summaries, "down") / 86_400.0,
        days,
        left_usd_day: per_day(sum(&series.summaries, "a_usd")),
        right_usd_day: per_day(sum(&series.summaries, "l_usd")),
        spread_a_bps: weighted(&series.summaries, "spread_a"),
        spread_l_bps: weighted(&series.summaries, "spread_l"),
        thin: weighted(&series.summaries, "thin"),
        basis_bps: weighted(&series.summaries, "basis"),
        taker_gated: taker,
        xemm_fixed: xemm(Leg::Left, x.required_bps, Some((x.min_touch_distance_bps, x.max_quote_distance_bps))),
        xemm_reverse_fixed: xemm(Leg::Right, x.required_bps, Some((x.min_touch_distance_bps, x.max_quote_distance_bps))),
        xemm_best: best_of(Leg::Left),
        xemm_reverse_best: best_of(Leg::Right),
        best_usd_day: 0.0,
        best: "",
        best_days_positive: 0,
        best_by_day: BTreeMap::new(),
        uncovered_days,
    };
    let candidates = [
        ("taker", s.taker_gated.pnl_usd, &s.taker_gated.by_day),
        ("xemm_fixed", s.xemm_fixed.pnl_usd, &s.xemm_fixed.by_day),
        ("xemm_reverse_fixed", s.xemm_reverse_fixed.pnl_usd, &s.xemm_reverse_fixed.by_day),
    ];
    let (best, pnl, by_day) = candidates.into_iter().max_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
    let by_day = by_day.clone();
    (s.best, s.best_usd_day, s.best_days_positive) = (best, per_day(pnl), by_day.values().filter(|&&v| v > 0.0).count());
    s.best_by_day = by_day;
    Ok(s)
}

/// Spearman rank correlation of `x` and `y` (average ranks for ties).
fn spearman(x: &[f64], y: &[f64]) -> Option<f64> {
    fn ranks(v: &[f64]) -> Vec<f64> {
        let mut order: Vec<usize> = (0..v.len()).collect();
        order.sort_by(|&i, &j| v[i].total_cmp(&v[j]));
        let mut r = vec![0.0; v.len()];
        let mut i = 0;
        while i < order.len() {
            let mut j = i;
            while j + 1 < order.len() && v[order[j + 1]] == v[order[i]] {
                j += 1;
            }
            for k in i..=j {
                r[order[k]] = (i + j) as f64 / 2.0;
            }
            i = j + 1;
        }
        r
    }
    let (rx, ry) = (ranks(x), ranks(y));
    let n = x.len() as f64;
    let mean = (n - 1.0) / 2.0;
    let cov: f64 = rx.iter().zip(&ry).map(|(a, b)| (a - mean) * (b - mean)).sum();
    let var = |r: &[f64]| r.iter().map(|a| (a - mean).powi(2)).sum::<f64>();
    let d = (var(&rx) * var(&ry)).sqrt();
    (x.len() >= 3 && d > 0.0).then(|| cov / d)
}

pub fn run(cfg: &Report, args: &Args) -> Result<()> {
    let mut cfg = cfg.clone();
    if let Some(tier) = &args.lighter {
        cfg.lighter_tier = tier.clone();
    }
    let lighter = cfg.lighter()?;
    cfg.validate()?;
    ensure!(args.latency.is_finite() && args.latency >= 0.0, "latency must be finite and nonnegative");
    let timing = match args.scenario.as_deref() {
        None => Timing::Arrival,
        Some("host") => Timing::Host,
        Some(name) => {
            let s = cfg.scenario.get(name).with_context(|| format!("no [report.scenario.{name}] in screener.toml"))?;
            let d = [s.aster_ms, s.lighter_ms, s.hyperliquid_ms];
            let spread = d.iter().copied().fold(f64::MIN, f64::max) - d.iter().copied().fold(f64::MAX, f64::min);
            ensure!(spread <= (PREROLL_MS - STALL_MS) as f64, "scenario {name}: its delays differ by {spread} ms; the recording covers up to {} ms", PREROLL_MS - STALL_MS);
            Timing::Delays(s)
        }
    };
    let data = load(&args.data, args.since.as_deref(), args.until.as_deref(), (cfg.taker.gate_window_hours / 24.0).ceil() as u64)?;
    for (file, rec, history, version) in &data.recordings {
        rec.check(&cfg, lighter, &data.pairs).with_context(|| format!("{file}: these settings would trade on moments it did not record"))?;
        ensure!(*history || *version >= 4 || !matches!(timing, Timing::Delays(_)), "{file}: format v{version} has no venue stamps or stall spans for a scenario; score newer days (--since)");
        if !history {
            for name in rec.floors.keys() {
                if let Some(pair) = data.pairs.get(name) { rec.check_latency(&cfg, pair, args.latency).with_context(|| file.clone())?; }
            }
        }
    }
    let empty = Series::default();
    let mut scores: Vec<Score> = data.pairs.values().map(|p| score(&cfg, args.latency, p, data.series.get(&p.name).unwrap_or(&empty), timing)).collect::<Result<_>>()?;
    scores.retain(|s| s.days > 0.0);
    scores.sort_by(|a, b| b.best_usd_day.total_cmp(&a.best_usd_day));

    // Does the ranking hold from one day to the next? Mean Spearman over consecutive days.
    let all_days: std::collections::BTreeSet<i64> = scores.iter().flat_map(|s| data.series[&s.pair].summaries.iter().filter_map(|x| x["t"].as_i64()).map(day)).collect();
    let days: Vec<i64> = all_days.into_iter().collect();
    let correlations: Vec<f64> = days
        .windows(2)
        .filter_map(|w| {
            // Newly collected routes have no result on earlier days, rather than zero PnL.
            let shared: Vec<_> = scores.iter().filter(|s| w.iter().all(|d| data.series[&s.pair].summaries.iter().any(|v|
                v["t"].as_i64().is_some_and(|t| day(t) == *d) && v["up"].as_f64().unwrap_or(0.0) > 0.0))).collect();
            let at = |d: i64| shared.iter().map(|s| s.best_by_day.get(&d).copied().unwrap_or(0.0)).collect::<Vec<_>>();
            spearman(&at(w[0]), &at(w[1]))
        })
        .collect();
    let stability = (!correlations.is_empty()).then(|| correlations.iter().sum::<f64>() / correlations.len() as f64);
    let delays = feed_delays(&data);
    let scenario = match timing { Timing::Delays(s) => serde_json::to_value(s)?, _ => Value::String(args.scenario.clone().unwrap_or("arrival".into())) };

    if args.json {
        let out = serde_json::json!({ "lighter_tier": cfg.lighter_tier, "latency": args.latency, "scenario": scenario, "feed_delays_here": delays, "clip_usd": cfg.clip_usd, "hyperliquid_assumptions": cfg.hyperliquid, "ranking": "fixed strategies; sweeps exploratory", "funding_included": false, "stablecoin_parity_assumed": true, "day_to_day_rank_correlation": stability, "pairs": scores });
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }
    println!("Lighter {}, latency x{}, clip ${}, timing {scenario}; $/day while both books known. A=Aster L=Lighter H=Hyperliquid.", cfg.lighter_tier, args.latency, cfg.clip_usd);
    for (venue, d) in &delays {
        println!("Feed delay here, venue stamp to arrival, p50/p99 ms: {venue:?} books {}/{}, prints {}/{} ({} / {} samples).",
            d["books"][0], d["books"][1], d["prints"][0], d["prints"][1], d["books"][2], d["prints"][2]);
    }
    println!("Ranked by fixed strategies. XEMM 0->1 / 1->0 means maker->hedge; sweeps are exploratory, fitted on these data.");
    println!("Hyperliquid assumptions: maker {} / taker {} bps, execution {} / notice {} / quote age {} ms (before latency multiplier).", cfg.hyperliquid.maker_bps, cfg.hyperliquid.taker_bps, cfg.hyperliquid.taker_ms, cfg.hyperliquid.fill_notice_ms, cfg.hyperliquid.quote_age_ms);
    println!("Funding excluded; stablecoin parity assumed. Compare the same UTC dates (--since/--until), after warmup, over >=7 days; routes are alternatives, not additive profits.");
    println!("{:<23} {:>5} {:>5} | {:>6} {:>6} {:>7} | {:>6} {:>7} {:>6} {:>7} | {:>4} {:>7} {:>4} {:>7} | {:<19} {:>5} {:>5}",
        "pair", "days", "down%", "tk/d", "kept", "tk$/d", "xf0/d", "xf0$/d", "xf1/d", "xf1$/d", "req0", "sweep0", "req1", "sweep1", "best fixed", "+days", "unres");
    for s in &scores {
        let t = &s.taker_gated;
        let kept = if t.expected_bps > 0.0 { format!("{:.0}%", 100.0 * t.realized_bps / t.expected_bps) } else { "-".into() };
        let d = |x: f64| x / s.days;
        let unresolved = t.unresolved + s.xemm_fixed.unresolved + s.xemm_reverse_fixed.unresolved;
        println!("{:<23} {:>5.2} {:>5.1} | {:>6.1} {:>6} {:>7.2} | {:>6.1} {:>7.2} {:>6.1} {:>7.2} | {:>4} {:>7.2} {:>4} {:>7.2} | {:<19} {:>2}/{:<2} {:>5}",
            s.pair, s.days, 100.0 * s.down_days / (s.days + s.down_days), d(t.trades as f64), kept, d(t.pnl_usd),
            d(s.xemm_fixed.fills as f64), d(s.xemm_fixed.pnl_usd), d(s.xemm_reverse_fixed.fills as f64), d(s.xemm_reverse_fixed.pnl_usd),
            s.xemm_best.required_bps, d(s.xemm_best.pnl_usd), s.xemm_reverse_best.required_bps, d(s.xemm_reverse_best.pnl_usd),
            s.best, s.best_days_positive, s.best_by_day.len(), unresolved);
    }
    match stability {
        Some(rho) => println!("Day-to-day rank correlation of the best $/day: {rho:.2} (1 = the same ranking every day, 0 = noise)."),
        None => println!("Day-to-day rank correlation: needs two days of data."),
    }
    let warmup = scores.iter().filter(|s| s.taker_gated.warmup).count();
    if warmup > 0 {
        println!("{warmup} pairs never gave the taker gate its {} samples.", cfg.taker.gate_min_samples);
    }
    let unresolved: usize = scores.iter().map(|s| s.taker_gated.unresolved + s.xemm_fixed.unresolved + s.xemm_reverse_fixed.unresolved).sum();
    if unresolved > 0 {
        println!("{unresolved} fills met an unknown book (a connection gap) or an unrecorded one (with a scenario, also a quote older than the pre-roll before its print's arrival): their PnL is left out.");
    }
    let uncovered: f64 = scores.iter().map(|s| s.uncovered_days).sum();
    if uncovered > 0.0 {
        let share = uncovered / scores.iter().map(|s| s.days + s.down_days).sum::<f64>();
        println!("{:.1}% of the pair time was around a feed here over {STALL_MS} ms late (a stall): left out.", 100.0 * share);
    }
    Ok(())
}

/// Per venue, the feed delay here of its book changes and of its prints (arrival minus the
/// venue's stamp, v3 lines; 0 means none): [p50, p99, samples] ms. The difference between a
/// venue's prints and books is what `aster_print_delay_ms` models on arrival time.
fn feed_delays(data: &Data) -> BTreeMap<universe::Venue, Value> {
    let mut ages: BTreeMap<universe::Venue, [Vec<i32>; 2]> = BTreeMap::new();
    for (name, s) in &data.series {
        let Some(pair) = data.pairs.get(name) else { continue };
        for (k, (market, leg)) in [(&pair.left, Leg::Left), (&pair.right, Leg::Right)].into_iter().enumerate() {
            let [books, prints] = ages.entry(market.venue).or_default();
            let mut last = None;
            for st in &s.states {
                let (stamp, book) = (st.t - st.age[k] as i64, if k == 0 { st.a } else { st.l });
                if st.age[k] != 0 && book.known() && last != Some(stamp) {
                    books.push(st.age[k]);
                }
                last = Some(stamp);
            }
            prints.extend(s.trades.iter().filter(|t| t.venue == leg && t.age != 0).map(|t| t.age));
        }
    }
    let quantiles = |v: &mut Vec<i32>| {
        v.sort_unstable();
        let at = |q: f64| v.get(((v.len() as f64 * q) as usize).min(v.len().saturating_sub(1))).copied();
        serde_json::json!([at(0.5), at(0.99), v.len()])
    };
    ages.into_iter().filter(|(_, [b, p])| !b.is_empty() || !p.is_empty())
        .map(|(venue, [mut b, mut p])| (venue, serde_json::json!({ "books": quantiles(&mut b), "prints": quantiles(&mut p) }))).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::collect::{Collector, Event};

    fn bbo(bid: f64, ask: f64) -> Bbo {
        Bbo { bid, bid_size: 100.0, ask, ask_size: 100.0 }
    }

    fn rules(min_samples: usize) -> crate::config::Taker {
        crate::config::Taker {
            margin_bps: 2.0,
            gate_percentile: 90.0,
            gate_window_hours: 72.0,
            gate_min_samples: min_samples,
            gate_extra_bps: 0.5,
            sample_interval_ms: 1000,
            cooldown_ms: 60_000,
            max_position_usd: 200.0,
        }
    }

    fn sim(rules: &crate::config::Taker) -> TakerSim<'_> {
        TakerSim { rules, required_bps: 6.0, clip_usd: 100.0, depth_usd: 1_000.0, fee_a: 4e-4, fee_l: 0.0, lag_a: 100, lag_l: 300, close_basis_bps: None }
    }

    #[test]
    fn a_round_trip_earns_its_two_realized_edges_less_fees() {
        // The window before held 50 samples at 6 bps: the gate is 6.5 bps (required + 0.5).
        let windows = vec![(-WINDOW_MS, Samples::from([(24, 50)]))];
        // Aster steady at 100.00/100.02 (mid 100.01).
        let a = bbo(100.0, 100.02);
        let states = [
            // Lighter bid 100.12: buying Aster at 100.02 and selling Lighter earns 10 bps...
            State { t: 0, a, l: bbo(100.12, 100.14), age: [0, 0] },
            // ...but by the Lighter fill (300 ms) the bid is 100.10: 8 bps realized.
            State { t: 200, a, l: bbo(100.10, 100.12), age: [0, 0] },
            // 100 s later Lighter's ask is 99.90: selling Aster at 100.00 earns 10 bps, and holds.
            State { t: 100_000, a, l: bbo(99.88, 99.90), age: [0, 0] },
        ];
        let rules = rules(50);
        let r = simulate_taker(&states, &states, &windows, &sim(&rules));
        assert_eq!(r.trades, 2);
        let qty = 100.0 / 100.01;
        // Cash: (100.10 - 100.02) + (100.00 - 99.90) per unit, less 4 bps on each Aster leg.
        let fees = qty * (100.02 + 100.00) * 4e-4;
        assert!((r.pnl_usd - (qty * (0.08 + 0.10) - fees)).abs() < 1e-9);
        assert!(r.inventory_clips.abs() < 1e-12);
        assert!((r.expected_bps - (0.10 / 100.01 * 1e4 + 0.10 / 100.01 * 1e4) / 2.0).abs() < 1e-9);
        assert!((r.realized_bps - (0.08 / 100.01 * 1e4 + 0.10 / 100.01 * 1e4) / 2.0).abs() < 1e-9);
    }

    #[test]
    fn the_gate_reads_the_windows_before_and_trades_only_their_top_decile() {
        // The window before held ten samples: 7.1..=16.1 bps (bins 28, 32, ... 64).
        let windows = vec![(-WINDOW_MS, (0..10).map(|i| (28 + 4 * i, 1)).collect::<Samples>())];
        let a = bbo(100.0, 100.0);
        let at = |t: i64, edge_bps: f64| State { t, a, l: bbo(100.0 + edge_bps / 100.0, 100.2), age: [0, 0] };
        // In that window itself no window came before: no gate. In the next, the gate is its P90
        // (the 9th of ten, bin 60): 15 bps. 14.8 waits, 15.2 trades, then the 60 s cooldown holds.
        let states = [at(-1_000, 20.0), at(1_000, 14.8), at(2_000, 15.2), at(3_000, 16.0)];
        let r = simulate_taker(&states, &states, &windows, &sim(&rules(5)));
        assert_eq!((r.samples, r.trades, r.warmup), (10, 1, false));
        assert!((r.expected_bps - 15.2).abs() < 1e-9);
        let never = simulate_taker(&states, &states, &windows, &sim(&rules(50)));
        assert_eq!((never.trades, never.warmup), (0, true));
    }

    #[test]
    fn an_xemm_fill_needs_a_trade_through_the_quote_and_pays_the_hedge_markout() {
        // Aster maker 100.00/100.02, Lighter hedge 100.00/100.02: reference 100.01.
        let states = [
            State { t: 0, a: bbo(100.0, 100.02), l: bbo(100.0, 100.02), age: [0, 0] },
            // The Lighter bid falls 1 cent before the hedge sells into it, 400 ms after the fill.
            State { t: 1_200, a: bbo(99.8, 100.02), l: bbo(99.99, 100.01), age: [0, 0] },
        ];
        let sim = XemmSim {
            maker: Leg::Left,
            required_bps: 10.0,
            distance: None,
            fee_maker: 0.0,
            fee_hedge: 0.0,
            fee_flatten: 4e-4,
            clip_usd: 100.0,
            depth_usd: 1_000.0,
            min_hedge_usd: 10.0,
            pending_age: 6_000,
            quote_age: 250,
            print_delay: 0,
            notice: 100,
            hedge_lag: 300,
            cooldown: 3_000,
            close_basis_bps: None,
        };
        // Our bid: 100.00 - 10 bps x 100.01 = 99.89999. A sale at 99.90 is not through it.
        let at = |t: i64, price: f64| Trade { t, venue: Leg::Left, price, size: 5.0, buy: false, age: 0 };
        let buy = |t: i64, price: f64| Trade { buy: true, ..at(t, price) };
        assert_eq!(simulate_xemm(&states, &states, &[at(1_000, 99.90)], &sim).fills, 0);
        let r = simulate_xemm(&states, &states, &[at(1_000, 99.85), at(2_000, 99.8)], &sim);
        // One fill (the second trade is inside the 3 s cooldown), hedged at 99.99.
        let price = 100.0 - 10e-4 * 100.01;
        let qty = 100.0 / 100.01;
        assert_eq!(r.fills, 1);
        assert!((r.pnl_usd - qty * (99.99 - price)).abs() < 1e-9);
        // The prints of one ms through our bid fill together (99.95 is not through it).
        let sized = |size: f64, price: f64| Trade { size, ..at(1_000, price) };
        let swept = simulate_xemm(&states, &states, &[sized(0.4, 99.85), sized(5.0, 99.95), sized(0.4, 99.8)], &sim);
        assert_eq!(swept.fills, 1);
        assert!((swept.pnl_usd - 0.8 * (99.99 - price)).abs() < 1e-9);
        // A $5 fill is under the $10 hedge minimum: the bot holds it, then flattens it on Aster (bid
        // 99.80, 4 bps taker), frozen for the 6 s wait and the 3 s cooldown after it.
        let small = simulate_xemm(&states, &states, &[sized(0.05, 99.85)], &sim);
        assert_eq!(small.inventory_clips, 0.0);
        assert!((small.pnl_usd - 0.05 * (99.8 - price - 99.8 * 4e-4)).abs() < 1e-9);
        let paused = |t| simulate_xemm(&states, &states, &[sized(0.05, 99.85), at(t, 99.7)], &sim).fills;
        assert_eq!((paused(9_900), paused(10_000)), (1, 2));
        // A print arriving 300 ms after the book change it made (Aster's) is moved back: the hedge
        // sells at 1,100 ms, before the Lighter bid fell.
        let late = simulate_xemm(&states, &states, &[at(1_000, 99.85)], &XemmSim { print_delay: 300, ..sim });
        assert!((late.pnl_usd - qty * (100.0 - price)).abs() < 1e-9);
        // Left open, that inventory (bought on Aster, sold on Lighter) closes at the given basis:
        // Lighter 10 bps over Aster's last mid, 99.91.
        let open = simulate_xemm(&states, &states, &[at(1_000, 99.85)], &XemmSim { close_basis_bps: Some(10.0), ..sim });
        assert!((open.pnl_usd - qty * (99.99 - price - 99.91 * 10e-4)).abs() < 1e-9);
        assert!((open.by_day.values().sum::<f64>() - open.pnl_usd).abs() < 1e-9);
        // The same sale on Lighter, with Lighter as the maker, leaves the opposite inventory.
        let lighter = XemmSim { maker: Leg::Right, ..sim };
        let mirror = simulate_xemm(&states, &states, &[Trade { venue: Leg::Right, ..at(1_000, 99.85) }], &lighter);
        assert!(open.inventory_clips > 0.0 && (mirror.inventory_clips + open.inventory_clips).abs() < 1e-12);
        // Holding it, the bot quotes only the side that reduces it, and not in the cooldown: a
        // purchase at 2 s, then a sale at 4.5 s, do not fill; a purchase at 5 s through our ask
        // (100.01 + 10 bps x 99.955) flattens it.
        let r = simulate_xemm(&states, &states, &[at(1_000, 99.85), buy(2_000, 100.2), at(4_500, 99.5), buy(5_000, 100.2)], &sim);
        assert_eq!((r.fills, r.inventory_clips), (2, 0.0));
        let ask = 100.01 + 10e-4 * 99.955;
        assert!((r.pnl_usd - qty * (99.99 - price) - qty * (ask - 100.01)).abs() < 1e-9);
        // The bot's gate: our bid sits 10 bps behind Aster's 100.00 bid, inside the 18 bps minimum;
        // within a 5 bps one it fills, unless that bid is thinner than the bot's depth.
        let gate = |min_bps: f64, bid_size: f64| {
            let thin = [State { a: Bbo { bid_size, ..states[0].a }, ..states[0] }, states[1]];
            simulate_xemm(&thin, &thin, &[at(1_000, 99.85)], &XemmSim { distance: Some((min_bps, 50.0)), ..sim }).fills
        };
        assert_eq!((gate(18.0, 100.0), gate(5.0, 100.0), gate(5.0, 1.0)), (0, 1, 0));
    }

    /// The collector keeps every moment the report trades on: scoring what it wrote gives the same
    /// trades as scoring every state and trade it saw, with the shipped settings and with stricter
    /// ones (the pricier Lighter tier, slower orders, a higher percentile), across a restart; at
    /// arrival here, and on the venues' clocks with the Tokyo delays, though the feeds here jitter
    /// and one stalls. Rebuilt from the stamps at arrival here, the books give the same result.
    #[test]
    fn the_recording_holds_every_moment_the_report_trades_on() {
        use universe::Venue::{Aster, Lighter, Hyperliquid};
        for venues in [(Aster, Lighter), (Aster, Hyperliquid), (Lighter, Hyperliquid)] { recording_case(venues); }
    }

    fn recording_case(venues: (universe::Venue, universe::Venue)) {
        let cfg = crate::config::Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/screener.toml"))).unwrap().report;
        let mut pair = universe::read_pairs(r#"[{"name":"X","aster":"XUSDT","lighter_id":1,"scale":1.0,"aster_subtypes":[],"aster_volume_usd":0.0,"lighter_volume_usd":0.0}]"#).unwrap().pop().unwrap();
        pair.left.venue = venues.0; pair.right.venue = venues.1;
        pair.name = format!("{}-{}:X", venues.0.code(), venues.1.code());
        let pairs = [pair.clone()];
        let rec = Recording::new(&cfg, &pairs);
        let mut collector = Collector::new(&rec, &pairs, HashMap::new(), 0);
        let header = [format!("P\t{}", serde_json::json!({ "recording": &rec })), format!("U\t{}", serde_json::to_string(&pairs).unwrap())];
        let lines = std::cell::RefCell::new(header.to_vec());
        let mut out = |l: String| lines.borrow_mut().push(l);
        let mut full = Series::default();
        // 40 minutes of a market whose basis swings ±10 bps every 8 minutes (the inventory fills both
        // ways and unwinds), wanders and now and then jumps, with 1 and 1.5 bps spreads, each side
        // thin now and then, trades at or through the touch, connection gaps, several events within a
        // millisecond, and a restart mid-window every 7.5 minutes (no prints in its first second: a
        // new run cannot see the quotes priced before it). Feed delays here: left 30-70 ms,
        // right 200-300 ms, which for 6 s stalls, its delay growing to 3.2 s.
        let mut seed = 1u64;
        let mut rand = move || {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (seed >> 11) as f64 / (1u64 << 53) as f64
        };
        let (mut t, mut mid, mut basis, mut started, mut restart) = (0i64, 100.0, 0.0, 0, 480_000);
        let (mut a, mut l) = (Bbo::default(), Bbo::default());
        // Every state, with the collector's stamps for it (it keeps them from going backwards).
        let state = |c: &Collector, t: i64, a: Bbo, l: Bbo| State { t, a, l, age: c.stamps(0).map(|s| (t - s) as i32) };
        while t < 40 * 60_000 {
            t += (rand() * 120.0) as i64;
            let stall = if (1_000_000..1_006_000).contains(&t) { (t - 1_000_000) / 2 } else { 0 };
            let delay = [30 + (rand() * 40.0) as i64, 200 + (rand() * 100.0) as i64 + stall];
            if t >= restart {
                // The new run's gate comes from what the old one wrote, its books from scratch.
                collector.finish(t, &mut out);
                full.states.push(state(&collector, t, Bbo::default(), Bbo::default()));
                let mut gates = HashMap::new();
                crate::collect::add_gates(&mut gates, lines.borrow().iter().cloned(), rec.span_ms(), t);
                collector = Collector::new(&rec, &pairs, gates, t);
                (a, l, started, restart) = (Bbo::default(), Bbo::default(), t, restart + 450_000);
            }
            mid *= 1.0 + (rand() - 0.5) * 1e-4;
            let swing = 10.0 * (t as f64 / 480_000.0 * std::f64::consts::TAU).sin();
            basis += 0.05 * (swing - basis) + (rand() - 0.5) * 2.0 + if rand() < 0.002 { 20.0 * (rand() - 0.5) } else { 0.0 };
            let thin = |r: f64| if r < 0.05 { 1.0 } else { 100.0 };
            let r = rand();
            if r < 0.495 {
                a = Bbo { bid: mid * (1.0 - 0.5e-4), bid_size: thin(rand()), ask: mid * (1.0 + 0.5e-4), ask_size: thin(rand()) };
                collector.on_event(t, Event::Book { pair: 0, venue: Leg::Left, bbo: a, stamp: (t - delay[0]).max(1) }, &mut out);
            } else if r < 0.99 {
                let m = mid * (1.0 + basis / 1e4);
                l = Bbo { bid: m * (1.0 - 0.75e-4), bid_size: thin(rand()), ask: m * (1.0 + 0.75e-4), ask_size: thin(rand()) };
                collector.on_event(t, Event::Book { pair: 0, venue: Leg::Right, bbo: l, stamp: (t - delay[1]).max(1) }, &mut out);
            } else if r < 0.9905 {
                l = Bbo::default();
                collector.closed(Leg::Right, t, &mut out);
            } else {
                let (venue, book) = if rand() < 0.5 { (Leg::Left, a) } else { (Leg::Right, l) };
                let buy = rand() < 0.5;
                let through = if rand() < 0.3 { 1.0 + rand() * 30e-4 } else { 1.0 };
                if book.known() && t - started >= PREROLL_MS {
                    let price = if buy { book.ask * through } else { book.bid / through };
                    let stamp = (t - delay[venue as usize] - 20).max(1);
                    full.trades.push(Trade { t, venue, price, size: 0.5, buy, age: (t - stamp) as i32 });
                    collector.on_event(t, Event::Trade { pair: 0, venue, price, size: 0.5, buy, stamp }, &mut out);
                }
                continue;
            }
            full.states.push(state(&collector, t, a, l));
        }
        collector.finish(t, &mut out);
        full.states.push(state(&collector, t, Bbo::default(), Bbo::default()));

        let mut data = Data::default();
        for line in lines.borrow().iter() {
            data.add("test", line, false).unwrap();
        }
        let data = data.sorted();
        let recorded = &data.series[&pair.name];
        (full.summaries, full.windows) = (recorded.summaries.clone(), recorded.windows.clone());
        // Selective even in this busy market: 55% (41% selecting on this host's arrival order only;
        // the mixes other feed delays could trade add the rest).
        assert!(recorded.states.len() * 5 < full.states.len() * 3, "{} of {} states recorded", recorded.states.len(), full.states.len());

        // Scenarios stay within the actual recording coverage, including HL maker quote age.
        for (tier, latency, percentile) in [("standard", 0.5, 90.0), ("standard", 1.0, 90.0), ("premium", 1.0, 90.0), ("standard", 2.0, 90.0), ("standard", 1.0, 95.0)] {
            let mut cfg = cfg.clone();
            (cfg.lighter_tier, cfg.taker.gate_percentile) = (tier.into(), percentile);
            rec.check(&cfg, cfg.lighter().unwrap(), &data.pairs).unwrap();
            rec.check_latency(&cfg, &pair, latency).unwrap();
            let score = |series: &Series, timing| score(&cfg, latency, &pair, series, timing).unwrap();
            let taker = |s: &Score| (s.taker_gated.trades, s.taker_gated.unresolved, s.taker_gated.by_day.clone());
            let xemm = |s: &Score| [&s.xemm_fixed, &s.xemm_reverse_fixed, &s.xemm_best, &s.xemm_reverse_best].map(|x| (x.fills, x.unresolved, x.required_bps, x.by_day.clone()));
            // On venue time a quote reaches back its age plus the print's delay here (up to 0.3 s),
            // past the pre-roll at x2: those fills are unresolved, not mispriced.
            let tokyo = (latency <= 1.0).then(|| Timing::Delays(&cfg.scenario["tokyo"]));
            for timing in [Some(Timing::Arrival), tokyo].into_iter().flatten() {
                let (r, f) = (score(recorded, timing), score(&full, timing));
                let arrival = matches!(timing, Timing::Arrival);
                let case = format!("{tier} x{latency} P{percentile}, {}", if arrival { "arrival" } else { "tokyo" });
                assert_eq!(taker(&r), taker(&f), "taker, {case}");
                assert_eq!(xemm(&r), xemm(&f), "xemm, {case}");
                assert!(r.uncovered_days == f.uncovered_days && (r.uncovered_days > 0.0) != arrival, "{case}");
                if (tier, latency, percentile) == ("standard", 1.0, 90.0) {
                    assert!(r.taker_gated.trades > 0 && r.xemm_best.fills > 0 && r.xemm_reverse_best.fills > 0, "the market must trade, {case}");
                }
            }
            let (arrival, host) = (score(recorded, Timing::Arrival), score(recorded, Timing::Host));
            assert_eq!(format!("{:?}", (&arrival.taker_gated, xemm(&arrival))), format!("{:?}", (&host.taker_gated, xemm(&host))), "host");
        }
        // A lower percentile would trade on moments not recorded: refused.
        let mut lower = cfg.clone();
        lower.taker.gate_percentile = 80.0;
        assert!(rec.check(&lower, lower.lighter().unwrap(), &data.pairs).is_err());
    }

    #[test]
    fn on_the_venues_clocks_a_lighter_change_can_lead_one_that_arrived_before_it() {
        let mut data = Data::default();
        // Aster 99.99/100 (stamp 990), Lighter 100.00/100.01 (700); Aster moves to 100.05/100.06
        // (1,090); then Lighter's 100.07/100.08, stamped 1,000, arrives last: here it never met
        // Aster's 100 ask, on the venues it did. Lighter changes unrecorded at 1,200.
        for line in ["B\t1000\tX\t99.99\t100\t100\t100.01\t15\t10\t300", "B\t1100\tX\t100.05\t100.06\t100\t100.01\t15\t10\t400",
            "B\t1300\tX\t100.05\t100.06\t100.07\t100.08\t15\t210\t300", "R\t1500\tX\t1\t300"] {
            data.add("v3", line, false).unwrap();
        }
        let s = &data.sorted().series["A-L:X"];
        let edges = |states: &[State]| states.iter().filter_map(|st| best_edge(&st.a, &st.l, 130.0)).map(|(_, e)| e.round()).collect::<Vec<_>>();
        assert_eq!(edges(&s.states), [0.0, 4.0, 1.0]);
        // Rebuilt at arrival: the same books. With Aster seen 10 ms after its stamps and Lighter
        // at once, the 7 bps edge shows at 1,000 ms (a leg's unchanged copies add states that
        // change nothing).
        let (view, venue, _, _) = timeline(s, None);
        assert_eq!(edges(&view), edges(&s.states));
        assert_eq!(venue.len(), 3);
        let (view, venue, _, _) = timeline(s, Some([10, 0]));
        assert_eq!(view.iter().map(|st| st.t).collect::<Vec<_>>(), [700, 700, 1000, 1000, 1100, 1100, 1200]);
        assert_eq!(edges(&view), [0.0, 7.0, 1.0, 1.0]);
        // From 1,200 ms Lighter is unrecorded, on the venues and in the view.
        assert!(state_at(&venue, 1_250).unwrap().l.bid.is_nan() && state_at(&view, 1_250).unwrap().l.bid.is_nan());
        assert_eq!(state_at(&venue, 1_050).unwrap().a.ask, 100.0);
    }

    #[test]
    fn spearman_is_one_for_the_same_order_and_minus_one_reversed() {
        assert_eq!(spearman(&[1.0, 2.0, 3.0, 4.0], &[10.0, 20.0, 30.0, 40.0]), Some(1.0));
        assert_eq!(spearman(&[1.0, 2.0, 3.0], &[3.0, 2.0, 1.0]), Some(-1.0));
        assert_eq!(spearman(&[1.0, 1.0, 1.0], &[1.0, 2.0, 3.0]), None);
    }

    #[test]
    fn old_and_new_files_share_only_the_aster_lighter_history() {
        let cfg = crate::config::Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/screener.toml"))).unwrap().report;
        let old = r#"[{"name":"X","aster":"XUSDT","lighter_id":1,"scale":1.0,"aster_subtypes":[],"aster_volume_usd":0.0,"lighter_volume_usd":0.0}]"#;
        let mut pairs = universe::read_pairs(old).unwrap();
        let rec = Recording::new(&cfg, &pairs);
        let mut legacy = serde_json::to_value(&rec).unwrap();
        legacy.as_object_mut().unwrap().remove("tail_ms");
        legacy.as_object_mut().unwrap().remove("preroll_ms");
        legacy["floors"] = serde_json::json!({"X":6.0});
        let mut data = Data::default();
        data.add("old", &format!("P\t{}", serde_json::json!({"recording":legacy})), false).unwrap();
        data.add("old", &format!("U\t{old}"), false).unwrap();
        data.add("old", "B\t1\tX\t100\t101\t100\t101\t15", false).unwrap();
        data.add("old", "T\t2\tX\tL\t101\t1\tB", false).unwrap();
        data.add("old", r#"S	{"t":0,"p":"X","samples":{"24":1},"up":1}"#, false).unwrap();
        pairs[0].right.venue = universe::Venue::Hyperliquid;
        pairs[0].name = "A-H:X".into();
        data.add("new", &format!("P\t{}", serde_json::json!({"version":2,"recording":Recording::new(&cfg,&pairs)})), false).unwrap();
        data.add("new", &format!("U\t{}", serde_json::to_string(&pairs).unwrap()), false).unwrap();
        data.add("new", "B\t3\tA-H:X\t100\t101\t100\t101\t15", false).unwrap();
        // v3 lines add the ages.
        data.add("new", "B\t5\tA-H:X\t100\t101\t100\t101\t15\t3\t250", false).unwrap();
        data.add("new", "T\t6\tA-H:X\t1\t101\t1\tB\t150", false).unwrap();
        assert!(data.add("new", "B\t7\tA-H:X\t100\t101\t100\t101\t15\t3", false).is_err());
        assert_eq!(data.pairs.len(), 2);
        assert_eq!(data.series["A-L:X"].states.len(), 1);
        assert_eq!((data.series["A-H:X"].states.len(), data.series["A-H:X"].trades.len()), (2, 1));
        assert_eq!(data.recordings[0].1.tail_ms, 1000);
        assert_eq!(data.recordings[1].1.tail_ms, 2000);
        assert!(data.recordings[0].1.check_latency(&cfg, &pairs[0], 2.0).is_err());
        assert!(data.recordings[1].1.check_latency(&cfg, &pairs[0], 2.0).is_ok());
        assert!(data.add("new", "T\t4\tA-H:X\tH\t101\t1\tB", false).is_err());
    }
}
